use crate::artifacts::{ArtifactStager, ArtifactStatusBoard, CredentialSource, StagedArtifact};
use crate::content_store::ContentStore;
use crate::prelude::*;
use crate::structs::edge_boot::{ClaimStage, Gate, OnRefused};
use crate::structs::{
    substitute_artifact_tokens, Allocation, AllocationHealth, StopCause, TaskAllocation,
    TaskConfig, TaskOnUnhealthyConfig, VolumeMount, VolumeReport, WorkerArtifactFetchConfigFull,
};
use crate::task_plugins::{
    NetworkGate, ResourceAssignment, ResourceEnforcement, StopContext, TaskInstance,
    TaskPluginRegistry,
};
use crate::worker::claims::{ClaimRecord, ClaimTable, LocalLedger};
use crate::worker::log_sink::{default_sinks, LogResource, LogSink};
use crate::worker::logs::{read_allocation_logs, CapturedStream};
use crate::worker::resources::NodeResourceGovernor;
use crate::worker::state::{
    pid_is_running, process_start_time, PersistedAllocation, PersistedStop, WorkerStateStore,
};
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

/// An allocation between "left the catalog" and "signalled": it keeps
/// serving until `until`, then takes the ladder like any other stop.
pub struct DrainingAllocation {
    allocation: Allocation,
    task: TaskConfig,
    instance: TaskInstance,
    cause: StopCause,
    remove_dirs: bool,
    until: std::time::Instant,
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
    /// Gate this launch is held at. [`Gate::Off`] for every ordinary
    /// allocation; [`Gate::Stage`] stops after `prepare`, which is exactly
    /// what makes staging overlap the commit without executing anything.
    pub gate: Gate,
    stager: Arc<ArtifactStager>,
    board: ArtifactStatusBoard,
    /// A published snapshot this allocation restores from, whose memory
    /// parts are not all in the node content store yet, and the keys the
    /// manager sent to open them: fetched first (Phase 7b cross-node
    /// restore).
    restore_fetch: Option<(
        crate::structs::checkpoint::SnapshotManifest,
        crate::checkpoint_seal::PartKeys,
    )>,
}

impl StagingJob {
    /// The blocking half of a staged start, run off the task-manager lock:
    /// fetch a published restore's missing parts into the node content
    /// store (pinned to this allocation until it starts), then stage the
    /// task's artifacts.
    fn stage(&self) -> R<Vec<StagedArtifact>> {
        if let Some((manifest, keys)) = self.restore_fetch.as_ref() {
            let started = std::time::Instant::now();
            let fetched = crate::worker::checkpoints::fetch_published_parts(
                &self.stager,
                manifest,
                keys,
                Some(&crate::content_store::PinOwner::allocation(
                    self.allocation.id,
                )),
            )
            .with_context(|| format!("restoring from snapshot {}", manifest.id))?;
            info!(
                allocation_id = self.allocation.id,
                snapshot_id = %manifest.id,
                fetched_bytes = fetched,
                duration_ms = started.elapsed().as_millis() as u64,
                "Fetched published snapshot parts for restore"
            );
        }
        if self.task.artifacts.is_empty() {
            return Ok(Vec::new());
        }
        self.stager.stage_task_artifacts(
            self.allocation.id,
            &self.task_dir,
            &self.task.artifacts,
            &self.board,
        )
    }
}

/// The outcome of ratifying a claim.
pub struct Ratified {
    pub claim: ClaimRecord,
    /// Set when this ratification *started* the workload (a `stage`-gated
    /// claim), so the caller must spawn its health monitor. `None` for a claim
    /// that was already running, and for a duplicate verdict — both already
    /// have one, and a second would double-count restarts.
    pub start_health_monitor: Option<(Allocation, TaskConfig)>,
}

/// What `begin_task_allocation` did with a new allocation.
pub enum BeginOutcome {
    /// No artifacts: the task was prepared and started synchronously.
    Started(Box<(Allocation, TaskConfig)>),
    /// A provisional allocation at gate `stage`: prepared, not started.
    /// Nothing outside the node can observe it, and no health monitor runs
    /// until ratification starts the workload.
    Staged(Box<(Allocation, TaskConfig)>),
    /// Artifacts declared: the allocation is registered as
    /// `staging artifacts` and the caller must run the staging job on a
    /// blocking thread (see [`launch_task_allocation`]).
    Staging(Box<StagingJob>),
    /// No artifacts, ordinary gate: the allocation is registered and the
    /// caller must finish the launch with [`complete_unlocked_launch`], off
    /// this lock (see [`launch_task_allocation`]).
    Launching(Box<UnlockedLaunch>),
    /// The same allocation is already being launched (or torn down after a
    /// cancelled launch). Nothing to do: if the manager still wants it once
    /// that settles, the next heartbeat's reconcile launches it.
    InFlight,
}

/// A launch whose slow half (volume mounts, the runtime's `prepare` and
/// `start`, which for some runtimes pulls an image) runs with the
/// task-manager lock released.
///
/// The heartbeat loop and the self-fence watchdog both need that lock. A
/// launch that held it through a slow pull or attach would stall heartbeats
/// past the lost window, get the node marked lost, and have its stateless
/// work replaced elsewhere while it kept running here.
///
/// The allocation is registered before the lock is released (a pid-less
/// placeholder in `instances`, plus `launching`), so the reconcile treats it
/// as present and never starts it twice. A stop or failure that lands
/// meanwhile only cancels it; the launch thread tears it down once the
/// runtime call returns.
pub struct UnlockedLaunch {
    generation: u64,
    allocation: Allocation,
    task: TaskConfig,
    instance: TaskInstance,
    plugin: Arc<crate::task_plugins::LoadedTaskPlugin>,
    volumes: Arc<WorkerVolumeManager>,
    volume_specs: Vec<VolumeMount>,
}

impl UnlockedLaunch {
    /// The slow half: mount, consume artifacts, `prepare`, `start`. Called
    /// with no lock held.
    fn run(&mut self) -> R<()> {
        let plugin = self.plugin.task_plugin();
        // Storage first: a task must never observe a missing or empty data
        // directory because its volume was still being mounted.
        if !self.volume_specs.is_empty() {
            self.volumes.unmount_for_allocation(self.allocation.id);
            let volume_instance =
                crate::worker::volumes::instance_ref_from_task(&self.allocation, &self.task);
            let mounted = self.volumes.mount_for_allocation(
                &self.allocation,
                volume_instance.as_ref(),
                &self.volume_specs,
            )?;
            apply_mounted_volumes(&mut self.instance, &self.task, &mounted);
        }
        let staged = self.instance.staged_artifacts.clone();
        for artifact in &staged {
            plugin
                .consume_artifact(artifact, &mut self.instance)
                .with_context(|| {
                    format!(
                        "consume artifact {} for task {}",
                        artifact.name.as_deref().unwrap_or("<unnamed>"),
                        self.task.name
                    )
                })?;
        }
        info!(
            allocation_id = self.allocation.id,
            task_name = %self.task.name,
            "Preparing task allocation"
        );
        plugin.prepare(&self.allocation, &self.task, &mut self.instance)?;
        info!(
            allocation_id = self.allocation.id,
            task_name = %self.task.name,
            "Starting task allocation"
        );
        plugin.start(&self.allocation, &self.task, &mut self.instance)
    }
}

/// How [`WorkerTaskManager::finish_unlocked_launch`] settled a launch.
enum LaunchFinish {
    Started(Box<(Allocation, TaskConfig)>),
    /// The runtime failed; the failure is already recorded.
    Failed(String),
    /// Stopped or failed while the runtime worked; the caller must tear it
    /// down without the lock, then call
    /// [`WorkerTaskManager::finish_cancelled_launch`].
    Cancelled(Box<UnlockedLaunch>),
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
    /// Priority band per desired allocation (local key), refreshed from the
    /// desired set on every reconcile. Read by the memory pressure response
    /// (`src/worker/pressure.rs`), which acts on lower bands first. Not
    /// persisted: the next heartbeat after a restart fills it again.
    bands: HashMap<i64, crate::structs::Band>,
    /// Allocations the memory pressure response stopped (local keys), held
    /// until the manager drops them from the desired set. Without it, a
    /// heartbeat that still lists an evicted allocation (the report of the
    /// eviction has not reached the manager yet) would relaunch it.
    pressure_evicted: std::collections::HashSet<i64>,
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
    /// Allocations inside their drain window: out of the live set and off
    /// the catalog, still serving, not yet signalled (docs/graceful-stop.md).
    draining: HashMap<i64, DrainingAllocation>,
    /// The default drain window — the node's heartbeat interval, set by the
    /// daemon each tick. Zero until it is, which is also what tests get.
    drain_window: std::time::Duration,
    state_store: WorkerStateStore,
    /// Root of per-allocation directories: `<state dir>/allocs/<alloc_id>/<task>/`.
    alloc_root: PathBuf,
    artifact_status: ArtifactStatusBoard,
    stager: Arc<ArtifactStager>,
    /// The node content store (`None` when disabled or it failed to open).
    content_store: Option<Arc<ContentStore>>,
    /// Publishing to the worker's `contentStore` (`None` when unconfigured
    /// or the node has no content store).
    content_publishing: Option<Arc<crate::content_publish::ContentPublishing>>,
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
    /// Provisional allocations this node has started but the store has not
    /// yet recorded (docs/edge-boot.md). Keyed by claim id; each record
    /// carries the negative worker-local allocation key the maps above use,
    /// so provisional work flows through every existing path unchanged.
    claims: ClaimTable,
    /// This node's advisory view of its own free capacity, so it cannot
    /// double-admit itself into oversubscription across concurrent claims.
    ledger: LocalLedger,
    /// The node's CPU/memory accounting and cgroup hierarchy (see
    /// `src/worker/resources.rs`). Disabled unless the worker was started
    /// with `resourceEnforcement.enabled`, in which case it governs every
    /// runtime that opted into the enforcement interface.
    resources: NodeResourceGovernor,
    /// Allocations whose runtime `prepare`/`start` is running on a launch
    /// thread without this lock held, keyed to the launch's generation (see
    /// [`UnlockedLaunch`]). While listed, the allocation sits in `instances`
    /// as a pid-less placeholder, exactly like one that is staging artifacts.
    launching: HashMap<i64, u64>,
    /// Launches stopped or failed while their runtime was still working.
    /// The launch thread tears them down when the runtime call returns, and
    /// the id cannot be launched again until it has.
    cancelled_launches: std::collections::HashSet<i64>,
    next_launch_generation: u64,
    /// The checkpoint channel's worker state: snapshot work in flight and
    /// done, reports owed to the manager, and allocations starting from a
    /// snapshot (see `crate::worker::checkpoints`).
    checkpoints: crate::worker::checkpoints::CheckpointState,
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
            bands: HashMap::new(),
            pressure_evicted: std::collections::HashSet::new(),
            preemptors: HashMap::new(),
            terminating: HashMap::new(),
            draining: HashMap::new(),
            drain_window: std::time::Duration::ZERO,
            state_store,
            alloc_root,
            artifact_status: ArtifactStatusBoard::new(),
            stager: Arc::new(ArtifactStager::new(
                WorkerArtifactFetchConfigFull::default(),
                Box::new(crate::artifacts::AnonymousCredentials),
            )),
            content_store: None,
            content_publishing: None,
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
            claims: ClaimTable::new(),
            ledger: LocalLedger::default(),
            resources: NodeResourceGovernor::from_env(),
            launching: HashMap::new(),
            cancelled_launches: std::collections::HashSet::new(),
            next_launch_generation: 0,
            checkpoints: Default::default(),
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

    /// Snapshot reports owed to the manager (`HeartbeatRequest.snapshot_reports`).
    pub fn drain_snapshot_reports(&mut self) -> Vec<crate::structs::SnapshotReport> {
        self.checkpoints.drain()
    }

    /// Put drained snapshot reports back after a failed heartbeat.
    pub fn requeue_snapshot_reports(&mut self, reports: Vec<crate::structs::SnapshotReport>) {
        self.checkpoints.requeue(reports);
    }

    pub fn checkpoints_mut(&mut self) -> &mut crate::worker::checkpoints::CheckpointState {
        &mut self.checkpoints
    }

    /// The runtime's half of a capture, under this lock: check the
    /// allocation can take a `kind` checkpoint, then (memory) have the
    /// runtime pause and write its memory parts. For a live memory
    /// checkpoint the task stays paused: the caller snapshots the volumes
    /// and then calls [`Self::checkpoint_resume`].
    pub(crate) fn checkpoint_begin(
        &mut self,
        allocation_id: i64,
        snapshot_id: &str,
        kind: crate::structs::checkpoint::SnapshotKind,
        then: crate::task_plugins::AfterCheckpoint,
    ) -> R<crate::worker::checkpoints::CaptureContext> {
        use crate::structs::checkpoint::SnapshotKind;
        use crate::task_plugins::{
            AfterCheckpoint, CheckpointDir, CheckpointQuery, CheckpointSupport,
        };
        let allocation = self
            .allocations
            .get(&allocation_id)
            .cloned()
            .with_context(|| format!("allocation {allocation_id} is not running on this node"))?;
        if self.terminated.contains_key(&allocation_id) {
            bail!("allocation {allocation_id} has exited; there is nothing running to capture");
        }
        let task = self
            .task_configs
            .get(&allocation_id)
            .cloned()
            .context("task config missing")?;
        let plugin = self
            .registry
            .plugin(&task.runtime)
            .context("task plugin not loaded")?;
        let content = self.content_store.clone();
        let mut memory = crate::structs::checkpoint::MemoryParts::default();
        let mut dir = None;
        let mut paused = false;
        match kind {
            SnapshotKind::Memory => {
                let support = plugin.checkpoint_support(&CheckpointQuery::new(&task.config));
                if support != CheckpointSupport::Memory {
                    bail!(
                        "task runtime `{}` cannot take memory checkpoints here (it supports {})",
                        task.runtime,
                        support.as_str()
                    );
                }
                let store = content.as_ref().context(
                    "memory snapshots need the node content store, which is disabled on this \
                     worker (content.enabled = false)",
                )?;
                // In the store's staging area, so ingesting each part is a
                // rename on the same filesystem.
                let out =
                    CheckpointDir::create(store.staging_path(&format!("ckpt-{snapshot_id}"))?)?;
                let then = match then {
                    AfterCheckpoint::Resume => AfterCheckpoint::Pause,
                    other => other,
                };
                let instance = self
                    .instances
                    .get_mut(&allocation_id)
                    .context("task instance missing")?;
                match plugin.checkpoint(&allocation, &task, instance, &out, then) {
                    Ok(parts) => memory = parts,
                    Err(err) => {
                        let _ = std::fs::remove_dir_all(out.path());
                        return Err(err);
                    }
                }
                paused = then == AfterCheckpoint::Pause;
                dir = Some(out);
            }
            SnapshotKind::Disk => {
                if then == AfterCheckpoint::Stop {
                    let instance = self
                        .instances
                        .get_mut(&allocation_id)
                        .context("task instance missing")?;
                    plugin.stop(&allocation, &task, instance)?;
                }
            }
        }
        Ok(crate::worker::checkpoints::CaptureContext {
            runtime: task.runtime.clone(),
            allocation,
            task,
            memory,
            dir,
            paused,
            volumes: self.volumes.clone(),
            content,
        })
    }

    /// Finish a suspend once its checkpoint is safely stored: kill a paused
    /// guest outright (it must not run past the captured instant), then tear
    /// the allocation down with cause `suspended`, which keeps its instance
    /// volume in place. A disk suspend (`paused` false) stops gracefully.
    /// Runs under the task-manager lock, so the exit poller never sees the
    /// stop as a crash.
    pub(crate) fn checkpoint_finish_suspend(&mut self, allocation_id: i64, paused: bool) -> R<()> {
        if paused {
            if let (Some(task), Some(instance)) = (
                self.task_configs.get(&allocation_id),
                self.instances.get_mut(&allocation_id),
            ) {
                crate::task_plugins::reap_task_process(
                    allocation_id,
                    &task.name,
                    instance,
                    &task.runtime,
                );
            }
        }
        self.stop_causes.insert(allocation_id, StopCause::Suspended);
        self.stop_allocation(allocation_id)
    }

    /// Resume a task [`Self::checkpoint_begin`] left paused.
    pub(crate) fn checkpoint_resume(&mut self, allocation_id: i64) -> R<()> {
        let allocation = self
            .allocations
            .get(&allocation_id)
            .context("allocation missing")?;
        let task = self
            .task_configs
            .get(&allocation_id)
            .context("task config missing")?;
        let plugin = self
            .registry
            .plugin(&task.runtime)
            .context("task plugin not loaded")?;
        let instance = self
            .instances
            .get_mut(&allocation_id)
            .context("task instance missing")?;
        plugin.resume_after_checkpoint(allocation, task, instance)
    }

    /// A handle to the volume manager for work that must NOT run under the
    /// task-manager lock — reclaim dispatch, whose driver calls can stall on a
    /// dead storage backend. Cheap to clone; see the field docs.
    pub fn volumes_handle(&self) -> Arc<WorkerVolumeManager> {
        self.volumes.clone()
    }

    /// The artifact stager, for work that fetches outside an allocation
    /// (warming a published snapshot).
    pub fn stager_handle(&self) -> Arc<ArtifactStager> {
        self.stager.clone()
    }

    /// Applies the worker's artifact fetch settings and credential source
    /// (called once after pre-registration delivers the worker config).
    pub fn configure_artifacts(
        &mut self,
        config: WorkerArtifactFetchConfigFull,
        credentials: Box<dyn CredentialSource>,
    ) {
        self.stager = Arc::new(
            ArtifactStager::new(config, credentials).with_content_store(self.content_store.clone()),
        );
    }

    /// Sets the node content store. Call before [`Self::configure_artifacts`],
    /// which hands it to the artifact stager.
    pub fn configure_content_store(&mut self, store: Option<Arc<ContentStore>>) {
        self.content_store = store;
    }

    /// The node content store, when this worker has one. Checkpoint code puts
    /// and pins snapshot parts here (see [`crate::content_store`]).
    pub fn content_store(&self) -> Option<Arc<ContentStore>> {
        self.content_store.clone()
    }

    /// Sets the publisher for the worker's `contentStore`.
    pub fn configure_content_publishing(
        &mut self,
        publishing: Option<Arc<crate::content_publish::ContentPublishing>>,
    ) {
        self.content_publishing = publishing;
    }

    /// Publishing to the shared content store, when this worker has one.
    /// Phase 7b publishes snapshot parts through this (`durability = "store"`).
    pub fn content_publishing(&self) -> Option<Arc<crate::content_publish::ContentPublishing>> {
        self.content_publishing.clone()
    }

    pub fn load_builtin_plugins(&mut self) {
        load_builtin_plugins_into(&mut self.registry);
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        self.registry.readiness_checks()
    }

    /// The allocation ids this node is running, as the **manager** knows
    /// them.
    ///
    /// Unratified provisional work is deliberately absent: the manager does
    /// not know about it yet, and reporting a negative local key as a running
    /// allocation would be a lie the reconcile loop would act on. It is
    /// reported separately, in `HeartbeatRequest.provisional_claims`.
    pub fn allocations_snapshot(&self) -> Vec<i64> {
        let mut allocations = self
            .instances
            .keys()
            .filter_map(|local_key| self.external_id(*local_key))
            .collect::<Vec<_>>();
        allocations.sort_unstable();
        allocations
    }

    /// Every allocation the usage sampler can measure, addressed by the id
    /// the manager knows (`src/worker/usage.rs`): those running in a
    /// worker-owned cgroup, and macOS dev-mode sandboxes on Apple
    /// `container`, which `container stats` measures instead. Other
    /// ungoverned work is absent, as is unratified provisional work.
    pub fn usage_targets(&self) -> Vec<crate::worker::usage::UsageTarget> {
        use crate::worker::usage::UsageSource;
        let mut targets: Vec<_> = self
            .instances
            .iter()
            .filter_map(|(local_key, instance)| {
                let allocation_id = self.external_id(*local_key).filter(|id| *id >= 0)?;
                let sandbox = self
                    .task_configs
                    .get(local_key)
                    .filter(|task| task.runtime == "sandbox")
                    .map(|_| crate::task_plugins::sandbox_usage_source(*local_key, instance))
                    .unwrap_or_default();
                let source = match (
                    instance.resource_assignment.as_ref(),
                    sandbox.apple_container,
                ) {
                    (Some(assignment), _) => UsageSource::Cgroup(assignment.absolute_cgroup_path()),
                    (None, Some(name)) => UsageSource::AppleContainer(name),
                    (None, None) => return None,
                };
                Some(crate::worker::usage::UsageTarget {
                    allocation_id,
                    source,
                    tap_device: sandbox.tap_device,
                    pressure: self.pressure_meta(*local_key),
                    idle: self.task_configs.get(local_key).and_then(|task| {
                        crate::worker::idle::IdlePolicy::for_task(
                            task,
                            crate::worker::idle::local_mhz_per_core(),
                        )
                    }),
                })
            })
            .collect();
        targets.sort_by_key(|target| target.allocation_id);
        targets
    }

    /// What the memory pressure response needs to know about one running
    /// allocation besides its measurements. Anything unknown falls back to
    /// [`crate::worker::pressure::PressureMeta::default`], which is
    /// untouchable (node-pinned).
    fn pressure_meta(&self, local_key: i64) -> crate::worker::pressure::PressureMeta {
        let Some(allocation) = self.allocations.get(&local_key) else {
            return Default::default();
        };
        let reserved_bytes = self
            .task_configs
            .get(&local_key)
            .and_then(|task| crate::worker::resources::memory_limit_bytes(&task.resources))
            .unwrap_or(0);
        // Preemption's rule, applied to what this node mounted: instance
        // storage on a node-local driver can only come back here.
        let node_pinned = self
            .volume_specs
            .get(&local_key)
            .into_iter()
            .flatten()
            .any(|mount| mount.instance_volume && self.volumes.is_node_local(&mount.driver));
        // `suspend.onPressure`: the `suspend` step may checkpoint it. Only a
        // keyed instance can be suspended, and only one the manager knows
        // (a provisional claim has no instance row to suspend).
        let on_pressure = allocation.instance_id.is_some()
            && local_key >= 0
            && self
                .task_configs
                .get(&local_key)
                .and_then(|task| task.suspend.as_ref())
                .and_then(|suspend| suspend.resolve().ok())
                .is_some_and(|suspend| suspend.on_pressure);
        crate::worker::pressure::PressureMeta {
            job_id: allocation.job_id.clone(),
            band: self.bands.get(&local_key).copied().unwrap_or_default(),
            reserved_bytes,
            node_pinned,
            on_pressure,
        }
    }

    /// Stop an allocation for memory pressure, ahead of the manager.
    ///
    /// The stop runs the ordinary graceful path (stop notice with `cause =
    /// node_pressure`, drain, signal, kill after `stop.killAfter`), and the
    /// allocation is held out of relaunch until the manager drops it from
    /// the desired set, which it does once the heartbeat reports the
    /// eviction.
    pub fn evict_for_pressure(&mut self, allocation_id: i64) -> R<()> {
        let local = self.local_key(allocation_id);
        if !self.instances.contains_key(&local) {
            bail!("allocation {allocation_id} is not running on this node");
        }
        self.pressure_evicted.insert(local);
        self.stop_causes.insert(local, StopCause::NodePressure);
        warn!(
            allocation_id,
            "Stopping allocation for node memory pressure"
        );
        self.stop_allocation(local)
    }

    /// The checkpoint kind the `suspend` pressure step takes for an
    /// allocation: the task's `suspend.checkpoint` when set, else memory when
    /// the runtime can take one of this allocation, else disk. The same
    /// default SuspendInstance uses.
    pub fn local_suspend_kind(
        &self,
        allocation_id: i64,
    ) -> R<crate::structs::checkpoint::SnapshotKind> {
        use crate::structs::checkpoint::SnapshotKind;
        let task = self
            .task_configs
            .get(&allocation_id)
            .with_context(|| format!("allocation {allocation_id} is not running on this node"))?;
        let declared = task
            .suspend
            .as_ref()
            .and_then(|suspend| suspend.resolve().ok())
            .and_then(|resolved| resolved.checkpoint);
        if let Some(kind) = declared {
            return Ok(kind);
        }
        let launched_for_memory = self
            .instances
            .get(&allocation_id)
            .is_some_and(|instance| instance.checkpoint_launch);
        let memory = launched_for_memory
            && self.registry.plugin(&task.runtime).is_some_and(|plugin| {
                plugin.checkpoint_support(&crate::task_plugins::CheckpointQuery::new(&task.config))
                    == crate::task_plugins::CheckpointSupport::Memory
            });
        Ok(if memory {
            SnapshotKind::Memory
        } else {
            SnapshotKind::Disk
        })
    }

    /// Keep an allocation from being relaunched until the manager drops it
    /// from the desired set (a local suspension the manager has not applied
    /// yet). The same hold a pressure eviction uses.
    pub fn hold_out_of_relaunch(&mut self, allocation_id: i64) {
        self.pressure_evicted.insert(allocation_id);
    }

    /// Undo [`Self::hold_out_of_relaunch`] when the work it guarded failed
    /// and the allocation keeps running.
    pub fn release_relaunch_hold(&mut self, allocation_id: i64) {
        self.pressure_evicted.remove(&allocation_id);
    }

    /// Give back guest memory above an allocation's reservation through its
    /// runtime (a sandbox balloon), for the `reclaim` pressure step. `None`
    /// when the runtime has no such lever, in which case the caller uses
    /// `memory.reclaim` on the cgroup.
    pub fn reclaim_guest_memory(&mut self, allocation_id: i64, bytes: u64) -> R<Option<u64>> {
        let local = self.local_key(allocation_id);
        let (Some(allocation), Some(task), Some(instance)) = (
            self.allocations.get(&local),
            self.task_configs.get(&local),
            self.instances.get(&local),
        ) else {
            return Ok(None);
        };
        let Some(plugin) = self.registry.plugin(&task.runtime) else {
            return Ok(None);
        };
        plugin.reclaim_guest_memory(allocation, task, instance, bytes)
    }

    /// Return what [`Self::reclaim_guest_memory`] took (deflate the balloon)
    /// once the pressure episode is over.
    pub fn release_guest_memory(&mut self, allocation_id: i64) -> R<()> {
        let local = self.local_key(allocation_id);
        let (Some(allocation), Some(task), Some(instance)) = (
            self.allocations.get(&local),
            self.task_configs.get(&local),
            self.instances.get(&local),
        ) else {
            return Ok(());
        };
        let Some(plugin) = self.registry.plugin(&task.runtime) else {
            return Ok(());
        };
        plugin.release_guest_memory(allocation, task, instance)
    }

    /// Whether this worker currently runs the allocation (used by identity
    /// refresh loops to stop once the allocation is gone).
    pub fn has_allocation(&self, allocation_id: i64) -> bool {
        self.instances.contains_key(&self.local_key(allocation_id))
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

    /// Rewrite the `onChange = "update"` KV files of a running allocation
    /// whose rendered content or mode changed (docs/kv-store.md, "Live file
    /// updates"), and record them in the stored task config so a later
    /// restart launches with the same content. Returns the marker of each
    /// rewritten file, for the caller to apply `changeMode`.
    ///
    /// Only entries carrying the manager's `liveUpdate` marker are touched;
    /// any other difference waits for the next (re)start, as before. Paths
    /// resolve exactly as the runtime's start path resolves them. The
    /// content may be a secret (a private key): it is never logged.
    pub fn apply_live_files(
        &mut self,
        allocation_id: i64,
        incoming: &[Value],
    ) -> Vec<crate::structs::LiveFileUpdate> {
        let local = self.local_key(allocation_id);
        let Some(task) = self.task_configs.get(&local) else {
            return Vec::new();
        };
        if !self.instances.contains_key(&local) {
            return Vec::new();
        }
        let (task_name, runtime) = (task.name.clone(), task.runtime.clone());
        let mut stored: Vec<Value> = task
            .config
            .get("files")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let root = self
            .registry
            .plugin(&runtime)
            .and_then(|plugin| plugin.file_delivery_root());
        let task_dir = self
            .instances
            .get(&local)
            .and_then(|instance| instance.task_dir.clone())
            .unwrap_or_else(|| self.task_dir_for(local, &task_name));

        let mut applied = Vec::new();
        for entry in incoming {
            let Some(marker) = entry.get("liveUpdate") else {
                continue;
            };
            let Ok(marker) =
                serde_json::from_value::<crate::structs::LiveFileUpdate>(marker.clone())
            else {
                continue;
            };
            let Some(path) = entry.get("path").and_then(Value::as_str) else {
                continue;
            };
            let content = entry.get("content").and_then(Value::as_str).unwrap_or("");
            let mode = entry.get("mode").and_then(Value::as_str);
            let position = stored
                .iter()
                .position(|existing| existing.get("path").and_then(Value::as_str) == Some(path));
            if let Some(existing) = position.map(|index| &stored[index]) {
                if existing.get("content").and_then(Value::as_str) == Some(content)
                    && existing.get("mode").and_then(Value::as_str) == mode
                {
                    continue;
                }
            }
            let host_path = match root {
                Some(root) => match crate::task_plugins::resolve_task_dir_destination(root, path) {
                    Ok(relative) => task_dir.join(relative),
                    Err(err) => {
                        warn!(allocation_id, path, error = %err, "Skipping live KV file");
                        continue;
                    }
                },
                None => PathBuf::from(path),
            };
            let bits = crate::task_plugins::injected_file_mode(mode);
            if let Err(err) = crate::worker::identity::write_token_file(&host_path, content, bits) {
                warn!(allocation_id, path, error = ?err, "Failed to rewrite live KV file");
                continue;
            }
            match position {
                Some(index) => stored[index] = entry.clone(),
                None => stored.push(entry.clone()),
            }
            info!(allocation_id, path, "Rewrote live KV file");
            applied.push(marker);
        }

        if !applied.is_empty() {
            if let Some(config) = self
                .task_configs
                .get_mut(&local)
                .and_then(|task| task.config.as_object_mut())
            {
                config.insert("files".into(), Value::Array(stored));
            }
            self.persist_state();
        }
        applied
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

    /// Hand a refreshed identity token to the runtime, for runtimes whose
    /// workload cannot read the host file the worker just rewrote (a sandbox
    /// guest, through its identity disk). A no-op for every other runtime.
    pub fn identity_tokens_refreshed(&mut self, allocation_id: i64) -> R<()> {
        let (Some(allocation), Some(task)) = (
            self.allocations.get(&allocation_id).cloned(),
            self.task_configs.get(&allocation_id).cloned(),
        ) else {
            return Ok(());
        };
        let Some(plugin) = self.registry.plugin(&task.runtime) else {
            return Ok(());
        };
        let Some(instance) = self.instances.get_mut(&allocation_id) else {
            return Ok(());
        };
        plugin
            .identity_tokens_refreshed(&allocation, &task, instance)
            .with_context(|| {
                format!("Failed to refresh identity tokens in allocation {allocation_id}")
            })
    }

    /// Per-allocation health, addressed by the ids the manager knows.
    /// Unratified provisional work is omitted for the same reason it is
    /// omitted from [`Self::allocations_snapshot`].
    pub fn allocation_health_snapshot(&self) -> Vec<AllocationHealth> {
        let mut entries = self
            .allocation_health
            .values()
            .filter_map(|entry| {
                let mut entry = entry.clone();
                entry.artifact_states = self.artifact_status.snapshot_json(entry.allocation_id);
                entry.allocation_id = self.external_id(entry.allocation_id)?;
                Some(entry)
            })
            .collect::<Vec<_>>();
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
            .filter(|allocation_id| !self.draining.contains_key(allocation_id))
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

    /// Terminal outcomes owed to the manager, addressed by the ids it knows.
    /// Reports for unratified provisional work are dropped: there is no
    /// allocation for the manager to complete.
    pub fn drain_terminations(&mut self) -> Vec<TerminatedAllocationReport> {
        let reports = std::mem::take(&mut self.terminated_reports);
        reports
            .into_iter()
            .filter_map(|mut report| {
                report.allocation_id = self.external_id(report.allocation_id)?;
                Some(report)
            })
            .collect()
    }

    /// Put drained terminal reports back after a failed heartbeat, so a dead
    /// connection cannot swallow a batch task's completion.
    pub fn requeue_terminations(&mut self, reports: Vec<TerminatedAllocationReport>) {
        self.terminated_reports.extend(reports);
    }

    /// Failures owed to the manager, addressed by the ids it knows. A
    /// failure on unratified provisional work is dropped for the same reason
    /// a terminal report is: there is nothing for the manager to reschedule,
    /// and the claim's own kill path already emitted the event.
    pub fn drain_failures(&mut self) -> Vec<FailedAllocationReport> {
        let failures = std::mem::take(&mut self.failed);
        failures
            .into_iter()
            .filter_map(|mut failure| {
                failure.allocation_id = self.external_id(failure.allocation_id)?;
                Some(failure)
            })
            .collect()
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
        let plugin = self.registry.plugin(runtime)?;
        let root = plugin.file_delivery_root()?;
        let mut host = self.alloc_dir(allocation_id).join(task_name);
        if let Some(subdir) = plugin.file_delivery_host_subdir() {
            host.push(subdir);
        }
        Some((root, host))
    }

    /// Root of per-allocation directories (`<state>/allocs/`).
    /// Where this worker persists its allocations and contact record.
    pub fn state_store(&self) -> &WorkerStateStore {
        &self.state_store
    }

    pub fn alloc_root(&self) -> &std::path::Path {
        &self.alloc_root
    }

    /// The host task directory (`allocs/<alloc>/<task>/`), whether or not
    /// the task has started yet.
    pub fn task_dir_for(&self, allocation_id: i64, task_name: &str) -> PathBuf {
        self.alloc_dir(allocation_id).join(task_name)
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
        let reason = reason.into();
        // An allocation that dies before its restore finished reports the
        // restore as failed, with the same reason, so the snapshot's event
        // carries it.
        if self.checkpoints.has_pending_restore(allocation_id) {
            self.checkpoints.record_restore(
                allocation_id,
                false,
                reason.clone(),
                std::time::Instant::now(),
            );
        }
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }
        // A launch still inside its runtime call is cancelled rather than
        // destroyed here: its launch thread owns the real instance and tears
        // it down (runtime, volumes, cores) once the call returns.
        let launching = self.launching.remove(&allocation_id).is_some();
        if launching {
            self.cancelled_launches.insert(allocation_id);
        } else if let (Some(allocation), Some(task)) = (
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
        if !launching {
            self.volumes.unmount_for_allocation(allocation_id);
            let assignment = self
                .instances
                .get(&allocation_id)
                .and_then(|instance| instance.resource_assignment.clone());
            self.resources.release(allocation_id, assignment.as_ref());
        }
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
            reason,
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
        // Desired ids are the manager's; the maps below are keyed by this
        // node's. They differ only for a ratified provisional allocation,
        // which keeps the local key it booted under.
        let desired_ids = allocations
            .iter()
            .map(|allocation| self.local_key(allocation.allocation_id))
            .collect::<std::collections::HashSet<_>>();
        // Bands ride every desired allocation; keep exactly the desired set's.
        self.bands = allocations
            .iter()
            .map(|allocation| {
                (
                    self.local_key(allocation.allocation_id),
                    crate::structs::Band::from_stored(&allocation.priority_band),
                )
            })
            .collect();
        // An eviction is settled once the manager stops asking for the
        // allocation; until then it must not be relaunched.
        self.pressure_evicted
            .retain(|local| desired_ids.contains(local));
        // Unratified claims are exempt from the stop sweep for the length of
        // their lease. The manager cannot list work it does not know about,
        // so an absence here is not a decision — it is ignorance, and reading
        // it as "stop this" would make every provisional start die on its
        // first heartbeat. When the lease expires the claim is killed by
        // `sweep_expired_claims`, which is the same outcome one tick later
        // with no new reaper.
        let protected = self
            .claims
            .unratified_local_keys()
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        // A restore replaces an instance's allocation in one manager
        // transaction. Allocation ids are never reused (AUTOINCREMENT), so
        // the replacement normally has a new id; a store migrated from a
        // version that reused ids can still hand one out once. An id this
        // node runs that now arrives with a restore payload it never started
        // from is that replacement: stop the old process so the restore
        // starts once it is gone, instead of mistaking it for the same
        // allocation and never restoring at all.
        let replaced: Vec<i64> = allocations
            .iter()
            .filter(|allocation| allocation.restore.is_some())
            .map(|allocation| self.local_key(allocation.allocation_id))
            .filter(|local| {
                self.instances.contains_key(local) && !self.checkpoints.restores.contains_key(local)
            })
            .collect();
        for allocation_id in replaced {
            info!(
                allocation_id,
                "Allocation id reused by a snapshot restore; stopping the previous task first"
            );
            if let Err(err) = self.stop_allocation(allocation_id) {
                warn!(allocation_id, error = ?err, "Failed to stop the allocation a restore replaces");
            }
        }
        let existing_ids = self.instances.keys().copied().collect::<Vec<_>>();
        for allocation_id in existing_ids {
            if desired_ids.contains(&allocation_id) || protected.contains(&allocation_id) {
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
            .filter(|allocation| {
                let local = self.local_key(allocation.allocation_id);
                !self.instances.contains_key(&local)
                    && !self.terminating.contains_key(&local)
                    && !self.draining.contains_key(&local)
                    && !self.cancelled_launches.contains(&local)
                    && !self.pressure_evicted.contains(&local)
            })
            .cloned()
            .collect();

        // A draining allocation still holds its ports and its process, so it
        // holds starts back exactly as an in-grace one does.
        if !pending.is_empty() && (self.has_terminating() || self.has_draining()) {
            debug!(
                held = pending.len(),
                terminating = self.terminating.len(),
                draining = self.draining.len(),
                "Holding new allocations until stopping allocations exit"
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

        if self.launching.remove(&allocation_id).is_some() {
            // Its runtime is mid-`prepare`/`start` on a launch thread. Tearing
            // down storage under it now would race that call, so leave the
            // teardown to the launch thread, which sees the cancellation as
            // soon as the runtime returns.
            info!(
                allocation_id,
                cause = %cause,
                "Cancelling allocation launch in progress"
            );
            self.cancelled_launches.insert(allocation_id);
            self.persist_state();
            return Ok(());
        }

        let (Some(allocation), Some(task), Some(mut instance)) = (allocation, task, instance)
        else {
            // Nothing was ever started for this id; there is no process to
            // wait on, so complete the teardown now. No assignment to release
            // — `release` still clears any reservation booked against the id.
            self.finish_teardown(allocation_id, remove_dirs, None);
            return Ok(());
        };

        // Drain before the signal (docs/graceful-stop.md): a task that
        // advertises a service keeps serving for one drain window after it
        // has left the catalog, so consumers on other nodes learn it is gone
        // before it stops accepting. Purely an ordering change — this node's
        // forwarder stopped routing to it on the heartbeat that removed it.
        let drain = self.drain_window_for(&task, remove_dirs);
        if !drain.is_zero() && Self::instance_process_alive(&mut instance) {
            info!(
                allocation_id,
                task_name = %task.name,
                cause = %cause,
                drain_ms = drain.as_millis() as u64,
                "Draining allocation before stop"
            );
            self.draining.insert(
                allocation_id,
                DrainingAllocation {
                    allocation,
                    task,
                    instance,
                    cause,
                    remove_dirs,
                    until: std::time::Instant::now() + drain,
                },
            );
            self.persist_state();
            return Ok(());
        }
        self.signal_and_terminate(
            allocation_id,
            allocation,
            task,
            instance,
            cause,
            remove_dirs,
        );
        Ok(())
    }

    /// How long `task` drains before its signal: `stop.drain` when set,
    /// otherwise the node's heartbeat interval. Zero for a task with no
    /// `service` (nothing routes to it) and for an in-place re-dispatch
    /// (`remove_dirs: false`: the same allocation relaunches at once and
    /// needs its ports back).
    fn drain_window_for(&self, task: &TaskConfig, remove_dirs: bool) -> std::time::Duration {
        if !remove_dirs || task.service.is_none() {
            return std::time::Duration::ZERO;
        }
        task.stop_config()
            .drain
            .unwrap_or(self.drain_window)
            .min(crate::structs::MAX_DRAIN)
    }

    /// The default drain window for stops on this node — its heartbeat
    /// interval, which is how long every other node needs to learn an
    /// endpoint is gone.
    pub fn set_drain_window(&mut self, window: std::time::Duration) {
        self.drain_window = window;
    }

    /// Advance every allocation whose drain window has closed (or whose
    /// process exited on its own) to the signal step. Driven by the task
    /// supervisor beside [`Self::reap_terminating`]. Returns the ids moved.
    pub fn reap_draining(&mut self) -> Vec<i64> {
        if self.draining.is_empty() {
            return Vec::new();
        }
        let now = std::time::Instant::now();
        let mut ready: Vec<i64> = self
            .draining
            .iter_mut()
            .filter_map(|(allocation_id, entry)| {
                (now >= entry.until || !Self::instance_process_alive(&mut entry.instance))
                    .then_some(*allocation_id)
            })
            .collect();
        ready.sort_unstable();
        for allocation_id in &ready {
            if let Some(entry) = self.draining.remove(allocation_id) {
                self.signal_and_terminate(
                    *allocation_id,
                    entry.allocation,
                    entry.task,
                    entry.instance,
                    entry.cause,
                    entry.remove_dirs,
                );
            }
        }
        ready
    }

    pub fn has_draining(&self) -> bool {
        !self.draining.is_empty()
    }

    /// Allocation ids inside their drain window, for tests and diagnostics.
    pub fn draining_ids(&self) -> Vec<i64> {
        let mut ids = self.draining.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// The signal step of the ladder: notice, signal, then the terminating
    /// set (graceful) or an immediate teardown.
    fn signal_and_terminate(
        &mut self,
        allocation_id: i64,
        allocation: Allocation,
        task: TaskConfig,
        mut instance: TaskInstance,
        cause: StopCause,
        remove_dirs: bool,
    ) {
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
            return;
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
        // Exec addresses an allocation by the manager's id; translate to the
        // worker-local key a ratified provisional allocation still runs under.
        let allocation_id = self.local_key(allocation_id);
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
        self.run_allocation_gated(allocation, task, Gate::Off)
    }

    /// [`Self::run_allocation`] with an explicit gate; `Gate::Stage` stops
    /// after `prepare`.
    pub fn run_allocation_gated(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        gate: Gate,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            task_runtime = %task.runtime,
            gate = %gate,
            "Received task allocation"
        );

        self.registry
            .load_plugin(&task.runtime, true)
            .with_context(|| format!("Failed to load task plugin {}", task.runtime))?;
        let instance = self.prepare_instance(allocation, task)?;
        self.instances.insert(allocation.id, instance);
        self.prepare_registered_allocation(allocation, task)?;
        if gate == Gate::Stage {
            self.set_allocation_health(allocation.id, false, "staged (awaiting ratification)");
            return Ok(());
        }
        self.start_registered_allocation(allocation, task, gate)
    }

    /// Everything up to (but not including) `start`: volume mounts, artifact
    /// consumption, and the runtime's own `prepare`.
    ///
    /// Split out from [`Self::start_prepared_allocation`] because it is
    /// exactly the work a provisional allocation at gate `stage` is allowed
    /// to do — the `stage` gate is a pause between two calls the worker
    /// already made in sequence, not a new execution path.
    fn prepare_registered_allocation(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
    ) -> R<()> {
        // A restore rolls the instance volume back before anything mounts
        // it, so the task never sees the state it is being rolled back from.
        self.restore_allocation_volumes(allocation)?;

        // Storage first: a task must never observe a missing or empty data
        // directory because its volume was still being mounted. A failure here
        // fails the allocation, which is the honest outcome — the manager
        // reschedules and the operator sees the driver's error.
        self.mount_allocation_volumes(allocation, task)?;

        // Resource placement next, and before `prepare()`: a task must never
        // observe its CPU/memory limits arriving after it started. Runtimes
        // that did not opt in get `None` and are untouched.
        let (enforcement, memory_override) = self
            .registry
            .plugin(&task.runtime)
            .map(|plugin| {
                (
                    plugin.resource_enforcement(),
                    plugin.enforced_memory_limit_bytes(task),
                )
            })
            .unwrap_or((ResourceEnforcement::Unsupported, None));
        let assignment = self
            .resources
            .assign(allocation.id, task, enforcement, memory_override)
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
        // Launch ready for memory checkpoints when the task asks for them
        // and the runtime can take them here (a sandbox on a real VMM).
        instance.checkpoint_launch = crate::structs::snapshot::memory_checkpoint_ask(task)
            .is_some()
            && plugin.checkpoint_support(&crate::task_plugins::CheckpointQuery::new(&task.config))
                == crate::task_plugins::CheckpointSupport::Memory;

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

        self.allocations.insert(allocation.id, allocation.clone());
        self.task_configs.insert(allocation.id, task.clone());
        Ok(())
    }

    /// The second half: `start` the prepared instance and register it as
    /// running.
    ///
    /// `gate` is [`Gate::Off`] for every ordinary allocation. When it is
    /// [`Gate::Boot`], the runtime's network gate is shut *before* the
    /// workload starts, so the guest never has a live interface it could have
    /// used — the ordering is the guarantee, not the mechanism.
    fn start_registered_allocation(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        gate: Gate,
    ) -> R<()> {
        let plugin = self
            .registry
            .plugin(&task.runtime)
            .context("Task plugin not loaded")?;
        let instance = self
            .instances
            .get_mut(&allocation.id)
            .context("Task instance missing")?;

        if gate == Gate::Boot {
            if plugin.network_gate_for(allocation) == NetworkGate::Advisory {
                // Stated loudly rather than assumed: for host-process
                // runtimes Legion withholds service registration and the
                // node-local forwarders, but the workload shares the host's
                // network stack and can still reach the outside world.
                // docs/edge-boot.md says so; this is the operator's copy.
                warn!(
                    allocation_id = allocation.id,
                    task_runtime = %task.runtime,
                    "The `boot` gate is advisory for this runtime: Legion withholds service \
                     registration and upstream forwarders, but the task can still reach the \
                     network through the host"
                );
            }
            plugin.set_network_gate(allocation, task, instance, false)?;
        }

        if let Some(restore) = self.checkpoints.restores.get(&allocation.id).cloned() {
            let started = std::time::Instant::now();
            let restored = Self::restore_from_snapshot(
                plugin,
                self.content_store.as_deref(),
                allocation,
                task,
                instance,
                &restore,
            );
            match restored {
                Ok(()) => {
                    self.checkpoints
                        .record_restore(allocation.id, true, String::new(), started)
                }
                Err(err) => {
                    let reason = format!("{err:#}");
                    self.checkpoints
                        .record_restore(allocation.id, false, reason, started);
                    return Err(err);
                }
            }
        } else {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Starting task allocation"
            );
            plugin.start(allocation, task, instance)?;
        }

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

    // ---------------------------------------------------------------------
    // Provisional allocations (docs/edge-boot.md)
    // ---------------------------------------------------------------------

    /// Configure the node's advisory capacity ledger from its own declared
    /// resources. Called once at worker startup, from the registration
    /// config the manager served.
    pub fn configure_ledger(&mut self, ledger: LocalLedger) {
        self.ledger = ledger;
    }

    /// Snapshot of the node's live claims, for `legion node claims` and the
    /// heartbeat report.
    pub fn claims_snapshot(&self) -> Vec<ClaimRecord> {
        self.claims.all()
    }

    /// The unratified claims, for `HeartbeatRequest.provisional_claims`.
    pub fn unratified_claims(&self) -> Vec<ClaimRecord> {
        self.claims.unratified()
    }

    /// Manager-facing allocation id for a worker-local key.
    ///
    /// The identity function for everything except a ratified provisional
    /// allocation, which keeps its negative local key (and therefore its
    /// directories, its open files, and its running process) and carries the
    /// manager's id alongside. Nothing is ever rekeyed and no directory ever
    /// moves — a ratification is a bookkeeping event, not a migration.
    fn external_id(&self, local_key: i64) -> Option<i64> {
        if local_key >= 0 {
            return Some(local_key);
        }
        self.claims
            .by_local_key(local_key)
            .and_then(|claim| claim.allocation_id)
    }

    /// Worker-local key for a manager-facing allocation id. The identity
    /// function unless the id names a ratified claim.
    /// Whether an allocation that just arrived on the work stream should be
    /// started at once (`worker/work_stream.rs`), the way a push starts one.
    /// No for anything this node already runs, launches, stops, or holds
    /// back, and for the committed form of an unratified provisional start:
    /// its ratification, not a second launch, finishes it. The heartbeat's
    /// reconcile settles every case this declines.
    pub fn wants_streamed_launch(&self, allocation: &TaskAllocation) -> bool {
        let local = self.local_key(allocation.allocation_id);
        !self.instances.contains_key(&local)
            && !self.launching.contains_key(&local)
            && !self.terminating.contains_key(&local)
            && !self.draining.contains_key(&local)
            && !self.cancelled_launches.contains(&local)
            && !self.pressure_evicted.contains(&local)
            && !self
                .claims
                .unratified_matches(&allocation.job_config_id, &allocation.task_name)
    }

    pub fn local_key(&self, allocation_id: i64) -> i64 {
        self.claims
            .by_allocation_id(allocation_id)
            .map(|claim| claim.local_key)
            .unwrap_or(allocation_id)
    }

    /// Admit and begin a provisional allocation.
    ///
    /// Admission is local and total: the ticket (if any) was already
    /// verified by the caller, this checks the claim table and the resource
    /// ledger, and *then* commits to running anything. A failure at this
    /// point has held nothing and started nothing.
    ///
    /// Returns the registered claim. The caller runs the work through
    /// [`launch_provisional_allocation`], which respects the gate.
    pub fn admit_claim(&mut self, record: ClaimRecord) -> R<ClaimRecord> {
        let claim_id = record.claim_id.clone();
        self.ledger.admit(&claim_id, &record.resources)?;
        match self.claims.insert(record) {
            Ok(stored) => Ok(stored),
            Err(err) => {
                // Nothing is left partially held: a claim the table refuses
                // releases its ledger reservation on the way out, or the node
                // would slowly leak capacity to starts that never happened.
                self.ledger.release_hold(&claim_id);
                Err(err)
            }
        }
    }

    /// Ratify a claim: the placement committed and the work is now an
    /// ordinary allocation under the manager's id.
    ///
    /// Opens the network for a claim held at gate `boot`, and starts a claim
    /// that was held at gate `stage` (which prepared everything and stopped
    /// short of `start`). Returns the claim as it now stands.
    pub fn ratify_claim(&mut self, claim_id: &str, final_payload: &TaskAllocation) -> R<Ratified> {
        let claim = self
            .claims
            .get(claim_id)
            .cloned()
            .with_context(|| format!("no claim {claim_id} on this node"))?;
        if claim.is_ratified() {
            // A duplicate verdict (the reply and the heartbeat catch-up both
            // landed) is a no-op, not an error — and must not start a second
            // health monitor for work that already has one.
            return Ok(Ratified {
                claim,
                start_health_monitor: None,
            });
        }
        let allocation_id = final_payload.allocation_id;

        let local_key = claim.local_key;
        // Adopt the committed payload before doing anything with it. The
        // payload the node booted (or staged) with could not name assigned
        // ports, the bound instance, or resolved volume mounts — those are
        // produced by the apply transaction — so this is where a provisional
        // start stops being provisional in every sense but its local key.
        let (mut allocation, task) = task_from_allocation(final_payload)?;
        allocation.id = local_key;
        self.volume_specs
            .insert(local_key, parse_volume_mounts(final_payload)?);
        self.allocations.insert(local_key, allocation.clone());
        self.task_configs.insert(local_key, task.clone());

        // A `stage`-gated claim starts its workload here, so its health
        // monitor starts here too — the launch path deliberately did not
        // spawn one for work that was not running.
        let mut start_health_monitor = None;
        match claim.gate {
            Gate::Stage => {
                // Everything expensive already happened; this is the start
                // the gate was holding back.
                self.start_registered_allocation(&allocation, &task, Gate::Off)?;
                start_health_monitor = Some((allocation.clone(), task.clone()));
            }
            Gate::Boot => {
                let plugin = self
                    .registry
                    .plugin(&task.runtime)
                    .context("Task plugin not loaded")?;
                let instance = self
                    .instances
                    .get_mut(&local_key)
                    .context("Task instance missing")?;
                plugin.set_network_gate(&allocation, &task, instance, true)?;
            }
            // `serve` never gated anything, and `off` never got here.
            Gate::Serve | Gate::Off => {}
        }

        self.ledger.release_hold(claim_id);
        self.ledger.commit(local_key, &claim.resources);
        let ratified = self
            .claims
            .ratify(claim_id, allocation_id)
            .context("claim vanished during ratification")?;
        // Persist now that the work has an id the cluster agrees on: a
        // restart from here must restore it, not orphan it.
        self.persist_state();
        info!(
            claim_id,
            allocation_id,
            gate = %claim.gate,
            "Ratified provisional allocation"
        );
        Ok(Ratified {
            claim: ratified,
            start_health_monitor,
        })
    }

    /// Kill a provisional allocation: refused, expired, or withdrawn.
    ///
    /// Every failure mode ends here — this is the single kill path the
    /// design's fail-closed claim rests on. `on_refused: 'stop'` runs the
    /// task's normal graceful-stop ladder; the default is a hard destroy,
    /// because the common refusal is "someone else owns this id" and a loser
    /// lingering is what the at-most-one invariant forbids.
    pub fn kill_claim(&mut self, claim_id: &str, reason: &str) -> Option<ClaimRecord> {
        let claim = self.claims.remove(claim_id)?;
        self.ledger.release_hold(claim_id);
        warn!(
            claim_id,
            local_key = claim.local_key,
            instance_id = %claim.instance_id,
            gate = %claim.gate,
            reason,
            "Killing provisional allocation"
        );
        let graceful = claim.on_refused == OnRefused::Stop;
        let stopped = if graceful {
            self.stop_allocation(claim.local_key)
        } else {
            self.stop_allocation_inner(claim.local_key, true)
        };
        if let Err(err) = stopped {
            warn!(claim_id, error = ?err, "Failed to stop provisional allocation");
        }
        // A provisional allocation the manager never knew about must not be
        // reported as a failed allocation — there is nothing to reschedule.
        self.failed
            .retain(|failure| failure.allocation_id != claim.local_key);
        self.terminated_reports
            .retain(|report| report.allocation_id != claim.local_key);
        self.allocation_health.remove(&claim.local_key);
        Some(claim)
    }

    /// Claims whose lease expired without a verdict. The backstop for every
    /// failure the reply path cannot cover: a partition, a dead manager, a
    /// lost reply. Returns what was killed, for events and metrics.
    pub fn sweep_expired_claims(&mut self, now: std::time::Instant) -> Vec<ClaimRecord> {
        let expired = self.claims.expired(now);
        let mut killed = Vec::with_capacity(expired.len());
        for claim in expired {
            if let Some(claim) = self.kill_claim(&claim.claim_id, "ratification lease expired") {
                killed.push(claim);
            }
        }
        killed
    }

    /// Record how far a claim has progressed, for the heartbeat report and
    /// the `StartClaim` stream.
    pub fn set_claim_stage(&mut self, claim_id: &str, stage: ClaimStage) {
        if let Some(claim) = self.claims.get_mut(claim_id) {
            claim.stage = stage;
        }
    }

    /// Whether this node already holds a live claim for an instance.
    pub fn instance_is_claimed(&self, instance_id: &str) -> bool {
        self.claims.instance_is_claimed(instance_id)
    }

    /// Entry point for a new allocation from the manager. Tasks without
    /// artifacts start synchronously; artifact-bearing tasks are registered
    /// as `staging artifacts` and return a [`StagingJob`] the caller runs on
    /// a blocking thread (never under this lock — fetches can take minutes
    /// and heartbeats must keep flowing).
    pub fn begin_task_allocation(&mut self, allocation: &TaskAllocation) -> R<BeginOutcome> {
        self.begin_task_allocation_gated(allocation, Gate::Off)
    }

    /// [`Self::begin_task_allocation`] with an explicit gate.
    ///
    /// `Gate::Off` is the ordinary path. `Gate::Stage` prepares everything
    /// and stops short of `start`; `Gate::Boot` starts with the runtime's
    /// network gate shut; `Gate::Serve` starts normally. The allocation id in
    /// `allocation` is the worker-local claim key for a provisional start.
    pub fn begin_task_allocation_gated(
        &mut self,
        allocation: &TaskAllocation,
        gate: Gate,
    ) -> R<BeginOutcome> {
        let (alloc, task) = task_from_allocation(allocation)?;
        // A duplicate dispatch (heartbeat and push racing) of an allocation
        // whose launch is still in its runtime call. Launching it again would
        // start it twice; tearing the first one down would waste the work.
        if self.launching.contains_key(&alloc.id) || self.cancelled_launches.contains(&alloc.id) {
            debug!(
                allocation_id = alloc.id,
                "Allocation launch already in flight; ignoring the duplicate dispatch"
            );
            return Ok(BeginOutcome::InFlight);
        }
        // The manager resolved these against the volume table, so they carry
        // concrete ids and the driver attrs from previous mounts. Recorded
        // before any start path so both the direct and post-staging routes
        // mount the same list.
        self.volume_specs
            .insert(alloc.id, parse_volume_mounts(allocation)?);
        // Start from a snapshot instead of booting (RestoreInstance). The
        // payload stays until the manager has the outcome.
        if let Some(restore) = allocation.restore.clone() {
            self.checkpoints.restores.insert(alloc.id, restore);
        }
        // A published snapshot restored on a node that does not hold its
        // parts fetches them first, on the staging thread.
        let restore_fetch = self.restore_parts_to_fetch(alloc.id);
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
        // Manager-dispatched work launches off the lock. Provisional starts
        // keep the locked path: their failures settle the claim (see
        // `launch_provisional_allocation`) rather than reaching the manager
        // as a failed allocation, which `record_failure` would do.
        // A restore keeps the locked path too: it runs inside the locked
        // start, which records its outcome on the checkpoint channel.
        let provisional = self.claims.by_local_key(alloc.id).is_some();
        let restoring = self.checkpoints.restores.contains_key(&alloc.id);
        if task.artifacts.is_empty() && gate == Gate::Off && !provisional && !restoring {
            self.registry
                .load_plugin(&task.runtime, true)
                .with_context(|| format!("Failed to load task plugin {}", task.runtime))?;
            info!(
                allocation_id = alloc.id,
                task_name = %task.name,
                task_runtime = %task.runtime,
                "Received task allocation"
            );
            let instance = self.prepare_instance(&alloc, &task)?;
            let launch = self.begin_unlocked_launch(&alloc, &task, instance)?;
            // Armed before the runtime call, like the staging path below:
            // the at-most-one guarantee must hold for the whole launch.
            if allocation.self_terminate_after_seconds > 0 {
                self.set_self_fence_window(
                    alloc.id,
                    std::time::Duration::from_secs(allocation.self_terminate_after_seconds),
                );
            }
            return Ok(BeginOutcome::Launching(Box::new(launch)));
        }
        if task.artifacts.is_empty() && restore_fetch.is_none() {
            self.run_allocation_gated(&alloc, &task, gate)?;
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
            if gate == Gate::Stage {
                // Nothing is running yet, so there is nothing to health
                // check. The monitor starts with the workload, at ratify.
                return Ok(BeginOutcome::Staged(Box::new((alloc, task))));
            }
            return Ok(BeginOutcome::Started(Box::new((alloc, task))));
        }

        info!(
            allocation_id = alloc.id,
            task_name = %task.name,
            task_runtime = %task.runtime,
            artifacts = task.artifacts.len(),
            restore_fetch = restore_fetch.is_some(),
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
        if !task.artifacts.is_empty() && !plugin.supports_artifacts() {
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
        self.set_allocation_health(
            alloc.id,
            false,
            if task.artifacts.is_empty() {
                "fetching snapshot"
            } else {
                "staging artifacts"
            },
        );
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
            gate,
            stager: self.stager.clone(),
            board: self.artifact_status.clone(),
            restore_fetch,
        })))
    }

    /// The manifest of the snapshot `allocation_id` restores from, when its
    /// memory parts must go through the fetch step: some are missing from
    /// this node's content store, or the snapshot is published. A published
    /// snapshot's parts are unpinned everywhere, the capturing node
    /// included, so even when they are all here the fetch step pins them to
    /// the allocation until it has started, and refetches any evicted in
    /// between. `None` for every allocation that is not restoring, and for a
    /// local snapshot whose parts are all here (pinned to the snapshot). An
    /// unreadable manifest is left for the restore itself to report.
    fn restore_parts_to_fetch(
        &self,
        allocation_id: i64,
    ) -> Option<(
        crate::structs::checkpoint::SnapshotManifest,
        crate::checkpoint_seal::PartKeys,
    )> {
        let restore = self.checkpoints.restores.get(&allocation_id)?;
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(&restore.manifest_json).ok()?;
        let published = manifest.memory.iter().any(|part| part.url.is_some());
        let store = self.content_store.as_deref();
        let missing = manifest.memory.iter().any(|part| {
            let Ok(digest) = crate::content_store::ContentDigest::parse(&part.digest) else {
                return false;
            };
            !store.is_some_and(|store| store.contains(&digest).unwrap_or(false))
        });
        (published || missing).then(|| {
            let keys = crate::checkpoint_seal::PartKeys::from_proto(
                &restore.part_keys,
                &restore.part_key_error,
            );
            (manifest, keys)
        })
    }

    /// Drop the pins a cross-node restore held on its fetched parts while
    /// the allocation started. The parts stay in the store as ordinary
    /// cache entries (and in the location index), until LRU evicts them.
    pub(crate) fn release_restore_fetch_pins(&self, allocation_id: i64) {
        if let Some(store) = self.content_store.as_deref() {
            if let Err(err) =
                store.release_owner(&crate::content_store::PinOwner::allocation(allocation_id))
            {
                warn!(allocation_id, error = ?err, "Failed releasing restore fetch pins");
            }
        }
    }

    /// Completes an allocation whose artifacts finished staging: interpolates
    /// `${artifact.<name>}` into the runtime config, exposes
    /// `LEGION_ARTIFACT_<NAME>` env vars, and runs consume/prepare/start.
    pub fn start_staged_allocation(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        staged: Vec<StagedArtifact>,
        gate: Gate,
    ) -> R<()> {
        let started_task = self.apply_staged_artifacts(allocation, task, staged)?;
        if gate == Gate::Stage {
            // The whole point of the `stage` gate: everything expensive is
            // done and nothing is executing. `ratify_claim` runs the start.
            self.prepare_registered_allocation(allocation, &started_task)?;
            self.set_allocation_health(allocation.id, false, "staged (awaiting ratification)");
            return Ok(());
        }
        self.prepare_registered_allocation(allocation, &started_task)?;
        self.start_registered_allocation(allocation, &started_task, gate)
    }

    /// [`Self::start_staged_allocation`] for the ordinary gate, with the
    /// runtime's `prepare`/`start` left to [`complete_unlocked_launch`] so
    /// they run off this lock.
    pub fn begin_staged_launch(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        staged: Vec<StagedArtifact>,
    ) -> R<UnlockedLaunch> {
        let started_task = self.apply_staged_artifacts(allocation, task, staged)?;
        let instance = self
            .instances
            .remove(&allocation.id)
            .context("Task instance missing for staged allocation")?;
        self.begin_unlocked_launch(allocation, &started_task, instance)
    }

    /// Hand staged artifacts to the instance: `LEGION_ARTIFACT_<NAME>` env and
    /// `${artifact.<name>}` interpolation. Returns the task config to start.
    fn apply_staged_artifacts(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        staged: Vec<StagedArtifact>,
    ) -> R<TaskConfig> {
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
        Ok(started_task)
    }

    /// The locked first half of an [`UnlockedLaunch`]: place the task's
    /// CPU/memory, then register the allocation (a pid-less placeholder
    /// instance) so nothing launches it again while its runtime works.
    fn begin_unlocked_launch(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        mut instance: TaskInstance,
    ) -> R<UnlockedLaunch> {
        let plugin = self
            .registry
            .plugin_handle(&task.runtime)
            .context("Task plugin not loaded")?;
        // Resource placement stays under the lock: the allocator and slice
        // masks are this struct's, and it is quick (a cgroup mkdir and a few
        // small writes). A task must never observe its limits arriving after
        // it started, so it happens before `prepare`.
        let runtime = plugin.task_plugin();
        instance.resource_assignment = self
            .resources
            .assign(
                allocation.id,
                task,
                runtime.resource_enforcement(),
                runtime.enforced_memory_limit_bytes(task),
            )
            .with_context(|| {
                format!(
                    "failed to place task {} within this node's resource limits",
                    task.name
                )
            })?;
        instance.log_dir = Some(self.state_store.allocation_log_dir(allocation.id));
        // Launch ready for memory checkpoints when the task asks for them
        // and the runtime can take them here (see
        // `prepare_registered_allocation`).
        instance.checkpoint_launch = crate::structs::snapshot::memory_checkpoint_ask(task)
            .is_some()
            && runtime.checkpoint_support(&crate::task_plugins::CheckpointQuery::new(&task.config))
                == crate::task_plugins::CheckpointSupport::Memory;

        self.next_launch_generation += 1;
        let generation = self.next_launch_generation;
        self.launching.insert(allocation.id, generation);
        self.instances.insert(
            allocation.id,
            TaskInstance {
                alloc_dir: instance.alloc_dir.clone(),
                task_dir: instance.task_dir.clone(),
                extra_env: instance.extra_env.clone(),
                log_dir: instance.log_dir.clone(),
                resource_assignment: instance.resource_assignment.clone(),
                ..TaskInstance::default()
            },
        );
        self.allocations.insert(allocation.id, allocation.clone());
        self.task_configs.insert(allocation.id, task.clone());
        self.set_allocation_health(allocation.id, false, "starting");

        Ok(UnlockedLaunch {
            generation,
            allocation: allocation.clone(),
            task: task.clone(),
            instance,
            plugin,
            volumes: self.volumes.clone(),
            volume_specs: self
                .volume_specs
                .get(&allocation.id)
                .cloned()
                .unwrap_or_default(),
        })
    }

    /// The locked second half: install the started instance, or record why
    /// it failed, or hand a launch that was cancelled meanwhile back for
    /// teardown.
    fn finish_unlocked_launch(
        &mut self,
        launch: UnlockedLaunch,
        result: R<()>,
        failure_prefix: &str,
    ) -> LaunchFinish {
        let allocation_id = launch.allocation.id;
        if self.launching.get(&allocation_id) != Some(&launch.generation) {
            return LaunchFinish::Cancelled(Box::new(launch));
        }
        self.launching.remove(&allocation_id);
        let UnlockedLaunch {
            allocation,
            task,
            instance,
            plugin,
            ..
        } = launch;
        // Replaces the placeholder. On failure too: `record_failure` tears
        // down through the real instance (its mounts, cores, any process).
        self.instances.insert(allocation_id, instance);
        if let Err(err) = result {
            let reason = format!("{failure_prefix}{err:#}");
            self.record_failure(allocation_id, reason.clone());
            return LaunchFinish::Failed(reason);
        }
        self.set_allocation_health(allocation_id, false, "starting");
        self.notify_log_sinks_started(&allocation, &task);
        self.persist_state();
        // See `start_registered_allocation`: a batch task on a runtime that
        // tracks no process reports its completion at start.
        if !plugin.task_plugin().supervises_process()
            && matches!(task.lifecycle, crate::structs::TaskLifecycle::Batch)
        {
            self.record_unsupervised_completion(allocation_id, &task.runtime);
        }
        LaunchFinish::Started(Box::new((allocation, task)))
    }

    /// Finish tearing down a cancelled launch, after its runtime instance
    /// was destroyed without the lock held.
    fn finish_cancelled_launch(
        &mut self,
        allocation_id: i64,
        assignment: Option<&ResourceAssignment>,
    ) {
        self.cancelled_launches.remove(&allocation_id);
        self.finish_teardown(allocation_id, true, assignment);
    }

    /// Roll the allocation's instance volumes back to the snapshot it
    /// restores from, with the driver's `restore` op, before they mount.
    /// A no-op for every allocation that is not restoring.
    fn restore_allocation_volumes(&mut self, allocation: &Allocation) -> R<()> {
        let Some(restore) = self.checkpoints.restores.get(&allocation.id).cloned() else {
            return Ok(());
        };
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(&restore.manifest_json)
                .with_context(|| {
                    format!(
                        "snapshot {} has an unreadable manifest",
                        restore.snapshot_id
                    )
                })?;
        let specs = self
            .volume_specs
            .get(&allocation.id)
            .cloned()
            .unwrap_or_default();
        if restore.fork {
            return self.clone_allocation_volumes(allocation, &restore, &manifest, &specs);
        }
        for snapshot in &manifest.volumes {
            let mount = specs
                .iter()
                .find(|mount| mount.volume_id == snapshot.volume_id)
                .with_context(|| {
                    format!(
                        "snapshot {} holds volume {}, which this allocation does not mount; the \
                         instance's storage changed since the snapshot was taken",
                        restore.snapshot_id, snapshot.volume_id
                    )
                })?;
            self.volumes.restore_volume(allocation, mount, snapshot)?;
        }
        Ok(())
    }

    /// Phase 6: give an allocation restoring into a NEW instance (a fork, or
    /// a create from a snapshot) its own copy of each snapshotted instance
    /// volume, with the driver's `clone` op. The snapshot holds the origin's
    /// volumes in mount order, which is also this allocation's order: the
    /// two run the same task.
    fn clone_allocation_volumes(
        &self,
        allocation: &Allocation,
        restore: &crate::structs::SnapshotRestore,
        manifest: &crate::structs::checkpoint::SnapshotManifest,
        specs: &[crate::structs::VolumeMount],
    ) -> R<()> {
        let instance_volumes: Vec<&crate::structs::VolumeMount> =
            specs.iter().filter(|mount| mount.instance_volume).collect();
        if instance_volumes.len() != manifest.volumes.len() {
            bail!(
                "snapshot {} holds {} instance volume(s), but this allocation mounts {}; the \
                 task's storage changed since the snapshot was taken",
                restore.snapshot_id,
                manifest.volumes.len(),
                instance_volumes.len()
            );
        }
        for (mount, snapshot) in instance_volumes.into_iter().zip(&manifest.volumes) {
            self.volumes.clone_volume(allocation, mount, snapshot)?;
        }
        Ok(())
    }

    /// Start a prepared instance from a snapshot instead of booting it.
    fn restore_from_snapshot(
        plugin: &dyn crate::task_plugins::TaskPlugin,
        content: Option<&crate::content_store::ContentStore>,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        restore: &crate::structs::SnapshotRestore,
    ) -> R<()> {
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(&restore.manifest_json)
                .with_context(|| {
                    format!(
                        "snapshot {} has an unreadable manifest",
                        restore.snapshot_id
                    )
                })?;
        let alloc_dir = instance
            .alloc_dir
            .clone()
            .context("allocation directory missing")?;
        let source = crate::worker::checkpoints::materialize_parts(
            content,
            &manifest,
            alloc_dir.join(".restore"),
        )?;
        let identity = crate::worker::checkpoints::resume_identity(allocation, task, restore);
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            snapshot_id = %restore.snapshot_id,
            kind = manifest.kind.as_str(),
            "Restoring task allocation from snapshot"
        );
        let result = plugin.restore(allocation, task, instance, &manifest, &source, &identity);
        // The runtime holds what it needs (hardlinks, open files) by now.
        let _ = std::fs::remove_dir_all(source.path());
        result
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
    fn mount_allocation_volumes(&mut self, allocation: &Allocation, task: &TaskConfig) -> R<()> {
        let Some(specs) = self.volume_specs.get(&allocation.id).cloned() else {
            return Ok(());
        };
        if specs.is_empty() {
            return Ok(());
        }
        // Re-mounting an allocation we already hold mounts for (an in-place
        // re-dispatch) would leak the old ones.
        self.volumes.unmount_for_allocation(allocation.id);

        // The task is passed in rather than read from `task_configs`: on a
        // first launch it is not registered there yet.
        let instance = crate::worker::volumes::instance_ref_from_task(allocation, task);
        let mounted = self
            .volumes
            .mount_for_allocation(allocation, instance.as_ref(), &specs)?;
        if let Some(instance) = self.instances.get_mut(&allocation.id) {
            apply_mounted_volumes(instance, task, &mounted);
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
        // Addressed by the manager's id; a ratified provisional allocation
        // still writes under the local key it booted with.
        let log_dir = self
            .state_store
            .allocation_log_dir(self.local_key(allocation_id));
        read_allocation_logs(&log_dir, stream, offset, max_bytes, tail)
    }

    pub fn register_health_monitor(&mut self, allocation_id: i64, handle: HealthMonitorHandle) {
        self.health_monitors.insert(allocation_id, handle);
    }

    pub fn restore_allocations(&mut self) -> R<Vec<(Allocation, TaskConfig)>> {
        let persisted = self.state_store.load()?;
        let mut active = Vec::new();
        let mut restored = Vec::new();
        let mut restored_claims: Vec<(i64, i64, TaskConfig)> = Vec::new();
        let mut resumed_stops = Vec::new();

        for mut record in persisted {
            let Some(pid) = record.pid else {
                warn!(
                    allocation_id = record.allocation.id,
                    task_name = %record.task.name,
                    "Skipping persisted allocation without pid"
                );
                continue;
            };
            let started = match persisted_process_identity(&record, pid) {
                PersistedProcess::Live { started } => started,
                PersistedProcess::Dead => {
                    warn!(
                        allocation_id = record.allocation.id,
                        task_name = %record.task.name,
                        pid,
                        "Skipping persisted allocation with dead pid"
                    );
                    continue;
                }
                PersistedProcess::Recycled => {
                    warn!(
                        allocation_id = record.allocation.id,
                        task_name = %record.task.name,
                        pid,
                        "Skipping persisted allocation whose pid now belongs to another process"
                    );
                    continue;
                }
            };
            // Backfills records written before start times were persisted.
            record.pid_start_time = Some(started);

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
            if let Some(stop) = record.stopping.clone() {
                // Mid-stop when the old worker exited: keep it out of the
                // live set (the manager no longer wants it) and finish the
                // stop. Its volumes are re-recorded as in use first, so
                // nothing reclaims storage under a task that is still alive.
                self.volume_specs
                    .insert(record.allocation.id, record.volumes.clone());
                let volume_instance = crate::worker::volumes::instance_ref_from_task(
                    &record.allocation,
                    &record.task,
                );
                if let Err(err) = self.volumes.mount_for_allocation(
                    &record.allocation,
                    volume_instance.as_ref(),
                    &record.volumes,
                ) {
                    warn!(
                        allocation_id = record.allocation.id,
                        error = ?err,
                        "Failed to re-establish volume mounts for a stopping allocation"
                    );
                }
                info!(
                    allocation_id = record.allocation.id,
                    task_name = %record.task.name,
                    cause = %stop.cause,
                    "Resuming the stop of an allocation the previous worker left mid-stop"
                );
                active.push(record.clone());
                resumed_stops.push((record.allocation, record.task, instance, stop));
                continue;
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
            if let Err(err) = self.mount_allocation_volumes(&record.allocation, &record.task) {
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
            // A ratified provisional allocation is restored under the
            // negative local key it booted with, so its directories, its open
            // files and its running process all still line up. The alias to
            // the manager's id is rebuilt here; without it the next heartbeat
            // would report nothing for an allocation the cluster believes is
            // running, and the manager would replace a live task.
            if let Some(external_id) = record.external_id {
                restored_claims.push((record.allocation.id, external_id, record.task.clone()));
            }
            active.push(record.clone());
            restored.push((record.allocation, record.task));
        }

        // Seed the key counter below every restored key before rebuilding the
        // aliases, so a claim admitted after the restart cannot be handed a
        // key a running allocation is still using.
        self.claims
            .reserve_below(restored_claims.iter().map(|(local, _, _)| *local));
        for (local_key, external_id, task) in restored_claims {
            let record = ClaimRecord {
                claim_id: format!("clm_restored_{}", external_id),
                local_key,
                gate: Gate::Off,
                stage: ClaimStage::Serving,
                lease_deadline: std::time::Instant::now(),
                lease_deadline_unix: 0,
                job_id: String::new(),
                job_config_id: String::new(),
                task_name: task.name.clone(),
                instance_id: String::new(),
                resources: task.resources.clone(),
                origin: "restored".into(),
                on_refused: OnRefused::Kill,
                ticket_jti: String::new(),
                allocation_id: Some(external_id),
            };
            if let Err(err) = self.claims.insert(record) {
                warn!(external_id, error = ?err, "Failed to restore provisional allocation alias");
            }
        }

        // One mask write for the whole restored set, after every
        // reservation is back in the allocator's view.
        self.resources.resync_masks();
        self.persist_state_with(&active);

        // After the whole set is persisted, because these persist again as
        // they move along the ladder.
        for (allocation, task, instance, stop) in resumed_stops {
            let allocation_id = allocation.id;
            match stop.kill_deadline_unix {
                // Already signalled: wait out what is left of the window.
                Some(deadline_unix) => {
                    self.terminating.insert(
                        allocation_id,
                        TerminatingAllocation {
                            allocation,
                            task,
                            instance,
                            cause: stop.cause,
                            deadline: instant_from_unix(deadline_unix),
                            remove_dirs: stop.remove_dirs,
                        },
                    );
                }
                // Still draining: the drain bought consumers time to notice
                // the endpoint was gone, and the restart already spent it.
                None => self.signal_and_terminate(
                    allocation_id,
                    allocation,
                    task,
                    instance,
                    stop.cause,
                    stop.remove_dirs,
                ),
            }
        }

        Ok(restored)
    }

    fn persist_state(&self) {
        let mut records = Vec::new();
        for (allocation_id, allocation) in &self.allocations {
            // Unratified provisional work is deliberately never persisted: a
            // worker restart kills it (edge boot plan §6.6), so writing it
            // down would only create something to clean up. Ratified claims
            // carry their manager-minted id in `external_id` so a restart
            // restores the alias along with the allocation.
            if self
                .claims
                .by_local_key(*allocation_id)
                .is_some_and(|claim| !claim.is_ratified())
            {
                continue;
            }
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
                if self.artifact_status.snapshot(*allocation_id).is_none()
                    && !self.launching.contains_key(allocation_id)
                {
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
                pid_start_time: pid.and_then(process_start_time),
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
                external_id: self
                    .claims
                    .by_local_key(*allocation_id)
                    .and_then(|claim| claim.allocation_id),
                stopping: None,
            });
        }

        // Stopping work is persisted too: a task inside its drain or grace
        // window is still alive and may still hold a volume, and a restarted
        // worker must finish stopping it rather than forget it.
        let stopping = self
            .draining
            .iter()
            .map(|(id, entry)| {
                let stop = PersistedStop {
                    cause: entry.cause,
                    remove_dirs: entry.remove_dirs,
                    kill_deadline_unix: None,
                };
                (*id, &entry.allocation, &entry.task, &entry.instance, stop)
            })
            .chain(self.terminating.iter().map(|(id, entry)| {
                let stop = PersistedStop {
                    cause: entry.cause,
                    remove_dirs: entry.remove_dirs,
                    kill_deadline_unix: Some(unix_from_instant(entry.deadline)),
                };
                (*id, &entry.allocation, &entry.task, &entry.instance, stop)
            }));
        for (allocation_id, allocation, task, instance, stop) in stopping {
            // No pid: the process is already gone (or never existed), so
            // there is nothing for a restarted worker to finish.
            let Some(pid) = instance.pid else {
                continue;
            };
            records.push(PersistedAllocation {
                allocation: allocation.clone(),
                task: task.clone(),
                pid: Some(pid),
                pid_start_time: process_start_time(pid),
                resource_assignment: instance.resource_assignment.clone(),
                self_terminate_after_secs: None,
                volumes: self
                    .volume_specs
                    .get(&allocation_id)
                    .cloned()
                    .unwrap_or_default(),
                external_id: None,
                stopping: Some(stop),
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

/// Expose mounted volumes to a task instance.
///
/// Runtimes with an isolated filesystem view get volume env values that are
/// in-task destinations (bind target / guest mount path), because the host
/// path is unreachable from inside. Host-process runtimes (exec) get the real
/// host path.
fn apply_mounted_volumes(
    instance: &mut TaskInstance,
    task: &TaskConfig,
    mounted: &[crate::worker::volumes::MountedVolume],
) {
    let in_task_view = matches!(task.runtime.as_str(), "container" | "sandbox");
    for (key, value) in volume_env(mounted, in_task_view) {
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

/// A monotonic deadline as wall-clock Unix seconds, rounded up so a
/// restarted worker never kills before the real deadline.
fn unix_from_instant(deadline: std::time::Instant) -> u64 {
    let at = std::time::SystemTime::now()
        + deadline.saturating_duration_since(std::time::Instant::now());
    let since_epoch = at.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    since_epoch.as_secs() + u64::from(since_epoch.subsec_nanos() > 0)
}

/// The inverse of [`unix_from_instant`]. A deadline already in the past
/// (or a clock stepped forward past it) is "now".
fn instant_from_unix(deadline_unix: u64) -> std::time::Instant {
    let remaining = (std::time::UNIX_EPOCH + std::time::Duration::from_secs(deadline_unix))
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    std::time::Instant::now() + remaining
}

fn load_builtin_plugins_into(registry: &mut TaskPluginRegistry) {
    if let Err(err) = registry.load_plugin("exec", true) {
        error!(?err, "Failed to load exec task plugin");
    }
    if let Err(err) = registry.load_plugin("container", true) {
        error!(?err, "Failed to load container task plugin");
    }
    if let Err(err) = registry.load_plugin("sandbox", true) {
        error!(?err, "Failed to load sandbox task plugin");
    }
    if let Err(err) = registry.load_plugin("noop", true) {
        debug!(?err, "Noop task plugin not available");
    }
}

/// What a persisted pid names today.
enum PersistedProcess {
    /// The task's own process, still running; `started` is its start time.
    Live { started: u64 },
    /// No process with that pid is running.
    Dead,
    /// The pid is live but started at a different time than the record
    /// says: it was recycled and belongs to something else now.
    Recycled,
}

fn persisted_process_identity(record: &PersistedAllocation, pid: u32) -> PersistedProcess {
    let Some(started) = process_start_time(pid) else {
        return PersistedProcess::Dead;
    };
    // A live pid is not proof it is still our task: after a host reboot (or
    // enough process churn) the number belongs to some unrelated process,
    // which we would report as running and later kill on stop. The start
    // time pins the identity. Records without one predate it and are taken
    // on liveness alone.
    if record
        .pid_start_time
        .is_some_and(|recorded| recorded != started)
    {
        return PersistedProcess::Recycled;
    }
    PersistedProcess::Live { started }
}

/// Self-fence persisted stateful allocations whose `selfTerminateAfter`
/// window has run out, for a worker process that has not reached its manager
/// yet. Returns the fenced allocation ids.
///
/// Tasks outlive the worker (`setsid`), but the in-process watchdog only
/// starts after registration. A worker restarted during a partition would
/// otherwise wait in its connect loop, or crash-loop on a failed
/// pre-registration, while its restored stateful tasks ran unfenced
/// indefinitely and the manager's `replaceAfter` kept counting.
/// `since_last_contact` comes from the persisted wall-clock contact record
/// ([`WorkerStateStore::since_last_manager_contact`]).
///
/// The kill uses the runtime's own `destroy`, exactly as the watchdog would
/// for a restored allocation. The persisted record is left alone: its pid is
/// dead now, so the restore that follows registration skips it, and the
/// manager's desired set decides whether it runs again.
pub fn fence_persisted_allocations(
    state_store: &WorkerStateStore,
    since_last_contact: std::time::Duration,
) -> R<Vec<i64>> {
    let expired = state_store
        .load()?
        .into_iter()
        .filter(|record| {
            record
                .self_terminate_after_secs
                .is_some_and(|window| since_last_contact > std::time::Duration::from_secs(window))
        })
        .filter_map(|record| {
            let pid = record.pid?;
            matches!(
                persisted_process_identity(&record, pid),
                PersistedProcess::Live { .. }
            )
            .then_some((record, pid))
        })
        .collect::<Vec<_>>();
    if expired.is_empty() {
        return Ok(Vec::new());
    }

    let mut registry = TaskPluginRegistry::new();
    load_builtin_plugins_into(&mut registry);
    let mut fenced = Vec::with_capacity(expired.len());
    for (record, pid) in expired {
        warn!(
            allocation_id = record.allocation.id,
            task_name = %record.task.name,
            lost_contact_secs = since_last_contact.as_secs(),
            "Self-fencing a restored allocation before reaching the manager"
        );
        let mut instance = TaskInstance {
            pid: Some(pid),
            ..TaskInstance::default()
        };
        let destroyed = match registry.plugin(&record.task.runtime) {
            Some(plugin) => plugin.destroy(&record.allocation, &record.task, &mut instance),
            None => Err(anyhow!("task plugin {} not loaded", record.task.runtime)),
        };
        if let Err(err) = destroyed {
            warn!(
                allocation_id = record.allocation.id,
                error = ?err,
                "Failed to self-fence a restored allocation"
            );
            continue;
        }
        fenced.push(record.allocation.id);
    }
    Ok(fenced)
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
        BeginOutcome::Launching(launch) => {
            if let Some((alloc, task)) = complete_unlocked_launch(manager, *launch, "")? {
                spawn_health_monitor(manager.clone(), alloc, task);
            }
            Ok(())
        }
        BeginOutcome::Staged(_) | BeginOutcome::InFlight => Ok(()),
        BeginOutcome::Staging(job) => {
            spawn_staging_thread(manager.clone(), *job);
            Ok(())
        }
    }
}

/// Run an [`UnlockedLaunch`]'s runtime calls with no lock held, then settle
/// it under the lock. Returns the started allocation (for its health
/// monitor), `None` when it was cancelled meanwhile, or the recorded failure.
///
/// Must run on a blocking thread: `prepare`/`start` shell out and may pull
/// images.
pub fn complete_unlocked_launch(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    mut launch: UnlockedLaunch,
    failure_prefix: &str,
) -> R<Option<(Allocation, TaskConfig)>> {
    let result = launch.run();
    let finish = manager
        .lock()
        .expect("task manager")
        .finish_unlocked_launch(launch, result, failure_prefix);
    match finish {
        LaunchFinish::Started(started) => Ok(Some(*started)),
        LaunchFinish::Failed(reason) => Err(anyhow!(reason)),
        LaunchFinish::Cancelled(mut launch) => {
            let allocation_id = launch.allocation.id;
            info!(
                allocation_id,
                task_name = %launch.task.name,
                "Tearing down a launch that was cancelled while it started"
            );
            // Destroy off the lock (it shells out), then release storage and
            // cores under it, in that order: never unmount under a live task.
            if let Err(err) = launch.plugin.task_plugin().destroy(
                &launch.allocation,
                &launch.task,
                &mut launch.instance,
            ) {
                warn!(
                    allocation_id,
                    error = ?err,
                    "Failed to destroy a cancelled launch"
                );
            }
            manager
                .lock()
                .expect("task manager")
                .finish_cancelled_launch(
                    allocation_id,
                    launch.instance.resource_assignment.as_ref(),
                );
            Ok(None)
        }
    }
}

/// Launch a provisional allocation, respecting its gate.
///
/// The mirror of [`launch_task_allocation`] for work the store has not
/// recorded yet: the payload's allocation id is the claim's worker-local key,
/// and a failure is reported through the claim's kill path rather than as a
/// failed allocation the manager would try to reschedule.
pub fn launch_provisional_allocation(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    allocation: &TaskAllocation,
    claim_id: &str,
    gate: Gate,
) -> R<()> {
    let outcome = {
        let mut guard = manager.lock().expect("task manager");
        match guard.begin_task_allocation_gated(allocation, gate) {
            Ok(outcome) => outcome,
            Err(err) => {
                guard.kill_claim(claim_id, &format!("provisional start failed: {err:#}"));
                return Err(err);
            }
        }
    };

    match outcome {
        BeginOutcome::Started(started) => {
            let (alloc, task) = *started;
            manager
                .lock()
                .expect("task manager")
                .set_claim_stage(claim_id, stage_for(gate));
            spawn_health_monitor(manager.clone(), alloc, task);
            Ok(())
        }
        BeginOutcome::Staged(_) => {
            manager
                .lock()
                .expect("task manager")
                .set_claim_stage(claim_id, ClaimStage::Staged);
            Ok(())
        }
        // Neither is produced for a claim: its local key is fresh per
        // admission (no duplicate), and claims keep the locked launch path.
        BeginOutcome::InFlight | BeginOutcome::Launching(_) => {
            bail!("provisional start {claim_id} took the unlocked launch path")
        }
        BeginOutcome::Staging(job) => {
            spawn_provisional_staging_thread(manager.clone(), *job, claim_id.to_string());
            Ok(())
        }
    }
}

/// The stage a claim has reached once its launch returns, given its gate.
fn stage_for(gate: Gate) -> ClaimStage {
    match gate {
        Gate::Off | Gate::Stage => ClaimStage::Staged,
        Gate::Boot => ClaimStage::Booted,
        Gate::Serve => ClaimStage::Serving,
    }
}

/// Stage a provisional allocation's artifacts, then take it to its gate.
///
/// Separate from [`spawn_staging_thread`] only in its failure handling: a
/// provisional start that fails is killed as a claim (releasing its ledger
/// hold and its instance slot), never reported as a failed allocation — the
/// manager has no allocation to reschedule.
fn spawn_provisional_staging_thread(
    manager: Arc<Mutex<WorkerTaskManager>>,
    job: StagingJob,
    claim_id: String,
) {
    let allocation_id = job.allocation.id;
    let gate = job.gate;
    let manager_for_thread = manager.clone();
    let claim_for_thread = claim_id.clone();
    let spawned = std::thread::Builder::new()
        .name(format!("provisional-staging-{allocation_id}"))
        .spawn(move || {
            let manager = manager_for_thread;
            let result = job.stage();

            let started = {
                let mut guard = manager.lock().expect("task manager");
                if !guard.is_allocation_active(allocation_id) {
                    debug!(allocation_id, "Claim removed while staging artifacts");
                    if job.restore_fetch.is_some() {
                        guard.release_restore_fetch_pins(allocation_id);
                    }
                    return;
                }
                let claim_id = claim_for_thread.as_str();
                let started = match result.and_then(|staged| {
                    guard.start_staged_allocation(&job.allocation, &job.task, staged, gate)
                }) {
                    Ok(()) => {
                        guard.set_claim_stage(claim_id, stage_for(gate));
                        true
                    }
                    Err(err) => {
                        guard.kill_claim(claim_id, &format!("provisional staging failed: {err:#}"));
                        false
                    }
                };
                if job.restore_fetch.is_some() {
                    guard.release_restore_fetch_pins(allocation_id);
                }
                started
            };
            if started && gate != Gate::Stage {
                spawn_health_monitor(manager, job.allocation, job.task);
            }
        });
    if let Err(err) = spawned {
        error!(allocation_id, error = ?err, "Failed to spawn provisional staging thread");
        manager
            .lock()
            .expect("task manager")
            .kill_claim(&claim_id, &format!("spawn staging thread: {err}"));
    }
}

fn spawn_staging_thread(manager: Arc<Mutex<WorkerTaskManager>>, job: StagingJob) {
    let allocation_id = job.allocation.id;
    let manager_for_thread = manager.clone();
    let spawned = std::thread::Builder::new()
        .name(format!("artifact-staging-{allocation_id}"))
        .spawn(move || {
            let manager = manager_for_thread;
            let result = job.stage();

            const START_FAILED: &str = "start after artifact staging failed: ";
            let launch = {
                let mut guard = manager.lock().expect("task manager");
                if !guard.is_allocation_active(allocation_id) {
                    if job.restore_fetch.is_some() {
                        guard.release_restore_fetch_pins(allocation_id);
                    }
                    // Stopped while staging; stop_allocation already cleaned
                    // the directories and status rows.
                    debug!(allocation_id, "Allocation removed while staging artifacts");
                    return;
                }
                let staged = match result {
                    Ok(staged) => staged,
                    Err(err) => {
                        // A restore that could not fetch its parts reports
                        // the failure on the checkpoint channel too, so the
                        // resume follows `onRestoreFailure`.
                        if job.restore_fetch.is_some()
                            && guard.checkpoints.has_pending_restore(allocation_id)
                        {
                            guard.checkpoints.record_restore(
                                allocation_id,
                                false,
                                format!("{err:#}"),
                                std::time::Instant::now(),
                            );
                        }
                        guard.record_failure(
                            allocation_id,
                            format!("artifact staging failed: {err:#}"),
                        );
                        if job.restore_fetch.is_some() {
                            guard.release_restore_fetch_pins(allocation_id);
                        }
                        return;
                    }
                };
                // Only the ordinary gate reaches this thread (provisional
                // starts stage on their own), but a gated job keeps the
                // locked path rather than silently skipping its gate. So
                // does a restore: it runs inside the locked start, which
                // records its outcome on the checkpoint channel.
                let restoring = guard.checkpoints.restores.contains_key(&allocation_id);
                if job.gate != Gate::Off || restoring {
                    let started =
                        guard.start_staged_allocation(&job.allocation, &job.task, staged, job.gate);
                    // The restore (if any) ran inside the start above, so
                    // the fetched parts no longer need their pins.
                    if job.restore_fetch.is_some() {
                        guard.release_restore_fetch_pins(allocation_id);
                    }
                    if let Err(err) = started {
                        guard.record_failure(allocation_id, format!("{START_FAILED}{err:#}"));
                        return;
                    }
                    drop(guard);
                    // A staged-but-not-started allocation has nothing to
                    // health check yet; the monitor starts when ratification
                    // does.
                    if job.gate != Gate::Stage {
                        spawn_health_monitor(manager, job.allocation, job.task);
                    }
                    return;
                }
                match guard.begin_staged_launch(&job.allocation, &job.task, staged) {
                    Ok(launch) => launch,
                    Err(err) => {
                        guard.record_failure(allocation_id, format!("{START_FAILED}{err:#}"));
                        return;
                    }
                }
            };
            // The runtime calls run here, with the lock released.
            if let Ok(Some((allocation, task))) =
                complete_unlocked_launch(&manager, launch, START_FAILED)
            {
                spawn_health_monitor(manager, allocation, task);
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

/// Parse a dispatch payload into its allocation and task config. Public so
/// the edge-boot spec cache can record what a node has been asked to run
/// without duplicating the parsing rules.
pub fn task_and_allocation(allocation: &TaskAllocation) -> R<(Allocation, TaskConfig)> {
    task_from_allocation(allocation)
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

    // Suspend and snapshots decide how a sandbox launches (ready for memory
    // checkpoints or not), the idle policy the usage reports carry, and
    // whether the pressure response may suspend the instance.
    let suspend = if allocation.task_suspend.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(&allocation.task_suspend).context("Invalid task suspend JSON")?)
    };
    let snapshots = if allocation.task_snapshots.trim().is_empty() {
        None
    } else {
        Some(
            serde_json::from_str(&allocation.task_snapshots)
                .context("Invalid task snapshots JSON")?,
        )
    };
    // The instance this allocation serves, from the env the manager
    // injected at dispatch: the pressure response only suspends instances,
    // and a sandbox's identity disk names its instance.
    let instance_id = task_config
        .get("env")
        .and_then(|env| env.get("LEGION_INSTANCE_ID"))
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string);

    let task = TaskConfig {
        config: task_config,
        checks,
        on_unhealthy,
        identities,
        artifacts,
        connect,
        resources,
        stop,
        suspend,
        snapshots,
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
        instance_id,
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

    /// Block until `marker` exists — a task's own signal that it reached
    /// the point a test needs it at (a trap installed, a loop entered).
    /// Panics rather than hanging if the task never gets there.
    fn wait_for_marker(marker: &std::path::Path) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !marker.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "the task never created {}",
                marker.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    // -----------------------------------------------------------------
    // Provisional allocations (docs/edge-boot.md)
    // -----------------------------------------------------------------

    fn provisional_payload(local_key: i64) -> TaskAllocation {
        TaskAllocation {
            task_network: String::new(),
            allocation_id: local_key,
            job_id: "agent-sandboxes".into(),
            job_config_id: "jc-1".into(),
            node_id: "node-1".into(),
            task_config_id: 7,
            task_name: "workspace".into(),
            // `exec` rather than `noop`: the noop runtime is a dylib that
            // is not built for unit tests, and a real host process is the
            // stronger check anyway — a `stage`-gated claim must leave no
            // process behind.
            task_runtime: "exec".into(),
            task_config: json!({ "command": ["sleep", "30"] }).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            network: String::new(),
            task_connect: String::new(),
            task_artifacts: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_volumes: String::new(),
            task_resources: json!({ "memory": 256 }).to_string(),
            task_stop: String::new(),
            provisional: None,
            priority_band: String::new(),
            restore: None,
            task_suspend: String::new(),
            task_snapshots: String::new(),
            identity_epoch: 0,
        }
    }

    /// An `exec` allocation with a `local-dir` instance volume, as the
    /// manager dispatches it.
    fn volume_payload(allocation_id: i64) -> TaskAllocation {
        TaskAllocation {
            allocation_id,
            task_runtime: "exec".into(),
            task_config: json!({ "command": ["sleep", "30"] }).to_string(),
            task_volumes: json!([{
                "volumeId": "vol-snap-test",
                "name": "data",
                "driver": "local-dir",
                "mode": "filesystem",
                "instanceVolume": true,
            }])
            .to_string(),
            ..provisional_payload(allocation_id)
        }
    }

    fn stop_and_reap(manager: &Arc<Mutex<WorkerTaskManager>>) {
        let _ = manager.lock().unwrap().reconcile_allocations(&[]);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while manager.lock().unwrap().has_terminating() {
            manager.lock().unwrap().reap_terminating();
            assert!(
                std::time::Instant::now() < deadline,
                "allocation never stopped"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The Phase 3 round trip on `exec`, no KVM: write a marker, take a disk
    /// snapshot through the checkpoint channel, mutate the marker, start a
    /// replacement allocation from the snapshot, and the marker is back.
    #[test]
    fn an_exec_disk_snapshot_round_trips_through_capture_and_restore() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        launch_task_allocation(&manager, &volume_payload(41))?;
        let volume_dir = manager
            .lock()
            .unwrap()
            .volumes_handle()
            .mounts_for(41)
            .and_then(|mounts| mounts.first().map(|mount| mount.host_path.clone()))
            .expect("instance volume mounted");
        std::fs::write(volume_dir.join("marker"), b"before")?;

        let capture = crate::structs::CheckpointRequest {
            snapshot_id: "snap-roundtrip".into(),
            op: "capture".into(),
            allocation_id: 41,
            kind: "disk".into(),
            then: "resume".into(),
            purpose: "manual".into(),
            manifest_json: String::new(),
            ..Default::default()
        };
        let fresh = manager
            .lock()
            .unwrap()
            .checkpoints_mut()
            .accept(std::slice::from_ref(&capture));
        assert_eq!(fresh.len(), 1);
        crate::worker::checkpoints::run_request(&manager, &capture);
        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert!(report.ok, "capture failed: {}", report.error);
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(&report.manifest_json)?;
        assert_eq!(
            manifest.kind,
            crate::structs::checkpoint::SnapshotKind::Disk
        );
        assert_eq!(manifest.volumes.len(), 1);
        assert_eq!(manifest.volumes[0].volume_id, "vol-snap-test");
        assert!(
            manifest.compat.is_empty(),
            "a disk snapshot is not CPU-bound"
        );
        // The task kept running through the capture.
        assert!(manager.lock().unwrap().is_allocation_active(41));

        std::fs::write(volume_dir.join("marker"), b"after")?;
        std::fs::write(volume_dir.join("new-file"), b"x")?;
        stop_and_reap(&manager);

        let restore = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-roundtrip".into(),
                kind: "disk".into(),
                manifest_json: report.manifest_json.clone(),
                on_resume: Vec::new(),
                ..Default::default()
            }),
            ..volume_payload(42)
        };
        manager.lock().unwrap().begin_task_allocation(&restore)?;
        assert_eq!(std::fs::read(volume_dir.join("marker"))?, b"before");
        assert!(!volume_dir.join("new-file").exists());
        assert!(manager.lock().unwrap().is_allocation_active(42));

        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].op, "restore");
        assert_eq!(reports[0].allocation_id, 42);
        assert!(reports[0].ok, "{}", reports[0].error);

        // Delete reclaims the volume snapshot.
        let delete = crate::structs::CheckpointRequest {
            op: "delete".into(),
            allocation_id: 0,
            manifest_json: report.manifest_json.clone(),
            ..capture
        };
        manager
            .lock()
            .unwrap()
            .checkpoints_mut()
            .accept(std::slice::from_ref(&delete));
        crate::worker::checkpoints::run_request(&manager, &delete);
        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert!(
            reports.iter().any(|r| r.op == "delete" && r.ok),
            "{reports:?}"
        );
        stop_and_reap(&manager);
        Ok(())
    }

    /// An `exec` allocation whose instance volume is `volume_id`.
    fn volume_payload_for(allocation_id: i64, volume_id: &str) -> TaskAllocation {
        TaskAllocation {
            task_volumes: json!([{
                "volumeId": volume_id,
                "name": "data",
                "driver": "local-dir",
                "mode": "filesystem",
                "instanceVolume": true,
            }])
            .to_string(),
            ..volume_payload(allocation_id)
        }
    }

    /// Phase 6 on `exec`, for real on any OS: a disk fork captures the
    /// origin's volume while it keeps running, and every fork restores into
    /// its OWN volume with the driver's `clone` op. A marker written before
    /// the fork is in every fork; a write in one fork is in no other fork and
    /// not in the origin.
    #[test]
    fn an_exec_disk_fork_gives_every_fork_its_own_copy_of_the_volume() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        let mount_of = |allocation_id: i64| {
            manager
                .lock()
                .unwrap()
                .volumes_handle()
                .mounts_for(allocation_id)
                .and_then(|mounts| mounts.first().map(|mount| mount.host_path.clone()))
                .expect("instance volume mounted")
        };
        launch_task_allocation(&manager, &volume_payload_for(71, "vol-origin"))?;
        let origin_dir = mount_of(71);
        std::fs::write(origin_dir.join("marker"), b"before-fork")?;

        let capture = crate::structs::CheckpointRequest {
            snapshot_id: "snap-fork".into(),
            op: "capture".into(),
            allocation_id: 71,
            kind: "disk".into(),
            then: "resume".into(),
            purpose: "fork".into(),
            manifest_json: String::new(),
            ..Default::default()
        };
        manager
            .lock()
            .unwrap()
            .checkpoints_mut()
            .accept(std::slice::from_ref(&capture));
        crate::worker::checkpoints::run_request(&manager, &capture);
        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(reports.len(), 1);
        assert!(reports[0].ok, "capture failed: {}", reports[0].error);
        assert_eq!(reports[0].pause_ms, 0, "a disk capture never pauses");
        let manifest_json = reports[0].manifest_json.clone();
        assert!(
            manager.lock().unwrap().is_allocation_active(71),
            "the origin keeps running"
        );

        let forks = [
            (72, "vol-f1", "run-17-f1"),
            (73, "vol-f2", "run-17-f2"),
            (74, "vol-f3", "run-17-f3"),
        ];
        let mut dirs = Vec::new();
        for (allocation_id, volume_id, instance_id) in forks {
            let payload = TaskAllocation {
                restore: Some(crate::structs::SnapshotRestore {
                    snapshot_id: "snap-fork".into(),
                    kind: "disk".into(),
                    manifest_json: manifest_json.clone(),
                    fork: true,
                    instance_id: instance_id.into(),
                    ..Default::default()
                }),
                ..volume_payload_for(allocation_id, volume_id)
            };
            manager.lock().unwrap().begin_task_allocation(&payload)?;
            assert!(manager.lock().unwrap().is_allocation_active(allocation_id));
            let dir = mount_of(allocation_id);
            assert_ne!(dir, origin_dir, "a fork never shares the origin's volume");
            assert_eq!(
                std::fs::read(dir.join("marker"))?,
                b"before-fork",
                "the marker written before the fork is in fork {instance_id}"
            );
            dirs.push(dir);
        }
        let restores = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(restores.len(), 3);
        assert!(restores
            .iter()
            .all(|report| report.op == "restore" && report.ok));

        // Divergence: a write in one fork is seen nowhere else.
        std::fs::write(dirs[0].join("marker"), b"fork-1 was here")?;
        std::fs::write(dirs[0].join("only-in-f1"), b"x")?;
        for other in [&dirs[1], &dirs[2], &origin_dir] {
            assert_eq!(std::fs::read(other.join("marker"))?, b"before-fork");
            assert!(!other.join("only-in-f1").exists());
        }
        // And the origin's later writes reach no fork.
        std::fs::write(origin_dir.join("origin-later"), b"y")?;
        assert!(dirs.iter().all(|dir| !dir.join("origin-later").exists()));
        stop_and_reap(&manager);
        Ok(())
    }

    /// Phase 4's acceptance check on `exec`, for real on any OS: a disk
    /// suspend stops the task without copying its volume, and the resume
    /// (a NEW allocation carrying the suspend manifest) cold-boots on the
    /// retained volume, which still holds everything the task wrote.
    #[test]
    fn an_exec_disk_suspend_cold_boots_on_its_retained_volume() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        launch_task_allocation(&manager, &volume_payload(51))?;
        let volume_dir = manager
            .lock()
            .unwrap()
            .volumes_handle()
            .mounts_for(51)
            .and_then(|mounts| mounts.first().map(|mount| mount.host_path.clone()))
            .expect("instance volume mounted");
        std::fs::write(volume_dir.join("marker"), b"written before suspend")?;

        let suspend = crate::structs::CheckpointRequest {
            snapshot_id: "snap-suspend".into(),
            op: "capture".into(),
            allocation_id: 51,
            kind: "disk".into(),
            then: "stop".into(),
            purpose: "suspend".into(),
            manifest_json: String::new(),
            ..Default::default()
        };
        manager
            .lock()
            .unwrap()
            .checkpoints_mut()
            .accept(std::slice::from_ref(&suspend));
        crate::worker::checkpoints::run_request(&manager, &suspend);
        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert!(report.ok, "suspend failed: {}", report.error);
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(&report.manifest_json)?;
        assert_eq!(
            manifest.kind,
            crate::structs::checkpoint::SnapshotKind::Disk
        );
        assert!(
            manifest.volumes.is_empty(),
            "a suspend keeps the volume in place instead of copying it"
        );
        assert!(manifest.memory.is_empty());
        // The task is stopping (or stopped) with cause `suspended`; nothing
        // is left running.
        assert!(!manager.lock().unwrap().is_allocation_active(51));
        stop_and_reap(&manager);
        assert!(
            volume_dir.join("marker").exists(),
            "the volume is retained across the suspend"
        );

        // Resume: a new allocation id, the suspend manifest, a cold boot.
        let resume = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-suspend".into(),
                kind: "disk".into(),
                manifest_json: report.manifest_json.clone(),
                on_resume: Vec::new(),
                ..Default::default()
            }),
            ..volume_payload(52)
        };
        manager.lock().unwrap().begin_task_allocation(&resume)?;
        assert!(manager.lock().unwrap().is_allocation_active(52));
        assert_eq!(
            std::fs::read(volume_dir.join("marker"))?,
            b"written before suspend"
        );
        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].op, "restore");
        assert_eq!(reports[0].allocation_id, 52);
        assert!(reports[0].ok, "{}", reports[0].error);
        stop_and_reap(&manager);
        Ok(())
    }

    /// The `suspend` pressure step on a real `exec` task: the worker
    /// captures and stops it with no request from the manager, reports a
    /// local suspension under the id it minted, keeps the allocation out of
    /// relaunch while the manager still lists it, and re-sends the report
    /// until the manager drops the allocation.
    #[test]
    fn a_pressure_suspend_reports_a_local_suspension_and_holds_the_allocation() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        let payload = volume_payload(61);
        launch_task_allocation(&manager, &payload)?;
        assert_eq!(
            manager.lock().unwrap().local_suspend_kind(61)?,
            crate::structs::checkpoint::SnapshotKind::Disk,
            "an exec task suspends to disk"
        );

        let snapshot_id = crate::worker::checkpoints::run_local_suspension(&manager, 61)?;
        assert!(snapshot_id.starts_with("snap-"), "{snapshot_id}");
        let reports = manager.lock().unwrap().drain_snapshot_reports();
        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert!(report.ok && report.local_suspension, "{report:?}");
        assert_eq!(report.snapshot_id, snapshot_id);
        assert_eq!(report.allocation_id, 61);
        assert!(!manager.lock().unwrap().is_allocation_active(61));

        // The manager has not applied it yet and still lists the allocation:
        // it is not relaunched, and the report goes out again.
        let pending = manager
            .lock()
            .unwrap()
            .reconcile_allocations(std::slice::from_ref(&payload));
        assert!(pending.is_empty(), "held out of relaunch: {pending:?}");
        manager
            .lock()
            .unwrap()
            .checkpoints_mut()
            .reconcile_local_suspensions(std::slice::from_ref(&payload));
        assert_eq!(manager.lock().unwrap().drain_snapshot_reports().len(), 1);

        // Applied: the manager drops it, and the worker forgets it.
        stop_and_reap(&manager);
        manager
            .lock()
            .unwrap()
            .checkpoints_mut()
            .reconcile_local_suspensions(&[]);
        assert!(manager.lock().unwrap().drain_snapshot_reports().is_empty());
        Ok(())
    }

    /// A pressure suspend that cannot capture releases its hold and leaves
    /// the task running.
    #[test]
    fn a_failed_pressure_suspend_keeps_the_task_running() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        launch_task_allocation(&manager, &volume_payload(62))?;
        // An exec task cannot take the memory checkpoint this suspend block
        // asks for (refused at submit; forced here to make the capture fail).
        manager
            .lock()
            .unwrap()
            .task_configs
            .get_mut(&62)
            .expect("task config")
            .suspend = Some(crate::structs::suspend::SuspendConfig {
            checkpoint: Some("memory".into()),
            ..Default::default()
        });
        let err = crate::worker::checkpoints::run_local_suspension(&manager, 62).unwrap_err();
        assert!(format!("{err:#}").contains("memory"), "{err:#}");
        assert!(manager.lock().unwrap().is_allocation_active(62));
        assert!(!manager.lock().unwrap().pressure_evicted.contains(&62));
        assert!(manager.lock().unwrap().drain_snapshot_reports().is_empty());
        stop_and_reap(&manager);
        Ok(())
    }

    /// A suspend that cannot capture leaves the task running: an `exec`
    /// task asked for a memory checkpoint reports the reason and keeps going.
    #[test]
    fn a_suspend_that_cannot_capture_leaves_the_task_running() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        launch_task_allocation(&manager, &volume_payload(53))?;
        let suspend = crate::structs::CheckpointRequest {
            snapshot_id: "snap-suspend-mem".into(),
            op: "capture".into(),
            allocation_id: 53,
            kind: "memory".into(),
            then: "stop".into(),
            purpose: "suspend".into(),
            manifest_json: String::new(),
            ..Default::default()
        };
        let report = crate::worker::checkpoints::run_capture(&manager, &suspend);
        assert!(!report.ok);
        assert!(
            report.error.contains("cannot take memory checkpoints"),
            "{}",
            report.error
        );
        assert!(manager.lock().unwrap().is_allocation_active(53));
        stop_and_reap(&manager);
        Ok(())
    }

    /// RestoreInstance can hand the replacement the id the old allocation
    /// had. The worker must stop the old task and restore, not keep running
    /// it as if nothing changed.
    #[test]
    fn a_restore_that_reuses_a_running_allocation_id_replaces_the_task() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        launch_task_allocation(&manager, &volume_payload(45))?;
        let reused = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-reused".into(),
                kind: "disk".into(),
                manifest_json: "{}".into(),
                on_resume: Vec::new(),
                ..Default::default()
            }),
            ..volume_payload(45)
        };
        let pending = manager
            .lock()
            .unwrap()
            .reconcile_allocations(std::slice::from_ref(&reused));
        assert!(pending.is_empty(), "held until the old task is gone");
        assert!(!manager.lock().unwrap().is_allocation_active(45));
        stop_and_reap(&manager);
        let pending = manager
            .lock()
            .unwrap()
            .reconcile_allocations(std::slice::from_ref(&reused));
        assert_eq!(pending.len(), 1, "then started again, from the snapshot");
        Ok(())
    }

    #[test]
    fn a_memory_snapshot_of_an_exec_task_fails_with_the_reason() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        launch_task_allocation(&manager, &volume_payload(43))?;
        let capture = crate::structs::CheckpointRequest {
            snapshot_id: "snap-mem".into(),
            op: "capture".into(),
            allocation_id: 43,
            kind: "memory".into(),
            then: "resume".into(),
            purpose: "manual".into(),
            manifest_json: String::new(),
            ..Default::default()
        };
        let report = crate::worker::checkpoints::run_capture(&manager, &capture);
        assert!(!report.ok);
        assert!(
            report.error.contains("cannot take memory checkpoints"),
            "{}",
            report.error
        );
        // A failed capture leaves the task running.
        assert!(manager.lock().unwrap().is_allocation_active(43));
        stop_and_reap(&manager);
        Ok(())
    }

    /// A restore that fails reports the failure on the checkpoint channel
    /// and fails the allocation with the same reason, never a silent cold
    /// start.
    #[test]
    fn a_failed_restore_is_reported_and_fails_the_allocation() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        let manifest = json!({
            "id": "snap-gone",
            "jobId": "agent-sandboxes",
            "task": "workspace",
            "originAllocId": 1,
            "runtime": "exec",
            "kind": "disk",
            "createdAt": "2026-10-10T00:00:00Z",
            "volumes": [{
                "id": "snap-gone",
                "volumeId": "vol-snap-test",
                "driver": "local-dir",
                "mode": "filesystem",
            }],
        });
        let restore = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-gone".into(),
                kind: "disk".into(),
                manifest_json: manifest.to_string(),
                on_resume: Vec::new(),
                ..Default::default()
            }),
            ..volume_payload(44)
        };
        let err = launch_task_allocation(&manager, &restore).unwrap_err();
        assert!(format!("{err:#}").contains("snap-gone"), "{err:#}");
        let mut guard = manager.lock().unwrap();
        let reports = guard.drain_snapshot_reports();
        assert_eq!(reports.len(), 1, "{reports:?}");
        assert!(!reports[0].ok);
        assert!(
            reports[0].error.contains("snap-gone"),
            "{}",
            reports[0].error
        );
        let failures = guard.drain_failures();
        assert_eq!(failures.len(), 1);
        assert!(!guard.is_allocation_active(44));
        Ok(())
    }

    /// A worker with its own node content store (and, given `shared`, a
    /// `file://` contentStore to publish to), as the daemon wires it.
    fn worker_with_store(
        temp_dir: &TempDir,
        shared: Option<&std::path::Path>,
    ) -> R<Arc<Mutex<WorkerTaskManager>>> {
        let mut manager = manager_in(temp_dir);
        let store = Arc::new(crate::content_store::ContentStore::open(
            temp_dir.path().join("content"),
            crate::content_store::ContentStoreConfig {
                min_free_disk: 0,
                ..Default::default()
            },
        )?);
        manager.configure_content_store(Some(store.clone()));
        manager.configure_content_publishing(shared.map(|shared| {
            Arc::new(crate::content_publish::ContentPublishing::new(
                store.clone(),
                Arc::new(crate::content_publish::FileContentPublisher::new(shared)),
            ))
        }));
        manager.configure_artifacts(
            crate::structs::WorkerArtifactFetchConfigFull::default(),
            Box::new(crate::artifacts::AnonymousCredentials),
        );
        Ok(Arc::new(Mutex::new(manager)))
    }

    /// The checkpoint key the tests' "manager" derives part keys from.
    const TEST_CHECKPOINT_KEY: [u8; 32] = [42; 32];

    /// What the manager attaches to a publish, warm, or restore: one key per
    /// memory part of `manifest`.
    fn part_keys_for(manifest: &Value) -> Vec<crate::structs::CheckpointPartKey> {
        manifest["memory"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|part| {
                let digest =
                    crate::content_store::ContentDigest::parse(part["digest"].as_str().unwrap())
                        .unwrap();
                crate::checkpoint_seal::PartKey::derive(&TEST_CHECKPOINT_KEY, "ck_test", &digest)
                    .unwrap()
                    .to_proto()
            })
            .collect()
    }

    /// Wait for the next snapshot report a worker queues.
    fn next_report(
        manager: &Arc<Mutex<WorkerTaskManager>>,
        what: &str,
    ) -> crate::structs::SnapshotReport {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(report) = manager.lock().unwrap().drain_snapshot_reports().pop() {
                return report;
            }
            assert!(std::time::Instant::now() < deadline, "no {what} report");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Phase 7b's cross-node transport, for real on any OS: node A publishes
    /// a snapshot's part to a `file://` content store, and node B, which
    /// never held it, fetches it through its artifact stager (verified by
    /// digest) into its own content store, then restores.
    ///
    /// The part rides a disk-kind manifest so the `exec` runtime can take
    /// the restore. The runtime half of a memory restore needs KVM and is in
    /// the Linux verification plan (Phase 7b).
    #[test]
    fn a_published_snapshot_restores_on_a_node_that_never_held_it() -> R<()> {
        let shared = TempDir::new()?;
        let node_a_dir = TempDir::new()?;
        let node_b_dir = TempDir::new()?;
        let node_a = worker_with_store(&node_a_dir, Some(shared.path()))?;
        let node_b = worker_with_store(&node_b_dir, None)?;

        // What a capture leaves on A: the part in A's store, pinned to the
        // snapshot.
        let bytes = b"guest memory page ".repeat(64 * 1024);
        let entry = node_a
            .lock()
            .unwrap()
            .content_store()
            .expect("store")
            .put_bytes(
                crate::content_store::ContentClass::Snapshots,
                &bytes,
                crate::content_store::PutOptions::default()
                    .pin(crate::content_store::PinOwner::snapshot("snap-moving")),
            )?;
        let captured = json!({
            "apiVersion": 1,
            "id": "snap-moving",
            "jobId": "agent-sandboxes",
            "task": "workspace",
            "originAllocId": 70,
            "runtime": "exec",
            "kind": "disk",
            "createdAt": "2026-10-10T00:00:00Z",
            "memory": [{
                "role": "scratch", "file": "scratch.bin",
                "digest": entry.digest.as_str(), "size": bytes.len(),
            }],
        });

        // A publishes, sealing with the key the manager sent.
        let publish = crate::structs::CheckpointRequest {
            snapshot_id: "snap-moving".into(),
            op: "publish".into(),
            kind: "disk".into(),
            purpose: "suspend".into(),
            manifest_json: captured.to_string(),
            part_keys: part_keys_for(&captured),
            ..Default::default()
        };
        node_a
            .lock()
            .unwrap()
            .checkpoints_mut()
            .accept(std::slice::from_ref(&publish));
        crate::worker::checkpoints::run_request(&node_a, &publish);
        let report = next_report(&node_a, "publish");
        assert!(report.ok, "{}", report.error);
        assert_eq!(report.total_bytes, bytes.len() as u64);
        let published =
            crate::structs::checkpoint::SnapshotManifest::from_json(&report.manifest_json)?;
        let url = published.memory[0].url.clone().expect("part url");
        assert!(url.starts_with("file://"), "{url}");
        assert_eq!(published.memory[0].key_id.as_deref(), Some("ck_test"));
        let key = crate::checkpoint_seal::PartKey::derive(
            &TEST_CHECKPOINT_KEY,
            "ck_test",
            &entry.digest,
        )?;
        let object = shared.path().join("sealed").join(key.object_name());
        let sealed = std::fs::read(&object)?;
        assert_eq!(&sealed[..8], crate::checkpoint_seal::SEAL_MAGIC);
        assert!(
            !sealed.windows(18).any(|w| w == b"guest memory page "),
            "the bucket never holds the guest's plaintext"
        );
        assert!(!shared.path().join("sha256").exists());

        // B restores from the published manifest, with the keys the manager
        // attaches to the restore payload.
        let restore = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-moving".into(),
                kind: "disk".into(),
                manifest_json: report.manifest_json.clone(),
                on_resume: Vec::new(),
                part_keys: part_keys_for(&captured),
                ..Default::default()
            }),
            ..provisional_payload(71)
        };
        launch_task_allocation(&node_b, &restore)?;
        let restored = next_report(&node_b, "restore");
        assert_eq!(restored.op, "restore");
        assert!(restored.ok, "{}", restored.error);
        assert!(node_b.lock().unwrap().is_allocation_active(71));
        let store_b = node_b.lock().unwrap().content_store().expect("store");
        let fetched = store_b.verify(&entry.digest)?;
        assert_eq!(fetched.size, bytes.len() as u64);
        assert!(
            store_b.pins(&entry.digest)?.is_empty(),
            "the fetch pins are released once the allocation started"
        );
        stop_and_reap(&node_b);
        Ok(())
    }

    /// Publishing unpins the parts on the node that captured them, so its
    /// content store can evict them like any cache entry. A resume there
    /// afterwards pins what is still present and fetches what was evicted,
    /// exactly as on a node that never held them. A delete then frees the
    /// unpinned parts at once.
    #[test]
    fn a_published_snapshot_is_unpinned_and_restores_at_home_after_eviction() -> R<()> {
        let shared = TempDir::new()?;
        let node_dir = TempDir::new()?;
        let node = worker_with_store(&node_dir, Some(shared.path()))?;
        let store = node.lock().unwrap().content_store().expect("store");
        let owner = crate::content_store::PinOwner::snapshot("snap-home");
        let bytes = b"guest memory page ".repeat(64 * 1024);
        let entry = store.put_bytes(
            crate::content_store::ContentClass::Snapshots,
            &bytes,
            crate::content_store::PutOptions::default().pin(owner.clone()),
        )?;
        let captured = json!({
            "apiVersion": 1, "id": "snap-home", "jobId": "agent-sandboxes",
            "task": "workspace", "originAllocId": 80, "runtime": "exec", "kind": "disk",
            "createdAt": "2026-10-10T00:00:00Z",
            "memory": [{
                "role": "scratch", "file": "scratch.bin",
                "digest": entry.digest.as_str(), "size": bytes.len(),
            }],
        });
        let usage_pinned = |store: &crate::content_store::ContentStore| -> R<u64> {
            Ok(store
                .usage()?
                .into_iter()
                .find(|class| class.class == crate::content_store::ContentClass::Snapshots)
                .map_or(0, |class| class.pinned_bytes))
        };
        assert_eq!(usage_pinned(&store)?, bytes.len() as u64);

        let publish = crate::structs::CheckpointRequest {
            snapshot_id: "snap-home".into(),
            op: "publish".into(),
            kind: "disk".into(),
            purpose: "suspend".into(),
            manifest_json: captured.to_string(),
            part_keys: part_keys_for(&captured),
            ..Default::default()
        };
        node.lock()
            .unwrap()
            .checkpoints_mut()
            .accept(std::slice::from_ref(&publish));
        crate::worker::checkpoints::run_request(&node, &publish);
        let report = next_report(&node, "publish");
        assert!(report.ok, "{}", report.error);
        assert!(store.contains(&entry.digest)?, "the bytes stay");
        assert!(store.pins(&entry.digest)?.is_empty(), "the pin goes");
        assert_eq!(usage_pinned(&store)?, 0, "no longer counted as pinned");

        // The evictor takes the unpinned part, then the instance resumes here.
        assert!(store.remove(&entry.digest)?);
        let restore = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-home".into(),
                kind: "disk".into(),
                manifest_json: report.manifest_json.clone(),
                on_resume: Vec::new(),
                part_keys: part_keys_for(&captured),
                ..Default::default()
            }),
            ..provisional_payload(81)
        };
        launch_task_allocation(&node, &restore)?;
        let restored = next_report(&node, "restore");
        assert!(restored.ok, "{}", restored.error);
        assert_eq!(store.verify(&entry.digest)?.size, bytes.len() as u64);
        assert!(
            store.pins(&entry.digest)?.is_empty(),
            "the restore's pin is released once the allocation started"
        );
        stop_and_reap(&node);

        // Deleting the snapshot frees its unpinned part on this node.
        let delete = crate::structs::CheckpointRequest {
            snapshot_id: "snap-home".into(),
            op: "delete".into(),
            manifest_json: report.manifest_json.clone(),
            ..Default::default()
        };
        let deleted = crate::worker::checkpoints::run_delete(&node, &delete);
        assert!(deleted.ok, "{}", deleted.error);
        assert!(!store.contains(&entry.digest)?);
        Ok(())
    }

    /// A restore of a part this node lacks and that was never published
    /// fails with a named reason, on the checkpoint channel and as the
    /// allocation failure.
    #[test]
    fn an_unpublished_part_on_another_node_fails_the_restore_by_name() -> R<()> {
        let node_dir = TempDir::new()?;
        let node = worker_with_store(&node_dir, None)?;
        let manifest = json!({
            "apiVersion": 1,
            "id": "snap-elsewhere",
            "jobId": "agent-sandboxes",
            "task": "workspace",
            "originAllocId": 72,
            "runtime": "exec",
            "kind": "disk",
            "createdAt": "2026-10-10T00:00:00Z",
            "memory": [{
                "role": "memory", "file": "memory",
                "digest": format!("sha256:{}", "cd".repeat(32)), "size": 4096,
            }],
        });
        let restore = TaskAllocation {
            restore: Some(crate::structs::SnapshotRestore {
                snapshot_id: "snap-elsewhere".into(),
                kind: "disk".into(),
                manifest_json: manifest.to_string(),
                on_resume: Vec::new(),
                ..Default::default()
            }),
            ..provisional_payload(73)
        };
        launch_task_allocation(&node, &restore)?;
        let report = next_report(&node, "restore");
        assert!(!report.ok);
        assert!(report.error.contains("never published"), "{}", report.error);
        assert!(!node.lock().unwrap().is_allocation_active(73));
        Ok(())
    }

    /// Publish `bytes` from a fresh node to `shared` under the test
    /// checkpoint key, and return the published manifest (with `url` and
    /// `keyId`) as JSON.
    fn published_manifest(shared: &std::path::Path, id: &str, bytes: &[u8]) -> R<Value> {
        let publisher_dir = TempDir::new()?;
        let publisher = worker_with_store(&publisher_dir, Some(shared))?;
        let publishing = publisher
            .lock()
            .unwrap()
            .content_publishing()
            .expect("publishing");
        let entry = publishing.store().put_bytes(
            crate::content_store::ContentClass::Snapshots,
            bytes,
            crate::content_store::PutOptions::default(),
        )?;
        let key = crate::checkpoint_seal::PartKey::derive(
            &TEST_CHECKPOINT_KEY,
            "ck_test",
            &entry.digest,
        )?;
        let published = publishing.publish(&key)?;
        Ok(json!({
            "id": id, "jobId": "j", "task": "t", "originAllocId": 1,
            "runtime": "sandbox", "kind": "memory", "createdAt": "2026-10-10T00:00:00Z",
            "memory": [{
                "role": "memory", "file": "memory", "digest": entry.digest.as_str(),
                "size": bytes.len(), "url": published.url, "keyId": published.key_id,
            }],
        }))
    }

    /// `warm` lands a published snapshot's parts in the node content store,
    /// unpinned (an ordinary cache entry), and a second warm fetches nothing.
    #[test]
    fn warm_fetches_published_parts_into_the_store_once() -> R<()> {
        let shared = TempDir::new()?;
        let warm_dir = TempDir::new()?;
        let warmed = worker_with_store(&warm_dir, None)?;
        let bytes = b"warm me ".repeat(32 * 1024);
        let manifest = published_manifest(shared.path(), "snap-warm", &bytes)?;
        let digest = crate::content_store::ContentDigest::of_bytes(&bytes);
        let warm = crate::structs::CheckpointRequest {
            snapshot_id: "snap-warm".into(),
            op: "warm".into(),
            manifest_json: manifest.to_string(),
            part_keys: part_keys_for(&manifest),
            ..Default::default()
        };
        let report = crate::worker::checkpoints::run_warm(&warmed, &warm);
        assert!(report.ok, "{}", report.error);
        assert_eq!(report.total_bytes, bytes.len() as u64);
        let store = warmed.lock().unwrap().content_store().expect("store");
        assert_eq!(store.verify(&digest)?.size, bytes.len() as u64);
        assert!(
            store.pins(&digest)?.is_empty(),
            "warmed parts are not pinned"
        );
        // Already warm: nothing fetched, and no key needed to know that.
        let again = crate::worker::checkpoints::run_warm(
            &warmed,
            &crate::structs::CheckpointRequest {
                part_keys: Vec::new(),
                part_key_error: "keyring locked".into(),
                ..warm.clone()
            },
        );
        assert!(again.ok, "{}", again.error);
        assert_eq!(again.total_bytes, 0, "already warm: nothing fetched");
        Ok(())
    }

    /// Every way a published part can fail to open names itself, and none
    /// leaves anything in the content store: no key from the manager (with
    /// its reason), the wrong key, a tampered object, and a manifest from
    /// before checkpoint encryption.
    #[test]
    fn a_published_part_that_cannot_be_opened_fails_by_name() -> R<()> {
        let shared = TempDir::new()?;
        let bytes = b"secret guest page ".repeat(64 * 1024);
        let manifest = published_manifest(shared.path(), "snap-sealed", &bytes)?;
        let digest = crate::content_store::ContentDigest::of_bytes(&bytes);
        let warm_with = |manifest: &Value,
                         part_keys: Vec<crate::structs::CheckpointPartKey>,
                         part_key_error: &str|
         -> R<String> {
            let dir = TempDir::new()?;
            let node = worker_with_store(&dir, None)?;
            let report = crate::worker::checkpoints::run_warm(
                &node,
                &crate::structs::CheckpointRequest {
                    snapshot_id: "snap-sealed".into(),
                    op: "warm".into(),
                    manifest_json: manifest.to_string(),
                    part_keys,
                    part_key_error: part_key_error.into(),
                    ..Default::default()
                },
            );
            let store = node.lock().unwrap().content_store().expect("store");
            assert!(!store.contains(&digest)?, "nothing unverified is stored");
            assert!(!report.ok);
            Ok(report.error)
        };

        let error = warm_with(
            &manifest,
            Vec::new(),
            "keyring locked: checkpoint key ck_test cannot be unwrapped",
        )?;
        assert!(error.contains("sent no key"), "{error}");
        assert!(error.contains("keyring locked"), "{error}");

        let wrong = crate::checkpoint_seal::PartKey::derive(&[7; 32], "ck_test", &digest)?;
        let error = warm_with(&manifest, vec![wrong.to_proto()], "")?;
        // The wrong key also names the wrong object, which is not there.
        assert!(error.contains("fetching part memory"), "{error}");

        let key =
            crate::checkpoint_seal::PartKey::derive(&TEST_CHECKPOINT_KEY, "ck_test", &digest)?;
        let object = shared.path().join("sealed").join(key.object_name());
        let mut sealed = std::fs::read(&object)?;
        sealed[100] ^= 0x40;
        std::fs::write(&object, &sealed)?;
        let error = warm_with(&manifest, part_keys_for(&manifest), "")?;
        assert!(
            error.contains("failed authentication at chunk 0"),
            "{error}"
        );
        assert!(
            error.contains("after 1 attempt"),
            "a tampered part is not downloaded again: {error}"
        );

        // A manifest published before checkpoint encryption: a URL to a
        // plaintext object and no keyId.
        let legacy = shared.path().join("sha256").join(digest.hex());
        std::fs::create_dir_all(legacy.parent().unwrap())?;
        std::fs::write(&legacy, &bytes)?;
        let mut old = manifest.clone();
        old["memory"][0]["url"] = json!(url::Url::from_file_path(&legacy).unwrap().to_string());
        old["memory"][0].as_object_mut().unwrap().remove("keyId");
        let error = warm_with(&old, part_keys_for(&old), "")?;
        assert!(
            error.contains("published before checkpoint encryption"),
            "{error}"
        );

        // Pointed at the plaintext object anyway, the opener refuses it.
        let mut relabeled = manifest.clone();
        relabeled["memory"][0]["url"] = old["memory"][0]["url"].clone();
        let error = warm_with(&relabeled, part_keys_for(&relabeled), "")?;
        assert!(error.contains("no LGNSEAL1 header"), "{error}");
        Ok(())
    }

    /// Publishing on a worker with no contentStore fails loudly, naming the
    /// fix.
    #[test]
    fn publishing_without_a_content_store_names_the_fix() -> R<()> {
        let node_dir = TempDir::new()?;
        let node = worker_with_store(&node_dir, None)?;
        let publish = crate::structs::CheckpointRequest {
            snapshot_id: "snap-nowhere".into(),
            op: "publish".into(),
            manifest_json: json!({
                "id": "snap-nowhere", "jobId": "j", "task": "t", "originAllocId": 1,
                "runtime": "sandbox", "kind": "memory", "createdAt": "2026-10-10T00:00:00Z",
            })
            .to_string(),
            ..Default::default()
        };
        let report = crate::worker::checkpoints::run_publish(&node, &publish);
        assert!(!report.ok);
        assert!(report.error.contains("no contentStore"), "{}", report.error);
        assert!(report.error.contains("Hint:"), "{}", report.error);
        Ok(())
    }

    fn claim_record(claim_id: &str, instance_id: &str, gate: Gate) -> ClaimRecord {
        ClaimRecord {
            claim_id: claim_id.into(),
            local_key: 0,
            gate,
            stage: ClaimStage::Claimed,
            lease_deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            lease_deadline_unix: 0,
            job_id: "agent-sandboxes".into(),
            job_config_id: "jc-1".into(),
            task_name: "workspace".into(),
            instance_id: instance_id.into(),
            resources: json!({ "memory": 256 }),
            origin: "edge".into(),
            on_refused: OnRefused::Kill,
            ticket_jti: String::new(),
            allocation_id: None,
        }
    }

    #[test]
    fn a_stage_gated_claim_prepares_without_starting_anything() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Stage))?;

        let outcome = manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Stage)?;
        assert!(
            matches!(outcome, BeginOutcome::Staged(..)),
            "a stage-gated launch must stop short of start"
        );
        // Registered (so it can be stopped) but reported as staged, not
        // running: nothing outside the node can observe it.
        assert!(manager.is_allocation_active(admitted.local_key));
        let health = manager.allocation_health;
        let entry = health.get(&admitted.local_key).expect("health entry");
        assert_eq!(entry.status, "staged (awaiting ratification)");
        Ok(())
    }

    #[test]
    fn an_unratified_claim_is_absent_from_everything_the_manager_reads() -> R<()> {
        // The manager must not be told about work it has not recorded: a
        // negative local key reported as a running allocation would be a lie
        // the reconcile loop acts on.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;

        assert!(manager.allocations_snapshot().is_empty());
        assert!(manager.allocation_health_snapshot().is_empty());
        assert_eq!(manager.unratified_claims().len(), 1);

        // Once ratified it appears under the manager's id, and disappears
        // from the provisional report.
        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        manager.ratify_claim("clm_a", &committed)?;
        assert_eq!(manager.allocations_snapshot(), vec![8123]);
        assert!(manager.unratified_claims().is_empty());
        assert_eq!(manager.local_key(8123), admitted.local_key);
        Ok(())
    }

    #[test]
    fn an_unratified_claim_survives_a_heartbeat_that_omits_it() -> R<()> {
        // The protection window. The manager cannot list work it does not
        // know about, so an absence here is ignorance, not a decision —
        // reading it as "stop this" would kill every provisional start on its
        // first heartbeat.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;

        manager.reconcile_allocations(&[]);
        assert!(
            manager.is_allocation_active(admitted.local_key),
            "an unratified claim must not be reconciled away"
        );
        Ok(())
    }

    #[test]
    fn a_ratified_claim_is_reconciled_like_any_other_allocation() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;
        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        manager.ratify_claim("clm_a", &committed)?;

        // Still desired: nothing happens, and no relaunch is proposed.
        let mut desired = provisional_payload(admitted.local_key);
        desired.allocation_id = 8123;
        assert!(manager.reconcile_allocations(&[desired]).is_empty());
        assert!(manager.is_allocation_active(admitted.local_key));

        // No longer desired: the protection window is over, so it stops.
        manager.reconcile_allocations(&[]);
        assert!(!manager.is_allocation_active(admitted.local_key));
        Ok(())
    }

    #[test]
    fn an_expired_lease_kills_the_claim_and_releases_its_capacity() -> R<()> {
        // The backstop for every failure the reply path cannot cover.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.configure_ledger(LocalLedger::new(
            &json!({ "memory": 512 }),
            &json!({}),
            None,
        ));
        let mut record = claim_record("clm_a", "user-42", Gate::Boot);
        record.lease_deadline = std::time::Instant::now() - std::time::Duration::from_secs(1);
        let admitted = manager.admit_claim(record)?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;

        let killed = manager.sweep_expired_claims(std::time::Instant::now());
        assert_eq!(killed.len(), 1);
        assert!(!manager.is_allocation_active(admitted.local_key));
        // The ledger hold is gone, so the node can admit the next start.
        manager.admit_claim(claim_record("clm_b", "user-43", Gate::Boot))?;
        Ok(())
    }

    #[test]
    fn a_ratified_claim_is_not_swept_by_its_lease() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let mut record = claim_record("clm_a", "user-42", Gate::Boot);
        record.lease_deadline = std::time::Instant::now() - std::time::Duration::from_secs(1);
        let admitted = manager.admit_claim(record)?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;
        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        manager.ratify_claim("clm_a", &committed)?;

        assert!(manager
            .sweep_expired_claims(std::time::Instant::now())
            .is_empty());
        assert!(manager.is_allocation_active(admitted.local_key));
        Ok(())
    }

    #[test]
    fn a_refused_claim_reports_nothing_to_the_manager() -> R<()> {
        // A provisional allocation the manager never knew about must not
        // arrive as a failed allocation: there is nothing to reschedule, and
        // a phantom failure would look like a real workload dying.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;

        manager.kill_claim("clm_a", "instance claimed elsewhere");
        assert!(manager.drain_failures().is_empty());
        assert!(manager.drain_terminations().is_empty());
        assert!(manager.allocation_health_snapshot().is_empty());
        assert!(manager.unratified_claims().is_empty());
        Ok(())
    }

    #[test]
    fn a_second_claim_for_the_same_instance_is_refused_locally() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        assert!(manager
            .admit_claim(claim_record("clm_b", "user-42", Gate::Boot))
            .is_err());
        assert!(manager.instance_is_claimed("user-42"));
        Ok(())
    }

    #[test]
    fn ratifying_an_unknown_claim_is_an_error_not_a_silent_no_op() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let mut committed = provisional_payload(-1);
        committed.allocation_id = 8123;
        assert!(manager.ratify_claim("clm_missing", &committed).is_err());
        Ok(())
    }

    #[test]
    fn a_duplicate_verdict_is_a_no_op() -> R<()> {
        // The RatifyClaim reply and the heartbeat catch-up can both land.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;
        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        manager.ratify_claim("clm_a", &committed)?;
        let again = manager.ratify_claim("clm_a", &committed)?;
        assert_eq!(again.claim.allocation_id, Some(8123));
        assert!(
            again.start_health_monitor.is_none(),
            "a duplicate verdict must not start a second health monitor"
        );
        assert_eq!(manager.allocations_snapshot(), vec![8123]);
        Ok(())
    }

    #[test]
    fn ratifying_a_stage_gated_claim_starts_it_and_asks_for_a_health_monitor() -> R<()> {
        // The `stage` gate is a pause between prepare and start, so this is
        // the point at which the workload becomes real — and the point at
        // which it needs supervising.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Stage))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Stage)?;

        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        let ratified = manager.ratify_claim("clm_a", &committed)?;
        assert!(
            ratified.start_health_monitor.is_some(),
            "a stage-gated claim starts at ratification and must be monitored"
        );
        Ok(())
    }

    #[test]
    fn ratifying_a_boot_gated_claim_does_not_restart_its_monitor() -> R<()> {
        // A `boot`-gated claim was already running (and already monitored);
        // ratification only opens its network.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Boot))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Boot)?;

        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        let ratified = manager.ratify_claim("clm_a", &committed)?;
        assert!(ratified.start_health_monitor.is_none());
        Ok(())
    }

    #[test]
    fn ratification_adopts_the_committed_payload() -> R<()> {
        // The payload a node booted with could not name assigned ports or a
        // bound instance; the verdict's payload replaces it before anything
        // starts.
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let admitted = manager.admit_claim(claim_record("clm_a", "user-42", Gate::Stage))?;
        manager
            .begin_task_allocation_gated(&provisional_payload(admitted.local_key), Gate::Stage)?;

        let mut committed = provisional_payload(admitted.local_key);
        committed.allocation_id = 8123;
        committed.task_config =
            json!({ "command": ["sleep", "30"], "env": { "LEGION_ALLOC_ID": "8123" } }).to_string();
        manager.ratify_claim("clm_a", &committed)?;

        let stored = manager
            .task_configs
            .get(&admitted.local_key)
            .expect("task config");
        assert_eq!(stored.config["env"]["LEGION_ALLOC_ID"], json!("8123"));
        Ok(())
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
        // The shell creates `trapped` once the trap is in place: a signal
        // that lands before then kills it like any other process, which on
        // a loaded machine turned this test's `Killed` into `Exited`.
        let trapped = temp_dir.path().join("trapped");
        let task = TaskConfig {
            config: json!({
                "command": [
                    "sh",
                    "-c",
                    format!("trap '' TERM; : > '{}'; sleep 30", trapped.display()),
                ],
            }),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: std::time::Duration::ZERO,
                drain: None,
            }),
            ..TaskConfig::new("stubborn", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        wait_for_marker(&trapped);

        let _ = manager.reconcile_allocations(&[]);
        assert_eq!(manager.terminating_ids(), vec![5]);

        // A zero grace window is already expired, so the very first reap
        // takes it — no sleeping on the deadline.
        let reaped = manager.reap_terminating();
        assert_eq!(
            reaped,
            vec![(5, ReapOutcome::Killed)],
            "a task still running at the deadline is killed, not waited on forever"
        );
        assert!(manager.terminating_ids().is_empty());
        Ok(())
    }

    /// Regression (audit W4): an allocation inside its grace window was
    /// dropped from durable state, so a worker restart forgot a task that
    /// ignored its signal (and its volume's in-use guard). The restarted
    /// worker must finish the stop.
    #[test]
    fn a_restart_finishes_stopping_an_allocation_inside_its_grace_window() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let trapped = temp_dir.path().join("trapped");
        let task = TaskConfig {
            config: json!({
                "command": [
                    "sh",
                    "-c",
                    format!("trap '' TERM; : > '{}'; sleep 30", trapped.display()),
                ],
            }),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: Duration::from_secs(1),
                drain: None,
            }),
            ..TaskConfig::new("stubborn", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        wait_for_marker(&trapped);
        let pid = manager.allocation_pid(alloc().id).expect("pid");
        let _ = manager.reconcile_allocations(&[]);
        assert_eq!(manager.terminating_ids(), vec![alloc().id]);

        // The worker restarts mid-grace.
        let mut restarted = manager_in(&temp_dir);
        assert!(
            restarted.restore_allocations()?.is_empty(),
            "a stopping allocation is not restored as running work"
        );
        assert!(!restarted.has_allocation(alloc().id));
        assert_eq!(restarted.terminating_ids(), vec![alloc().id]);
        assert!(restarted.has_terminating(), "new starts stay held back");

        // The deadline carried over, so the reaper kills it when it passes.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let reaped = loop {
            let reaped = restarted.reap_terminating();
            if !reaped.is_empty() {
                break reaped;
            }
            assert!(std::time::Instant::now() < deadline, "never reaped");
            thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(reaped, vec![(alloc().id, ReapOutcome::Killed)]);
        // A zombie (the old manager still holds the `Child`) counts as gone.
        assert!(!pid_is_running(pid), "the stubborn task is gone");
        assert!(
            WorkerStateStore::new(temp_dir.path().join("worker_state.json"))
                .load()?
                .is_empty(),
            "a finished stop leaves nothing persisted"
        );
        Ok(())
    }

    /// A drain interrupted by a restart is not resumed: the restart already
    /// spent the time it bought, so the restarted worker signals at once.
    #[test]
    fn a_restart_signals_an_allocation_that_was_draining() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.set_drain_window(Duration::from_secs(3600));
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            service: Some(serde_json::from_value(json!({"port": 8080}))?),
            ..TaskConfig::new("web", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        let _ = manager.reconcile_allocations(&[]);
        assert_eq!(manager.draining_ids(), vec![alloc().id]);

        let mut restarted = manager_in(&temp_dir);
        assert!(restarted.restore_allocations()?.is_empty());
        assert!(restarted.draining_ids().is_empty());
        assert_eq!(restarted.terminating_ids(), vec![alloc().id]);
        // `sleep` exits on SIGTERM well inside its default window.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while restarted.has_terminating() {
            restarted.reap_terminating();
            assert!(std::time::Instant::now() < deadline, "never reaped");
            thread::sleep(Duration::from_millis(50));
        }
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
                drain: None,
            }),
            ..TaskConfig::new("victim", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        // The manager's next desired set drops allocation 5 and adds 6 — the
        // shape a preemption plan produces, since victim and preemptor commit
        // together and arrive in one heartbeat diff.
        let preemptor = TaskAllocation {
            task_stop: String::new(),
            provisional: None,
            allocation_id: 6,
            ..Default::default()
        };
        let launch = manager.reconcile_allocations(std::slice::from_ref(&preemptor));
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
                drain: None,
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

    fn wait_for_reap(manager: &mut WorkerTaskManager) -> Vec<(i64, ReapOutcome)> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let reaped = manager.reap_terminating();
            if !reaped.is_empty() {
                return reaped;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "allocation was never reaped"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Drain before stop (docs/graceful-stop.md): a task that advertises a
    /// service leaves the live set at once but is not signalled until its
    /// drain window closes, then takes the ordinary ladder.
    #[test]
    fn a_service_task_drains_before_its_signal() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.set_drain_window(Duration::from_millis(300));
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            service: Some(serde_json::from_value(json!({"port": 8080}))?),
            ..TaskConfig::new("web", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        assert!(manager.reconcile_allocations(&[]).is_empty());
        assert_eq!(manager.draining_ids(), vec![alloc().id]);
        assert!(manager.terminating_ids().is_empty(), "not signalled yet");
        assert!(
            !manager.is_allocation_active(alloc().id),
            "out of the live set"
        );
        assert!(
            manager.reap_draining().is_empty(),
            "the window has not closed"
        );

        thread::sleep(Duration::from_millis(350));
        assert_eq!(manager.reap_draining(), vec![alloc().id]);
        assert!(manager.draining_ids().is_empty());
        assert_eq!(
            manager.terminating_ids(),
            vec![alloc().id],
            "now on the ladder"
        );
        wait_for_reap(&mut manager);
        assert!(manager.terminating_ids().is_empty());
        Ok(())
    }

    /// Nothing routes to a task without a service, so it is signalled at
    /// once — the drain window must not slow every stop on the node.
    #[test]
    fn a_task_without_a_service_is_not_drained() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.set_drain_window(Duration::from_millis(300));
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..TaskConfig::new("batch", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        assert!(manager.reconcile_allocations(&[]).is_empty());
        assert!(manager.draining_ids().is_empty());
        assert_eq!(manager.terminating_ids(), vec![alloc().id]);
        wait_for_reap(&mut manager);
        Ok(())
    }

    /// `stop.drain = "0s"` opts a service task out of the window.
    #[test]
    fn stop_drain_zero_disables_the_window() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.set_drain_window(Duration::from_millis(300));
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            service: Some(serde_json::from_value(json!({"port": 8080}))?),
            stop: Some(crate::structs::StopConfig {
                drain: Some(Duration::ZERO),
                ..Default::default()
            }),
            ..TaskConfig::new("web", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        assert!(manager.reconcile_allocations(&[]).is_empty());
        assert!(manager.draining_ids().is_empty());
        assert_eq!(manager.terminating_ids(), vec![alloc().id]);
        wait_for_reap(&mut manager);
        Ok(())
    }

    /// A draining allocation holds starts back exactly as an in-grace one
    /// does: it still owns its ports.
    #[test]
    fn a_draining_allocation_holds_new_starts_back() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        manager.set_drain_window(Duration::from_millis(200));
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            service: Some(serde_json::from_value(json!({"port": 8080}))?),
            ..TaskConfig::new("web", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        let replacement = TaskAllocation {
            allocation_id: 6,
            ..provisional_payload(6)
        };
        assert!(
            manager
                .reconcile_allocations(std::slice::from_ref(&replacement))
                .is_empty(),
            "held while the old allocation drains"
        );
        assert_eq!(manager.draining_ids(), vec![alloc().id]);
        thread::sleep(Duration::from_millis(250));
        manager.reap_draining();
        wait_for_reap(&mut manager);
        let launch = manager.reconcile_allocations(&[replacement]);
        assert_eq!(
            launch.iter().map(|a| a.allocation_id).collect::<Vec<_>>(),
            vec![6]
        );
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
                drain: None,
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
                    drain: None,
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
            task_stop: String::new(),
            provisional: None,
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

    fn sleep_dispatch(allocation_id: i64) -> TaskAllocation {
        TaskAllocation {
            allocation_id,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["sleep", "30"]}).to_string(),
            ..Default::default()
        }
    }

    fn take_launch(outcome: BeginOutcome) -> UnlockedLaunch {
        match outcome {
            BeginOutcome::Launching(launch) => *launch,
            _ => panic!("an artifact-less dispatch must launch off the lock"),
        }
    }

    /// Regression (audit W3): the runtime's `prepare`/`start` used to run
    /// under the task-manager lock, starving heartbeats behind a slow image
    /// pull or volume attach. The launch now registers the allocation, lets
    /// the lock go for the runtime calls, and only then installs the process.
    /// Holding the lock for the whole runtime call below proves the launch
    /// never needs it in between.
    #[test]
    fn a_launch_runs_its_runtime_without_the_lock_and_never_twice() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        let dispatch = sleep_dispatch(21);
        let launch = {
            let mut guard = manager.lock().expect("task manager");
            let mut launch = take_launch(guard.begin_task_allocation(&dispatch)?);
            // Registered before the runtime runs: reported as this node's,
            // and neither the reconcile nor a duplicate dispatch starts it.
            assert!(guard.has_allocation(21));
            assert_eq!(guard.allocations_snapshot(), vec![21]);
            assert!(guard
                .reconcile_allocations(std::slice::from_ref(&dispatch))
                .is_empty());
            assert!(matches!(
                guard.begin_task_allocation(&dispatch)?,
                BeginOutcome::InFlight
            ));
            // The runtime call, with the lock held by someone else.
            let result = launch.run();
            assert!(result.is_ok(), "{result:?}");
            assert_eq!(guard.allocation_pid(21), None, "not installed yet");
            drop(guard);
            launch
        };
        let pid = launch.instance.pid.expect("started");
        // Settle it the way `complete_unlocked_launch` does.
        let finish = manager
            .lock()
            .expect("task manager")
            .finish_unlocked_launch(launch, Ok(()), "");
        assert!(matches!(finish, LaunchFinish::Started(_)));
        let mut guard = manager.lock().expect("task manager");
        assert_eq!(guard.allocation_pid(21), Some(pid));
        let _ = guard.reconcile_allocations(&[]);
        while guard.has_terminating() {
            guard.reap_terminating();
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }

    /// A stop that lands while the runtime is starting the task cancels the
    /// launch: the launch thread destroys what the runtime started, and the
    /// task is never installed as running.
    #[test]
    fn a_stop_during_launch_tears_the_started_task_down() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        let mut launch = take_launch(
            manager
                .lock()
                .expect("task manager")
                .begin_task_allocation(&sleep_dispatch(22))?,
        );
        launch.run()?;
        let pid = launch.instance.pid.expect("started");
        // The manager stops it before the launch settles.
        {
            let mut guard = manager.lock().expect("task manager");
            let _ = guard.reconcile_allocations(&[]);
            assert!(!guard.has_allocation(22));
            // Held back until the teardown finishes, so it cannot be
            // launched twice.
            assert!(guard
                .reconcile_allocations(&[sleep_dispatch(22)])
                .is_empty());
        }
        let finish = manager
            .lock()
            .expect("task manager")
            .finish_unlocked_launch(launch, Ok(()), "");
        let LaunchFinish::Cancelled(mut cancelled) = finish else {
            panic!("a stopped launch must come back cancelled");
        };
        // What `complete_unlocked_launch` does with it, minus the lock dance.
        cancelled.plugin.task_plugin().destroy(
            &cancelled.allocation,
            &cancelled.task,
            &mut cancelled.instance,
        )?;
        manager
            .lock()
            .expect("task manager")
            .finish_cancelled_launch(22, None);
        assert!(!pid_is_running(pid), "the started task was destroyed");
        let mut guard = manager.lock().expect("task manager");
        assert!(!guard.has_allocation(22));
        assert_eq!(
            guard.reconcile_allocations(&[sleep_dispatch(22)]).len(),
            1,
            "launchable again once the teardown finished"
        );
        Ok(())
    }

    /// The whole driver: a failed start is recorded as a failure, under the
    /// launch's own message, and leaves nothing registered.
    #[test]
    fn a_failed_unlocked_launch_is_recorded_as_a_failure() -> R<()> {
        let temp_dir = TempDir::new()?;
        let manager = Arc::new(Mutex::new(manager_in(&temp_dir)));
        let dispatch = TaskAllocation {
            task_config: json!({"command": ["/nonexistent/legion-test-binary"]}).to_string(),
            ..sleep_dispatch(23)
        };
        let result = launch_task_allocation(&manager, &dispatch);
        assert!(result.is_err());
        let mut guard = manager.lock().expect("task manager");
        assert!(!guard.has_allocation(23));
        let failures = guard.drain_failures();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].allocation_id, 23);
        Ok(())
    }

    /// An allocation the memory pressure response stopped is not relaunched
    /// while the manager still lists it (the eviction report is in flight),
    /// and the hold is dropped once the manager stops asking for it. The
    /// desired set also carries each allocation's band to the response.
    #[test]
    fn a_pressure_evicted_allocation_is_held_out_of_relaunch() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let ta = TaskAllocation {
            allocation_id: 9,
            job_id: "job".into(),
            task_name: "exec-task".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            priority_band: "low".into(),
            ..Default::default()
        };
        manager.pressure_evicted.insert(9);
        assert!(manager
            .reconcile_allocations(std::slice::from_ref(&ta))
            .is_empty());
        assert_eq!(manager.bands.get(&9), Some(&crate::structs::Band::Low));

        // The manager dropped it: the hold goes, and a later re-dispatch of
        // the same id would launch.
        manager.reconcile_allocations(&[]);
        assert!(manager.pressure_evicted.is_empty());
        assert_eq!(
            manager
                .reconcile_allocations(std::slice::from_ref(&ta))
                .len(),
            1
        );

        // Nothing running: nothing to evict.
        assert!(manager.evict_for_pressure(9).is_err());
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
                        to: None,
                        vip: None,
                        canonical_port: None,
                    },
                    crate::structs::AllocUpstream {
                        service: "cache".into(),
                        local_port: 6379,
                        balance: "least_conn".into(),
                        to: None,
                        vip: None,
                        canonical_port: None,
                    },
                ],
                vip_block: None,
                mode: None,
                ingress_tls: None,
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
            task_stop: String::new(),
            provisional: None,
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
            other => panic!(
                "artifact task must stage, got {}",
                match other {
                    BeginOutcome::Started(..) => "started",
                    BeginOutcome::Staged(..) => "staged",
                    BeginOutcome::Launching(..) => "launching",
                    BeginOutcome::InFlight => "in flight",
                    BeginOutcome::Staging(..) => unreachable!(),
                }
            ),
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
            task_stop: String::new(),
            provisional: None,
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
            _ => panic!("artifact task must stage"),
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
        manager.start_staged_allocation(&allocation, &task, staged, Gate::Off)?;

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

    /// Regression (audit W2): restore used to adopt any live pid. A recycled
    /// pid (here: the test process itself, which is alive but is not the
    /// task) must be skipped when its start time does not match the record.
    #[test]
    fn restore_skips_a_live_pid_that_started_at_a_different_time() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_path = temp_dir.path().join("worker_state.json");
        let pid = std::process::id();
        let started = process_start_time(pid).context("test process start time")?;
        let record = |id: i64, pid_start_time: Option<u64>| PersistedAllocation {
            allocation: Allocation { id, ..alloc() },
            task: TaskConfig::new("exec-task", "exec"),
            pid: Some(pid),
            pid_start_time,
            self_terminate_after_secs: None,
            volumes: Vec::new(),
            external_id: None,
            resource_assignment: None,
            stopping: None,
        };
        let store = WorkerStateStore::new(state_path.clone());
        store.save(&[
            // Same pid, different process: must not be adopted.
            record(1, Some(started - 60)),
            // Same pid, same start time: really is the task.
            record(2, Some(started)),
            // Written before start times were persisted: adopted once, on
            // liveness alone, and backfilled.
            record(3, None),
        ])?;

        let mut restarted = WorkerTaskManager::with_state_store(store.clone());
        let mut restored = restarted
            .restore_allocations()?
            .into_iter()
            .map(|(allocation, _)| allocation.id)
            .collect::<Vec<_>>();
        restored.sort_unstable();
        assert_eq!(restored, vec![2, 3]);
        assert!(store
            .load()?
            .iter()
            .all(|record| record.pid_start_time == Some(started)));
        Ok(())
    }

    /// Regression (audit W6): before a restarted worker reaches its manager,
    /// only allocations whose window the persisted silence exceeds are
    /// fenced; a long window keeps running.
    #[test]
    fn fence_persisted_allocations_kills_only_expired_windows() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_path = temp_dir.path().join("worker_state.json");
        let mut manager =
            WorkerTaskManager::with_state_store(WorkerStateStore::new(state_path.clone()));
        manager.load_builtin_plugins();
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        manager.run_allocation(&Allocation { id: 1, ..alloc() }, &task)?;
        manager.set_self_fence_window(1, Duration::from_secs(5));
        manager.run_allocation(&Allocation { id: 2, ..alloc() }, &task)?;
        manager.set_self_fence_window(2, Duration::from_secs(3600));
        // Stateless: never fenced, however long the silence.
        manager.run_allocation(&Allocation { id: 3, ..alloc() }, &task)?;
        let pids = [1, 2, 3].map(|id| manager.allocation_pid(id).expect("pid"));

        let fenced = fence_persisted_allocations(
            &WorkerStateStore::new(state_path.clone()),
            Duration::from_secs(60),
        )?;
        assert_eq!(fenced, vec![1]);
        // The fenced pid is the old process's child no longer; `manager`
        // still owns its `Child`, so reap it here to drop the zombie.
        manager.poll_task_exits();
        assert!(!pid_is_running(pids[0]), "expired stateful task is killed");
        assert!(pid_is_running(pids[1]), "long window keeps running");
        assert!(pid_is_running(pids[2]), "stateless task keeps running");

        // Idempotent: the dead pid is skipped on the next tick.
        assert!(fence_persisted_allocations(
            &WorkerStateStore::new(state_path),
            Duration::from_secs(60)
        )?
        .is_empty());
        manager.reconcile_allocations(&[]);
        while manager.has_terminating() {
            manager.reap_terminating();
            thread::sleep(Duration::from_millis(20));
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
