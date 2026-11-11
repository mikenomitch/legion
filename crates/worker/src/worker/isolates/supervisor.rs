//! The isolate supervisor: everything the worker does for isolates
//! (docs/task-runtime-isolate.md, brief item 4). It owns the node's cordon
//! host processes, prepares each isolate task whose envelope runs here
//! (module compiled, loaded and configured in every replica of its
//! cordon), decides where a run goes, keeps session isolates and their
//! checkpoints, and reports aggregates.
//!
//! Nothing here takes the task manager lock across an await, and nothing
//! per isolate goes through the claim machinery: a run is one `Invoke` on
//! the host.

use super::binary::{ImageCatalog, IsolateBinary};
use super::host::{HostLauncher, HostSpawn};
use super::modules::{ModuleArtifact, ModuleSource, ModuleWant};
use super::plan::{self, SlotPlan, TaskRoom};
use super::sessions::{
    CheckpointStore, SessionBook, SessionRecord, SessionState, StoredCheckpoint,
};
use super::stats::{Deltas, Rates, TaskFacts};
use super::tier::{self, Tier, TierInputs, TierRefusal, VmMemo};
use super::{EgressBackend, IsolateEnvelope, IsolateTaskRef, NoEgress, NoVm, VmBackend};
use crate::prelude::*;
use crate::structs::isolate::{
    IsolateLifetime, IsolateTaskConfig, ResolvedIsolate, WorkerIsolateConfigFull, ISOLATE_RUNTIME,
};
use crate::structs::isolate_host::isolate_host_client::IsolateHostClient;
use crate::structs::isolate_host::{self as host, HelloReply, StatsReply};
use crate::structs::{
    IsolateHostStats, IsolateTaskStats, RunDryRun, RunSessionRequest, RunSessionResponse,
    TaskConfig,
};
use crate::worker::task_manager::session_runs::{RunCall, RunRegistry};
use crate::worker::task_manager::WorkerTaskManager;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{RwLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tonic::transport::Channel;
use tonic::{Code, Status};

/// How long a host has to answer `Hello`.
const HELLO_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a stats poll waits for one host.
const STATS_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a run waits for its task's module by default.
const DEFAULT_READY_WAIT: Duration = Duration::from_secs(60);
/// A failed preparation is tried again after this long.
const RETRY_FAILED_AFTER: Duration = Duration::from_secs(30);
/// The longest wait between host restarts.
const MAX_RESTART_BACKOFF: Duration = Duration::from_secs(30);
/// A warm tenant instance idle this long is dropped (`TaskSpec`).
const TENANT_IDLE: Duration = Duration::from_secs(60);
/// Bytes one tenant or session may keep in `legion:isolate/state`.
pub(crate) const STATE_QUOTA_BYTES: u64 = 1 << 20;
/// How long a drain gives in-flight invocations at worker shutdown.
const DRAIN_GRACE: Duration = Duration::from_secs(5);

pub(crate) type TaskKey = (String, String);

/// Whether a task can take work.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Readiness {
    Preparing,
    Ready,
    Failed(String),
}

struct TaskEntry {
    job_id: String,
    task_name: String,
    cordon: String,
    envelope_allocation_id: i64,
    task: TaskConfig,
    resolved: ResolvedIsolate,
    vm_task: Option<String>,
    room: TaskRoom,
    /// The envelope's memory, MiB (the cordon cgroup's share).
    envelope_memory_mib: f64,
    /// The task spec as the envelope carried it; a change re-prepares.
    fingerprint: String,
    readiness: watch::Sender<Readiness>,
    attempted: Instant,
    module: Option<ModuleArtifact>,
    entry: Option<host::File>,
    hibernate_after: Duration,
    keep_for: Duration,
    /// The image's modules, for the pre-flight scan.
    image_modules: Vec<String>,
}

impl TaskEntry {
    fn problem(&self) -> String {
        match &*self.readiness.borrow() {
            Readiness::Failed(problem) => problem.clone(),
            Readiness::Preparing => "the module is still compiling".into(),
            Readiness::Ready => String::new(),
        }
    }

    fn image(&self) -> Option<&str> {
        self.resolved.image.as_ref().map(|image| image.facts.name)
    }
}

/// One live host process.
struct HostConn {
    client: IsolateHostClient<Channel>,
    pid: u32,
    hello: HelloReply,
    confinement: Vec<String>,
    started_unix_ms: i64,
    kill: Box<dyn Fn() + Send + Sync>,
    exited: watch::Receiver<Option<String>>,
    cgroup: Option<PathBuf>,
}

impl HostConn {
    fn alive(&self) -> bool {
        self.exited.borrow().is_none()
    }
}

/// One replica of a cordon's host.
pub struct Replica {
    cordon: String,
    index: u32,
    conn: Mutex<Option<Arc<HostConn>>>,
    restarts: AtomicU32,
    problem: Mutex<String>,
    retired: AtomicBool,
    /// A restarted host is live but its tasks are not reloaded yet: calls
    /// wait (answer UNAVAILABLE, retry) instead of reaching a host that
    /// does not know their task.
    reloading: AtomicBool,
    last_stats: Mutex<Option<StatsReply>>,
}

impl Replica {
    /// The connection calls may use: live, and done reloading.
    fn serving(&self) -> Option<Arc<HostConn>> {
        if self.reloading.load(Ordering::Acquire) {
            return None;
        }
        self.conn()
    }

    fn conn(&self) -> Option<Arc<HostConn>> {
        self.conn
            .lock()
            .ok()
            .and_then(|conn| conn.clone())
            .filter(|conn| conn.alive())
    }

    fn set_problem(&self, problem: impl Into<String>) {
        if let Ok(mut slot) = self.problem.lock() {
            *slot = problem.into();
        }
    }

    fn problem(&self) -> String {
        self.problem.lock().map(|p| p.clone()).unwrap_or_default()
    }
}

#[derive(Default)]
struct Cordon {
    replicas: Vec<Arc<Replica>>,
}

#[derive(Default)]
struct State {
    tasks: BTreeMap<TaskKey, TaskEntry>,
    cordons: BTreeMap<String, Cordon>,
    vm_runs: HashMap<TaskKey, u64>,
}

/// What [`IsolateSupervisor::new`] is built from.
pub struct SupervisorParts {
    pub node_id: String,
    pub config: WorkerIsolateConfigFull,
    pub binary: IsolateBinary,
    pub images: Option<ImageCatalog>,
    pub launcher: Arc<dyn HostLauncher>,
    pub modules: Arc<dyn ModuleSource>,
    /// Directories every host may read (compiled artifacts).
    pub read_dirs: Vec<PathBuf>,
    /// The session book's file; `None` keeps it in memory.
    pub book_path: Option<PathBuf>,
    pub checkpoints: CheckpointStore,
    pub run_registry: Arc<RunRegistry>,
    pub gate: Arc<crate::worker::runtime_state::RuntimeGate>,
    /// `edgeBoot.queueTimeout`: how long an invocation waits for room.
    pub queue_timeout: Duration,
    pub max_map_count: u64,
    /// Why this node cannot page isolates out, or `None` when it can.
    pub paging_unavailable: Option<String>,
}

/// A session isolate start's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedIsolate {
    pub session_id: String,
    pub deduplicated: bool,
}

pub struct IsolateSupervisor {
    me: Weak<IsolateSupervisor>,
    node_id: String,
    config: WorkerIsolateConfigFull,
    binary: IsolateBinary,
    images: Option<ImageCatalog>,
    launcher: Arc<dyn HostLauncher>,
    modules: Arc<dyn ModuleSource>,
    read_dirs: Vec<PathBuf>,
    checkpoints: CheckpointStore,
    run_registry: Arc<RunRegistry>,
    gate: Arc<crate::worker::runtime_state::RuntimeGate>,
    queue_timeout: Duration,
    slots: SlotPlan,
    paging_unavailable: Option<String>,
    task_manager: Mutex<Option<Weak<Mutex<WorkerTaskManager>>>>,
    egress: RwLock<Arc<dyn EgressBackend>>,
    vm: RwLock<Arc<dyn VmBackend>>,
    state: Mutex<State>,
    sessions: Mutex<SessionBook>,
    memo: Mutex<VmMemo>,
    cordon_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    stopping: AtomicBool,
    round_robin: AtomicUsize,
    rates: Mutex<Rates>,
    deltas: Mutex<Deltas>,
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    /// Where envelope socket dirs are, for ingress.
    alloc_root: Mutex<Option<PathBuf>>,
    /// Ingress sockets served per task (dropped to stop serving).
    ingress: Mutex<HashMap<TaskKey, Vec<super::ingress::IngressServer>>>,
    /// Where guest output goes: the envelope allocations' task logs.
    guest_logs: RwLock<Option<super::guest_logs::GuestLogs>>,
    /// Invocations in flight: (cordon, job, task, invocation id) and the
    /// tenant the worker gave each. A host may act as a request or tenant
    /// isolate (child VMs) only while the worker has such an invocation
    /// open with it.
    inflight: Mutex<HashMap<InflightKey, String>>,
    /// Invocations that ended in the last [`RECENT_INVOCATION_GRACE`]: a
    /// host forwards an invocation's log lines after its reply, so `Log`
    /// accepts them a little after the call ended.
    recent: Mutex<RecentInvocations>,
}

/// (cordon, job, task, invocation id).
type InflightKey = (String, String, String, String);

/// How long after an invocation ends its host may still log for it.
const RECENT_INVOCATION_GRACE: Duration = Duration::from_secs(30);
/// The most ended invocations remembered for that (oldest dropped first).
const RECENT_INVOCATIONS_MAX: usize = 16_384;

/// Ended invocations, oldest first, with the tenant each ran for.
#[derive(Default)]
struct RecentInvocations {
    order: std::collections::VecDeque<(Instant, InflightKey)>,
    tenants: HashMap<InflightKey, (Instant, String)>,
}

impl RecentInvocations {
    fn prune(&mut self, now: Instant) {
        while let Some((at, _)) = self.order.front() {
            let expired = now.duration_since(*at) > RECENT_INVOCATION_GRACE;
            if !expired && self.order.len() <= RECENT_INVOCATIONS_MAX {
                break;
            }
            let (at, key) = self.order.pop_front().expect("a front entry");
            // An id that ended twice (a repeated derived run id) keeps its
            // newer entry.
            if self.tenants.get(&key).is_some_and(|(when, _)| *when == at) {
                self.tenants.remove(&key);
            }
        }
    }

    fn push(&mut self, key: InflightKey, tenant: String) {
        let now = Instant::now();
        self.tenants.insert(key.clone(), (now, tenant));
        self.order.push_back((now, key));
        self.prune(now);
    }

    fn tenant(&mut self, key: &InflightKey) -> Option<String> {
        self.prune(Instant::now());
        self.tenants.get(key).map(|(_, tenant)| tenant.clone())
    }
}

/// Removes an in-flight invocation when the call ends, however it ends.
pub(crate) struct Inflight<'a> {
    supervisor: &'a IsolateSupervisor,
    key: InflightKey,
}

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        let tenant = lock(&self.supervisor.inflight).remove(&self.key);
        if let Some(tenant) = tenant {
            lock(&self.supervisor.recent).push(self.key.clone(), tenant);
        }
    }
}

impl std::fmt::Debug for IsolateSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsolateSupervisor")
            .field("node_id", &self.node_id)
            .field("binary", &self.binary.path)
            .finish()
    }
}

fn status_text(status: &Status) -> String {
    status.message().to_string()
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

impl IsolateSupervisor {
    pub fn new(parts: SupervisorParts) -> Arc<Self> {
        let slots = plan::plan_slots(
            parts.config.slots_per_host,
            parts.config.layout,
            parts.max_map_count,
        );
        if let Some(clamped) = slots.clamped.as_deref() {
            info!("{clamped}");
        }
        if let Some(why) = parts.paging_unavailable.as_deref() {
            info!("Isolate paging out is off on this node: {why}");
        }
        Arc::new_cyclic(|me| IsolateSupervisor {
            me: me.clone(),
            node_id: parts.node_id,
            config: parts.config,
            binary: parts.binary,
            images: parts.images,
            launcher: parts.launcher,
            modules: parts.modules,
            read_dirs: parts.read_dirs,
            checkpoints: parts.checkpoints,
            run_registry: parts.run_registry,
            gate: parts.gate,
            queue_timeout: parts.queue_timeout,
            slots,
            paging_unavailable: parts.paging_unavailable,
            task_manager: Mutex::new(None),
            egress: RwLock::new(Arc::new(NoEgress)),
            vm: RwLock::new(Arc::new(NoVm)),
            state: Mutex::new(State::default()),
            sessions: Mutex::new(SessionBook::open(parts.book_path)),
            memo: Mutex::new(VmMemo::default()),
            cordon_locks: Mutex::new(HashMap::new()),
            stopping: AtomicBool::new(false),
            round_robin: AtomicUsize::new(0),
            rates: Mutex::new(Rates::default()),
            deltas: Mutex::new(Deltas::default()),
            runtime: Mutex::new(tokio::runtime::Handle::try_current().ok()),
            alloc_root: Mutex::new(None),
            ingress: Mutex::new(HashMap::new()),
            guest_logs: RwLock::new(None),
            inflight: Mutex::new(Default::default()),
            recent: Mutex::new(Default::default()),
        })
    }

    fn arc(&self) -> Arc<Self> {
        self.me
            .upgrade()
            .expect("the supervisor is alive while it runs")
    }

    /// The task manager this supervisor serves (for VM runs and task
    /// discovery).
    pub fn attach(&self, task_manager: &Arc<Mutex<WorkerTaskManager>>) {
        *lock(&self.task_manager) = Some(Arc::downgrade(task_manager));
    }

    /// Where the node keeps allocation directories: ingress sockets go in
    /// the envelope's.
    /// Guest output (`IsolateWorker.Log`) is appended to the envelope
    /// allocation's task log under `logs_root` (the worker's
    /// `<state dir>/logs`).
    pub fn set_log_root(&self, logs_root: PathBuf) {
        if let Ok(mut slot) = self.guest_logs.write() {
            *slot = Some(super::guest_logs::GuestLogs::new(logs_root));
        }
    }

    /// Where guest output is written, when it is.
    pub fn guest_logs(&self) -> Option<super::guest_logs::GuestLogs> {
        self.guest_logs.read().ok().and_then(|slot| slot.clone())
    }

    pub fn set_alloc_root(&self, alloc_root: PathBuf) {
        *lock(&self.alloc_root) = Some(alloc_root);
    }

    fn task_manager(&self) -> Option<Arc<Mutex<WorkerTaskManager>>> {
        lock(&self.task_manager).as_ref().and_then(Weak::upgrade)
    }

    /// Wire the datapath's outbound HTTP for isolates.
    pub fn set_egress_backend(&self, backend: Arc<dyn EgressBackend>) {
        *self.egress.write().unwrap_or_else(|p| p.into_inner()) = backend;
    }

    /// Wire child VMs for isolates.
    pub fn set_vm_backend(&self, backend: Arc<dyn VmBackend>) {
        *self.vm.write().unwrap_or_else(|p| p.into_inner()) = backend;
    }

    pub fn egress_backend(&self) -> Arc<dyn EgressBackend> {
        self.egress
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn vm_backend(&self) -> Arc<dyn VmBackend> {
        self.vm.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Slots per host after the `vm.max_map_count` clamp.
    pub fn slots_per_host(&self) -> u32 {
        self.slots.slots
    }

    // -----------------------------------------------------------------
    // Task discovery
    // -----------------------------------------------------------------

    /// Bring the supervisor's tasks in line with the envelopes the task
    /// manager holds.
    pub fn sync_from_task_manager(&self) {
        let Some(task_manager) = self.task_manager() else {
            return;
        };
        let envelopes = lock(&task_manager).isolate_envelopes();
        self.sync(envelopes);
    }

    /// Bring the supervisor's tasks in line with `envelopes`: prepare new
    /// or changed tasks, reconfigure resized ones, remove departed ones.
    /// Work is spawned; this returns at once.
    pub fn sync(&self, envelopes: Vec<IsolateEnvelope>) {
        let mut prepare = Vec::new();
        let mut reconfigure = Vec::new();
        let mut removed = Vec::new();
        // Cordons whose envelopes grew or shrank: their hosts' cgroup
        // `memory.max` follows (a new or departed task changes it too).
        let mut resized: Vec<String> = Vec::new();
        {
            let mut state = lock(&self.state);
            let mut seen = std::collections::HashSet::new();
            for envelope in envelopes {
                let task = envelope.spec.task;
                let key = (envelope.job_id.clone(), task.name.clone());
                seen.insert(key.clone());
                let fingerprint = serde_json::to_string(&task).unwrap_or_default();
                let room = plan::task_room(&envelope.room, &task);
                let envelope_memory_mib = envelope.room.get("memory").copied().unwrap_or(0.0);
                if let Some(entry) = state.tasks.get_mut(&key) {
                    entry.envelope_allocation_id = envelope.allocation_id;
                    if entry.envelope_memory_mib != envelope_memory_mib
                        && !resized.contains(&entry.cordon)
                    {
                        resized.push(entry.cordon.clone());
                    }
                    entry.envelope_memory_mib = envelope_memory_mib;
                    if entry.fingerprint != fingerprint {
                        match build_entry(&envelope.job_id, task, room, &self.images) {
                            Ok(fresh) => {
                                let readiness = entry.readiness.clone();
                                *entry = TaskEntry {
                                    readiness,
                                    envelope_allocation_id: envelope.allocation_id,
                                    envelope_memory_mib,
                                    fingerprint,
                                    ..fresh
                                };
                                entry.readiness.send_replace(Readiness::Preparing);
                                prepare.push(key);
                            }
                            Err(problem) => {
                                entry.fingerprint = fingerprint;
                                entry.readiness.send_replace(Readiness::Failed(problem));
                            }
                        }
                        continue;
                    }
                    if entry.room != room {
                        entry.room = room;
                        reconfigure.push(key);
                        continue;
                    }
                    if matches!(&*entry.readiness.borrow(), Readiness::Failed(_))
                        && entry.attempted.elapsed() > RETRY_FAILED_AFTER
                    {
                        entry.attempted = Instant::now();
                        entry.readiness.send_replace(Readiness::Preparing);
                        prepare.push(key);
                    }
                    continue;
                }
                let (readiness, _) = watch::channel(Readiness::Preparing);
                let entry = match build_entry(&envelope.job_id, task.clone(), room, &self.images) {
                    Ok(entry) => TaskEntry {
                        envelope_allocation_id: envelope.allocation_id,
                        envelope_memory_mib,
                        fingerprint,
                        readiness,
                        ..entry
                    },
                    Err(problem) => {
                        warn!(job_id = %envelope.job_id, task = %task.name, %problem, "Isolate task cannot run on this node");
                        readiness.send_replace(Readiness::Failed(problem));
                        let config = IsolateTaskConfig::of(&task)
                            .ok()
                            .flatten()
                            .unwrap_or_default();
                        TaskEntry {
                            job_id: envelope.job_id.clone(),
                            task_name: task.name.clone(),
                            cordon: crate::structs::isolate::cordon_name(&envelope.job_id, &config),
                            envelope_allocation_id: envelope.allocation_id,
                            resolved: placeholder_resolved(),
                            vm_task: None,
                            room,
                            envelope_memory_mib,
                            fingerprint,
                            readiness,
                            attempted: Instant::now(),
                            module: None,
                            entry: None,
                            hibernate_after: Duration::ZERO,
                            keep_for: Duration::ZERO,
                            image_modules: Vec::new(),
                            task,
                        }
                    }
                };
                let failed = matches!(&*entry.readiness.borrow(), Readiness::Failed(_));
                if !resized.contains(&entry.cordon) {
                    resized.push(entry.cordon.clone());
                }
                state.tasks.insert(key.clone(), entry);
                if !failed {
                    prepare.push(key);
                }
            }
            let gone: Vec<TaskKey> = state
                .tasks
                .keys()
                .filter(|key| !seen.contains(*key))
                .cloned()
                .collect();
            for key in gone {
                if let Some(entry) = state.tasks.remove(&key) {
                    if !resized.contains(&entry.cordon) {
                        resized.push(entry.cordon.clone());
                    }
                    removed.push((key, entry.cordon));
                }
            }
        }
        if prepare.is_empty() && reconfigure.is_empty() && removed.is_empty() && resized.is_empty()
        {
            return;
        }
        let me = self.arc();
        let spawned = self.spawn(async move {
            for key in prepare {
                let me = me.clone();
                tokio::spawn(async move { me.prepare(key).await });
            }
            for key in reconfigure {
                me.ensure_cordon_for(&key).await.ok();
                let _ = me.configure_everywhere(&key).await;
            }
            for (key, cordon) in removed {
                me.remove_task(key, &cordon).await;
            }
            for cordon in resized {
                me.resize_cordon_cgroups(&cordon).await;
            }
        });
        if !spawned {
            warn!("No async runtime to prepare isolate tasks on");
        }
    }

    fn spawn<F>(&self, future: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(future);
            return true;
        }
        if let Some(handle) = lock(&self.runtime).clone() {
            handle.spawn(future);
            return true;
        }
        false
    }

    /// Fetch, compile, load and configure one task.
    async fn prepare(self: Arc<Self>, key: TaskKey) {
        let started = Instant::now();
        let job_id = key.0.clone();
        let task_name = key.1.clone();
        let Some((want, task, allocation_id, entry_ref)) = ({
            let mut state = lock(&self.state);
            state.tasks.get_mut(&key).map(|entry| {
                entry.attempted = Instant::now();
                let want = match (&entry.resolved.module, &entry.task.config.get("image")) {
                    (Some(module), _) => ModuleWant::Component(module.clone()),
                    (None, Some(image)) => {
                        ModuleWant::Image(image.as_str().unwrap_or_default().to_string())
                    }
                    (None, None) => ModuleWant::Image(entry.image().unwrap_or_default().into()),
                };
                (
                    want,
                    entry.task.clone(),
                    entry.envelope_allocation_id,
                    entry.resolved.entry.clone(),
                )
            })
        }) else {
            return;
        };
        let modules = self.modules.clone();
        let job = job_id.clone();
        let built = tokio::task::spawn_blocking(move || {
            let artifact = modules.compiled(&want)?;
            let entry = match entry_ref.as_deref() {
                Some(entry) => {
                    let (name, content) = modules.entry_file(&job, &task, allocation_id, entry)?;
                    Some(host::File {
                        path: name,
                        content,
                        ..Default::default()
                    })
                }
                None => None,
            };
            Ok::<_, String>((artifact, entry))
        })
        .await
        .unwrap_or_else(|err| Err(format!("preparing the module panicked: {err}")));
        let (artifact, entry) = match built {
            Ok(built) => built,
            Err(problem) => {
                self.fail_task(&key, problem);
                return;
            }
        };
        {
            let mut state = lock(&self.state);
            let Some(task) = state.tasks.get_mut(&key) else {
                return;
            };
            task.module = Some(artifact);
            task.entry = entry;
        }
        if let Err(problem) = self.ensure_cordon_for(&key).await {
            self.fail_task(&key, problem);
            return;
        }
        if let Err(problem) = self.configure_everywhere(&key).await {
            self.fail_task(&key, problem);
            return;
        }
        if let Some(task) = lock(&self.state).tasks.get(&key) {
            task.readiness.send_replace(Readiness::Ready);
        }
        self.serve_ingress(&key).await;
        info!(
            job_id = %job_id,
            task = %task_name,
            millis = started.elapsed().as_millis() as u64,
            "Isolate task ready on this node"
        );
    }

    /// Serve the task's ingress ports on its envelope's sockets, where the
    /// node's ingress listener dials.
    async fn serve_ingress(&self, key: &TaskKey) {
        let Some(alloc_root) = lock(&self.alloc_root).clone() else {
            return;
        };
        let Some((allocation, labels)) = lock(&self.state).tasks.get(key).map(|task| {
            (
                task.envelope_allocation_id,
                task.task
                    .network
                    .as_ref()
                    .map(|network| {
                        network
                            .ports
                            .iter()
                            .map(|port| port.name.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
            )
        }) else {
            return;
        };
        if labels.is_empty() {
            return;
        }
        let max_body = self.config.max_body_bytes().unwrap_or(16 << 20);
        match super::ingress::serve_ports(
            &alloc_root,
            allocation,
            &labels,
            &key.0,
            &key.1,
            max_body,
            self.arc(),
        )
        .await
        {
            Ok(servers) => {
                lock(&self.ingress).insert(key.clone(), servers);
            }
            Err(err) => {
                warn!(job_id = %key.0, task = %key.1, error = %format!("{err:#}"), "Isolate ingress not served")
            }
        }
    }

    fn fail_task(&self, key: &TaskKey, problem: String) {
        warn!(job_id = %key.0, task = %key.1, %problem, "Isolate task cannot run on this node");
        if let Some(task) = lock(&self.state).tasks.get(key) {
            task.readiness.send_replace(Readiness::Failed(problem));
        }
    }

    fn cordon_lock(&self, cordon: &str) -> Arc<tokio::sync::Mutex<()>> {
        lock(&self.cordon_locks)
            .entry(cordon.to_string())
            .or_default()
            .clone()
    }

    /// Make sure the task's cordon runs as many replicas as its tasks need,
    /// and that the first is up.
    async fn ensure_cordon_for(&self, key: &TaskKey) -> Result<(), String> {
        let Some(cordon) = lock(&self.state).tasks.get(key).map(|t| t.cordon.clone()) else {
            return Err("the task left this node".into());
        };
        let guard = self.cordon_lock(&cordon);
        let _held = guard.lock().await;
        let (needed, existing) = {
            let state = lock(&self.state);
            let resident: u64 = state
                .tasks
                .values()
                .filter(|task| task.cordon == cordon)
                .map(|task| u64::from(task.room.max_resident))
                .sum();
            let existing = state
                .cordons
                .get(&cordon)
                .map(|c| c.replicas.len() as u32)
                .unwrap_or(0);
            (plan::replicas_needed(resident, self.slots.slots), existing)
        };
        let mut started = Vec::new();
        for index in existing..needed {
            let replica = Arc::new(Replica {
                cordon: cordon.clone(),
                index,
                conn: Mutex::new(None),
                restarts: AtomicU32::new(0),
                problem: Mutex::new(String::new()),
                retired: AtomicBool::new(false),
                reloading: AtomicBool::new(false),
                last_stats: Mutex::new(None),
            });
            lock(&self.state)
                .cordons
                .entry(cordon.clone())
                .or_default()
                .replicas
                .push(replica.clone());
            if let Err(err) = self.start_replica(&replica).await {
                let problem =
                    format!("the isolate host for cordon {cordon} did not start: {err:#}");
                replica.set_problem(problem.clone());
                self.spawn_restart(replica.clone());
                if index == 0 {
                    return Err(problem);
                }
            }
            started.push(replica);
        }
        // A new replica changes every task's split: reconfigure the
        // cordon's other tasks (this task is configured by the caller).
        if existing > 0 && needed > existing {
            let others: Vec<TaskKey> = lock(&self.state)
                .tasks
                .iter()
                .filter(|(other, task)| {
                    *other != key && task.cordon == cordon && task.module.is_some()
                })
                .map(|(other, _)| other.clone())
                .collect();
            drop(_held);
            for other in others {
                let _ = self.configure_everywhere(&other).await;
            }
        }
        let first_up = lock(&self.state)
            .cordons
            .get(&cordon)
            .and_then(|c| c.replicas.first().cloned())
            .is_some_and(|replica| replica.conn().is_some());
        if first_up {
            Ok(())
        } else {
            Err(lock(&self.state)
                .cordons
                .get(&cordon)
                .and_then(|c| c.replicas.first().map(|r| r.problem()))
                .filter(|problem| !problem.is_empty())
                .unwrap_or_else(|| format!("the isolate host for cordon {cordon} is not running")))
        }
    }

    /// The sum of the cordon's envelopes, for its hosts' cgroup.
    fn cordon_memory_bytes(&self, cordon: &str) -> Option<u64> {
        let state = lock(&self.state);
        let replicas = state
            .cordons
            .get(cordon)
            .map(|c| c.replicas.len().max(1))
            .unwrap_or(1) as f64;
        let mib: f64 = state
            .tasks
            .values()
            .filter(|task| task.cordon == cordon)
            .map(|task| task.envelope_memory_mib)
            .sum();
        // Each replica may hold the whole of it: the split moves as load
        // does, and memory.max is a ceiling, not a reservation.
        let _ = replicas;
        (mib > 0.0).then_some((mib * 1024.0 * 1024.0) as u64)
    }

    /// Set each live replica's cgroup `memory.max` to the cordon's envelope
    /// sum now (an envelope grew or shrank, a task came or went). Hosts
    /// without a cgroup (enforcement off) are skipped; a failed write is
    /// logged and the old ceiling stays.
    async fn resize_cordon_cgroups(&self, cordon: &str) {
        let Some(bytes) = self.cordon_memory_bytes(cordon) else {
            return;
        };
        for replica in self.replicas(cordon) {
            let Some(cgroup) = replica.conn().and_then(|conn| conn.cgroup.clone()) else {
                continue;
            };
            let path = cgroup.join("memory.max");
            let written =
                tokio::task::spawn_blocking(move || std::fs::write(&path, bytes.to_string())).await;
            match written {
                Ok(Ok(())) => {
                    debug!(%cordon, replica = replica.index, bytes, "Isolate host memory.max follows its envelopes")
                }
                Ok(Err(err)) => {
                    warn!(%cordon, replica = replica.index, error = %err, "Could not resize the isolate host's cgroup")
                }
                Err(_) => {}
            }
        }
    }

    /// Start one replica's host process and handshake with it.
    async fn start_replica(&self, replica: &Arc<Replica>) -> R<()> {
        let spawn = HostSpawn {
            cordon: replica.cordon.clone(),
            replica: replica.index,
            config: plan::host_config(
                &self.config,
                &replica.cordon,
                self.slots.slots,
                &self.read_dirs,
            ),
            memory_max_bytes: self.cordon_memory_bytes(&replica.cordon),
        };
        let worker = super::service::WorkerService::new(
            self.me.clone(),
            &replica.cordon,
            super::host::HostCallLimits::for_host(&spawn.config),
        );
        let launched = self.launcher.launch(&spawn, worker).await?;
        let mut client = launched.client.clone();
        let hello =
            match tokio::time::timeout(HELLO_TIMEOUT, client.hello(host::HelloRequest {})).await {
                Ok(Ok(hello)) => hello.into_inner(),
                Ok(Err(status)) => {
                    (launched.kill)();
                    bail!("its Hello failed: {}", status.message());
                }
                Err(_) => {
                    (launched.kill)();
                    bail!(
                        "it did not answer Hello within {}s",
                        HELLO_TIMEOUT.as_secs()
                    );
                }
            };
        if hello.protocol_version != host::PROTOCOL_VERSION {
            (launched.kill)();
            bail!(
                "it speaks protocol {}, this worker {}",
                hello.protocol_version,
                host::PROTOCOL_VERSION
            );
        }
        let (applied, missing) = plan::confinement_layers(hello.confinement.as_ref());
        if self.config.confine == crate::structs::isolate::ConfineMode::Enforce
            && !missing.is_empty()
        {
            (launched.kill)();
            bail!(
                "it could not confine itself ({} did not apply) and isolate.confine = \"enforce\"; \
                 run the worker where the kernel offers them, or set isolate.confine = \
                 \"best-effort\"",
                missing.join(", ")
            );
        }
        if !missing.is_empty() {
            let notes = hello
                .confinement
                .as_ref()
                .map(|c| c.notes.join("; "))
                .unwrap_or_default();
            warn!(cordon = %replica.cordon, missing = %missing.join(","), %notes, confine = ?self.config.confine, "Isolate host runs without every confinement layer");
        }
        let conn = Arc::new(HostConn {
            client: launched.client,
            pid: launched.pid,
            confinement: applied,
            started_unix_ms: crate::util::now_unix_ms(),
            kill: launched.kill,
            exited: launched.exited.clone(),
            cgroup: launched.cgroup,
            hello,
        });
        *lock(&replica.conn) = Some(conn.clone());
        replica.set_problem(self.slots.clamped.clone().unwrap_or_default());
        info!(cordon = %replica.cordon, replica = replica.index, pid = conn.pid, slots = self.slots.slots, "Isolate host started");
        // Watch it.
        let me = self.arc();
        let watched = replica.clone();
        let mut exited = launched.exited;
        tokio::spawn(async move {
            let why = loop {
                if let Some(why) = exited.borrow().clone() {
                    break why;
                }
                if exited.changed().await.is_err() {
                    break "its exit could not be observed".to_string();
                }
            };
            me.on_host_exit(watched, conn.pid, why).await;
        });
        self.reopen_sessions(replica).await;
        Ok(())
    }

    async fn on_host_exit(self: Arc<Self>, replica: Arc<Replica>, pid: u32, why: String) {
        {
            let mut conn = lock(&replica.conn);
            if conn.as_ref().is_some_and(|c| c.pid == pid) {
                *conn = None;
            }
        }
        if self.stopping.load(Ordering::Acquire) || replica.retired.load(Ordering::Acquire) {
            return;
        }
        replica.restarts.fetch_add(1, Ordering::AcqRel);
        let problem = format!(
            "the isolate host for cordon {} {why}; restarting",
            replica.cordon
        );
        warn!(cordon = %replica.cordon, replica = replica.index, pid, "{problem}");
        replica.set_problem(problem);
        self.spawn_restart(replica);
    }

    /// Restart a replica with backoff until it runs, then load every task
    /// of its cordon into it again.
    fn spawn_restart(&self, replica: Arc<Replica>) {
        let me = self.arc();
        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                let delay = Duration::from_millis(200u64.saturating_mul(1 << attempt.min(8)))
                    .min(MAX_RESTART_BACKOFF);
                tokio::time::sleep(delay).await;
                if me.stopping.load(Ordering::Acquire) || replica.retired.load(Ordering::Acquire) {
                    return;
                }
                if replica.conn().is_some() {
                    return;
                }
                replica.reloading.store(true, Ordering::Release);
                match me.start_replica(&replica).await {
                    Ok(()) => {
                        let tasks: Vec<TaskKey> = lock(&me.state)
                            .tasks
                            .iter()
                            .filter(|(_, task)| {
                                task.cordon == replica.cordon && task.module.is_some()
                            })
                            .map(|(key, _)| key.clone())
                            .collect();
                        let replicas = me.replica_count(&replica.cordon);
                        for key in tasks {
                            if let Err(problem) = me.configure_on(&replica, &key, replicas).await {
                                warn!(job_id = %key.0, task = %key.1, %problem, "Could not reload an isolate task into its restarted host");
                            }
                        }
                        replica.reloading.store(false, Ordering::Release);
                        return;
                    }
                    Err(err) => {
                        attempt += 1;
                        let problem = format!(
                            "the isolate host for cordon {} did not restart: {err:#}",
                            replica.cordon
                        );
                        warn!(cordon = %replica.cordon, attempt, "{problem}");
                        replica.set_problem(problem);
                    }
                }
            }
        });
    }

    fn replica_count(&self, cordon: &str) -> u32 {
        lock(&self.state)
            .cordons
            .get(cordon)
            .map(|c| c.replicas.len() as u32)
            .unwrap_or(1)
    }

    fn replicas(&self, cordon: &str) -> Vec<Arc<Replica>> {
        lock(&self.state)
            .cordons
            .get(cordon)
            .map(|c| c.replicas.clone())
            .unwrap_or_default()
    }

    /// The `TaskSpec` for one replica.
    fn task_spec(&self, task: &TaskEntry, replicas: u32, index: u32) -> host::TaskSpec {
        let resolved = &task.resolved;
        let lifetime = match resolved.lifetime {
            IsolateLifetime::Request => host::Lifetime::Request,
            IsolateLifetime::Tenant => host::Lifetime::Tenant,
            IsolateLifetime::Session => host::Lifetime::Session,
        };
        host::TaskSpec {
            job_id: task.job_id.clone(),
            task_name: task.task_name.clone(),
            module_id: task
                .module
                .as_ref()
                .map(|m| m.module_id.clone())
                .unwrap_or_default(),
            lifetime: lifetime as i32,
            limits: Some(host::Limits {
                cpu_micros: resolved.cpu.as_micros() as u64,
                wall_micros: resolved.wall.as_micros() as u64,
                output_bytes: resolved.output,
            }),
            memory_max_bytes: crate::structs::isolate::memory_max_mib(&task.task) << 20,
            memory_reservation_bytes: crate::structs::isolate::reservation_mib(&task.task) << 20,
            env: resolved.env.clone().into_iter().collect(),
            cpu_weight: u32::try_from(plan::task_cpu_mhz(&task.task)).unwrap_or(u32::MAX),
            max_resident: plan::split(task.room.max_resident, replicas, index),
            max_active: plan::split(task.room.max_active, replicas, index).max(1),
            queue_timeout_millis: u32::try_from(self.queue_timeout.as_millis()).unwrap_or(u32::MAX),
            has_vm: task.vm_task.is_some(),
            egress: task.task.connect.is_some() || task.task.network.is_some(),
            hibernate_after_millis: task.hibernate_after.as_millis() as u64,
            tenant_idle_millis: TENANT_IDLE.as_millis() as u64,
            state_quota_bytes: STATE_QUOTA_BYTES,
            entry: task.entry.clone(),
        }
    }

    /// Load the task's module into one replica and configure the task.
    async fn configure_on(
        &self,
        replica: &Replica,
        key: &TaskKey,
        replicas: u32,
    ) -> Result<(), String> {
        let conn = replica.conn().ok_or_else(|| {
            format!(
                "the isolate host for cordon {} is not running",
                replica.cordon
            )
        })?;
        let (module, spec) = {
            let state = lock(&self.state);
            let task = state.tasks.get(key).ok_or("the task left this node")?;
            let module = task
                .module
                .clone()
                .ok_or("the module is not compiled yet")?;
            (module, self.task_spec(task, replicas, replica.index))
        };
        // Canonical: in the host's mount root only a directory's canonical
        // path exists.
        let artifact = std::fs::canonicalize(&module.path).unwrap_or(module.path.clone());
        let mut client = conn.client.clone();
        client
            .load_module(host::LoadModuleRequest {
                module_id: module.module_id.clone(),
                artifact_path: artifact.display().to_string(),
            })
            .await
            .map_err(|status| {
                format!("the isolate host refused the module: {}", status.message())
            })?;
        client
            .configure_task(spec)
            .await
            .map_err(|status| format!("the isolate host refused the task: {}", status.message()))?;
        Ok(())
    }

    /// Configure a task on every replica of its cordon.
    async fn configure_everywhere(&self, key: &TaskKey) -> Result<(), String> {
        let Some(cordon) = lock(&self.state).tasks.get(key).map(|t| t.cordon.clone()) else {
            return Ok(());
        };
        let replicas = self.replicas(&cordon);
        let count = replicas.len() as u32;
        let mut first_error = None;
        for replica in replicas {
            if replica.conn().is_none() {
                // Restarting: the restart loads it.
                continue;
            }
            if let Err(problem) = self.configure_on(&replica, key, count).await {
                warn!(job_id = %key.0, task = %key.1, replica = replica.index, %problem, "Isolate task not configured in a host replica");
                first_error.get_or_insert(problem);
            }
        }
        match first_error {
            Some(problem) => Err(problem),
            None => Ok(()),
        }
    }

    /// A task left the node: remove it from its hosts, end its session
    /// isolates, and stop the cordon's hosts when it was the last.
    async fn remove_task(&self, key: TaskKey, cordon: &str) {
        info!(job_id = %key.0, task = %key.1, "Isolate task left this node");
        lock(&self.ingress).remove(&key);
        for replica in self.replicas(cordon) {
            if let Some(conn) = replica.conn() {
                let mut client = conn.client.clone();
                let _ = client
                    .remove_task(host::RemoveTaskRequest {
                        job_id: key.0.clone(),
                        task_name: key.1.clone(),
                    })
                    .await;
            }
        }
        let ids: Vec<String> = lock(&self.sessions)
            .records()
            .filter(|record| record.job_id == key.0 && record.task_name == key.1)
            .map(|record| record.session_id.clone())
            .collect();
        for id in ids {
            self.stop_session(&id, "its task left the node").await;
        }
        let empty = !lock(&self.state)
            .tasks
            .values()
            .any(|task| task.cordon == cordon);
        if empty {
            let cordon_entry = lock(&self.state).cordons.remove(cordon);
            for replica in cordon_entry.map(|c| c.replicas).unwrap_or_default() {
                replica.retired.store(true, Ordering::Release);
                if let Some(conn) = replica.conn() {
                    info!(cordon = %cordon, replica = replica.index, pid = conn.pid, "Stopping the isolate host: its cordon has no tasks left on this node");
                    (conn.kill)();
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Lookups for the server and the IsolateWorker service
    // -----------------------------------------------------------------

    /// Whether this supervisor runs the task (it is an isolate task with an
    /// envelope here).
    pub fn has_task(&self, job_id: &str, task_name: &str) -> bool {
        lock(&self.state)
            .tasks
            .contains_key(&(job_id.to_string(), task_name.to_string()))
    }

    /// Whether `job/task` is an isolate task this node holds an envelope
    /// for, discovering it from the task manager when it is new.
    pub fn is_isolate_task(&self, job_id: &str, task_name: &str) -> bool {
        if self.has_task(job_id, task_name) {
            return true;
        }
        let Some(task_manager) = self.task_manager() else {
            return false;
        };
        let spec = lock(&task_manager).session_spec(job_id, task_name);
        let isolate = spec.is_some_and(|spec| spec.task.runtime == ISOLATE_RUNTIME);
        if isolate {
            self.sync_from_task_manager();
        }
        isolate
    }

    /// The task an `Origin` names, when it belongs to `cordon`.
    pub fn task_ref(&self, cordon: &str, origin: &host::Origin) -> Option<IsolateTaskRef> {
        let state = lock(&self.state);
        let task = state
            .tasks
            .get(&(origin.job_id.clone(), origin.task_name.clone()))?;
        (task.cordon == cordon).then(|| IsolateTaskRef {
            job_id: task.job_id.clone(),
            task_name: task.task_name.clone(),
            envelope_allocation_id: task.envelope_allocation_id,
            isolate_id: origin.isolate_id.clone(),
            tenant: origin.tenant.clone(),
            session: origin.session,
        })
    }

    pub(crate) fn inflight(
        &self,
        cordon: &str,
        key: &TaskKey,
        id: &str,
        tenant: &str,
    ) -> Inflight<'_> {
        let entry = (
            cordon.to_string(),
            key.0.clone(),
            key.1.clone(),
            id.to_string(),
        );
        lock(&self.inflight).insert(entry.clone(), tenant.to_string());
        Inflight {
            supervisor: self,
            key: entry,
        }
    }

    /// Whether the isolate a host request names is one this worker gave
    /// that host's cordon: a session isolate of exactly that job and task
    /// it holds, or an invocation of that task in flight on it. A host can
    /// otherwise claim any id (child VMs are owned by these ids).
    pub fn issued(&self, cordon: &str, task: &IsolateTaskRef) -> bool {
        self.issued_tenant(cordon, task).is_some()
    }

    /// [`Self::issued`], with the tenant the worker gave that isolate (the
    /// session's recorded tenant, or the tenant of the invocation in
    /// flight): what a host's request is attributed to, whatever tenant its
    /// `Origin` claims.
    pub fn issued_tenant(&self, cordon: &str, task: &IsolateTaskRef) -> Option<String> {
        if task.session {
            return lock(&self.sessions)
                .get(&task.isolate_id)
                .filter(|r| r.job_id == task.job_id && r.task_name == task.task_name)
                .map(|r| r.tenant.clone());
        }
        lock(&self.inflight)
            .get(&(
                cordon.to_string(),
                task.job_id.clone(),
                task.task_name.clone(),
                task.isolate_id.clone(),
            ))
            .cloned()
    }

    /// The tenant to attribute a host's log lines to, when the isolate they
    /// name is one this worker gave that host's cordon: a session isolate
    /// of that job and task (its recorded tenant), or an invocation of that
    /// task in flight or ended within [`RECENT_INVOCATION_GRACE`] (the
    /// tenant the worker invoked it for). `None`: the host named an isolate
    /// it was never given, and its lines are refused. The host's own word
    /// for the tenant is never used.
    pub fn log_tenant(&self, cordon: &str, task: &IsolateTaskRef) -> Option<String> {
        if let Some(tenant) = self.issued_tenant(cordon, task) {
            return Some(tenant);
        }
        if task.session {
            return None;
        }
        lock(&self.recent).tenant(&(
            cordon.to_string(),
            task.job_id.clone(),
            task.task_name.clone(),
            task.isolate_id.clone(),
        ))
    }

    pub fn vm_task_of(&self, job_id: &str, task_name: &str) -> Option<String> {
        lock(&self.state)
            .tasks
            .get(&(job_id.to_string(), task_name.to_string()))
            .and_then(|task| task.vm_task.clone())
    }

    fn task_ref_for(
        &self,
        job_id: &str,
        task_name: &str,
        isolate_id: &str,
        tenant: &str,
        session: bool,
    ) -> IsolateTaskRef {
        let allocation = lock(&self.state)
            .tasks
            .get(&(job_id.to_string(), task_name.to_string()))
            .map(|task| task.envelope_allocation_id)
            .unwrap_or_default();
        IsolateTaskRef {
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            envelope_allocation_id: allocation,
            isolate_id: isolate_id.to_string(),
            tenant: tenant.to_string(),
            session,
        }
    }

    /// Wait until the task can take work, up to `wait`.
    async fn ready(&self, key: &TaskKey, wait: Duration) -> Result<(), Status> {
        let receiver = lock(&self.state)
            .tasks
            .get(key)
            .map(|task| task.readiness.subscribe());
        let Some(mut receiver) = receiver else {
            return Err(Status::failed_precondition(
                crate::worker::task_manager::SessionRefusal::NoEnvelope {
                    job_id: key.0.clone(),
                    task_name: key.1.clone(),
                }
                .to_string(),
            ));
        };
        let waited = tokio::time::timeout(wait, async {
            loop {
                let now = receiver.borrow().clone();
                match now {
                    Readiness::Ready => return Ok(()),
                    Readiness::Failed(problem) => {
                        return Err(Status::failed_precondition(format!(
                            "isolate task {}/{} cannot run on node {}: {problem}",
                            key.0, key.1, self.node_id
                        )))
                    }
                    Readiness::Preparing => {}
                }
                if receiver.changed().await.is_err() {
                    return Err(Status::unavailable("the task left this node; retry"));
                }
            }
        })
        .await;
        match waited {
            Ok(result) => result,
            Err(_) => Err(Status::unavailable(format!(
                "isolate task {}/{}: the module is still compiling on node {}; retry",
                key.0, key.1, self.node_id
            ))),
        }
    }

    /// Discover the task if needed and return its key.
    fn known_task(&self, job_id: &str, task_name: &str) -> Result<TaskKey, Status> {
        let key = (job_id.to_string(), task_name.to_string());
        if !self.has_task(job_id, task_name) {
            self.sync_from_task_manager();
        }
        if self.has_task(job_id, task_name) {
            return Ok(key);
        }
        Err(Status::failed_precondition(
            crate::worker::task_manager::SessionRefusal::NoEnvelope {
                job_id: job_id.to_string(),
                task_name: task_name.to_string(),
            }
            .to_string(),
        ))
    }

    /// The replica a call goes to: a session's own, a tenant's by hash (so
    /// its warm instance is found again), or the next live one.
    fn pick_replica(
        &self,
        cordon: &str,
        tenant: &str,
        session_replica: Option<u32>,
    ) -> Result<(Arc<Replica>, Arc<HostConn>), Status> {
        let replicas = self.replicas(cordon);
        if replicas.is_empty() {
            return Err(Status::unavailable(format!(
                "no isolate host runs for cordon {cordon} on node {}; retry",
                self.node_id
            )));
        }
        let unavailable = |replica: &Replica| {
            let problem = replica.problem();
            Status::unavailable(if problem.is_empty() {
                format!("the isolate host for cordon {cordon} is restarting; retry")
            } else {
                format!("{problem}; retry")
            })
        };
        if let Some(index) = session_replica {
            let replica = replicas
                .get(index as usize)
                .cloned()
                .unwrap_or_else(|| replicas[0].clone());
            let conn = replica.serving().ok_or_else(|| unavailable(&replica))?;
            return Ok((replica, conn));
        }
        if !tenant.is_empty() {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            tenant.hash(&mut hasher);
            let replica = replicas[(hasher.finish() % replicas.len() as u64) as usize].clone();
            let conn = replica.serving().ok_or_else(|| unavailable(&replica))?;
            return Ok((replica, conn));
        }
        let start = self.round_robin.fetch_add(1, Ordering::Relaxed);
        for offset in 0..replicas.len() {
            let replica = &replicas[(start + offset) % replicas.len()];
            if let Some(conn) = replica.serving() {
                return Ok((replica.clone(), conn));
            }
        }
        Err(unavailable(&replicas[0]))
    }

    /// A host call failed: when its process is gone, say so as retryable.
    fn host_error(&self, conn: &HostConn, cordon: &str, status: Status) -> Status {
        let transport = matches!(
            status.code(),
            Code::Unavailable | Code::Unknown | Code::Internal | Code::Cancelled
        ) && (status.message().contains("transport")
            || status.message().contains("connection")
            || status.message().contains("broken pipe")
            || status.message().contains("h2 protocol"));
        if !conn.alive() || transport {
            return Status::unavailable(format!(
                "the isolate host for cordon {cordon} exited during the call; the worker is \
                 restarting it; retry"
            ));
        }
        status
    }

    // -----------------------------------------------------------------
    // Runs
    // -----------------------------------------------------------------

    /// `Worker.RunSession` on an isolate task.
    pub async fn run(
        &self,
        call: RunCall,
        launch_pool: Option<Arc<crate::worker::launch_pool::LaunchPool>>,
    ) -> Result<RunSessionResponse, Status> {
        let request = call.request.clone();
        if !request.command.is_empty() {
            return Err(Status::invalid_argument(format!(
                "command does not apply to isolate task {}: an isolate runs the code the run \
                 sends in files (its entry is main.<ext>, else the first file)",
                call.task_name
            )));
        }
        if !matches!(
            request.tier.trim(),
            "" | tier::TIER_AUTO | tier::TIER_ISOLATE | tier::TIER_VM
        ) {
            return Err(Status::invalid_argument(format!(
                "tier {:?} is not one of \"auto\", \"isolate\", \"vm\"",
                request.tier
            )));
        }
        if !request.tenant.is_empty() {
            crate::structs::isolate::validate_tenant_key(&request.tenant)
                .map_err(Status::invalid_argument)?;
        }
        check_args(&request.args)?;
        let key = self.known_task(&call.job_id, &call.task_name)?;
        let lifetime = lock(&self.state)
            .tasks
            .get(&key)
            .map(|task| task.resolved.lifetime)
            .unwrap_or_default();
        if !request.session_id.is_empty() && lifetime != IsolateLifetime::Session {
            return Err(Status::failed_precondition(format!(
                "session_id applies only to an isolate task with lifetime = \"session\"; task {} \
                 has lifetime = \"{}\"",
                call.task_name,
                lifetime.as_str()
            )));
        }
        let staged = crate::worker::task_manager::session_runs::staged_files(&request)?;
        let decided = self.decide(&key, &request, &staged)?;
        if request.dry_run {
            return Ok(self.dry_run(&key, &call, decided));
        }
        let me = self.arc();
        let node_id = self.node_id.clone();
        let (job_id, task_name) = (call.job_id.clone(), call.task_name.clone());
        crate::worker::task_manager::session_runs::run_keyed(
            &self.run_registry,
            &job_id,
            &task_name,
            &node_id,
            &request,
            move |run_id| async move {
                let result = match decided.tier.clone() {
                    Tier::Isolate { reason } => {
                        me.run_isolate(&key, &call, staged, &decided, &reason, &run_id)
                            .await
                    }
                    Tier::Vm { reason } => {
                        me.run_vm(&key, call, staged, &decided, &reason, launch_pool)
                            .await
                    }
                };
                crate::telemetry::worker_metrics()
                    .session_runs_total
                    .inc(&[match &result {
                        Ok(reply) if reply.stopped.is_some() => "stopped",
                        Ok(_) => "ran",
                        Err(_) => "failed",
                    }]);
                result
            },
        )
        .await
    }

    /// The tier decision and what went into it.
    fn decide(
        &self,
        key: &TaskKey,
        request: &RunSessionRequest,
        staged: &crate::worker::task_manager::session_runs::StagedFiles,
    ) -> Result<Decided, Status> {
        let (has_vm, image, modules, entry_source, memo_for, entry_name) = {
            let state = lock(&self.state);
            let task = state
                .tasks
                .get(key)
                .ok_or_else(|| Status::unavailable("the task left this node; retry"))?;
            let names: Vec<&str> = staged
                .files
                .iter()
                .filter(|(path, _, _)| path != crate::structs::run::RUN_INPUT_FILE)
                .map(|(path, _, _)| path.as_str())
                .collect();
            // `config.entry` IS the entry (the host stages it from
            // `TaskSpec.entry`): the run's files are data, and one named like
            // the entry would shadow it, so it is refused.
            let entry_name = if task.resolved.entry.is_some() {
                if let Some(entry) = task.entry.as_ref() {
                    if names.contains(&entry.path.as_str()) {
                        return Err(Status::invalid_argument(format!(
                            "file {} has the name of task {}'s config.entry, which is the \
                             entry of every run; the run's files are data\nHint: rename the \
                             file",
                            entry.path, key.1
                        )));
                    }
                }
                None
            } else {
                crate::structs::isolate::run_entry(task.image(), &names).map(str::to_string)
            };
            let source = match entry_name.as_deref() {
                Some(name) => staged
                    .files
                    .iter()
                    .find(|(path, _, _)| path == name)
                    .map(|(_, content, _)| content.clone())
                    .unwrap_or_default(),
                None => task
                    .entry
                    .as_ref()
                    .map(|f| f.content.clone())
                    .unwrap_or_default(),
            };
            (
                task.vm_task.is_some(),
                task.image().map(str::to_string),
                task.image_modules.clone(),
                source,
                task.resolved.memo_for,
                entry_name,
            )
        };
        let run_files: Vec<String> = staged
            .files
            .iter()
            .map(|(path, _, _)| path.clone())
            .collect();
        let scan = image
            .as_deref()
            .and_then(super::preflight::Language::of_image)
            .map(|language| super::preflight::scan(language, &entry_source, &modules, &run_files))
            .unwrap_or_default();
        let memo_key = VmMemo::key(&key.0, &key.1, &entry_source);
        let memo_age = lock(&self.memo).lookup(&memo_key, memo_for, Instant::now());
        let runtime_disabled = self
            .gate
            .current()
            .refusing(ISOLATE_RUNTIME, &key.0)
            .is_some();
        let decision = tier::decide(&TierInputs {
            requested: &request.tier,
            has_vm,
            runtime_disabled,
            missing_imports: &scan.missing,
            image: image.as_deref(),
            memo_age,
        });
        let tier = match decision {
            Ok(tier) => tier,
            Err(TierRefusal::Invalid(message)) => return Err(Status::invalid_argument(message)),
            Err(TierRefusal::NoVm) => {
                return Err(Status::failed_precondition(format!(
                    "tier = \"vm\" needs a VM task: isolate task {} has no config.vm\nHint: add \
                     config.vm = {{ task = \"<sandbox session task>\" }} to the task, or run \
                     with tier = \"isolate\"",
                    key.1
                )))
            }
            Err(TierRefusal::RuntimeDisabled) => {
                return Err(match self.gate.check(ISOLATE_RUNTIME, &key.0) {
                    Err(refusal) => refusal.to_status(),
                    Ok(()) => Status::unavailable("the isolate runtime is disabled; retry"),
                })
            }
        };
        Ok(Decided {
            tier,
            imports: scan.imports,
            missing: scan.missing,
            memo_key,
            has_vm,
            image,
            entry_name,
        })
    }

    fn dry_run(&self, key: &TaskKey, call: &RunCall, decided: Decided) -> RunSessionResponse {
        let (tier_name, reason) = match &decided.tier {
            Tier::Isolate { reason } => (tier::TIER_ISOLATE, reason.clone()),
            Tier::Vm { reason } => (tier::TIER_VM, reason.clone()),
        };
        let (wall, output) = lock(&self.state)
            .tasks
            .get(key)
            .map(|task| (task.resolved.wall_text.clone(), task.resolved.output))
            .unwrap_or_default();
        let refusal = match &decided.tier {
            Tier::Isolate { .. } => self.room_refusal(key),
            Tier::Vm { .. } => {
                let vm_task = self.vm_task_of(&key.0, &key.1).unwrap_or_default();
                self.task_manager().and_then(|tm| {
                    let mut guard = lock(&tm);
                    match guard.session_spec(&key.0, &vm_task) {
                        Some(spec) => guard.session_room_refusal(&key.0, &spec.task),
                        None => Some(
                            crate::worker::task_manager::SessionRefusal::NoEnvelope {
                                job_id: key.0.clone(),
                                task_name: vm_task.clone(),
                            }
                            .to_string(),
                        ),
                    }
                })
            }
        };
        RunSessionResponse {
            node_id: self.node_id.clone(),
            task_name: call.task_name.clone(),
            dry_run: Some(RunDryRun {
                node_id: self.node_id.clone(),
                has_room: refusal.is_none(),
                reason: refusal.unwrap_or_default(),
                wall,
                output_bytes: output,
                tier: tier_name.into(),
                tier_reason: reason,
                imports: decided.imports,
                missing_imports: decided.missing,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Why the host has no room for one more isolate of the task now.
    fn room_refusal(&self, key: &TaskKey) -> Option<String> {
        let state = lock(&self.state);
        let task = state.tasks.get(key)?;
        let problem = task.problem();
        if !problem.is_empty() && !matches!(&*task.readiness.borrow(), Readiness::Preparing) {
            return Some(problem);
        }
        let resident: u32 = state
            .cordons
            .get(&task.cordon)
            .map(|c| {
                c.replicas
                    .iter()
                    .filter_map(|r| lock(&r.last_stats).clone())
                    .flat_map(|stats| stats.tasks)
                    .filter(|t| t.job_id == key.0 && t.task_name == key.1)
                    .map(|t| t.resident)
                    .sum()
            })
            .unwrap_or(0);
        if task.room.max_resident == 0 {
            return Some(format!(
                "the envelope of {} holds less memory than one isolate's reservation",
                key.1
            ));
        }
        (resident >= task.room.max_resident).then(|| {
            format!(
                "the envelope of {} on this node holds {resident} resident isolates, its room",
                key.1
            )
        })
    }

    async fn run_isolate(
        &self,
        key: &TaskKey,
        call: &RunCall,
        staged: crate::worker::task_manager::session_runs::StagedFiles,
        decided: &Decided,
        reason: &str,
        run_id: &str,
    ) -> Result<RunSessionResponse, Status> {
        let request = &call.request;
        let wait = request
            .wait_timeout_seconds
            .map(|s| Duration::from_secs(u64::from(s.max(1))).min(Duration::from_secs(600)))
            .unwrap_or(DEFAULT_READY_WAIT);
        self.ready(key, wait).await?;
        let cordon = lock(&self.state)
            .tasks
            .get(key)
            .map(|task| task.cordon.clone())
            .ok_or_else(|| Status::unavailable("the task left this node; retry"))?;
        let mut session_tenant = String::new();
        let session_replica = if request.session_id.is_empty() {
            None
        } else {
            let mut book = lock(&self.sessions);
            let record = book
                .get_mut(&request.session_id)
                .filter(|r| r.job_id == key.0 && r.task_name == key.1)
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "session isolate {} of task {} is not on node {}",
                        request.session_id, key.1, self.node_id
                    ))
                })?;
            record.last_active_unix_ms = crate::util::now_unix_ms();
            check_session_tenant(&request.session_id, &record.tenant, &request.tenant)?;
            session_tenant = record.tenant.clone();
            Some(record.replica)
        };
        // A session's own tenant is the run's, everywhere (stats, egress
        // origin, fairness).
        let tenant = if request.session_id.is_empty() {
            request.tenant.clone()
        } else {
            session_tenant
        };
        let (_, conn) = self.pick_replica(&cordon, &tenant, session_replica)?;
        let get: Vec<String> = staged
            .get
            .iter()
            .map(|(_, relative)| relative.clone())
            .collect();
        let asked: Vec<String> = staged.get.iter().map(|(asked, _)| asked.clone()).collect();
        let files = staged
            .files
            .into_iter()
            .filter(|(path, _, _)| path != crate::structs::run::RUN_INPUT_FILE)
            .map(|(path, content, _)| host::File {
                path,
                content,
                ..Default::default()
            })
            .collect();
        let invoke = host::InvokeRequest {
            job_id: key.0.clone(),
            task_name: key.1.clone(),
            invocation_id: run_id.to_string(),
            tenant: tenant.clone(),
            session_id: request.session_id.clone(),
            call: Some(host::invoke_request::Call::Run(host::RunCall {
                files,
                input: request.input.clone(),
                entry: decided.entry_name.clone().unwrap_or_default(),
                args: request.args.clone(),
                get,
            })),
        };
        let _inflight = self.inflight(&cordon, key, run_id, &tenant);
        let mut client = conn.client.clone();
        let reply = client
            .invoke(invoke)
            .await
            .map_err(|status| self.host_error(&conn, &cordon, status))?
            .into_inner();
        self.observe_invocation(&reply, decided.image.as_deref());
        if !request.session_id.is_empty() {
            let mut book = lock(&self.sessions);
            if let Some(record) = book.get_mut(&request.session_id) {
                record.state = SessionState::Resident;
            }
        }
        let identity = if request.session_id.is_empty() {
            run_id.to_string()
        } else {
            request.session_id.clone()
        };
        let mapped = tier::map_invoke_reply(
            reply,
            &self.node_id,
            &key.1,
            &identity,
            decided.has_vm,
            reason,
            &asked,
        );
        let mapped = RunSessionResponse {
            run_id: run_id.to_string(),
            ..mapped
        };
        if let Some(stop) = mapped.stopped.as_ref() {
            if decided.has_vm && tier::wants_vm(&stop.reason) {
                lock(&self.memo).record(decided.memo_key, Instant::now());
            }
            info!(
                job_id = %key.0,
                task = %key.1,
                run_id,
                reason = %stop.reason,
                limit = %stop.limit,
                next_tier = %mapped.next_tier,
                "Isolate run stopped"
            );
        }
        if request.session_id.is_empty() {
            let task = self.task_ref_for(&key.0, &key.1, run_id, &tenant, false);
            self.vm_backend()
                .owner_ended(&task, "its run finished")
                .await;
        }
        Ok(mapped)
    }

    fn observe_invocation(&self, reply: &host::InvokeReply, image: Option<&str>) {
        let metrics = crate::telemetry::worker_metrics();
        if reply.instantiate_micros > 0 {
            metrics.isolate_instantiate_seconds.observe(
                &[image.unwrap_or("module")],
                reply.instantiate_micros as f64 / 1e6,
            );
        }
        if let Some(stop) = reply.stopped.as_ref() {
            metrics.isolate_stopped_total.inc(&[stop.reason.as_str()]);
            if !stop.limit.is_empty() {
                metrics.isolate_limit_total.inc(&[stop.limit.as_str()]);
            }
        }
    }

    async fn run_vm(
        &self,
        key: &TaskKey,
        call: RunCall,
        staged: crate::worker::task_manager::session_runs::StagedFiles,
        decided: &Decided,
        reason: &str,
        launch_pool: Option<Arc<crate::worker::launch_pool::LaunchPool>>,
    ) -> Result<RunSessionResponse, Status> {
        let vm_task = self.vm_task_of(&key.0, &key.1).ok_or_else(|| {
            Status::failed_precondition(format!("isolate task {} has no config.vm", key.1))
        })?;
        let (Some(task_manager), Some(launch_pool)) = (self.task_manager(), launch_pool) else {
            return Err(Status::failed_precondition(
                "this node cannot start a VM for the run",
            ));
        };
        let configured = if decided.entry_name.is_none() {
            lock(&self.state)
                .tasks
                .get(key)
                .and_then(|task| task.entry.clone())
        } else {
            None
        };
        let (files, entry_path) = stage_for_vm(
            decided.image.as_deref(),
            decided.entry_name.as_deref(),
            configured,
            staged.files,
        )?;
        let vm_args = crate::worker::task_manager::session_runs::VmArgs {
            args: call.request.args.clone(),
            entry_path,
        };
        let request = RunSessionRequest {
            job_id: key.0.clone(),
            task_name: vm_task.clone(),
            files,
            tier: String::new(),
            tenant: String::new(),
            session_id: String::new(),
            args: Vec::new(),
            idempotency_key: None,
            ..call.request.clone()
        };
        let started = Instant::now();
        let reply = crate::worker::task_manager::session_runs::run_session_with_args(
            task_manager,
            launch_pool,
            self.node_id.clone(),
            RunCall {
                job_id: key.0.clone(),
                task_name: vm_task.clone(),
                request,
                ticket_jti: call.ticket_jti.clone(),
            },
            Some(vm_args),
        )
        .await?;
        // Counted once the VM run happened (a refused one did not).
        crate::telemetry::worker_metrics()
            .isolate_vm_runs_total
            .inc(&[vm_reason_label(reason)]);
        *lock(&self.state).vm_runs.entry(key.clone()).or_default() += 1;
        let elapsed = started.elapsed().as_millis() as u64;
        Ok(RunSessionResponse {
            tier: tier::TIER_VM.into(),
            tier_reason: reason.to_string(),
            next_tier: tier::TIER_VM.into(),
            vm_ms: elapsed.saturating_sub(reply.wall_ms),
            task_name: vm_task,
            cpu_micros: reply.cpu_ms * 1000,
            wall_micros: reply.wall_ms * 1000,
            ..reply
        })
    }

    // -----------------------------------------------------------------
    // Session isolates
    // -----------------------------------------------------------------

    /// The session isolates this node holds (restored from the book after
    /// a worker restart), as child owners: register each with the child VM
    /// backend before its first child reconcile, or their children are
    /// reaped as orphans.
    pub fn session_owner_refs(&self) -> Vec<IsolateTaskRef> {
        let records: Vec<SessionRecord> = lock(&self.sessions).records().cloned().collect();
        records
            .into_iter()
            .map(|r| self.task_ref_for(&r.job_id, &r.task_name, &r.session_id, &r.tenant, true))
            .collect()
    }

    /// Whether `session_id` names a session isolate on this node.
    pub fn knows_session(&self, session_id: &str) -> bool {
        lock(&self.sessions).get(session_id).is_some()
    }

    /// A session isolate's job and task.
    pub fn session_owner(&self, session_id: &str) -> Option<(String, String)> {
        lock(&self.sessions)
            .get(session_id)
            .map(|r| (r.job_id.clone(), r.task_name.clone()))
    }

    /// Start (open) a session isolate. `session_id` is the caller's minted
    /// or derived id.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_session(
        &self,
        job_id: &str,
        task_name: &str,
        session_id: &str,
        tenant: &str,
        origin: &str,
        ticket_jti: &str,
        idempotency_key: &str,
    ) -> Result<StartedIsolate, Status> {
        let key = self.known_task(job_id, task_name)?;
        let lifetime = lock(&self.state)
            .tasks
            .get(&key)
            .map(|task| task.resolved.lifetime)
            .unwrap_or_default();
        if lifetime != IsolateLifetime::Session {
            return Err(Status::failed_precondition(format!(
                "isolate task {task_name} has lifetime = \"{}\": there is nothing to start; run \
                 code with `legion session run`",
                lifetime.as_str()
            )));
        }
        if let Some(existing) = lock(&self.sessions).get(session_id) {
            if existing.job_id == job_id && existing.task_name == task_name {
                return Ok(StartedIsolate {
                    session_id: session_id.to_string(),
                    deduplicated: true,
                });
            }
            return Err(Status::already_exists(format!(
                "session {session_id} already exists for another task"
            )));
        }
        if let Some(refusal) = self.gate.check(ISOLATE_RUNTIME, job_id).err() {
            return Err(refusal.to_status());
        }
        self.ready(&key, DEFAULT_READY_WAIT).await?;
        let cordon = lock(&self.state)
            .tasks
            .get(&key)
            .map(|task| task.cordon.clone())
            .unwrap_or_default();
        // The replica holding the fewest sessions.
        let replicas = self.replicas(&cordon);
        let cordon_tasks: std::collections::HashSet<TaskKey> = lock(&self.state)
            .tasks
            .iter()
            .filter(|(_, task)| task.cordon == cordon)
            .map(|(key, _)| key.clone())
            .collect();
        let index = {
            let book = lock(&self.sessions);
            (0..replicas.len() as u32)
                .filter(|index| replicas[*index as usize].conn().is_some())
                .min_by_key(|index| {
                    book.records()
                        .filter(|r| {
                            r.replica == *index
                                && cordon_tasks.contains(&(r.job_id.clone(), r.task_name.clone()))
                        })
                        .count()
                })
        };
        let (_, conn) = self.pick_replica(&cordon, "", Some(index.unwrap_or(0)))?;
        let mut client = conn.client.clone();
        client
            .open_session(host::OpenSessionRequest {
                job_id: job_id.to_string(),
                task_name: task_name.to_string(),
                session_id: session_id.to_string(),
                tenant: tenant.to_string(),
                memory_max_bytes: 0,
                memory_reservation_bytes: 0,
            })
            .await
            .map_err(|status| self.host_error(&conn, &cordon, status))?;
        let now = crate::util::now_unix_ms();
        lock(&self.sessions).insert(SessionRecord {
            session_id: session_id.to_string(),
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            tenant: tenant.to_string(),
            replica: index.unwrap_or(0),
            state: SessionState::Resident,
            started_unix_ms: now,
            last_active_unix_ms: now,
            idempotency_key: idempotency_key.to_string(),
            ticket_jti: ticket_jti.to_string(),
            origin: origin.to_string(),
            ..Default::default()
        });
        info!(
            job_id,
            task = task_name,
            session_id,
            "Session isolate started"
        );
        Ok(StartedIsolate {
            session_id: session_id.to_string(),
            deduplicated: false,
        })
    }

    /// End a session isolate: close it in its host, end the children it
    /// owns, drop its checkpoint. Returns whether it existed.
    pub async fn stop_session(&self, session_id: &str, reason: &str) -> bool {
        let Some(record) = lock(&self.sessions).remove(session_id) else {
            return false;
        };
        let cordon = lock(&self.state)
            .tasks
            .get(&(record.job_id.clone(), record.task_name.clone()))
            .map(|task| task.cordon.clone());
        if let Some(cordon) = cordon {
            if let Ok((_, conn)) = self.pick_replica(&cordon, "", Some(record.replica)) {
                let mut client = conn.client.clone();
                let _ = client
                    .close_session(host::CloseSessionRequest {
                        session_id: session_id.to_string(),
                        reason: reason.to_string(),
                    })
                    .await;
            }
        }
        let task = self.task_ref_for(
            &record.job_id,
            &record.task_name,
            session_id,
            &record.tenant,
            true,
        );
        self.vm_backend().owner_ended(&task, reason).await;
        if let Some(checkpoint) = record.checkpoint.clone() {
            let store = self.checkpoints.clone();
            let id = session_id.to_string();
            let _ =
                tokio::task::spawn_blocking(move || store.drop_checkpoint(&id, &checkpoint)).await;
        }
        info!(job_id = %record.job_id, task = %record.task_name, session_id, reason, "Session isolate stopped");
        true
    }

    /// A synthesized `SessionStatus` for existing session callers.
    pub fn session_status(&self, session_id: &str) -> Option<crate::structs::SessionStatus> {
        let record = lock(&self.sessions).get(session_id).cloned()?;
        let allocation = lock(&self.state)
            .tasks
            .get(&(record.job_id.clone(), record.task_name.clone()))
            .map(|task| task.envelope_allocation_id)
            .unwrap_or_default();
        Some(crate::structs::SessionStatus {
            session_id: record.session_id,
            job_id: record.job_id,
            task_name: record.task_name,
            envelope_allocation_id: allocation,
            state: match record.state {
                SessionState::Resident => "running".into(),
                SessionState::Hibernated => "suspended".into(),
            },
            lease_issued_at_unix_ms: record.started_unix_ms,
            generation: record.generation.max(1),
            origin: record.origin,
            ticket_jti: record.ticket_jti,
            ..Default::default()
        })
    }

    /// `Worker.GetIsolate`, without its children (the server adds them).
    pub async fn get_isolate(&self, session_id: &str) -> Option<crate::structs::GetIsolateReply> {
        let record = lock(&self.sessions).get(session_id).cloned()?;
        let mut reply = crate::structs::GetIsolateReply {
            session_id: record.session_id.clone(),
            node_id: self.node_id.clone(),
            job_id: record.job_id.clone(),
            task_name: record.task_name.clone(),
            tenant: record.tenant.clone(),
            state: "hibernated".into(),
            started_unix_ms: record.started_unix_ms,
            last_active_unix_ms: record.last_active_unix_ms,
            checkpoint_generation: record.generation,
            checkpoint_bytes: record
                .checkpoint
                .as_ref()
                .map(|c| c.bytes)
                .unwrap_or_default(),
            ..Default::default()
        };
        if record.state == SessionState::Resident {
            reply.state = "empty".into();
            let cordon = lock(&self.state)
                .tasks
                .get(&(record.job_id.clone(), record.task_name.clone()))
                .map(|task| task.cordon.clone());
            if let Some(Ok((_, conn))) =
                cordon.map(|c| self.pick_replica(&c, "", Some(record.replica)))
            {
                let mut client = conn.client.clone();
                match client
                    .get_session(host::GetSessionRequest {
                        session_id: session_id.to_string(),
                    })
                    .await
                {
                    Ok(info) => {
                        let info = info.into_inner();
                        reply.state = info.tier;
                        reply.last_active_unix_ms =
                            info.last_active_unix_ms.max(reply.last_active_unix_ms);
                        reply.invocations = info.invocations;
                        reply.memory_bytes = info.memory_bytes;
                        reply.state_bytes = info.state_bytes;
                    }
                    Err(status)
                        if status.code() == Code::NotFound && record.checkpoint.is_some() =>
                    {
                        reply.state = "hibernated".into();
                    }
                    Err(_) => {}
                }
            }
        }
        Some(reply)
    }

    /// Hibernate this one session isolate now (`Reclaim{HIBERNATE}` with
    /// `session_ids` = it); the host's other sessions stay resident. A busy
    /// session answers FAILED_PRECONDITION.
    pub async fn hibernate(&self, session_id: &str) -> Result<(u64, u64, bool), Status> {
        let record = lock(&self.sessions)
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                Status::not_found(format!("session isolate {session_id} is not on this node"))
            })?;
        if record.state == SessionState::Hibernated {
            return Ok((
                record
                    .checkpoint
                    .as_ref()
                    .map(|c| c.bytes)
                    .unwrap_or_default(),
                record.generation,
                true,
            ));
        }
        let cordon = lock(&self.state)
            .tasks
            .get(&(record.job_id.clone(), record.task_name.clone()))
            .map(|task| task.cordon.clone())
            .ok_or_else(|| Status::unavailable("its task left this node"))?;
        let (_, conn) = self.pick_replica(&cordon, "", Some(record.replica))?;
        let mut client = conn.client.clone();
        client
            .reclaim(host::ReclaimRequest {
                level: host::ReclaimLevel::ReclaimHibernate as i32,
                target_bytes: 0,
                min_idle_millis: 0,
                // Only this session: the host's other sessions stay resident.
                session_ids: vec![session_id.to_string()],
                ..Default::default()
            })
            .await
            .map_err(|status| self.host_error(&conn, &cordon, status))?;
        let after = lock(&self.sessions).get(session_id).cloned();
        match after {
            Some(after) if after.state == SessionState::Hibernated => Ok((
                after
                    .checkpoint
                    .as_ref()
                    .map(|c| c.bytes)
                    .unwrap_or_default(),
                after.generation,
                false,
            )),
            Some(_) => Err(Status::failed_precondition(format!(
                "session isolate {session_id} is busy (a call is running in it); suspend it \
                 again once it is idle"
            ))),
            None => Err(Status::not_found(format!(
                "session isolate {session_id} ended"
            ))),
        }
    }

    /// Store a hibernating session's checkpoint (`IsolateWorker.SaveCheckpoint`).
    pub async fn save_checkpoint(
        &self,
        task: &IsolateTaskRef,
        header: &host::CheckpointHeader,
        body: Vec<u8>,
    ) -> Result<u64, Status> {
        let session_id = task.isolate_id.clone();
        let (previous, generation) = {
            let book = lock(&self.sessions);
            let record = book
                .get(&session_id)
                .filter(|r| r.job_id == task.job_id && r.task_name == task.task_name)
                .ok_or_else(|| {
                    Status::not_found(format!("session isolate {session_id} is not on this node"))
                })?;
            (record.checkpoint.clone(), record.generation + 1)
        };
        let store = self.checkpoints.clone();
        let id = session_id.clone();
        let now = crate::util::now_unix_ms();
        let mut stored =
            tokio::task::spawn_blocking(move || store.put(&id, generation, &body, now))
                .await
                .map_err(|err| Status::internal(format!("storing the checkpoint panicked: {err}")))?
                .map_err(|err| {
                    Status::resource_exhausted(format!("could not keep the checkpoint: {err:#}"))
                })?;
        stored.engine_version = header.engine_version.clone();
        stored.module_id = header.module_id.clone();
        let replaced = {
            let mut book = lock(&self.sessions);
            match book.get_mut(&session_id) {
                Some(record) => {
                    record.checkpoint = Some(stored.clone());
                    record.generation = generation;
                    record.state = SessionState::Hibernated;
                    book.save();
                    previous
                }
                // Stopped while the checkpoint was written.
                None => Some(stored),
            }
        };
        if let Some(old) = replaced.filter(|old| {
            !same_checkpoint(
                old,
                lock(&self.sessions)
                    .get(&session_id)
                    .and_then(|r| r.checkpoint.as_ref()),
            )
        }) {
            let store = self.checkpoints.clone();
            let _ =
                tokio::task::spawn_blocking(move || store.drop_checkpoint(&session_id, &old)).await;
        }
        Ok(generation)
    }

    /// Read a session's checkpoint back (`IsolateWorker.LoadCheckpoint`).
    pub async fn load_checkpoint(
        &self,
        task: &IsolateTaskRef,
    ) -> Result<(host::CheckpointHeader, Vec<u8>), Status> {
        let record = lock(&self.sessions)
            .get(&task.isolate_id)
            .filter(|r| r.job_id == task.job_id && r.task_name == task.task_name)
            .cloned()
            .ok_or_else(|| {
                Status::not_found(format!(
                    "session isolate {} is not on this node",
                    task.isolate_id
                ))
            })?;
        let checkpoint = record.checkpoint.clone().ok_or_else(|| {
            Status::not_found(format!(
                "session isolate {} has never hibernated",
                task.isolate_id
            ))
        })?;
        let store = self.checkpoints.clone();
        let read = checkpoint.clone();
        let body = tokio::task::spawn_blocking(move || store.get(&read))
            .await
            .map_err(|err| Status::internal(format!("reading the checkpoint panicked: {err}")))?
            .map_err(|err| Status::not_found(format!("the checkpoint is gone: {err:#}")))?;
        if let Some(record) = lock(&self.sessions).get_mut(&task.isolate_id) {
            record.state = SessionState::Resident;
        }
        lock(&self.sessions).save();
        crate::telemetry::worker_metrics()
            .isolate_wakes_total
            .inc(&["hibernated"]);
        Ok((
            host::CheckpointHeader {
                origin: Some(host::Origin {
                    job_id: task.job_id.clone(),
                    task_name: task.task_name.clone(),
                    isolate_id: task.isolate_id.clone(),
                    tenant: record.tenant.clone(),
                    session: true,
                }),
                engine_version: checkpoint.engine_version,
                module_id: checkpoint.module_id,
                bytes: body.len() as u64,
                freed_bytes: 0,
            },
            body,
        ))
    }

    /// A host (re)started: tell it about the sessions it should hold. A
    /// session with a checkpoint is opened again (it wakes from the
    /// checkpoint on its next call); one that never hibernated was lost
    /// with the old process.
    async fn reopen_sessions(&self, replica: &Replica) {
        let Some(conn) = replica.conn() else { return };
        let records: Vec<SessionRecord> = {
            let state = lock(&self.state);
            lock(&self.sessions)
                .records()
                .filter(|r| r.replica == replica.index)
                .filter(|r| {
                    state
                        .tasks
                        .get(&(r.job_id.clone(), r.task_name.clone()))
                        .is_some_and(|task| task.cordon == replica.cordon)
                })
                .cloned()
                .collect()
        };
        for record in records {
            if record.checkpoint.is_none() {
                warn!(session_id = %record.session_id, job_id = %record.job_id, "A session isolate that never hibernated was lost with its host process");
                lock(&self.sessions).remove(&record.session_id);
                continue;
            }
            let mut client = conn.client.clone();
            let opened = client
                .open_session(host::OpenSessionRequest {
                    job_id: record.job_id.clone(),
                    task_name: record.task_name.clone(),
                    session_id: record.session_id.clone(),
                    tenant: record.tenant.clone(),
                    memory_max_bytes: record.memory_max_bytes,
                    memory_reservation_bytes: record.memory_reservation_bytes,
                })
                .await;
            match opened {
                Ok(_) => {
                    if let Some(r) = lock(&self.sessions).get_mut(&record.session_id) {
                        r.state = SessionState::Hibernated;
                    }
                }
                Err(status) => {
                    debug!(session_id = %record.session_id, error = %status_text(&status), "Session isolate not reopened yet (its task is not configured); the task's configuration reopens it");
                }
            }
        }
        lock(&self.sessions).save();
    }

    /// Resize a session isolate (docs/resize.md): re-admit its reservation
    /// against the envelope, then change it in place (resident) or in its
    /// record (hibernated). Returns (outcome, message, from, to).
    pub async fn resize_session(
        &self,
        session_id: &str,
        memory_mib: Option<f64>,
        memory_max_mib: Option<f64>,
        cpu: Option<f64>,
        dry_run: bool,
    ) -> Result<(&'static str, String, Value, Value, String, String), Status> {
        let record = lock(&self.sessions)
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                Status::not_found(format!("session isolate {session_id} is not on this node"))
            })?;
        let key = (record.job_id.clone(), record.task_name.clone());
        let (task_reservation, task_max, envelope_mib, cordon) = {
            let state = lock(&self.state);
            let task = state
                .tasks
                .get(&key)
                .ok_or_else(|| Status::unavailable("its task left this node"))?;
            (
                crate::structs::isolate::reservation_mib(&task.task),
                crate::structs::isolate::memory_max_mib(&task.task),
                task.envelope_memory_mib,
                task.cordon.clone(),
            )
        };
        let current_res = if record.memory_reservation_bytes > 0 {
            record.memory_reservation_bytes >> 20
        } else {
            task_reservation
        };
        let current_max = if record.memory_max_bytes > 0 {
            record.memory_max_bytes >> 20
        } else {
            task_max
        };
        let from = json!({ "memory": current_res, "memoryMax": current_max });
        let (job, task) = key.clone();
        if cpu.is_some() {
            return Ok((
                "not_supported",
                "an isolate's cpu is its task's weight in the host's scheduler, not a per-session \
                 amount; change resources.cpu on the task"
                    .into(),
                from.clone(),
                from,
                job,
                task,
            ));
        }
        let new_res = memory_mib
            .map(|m| m.ceil().max(0.0) as u64)
            .unwrap_or(current_res);
        let new_max = memory_max_mib
            .map(|m| m.ceil().max(0.0) as u64)
            .unwrap_or(current_max);
        let to = json!({ "memory": new_res, "memoryMax": new_max });
        let worker_max = self.config.max_memory_bytes().unwrap_or(4 << 30) >> 20;
        if new_max > worker_max {
            return Ok((
                "needs_restart",
                format!(
                    "memoryMax {new_max} MiB is above this worker's isolate.maxMemory ({worker_max} \
                     MiB), the size of one memory slot; no isolate on this node can grow past it"
                ),
                from,
                to,
                job,
                task,
            ));
        }
        if new_res > new_max {
            return Err(Status::invalid_argument(format!(
                "memory {new_res} MiB is above memoryMax {new_max} MiB"
            )));
        }
        if (new_res as f64) > envelope_mib {
            return Ok((
                "refused",
                format!("a {new_res} MiB reservation does not fit the task's {envelope_mib} MiB envelope"),
                from,
                to,
                job,
                task,
            ));
        }
        if dry_run {
            return Ok(("applied", String::new(), from, to, job, task));
        }
        if record.state == SessionState::Resident {
            if let Ok((_, conn)) = self.pick_replica(&cordon, "", Some(record.replica)) {
                let mut client = conn.client.clone();
                match client
                    .resize_session(host::ResizeSessionRequest {
                        session_id: session_id.to_string(),
                        memory_max_bytes: new_max << 20,
                        memory_reservation_bytes: new_res << 20,
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(status) if status.code() == Code::FailedPrecondition => {
                        return Ok(("refused", status.message().to_string(), from, to, job, task));
                    }
                    Err(status) if status.code() == Code::NotFound => {}
                    Err(status) => return Err(self.host_error(&conn, &cordon, status)),
                }
            }
        }
        if let Some(r) = lock(&self.sessions).get_mut(session_id) {
            r.memory_max_bytes = new_max << 20;
            r.memory_reservation_bytes = new_res << 20;
        }
        lock(&self.sessions).save();
        Ok(("applied", String::new(), from, to, job, task))
    }

    // -----------------------------------------------------------------
    // Ingress
    // -----------------------------------------------------------------

    async fn http(
        &self,
        job_id: &str,
        task_name: &str,
        tenant: &str,
        session_id: &str,
        request: host::HttpCall,
    ) -> Result<host::HttpOutcome, Status> {
        if !tenant.is_empty() {
            crate::structs::isolate::validate_tenant_key(tenant)
                .map_err(Status::invalid_argument)?;
        }
        let key = self.known_task(job_id, task_name)?;
        if let Some(refusal) = self.gate.check(ISOLATE_RUNTIME, job_id).err() {
            return Err(refusal.to_status());
        }
        self.ready(&key, DEFAULT_READY_WAIT).await?;
        let cordon = lock(&self.state)
            .tasks
            .get(&key)
            .map(|t| t.cordon.clone())
            .unwrap_or_default();
        let mut tenant = tenant.to_string();
        let session_replica = if session_id.is_empty() {
            None
        } else {
            let (replica, own) = lock(&self.sessions)
                .get(session_id)
                .filter(|r| r.job_id == job_id && r.task_name == task_name)
                .map(|r| (r.replica, r.tenant.clone()))
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "session isolate {session_id} is not on node {}",
                        self.node_id
                    ))
                })?;
            check_session_tenant(session_id, &own, &tenant)?;
            tenant = own;
            Some(replica)
        };
        let tenant = tenant.as_str();
        let (_, conn) = self.pick_replica(&cordon, tenant, session_replica)?;
        let invocation_id = crate::structs::mint_session_id(task_name, &self.node_id);
        let _inflight = self.inflight(&cordon, &key, &invocation_id, tenant);
        let mut client = conn.client.clone();
        let reply = client
            .invoke(host::InvokeRequest {
                job_id: job_id.to_string(),
                task_name: task_name.to_string(),
                invocation_id: invocation_id.clone(),
                tenant: tenant.to_string(),
                session_id: session_id.to_string(),
                call: Some(host::invoke_request::Call::Http(request)),
            })
            .await
            .map_err(|status| self.host_error(&conn, &cordon, status))?
            .into_inner();
        let image = lock(&self.state)
            .tasks
            .get(&key)
            .and_then(|t| t.image().map(str::to_string));
        self.observe_invocation(&reply, image.as_deref());
        if session_id.is_empty() {
            let task = self.task_ref_for(job_id, task_name, &invocation_id, tenant, false);
            self.vm_backend()
                .owner_ended(&task, "its request finished")
                .await;
        }
        match (reply.outcome, reply.stopped) {
            (Some(host::invoke_reply::Outcome::Http(outcome)), None) => Ok(outcome),
            (_, Some(stop)) => Ok(host::HttpOutcome {
                status: if stop.reason == "limits" { 503 } else { 500 },
                headers: vec![host::Header {
                    name: "content-type".into(),
                    value: b"text/plain; charset=utf-8".to_vec(),
                }],
                body: format!(
                    "isolate stopped: {} {} {}\n",
                    stop.reason, stop.limit, stop.detail
                )
                .into_bytes(),
            }),
            _ => Err(Status::internal(
                "the isolate host answered an HTTP call without a response",
            )),
        }
    }

    // -----------------------------------------------------------------
    // Reporting
    // -----------------------------------------------------------------

    /// Poll every host and build the node's aggregates. With a filter, only
    /// those tasks (and the hosts serving them); without, also updates the
    /// rates and the metrics (the heartbeat's call).
    pub async fn stats(
        &self,
        job_id: &str,
        task_name: &str,
    ) -> (Vec<IsolateTaskStats>, Vec<IsolateHostStats>) {
        let replicas: Vec<Arc<Replica>> = lock(&self.state)
            .cordons
            .values()
            .flat_map(|c| c.replicas.clone())
            .collect();
        let mut replies = Vec::new();
        let mut hosts = Vec::new();
        for replica in &replicas {
            let conn = replica.conn();
            let reply = match conn.as_ref() {
                Some(conn) => {
                    let mut client = conn.client.clone();
                    match tokio::time::timeout(STATS_TIMEOUT, client.stats(host::StatsRequest {}))
                        .await
                    {
                        Ok(Ok(reply)) => Some(reply.into_inner()),
                        _ => None,
                    }
                }
                None => None,
            };
            if let Some(reply) = reply.as_ref() {
                *lock(&replica.last_stats) = Some(reply.clone());
                replies.push((replica.cordon.clone(), reply.clone()));
            }
            hosts.push(IsolateHostStats {
                cordon: replica.cordon.clone(),
                replica: replica.index,
                rss_bytes: reply.as_ref().map(|r| r.rss_bytes).unwrap_or_default(),
                slots: reply.as_ref().map(|r| r.slots).unwrap_or(self.slots.slots),
                slots_used: reply.as_ref().map(|r| r.slots_used).unwrap_or_default(),
                shards: reply
                    .as_ref()
                    .map(|r| r.shards)
                    .or(conn.as_ref().map(|c| c.hello.shards))
                    .unwrap_or_default(),
                engine_version: conn
                    .as_ref()
                    .map(|c| c.hello.engine_version.clone())
                    .unwrap_or_default(),
                pid: conn.as_ref().map(|c| c.pid).unwrap_or_default(),
                started_unix_ms: conn.as_ref().map(|c| c.started_unix_ms).unwrap_or_default(),
                layout: plan::layout_name(self.config.layout).to_string(),
                confinement: conn
                    .as_ref()
                    .map(|c| c.confinement.clone())
                    .unwrap_or_default(),
                restarts: replica.restarts.load(Ordering::Acquire),
            });
        }
        let facts = self.facts();
        let all_replies: Vec<StatsReply> = replies.iter().map(|(_, r)| r.clone()).collect();
        let mut tasks = super::stats::fold(&facts, &all_replies);
        let filtered = !job_id.is_empty();
        if filtered {
            tasks.retain(|t| {
                t.job_id == job_id && (task_name.is_empty() || t.task_name == task_name)
            });
            let cordons: std::collections::HashSet<String> =
                tasks.iter().map(|t| t.cordon.clone()).collect();
            hosts.retain(|h| cordons.contains(&h.cordon));
        } else {
            lock(&self.rates).apply(&mut tasks, Instant::now());
            self.update_metrics(&tasks, &replies, &hosts);
        }
        (tasks, hosts)
    }

    fn facts(&self) -> Vec<TaskFacts> {
        let state = lock(&self.state);
        let book = lock(&self.sessions);
        let vm = self.vm_backend();
        state
            .tasks
            .values()
            .map(|task| {
                let hibernated = book
                    .records()
                    .filter(|r| {
                        r.job_id == task.job_id
                            && r.task_name == task.task_name
                            && r.state == SessionState::Hibernated
                    })
                    .count() as u32;
                let mut problem = task.problem();
                if problem.is_empty() {
                    if let Some(cordon) = state.cordons.get(&task.cordon) {
                        problem = cordon
                            .replicas
                            .iter()
                            .filter(|r| r.conn().is_none())
                            .map(|r| r.problem())
                            .find(|p| !p.is_empty())
                            .unwrap_or_default();
                    }
                }
                TaskFacts {
                    job_id: task.job_id.clone(),
                    task_name: task.task_name.clone(),
                    cordon: task.cordon.clone(),
                    hibernated,
                    vm_runs: state
                        .vm_runs
                        .get(&(task.job_id.clone(), task.task_name.clone()))
                        .copied()
                        .unwrap_or_default(),
                    child_vms: vm.child_count(&task.job_id, &task.task_name),
                    reservation_bytes: crate::structs::isolate::reservation_mib(&task.task) << 20,
                    problem,
                }
            })
            .collect()
    }

    fn update_metrics(
        &self,
        tasks: &[IsolateTaskStats],
        replies: &[(String, StatsReply)],
        hosts: &[IsolateHostStats],
    ) {
        let metrics = crate::telemetry::worker_metrics();
        let sum = |f: fn(&IsolateTaskStats) -> u32| tasks.iter().map(f).sum::<u32>() as i64;
        let resident = sum(|t| t.resident);
        let paged = sum(|t| t.paged_out);
        metrics.isolate_resident.set(&["hot"], resident - paged);
        metrics.isolate_resident.set(&["paged_out"], paged);
        metrics
            .isolate_resident
            .set(&["hibernated"], sum(|t| t.hibernated));
        metrics.isolate_child_vms.set(sum(|t| t.child_vms));
        let mut deltas = lock(&self.deltas);
        for (index, (cordon, reply)) in replies.iter().enumerate() {
            for task in &reply.tasks {
                let base = format!("{cordon}/{index}/{}/{}", task.job_id, task.task_name);
                metrics
                    .isolate_turns_total
                    .add(deltas.delta(format!("{base}/turns"), task.turns));
                metrics
                    .isolate_cpu_microseconds_total
                    .add(deltas.delta(format!("{base}/cpu"), task.cpu_micros));
                for (from, count) in &task.starts {
                    let delta = deltas.delta(format!("{base}/start/{from}"), *count);
                    if delta > 0 && from != "hibernated" {
                        metrics.isolate_wakes_total.inc_by(&[from.as_str()], delta);
                    }
                }
            }
        }
        let mut by_cordon: BTreeMap<&str, (u64, u32, u32)> = BTreeMap::new();
        for host in hosts {
            let entry = by_cordon.entry(host.cordon.as_str()).or_default();
            entry.0 += host.rss_bytes;
            entry.1 += host.slots_used;
            entry.2 += host.slots.saturating_sub(host.slots_used);
        }
        for (cordon, (rss, used, free)) in by_cordon {
            metrics.isolate_host_rss_bytes.set(&[cordon], rss as i64);
            metrics
                .isolate_slots
                .set(&[cordon, "used"], i64::from(used));
            metrics
                .isolate_slots
                .set(&[cordon, "free"], i64::from(free));
        }
    }

    // -----------------------------------------------------------------
    // Memory: paging out, pressure, drains
    // -----------------------------------------------------------------

    /// Ask every host for memory back. `PageOut` writes what the host names
    /// to its cgroup's `memory.reclaim` (only where the node can page out);
    /// `Hibernate` hibernates idle sessions and drops idle tenants. Returns
    /// the bytes given back (an estimate).
    ///
    /// `cordons` narrows the hosts asked; `job_id` (non-empty) narrows each
    /// host's reclaim to that job's isolates (`ReclaimRequest.job_id`).
    pub async fn reclaim(
        &self,
        level: host::ReclaimLevel,
        target_bytes: u64,
        min_idle: Duration,
        cordons: Option<&[String]>,
        job_id: &str,
    ) -> u64 {
        if level == host::ReclaimLevel::ReclaimPageOut && self.paging_unavailable.is_some() {
            return 0;
        }
        let replicas: Vec<Arc<Replica>> = lock(&self.state)
            .cordons
            .iter()
            .filter(|(name, _)| cordons.is_none_or(|only| only.contains(name)))
            .flat_map(|(_, c)| c.replicas.clone())
            .collect();
        let mut freed = 0u64;
        for replica in replicas {
            if target_bytes > 0 && freed >= target_bytes {
                break;
            }
            let Some(conn) = replica.conn() else { continue };
            let mut client = conn.client.clone();
            let Ok(reply) = client
                .reclaim(host::ReclaimRequest {
                    level: level as i32,
                    target_bytes: target_bytes.saturating_sub(freed),
                    min_idle_millis: min_idle.as_millis() as u64,
                    job_id: job_id.to_string(),
                    ..Default::default()
                })
                .await
            else {
                continue;
            };
            let reply = reply.into_inner();
            if level == host::ReclaimLevel::ReclaimPageOut && reply.bytes > 0 {
                if let Some(cgroup) = conn.cgroup.as_ref() {
                    let path = cgroup.join("memory.reclaim");
                    let bytes = reply.bytes;
                    let _ = tokio::task::spawn_blocking(move || {
                        std::fs::write(path, bytes.to_string())
                    })
                    .await;
                }
            }
            freed = freed.saturating_add(reply.bytes);
        }
        freed
    }

    /// The pressure responder's entry (a blocking thread): page out on its
    /// `reclaim` step, hibernate on its `suspend` step.
    pub fn reclaim_blocking(&self, hibernate: bool, target_bytes: u64) -> u64 {
        let Some(handle) = lock(&self.runtime).clone() else {
            return 0;
        };
        let level = if hibernate {
            host::ReclaimLevel::ReclaimHibernate
        } else {
            host::ReclaimLevel::ReclaimPageOut
        };
        handle.block_on(self.reclaim(level, target_bytes, Duration::ZERO, None, ""))
    }

    /// One maintenance pass: discover tasks, hibernate drained jobs'
    /// sessions, evict checkpoints past `suspend.keepFor`.
    pub async fn maintain(&self) {
        self.sync_from_task_manager();
        // `operator runtime disable isolate --drain`: hibernate the session
        // isolates of the covered jobs only (`ReclaimRequest.job_id`), in
        // the cordons that serve them.
        let drained: BTreeMap<String, Vec<String>> = {
            let state = lock(&self.state);
            let mut by_job: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for task in state
                .tasks
                .values()
                .filter(|task| self.gate.drains(ISOLATE_RUNTIME, &task.job_id))
            {
                let cordons = by_job.entry(task.job_id.clone()).or_default();
                if !cordons.contains(&task.cordon) {
                    cordons.push(task.cordon.clone());
                }
            }
            by_job
        };
        if !drained.is_empty() {
            let resident: Vec<String> = lock(&self.sessions)
                .records()
                .filter(|r| r.state == SessionState::Resident)
                .map(|r| r.job_id.clone())
                .collect();
            for (job_id, cordons) in &drained {
                if !resident.contains(job_id) {
                    continue;
                }
                self.reclaim(
                    host::ReclaimLevel::ReclaimHibernate,
                    0,
                    Duration::ZERO,
                    Some(cordons),
                    job_id,
                )
                .await;
            }
        }
        // keepFor: a hibernated session whose checkpoint is older ends.
        let now = crate::util::now_unix_ms();
        let expired: Vec<String> = {
            let state = lock(&self.state);
            lock(&self.sessions)
                .records()
                .filter(|r| r.state == SessionState::Hibernated)
                .filter(|r| {
                    let keep_for = state
                        .tasks
                        .get(&(r.job_id.clone(), r.task_name.clone()))
                        .map(|t| t.keep_for)
                        .unwrap_or(Duration::ZERO);
                    !keep_for.is_zero()
                        && r.checkpoint
                            .as_ref()
                            .is_some_and(|c| now - c.taken_unix_ms > keep_for.as_millis() as i64)
                })
                .map(|r| r.session_id.clone())
                .collect()
        };
        for id in expired {
            self.stop_session(&id, "suspend.keepFor passed").await;
        }
    }

    /// Run [`Self::maintain`] every second and page out every
    /// `isolate.pageOutAfter`, until the supervisor stops.
    pub fn spawn_maintenance(self: &Arc<Self>) {
        *lock(&self.runtime) = tokio::runtime::Handle::try_current().ok();
        let me = Arc::downgrade(self);
        let page_out_after = self.config.page_out_after().unwrap_or_default();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_page_out = Instant::now();
            loop {
                tick.tick().await;
                let Some(me) = me.upgrade() else { return };
                if me.stopping.load(Ordering::Acquire) {
                    return;
                }
                me.maintain().await;
                if !page_out_after.is_zero() && last_page_out.elapsed() >= page_out_after {
                    last_page_out = Instant::now();
                    me.reclaim(
                        host::ReclaimLevel::ReclaimPageOut,
                        0,
                        page_out_after,
                        None,
                        "",
                    )
                    .await;
                }
            }
        });
    }

    /// Worker shutdown: hibernate every session isolate, then stop the
    /// hosts.
    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        let replicas: Vec<Arc<Replica>> = lock(&self.state)
            .cordons
            .values()
            .flat_map(|c| c.replicas.clone())
            .collect();
        for replica in replicas {
            let Some(conn) = replica.conn() else { continue };
            let mut client = conn.client.clone();
            let drained = tokio::time::timeout(
                DRAIN_GRACE + Duration::from_secs(10),
                client.drain(host::DrainRequest {
                    hibernate: true,
                    grace_millis: DRAIN_GRACE.as_millis() as u32,
                }),
            )
            .await;
            if let Ok(Ok(reply)) = drained {
                info!(cordon = %replica.cordon, replica = replica.index, hibernated = reply.get_ref().hibernated, "Isolate host drained");
            }
            (conn.kill)();
        }
        lock(&self.sessions).save();
    }
}

fn same_checkpoint(a: &StoredCheckpoint, b: Option<&StoredCheckpoint>) -> bool {
    b.is_some_and(|b| a.digest == b.digest && a.path == b.path)
}

/// What [`IsolateSupervisor::decide`] found.
#[derive(Debug, Clone)]
struct Decided {
    tier: Tier,
    imports: Vec<String>,
    missing: Vec<String>,
    memo_key: [u8; 32],
    has_vm: bool,
    image: Option<String>,
    /// The run's entry file, relative to the run directory.
    entry_name: Option<String>,
}

/// The most `args` a run may carry, and their total size.
pub const MAX_RUN_ARGS: usize = 64;
pub const MAX_RUN_ARGS_BYTES: usize = 64 << 10;

/// A call that names a session isolate may not name a different tenant:
/// the session's own tenant is the call's.
fn check_session_tenant(session_id: &str, own: &str, asked: &str) -> Result<(), Status> {
    if !asked.is_empty() && asked != own {
        return Err(Status::invalid_argument(format!(
            "session isolate {session_id} belongs to {}, not tenant {asked:?}; a call in a \
             session uses the session's tenant\nHint: drop the tenant (Legion-Tenant), or \
             start a session for that tenant",
            if own.is_empty() {
                "no tenant".to_string()
            } else {
                format!("tenant {own:?}")
            }
        )));
    }
    Ok(())
}

/// The run's files as a VM-tier run stages them, and the path of its
/// entry there. The entry is always staged under a fixed name for the task,
/// never one the caller chose (security review N4): the image's VM entry
/// (`main.py`, `main.mjs`), or `main` for a module task. The run's `args`
/// are appended to the VM task's `run.command` only when its last element
/// is exactly that path, so a caller cannot name a file to match a
/// command's operand. A task with `config.entry` and no run entry stages
/// its configured entry there (the run's files are data).
pub(crate) fn stage_for_vm(
    image: Option<&str>,
    entry_name: Option<&str>,
    configured: Option<host::File>,
    files: Vec<(String, Vec<u8>, bool)>,
) -> Result<(Vec<crate::structs::RunFile>, String), Status> {
    let vm_entry = image
        .map(crate::structs::isolate::vm_entry_name)
        .unwrap_or("main");
    let has_entry = entry_name.is_some() || configured.is_some();
    let mut staged = Vec::with_capacity(files.len() + 1);
    for (path, content, executable) in files {
        if path == crate::structs::run::RUN_INPUT_FILE {
            continue;
        }
        let path = if entry_name == Some(path.as_str()) {
            vm_entry.to_string()
        } else if has_entry && path == vm_entry {
            return Err(Status::invalid_argument(format!(
                "file {path} has the name the VM tier stages the run's entry under; rename it"
            )));
        } else {
            path
        };
        staged.push(crate::structs::RunFile {
            path,
            content,
            executable,
        });
    }
    if entry_name.is_none() {
        if let Some(entry) = configured {
            staged.push(crate::structs::RunFile {
                path: vm_entry.to_string(),
                content: entry.content,
                executable: false,
            });
        }
    }
    Ok((
        staged,
        format!("{}/{vm_entry}", crate::structs::run::RUN_DIR),
    ))
}

fn check_args(args: &[String]) -> Result<(), Status> {
    if args.len() > MAX_RUN_ARGS {
        return Err(Status::invalid_argument(format!(
            "the run has {} args; at most {MAX_RUN_ARGS}",
            args.len()
        )));
    }
    let bytes: usize = args.iter().map(String::len).sum();
    if bytes > MAX_RUN_ARGS_BYTES {
        return Err(Status::invalid_argument(format!(
            "the run's args are {bytes} bytes together; at most {MAX_RUN_ARGS_BYTES} (64 KiB)"
        )));
    }
    Ok(())
}

/// The `legion_isolate_vm_runs_total` reason label for a tier reason.
fn vm_reason_label(reason: &str) -> &'static str {
    if reason == "requested" {
        "requested"
    } else if reason.contains("disabled") {
        "disabled"
    } else if reason.contains("import") {
        "import"
    } else {
        "memo"
    }
}

fn placeholder_resolved() -> ResolvedIsolate {
    ResolvedIsolate {
        image: None,
        entry: None,
        module: None,
        lifetime: IsolateLifetime::Request,
        cordon: Default::default(),
        cpu: Duration::ZERO,
        wall: Duration::ZERO,
        wall_text: String::new(),
        output: 0,
        vm_task: None,
        memo_for: Duration::ZERO,
        env: BTreeMap::new(),
    }
}

/// A task entry from its spec, or why the node cannot run it.
fn build_entry(
    job_id: &str,
    task: TaskConfig,
    room: TaskRoom,
    images: &Option<ImageCatalog>,
) -> Result<TaskEntry, String> {
    let config = IsolateTaskConfig::of(&task)
        .map_err(|err| format!("{err:#}"))?
        .ok_or("not an isolate task")?;
    let resolved = config.resolve()?;
    let suspend = task
        .suspend
        .as_ref()
        .map(|s| s.resolve())
        .transpose()?
        .unwrap_or_default();
    if suspend.durability == crate::structs::snapshot::SnapshotDurability::Store
        && resolved.lifetime == IsolateLifetime::Session
    {
        return Err(
            "suspend.durability = \"store\" is not available for isolate sessions yet: \
                    their checkpoints stay on the node that took them; use durability = \
                    \"local\""
                .into(),
        );
    }
    let image_modules = resolved
        .image
        .as_ref()
        .and_then(|image| {
            images
                .as_ref()
                .and_then(|catalog| catalog.images.get(image.facts.name))
                .map(|entry| entry.modules.clone())
        })
        .unwrap_or_default();
    let (readiness, _) = watch::channel(Readiness::Preparing);
    Ok(TaskEntry {
        job_id: job_id.to_string(),
        task_name: task.name.clone(),
        cordon: crate::structs::isolate::cordon_name(job_id, &config),
        envelope_allocation_id: 0,
        vm_task: crate::structs::isolate::vm_task_name(&task, &config),
        room,
        envelope_memory_mib: 0.0,
        fingerprint: String::new(),
        readiness,
        attempted: Instant::now(),
        module: None,
        entry: None,
        hibernate_after: suspend
            .idle
            .as_ref()
            .map(|idle| idle.after)
            .unwrap_or_default(),
        keep_for: suspend.keep_for,
        image_modules,
        resolved,
        task,
    })
}

#[tonic::async_trait]
impl super::IngressTarget for IsolateSupervisor {
    async fn handle(
        &self,
        job_id: &str,
        task_name: &str,
        tenant: &str,
        session_id: &str,
        request: host::HttpCall,
    ) -> Result<host::HttpOutcome, Status> {
        self.http(job_id, task_name, tenant, session_id, request)
            .await
    }
}
