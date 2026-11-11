//! The host without its sockets: modules, tasks, instances, invocations,
//! sessions, and memory reclaim, behind an API the gRPC shell
//! ([`crate::service`]) and the tests call directly.
//!
//! **Where things run.** RPC handlers run on the host's small tokio runtime.
//! An invocation, from admission to its reply, is one task on a shard
//! ([`crate::shard`]): guest code only ever executes there, and waiting
//! (for admission, for a busy tenant instance, for the worker) is a pending
//! future, not a blocked thread. Tenant and session instances stay on the
//! shard that made them; a request instance goes to the less loaded of two
//! random shards.
//!
//! **Admission** (per task): at most `max_active` invocations run at once,
//! and live instances hold at most `max_resident` reservations; a new
//! instance that does not fit first evicts the task's coldest idle tenant
//! instance, else waits in FIFO order up to `queue_timeout_millis`. A pool
//! that has no free slot is the same `RESOURCE_EXHAUSTED`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use legion_proto::isolate_host::{
    invoke_reply, invoke_request, CheckpointHeader, ConfigureTaskReply, Confinement, DrainReply,
    DrainRequest, File, HelloReply, HostConfig, HttpCall, HttpOutcome, Lifetime, LoadModuleReply,
    LoadModuleRequest, LogBatch, LogLine, OpenSessionReply, OpenSessionRequest, Origin,
    ReclaimLevel, ReclaimReply, ReclaimRequest, RemoveTaskReply, RemoveTaskRequest,
    ResizeSessionReply, ResizeSessionRequest, RunCall, RunOutcome, SessionInfo, StatsReply,
    Stopped, TaskSpec, TaskStats, UnloadModuleReply, UnloadModuleRequest, PROTOCOL_VERSION,
};
use legion_proto::isolate_host::{InvokeReply, InvokeRequest};
use rand_010::RngExt as _;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tonic::Status;
use tracing::{debug, info, instrument, warn};
use wasmtime::component::Instance;
use wasmtime::{Engine, Store, UpdateDeadline};

use crate::checkpoint::{Checkpoint, SessionMeta};
use crate::engine::{self, EngineParams, PoolSize, ENGINE_VERSION};
use crate::guest::{
    collect_body, fmt_duration, Ctx, GuestSetup, Ident, Invocation, StateMap, Stop,
};
use crate::module::LoadedModule;
use crate::scratch::RUN_DIR;
use crate::shard::{allowed_cpus, Shard};
use crate::turn;
use crate::worker_api::WorkerApi;
use crate::world::runner;

/// Defaults for `HostConfig` fields left at 0.
pub const DEFAULT_SLOTS: u32 = 1000;
pub const DEFAULT_EPOCH_MILLIS: u32 = 1;
pub const DEFAULT_SLICE_MILLIS: u32 = 5;
pub const DEFAULT_MAX_BODY: u64 = 16 << 20;
pub const DEFAULT_STACK: u64 = 128 << 10;

/// Defaults for `TaskSpec` limits left at 0.
const DEFAULT_CPU: Duration = Duration::from_secs(10);
const DEFAULT_WALL: Duration = Duration::from_secs(60);
const DEFAULT_OUTPUT: u64 = 1 << 20;
const DEFAULT_STATE_QUOTA: u64 = 1 << 20;
/// `max_active = 0` means no limit beyond the slots.
const UNLIMITED_ACTIVE: usize = 1 << 20;

type TaskKey = (String, String);

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn mib(bytes: u64) -> String {
    if bytes >= 1 << 20 && bytes.is_multiple_of(1 << 20) {
        format!("{}MiB", bytes >> 20)
    } else if bytes >= 1 << 20 {
        format!("{:.1}MiB", bytes as f64 / (1 << 20) as f64)
    } else {
        format!("{}KiB", bytes.div_ceil(1024))
    }
}

fn task_name(job: &str, task: &str) -> String {
    format!("{job}/{task}")
}

// ---------------------------------------------------------------------------
// Per-task bookkeeping
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Counters {
    invocations: AtomicU64,
    turns: AtomicU64,
    cpu_micros: AtomicU64,
    refusals: AtomicU64,
    instantiations: AtomicU64,
    instantiate_micros: AtomicU64,
    hibernations: AtomicU64,
    queued: AtomicU32,
    active: AtomicU32,
    maps: Mutex<CounterMaps>,
}

#[derive(Default)]
struct CounterMaps {
    stopped: HashMap<String, u64>,
    limits: HashMap<String, u64>,
    starts: HashMap<String, u64>,
}

struct TaskEntry {
    key: TaskKey,
    spec: Mutex<Arc<TaskSpec>>,
    module: Mutex<Arc<LoadedModule>>,
    active: Arc<Semaphore>,
    active_max: Mutex<usize>,
    /// Reservation units held by live instances.
    resident: Mutex<u64>,
    resident_freed: Notify,
    live: AtomicU32,
    tenants: Mutex<HashMap<String, Arc<Holder>>>,
    counters: Counters,
    removed: AtomicBool,
}

impl TaskEntry {
    fn spec(&self) -> Arc<TaskSpec> {
        self.spec.lock().unwrap().clone()
    }
    fn module(&self) -> Arc<LoadedModule> {
        self.module.lock().unwrap().clone()
    }
    fn name(&self) -> String {
        task_name(&self.key.0, &self.key.1)
    }
    /// One instance's default charge: the reservation in bytes, or 1 when
    /// the task names none (then `max_resident` counts instances).
    fn unit(spec: &TaskSpec) -> u64 {
        spec.memory_reservation_bytes.max(1)
    }
    fn capacity(spec: &TaskSpec) -> u64 {
        if spec.max_resident == 0 {
            u64::MAX
        } else {
            spec.max_resident as u64 * Self::unit(spec)
        }
    }
    fn count(&self, map: fn(&mut CounterMaps) -> &mut HashMap<String, u64>, k: &str) {
        *map(&mut self.counters.maps.lock().unwrap())
            .entry(k.to_string())
            .or_default() += 1;
    }
}

/// A reservation held by a live instance; given back on drop.
struct Charge {
    task: Arc<TaskEntry>,
    units: u64,
    host: Arc<Inner>,
}

impl Drop for Charge {
    fn drop(&mut self) {
        *self.task.resident.lock().unwrap() -= self.units;
        self.task.live.fetch_sub(1, Ordering::Relaxed);
        self.host.slots_used.fetch_sub(1, Ordering::Relaxed);
        self.task.resident_freed.notify_waiters();
    }
}

/// A live instance.
struct Live {
    // Drop order: the store (and its pool slot) before the charge.
    store: Store<Ctx>,
    instance: Instance,
    charge: Charge,
}

impl Live {
    fn memory_bytes(&self) -> u64 {
        self.store.data().limiter.linear()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HolderKind {
    Tenant,
    Session,
}

/// Where a tenant's or a session's instance lives between calls.
struct Holder {
    kind: HolderKind,
    /// The tenant key, or the session id.
    key: String,
    job: String,
    task: String,
    tenant: String,
    shard: usize,
    inst: tokio::sync::Mutex<Option<Live>>,
    meta: Mutex<HolderMeta>,
}

#[derive(Default)]
struct HolderMeta {
    last_active: Option<Instant>,
    last_active_unix_ms: i64,
    paged_out: bool,
    memory_bytes: u64,
    state_bytes: u64,
    invocations: u64,
    active: bool,
    has_instance: bool,
    /// A session's state while it has no instance (opened, or its instance
    /// was dropped after a trap).
    state: Option<StateMap>,
    /// A session's own ceiling and reservation (`ResizeSession`); 0 = the
    /// task's.
    memory_max: u64,
    reservation: u64,
    closed: bool,
    hibernations: u64,
    /// The next instance is made from a checkpoint (`control.info().resumed`).
    resumed: bool,
    /// The invocation running in it now, to cut it.
    running: Option<Arc<Running>>,
}

impl Holder {
    fn idle_for(&self) -> Option<Duration> {
        let m = self.meta.lock().unwrap();
        if m.active || !m.has_instance {
            return None;
        }
        m.last_active.map(|t| t.elapsed())
    }
}

/// A cut request for an in-flight invocation.
struct Running {
    cancel: Notify,
    why: Mutex<Option<Stop>>,
}

impl Running {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: Notify::new(),
            why: Mutex::new(None),
        })
    }
    fn cut(&self, why: Stop) {
        self.why.lock().unwrap().get_or_insert(why);
        self.cancel.notify_one();
    }
}

// ---------------------------------------------------------------------------
// The host
// ---------------------------------------------------------------------------

/// The jobspec's word for a lifetime (`config.lifetime`).
fn lifetime_word(l: Lifetime) -> &'static str {
    match l {
        Lifetime::Request => "request",
        Lifetime::Tenant => "tenant",
        Lifetime::Session => "session",
    }
}

/// What a [`Host`] is built from.
pub struct HostSetup {
    pub config: HostConfig,
    pub worker: Arc<dyn WorkerApi>,
    /// The runtime timers and worker calls run on. Shard threads enter it.
    pub rt: tokio::runtime::Handle,
    /// What confinement applied (reported in `Hello`).
    pub confinement: Confinement,
    /// `/proc/self/statm`, opened before confinement, for RSS.
    pub statm: Option<std::fs::File>,
}

pub struct Inner {
    cfg: HostConfig,
    params: EngineParams,
    engine: Engine,
    linker: wasmtime::component::Linker<Ctx>,
    compat_key: String,
    shards: Vec<Shard>,
    worker: Arc<dyn WorkerApi>,
    rt: tokio::runtime::Handle,
    read_dirs: Vec<PathBuf>,
    confinement: Confinement,
    statm: Mutex<Option<std::fs::File>>,
    modules: Mutex<HashMap<String, Arc<LoadedModule>>>,
    tasks: Mutex<HashMap<TaskKey, Arc<TaskEntry>>>,
    sessions: Mutex<HashMap<String, Arc<Holder>>>,
    draining: AtomicBool,
    stopping: AtomicBool,
    slots_used: AtomicU32,
    inflight: AtomicU32,
    started_unix_ms: i64,
    drained: Notify,
}

/// The isolate host. Cheap to clone.
#[derive(Clone)]
pub struct Host(Arc<Inner>);

/// Fill in `HostConfig` defaults.
pub fn normalize(mut c: HostConfig) -> HostConfig {
    if c.slots == 0 {
        c.slots = DEFAULT_SLOTS;
    }
    if c.slot_bytes == 0 {
        c.slot_bytes = EngineParams::default().slot_bytes;
    }
    if c.epoch_millis == 0 {
        c.epoch_millis = DEFAULT_EPOCH_MILLIS;
    }
    if c.slice_millis == 0 {
        c.slice_millis = DEFAULT_SLICE_MILLIS;
    }
    if c.max_body_bytes == 0 {
        c.max_body_bytes = DEFAULT_MAX_BODY;
    }
    if c.async_stack_bytes == 0 {
        c.async_stack_bytes = DEFAULT_STACK;
    }
    c
}

impl Host {
    /// Build the engine, start the epoch ticker and the shards.
    pub fn new(setup: HostSetup) -> anyhow::Result<Self> {
        if setup.config.protocol_version != PROTOCOL_VERSION {
            anyhow::bail!(
                "the worker speaks isolate host protocol {}, this host speaks {}; \
                 install the legion-isolate that ships with the worker",
                setup.config.protocol_version,
                PROTOCOL_VERSION
            );
        }
        let cfg = normalize(setup.config);
        let params = EngineParams::from_host_config(&cfg);
        let engine = engine::host_engine(
            &params,
            PoolSize {
                slots: cfg.slots,
                async_stack_bytes: cfg.async_stack_bytes,
            },
        )?;
        let compat_key = engine::compat_key_for(&engine, &params);
        let linker = crate::module::linker(&engine)?;
        let cpus = allowed_cpus();
        let n = if cfg.shards == 0 {
            cpus.len().max(1)
        } else {
            cfg.shards as usize
        };
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let cpu = (cfg.shards == 0).then(|| cpus[i % cpus.len().max(1)]);
            shards.push(Shard::start(
                i,
                cpu,
                Duration::from_millis(cfg.slice_millis as u64),
                setup.rt.clone(),
            )?);
        }
        let read_dirs = cfg
            .read_dirs
            .iter()
            .filter_map(|d| std::fs::canonicalize(d).ok())
            .collect();
        let inner = Arc::new(Inner {
            params,
            engine,
            linker,
            compat_key,
            shards,
            worker: setup.worker,
            rt: setup.rt,
            read_dirs,
            confinement: setup.confinement,
            statm: Mutex::new(setup.statm),
            modules: Mutex::default(),
            tasks: Mutex::default(),
            sessions: Mutex::default(),
            draining: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            slots_used: AtomicU32::new(0),
            inflight: AtomicU32::new(0),
            started_unix_ms: unix_ms(),
            drained: Notify::new(),
            cfg,
        });
        start_ticker(&inner);
        start_reaper(&inner);
        Ok(Self(inner))
    }

    pub fn config(&self) -> &HostConfig {
        &self.0.cfg
    }

    pub fn compat_key(&self) -> &str {
        &self.0.compat_key
    }

    pub fn engine(&self) -> &Engine {
        &self.0.engine
    }

    /// Stop the ticker, the reaper, and the shards.
    pub fn shutdown(&self) {
        self.0.stopping.store(true, Ordering::Release);
        for s in &self.0.shards {
            s.shutdown();
        }
    }

    pub fn hello(&self) -> HelloReply {
        let c = &self.0.cfg;
        HelloReply {
            protocol_version: PROTOCOL_VERSION,
            engine_version: ENGINE_VERSION.into(),
            compat_key: self.0.compat_key.clone(),
            pid: crate::confine::worker_visible_pid(),
            shards: self.0.shards.len() as u32,
            slots: c.slots,
            layout: self.0.params.layout as i32,
            confinement: Some(self.0.confinement.clone()),
        }
    }

    // -- modules ------------------------------------------------------------

    fn inside_read_dirs(&self, path: &Path) -> Result<PathBuf, Status> {
        let real = std::fs::canonicalize(path)
            .map_err(|e| Status::not_found(format!("artifact {}: {e}", path.display())))?;
        if self.0.read_dirs.iter().any(|d| real.starts_with(d)) {
            Ok(real)
        } else {
            Err(Status::permission_denied(format!(
                "artifact {} is outside the host's artifact directories; the host loads \
                 only what its worker's compiler wrote",
                path.display()
            )))
        }
    }

    #[instrument(skip_all, fields(module_id = %req.module_id))]
    pub fn load_module(&self, req: LoadModuleRequest) -> Result<LoadModuleReply, Status> {
        let reply = |m: &LoadedModule, already| LoadModuleReply {
            exports_cli_run: m.exports.cli_run,
            exports_http_handler: m.exports.http_handler,
            exports_runner: m.exports.runner,
            initial_memory_bytes: m.initial_memory_bytes,
            already_loaded: already,
        };
        if let Some(m) = self.0.modules.lock().unwrap().get(&req.module_id) {
            return Ok(reply(m, true));
        }
        let path = self.inside_read_dirs(Path::new(&req.artifact_path))?;
        // SAFETY: the path is inside a directory only the worker's compiler
        // writes (checked above, after resolving links).
        let m =
            unsafe { LoadedModule::load(&self.0.engine, &self.0.linker, &req.module_id, &path) }
                .map_err(|e| {
                    Status::failed_precondition(format!(
                        "module {} cannot be loaded: {e:#}",
                        req.module_id
                    ))
                })?;
        let r = reply(&m, false);
        self.0
            .modules
            .lock()
            .unwrap()
            .entry(req.module_id.clone())
            .or_insert(m);
        info!(exports = ?r, "module loaded");
        Ok(r)
    }

    pub fn unload_module(&self, req: UnloadModuleRequest) -> Result<UnloadModuleReply, Status> {
        let used = self
            .0
            .tasks
            .lock()
            .unwrap()
            .values()
            .any(|t| t.module().module_id == req.module_id);
        if used {
            return Err(Status::failed_precondition(format!(
                "module {} is used by a task; remove the task first",
                req.module_id
            )));
        }
        Ok(UnloadModuleReply {
            unloaded: self
                .0
                .modules
                .lock()
                .unwrap()
                .remove(&req.module_id)
                .is_some(),
        })
    }

    // -- tasks --------------------------------------------------------------

    #[instrument(skip_all, fields(job_id = %spec.job_id, task = %spec.task_name))]
    pub fn configure_task(&self, spec: TaskSpec) -> Result<ConfigureTaskReply, Status> {
        let module = self
            .0
            .modules
            .lock()
            .unwrap()
            .get(&spec.module_id)
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "module {} is not loaded; LoadModule it first",
                    spec.module_id
                ))
            })?;
        if spec.memory_max_bytes > self.0.cfg.slot_bytes {
            return Err(Status::invalid_argument(format!(
                "memoryMax {} is above this host's slot size {}",
                mib(spec.memory_max_bytes),
                mib(self.0.cfg.slot_bytes)
            )));
        }
        let life = Lifetime::try_from(spec.lifetime).unwrap_or(Lifetime::Request);
        if life != Lifetime::Request && !module.exports.runner && !module.exports.http_handler {
            return Err(Status::failed_precondition(format!(
                "lifetime \"{}\" needs a component that exports legion:isolate/runner or \
                 wasi:http/incoming-handler; wasi:cli/run is single-shot",
                lifetime_word(life)
            )));
        }
        let key = (spec.job_id.clone(), spec.task_name.clone());
        let max_active = if spec.max_active == 0 {
            UNLIMITED_ACTIVE
        } else {
            spec.max_active as usize
        };
        let mut tasks = self.0.tasks.lock().unwrap();
        match tasks.get(&key) {
            Some(t) => {
                let mut cur = t.active_max.lock().unwrap();
                if max_active > *cur {
                    t.active.add_permits(max_active - *cur);
                } else if max_active < *cur {
                    t.active.forget_permits(*cur - max_active);
                }
                *cur = max_active;
                *t.spec.lock().unwrap() = Arc::new(spec);
                *t.module.lock().unwrap() = module;
                t.resident_freed.notify_waiters();
            }
            None => {
                info!("task configured");
                tasks.insert(
                    key.clone(),
                    Arc::new(TaskEntry {
                        key,
                        spec: Mutex::new(Arc::new(spec)),
                        module: Mutex::new(module),
                        active: Arc::new(Semaphore::new(max_active)),
                        active_max: Mutex::new(max_active),
                        resident: Mutex::new(0),
                        resident_freed: Notify::new(),
                        live: AtomicU32::new(0),
                        tenants: Mutex::default(),
                        counters: Counters::default(),
                        removed: AtomicBool::new(false),
                    }),
                );
            }
        }
        Ok(ConfigureTaskReply {})
    }

    #[instrument(skip_all, fields(job_id = %req.job_id, task = %req.task_name))]
    pub async fn remove_task(&self, req: RemoveTaskRequest) -> Result<RemoveTaskReply, Status> {
        let key = (req.job_id.clone(), req.task_name.clone());
        let task =
            self.0.tasks.lock().unwrap().remove(&key).ok_or_else(|| {
                Status::not_found(format!("no task {}", task_name(&key.0, &key.1)))
            })?;
        task.removed.store(true, Ordering::Release);
        let mut dropped = 0;
        let tenants: Vec<_> = task
            .tenants
            .lock()
            .unwrap()
            .drain()
            .map(|(_, h)| h)
            .collect();
        let sessions: Vec<_> = {
            let mut s = self.0.sessions.lock().unwrap();
            let ids: Vec<_> = s
                .iter()
                .filter(|(_, h)| h.job == key.0 && h.task == key.1)
                .map(|(k, _)| k.clone())
                .collect();
            ids.into_iter().filter_map(|id| s.remove(&id)).collect()
        };
        for h in tenants.into_iter().chain(sessions) {
            if drop_instance(&h, Stop::new("ended", "its task was removed")).await {
                dropped += 1;
            }
        }
        info!(dropped, "task removed");
        Ok(RemoveTaskReply { dropped })
    }

    fn task(&self, job: &str, task: &str) -> Result<Arc<TaskEntry>, Status> {
        self.0
            .tasks
            .lock()
            .unwrap()
            .get(&(job.to_string(), task.to_string()))
            .cloned()
            .ok_or_else(|| Status::not_found(format!("no task {}", task_name(job, task))))
    }

    // -- invocations --------------------------------------------------------

    /// Run one invocation. See `IsolateHost.Invoke` for the error contract.
    #[instrument(skip_all, fields(job_id = %req.job_id, task = %req.task_name, invocation = %req.invocation_id))]
    pub async fn invoke(&self, req: InvokeRequest) -> Result<InvokeReply, Status> {
        let t0 = Instant::now();
        if self.0.draining.load(Ordering::Acquire) {
            return Err(Status::unavailable("the isolate host is draining"));
        }
        let task = self.task(&req.job_id, &req.task_name)?;
        let spec = task.spec();
        let module = task.module();
        let life = Lifetime::try_from(spec.lifetime).unwrap_or(Lifetime::Request);
        let call = req
            .call
            .clone()
            .ok_or_else(|| Status::invalid_argument("the invocation has no call"))?;
        match &call {
            invoke_request::Call::Run(_) => {
                if !module.exports.cli_run && !module.exports.runner {
                    return Err(Status::failed_precondition(format!(
                        "task {} takes no runs: its component exports neither wasi:cli/run \
                         nor legion:isolate/runner",
                        task.name()
                    )));
                }
                if !module.exports.runner && life != Lifetime::Request {
                    return Err(Status::failed_precondition(format!(
                        "task {} has lifetime \"{}\" but its component exports only \
                         wasi:cli/run, which is single-shot: use lifetime \"request\" or \
                         export legion:isolate/runner",
                        task.name(),
                        lifetime_word(life)
                    )));
                }
            }
            invoke_request::Call::Http(_) => {
                if !module.exports.http_handler {
                    return Err(Status::failed_precondition(format!(
                        "task {} takes no HTTP requests: its component does not export \
                         wasi:http/incoming-handler",
                        task.name()
                    )));
                }
            }
        }
        let holder = match life {
            Lifetime::Request => None,
            Lifetime::Tenant => Some(self.tenant_holder(&task, &req.tenant)),
            Lifetime::Session => {
                if req.session_id.is_empty() {
                    return Err(Status::invalid_argument(
                        "a session isolate's invocation needs a session_id",
                    ));
                }
                Some(self.session_holder(&req, &task).await?)
            }
        };
        let shard = match &holder {
            Some(h) => self.0.shards[h.shard].clone(),
            None => self.pick_shard(),
        };
        let origin = Origin {
            job_id: req.job_id.clone(),
            task_name: req.task_name.clone(),
            isolate_id: if life == Lifetime::Session {
                req.session_id.clone()
            } else {
                req.invocation_id.clone()
            },
            tenant: req.tenant.clone(),
            session: life == Lifetime::Session,
        };
        let limits = spec.limits.unwrap_or_default();
        let nz = |us: u64, d: Duration| {
            if us == 0 {
                d
            } else {
                Duration::from_micros(us)
            }
        };
        let inv = Arc::new(Invocation::new(
            origin,
            nz(limits.cpu_micros, DEFAULT_CPU),
            nz(limits.wall_micros, DEFAULT_WALL),
            Duration::from_millis(self.0.cfg.slice_millis as u64),
        ));
        let flow = (
            req.job_id.clone(),
            req.task_name.clone(),
            req.tenant.clone(),
        );
        let weight = spec.cpu_weight.max(1);
        let job = Job {
            host: self.0.clone(),
            task: task.clone(),
            spec,
            module,
            holder,
            call,
            inv: inv.clone(),
            t0,
            shard: shard.id(),
        };
        task.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.0.inflight.fetch_add(1, Ordering::AcqRel);
        let rx = shard.spawn(flow, weight, Some(inv), job.run());
        let out = rx
            .await
            .unwrap_or_else(|_| Err(Status::unavailable("the isolate host is shutting down")));
        if self.0.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
        out
    }

    fn pick_shard(&self) -> Shard {
        let s = &self.0.shards;
        if s.len() == 1 {
            return s[0].clone();
        }
        let mut rng = rand_010::rng();
        let a = rng.random_range(0..s.len());
        let b = rng.random_range(0..s.len());
        if s[a].stats().live <= s[b].stats().live {
            s[a].clone()
        } else {
            s[b].clone()
        }
    }

    fn tenant_holder(&self, task: &Arc<TaskEntry>, tenant: &str) -> Arc<Holder> {
        let mut t = task.tenants.lock().unwrap();
        t.entry(tenant.to_string())
            .or_insert_with(|| {
                Arc::new(Holder {
                    kind: HolderKind::Tenant,
                    key: tenant.to_string(),
                    job: task.key.0.clone(),
                    task: task.key.1.clone(),
                    tenant: tenant.to_string(),
                    shard: self.pick_shard().id(),
                    inst: tokio::sync::Mutex::new(None),
                    meta: Mutex::new(HolderMeta {
                        state: Some(StateMap::default()),
                        ..Default::default()
                    }),
                })
            })
            .clone()
    }

    fn new_session(&self, meta: SessionMeta, state: StateMap, resumed: bool) -> Arc<Holder> {
        Arc::new(Holder {
            kind: HolderKind::Session,
            key: meta.session_id.clone(),
            job: meta.job_id.clone(),
            task: meta.task_name.clone(),
            tenant: meta.tenant.clone(),
            shard: self.pick_shard().id(),
            inst: tokio::sync::Mutex::new(None),
            meta: Mutex::new(HolderMeta {
                state: Some(state),
                memory_max: meta.memory_max_bytes,
                reservation: meta.memory_reservation_bytes,
                invocations: meta.invocations,
                hibernations: meta.hibernations,
                resumed,
                ..Default::default()
            }),
        })
    }

    /// The session's holder, restored from the worker's checkpoint when the
    /// host does not hold it.
    async fn session_holder(
        &self,
        req: &InvokeRequest,
        task: &Arc<TaskEntry>,
    ) -> Result<Arc<Holder>, Status> {
        if let Some(h) = self.0.sessions.lock().unwrap().get(&req.session_id) {
            if h.job != req.job_id || h.task != req.task_name {
                return Err(Status::not_found(format!(
                    "session {} belongs to another task",
                    req.session_id
                )));
            }
            return Ok(h.clone());
        }
        let origin = Origin {
            job_id: req.job_id.clone(),
            task_name: req.task_name.clone(),
            isolate_id: req.session_id.clone(),
            tenant: req.tenant.clone(),
            session: true,
        };
        let Some((header, body)) = self.0.worker.load_checkpoint(origin).await? else {
            return Err(Status::not_found(format!(
                "no session {} (not open here and no checkpoint)",
                req.session_id
            )));
        };
        // A checkpoint written under another engine version or module
        // still restores: what it holds (the state map and the session's
        // metadata) does not depend on either. Linear memory never
        // survives hibernation, so there is nothing engine-specific to
        // refuse.
        if header.engine_version != ENGINE_VERSION || header.module_id != task.module().module_id {
            debug!(
                engine = %header.engine_version,
                module = %header.module_id,
                "restoring a checkpoint from another engine or module at the state level"
            );
        }
        let cp = Checkpoint::decode(&body).map_err(|e| Status::data_loss(e.to_string()))?;
        let mut meta = cp.meta;
        meta.session_id = req.session_id.clone();
        meta.job_id = req.job_id.clone();
        meta.task_name = req.task_name.clone();
        if meta.tenant.is_empty() {
            meta.tenant = req.tenant.clone();
        }
        let h = self.new_session(meta, StateMap::from_map(cp.state), true);
        let mut s = self.0.sessions.lock().unwrap();
        Ok(s.entry(req.session_id.clone()).or_insert(h).clone())
    }

    // -- sessions -----------------------------------------------------------

    #[instrument(skip_all, fields(job_id = %req.job_id, task = %req.task_name, session_id = %req.session_id))]
    pub fn open_session(&self, req: OpenSessionRequest) -> Result<OpenSessionReply, Status> {
        let task = self.task(&req.job_id, &req.task_name)?;
        let spec = task.spec();
        if Lifetime::try_from(spec.lifetime).ok() != Some(Lifetime::Session) {
            return Err(Status::failed_precondition(format!(
                "task {} does not have lifetime \"session\"",
                task.name()
            )));
        }
        if req.memory_max_bytes > self.0.cfg.slot_bytes {
            return Err(Status::invalid_argument(format!(
                "memoryMax {} is above this host's slot size {}",
                mib(req.memory_max_bytes),
                mib(self.0.cfg.slot_bytes)
            )));
        }
        let mut s = self.0.sessions.lock().unwrap();
        if s.contains_key(&req.session_id) {
            return Ok(OpenSessionReply { existed: true });
        }
        let h = self.new_session(
            SessionMeta {
                session_id: req.session_id.clone(),
                job_id: req.job_id.clone(),
                task_name: req.task_name.clone(),
                tenant: req.tenant.clone(),
                memory_max_bytes: req.memory_max_bytes,
                memory_reservation_bytes: req.memory_reservation_bytes,
                ..Default::default()
            },
            StateMap::default(),
            false,
        );
        s.insert(req.session_id, h);
        Ok(OpenSessionReply { existed: false })
    }

    #[instrument(skip_all, fields(session_id = %session_id))]
    pub async fn close_session(&self, session_id: &str, reason: &str) -> bool {
        let h = self.0.sessions.lock().unwrap().remove(session_id);
        let Some(h) = h else {
            return false;
        };
        h.meta.lock().unwrap().closed = true;
        let why = if reason.is_empty() {
            "its session was closed".to_string()
        } else {
            format!("its session was closed: {reason}")
        };
        drop_instance(&h, Stop::new("ended", why)).await;
        true
    }

    pub fn get_session(&self, session_id: &str) -> Result<SessionInfo, Status> {
        let h = self
            .0
            .sessions
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("the host holds no session {session_id}")))?;
        let m = h.meta.lock().unwrap();
        Ok(SessionInfo {
            session_id: h.key.clone(),
            job_id: h.job.clone(),
            task_name: h.task.clone(),
            tenant: h.tenant.clone(),
            tier: if !m.has_instance {
                "empty"
            } else if m.paged_out {
                "paged_out"
            } else {
                "hot"
            }
            .into(),
            last_active_unix_ms: m.last_active_unix_ms,
            invocations: m.invocations,
            memory_bytes: m.memory_bytes,
            state_bytes: m.state_bytes,
            active: m.active,
        })
    }

    /// Change a session isolate's ceiling and reservation.
    #[instrument(skip_all, fields(session_id = %req.session_id))]
    pub async fn resize_session(
        &self,
        req: ResizeSessionRequest,
    ) -> Result<ResizeSessionReply, Status> {
        let h = self
            .0
            .sessions
            .lock()
            .unwrap()
            .get(&req.session_id)
            .cloned()
            .ok_or_else(|| {
                Status::not_found(format!("the host holds no session {}", req.session_id))
            })?;
        if req.memory_max_bytes > self.0.cfg.slot_bytes {
            return Err(Status::invalid_argument(format!(
                "memoryMax {} is above this host's slot size {}",
                mib(req.memory_max_bytes),
                mib(self.0.cfg.slot_bytes)
            )));
        }
        let task = self.task(&h.job, &h.task)?;
        let spec = task.spec();
        // Serialize with calls: a resize lands between invocations.
        let mut inst = h.inst.lock().await;
        let mut m = h.meta.lock().unwrap();
        let max = if req.memory_max_bytes == 0 {
            effective(m.memory_max, spec.memory_max_bytes)
        } else {
            req.memory_max_bytes
        };
        let reservation = if req.memory_reservation_bytes == 0 {
            effective(m.reservation, TaskEntry::unit(&spec))
        } else {
            req.memory_reservation_bytes
        };
        if let Some(live) = inst.as_mut() {
            let used = live.store.data().account().total();
            if max < used {
                return Err(Status::failed_precondition(format!(
                    "the session uses {} now; a ceiling of {} would take memory back, which \
                     a resize never does",
                    mib(used),
                    mib(max)
                )));
            }
            live.store.data().account().set_limit(max);
            // The worker admitted the change against the envelope already;
            // the host's own count follows it.
            let delta = reservation as i64 - live.charge.units as i64;
            {
                let mut used = task.resident.lock().unwrap();
                *used = (*used as i64 + delta).max(0) as u64;
            }
            live.charge.units = reservation;
            task.resident_freed.notify_waiters();
        }
        m.memory_max = max;
        m.reservation = reservation;
        Ok(ResizeSessionReply {
            memory_max_bytes: max,
            memory_reservation_bytes: reservation,
        })
    }

    // -- reclaim, stats, drain ---------------------------------------------

    fn holders(&self) -> Vec<Arc<Holder>> {
        let mut all: Vec<Arc<Holder>> = self.0.sessions.lock().unwrap().values().cloned().collect();
        for t in self.0.tasks.lock().unwrap().values() {
            all.extend(t.tenants.lock().unwrap().values().cloned());
        }
        all
    }

    #[instrument(skip_all, fields(level = req.level, target = req.target_bytes))]
    pub async fn reclaim(&self, req: ReclaimRequest) -> ReclaimReply {
        let level = ReclaimLevel::try_from(req.level).unwrap_or(ReclaimLevel::ReclaimPageOut);
        let named = !req.session_ids.is_empty();
        // Named sessions are hibernated however briefly they have been idle;
        // a session with a call in flight is never a candidate (`idle_for`
        // is `None` while it is active) and is not counted.
        let min_idle = if named && level == ReclaimLevel::ReclaimHibernate {
            Duration::ZERO
        } else {
            Duration::from_millis(req.min_idle_millis)
        };
        let mut idle: Vec<(Duration, Arc<Holder>)> = self
            .holders()
            .into_iter()
            .filter(|h| {
                (!named || (h.kind == HolderKind::Session && req.session_ids.contains(&h.key)))
                    && (req.job_id.is_empty() || h.job == req.job_id)
                    && (req.task_name.is_empty() || h.task == req.task_name)
            })
            .filter_map(|h| h.idle_for().filter(|d| *d >= min_idle).map(|d| (d, h)))
            .collect();
        // Coldest first.
        idle.sort_by_key(|(d, _)| std::cmp::Reverse(*d));
        let mut out = ReclaimReply::default();
        for (_, h) in idle {
            if req.target_bytes != 0 && out.bytes >= req.target_bytes {
                break;
            }
            match level {
                ReclaimLevel::ReclaimPageOut => {
                    let mut m = h.meta.lock().unwrap();
                    if m.paged_out || m.active || !m.has_instance {
                        continue;
                    }
                    m.paged_out = true;
                    out.bytes += m.memory_bytes;
                    out.paged_out += 1;
                }
                ReclaimLevel::ReclaimHibernate => match h.kind {
                    HolderKind::Session => {
                        if let Ok(Some(freed)) = self.hibernate(&h).await {
                            out.bytes += freed;
                            out.hibernated += 1;
                        }
                    }
                    HolderKind::Tenant => {
                        if let Some(freed) = drop_if_idle(&h) {
                            out.bytes += freed;
                            out.dropped += 1;
                        }
                    }
                },
            }
        }
        out
    }

    /// Hibernate one idle session isolate: checkpoint its state through the
    /// worker, then drop it. `Ok(None)` when it is busy (only between
    /// calls) or has no instance.
    async fn hibernate(&self, h: &Arc<Holder>) -> Result<Option<u64>, Status> {
        let Ok(mut guard) = h.inst.try_lock() else {
            return Ok(None);
        };
        let (state, freed) = match guard.as_ref() {
            Some(live) => (
                live.store.data().state.clone().unwrap_or_default(),
                live.memory_bytes(),
            ),
            None => {
                // No instance: nothing in memory to give back.
                return Ok(None);
            }
        };
        let task = self.task(&h.job, &h.task).ok();
        let module_id = task
            .as_ref()
            .map(|t| t.module().module_id.clone())
            .unwrap_or_default();
        let meta = {
            let m = h.meta.lock().unwrap();
            SessionMeta {
                session_id: h.key.clone(),
                job_id: h.job.clone(),
                task_name: h.task.clone(),
                tenant: h.tenant.clone(),
                invocations: m.invocations,
                memory_max_bytes: m.memory_max,
                memory_reservation_bytes: m.reservation,
                hibernations: m.hibernations + 1,
            }
        };
        let body = Checkpoint {
            meta,
            state: state.map,
        }
        .encode();
        let header = CheckpointHeader {
            origin: Some(Origin {
                job_id: h.job.clone(),
                task_name: h.task.clone(),
                isolate_id: h.key.clone(),
                tenant: h.tenant.clone(),
                session: true,
            }),
            engine_version: ENGINE_VERSION.into(),
            module_id,
            bytes: body.len() as u64,
            freed_bytes: freed,
        };
        self.0.worker.save_checkpoint(header, body).await.inspect_err(|s| {
            warn!(session_id = %h.key, error = %s.message(), "checkpoint not saved; keeping the instance");
        })?;
        // The checkpoint holds the session now: forget it here.
        *guard = None;
        drop(guard);
        self.0.sessions.lock().unwrap().remove(&h.key);
        if let Some(t) = task {
            t.counters.hibernations.fetch_add(1, Ordering::Relaxed);
        }
        debug!(session_id = %h.key, freed, "session hibernated");
        Ok(Some(freed))
    }

    pub fn stats(&self) -> StatsReply {
        let tasks: Vec<_> = self.0.tasks.lock().unwrap().values().cloned().collect();
        let sessions: Vec<_> = self.0.sessions.lock().unwrap().values().cloned().collect();
        let mut out = Vec::new();
        for t in tasks {
            let c = &t.counters;
            let mut resident = 0;
            let mut paged = 0;
            let mut memory = 0;
            let holders: Vec<_> = t
                .tenants
                .lock()
                .unwrap()
                .values()
                .cloned()
                .chain(
                    sessions
                        .iter()
                        .filter(|h| h.job == t.key.0 && h.task == t.key.1)
                        .cloned(),
                )
                .collect();
            for h in holders {
                let m = h.meta.lock().unwrap();
                if m.has_instance {
                    resident += 1;
                    memory += m.memory_bytes;
                    if m.paged_out {
                        paged += 1;
                    }
                }
            }
            let maps = c.maps.lock().unwrap();
            out.push(TaskStats {
                job_id: t.key.0.clone(),
                task_name: t.key.1.clone(),
                // Live instances include in-flight request instances.
                resident: t.live.load(Ordering::Relaxed).max(resident),
                active: c.active.load(Ordering::Relaxed),
                paged_out: paged,
                queued: c.queued.load(Ordering::Relaxed),
                invocations: c.invocations.load(Ordering::Relaxed),
                turns: c.turns.load(Ordering::Relaxed),
                cpu_micros: c.cpu_micros.load(Ordering::Relaxed),
                stopped: maps.stopped.clone(),
                limits: maps.limits.clone(),
                refusals: c.refusals.load(Ordering::Relaxed),
                starts: maps.starts.clone(),
                instantiations: c.instantiations.load(Ordering::Relaxed),
                instantiate_micros: c.instantiate_micros.load(Ordering::Relaxed),
                hibernations: c.hibernations.load(Ordering::Relaxed),
                memory_bytes: memory,
            });
        }
        StatsReply {
            tasks: out,
            rss_bytes: self.rss(),
            slots: self.0.cfg.slots,
            slots_used: self.0.slots_used.load(Ordering::Relaxed),
            shards: self.0.shards.len() as u32,
            started_unix_ms: self.0.started_unix_ms,
        }
    }

    fn rss(&self) -> u64 {
        use std::io::{Read, Seek};
        let mut g = self.0.statm.lock().unwrap();
        let Some(f) = g.as_mut() else {
            return 0;
        };
        let mut s = String::new();
        if f.seek(std::io::SeekFrom::Start(0)).is_err() || f.read_to_string(&mut s).is_err() {
            return 0;
        }
        let pages: u64 = s
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        // SAFETY: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(4096) as u64;
        pages * page
    }

    #[instrument(skip_all, fields(hibernate = req.hibernate, grace_ms = req.grace_millis))]
    pub async fn drain(&self, req: DrainRequest) -> DrainReply {
        self.0.draining.store(true, Ordering::Release);
        let deadline = Instant::now() + Duration::from_millis(req.grace_millis as u64);
        // Let running invocations finish within the grace.
        loop {
            let notified = self.0.drained.notified();
            if self.0.inflight.load(Ordering::Acquire) == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline.into(), notified)
                .await
                .is_err()
            {
                break;
            }
        }
        let mut cut = 0;
        for h in self.holders() {
            let running = h.meta.lock().unwrap().running.clone();
            if let Some(r) = running {
                r.cut(Stop::new("ended", "the isolate host drained under it"));
                cut += 1;
            }
        }
        // Request instances in flight have no holder: they are cut by
        // their wall limit or finish; count them too.
        let request_inflight = self.0.inflight.load(Ordering::Acquire);
        cut = cut.max(request_inflight);
        let mut hibernated = 0;
        if req.hibernate {
            // Give cut invocations a moment to unwind before checkpointing.
            let until = Instant::now() + Duration::from_millis(200);
            while self.0.inflight.load(Ordering::Acquire) > 0 && Instant::now() < until {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let sessions: Vec<_> = self.0.sessions.lock().unwrap().values().cloned().collect();
            for h in sessions {
                if let Ok(Some(_)) = self.hibernate(&h).await {
                    hibernated += 1;
                }
            }
        }
        info!(hibernated, cut, "drained");
        DrainReply { hibernated, cut }
    }
}

fn effective(own: u64, task: u64) -> u64 {
    if own == 0 {
        task
    } else {
        own
    }
}

/// Drop a holder's instance (cutting a running call); true if one existed.
async fn drop_instance(h: &Arc<Holder>, why: Stop) -> bool {
    let running = h.meta.lock().unwrap().running.clone();
    if let Some(r) = running {
        r.cut(why);
    }
    let mut g = h.inst.lock().await;
    let had = g.take().is_some();
    let mut m = h.meta.lock().unwrap();
    m.has_instance = false;
    m.memory_bytes = 0;
    m.paged_out = false;
    had
}

/// Drop an idle tenant instance without waiting; returns the bytes freed.
fn drop_if_idle(h: &Arc<Holder>) -> Option<u64> {
    let mut g = h.inst.try_lock().ok()?;
    let live = g.take()?;
    let freed = live.memory_bytes();
    drop(live);
    let mut m = h.meta.lock().unwrap();
    m.has_instance = false;
    m.memory_bytes = 0;
    m.paged_out = false;
    Some(freed)
}

fn start_ticker(inner: &Arc<Inner>) {
    let weak = inner.engine.weak();
    let me = Arc::downgrade(inner);
    let every = Duration::from_millis(inner.cfg.epoch_millis as u64);
    std::thread::Builder::new()
        .name("isolate-epoch".into())
        .spawn(move || loop {
            std::thread::sleep(every);
            let Some(inner) = me.upgrade() else { return };
            if inner.stopping.load(Ordering::Acquire) {
                return;
            }
            drop(inner);
            match weak.upgrade() {
                Some(e) => e.increment_epoch(),
                None => return,
            }
        })
        .expect("starting the epoch ticker");
}

/// Drops idle tenant instances and hibernates idle sessions on their
/// task's schedule.
fn start_reaper(inner: &Arc<Inner>) {
    let me = Arc::downgrade(inner);
    inner.rt.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let Some(inner) = me.upgrade() else { return };
            if inner.stopping.load(Ordering::Acquire) {
                return;
            }
            let host = Host(inner);
            let tasks: Vec<_> = host.0.tasks.lock().unwrap().values().cloned().collect();
            for t in tasks {
                let spec = t.spec();
                if spec.tenant_idle_millis > 0 {
                    let limit = Duration::from_millis(spec.tenant_idle_millis);
                    let idle: Vec<_> = t
                        .tenants
                        .lock()
                        .unwrap()
                        .values()
                        .filter(|h| h.idle_for().is_some_and(|d| d >= limit))
                        .cloned()
                        .collect();
                    for h in idle {
                        drop_if_idle(&h);
                    }
                }
            }
            let sessions: Vec<_> = host.0.sessions.lock().unwrap().values().cloned().collect();
            for h in sessions {
                let Ok(t) = host.task(&h.job, &h.task) else {
                    continue;
                };
                let after = t.spec().hibernate_after_millis;
                if after > 0
                    && h.idle_for()
                        .is_some_and(|d| d >= Duration::from_millis(after))
                {
                    let _ = host.hibernate(&h).await;
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// One invocation, on its shard
// ---------------------------------------------------------------------------

struct Job {
    host: Arc<Inner>,
    task: Arc<TaskEntry>,
    spec: Arc<TaskSpec>,
    module: Arc<LoadedModule>,
    holder: Option<Arc<Holder>>,
    call: invoke_request::Call,
    inv: Arc<Invocation>,
    t0: Instant,
    shard: usize,
}

/// What the call returned before outputs are collected.
enum CallResult {
    Exit(i32, Option<String>),
    Http(HttpOutcome),
}

impl Job {
    fn refuse(&self, why: String) -> Status {
        self.task.counters.refusals.fetch_add(1, Ordering::Relaxed);
        Status::resource_exhausted(why)
    }

    fn deadline(&self) -> tokio::time::Instant {
        (self.t0 + Duration::from_millis(self.spec.queue_timeout_millis as u64)).into()
    }

    async fn run(self) -> Result<InvokeReply, Status> {
        let counters = &self.task.counters;
        let permit = self.admit().await;
        counters.queued.fetch_sub(1, Ordering::Relaxed);
        let _permit = permit?;
        counters.active.fetch_add(1, Ordering::Relaxed);
        let r = self.run_admitted().await;
        counters.active.fetch_sub(1, Ordering::Relaxed);
        r
    }

    async fn admit(&self) -> Result<OwnedSemaphorePermit, Status> {
        match tokio::time::timeout_at(self.deadline(), self.task.active.clone().acquire_owned())
            .await
        {
            Ok(Ok(p)) => Ok(p),
            Ok(Err(_)) => Err(Status::unavailable("the task is being removed")),
            Err(_) => Err(self.refuse(format!(
                "task {} already runs its most invocations at once (maxActive {}); none \
                 finished within the queue timeout of {}",
                self.task.name(),
                self.spec.max_active,
                fmt_duration(Duration::from_millis(self.spec.queue_timeout_millis as u64))
            ))),
        }
    }

    /// Take a resident reservation of `units` for a new instance.
    async fn reserve(&self, units: u64) -> Result<Charge, Status> {
        let cap = TaskEntry::capacity(&self.spec);
        loop {
            let freed = self.task.resident_freed.notified();
            {
                let mut used = self.task.resident.lock().unwrap();
                let slots_ok = self.host.slots_used.load(Ordering::Relaxed) < self.host.cfg.slots;
                if *used + units <= cap && slots_ok {
                    *used += units;
                    self.task.live.fetch_add(1, Ordering::Relaxed);
                    self.host.slots_used.fetch_add(1, Ordering::Relaxed);
                    return Ok(Charge {
                        task: self.task.clone(),
                        units,
                        host: self.host.clone(),
                    });
                }
            }
            // Make room: the task's coldest idle tenant instance goes first.
            if self.evict_one_tenant() {
                continue;
            }
            if tokio::time::timeout_at(self.deadline(), freed)
                .await
                .is_err()
            {
                return Err(self.refuse(format!(
                    "task {} holds its most resident isolates (maxResident {}) or the host \
                     has no free slot; none was freed within the queue timeout of {}",
                    self.task.name(),
                    self.spec.max_resident,
                    fmt_duration(Duration::from_millis(self.spec.queue_timeout_millis as u64))
                )));
            }
        }
    }

    fn evict_one_tenant(&self) -> bool {
        let mut idle: Vec<(Duration, Arc<Holder>)> = self
            .task
            .tenants
            .lock()
            .unwrap()
            .values()
            .filter(|h| {
                self.holder
                    .as_ref()
                    .is_none_or(|mine| !Arc::ptr_eq(mine, h))
            })
            .filter_map(|h| h.idle_for().map(|d| (d, h.clone())))
            .collect();
        idle.sort_by_key(|(d, _)| std::cmp::Reverse(*d));
        idle.into_iter().any(|(_, h)| drop_if_idle(&h).is_some())
    }

    fn memory_max(&self) -> u64 {
        let own = self
            .holder
            .as_ref()
            .map(|h| h.meta.lock().unwrap().memory_max)
            .unwrap_or(0);
        let m = effective(own, self.spec.memory_max_bytes);
        if m == 0 {
            self.host.cfg.slot_bytes
        } else {
            m
        }
    }

    fn reservation_units(&self) -> u64 {
        let own = self
            .holder
            .as_ref()
            .map(|h| h.meta.lock().unwrap().reservation)
            .unwrap_or(0);
        effective(own, TaskEntry::unit(&self.spec))
    }

    /// Make a new instance (store, context, slot).
    async fn instantiate(&self, state: Option<StateMap>, resumed: bool) -> Result<Live, Status> {
        let charge = self.reserve(self.reservation_units()).await?;
        let life = Lifetime::try_from(self.spec.lifetime).unwrap_or(Lifetime::Request);
        let mut env: Vec<(String, String)> = self
            .spec
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        env.sort();
        env.push(("LEGION_RUN_DIR".into(), RUN_DIR.into()));
        let mut args = vec![self.spec.task_name.clone()];
        if let invoke_request::Call::Run(run) = &self.call {
            if run.input.is_some() {
                env.push(("LEGION_RUN_INPUT".into(), format!("{RUN_DIR}/input.json")));
            }
            if let Some(entry) = self.entry(run) {
                env.push(("LEGION_RUN_ENTRY".into(), format!("{RUN_DIR}/{entry}")));
            }
            if !self.module.exports.runner {
                args.extend(run.args.iter().cloned());
            }
        }
        let holder = self.holder.as_ref();
        let ctx = Ctx::new(GuestSetup {
            ident: Ident {
                job: self.spec.job_id.clone(),
                task: self.spec.task_name.clone(),
                lifetime: life,
                tenant: (!self.inv.origin.tenant.is_empty())
                    .then(|| self.inv.origin.tenant.clone()),
                session: holder
                    .filter(|h| h.kind == HolderKind::Session)
                    .map(|h| h.key.clone()),
            },
            env,
            args,
            memory_max: self.memory_max(),
            has_vm: self.spec.has_vm,
            egress: self.spec.egress,
            max_body: self.host.cfg.max_body_bytes,
            state_quota: if self.spec.state_quota_bytes == 0 {
                DEFAULT_STATE_QUOTA
            } else {
                self.spec.state_quota_bytes
            },
            state,
            resumed,
            worker: self.host.worker.clone(),
        });
        let mut store = Store::new(&self.host.engine, ctx);
        store.limiter(|c| &mut c.limiter);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(|mut s| {
            let inv = s.data_mut().inv.clone();
            if let Some(stop) = inv.check_wall().or_else(|| inv.check_cpu()) {
                let stop = inv.stop(stop);
                return Err(wasmtime::Error::new(stop));
            }
            if turn::cpu_in_turn_ns() >= inv.slice_ns {
                Ok(UpdateDeadline::Yield(1))
            } else {
                Ok(UpdateDeadline::Continue(1))
            }
        });
        store.data_mut().begin(self.inv.clone(), 0);
        let t = Instant::now();
        let instance = self
            .module
            .pre
            .instantiate_async(&mut store)
            .await
            .map_err(|e| {
                let msg = format!("{e:#}");
                if msg.contains("maximum concurrent") || msg.contains("limit of") {
                    self.refuse(format!("the isolate host has no free slot: {msg}"))
                } else {
                    Status::failed_precondition(format!(
                        "instantiating {}: {}",
                        self.task.name(),
                        first_line(&msg)
                    ))
                }
            })?;
        let us = t.elapsed().as_micros() as u64;
        let c = &self.task.counters;
        c.instantiations.fetch_add(1, Ordering::Relaxed);
        c.instantiate_micros.fetch_add(us, Ordering::Relaxed);
        Ok(Live {
            store,
            instance,
            charge,
        })
    }

    fn entry(&self, run: &RunCall) -> Option<String> {
        if !run.entry.is_empty() {
            Some(run.entry.clone())
        } else {
            self.spec
                .entry
                .as_ref()
                .filter(|f| !f.path.is_empty())
                .map(|f| f.path.clone())
        }
    }

    async fn run_admitted(&self) -> Result<InvokeReply, Status> {
        let running = Running::new();
        // Tenant and session calls are serialized on the holder's instance.
        let mut guard = match &self.holder {
            Some(h) => Some(
                tokio::time::timeout_at(self.deadline(), h.inst.lock())
                    .await
                    .map_err(|_| {
                        self.refuse(format!(
                            "the {} instance of task {} stayed busy past the queue timeout",
                            if h.kind == HolderKind::Session {
                                "session"
                            } else {
                                "tenant"
                            },
                            self.task.name()
                        ))
                    })?,
            ),
            None => None,
        };
        if self.task.removed.load(Ordering::Acquire) {
            return Err(Status::not_found(format!("no task {}", self.task.name())));
        }
        if let Some(h) = &self.holder {
            let mut m = h.meta.lock().unwrap();
            if m.closed {
                return Err(Status::not_found(format!("session {} was closed", h.key)));
            }
            m.active = true;
            m.running = Some(running.clone());
        }
        let queue_us = self.t0.elapsed().as_micros() as u64;

        // An instance: reuse, or make one.
        let mut instantiate_us = 0;
        let (mut live, start) = match guard.as_mut().and_then(|g| g.take()) {
            Some(live) => {
                let paged = self
                    .holder
                    .as_ref()
                    .map(|h| std::mem::take(&mut h.meta.lock().unwrap().paged_out))
                    .unwrap_or(false);
                (live, if paged { "paged" } else { "warm" })
            }
            None => {
                let (state, resumed) = match &self.holder {
                    Some(h) => {
                        let mut m = h.meta.lock().unwrap();
                        (
                            Some(m.state.take().unwrap_or_default()),
                            std::mem::take(&mut m.resumed),
                        )
                    }
                    None => (None, false),
                };
                let t = Instant::now();
                let made = self.instantiate(state.clone(), resumed).await;
                instantiate_us = t.elapsed().as_micros() as u64;
                match made {
                    Ok(l) => (l, if resumed { "hibernated" } else { "fresh" }),
                    Err(e) => {
                        if let Some(h) = &self.holder {
                            let mut m = h.meta.lock().unwrap();
                            m.state = state;
                            m.resumed = resumed;
                            m.active = false;
                            m.running = None;
                        }
                        return Err(e);
                    }
                }
            }
        };
        if let Some(h) = &self.holder {
            h.meta.lock().unwrap().has_instance = true;
        }
        self.task.count(|m| &mut m.starts, start);

        let limits = self.spec.limits.unwrap_or_default();
        let budget = if limits.output_bytes == 0 {
            DEFAULT_OUTPUT
        } else {
            limits.output_bytes
        };
        live.store.data_mut().begin(self.inv.clone(), budget);

        // The call, bounded by the wall limit and cut on close or drain.
        let outcome = {
            let deadline: tokio::time::Instant = self.inv.deadline().into();
            let cancelled = running.cancel.notified();
            tokio::pin!(cancelled);
            tokio::select! {
                r = self.call(&mut live) => r,
                _ = tokio::time::sleep_until(deadline) => Err(wasmtime::Error::new(self.inv.stop(self.inv.wall_stop()))),
                _ = &mut cancelled => {
                    let why = running.why.lock().unwrap().clone()
                        .unwrap_or_else(|| Stop::new("ended", "the invocation was cut"));
                    Err(wasmtime::Error::new(self.inv.stop(why)))
                }
            }
        };

        // Outputs.
        let mut reply = InvokeReply {
            queue_micros: queue_us.saturating_sub(instantiate_us),
            instantiate_micros: if start == "warm" || start == "paged" {
                0
            } else {
                instantiate_us
            },
            start: start.into(),
            ..Default::default()
        };
        let mut keep = self.holder.is_some();
        let stop = match outcome {
            Ok(CallResult::Exit(code, result)) => {
                let printed = live.store.data().out.take();
                self.finish_run(&mut live, &mut reply, Some(code), result, printed);
                None
            }
            Ok(CallResult::Http(h)) => {
                let printed = live.store.data().out.take();
                fill_printed(&mut reply, &printed);
                reply.outcome = Some(invoke_reply::Outcome::Http(h));
                None
            }
            Err(e) => {
                // An exit through wasi:cli/exit is a normal end.
                if let Some(exit) = e.downcast_ref::<wasmtime_wasi::I32Exit>() {
                    let printed = live.store.data().out.take();
                    self.finish_run(&mut live, &mut reply, Some(exit.0), None, printed);
                    keep = false;
                    None
                } else {
                    keep = false;
                    let stop = classify(&e, &self.inv, &live);
                    let printed = live.store.data().out.take();
                    if matches!(self.call, invoke_request::Call::Run(_)) {
                        self.finish_run(&mut live, &mut reply, None, None, printed);
                    } else {
                        fill_printed(&mut reply, &printed);
                    }
                    Some(stop)
                }
            }
        };
        // A `wasi:cli/run` command is single-shot.
        if self.holder.is_none() || (self.module.command.is_some() && !self.module.exports.runner) {
            keep = false;
        }
        // `resumed` holds for the first call after a restore only.
        live.store.data_mut().resumed = false;
        let memory_peak = live.store.data().account().peak();
        let memory_now = live.memory_bytes();
        let park = live.store.data().park;
        let state = live.store.data().state.clone();
        let c = &self.task.counters;
        c.invocations.fetch_add(1, Ordering::Relaxed);
        c.turns
            .fetch_add(self.inv.turns.load(Ordering::Relaxed), Ordering::Relaxed);
        // Includes the turn running now, which the shard charges after it.
        let cpu_us = self.inv.cpu_now_ns() / 1000;
        c.cpu_micros.fetch_add(cpu_us, Ordering::Relaxed);
        if let Some(s) = &stop {
            self.task.count(|m| &mut m.stopped, s.reason);
            if !s.limit.is_empty() {
                self.task.count(|m| &mut m.limits, s.limit);
            }
            reply.stopped = Some(Stopped {
                reason: s.reason.into(),
                limit: s.limit.into(),
                detail: s.detail.clone(),
            });
        }
        reply.cpu_micros = cpu_us;
        reply.wall_micros = self.inv.started.elapsed().as_micros() as u64;
        reply.memory_peak_bytes = memory_peak;

        if let Some(h) = &self.holder {
            let mut m = h.meta.lock().unwrap();
            m.active = false;
            m.running = None;
            m.invocations += 1;
            m.last_active = Some(Instant::now());
            m.last_active_unix_ms = unix_ms();
            m.state_bytes = state.as_ref().map_or(0, |s| s.bytes);
            if keep {
                m.memory_bytes = memory_now;
            } else {
                m.has_instance = false;
                m.memory_bytes = 0;
                // The state outlives the instance (a session after a trap).
                m.state = state;
                // The next instance starts with fresh memory but this state.
                if h.kind == HolderKind::Session {
                    m.resumed = true;
                }
            }
        }
        if keep {
            if let Some(g) = guard.as_mut() {
                **g = Some(live);
            }
        } else {
            drop(live);
        }
        drop(guard);

        self.forward_logs(&reply);
        if park && keep {
            if let Some(h) = self.holder.clone() {
                let host = Host(self.host.clone());
                self.host.rt.spawn(async move {
                    let _ = host.hibernate(&h).await;
                });
            }
        }
        let _ = self.shard;
        Ok(reply)
    }

    /// Stage a run's files, make the call, and return how it ended.
    async fn call(&self, live: &mut Live) -> wasmtime::Result<CallResult> {
        match &self.call {
            invoke_request::Call::Run(run) => {
                self.stage(live, run)?;
                if self.module.exports.runner {
                    let w = crate::world::Image::new(&mut live.store, &live.instance)?;
                    let entry = self.entry(run);
                    let r = w
                        .legion_isolate_runner()
                        .call_run(
                            &mut live.store,
                            &runner::Request {
                                entry,
                                input: run.input.clone(),
                                args: run.args.clone(),
                            },
                        )
                        .await?;
                    // The guest printed its own errors; `result` falls back
                    // to `result.json` from the run directory.
                    Ok(CallResult::Exit(r.exit, r.result))
                } else {
                    let cmd =
                        wasmtime_wasi::p2::bindings::Command::new(&mut live.store, &live.instance)?;
                    let r = cmd.wasi_cli_run().call_run(&mut live.store).await?;
                    Ok(CallResult::Exit(if r.is_ok() { 0 } else { 1 }, None))
                }
            }
            invoke_request::Call::Http(h) => self.http(live, h).await.map(CallResult::Http),
        }
    }

    fn stage(&self, live: &mut Live, run: &RunCall) -> wasmtime::Result<()> {
        let max = live.store.data().account().limit();
        let ctx = live.store.data_mut();
        ctx.scratch.clear();
        ctx.scratch.set_now(turn::now_datetime());
        let fail = |e: crate::scratch::ScratchError| {
            wasmtime::Error::new(self.inv.stop(Stop::limit(
                "memory",
                format!(
                    "the run's files do not fit in its {} memory limit ({e})",
                    mib(max)
                ),
            )))
        };
        ctx.scratch.create_dir_all(RUN_DIR).map_err(fail)?;
        if let Some(entry) = self.spec.entry.as_ref().filter(|f| !f.path.is_empty()) {
            ctx.scratch
                .write_file(&format!("{RUN_DIR}/{}", entry.path), &entry.content)
                .map_err(fail)?;
        }
        for f in &run.files {
            ctx.scratch
                .write_file(&format!("{RUN_DIR}/{}", f.path), &f.content)
                .map_err(fail)?;
        }
        if let Some(input) = &run.input {
            ctx.scratch
                .write_file(&format!("{RUN_DIR}/input.json"), input.as_bytes())
                .map_err(fail)?;
        }
        Ok(())
    }

    /// `result.json` and the `get` files, within the output budget.
    fn finish_run(
        &self,
        live: &mut Live,
        reply: &mut InvokeReply,
        exit: Option<i32>,
        result: Option<String>,
        printed: crate::guest::Printed,
    ) {
        fill_printed(reply, &printed);
        let mut left = printed.left;
        let scratch = &live.store.data().scratch;
        let result = result.or_else(|| {
            scratch
                .read_file(
                    &format!("{RUN_DIR}/result.json"),
                    self.host.cfg.max_body_bytes as usize,
                )
                .filter(|(_, truncated)| !truncated)
                .and_then(|(b, _)| String::from_utf8(b).ok())
                .filter(|s| serde_json::from_str::<serde_json::Value>(s).is_ok())
        });
        let mut files = Vec::new();
        if let invoke_request::Call::Run(run) = &self.call {
            for path in &run.get {
                let full = format!("{RUN_DIR}/{}", path.trim_start_matches('/'));
                match scratch.read_file(&full, left as usize) {
                    Some((content, truncated)) => {
                        left -= content.len() as u64;
                        if truncated {
                            reply.truncated = true;
                        }
                        files.push(File {
                            path: path.clone(),
                            content,
                            truncated,
                            missing: false,
                        });
                    }
                    None => files.push(File {
                        path: path.clone(),
                        missing: true,
                        ..Default::default()
                    }),
                }
            }
        }
        reply.outcome = Some(invoke_reply::Outcome::Run(RunOutcome {
            exit,
            result: result.unwrap_or_default(),
            files,
        }));
    }

    async fn http(&self, live: &mut Live, call: &HttpCall) -> wasmtime::Result<HttpOutcome> {
        use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
        use wasmtime_wasi_http::WasiHttpView;
        let max = self.host.cfg.max_body_bytes;
        let https = call.scheme.eq_ignore_ascii_case("https");
        let scheme = if https { Scheme::Https } else { Scheme::Http };
        let authority = if call.authority.is_empty() {
            "localhost"
        } else {
            call.authority.as_str()
        };
        let path = if call.path.is_empty() {
            "/"
        } else {
            call.path.as_str()
        };
        let mut b = hyper::Request::builder()
            .method(call.method.as_str())
            .uri(format!(
                "{}://{authority}{path}",
                if https { "https" } else { "http" }
            ));
        for h in &call.headers {
            b = b.header(h.name.as_str(), h.value.as_slice());
        }
        use http_body_util::BodyExt;
        let body = http_body_util::Full::new(Bytes::from(call.body.clone())).map_err(
            |never: std::convert::Infallible| -> wasmtime_wasi_http::Error { match never {} },
        );
        let req = b
            .body(body)
            .map_err(|e| wasmtime::format_err!("the request is not valid HTTP: {e}"))?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = live
            .store
            .data_mut()
            .http()
            .new_incoming_request(scheme, req)?;
        let out = live.store.data_mut().http().new_response_outparam(tx)?;
        let proxy = wasmtime_wasi_http::p2::bindings::Proxy::new(&mut live.store, &live.instance)?;
        // The guest may set its response and keep writing the body before
        // `handle` returns, so the body is read while the handler runs. It
        // needs no store access: it is a channel the guest's writes feed.
        let response = async move {
            let resp: hyper::Response<_> = rx
                .await
                .map_err(|_| {
                    wasmtime::format_err!("the handler returned without setting a response")
                })?
                .map_err(|code| {
                    wasmtime::format_err!("the handler answered with error {code:?}")
                })?;
            let (parts, body) = resp.into_parts();
            let body = collect_body(body, max).await.map_err(|e| match e {
                Some(e) => wasmtime::format_err!("reading the response body: {e:?}"),
                None => wasmtime::format_err!("the response body is larger than {}", mib(max)),
            })?;
            Ok::<_, wasmtime::Error>((parts, body))
        };
        let handled = proxy
            .wasi_http_incoming_handler()
            .call_handle(&mut live.store, req, out);
        tokio::pin!(response);
        tokio::pin!(handled);
        let mut handler_done = false;
        let (parts, body) = loop {
            tokio::select! {
                r = &mut handled, if !handler_done => {
                    r?;
                    handler_done = true;
                }
                r = &mut response => {
                    let got = r?;
                    if !handler_done {
                        (&mut handled).await?;
                    }
                    break got;
                }
            }
        };
        Ok(HttpOutcome {
            status: parts.status.as_u16() as u32,
            headers: parts
                .headers
                .iter()
                .map(|(n, v)| legion_proto::isolate_host::Header {
                    name: n.as_str().into(),
                    value: v.as_bytes().to_vec(),
                })
                .collect(),
            body: body.to_vec(),
        })
    }

    fn forward_logs(&self, reply: &InvokeReply) {
        let session = self
            .holder
            .as_ref()
            .is_some_and(|h| h.kind == HolderKind::Session);
        let http = matches!(self.call, invoke_request::Call::Http(_));
        if !(session || http) || (reply.stdout.is_empty() && reply.stderr.is_empty()) {
            return;
        }
        let now = unix_ms();
        let mut lines = Vec::new();
        for (stderr, buf) in [(false, &reply.stdout), (true, &reply.stderr)] {
            for line in buf.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                lines.push(LogLine {
                    stderr,
                    line: line.to_vec(),
                    unix_ms: now,
                });
            }
        }
        let batch = LogBatch {
            origin: Some(self.inv.origin.clone()),
            lines,
        };
        let worker = self.host.worker.clone();
        self.host.rt.spawn(async move {
            let _ = worker.log(batch).await;
        });
    }
}

fn fill_printed(reply: &mut InvokeReply, p: &crate::guest::Printed) {
    reply.stdout = p.stdout.clone();
    reply.stderr = p.stderr.clone();
    reply.truncated |= p.truncated;
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}

/// Why a call that failed stopped, in the reply's words.
fn classify(e: &wasmtime::Error, inv: &Invocation, live: &Live) -> Stop {
    if let Some(s) = inv.stopped() {
        return s;
    }
    if let Some(s) = e.downcast_ref::<Stop>() {
        return inv.stop(s.clone());
    }
    let account = live.store.data().account();
    if account.refused() {
        return inv.stop(Stop::limit(
            "memory",
            format!("exceeded its {} memory limit", mib(account.limit())),
        ));
    }
    let detail = match e.downcast_ref::<wasmtime::Trap>() {
        Some(wasmtime::Trap::StackOverflow) => "the guest overflowed its stack".to_string(),
        Some(wasmtime::Trap::UnreachableCodeReached) => {
            "the guest trapped: it executed `unreachable` (a panic or abort)".to_string()
        }
        Some(t) => format!("the guest trapped: {t}"),
        None => format!("the guest failed: {}", first_line(&format!("{e}"))),
    };
    inv.stop(Stop::new("trap", detail))
}
