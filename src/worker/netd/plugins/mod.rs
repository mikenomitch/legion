//! Network plugins on the node: WASM modules on the proxy-wasm ABI, run
//! inside netd as `plugin` hops (docs/network-plugins.md; mesh interfaces
//! plan §6.B).
//!
//! [`PluginHost`] is the node's one registry. A plugin object reaches it as a
//! `plugin` material (by name, versioned by revision); its module is fetched
//! by the artifact fetchers into the node content store and compiled once
//! per checksum, however many allocations use it. Instances are pooled per
//! (plugin revision, scope, config): the scope is the plugin itself for a
//! reference a policy object makes, and the job for a reference a job makes,
//! so one job's plugin never shares instances, shared data, or metrics with
//! another job's.
//!
//! A request picks an instance when it starts and keeps it for its stream
//! context's lifetime; each callback locks the instance only while it runs.
//! A broken instance (a trap, exhausted fuel, a memory limit) is replaced
//! for the next request; the requests whose contexts lived in it fail their
//! remaining callbacks, and each failure follows its hop's `onError`.

pub mod abi;
pub mod harness;
pub mod vm;

use crate::prelude::*;
use crate::structs::netplugin::{NetPluginObject, PluginConfig};
use crate::structs::NetPluginNodeStatus;
use vm::{Compiled, PluginMetrics, Scope, SharedData, Vm};

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How long a failed fetch or compile waits before the next try.
pub const RETRY_AFTER: Duration = Duration::from_secs(30);
/// Callback samples kept per plugin for the p99.
const SAMPLES: usize = 1024;
/// Log lines one plugin may write per second before the rest are dropped.
const LOG_LINES_PER_SECOND: u64 = 100;

/// Fetches a module's bytes, checked against its checksum. Blocking.
pub trait ModuleLoader: Send + Sync {
    fn load(&self, source: &str, checksum: &str) -> Result<Vec<u8>, String>;
}

/// Reads `file://` and plain paths: the loader `legion net plugin test` and
/// the tests use.
pub struct FileLoader;

impl ModuleLoader for FileLoader {
    fn load(&self, source: &str, checksum: &str) -> Result<Vec<u8>, String> {
        let path = source.strip_prefix("file://").unwrap_or(source);
        let bytes = std::fs::read(path).map_err(|err| format!("reading {path}: {err}"))?;
        verify_checksum(&bytes, checksum)?;
        Ok(bytes)
    }
}

/// Fetches through the worker's artifact stager into the node content store,
/// so a module is downloaded once per node and survives restarts.
pub struct StagerLoader(pub Arc<crate::artifacts::stage::ArtifactStager>);

impl ModuleLoader for StagerLoader {
    fn load(&self, source: &str, checksum: &str) -> Result<Vec<u8>, String> {
        let mut spec = crate::structs::artifact::ArtifactSpec::from_source(source);
        spec.checksum = Some(checksum.to_string());
        let fetched = self
            .0
            .fetch_into_store(
                &spec,
                crate::content_store::ContentClass::Artifacts,
                crate::content_store::PutOptions::default().label("net plugin module"),
            )
            .map_err(|err| format!("{err:#}"))?;
        let bytes = std::fs::read(&fetched.entry.path)
            .map_err(|err| format!("reading the stored module: {err}"))?;
        verify_checksum(&bytes, checksum)?;
        Ok(bytes)
    }
}

pub fn verify_checksum(bytes: &[u8], checksum: &str) -> Result<(), String> {
    use sha2::Digest;
    let got = format!("sha256:{:x}", sha2::Sha256::digest(bytes));
    if !checksum.eq_ignore_ascii_case(&got) {
        return Err(format!(
            "module checksum mismatch: expected {checksum}, got {got}"
        ));
    }
    Ok(())
}

/// Where a plugin's module stands on this node.
#[derive(Debug, Clone)]
pub enum LoadState {
    Fetching,
    Ready(Arc<Compiled>),
    Failed { error: String, at: Instant },
}

/// Health counters for one plugin on this node.
#[derive(Debug, Default)]
pub struct PluginStats {
    pub calls: AtomicU64,
    pub errors: AtomicU64,
    /// Fuel burned by every callback.
    pub fuel: AtomicU64,
    /// Time spent in callbacks, in microseconds.
    pub micros: AtomicU64,
    last_error: Mutex<String>,
    samples: Mutex<VecDeque<u32>>,
}

impl PluginStats {
    pub fn record(&self, elapsed: Duration, error: Option<&str>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        if let Some(error) = error {
            self.errors.fetch_add(1, Ordering::Relaxed);
            *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = error.to_string();
        }
        let mut samples = self.samples.lock().unwrap_or_else(|p| p.into_inner());
        if samples.len() >= SAMPLES {
            samples.pop_front();
        }
        samples.push_back(elapsed.as_micros().min(u32::MAX as u128) as u32);
    }

    pub fn last_error(&self) -> String {
        self.last_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn p99_micros(&self) -> u64 {
        let samples = self.samples.lock().unwrap_or_else(|p| p.into_inner());
        if samples.is_empty() {
            return 0;
        }
        let mut sorted: Vec<u32> = samples.iter().copied().collect();
        sorted.sort_unstable();
        let index = ((sorted.len() as f64) * 0.99).ceil() as usize;
        sorted[index.saturating_sub(1).min(sorted.len() - 1)] as u64
    }
}

/// One plugin object as this node holds it.
pub struct PluginEntry {
    pub object: NetPluginObject,
    pub revision: u64,
    state: Mutex<LoadState>,
    pub stats: Arc<PluginStats>,
    pools: Mutex<HashMap<(String, u64), Arc<InstancePool>>>,
    /// Per scope; carried across revisions, so an update keeps a meter's
    /// counts.
    shared: Arc<Mutex<HashMap<String, Arc<SharedData>>>>,
    metrics: Arc<Mutex<HashMap<String, Arc<PluginMetrics>>>>,
    log_window: Arc<Mutex<(Instant, u64, u64)>>,
}

impl std::fmt::Debug for PluginEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginEntry")
            .field("name", &self.object.name)
            .field("revision", &self.revision)
            .finish()
    }
}

impl PluginEntry {
    pub fn state(&self) -> LoadState {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn set_state(&self, state: LoadState) {
        *self.state.lock().unwrap_or_else(|p| p.into_inner()) = state;
    }

    /// One scope's shared data, if anything ran under it.
    pub fn shared_scope(&self, scope: &str) -> Option<Arc<SharedData>> {
        self.shared
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(scope)
            .cloned()
    }

    /// The current value of a module-defined metric under one scope.
    pub fn metric_value(&self, scope: &str, name: &str) -> Option<i64> {
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(scope)
            .cloned()?;
        let entries = metrics.entries.lock().unwrap_or_else(|p| p.into_inner());
        entries
            .iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, _, v)| *v)
    }

    fn instances(&self) -> usize {
        self.pools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(|pool| pool.live())
            .sum()
    }
}

/// The node's plugins.
pub struct PluginHost {
    entries: Mutex<HashMap<String, Arc<PluginEntry>>>,
    compiled: Mutex<HashMap<String, Arc<Compiled>>>,
    loader: Option<Arc<dyn ModuleLoader>>,
    node_id: Mutex<String>,
    credentials:
        Arc<Mutex<HashMap<(String, String), (i64, crate::worker::forwarder::SecretValue)>>>,
    /// Instances per pool: enough for this node's cores to run callbacks at
    /// once.
    pool_size: usize,
}

impl std::fmt::Debug for PluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginHost").finish()
    }
}

impl Default for PluginHost {
    fn default() -> Self {
        PluginHost::new(None)
    }
}

impl PluginHost {
    pub fn new(loader: Option<Arc<dyn ModuleLoader>>) -> Self {
        PluginHost {
            entries: Mutex::new(HashMap::new()),
            compiled: Mutex::new(HashMap::new()),
            loader,
            node_id: Mutex::new(String::new()),
            credentials: Arc::new(Mutex::new(HashMap::new())),
            pool_size: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2)
                .clamp(1, 16),
        }
    }

    pub fn set_node_id(&self, node_id: &str) {
        *self.node_id.lock().unwrap_or_else(|p| p.into_inner()) = node_id.to_string();
    }

    pub fn entry(&self, name: &str) -> Option<Arc<PluginEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .cloned()
    }

    /// The revision this node holds for `name` (0 for none): the version a
    /// `plugin` material ref carries.
    pub fn held_revision(&self, name: &str) -> i64 {
        self.entry(name).map(|e| e.revision as i64).unwrap_or(0)
    }

    /// Every (plugin, credential) the held plugins declare, with the version
    /// held.
    pub fn wanted_credentials(&self) -> Vec<(String, String, i64)> {
        let credentials = self.credentials.lock().unwrap_or_else(|p| p.into_inner());
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .flat_map(|entry| {
                entry.object.credentials.keys().map(|credential| {
                    let key = (entry.object.name.clone(), credential.clone());
                    let version = credentials.get(&key).map(|(v, _)| *v).unwrap_or(0);
                    (key.0, key.1, version)
                })
            })
            .collect()
    }

    pub fn set_credential(&self, plugin: &str, credential: &str, version: i64, value: String) {
        self.credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                (plugin.to_string(), credential.to_string()),
                (version, crate::worker::forwarder::SecretValue::new(value)),
            );
    }

    pub fn drop_credential(&self, plugin: &str, credential: &str) {
        self.credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(plugin.to_string(), credential.to_string()));
    }

    /// Drop every plugin no local allocation references any more.
    pub fn retain(&self, referenced: &HashSet<String>) {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|name, _| referenced.contains(name));
        self.credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(plugin, _), _| referenced.contains(plugin));
    }

    /// The manager says the plugin is gone.
    pub fn remove(&self, name: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
    }

    /// Hold `object` at `revision`. A new revision replaces the entry (its
    /// pools with it; in-flight requests finish on the instances they hold)
    /// and loads the module in the background; the same revision is a no-op
    /// unless the last load failed long enough ago to retry.
    pub fn apply(self: &Arc<Self>, mut object: NetPluginObject, revision: u64) {
        if let Err(err) = object.normalize_and_validate() {
            warn!(plugin = %object.name, error = %err, "Rejected a net plugin object from the manager");
            return;
        }
        let name = object.name.clone();
        let previous = self.entry(&name);
        if let Some(previous) = previous.as_ref() {
            if previous.revision == revision {
                if let LoadState::Failed { at, .. } = previous.state() {
                    if at.elapsed() >= RETRY_AFTER {
                        self.load(previous.clone());
                    }
                }
                return;
            }
        }
        let entry = Arc::new(PluginEntry {
            object,
            revision,
            state: Mutex::new(LoadState::Fetching),
            stats: previous
                .as_ref()
                .map(|p| p.stats.clone())
                .unwrap_or_default(),
            pools: Mutex::new(HashMap::new()),
            shared: previous
                .as_ref()
                .map(|p| p.shared.clone())
                .unwrap_or_default(),
            metrics: previous
                .as_ref()
                .map(|p| p.metrics.clone())
                .unwrap_or_default(),
            log_window: Arc::new(Mutex::new((Instant::now(), 0, 0))),
        });
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name, entry.clone());
        self.load(entry);
    }

    /// Fetch and compile in the background (or at once from the compile
    /// cache).
    fn load(self: &Arc<Self>, entry: Arc<PluginEntry>) {
        let checksum = entry.object.module.checksum.clone();
        if let Some(compiled) = self
            .compiled
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&checksum)
            .cloned()
        {
            entry.set_state(LoadState::Ready(compiled));
            return;
        }
        entry.set_state(LoadState::Fetching);
        let host = self.clone();
        let run = move || {
            let result = host.fetch_and_compile(&entry);
            match result {
                Ok(compiled) => {
                    host.compiled
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .insert(checksum.clone(), compiled.clone());
                    info!(plugin = %entry.object.name, revision = entry.revision, module = %checksum, "Net plugin ready");
                    entry.set_state(LoadState::Ready(compiled));
                }
                Err(error) => {
                    warn!(plugin = %entry.object.name, revision = entry.revision, error = %error, "Net plugin failed to load");
                    entry.set_state(LoadState::Failed {
                        error,
                        at: Instant::now(),
                    });
                }
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(run);
            }
            Err(_) => run(),
        }
    }

    fn fetch_and_compile(&self, entry: &PluginEntry) -> Result<Arc<Compiled>, String> {
        let loader = self.loader.as_ref().ok_or_else(|| {
            "this node cannot fetch plugin modules (no artifact stager)".to_string()
        })?;
        let bytes = loader.load(&entry.object.module.source, &entry.object.module.checksum)?;
        vm::compile(&bytes).map(Arc::new)
    }

    /// Load a module from bytes at once (`legion net plugin test`, tests).
    pub fn install_compiled(
        &self,
        object: NetPluginObject,
        revision: u64,
        bytes: &[u8],
    ) -> Result<Arc<PluginEntry>, String> {
        let compiled = Arc::new(vm::compile(bytes)?);
        let entry = Arc::new(PluginEntry {
            object,
            revision,
            state: Mutex::new(LoadState::Ready(compiled)),
            stats: Arc::default(),
            pools: Mutex::new(HashMap::new()),
            shared: Arc::default(),
            metrics: Arc::default(),
            log_window: Arc::new(Mutex::new((Instant::now(), 0, 0))),
        });
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(entry.object.name.clone(), entry.clone());
        Ok(entry)
    }

    /// The instance pool for a reference under `scope` (`policy` or
    /// `job:<id>`), started on first use.
    pub fn pool(
        &self,
        reference: &PluginConfig,
        scope: &str,
    ) -> Result<(Arc<PluginEntry>, Arc<InstancePool>), String> {
        let entry = self.entry(&reference.name).ok_or_else(|| {
            "is not on this node yet (its object has not arrived from the manager)".to_string()
        })?;
        let compiled = match entry.state() {
            LoadState::Ready(compiled) => compiled,
            LoadState::Fetching => return Err("is still loading its module".to_string()),
            LoadState::Failed { error, .. } => return Err(format!("failed to load: {error}")),
        };
        let configuration = reference.configuration_bytes();
        let config_hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            configuration.hash(&mut hasher);
            hasher.finish()
        };
        let key = (scope.to_string(), config_hash);
        let mut pools = entry.pools.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(pool) = pools.get(&key) {
            return Ok((entry.clone(), pool.clone()));
        }
        let shared = entry
            .shared
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(scope.to_string())
            .or_default()
            .clone();
        let metrics = entry
            .metrics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(scope.to_string())
            .or_default()
            .clone();
        let object = &entry.object;
        let node_id = self
            .node_id
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let mut properties = BTreeMap::new();
        properties.insert(
            vm::prop_key(&["plugin_name"]),
            object.name.clone().into_bytes(),
        );
        properties.insert(
            vm::prop_key(&["legion", "plugin"]),
            object.name.clone().into_bytes(),
        );
        properties.insert(
            vm::prop_key(&["legion", "plugin_revision"]),
            entry.revision.to_string().into_bytes(),
        );
        properties.insert(vm::prop_key(&["legion", "node"]), node_id.into_bytes());
        properties.insert(
            vm::prop_key(&["legion", "scope"]),
            scope.as_bytes().to_vec(),
        );
        let signer: Option<Arc<dyn vm::Signer>> = (!object.credentials.is_empty()).then(|| {
            Arc::new(PluginSigner {
                plugin: object.name.clone(),
                credentials: self.credentials.clone(),
                declared: object.credentials.keys().cloned().collect(),
            }) as Arc<dyn vm::Signer>
        });
        let window = entry.log_window.clone();
        let scope_name = scope.to_string();
        let scope = Arc::new(Scope {
            plugin: object.name.clone(),
            revision: entry.revision,
            configuration,
            fuel: object.limits.fuel(),
            deadline: object.limits.deadline(),
            memory_bytes: object.limits.memory_bytes() as usize,
            max_body_bytes: object.limits.max_body_bytes(),
            shared,
            metrics,
            signer,
            targets: object.targets.clone(),
            properties,
            log: Arc::new(move |scope: &Scope, line: &vm::LogLine| {
                log_line(&window, &scope_name, scope, line)
            }),
        });
        let pool = Arc::new(InstancePool::new(
            compiled,
            scope,
            self.pool_size,
            entry.stats.clone(),
        ));
        pools.insert(key, pool.clone());
        Ok((entry.clone(), pool))
    }

    /// One status row per held plugin, for the heartbeat.
    pub fn report(&self) -> Vec<NetPluginNodeStatus> {
        let mut rows: Vec<NetPluginNodeStatus> = self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(|entry| {
                let (state, error) = match entry.state() {
                    LoadState::Fetching => ("fetching", String::new()),
                    LoadState::Ready(_) => ("ready", String::new()),
                    LoadState::Failed { error, .. } => ("failed", error),
                };
                NetPluginNodeStatus {
                    node_id: String::new(),
                    plugin: entry.object.name.clone(),
                    revision: entry.revision,
                    module_checksum: entry.object.module.checksum.clone(),
                    state: state.to_string(),
                    error,
                    calls: entry.stats.calls.load(Ordering::Relaxed),
                    errors: entry.stats.errors.load(Ordering::Relaxed),
                    last_error: entry.stats.last_error(),
                    p99_micros: entry.stats.p99_micros(),
                    instances: entry.instances() as u64,
                }
            })
            .collect();
        rows.sort_by(|a, b| a.plugin.cmp(&b.plugin));
        rows
    }

    /// Publish every scope's module-defined metrics on the node's metrics
    /// endpoint.
    pub fn export_metrics(&self) {
        let telemetry = crate::manager::telemetry::worker_metrics();
        for entry in self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            let scopes = entry.metrics.lock().unwrap_or_else(|p| p.into_inner());
            for (scope, metrics) in scopes.iter() {
                for (name, _kind, value) in metrics
                    .entries
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .iter()
                {
                    telemetry
                        .net_plugin_metric
                        .set(&[&entry.object.name, scope, name], *value);
                }
            }
        }
    }
}

fn log_line(
    window: &Mutex<(Instant, u64, u64)>,
    scope_name: &str,
    scope: &Scope,
    line: &vm::LogLine,
) {
    {
        let mut window = window.lock().unwrap_or_else(|p| p.into_inner());
        if window.0.elapsed() >= Duration::from_secs(1) {
            if window.2 > 0 {
                warn!(plugin = %scope.plugin, dropped = window.2, "Net plugin log lines dropped (over 100 per second)");
            }
            *window = (Instant::now(), 0, 0);
        }
        if window.1 >= LOG_LINES_PER_SECOND {
            window.2 += 1;
            return;
        }
        window.1 += 1;
    }
    let level = abi::log_level_name(line.level);
    let message = line.message.as_str();
    let (plugin, context) = (scope.plugin.as_str(), line.context);
    match line.level {
        i32::MIN..=0 => {
            tracing::trace!(target: "legion::net_plugin", plugin, scope = scope_name, context, level, "{message}")
        }
        1 => {
            debug!(target: "legion::net_plugin", plugin, scope = scope_name, context, level, "{message}")
        }
        2 => {
            info!(target: "legion::net_plugin", plugin, scope = scope_name, context, level, "{message}")
        }
        3 => {
            warn!(target: "legion::net_plugin", plugin, scope = scope_name, context, level, "{message}")
        }
        _ => {
            error!(target: "legion::net_plugin", plugin, scope = scope_name, context, level, "{message}")
        }
    }
}

/// Instances of one (plugin revision, scope, config).
pub struct InstancePool {
    compiled: Arc<Compiled>,
    pub scope: Arc<Scope>,
    slots: Vec<Mutex<Option<Arc<VmSlot>>>>,
    next: AtomicUsize,
    pub stats: Arc<PluginStats>,
}

/// One instance, locked for the duration of a callback.
pub struct VmSlot {
    pub vm: Mutex<Vm>,
}

impl InstancePool {
    fn new(
        compiled: Arc<Compiled>,
        scope: Arc<Scope>,
        size: usize,
        stats: Arc<PluginStats>,
    ) -> Self {
        InstancePool {
            compiled,
            scope,
            slots: (0..size.max(1)).map(|_| Mutex::new(None)).collect(),
            next: AtomicUsize::new(0),
            stats,
        }
    }

    /// An instance for a new request: the next slot round-robin, started (or
    /// restarted, if the last one broke) on demand. Starting costs one
    /// instantiation plus the root context's start and configure callbacks.
    pub fn pick(&self) -> Result<Arc<VmSlot>, String> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let mut slot = self.slots[index].lock().unwrap_or_else(|p| p.into_inner());
        if let Some(current) = slot.as_ref() {
            let broken = current
                .vm
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .broken
                .is_some();
            if !broken {
                return Ok(current.clone());
            }
        }
        let started = Instant::now();
        match Vm::start(&self.compiled, self.scope.clone()) {
            Ok(vm) => {
                let fresh = Arc::new(VmSlot { vm: Mutex::new(vm) });
                *slot = Some(fresh.clone());
                Ok(fresh)
            }
            Err(err) => {
                let err = format!("could not start: {err}");
                self.stats.record(started.elapsed(), Some(&err));
                *slot = None;
                Err(err)
            }
        }
    }

    fn live(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.lock().unwrap_or_else(|p| p.into_inner()).is_some())
            .count()
    }
}

/// `legion_sign`: HMAC with a declared credential the module never reads.
struct PluginSigner {
    plugin: String,
    credentials:
        Arc<Mutex<HashMap<(String, String), (i64, crate::worker::forwarder::SecretValue)>>>,
    declared: Vec<String>,
}

/// The `legion_sign` request.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SignRequest {
    credential: String,
    algorithm: String,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    data_base64: Option<String>,
    #[serde(default)]
    date: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    string_to_sign: Option<String>,
}

impl vm::Signer for PluginSigner {
    fn sign(&self, request: &[u8]) -> Result<Vec<u8>, String> {
        let request: SignRequest =
            serde_json::from_slice(request).map_err(|err| format!("bad request: {err}"))?;
        if !self.declared.contains(&request.credential) {
            return Err(format!(
                "plugin {} declares no credential {:?}",
                self.plugin, request.credential
            ));
        }
        let key = self
            .credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(self.plugin.clone(), request.credential.clone()))
            .map(|(_, value)| value.clone())
            .ok_or_else(|| {
                format!(
                    "credential {:?} has not reached this node yet",
                    request.credential
                )
            })?;
        sign(key.expose().as_bytes(), &request)
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&key, data).as_ref().to_vec()
}

fn sign(key: &[u8], request: &SignRequest) -> Result<Vec<u8>, String> {
    match request.algorithm.as_str() {
        "hmac-sha256" => {
            let data = match (&request.data, &request.data_base64) {
                (Some(text), None) => text.as_bytes().to_vec(),
                (None, Some(encoded)) => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map_err(|err| format!("dataBase64: {err}"))?
                }
                _ => return Err("hmac-sha256 takes exactly one of data or dataBase64".into()),
            };
            Ok(hmac(key, &data))
        }
        // AWS Signature Version 4: the signing key is derived from the
        // secret, the date, the region, and the service; the signature over
        // the module's string-to-sign comes back hex-encoded.
        "aws4-hmac-sha256" => {
            let (Some(date), Some(region), Some(service), Some(string_to_sign)) = (
                request.date.as_deref(),
                request.region.as_deref(),
                request.service.as_deref(),
                request.string_to_sign.as_deref(),
            ) else {
                return Err(
                    "aws4-hmac-sha256 needs date (YYYYMMDD), region, service, and stringToSign"
                        .into(),
                );
            };
            let mut secret = b"AWS4".to_vec();
            secret.extend_from_slice(key);
            let k_date = hmac(&secret, date.as_bytes());
            let k_region = hmac(&k_date, region.as_bytes());
            let k_service = hmac(&k_region, service.as_bytes());
            let k_signing = hmac(&k_service, b"aws4_request");
            let signature = hmac(&k_signing, string_to_sign.as_bytes());
            Ok(signature
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
                .into_bytes())
        }
        other => Err(format!(
            "unknown algorithm {other:?}; legion_sign supports hmac-sha256 and aws4-hmac-sha256"
        )),
    }
}

#[cfg(test)]
mod plugins_test {
    use super::*;

    #[test]
    fn sigv4_matches_the_aws_example_test() {
        // From the AWS SigV4 documentation's worked GET example.
        let request = SignRequest {
            credential: "aws".into(),
            algorithm: "aws4-hmac-sha256".into(),
            data: None,
            data_base64: None,
            date: Some("20150830".into()),
            region: Some("us-east-1".into()),
            service: Some("iam".into()),
            string_to_sign: Some(
                "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n\
                 f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
                    .into(),
            ),
        };
        let signature = sign(b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", &request).unwrap();
        assert_eq!(
            String::from_utf8(signature).unwrap(),
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn stats_report_a_p99_test() {
        let stats = PluginStats::default();
        for micros in 1..=100u64 {
            stats.record(Duration::from_micros(micros), None);
        }
        assert_eq!(stats.p99_micros(), 99);
        stats.record(Duration::from_micros(1), Some("boom"));
        assert_eq!(stats.errors.load(Ordering::Relaxed), 1);
        assert_eq!(stats.last_error(), "boom");
    }

    #[test]
    fn checksums_are_verified_test() {
        let sum = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        verify_checksum(b"hello", sum).unwrap();
        assert!(verify_checksum(b"hellp", sum).is_err());
    }
}
