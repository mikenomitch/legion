//! The volume plugin system: the contract between Legion and storage drivers.
//!
//! See `docs/volumes.md` for the user-facing doc and
//! `internal_docs/plans/volume-plugin-system-plan.md` for the design (its
//! judgement calls are in `internal_docs/calls/volume-plugin-system-plan_calls.md`).
//!
//! Layout:
//! - this module — the [`VolumePlugin`] trait, request/response types, and the
//!   [`VolumePluginRegistry`];
//! - [`builtins`] — the in-process `local-dir`, `host`, `nfs`, `tmpfs` drivers;
//! - [`exec`] — the CNI-style exec ABI for out-of-tree plugins;
//! - [`conformance`] — the contract test harness shared by unit tests and
//!   `legion volume plugin test`.
//!
//! **Everything here runs on a worker.** The manager owns the `volume` table,
//! node pinning, and destroy orchestration, but never loads a driver: it learns
//! what plugins exist from the capabilities workers advertise in their
//! fingerprints. That keeps storage credentials and `mount(2)` out of the
//! control plane.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::prelude::*;

pub mod builtins;
pub mod conformance;
pub mod exec;

pub use exec::ExecVolumeDriver;

/// Volume contract version. Bumped only for breaking ABI changes; exec plugins
/// declare it in `plugin.json` (`legionVolumeApiVersion`) and echo it in every
/// response.
pub const VOLUME_API_VERSION: u32 = 1;

/// Plugin used when a task declares storage without naming one.
pub const DEFAULT_VOLUME_DRIVER: &str = "local-dir";

/// The drivers compiled into every Legion binary.
///
/// Used as the assumed plugin set for a node that advertises no
/// `volumeDrivers` fingerprint at all — a worker from before this feature, or
/// a test fixture. Excluding such nodes outright would strand every stateful
/// job until an entire cluster was upgraded; assuming the built-ins is both
/// backward-compatible and true, since they ship in the binary.
pub const BUILTIN_VOLUME_DRIVERS: [&str; 4] = ["local-dir", "host", "nfs", "tmpfs"];

/// The only access mode v1 admits. Reported by `capabilities` and recorded on
/// the volume row so multi-reader can be enabled per plugin later without a
/// jobspec change (plan, resolved question 2).
pub const ACCESS_MODE_SINGLE_WRITER: &str = "single-writer";

/// Access mode for volumes several tasks may mount read-only at once. Only the
/// `host` plugin advertises it today (it has no per-instance lifecycle).
pub const ACCESS_MODE_MULTI_READER: &str = "multi-reader";

/// An operation in the volume contract.
///
/// v1 dispatches the first four. The rest are **reserved**: their names are
/// fixed so a plugin can advertise them in `capabilities.ops` and Legion can
/// start dispatching them without an ABI break (plan, judgement call 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeOp {
    Capabilities,
    Mount,
    Unmount,
    Destroy,
    // --- reserved ---
    Create,
    Attach,
    Detach,
    Snapshot,
    Restore,
    Clone,
    Expand,
}

impl VolumeOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            VolumeOp::Capabilities => "capabilities",
            VolumeOp::Mount => "mount",
            VolumeOp::Unmount => "unmount",
            VolumeOp::Destroy => "destroy",
            VolumeOp::Create => "create",
            VolumeOp::Attach => "attach",
            VolumeOp::Detach => "detach",
            VolumeOp::Snapshot => "snapshot",
            VolumeOp::Restore => "restore",
            VolumeOp::Clone => "clone",
            VolumeOp::Expand => "expand",
        }
    }
}

impl std::fmt::Display for VolumeOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// How a volume is handed to the consumer.
///
/// The same split Kubernetes makes with `volumeMode: Filesystem | Block`, for
/// the same reason: a process consumes a directory, but a guest kernel consumes
/// a block device. Without it, a microVM task kind cannot use a volume at all
/// and has to grow its own parallel storage mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeMode {
    /// A host directory the consumer reads and writes directly. What `exec`
    /// and `container` tasks want, and the default.
    #[default]
    Filesystem,
    /// A raw block device or disk image the consumer attaches — a virtio-blk
    /// backing store for a microVM, say. `mount` returns `devicePath` (and the
    /// filesystem it was formatted with) instead of `mountPath`.
    Block,
}

impl VolumeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            VolumeMode::Filesystem => "filesystem",
            VolumeMode::Block => "block",
        }
    }
}

impl std::fmt::Display for VolumeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for VolumeMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "filesystem" => Ok(VolumeMode::Filesystem),
            "block" => Ok(VolumeMode::Block),
            other => Err(format!(
                "unknown volume mode {other:?} (expected `filesystem` or `block`)"
            )),
        }
    }
}

/// Default filesystem a block volume is formatted with when the job names none.
pub const DEFAULT_BLOCK_FS: &str = "ext4";

/// Default size for a block volume when the job names none. Block volumes are
/// fixed-size images, so unlike a directory there has to be *some* number.
pub const DEFAULT_BLOCK_SIZE: &str = "1GiB";

/// What a plugin can do, and whether it can do it *here*.
///
/// The manager consumes this from worker fingerprints: it is the only thing
/// that lets admission reject `plugin: 'zfs'` before a task ever starts, and
/// the only source of the node-local flag the scheduler pins on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeCapabilities {
    /// Contract version this plugin speaks.
    #[serde(default = "default_api_version")]
    pub api_version: u32,
    /// Plugin name; must match the registry key it was loaded under.
    pub name: String,
    /// True when a volume only exists on the node that created it, so an
    /// instance holding one can only be replaced on that same node.
    pub node_local: bool,
    /// True when the volume's contents do NOT survive an unmount (`tmpfs`).
    /// Rejected for `stateful.storage`, which promises data survives
    /// reschedules.
    #[serde(default)]
    pub ephemeral: bool,
    /// Access modes the driver supports. v1 admits only
    /// [`ACCESS_MODE_SINGLE_WRITER`] for instance storage.
    #[serde(default = "default_access_modes")]
    pub access_modes: Vec<String>,
    /// Volume modes the driver can serve. Admission intersects this with the
    /// task kind's supported modes and the job's requested mode, so a
    /// filesystem-only driver can never be handed to a task that needs a block
    /// device (or vice versa).
    #[serde(default = "default_volume_modes")]
    pub volume_modes: Vec<VolumeMode>,
    /// Params a job MUST supply. Admission names the missing one.
    #[serde(default)]
    pub required_params: Vec<String>,
    /// Params a job MAY supply, for documentation and `legion volume plugins`.
    #[serde(default)]
    pub optional_params: Vec<String>,
    /// Reserved ops this plugin implements beyond the required four. Nothing
    /// dispatches these yet; advertising one is forward-compatible.
    #[serde(default)]
    pub ops: Vec<String>,
    /// False when the driver is installed but unusable on this node (no
    /// `mount` binary, wrong OS, host volume undeclared). Placement skips the
    /// node and admission explains why instead of failing at task start.
    #[serde(default = "default_true")]
    pub available: bool,
    /// One line of human-readable context — why it is unavailable, or what it
    /// does. Surfaced verbatim by `legion volume plugins` and the UI.
    #[serde(default)]
    pub detail: String,
}

fn default_api_version() -> u32 {
    VOLUME_API_VERSION
}

fn default_true() -> bool {
    true
}

fn default_access_modes() -> Vec<String> {
    vec![ACCESS_MODE_SINGLE_WRITER.to_string()]
}

fn default_volume_modes() -> Vec<VolumeMode> {
    vec![VolumeMode::Filesystem]
}

impl VolumeCapabilities {
    /// A single-writer, node-local, available plugin — the shape most drivers
    /// start from.
    pub fn new(name: impl Into<String>, node_local: bool) -> Self {
        VolumeCapabilities {
            api_version: VOLUME_API_VERSION,
            name: name.into(),
            node_local,
            ephemeral: false,
            access_modes: default_access_modes(),
            volume_modes: default_volume_modes(),
            required_params: Vec::new(),
            optional_params: Vec::new(),
            ops: Vec::new(),
            available: true,
            detail: String::new(),
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = detail.into();
        self
    }

    pub fn unavailable(mut self, detail: impl Into<String>) -> Self {
        self.available = false;
        self.detail = detail.into();
        self
    }

    pub fn with_params(mut self, required: &[&str], optional: &[&str]) -> Self {
        self.required_params = required.iter().map(|value| value.to_string()).collect();
        self.optional_params = optional.iter().map(|value| value.to_string()).collect();
        self
    }

    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }

    pub fn with_access_modes(mut self, modes: &[&str]) -> Self {
        self.access_modes = modes.iter().map(|value| value.to_string()).collect();
        self
    }

    pub fn with_volume_modes(mut self, modes: &[VolumeMode]) -> Self {
        self.volume_modes = modes.to_vec();
        self
    }

    pub fn serves_mode(&self, mode: VolumeMode) -> bool {
        self.volume_modes.contains(&mode)
    }

    /// Names the params a job left out. Used by admission so the error points
    /// at the field, not at a driver stack trace.
    pub fn missing_params(&self, params: &BTreeMap<String, Value>) -> Vec<String> {
        self.required_params
            .iter()
            .filter(|name| !params.contains_key(*name))
            .cloned()
            .collect()
    }
}

/// The volume a request acts on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeRef {
    /// Stable volume id (the second half of the `plugin/volume_id` handle).
    pub id: String,
    /// Declared volume name within the task (`data`, `models`, …). Becomes
    /// `LEGION_VOLUME_<NAME>`.
    pub name: String,
    pub driver: String,
    /// Driver-owned metadata recorded on the volume row by a previous mount
    /// (an NFS export path, a ZFS dataset). Opaque to Legion.
    #[serde(default)]
    pub attrs: BTreeMap<String, Value>,
    #[serde(default)]
    pub read_only: bool,
}

/// The instance a volume belongs to, when it belongs to one. `None` for
/// `task.volumes[]` claims, which have no per-instance lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeInstanceRef {
    pub id: String,
    pub job: String,
    pub group: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,
}

/// The node the operation runs on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeNodeRef {
    pub id: String,
    /// The worker's state directory.
    pub data_dir: PathBuf,
    /// Where plugins should put volume data by default
    /// (`<data_dir>/volumes`). Plugins are free to ignore it.
    pub volume_dir: PathBuf,
}

/// One volume operation request. The same struct serializes to the exec ABI's
/// stdin payload, so in-process and exec plugins see byte-identical inputs —
/// which is what makes the conformance harness meaningful for both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeRequest {
    #[serde(default = "default_api_version")]
    pub api_version: u32,
    pub op: VolumeOp,
    /// How the consumer wants the volume: a directory (`filesystem`, the
    /// default) or a block device/image (`block`).
    #[serde(default)]
    pub mode: VolumeMode,
    pub volume: VolumeRef,
    /// Plugin-specific params straight from the jobspec, unvalidated beyond
    /// the plugin's own `requiredParams`.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<VolumeInstanceRef>,
    /// The allocation this mount serves. 0 for lifecycle ops (`destroy`) that
    /// belong to no allocation.
    #[serde(default)]
    pub alloc_id: i64,
    pub node: VolumeNodeRef,
}

impl VolumeRequest {
    /// A request for `op` against `volume` on `node`, with no instance or
    /// allocation. Lifecycle ops and tests use this; the mount pipeline fills
    /// in the rest.
    pub fn new(op: VolumeOp, volume: VolumeRef, node: VolumeNodeRef) -> Self {
        VolumeRequest {
            api_version: VOLUME_API_VERSION,
            op,
            mode: VolumeMode::Filesystem,
            volume,
            params: BTreeMap::new(),
            instance: None,
            alloc_id: 0,
            node,
        }
    }

    pub fn with_mode(mut self, mode: VolumeMode) -> Self {
        self.mode = mode;
        self
    }

    /// The filesystem a block volume should carry, defaulting to
    /// [`DEFAULT_BLOCK_FS`]. Meaningless in filesystem mode.
    pub fn block_fs(&self) -> String {
        self.param_str("fs")
            .unwrap_or_else(|| DEFAULT_BLOCK_FS.to_string())
    }

    /// The size a block volume should be created at, in bytes. Block volumes
    /// are fixed-size images, so unlike a directory there has to be a number.
    pub fn block_size_bytes(&self) -> R<u64> {
        let raw = self
            .param_str("size")
            .unwrap_or_else(|| DEFAULT_BLOCK_SIZE.to_string());
        crate::structs::config::parse_byte_size(&raw)
            .with_context(|| format!("invalid volume `size` param {raw:?}"))
    }

    pub fn with_params(mut self, params: BTreeMap<String, Value>) -> Self {
        self.params = params;
        self
    }

    /// A param as a string, whether the jobspec wrote it as a string, number,
    /// or bool. Nickel users write `size = "10G"` and `port = 2049`
    /// interchangeably; drivers should not have to care.
    pub fn param_str(&self, key: &str) -> Option<String> {
        param_str(&self.params, key)
    }

    pub fn param_bool(&self, key: &str) -> Option<bool> {
        match self.params.get(key)? {
            Value::Bool(value) => Some(*value),
            Value::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    pub fn require_param(&self, key: &str) -> R<String> {
        self.param_str(key).with_context(|| {
            format!(
                "volume driver {} requires the `{key}` param",
                self.volume.driver
            )
        })
    }
}

/// String view of a param value, accepting the JSON scalar shapes Nickel
/// produces.
pub fn param_str(params: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    match params.get(key)? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// What a successful `mount` returns.
///
/// Exactly one of `mount_path` / `device_path` is populated, per the requested
/// [`VolumeMode`].
///
/// **A returned device need not be fully populated yet.** A driver is allowed
/// to hand back a device backed by lazy fetch (an NBD/FUSE device over a
/// content-addressed base, a qcow2 backing chain, dm-thin over a sparse file)
/// so a consumer can start before the data has finished syncing. Nothing in the
/// contract requires `mount` to block until every byte is local — plugin authors
/// should feel free to rely on that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MountResult {
    /// Filesystem mode: the absolute host path the allocation reads and writes.
    /// For container tasks this becomes the bind source; for exec tasks it is
    /// the value of `LEGION_VOLUME_<NAME>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_path: Option<PathBuf>,
    /// Block mode: an absolute path to a raw block device or raw disk image
    /// the consumer attaches (a virtio-blk backing store, say). A plain image
    /// file is a valid answer and needs no privileges to produce, which is why
    /// `local-dir` can serve block mode at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_path: Option<PathBuf>,
    /// Block mode: the filesystem the device carries (`ext4`), so the consumer
    /// knows what to mount inside the guest without probing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs: Option<String>,
    /// True when this mount **created** the storage rather than reattaching it.
    /// Lets a consumer tell first-boot-initialize from reattach without
    /// guessing from an empty directory — and makes "is this volume fresh?" a
    /// driver-side fact.
    #[serde(default)]
    pub fresh: bool,
    /// Driver metadata to persist on the volume row and hand back on the next
    /// operation. Empty for drivers that need no state.
    #[serde(default)]
    pub attrs: BTreeMap<String, Value>,
}

impl MountResult {
    /// A filesystem-mode result.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        MountResult {
            mount_path: Some(path.into()),
            device_path: None,
            fs: None,
            fresh: false,
            attrs: BTreeMap::new(),
        }
    }

    /// A block-mode result: the device (or image) and the filesystem on it.
    pub fn device(path: impl Into<PathBuf>, fs: impl Into<String>) -> Self {
        MountResult {
            mount_path: None,
            device_path: Some(path.into()),
            fs: Some(fs.into()),
            fresh: false,
            attrs: BTreeMap::new(),
        }
    }

    pub fn fresh(mut self, fresh: bool) -> Self {
        self.fresh = fresh;
        self
    }

    pub fn with_attr(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.attrs.insert(key.to_string(), value.into());
        self
    }

    /// The path the consumer actually uses, whichever mode produced it.
    pub fn resolved_path(&self) -> Option<&PathBuf> {
        self.device_path.as_ref().or(self.mount_path.as_ref())
    }

    /// Reject a driver that answered in the wrong shape — a filesystem-mode
    /// result for a block request is a plugin bug, and catching it here beats
    /// the consumer failing on a path it cannot open.
    pub fn validate_for(&self, mode: VolumeMode, driver: &str) -> R<()> {
        let path = match mode {
            VolumeMode::Filesystem => self.mount_path.as_ref(),
            VolumeMode::Block => self.device_path.as_ref(),
        };
        let path = path.with_context(|| {
            format!(
                "volume driver `{driver}` returned no {} for a {mode} mount",
                match mode {
                    VolumeMode::Filesystem => "mountPath",
                    VolumeMode::Block => "devicePath",
                }
            )
        })?;
        if !path.is_absolute() {
            bail!("volume driver `{driver}` returned a relative path {path:?}");
        }
        Ok(())
    }
}

/// A storage driver.
///
/// **Every method must be idempotent.** Legion retries on worker restarts and
/// partial failures, so "already mounted" / "already gone" is a success, not an
/// error. That rule is what lets the manager treat volume state as a simple
/// two-phase state machine.
pub trait VolumeDriver: Send + Sync {
    fn name(&self) -> &str;

    /// What this driver supports, evaluated against *this* node right now —
    /// see [`VolumeCapabilities::available`].
    fn capabilities(&self) -> VolumeCapabilities;

    /// Make the volume available at a host path, provisioning it if this is
    /// the first mount. Idempotent: mounting an already-mounted volume returns
    /// the same path.
    fn mount(&self, request: &VolumeRequest) -> R<MountResult>;

    /// Release the mount. The volume and its data survive. Idempotent:
    /// unmounting what is not mounted succeeds.
    fn unmount(&self, request: &VolumeRequest) -> R<()>;

    /// Permanently delete the volume and its data. Idempotent: destroying what
    /// is already gone succeeds.
    fn destroy(&self, request: &VolumeRequest) -> R<()>;
}

/// Where a worker keeps volume data and finds out-of-tree plugins.
#[derive(Debug, Clone)]
pub struct VolumeRuntimeConfig {
    /// `<worker state dir>/volumes` — the default parent for driver data.
    pub volume_root: PathBuf,
    /// The worker's state directory (`node.dataDir` in the ABI).
    pub data_dir: PathBuf,
    /// Operator-declared host volumes, keyed by claim name (`host` plugin).
    pub host_volumes: HashMap<String, HostVolumeConfig>,
    /// Root to scan for exec plugins (`<dir>/volumes/<name>/`).
    pub plugin_dir: PathBuf,
}

impl Default for VolumeRuntimeConfig {
    fn default() -> Self {
        VolumeRuntimeConfig {
            volume_root: PathBuf::from("volumes"),
            data_dir: PathBuf::from("."),
            host_volumes: HashMap::new(),
            plugin_dir: crate::util::plugin_root_dir(),
        }
    }
}

/// One entry of the worker config's `hostVolumes` map: a path on this machine
/// that jobs may claim by name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HostVolumeConfig {
    /// Absolute path on the worker. Filesystem mode.
    #[serde(default)]
    pub path: String,
    /// Absolute path to a block device (`/dev/mapper/legion-models`) or raw
    /// disk image. Block mode — the bare-metal escape hatch for handing a
    /// dedicated device to a guest. Exactly one of `path`/`device` should be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    /// Filesystem the device carries, reported to the consumer so it knows what
    /// to mount inside a guest. Block mode only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs: Option<String>,
    /// When true, tasks may only mount it read-only. A read-write claim is
    /// rejected rather than silently downgraded.
    #[serde(default)]
    pub read_only: bool,
    /// Create the directory if missing. Off by default: a typo in a path
    /// should not silently produce an empty volume where real data was meant
    /// to be.
    #[serde(default)]
    pub create: bool,
}

/// The plugins available on one worker: built-ins plus anything discovered
/// under `plugins/volumes/`.
pub struct VolumeDriverRegistry {
    drivers: HashMap<String, Box<dyn VolumeDriver>>,
    config: VolumeRuntimeConfig,
}

impl VolumeDriverRegistry {
    /// Built-ins only. `with_discovery` adds exec plugins from disk.
    pub fn with_builtins(config: VolumeRuntimeConfig) -> Self {
        let mut registry = VolumeDriverRegistry {
            drivers: HashMap::new(),
            config: config.clone(),
        };
        for driver in builtins::all(&config) {
            registry.drivers.insert(driver.name().to_string(), driver);
        }
        registry
    }

    /// Built-ins plus every valid exec plugin under
    /// `<plugin_dir>/volumes/<name>/`. Discovery failures are logged and
    /// skipped — one broken plugin directory must not stop a worker from
    /// starting.
    pub fn with_discovery(config: VolumeRuntimeConfig) -> Self {
        let mut registry = Self::with_builtins(config.clone());
        let root = config.plugin_dir.join("volumes");
        match exec::discover(&root) {
            Ok(found) => {
                for driver in found {
                    let name = driver.name().to_string();
                    if registry.drivers.contains_key(&name) {
                        // Built-ins win: an exec plugin cannot silently
                        // shadow `local-dir` and change what a running
                        // cluster's default storage means.
                        warn!(
                            driver = %name,
                            "Ignoring exec volume driver that shadows a built-in"
                        );
                        continue;
                    }
                    info!(driver = %name, "Loaded exec volume driver");
                    registry.drivers.insert(name, Box::new(driver));
                }
            }
            Err(err) => {
                debug!(dir = %root.display(), error = ?err, "No exec volume drivers discovered");
            }
        }
        registry
    }

    pub fn config(&self) -> &VolumeRuntimeConfig {
        &self.config
    }

    pub fn get(&self, name: &str) -> Option<&dyn VolumeDriver> {
        self.drivers.get(name).map(|driver| driver.as_ref())
    }

    pub fn require(&self, name: &str) -> R<&dyn VolumeDriver> {
        self.get(name).with_context(|| {
            format!(
                "no volume driver named `{name}` on this node (have: {})",
                self.names().join(", ")
            )
        })
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self.drivers.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    /// Capabilities of every registered driver, keyed by name. This is what
    /// the worker publishes in `fingerprint.volumeDrivers` and what the
    /// manager validates jobs against.
    pub fn capabilities(&self) -> BTreeMap<String, VolumeCapabilities> {
        let mut out = BTreeMap::new();
        for (name, driver) in &self.drivers {
            out.insert(name.clone(), driver.capabilities());
        }
        out
    }

    /// The node reference plugins receive. Built once per registry so every
    /// request agrees on the paths.
    pub fn node_ref(&self, node_id: &str) -> VolumeNodeRef {
        VolumeNodeRef {
            id: node_id.to_string(),
            data_dir: self.config.data_dir.clone(),
            volume_dir: self.config.volume_root.clone(),
        }
    }
}

/// A parsed `{plugin}/{volume_id}` storage handle.
///
/// The format predates volume drivers (instances already minted
/// `local-dir/<rand>`), which is exactly why it was kept: every existing
/// instance row parses without a migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageHandle {
    pub driver: String,
    pub volume_id: String,
}

impl StorageHandle {
    pub fn new(driver: impl Into<String>, volume_id: impl Into<String>) -> Self {
        StorageHandle {
            driver: driver.into(),
            volume_id: volume_id.into(),
        }
    }

    /// Parse `driver/volume_id`. Driver names never contain `/`, so the split
    /// is unambiguous even if a driver ever puts one in a volume id.
    pub fn parse(raw: &str) -> R<Self> {
        let (driver, volume_id) = raw
            .split_once('/')
            .with_context(|| format!("malformed storage handle {raw:?}: expected driver/id"))?;
        if driver.is_empty() || volume_id.is_empty() {
            bail!("malformed storage handle {raw:?}: expected driver/id");
        }
        Ok(StorageHandle {
            driver: driver.to_string(),
            volume_id: volume_id.to_string(),
        })
    }
}

impl std::fmt::Display for StorageHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.driver, self.volume_id)
    }
}

/// Best-effort "is something mounted at this path" check, used by the
/// mount-based drivers to stay idempotent.
///
/// Linux reads `/proc/self/mounts`; everything else shells out to `mount`. A
/// false negative just means we try to mount again and the kernel tells us it
/// is busy, which the drivers treat as success — so being approximate here is
/// safe.
pub fn is_mount_point(path: &Path) -> bool {
    let target = path.to_string_lossy();
    #[cfg(target_os = "linux")]
    {
        if let Ok(mounts) = std::fs::read_to_string("/proc/self/mounts") {
            return mounts.lines().any(|line| {
                line.split_whitespace()
                    .nth(1)
                    .is_some_and(|mount_point| mount_point == target)
            });
        }
    }
    std::process::Command::new("mount")
        .output()
        .ok()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.contains(&format!(" on {target} ")))
        })
        .unwrap_or(false)
}

/// Run a mount-family command, returning its stderr on failure so the driver
/// error names the real problem (`access denied by server`) instead of an exit
/// code.
pub fn run_mount_command(program: &str, args: &[String]) -> R<()> {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("failed to run `{program}` (is it installed and on PATH?)"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    bail!(
        "`{program} {}` failed: {}",
        args.join(" "),
        if stderr.is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr
        }
    );
}

/// Create a directory (and parents) with a plugin-supplied mode.
pub fn create_dir_with_mode(path: &Path, mode: Option<&str>) -> R<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create volume directory {}", path.display()))?;
    if let Some(mode) = mode {
        let parsed = u32::from_str_radix(mode.trim_start_matches("0o"), 8)
            .with_context(|| format!("invalid volume `mode` param {mode:?}: expected octal"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(parsed))
                .with_context(|| format!("failed to chmod {}", path.display()))?;
        }
        #[cfg(not(unix))]
        let _ = parsed;
    }
    Ok(())
}

/// The env var name a volume is exposed under: `LEGION_VOLUME_<NAME>`, with
/// the name upper-cased and non-alphanumerics folded to `_`.
pub fn volume_env_var(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("LEGION_VOLUME_{sanitized}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_handle_roundtrips() -> R<()> {
        let handle = StorageHandle::parse("local-dir/abc123")?;
        assert_eq!(handle.driver, "local-dir");
        assert_eq!(handle.volume_id, "abc123");
        assert_eq!(handle.to_string(), "local-dir/abc123");
        Ok(())
    }

    #[test]
    fn storage_handle_rejects_malformed() {
        assert!(StorageHandle::parse("nope").is_err());
        assert!(StorageHandle::parse("/abc").is_err());
        assert!(StorageHandle::parse("driver/").is_err());
    }

    #[test]
    fn volume_env_var_sanitizes_names() {
        assert_eq!(volume_env_var("data"), "LEGION_VOLUME_DATA");
        assert_eq!(volume_env_var("my-vol.1"), "LEGION_VOLUME_MY_VOL_1");
    }

    #[test]
    fn param_str_accepts_scalar_shapes() {
        let mut params = BTreeMap::new();
        params.insert("size".to_string(), json!("10G"));
        params.insert("port".to_string(), json!(2049));
        params.insert("ro".to_string(), json!(true));
        assert_eq!(param_str(&params, "size"), Some("10G".into()));
        assert_eq!(param_str(&params, "port"), Some("2049".into()));
        assert_eq!(param_str(&params, "ro"), Some("true".into()));
        assert_eq!(param_str(&params, "missing"), None);
    }

    #[test]
    fn missing_params_names_the_gap() {
        let caps = VolumeCapabilities::new("nfs", false).with_params(&["server", "export"], &[]);
        let mut params = BTreeMap::new();
        params.insert("server".to_string(), json!("10.0.0.1"));
        assert_eq!(caps.missing_params(&params), vec!["export".to_string()]);
    }
}
