//! The task-level `suspend` jobspec block and the suspend state an instance
//! carries (suspend plan §6.6, §7; docs/suspend-and-resume.md).
//!
//! ```nickel
//! suspend = {
//!     checkpoint = "memory",        # "memory" | "disk"; default: the best the runtime can take
//!     reserve = 0.25,               # hold a share of the reservation while suspended
//!                                   # (false | true | 0..1 | { memory = "1GiB", cpu = 100 })
//!     onRestoreFailure = "cold",    # "cold" | "fail"
//!     resumeTimeout = "30s",        # how long a resume waits for its node before booting cold
//!     onResume = ["/usr/local/bin/reconnect"],  # run in the guest after every restore
//!     durability = "local",         # "local" | "store" (publish to the worker's contentStore)
//!     archive = "slim",             # "slim" | "manifestOnly": once published, keep the
//!                                   # checkpoint's manifest in the bucket, not the store
//!     idle = {                      # automatic suspend when quiet
//!         after = "3m",
//!         signals = ["network", "cpu", "exec"],
//!         cpuBelow = 0.02,
//!     },
//!     onPressure = true,            # the `suspend` memory pressure step may take it
//! },
//! ```
//!
//! The `hint` idle signal (the workload's own busy/idle report) is not
//! implemented. A spec that lists it is refused at submit rather than
//! accepted and ignored.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::structs::checkpoint::SnapshotKind;
use crate::structs::snapshot::SnapshotDurability;

/// Default `suspend.resumeTimeout`.
pub const DEFAULT_RESUME_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for `suspend.onResume` inside the guest. The guest's
/// resume handler enforces it, so it is defined there.
pub use legion_guest::resume::ON_RESUME_TIMEOUT;

/// Default `suspend.idle.after`.
pub const DEFAULT_IDLE_AFTER: Duration = Duration::from_secs(180);

/// Shortest `suspend.idle.after` accepted. The worker samples activity every
/// 10s and the manager sweeps every 15s by default, so a shorter window
/// could not be honored and would read as a promise the system breaks.
pub const MIN_IDLE_AFTER: Duration = Duration::from_secs(30);

/// Default `suspend.idle.cpuBelow`: 2% of the task's `cpu`.
pub const DEFAULT_IDLE_CPU_BELOW: f64 = 0.02;

/// One kind of activity that keeps an instance awake (plan §6.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IdleSignal {
    /// Bytes through any Legion-owned listener or proxy (the node datapath).
    Network,
    /// Any open connection through the datapath, even a quiet one: ingress
    /// the datapath fronts, and egress through a gated allocation's proxy.
    Connections,
    /// CPU use above `cpuBelow` of the task's `cpu`.
    Cpu,
    /// An open exec session (`legion alloc exec`, an action).
    Exec,
}

impl IdleSignal {
    pub const DEFAULTS: [IdleSignal; 3] = [IdleSignal::Network, IdleSignal::Cpu, IdleSignal::Exec];

    pub fn as_str(&self) -> &'static str {
        match self {
            IdleSignal::Network => "network",
            IdleSignal::Connections => "connections",
            IdleSignal::Cpu => "cpu",
            IdleSignal::Exec => "exec",
        }
    }

    /// Parse one `signals` entry. `hint` is refused with a pointer at what
    /// works today.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "network" => Ok(IdleSignal::Network),
            "connections" => Ok(IdleSignal::Connections),
            "cpu" => Ok(IdleSignal::Cpu),
            "exec" => Ok(IdleSignal::Exec),
            "hint" => Err(
                "suspend.idle.signals \"hint\" is not implemented yet: the workload's own \
                 busy/idle signal ships in suspend plan Phase 8\nHint: remove \"hint\". To keep \
                 an instance awake through a quiet call, add \"connections\" (any open \
                 connection through the node datapath, inbound or out through a gated \
                 task's egress proxy, counts as activity; a sandbox has one only on gated \
                 networking)"
                    .to_string(),
            ),
            other => Err(format!(
                "suspend.idle.signals {other:?} is not valid (expected \"network\", \
                 \"connections\", \"cpu\", or \"exec\")"
            )),
        }
    }
}

/// The `suspend.idle` block, as written.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdleConfig {
    /// Quiet for this long, then suspend. Default `"3m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// What counts as activity. Default `["network", "cpu", "exec"]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signals: Option<Vec<String>>,
    /// The `cpu` signal's threshold, as a fraction of the task's `cpu`.
    /// Default 0.02.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_below: Option<f64>,
}

/// [`IdleConfig`] with defaults applied.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedIdle {
    pub after: Duration,
    /// Deduplicated, sorted.
    pub signals: Vec<IdleSignal>,
    pub cpu_below: f64,
}

impl Eq for ResolvedIdle {}

impl Default for ResolvedIdle {
    fn default() -> Self {
        ResolvedIdle {
            after: DEFAULT_IDLE_AFTER,
            signals: IdleSignal::DEFAULTS.to_vec(),
            cpu_below: DEFAULT_IDLE_CPU_BELOW,
        }
    }
}

impl IdleConfig {
    pub fn resolve(&self) -> Result<ResolvedIdle, String> {
        let after = match self.after.as_deref().map(str::trim) {
            None | Some("") => DEFAULT_IDLE_AFTER,
            Some(raw) => {
                let after = humantime::parse_duration(raw).map_err(|err| {
                    format!("suspend.idle.after {raw:?} is not a duration: {err}")
                })?;
                if after < MIN_IDLE_AFTER {
                    return Err(format!(
                        "suspend.idle.after {raw:?} is shorter than {}s: workers report activity \
                         every 10s and the manager checks every 15s, so a shorter window cannot \
                         be honored\nHint: set after = \"30s\" or longer",
                        MIN_IDLE_AFTER.as_secs()
                    ));
                }
                after
            }
        };
        let mut signals = match self.signals.as_ref() {
            None => IdleSignal::DEFAULTS.to_vec(),
            Some(names) => names
                .iter()
                .map(|name| IdleSignal::parse(name))
                .collect::<Result<Vec<_>, _>>()?,
        };
        signals.sort();
        signals.dedup();
        if signals.is_empty() {
            return Err(
                "suspend.idle.signals is empty, so nothing would ever count as activity and \
                 the instance would suspend `after` it starts, every time\nHint: list at least \
                 one signal, or remove signals to use the default [\"network\", \"cpu\", \"exec\"]"
                    .to_string(),
            );
        }
        let cpu_below = self.cpu_below.unwrap_or(DEFAULT_IDLE_CPU_BELOW);
        if !(cpu_below > 0.0 && cpu_below <= 1.0) {
            return Err(format!(
                "suspend.idle.cpuBelow {cpu_below} is not valid: it is a fraction of the task's \
                 cpu, greater than 0 and at most 1 (0.02 is 2%)"
            ));
        }
        Ok(ResolvedIdle {
            after,
            signals,
            cpu_below,
        })
    }
}

/// `suspend.reserve`, as written: a flag, a ratio, or fixed amounts.
///
/// ```nickel
/// reserve = true,                            # 1.0: hold everything, cores included
/// reserve = 0.25,                            # a quarter of memory and cpu MHz
/// reserve = { memory = "1GiB", cpu = 100 },  # fixed amounts (MiB, MHz)
/// ```
///
/// Deserialized by shape only, so a range or amount error surfaces from
/// [`SuspendConfig::resolve`] with the field name and a hint, not as a
/// generic "did not match any variant".
#[derive(Debug, Clone, PartialEq)]
pub enum SuspendReserveConfig {
    Flag(bool),
    Ratio(f64),
    Amount(SuspendReserveAmount),
}

/// The fixed-amount form of `suspend.reserve`.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SuspendReserveAmount {
    /// A byte size (`"1GiB"`) or a number of MiB, like `resources.memory`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<serde_json::Value>,
    /// MHz, like `resources.cpu`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
}

impl Serialize for SuspendReserveConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            SuspendReserveConfig::Flag(flag) => serializer.serialize_bool(*flag),
            SuspendReserveConfig::Ratio(ratio) => serializer.serialize_f64(*ratio),
            SuspendReserveConfig::Amount(amount) => amount.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for SuspendReserveConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::Bool(flag) => Ok(SuspendReserveConfig::Flag(flag)),
            serde_json::Value::Number(number) => number
                .as_f64()
                .map(SuspendReserveConfig::Ratio)
                .ok_or_else(|| D::Error::custom("suspend.reserve is not a usable number")),
            object @ serde_json::Value::Object(_) => serde_json::from_value(object)
                .map(SuspendReserveConfig::Amount)
                .map_err(|err| {
                    D::Error::custom(format!(
                        "suspend.reserve {{ .. }} takes `memory` and `cpu` only: {err}"
                    ))
                }),
            other => Err(D::Error::custom(format!(
                "suspend.reserve must be true, false, a ratio between 0 and 1, or \
                 {{ memory = \"1GiB\", cpu = 100 }}, got {other}"
            ))),
        }
    }
}

/// [`SuspendReserveConfig`] resolved: what a suspended instance keeps
/// charged to its node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SuspendReserve {
    /// A share of the reservation's memory and cpu MHz, `0.0..=1.0`. At
    /// exactly 1.0 the whole reservation is held, exclusive `cores` and
    /// devices included. Below it, those are released.
    Ratio(f64),
    /// Fixed amounts, never more than the task reserves. `cores` and devices
    /// are released.
    Amount { memory_mib: f64, cpu_mhz: f64 },
}

// Every value is finite (checked by `resolve`), so equality is total.
impl Eq for SuspendReserve {}

impl SuspendReserve {
    /// `reserve = false`, the default: hold nothing.
    pub const NONE: SuspendReserve = SuspendReserve::Ratio(0.0);
    /// `reserve = true`: hold everything.
    pub const FULL: SuspendReserve = SuspendReserve::Ratio(1.0);

    /// Whether this holds the whole reservation.
    pub fn is_full(&self) -> bool {
        matches!(self, SuspendReserve::Ratio(ratio) if *ratio >= 1.0)
    }
}

impl SuspendReserveConfig {
    pub fn resolve(&self) -> Result<SuspendReserve, String> {
        match self {
            SuspendReserveConfig::Flag(true) => Ok(SuspendReserve::FULL),
            SuspendReserveConfig::Flag(false) => Ok(SuspendReserve::NONE),
            SuspendReserveConfig::Ratio(ratio) => {
                if !(ratio.is_finite() && (0.0..=1.0).contains(ratio)) {
                    return Err(format!(
                        "suspend.reserve {ratio} is out of range: a ratio is between 0 and 1 \
                         (0.25 holds a quarter of the instance's memory and cpu while it is \
                         suspended)\nHint: use true to hold everything, or a fixed amount like \
                         {{ memory = \"1GiB\", cpu = 100 }}"
                    ));
                }
                Ok(SuspendReserve::Ratio(*ratio))
            }
            SuspendReserveConfig::Amount(amount) => {
                let memory_mib = match amount.memory.as_ref() {
                    None => 0.0,
                    Some(serde_json::Value::Number(mib)) => mib.as_f64().unwrap_or(-1.0),
                    Some(serde_json::Value::String(size)) => {
                        crate::structs::config::parse_byte_size(size)
                            .map_err(|err| format!("suspend.reserve.memory: {err:#}"))?
                            as f64
                            / (1024.0 * 1024.0)
                    }
                    Some(other) => {
                        return Err(format!(
                            "suspend.reserve.memory must be a size like \"1GiB\" or a number of \
                             MiB, got {other}"
                        ))
                    }
                };
                let cpu_mhz = amount.cpu.unwrap_or(0.0);
                for (field, value) in [("memory", memory_mib), ("cpu", cpu_mhz)] {
                    if !(value.is_finite() && value >= 0.0) {
                        return Err(format!(
                            "suspend.reserve.{field} must be zero or more, got {value}"
                        ));
                    }
                }
                Ok(SuspendReserve::Amount {
                    memory_mib,
                    cpu_mhz,
                })
            }
        }
    }
}

/// What a resume does when it cannot restore its checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnRestoreFailure {
    /// Boot fresh on the retained storage, emit `instance.restore_failed`,
    /// and keep the instance usable. The default.
    #[default]
    Cold,
    /// Leave the instance `suspended`, with the error in its `detail`.
    Fail,
}

impl OnRestoreFailure {
    pub fn as_str(&self) -> &'static str {
        match self {
            OnRestoreFailure::Cold => "cold",
            OnRestoreFailure::Fail => "fail",
        }
    }
}

/// `suspend.archive`: what a suspended instance keeps in the manager's
/// store once its checkpoint is published (docs/suspend-and-resume.md,
/// "Archived instances").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveMode {
    /// Keep the instance row, which still reserves a named id, and drop the
    /// snapshot row: the checkpoint's manifest lives in the `contentStore`.
    Slim,
    /// Keep nothing for an unnamed instance: its id is unique by 130
    /// random bits, so the manifest in the `contentStore` is the whole
    /// record. A named instance, or one that owns a volume, is archived
    /// `Slim` instead.
    ManifestOnly,
}

impl ArchiveMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArchiveMode::Slim => "slim",
            ArchiveMode::ManifestOnly => "manifestOnly",
        }
    }
}

/// The task-level `suspend` block, as written.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SuspendConfig {
    /// `memory` or `disk`. Unset: `memory` for a `sandbox` task, `disk` for
    /// every other runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<String>,
    /// How much of the instance's reservation stays charged to its node
    /// while it is suspended: `false` (default) or `true`, a ratio in
    /// `0..=1`, or fixed amounts `{ memory = "1GiB", cpu = 100 }`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserve: Option<SuspendReserveConfig>,
    /// `cold` (default) or `fail`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_restore_failure: Option<String>,
    /// How long a resume waits to be placed on its checkpoint's node before
    /// `onRestoreFailure = "cold"` boots it fresh instead. Default `"30s"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_timeout: Option<String>,
    /// argv the guest runs after every restore, once its identity is
    /// refreshed. Sandbox memory checkpoints only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_resume: Option<Vec<String>>,
    /// `local` (default): the checkpoint stays on the node that took it.
    /// `store`: the worker also publishes it to its `contentStore`, so it
    /// resumes on any compatible node and survives the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<String>,
    /// Archive the instance once its checkpoint is published: `slim` keeps
    /// only the instance row and moves the checkpoint's manifest to the
    /// `contentStore`; `manifestOnly` keeps no row at all for an unnamed
    /// instance. Unset: a suspended instance keeps its snapshot row.
    /// Needs `durability = "store"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<String>,
    /// Automatic suspend when quiet. Unset: never suspended for idleness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle: Option<IdleConfig>,
    /// Let the node's memory pressure response suspend this instance (the
    /// `suspend` step) instead of evicting it. Default false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_pressure: Option<bool>,
}

/// [`SuspendConfig`] with defaults applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSuspend {
    /// The declared checkpoint kind, `None` when the spec leaves it to the
    /// runtime.
    pub checkpoint: Option<SnapshotKind>,
    pub reserve: SuspendReserve,
    pub on_restore_failure: OnRestoreFailure,
    pub resume_timeout: Duration,
    pub on_resume: Vec<String>,
    /// The idle policy, when the task declares `suspend.idle`.
    pub idle: Option<ResolvedIdle>,
    /// `suspend.onPressure`.
    pub on_pressure: bool,
    pub durability: SnapshotDurability,
    /// `suspend.archive`, when set.
    pub archive: Option<ArchiveMode>,
}

impl Default for ResolvedSuspend {
    fn default() -> Self {
        ResolvedSuspend {
            checkpoint: None,
            reserve: SuspendReserve::NONE,
            on_restore_failure: OnRestoreFailure::Cold,
            resume_timeout: DEFAULT_RESUME_TIMEOUT,
            on_resume: Vec::new(),
            idle: None,
            on_pressure: false,
            durability: SnapshotDurability::Local,
            archive: None,
        }
    }
}

impl SuspendConfig {
    /// Parse and default every field. Shape errors only: the checks that need
    /// the rest of the task live in [`validate_suspend`].
    pub fn resolve(&self) -> Result<ResolvedSuspend, String> {
        let idle = self.idle.as_ref().map(IdleConfig::resolve).transpose()?;
        let reserve = self
            .reserve
            .as_ref()
            .map(SuspendReserveConfig::resolve)
            .transpose()?
            .unwrap_or(SuspendReserve::NONE);
        let durability = self
            .durability
            .as_deref()
            .unwrap_or_default()
            .parse::<SnapshotDurability>()
            .map_err(|err| format!("suspend.{err}"))?;
        let checkpoint = match self.checkpoint.as_deref().map(str::trim) {
            None | Some("") => None,
            Some("memory") => Some(SnapshotKind::Memory),
            Some("disk") => Some(SnapshotKind::Disk),
            Some(other) => {
                return Err(format!(
                    "suspend.checkpoint {other:?} is not valid (expected \"memory\" or \"disk\")"
                ))
            }
        };
        let on_restore_failure = match self.on_restore_failure.as_deref().map(str::trim) {
            None | Some("") | Some("cold") => OnRestoreFailure::Cold,
            Some("fail") => OnRestoreFailure::Fail,
            Some(other) => {
                return Err(format!(
                    "suspend.onRestoreFailure {other:?} is not valid (expected \"cold\" or \
                     \"fail\")"
                ))
            }
        };
        let resume_timeout = match self.resume_timeout.as_deref().map(str::trim) {
            None | Some("") => DEFAULT_RESUME_TIMEOUT,
            Some(raw) => {
                let timeout = humantime::parse_duration(raw).map_err(|err| {
                    format!("suspend.resumeTimeout {raw:?} is not a duration: {err}")
                })?;
                if timeout < Duration::from_secs(1) {
                    return Err(format!(
                        "suspend.resumeTimeout {raw:?} is shorter than one second; a resume \
                         needs at least a heartbeat to be placed"
                    ));
                }
                timeout
            }
        };
        let archive = match self.archive.as_deref().map(str::trim) {
            None | Some("") => None,
            Some("slim") => Some(ArchiveMode::Slim),
            Some("manifestOnly") => Some(ArchiveMode::ManifestOnly),
            Some(other) => {
                return Err(format!(
                    "suspend.archive {other:?} is not valid (expected \"slim\" or \
                     \"manifestOnly\")"
                ))
            }
        };
        let on_resume = self.on_resume.clone().unwrap_or_default();
        if self.on_resume.is_some() && on_resume.iter().all(|arg| arg.trim().is_empty()) {
            return Err(
                "suspend.onResume is empty; give it a command (an argv list) or remove it"
                    .to_string(),
            );
        }
        Ok(ResolvedSuspend {
            checkpoint,
            reserve,
            on_restore_failure,
            resume_timeout,
            on_resume,
            idle,
            on_pressure: self.on_pressure.unwrap_or(false),
            durability,
            archive,
        })
    }
}

/// The checkpoint kind a task's suspend uses by default: the declared one,
/// else memory for a sandbox and disk for every other runtime.
pub fn default_checkpoint_kind(task: &crate::structs::TaskConfig) -> SnapshotKind {
    task.suspend
        .as_ref()
        .and_then(|suspend| suspend.resolve().ok())
        .and_then(|resolved| resolved.checkpoint)
        .unwrap_or(if task.runtime == "sandbox" {
            SnapshotKind::Memory
        } else {
            SnapshotKind::Disk
        })
}

/// The bytes a memory checkpoint of this task is expected to take, when its
/// `suspend` block checkpoints memory: the guest's memory size
/// (`config.memory_mib`, which submit pins for a sandbox, else
/// `resources.memoryMax`, else `resources.memory`). An upper bound, since a
/// checkpoint of a guest that never touched its memory can be smaller.
/// `None` for every other task, which the snapshot budget never constrains.
pub fn expected_memory_checkpoint_bytes(task: &crate::structs::TaskConfig) -> Option<u64> {
    task.suspend.as_ref()?;
    if default_checkpoint_kind(task) != SnapshotKind::Memory {
        return None;
    }
    let number = |value: Option<&serde_json::Value>| value.and_then(serde_json::Value::as_f64);
    let mib = number(task.config.get("memory_mib"))
        .or_else(|| number(task.resources.get(crate::resources::MEMORY_MAX_KEY)))
        .or_else(|| number(task.resources.get("memory")))
        .unwrap_or(512.0);
    Some((mib.max(0.0) * 1024.0 * 1024.0) as u64)
}

/// The error for a task that sets `suspend` without being a keyed stateful
/// task (plan §6.7).
pub fn not_keyed_error(task_name: &str) -> String {
    format!(
        "task {task_name:?} sets `suspend` but is not a keyed stateful task\nHint: suspend \
         applies to keyed instances (stateful.mode = \"keyed\"); for stateless services, \
         scale with `legion job scale`"
    )
}

/// The error for a memory checkpoint on a runtime that only takes disk ones
/// (plan §6.6). `subject` names what asked: an instance at call time, a task
/// at submit.
pub fn disk_only_runtime_error(subject: &str, runtime: &str, at_submit: bool) -> String {
    let fix = if at_submit {
        "set suspend.checkpoint = \"disk\""
    } else {
        "use --checkpoint disk"
    };
    format!(
        "{subject} cannot take a memory checkpoint: runtime {runtime:?} supports \"disk\" only\n\
         Hint: {fix}, or run this task with runtime = \"sandbox\""
    )
}

/// Submit-time checks for a task's `suspend` block.
pub fn validate_suspend(task: &crate::structs::TaskConfig) -> Result<(), String> {
    let Some(suspend) = task.suspend.as_ref() else {
        return Ok(());
    };
    let keyed = task
        .stateful
        .as_ref()
        .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Keyed);
    if !keyed {
        return Err(not_keyed_error(&task.name));
    }
    let resolved = suspend
        .resolve()
        .map_err(|err| format!("task {:?}: {err}", task.name))?;
    if let SuspendReserve::Amount {
        memory_mib,
        cpu_mhz,
    } = resolved.reserve
    {
        validate_reserve_amount(task, memory_mib, cpu_mhz)?;
    }
    if resolved.durability == SnapshotDurability::Store {
        crate::structs::snapshot::validate_store_durability_storage(task, "suspend.durability")?;
    }
    if let Some(archive) = resolved.archive {
        validate_archive(task, &resolved, archive)?;
    }
    if resolved.checkpoint == Some(SnapshotKind::Memory) && task.runtime != "sandbox" {
        return Err(disk_only_runtime_error(
            &format!("task {:?}", task.name),
            &task.runtime,
            true,
        ));
    }
    if let Some(idle) = resolved.idle.as_ref() {
        validate_sandbox_idle_signals(task, suspend, idle)?;
    }
    if !resolved.on_resume.is_empty()
        && (task.runtime != "sandbox" || resolved.checkpoint == Some(SnapshotKind::Disk))
    {
        return Err(format!(
            "task {:?} sets suspend.onResume, which runs inside a sandbox guest restored from a \
             memory checkpoint, but this task resumes with a cold boot (runtime {:?}, \
             checkpoint {})\nHint: remove onResume and do the work in the task's own startup",
            task.name,
            task.runtime,
            if resolved.checkpoint == Some(SnapshotKind::Disk) {
                "\"disk\""
            } else {
                "disk-only"
            }
        ));
    }
    Ok(())
}

/// `suspend.archive` needs a checkpoint that lives in the `contentStore`
/// (`durability = "store"` and a memory checkpoint, since a disk checkpoint
/// stays in the node's volume), and holds no capacity: an archived instance
/// keeps no allocation row, so `reserve` would have nothing to hold.
fn validate_archive(
    task: &crate::structs::TaskConfig,
    resolved: &ResolvedSuspend,
    archive: ArchiveMode,
) -> Result<(), String> {
    let mode = archive.as_str();
    if resolved.durability != SnapshotDurability::Store {
        return Err(format!(
            "task {:?} sets suspend.archive = {mode:?}, but its checkpoints stay on the node \
             that took them (durability = \"local\")\nHint: set suspend.durability = \"store\" \
             so the checkpoint is published to the worker's contentStore before it is archived",
            task.name
        ));
    }
    if default_checkpoint_kind(task) != SnapshotKind::Memory {
        return Err(format!(
            "task {:?} sets suspend.archive = {mode:?}, but it suspends with a disk checkpoint, \
             which stays in the node's volume and is never published\nHint: archive needs \
             suspend.checkpoint = \"memory\" (runtime = \"sandbox\")",
            task.name
        ));
    }
    if resolved.reserve != SuspendReserve::NONE {
        return Err(format!(
            "task {:?} sets suspend.archive = {mode:?} and suspend.reserve; an archived \
             instance holds no capacity on any node, so there is nothing to reserve\nHint: \
             remove suspend.reserve, or remove archive to keep resumes on held capacity",
            task.name
        ));
    }
    Ok(())
}

/// A fixed `suspend.reserve` amount must not exceed what the task reserves,
/// and it can only hold a dimension the task declares: the manager's
/// `taskResourceDefaults` are not known at submit, so an amount checked
/// against them could pass here and mean something else on the node.
fn validate_reserve_amount(
    task: &crate::structs::TaskConfig,
    memory_mib: f64,
    cpu_mhz: f64,
) -> Result<(), String> {
    let declared = |names: &[&str]| {
        names.iter().find_map(|name| {
            task.resources
                .get(*name)
                .and_then(serde_json::Value::as_f64)
        })
    };
    for (field, held, names, unit) in [
        ("memory", memory_mib, &["memory"][..], "MiB"),
        ("cpu", cpu_mhz, &["cpu", "cpus"][..], "MHz"),
    ] {
        if held <= 0.0 {
            continue;
        }
        match declared(names) {
            Some(reserved) if held > reserved => {
                return Err(format!(
                    "task {:?}: suspend.reserve.{field} ({held:.0} {unit}) exceeds the task's \
                     resources.{field} ({reserved:.0} {unit}). A suspended instance cannot hold \
                     more than it reserves while running\nHint: lower suspend.reserve.{field}, \
                     or use reserve = true to hold the whole reservation",
                    task.name
                ));
            }
            Some(_) => {}
            None if field == "cpu" && task.resources.get("cores").is_some() => {
                return Err(format!(
                    "task {:?} reserves exclusive cores, not cpu MHz, so suspend.reserve.cpu has \
                     nothing to hold\nHint: remove suspend.reserve.cpu (cores are released while \
                     suspended unless reserve = true)",
                    task.name
                ));
            }
            None => {
                return Err(format!(
                    "task {:?} sets suspend.reserve.{field} but not resources.{field}, so the \
                     amount cannot be checked against what the task reserves\nHint: set \
                     resources.{field} explicitly, or use a ratio like reserve = 0.25",
                    task.name
                ));
            }
        }
    }
    Ok(())
}

/// The `hypervisor` a sandbox task's config declares, `"auto"` when unset.
fn sandbox_hypervisor(task: &crate::structs::TaskConfig) -> &str {
    task.config
        .get("hypervisor")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
}

/// Whether a sandbox task always runs as a microVM, which takes no exec
/// sessions: its `hypervisor` names a VMM rather than `auto` or `dev`.
fn sandbox_is_always_microvm(task: &crate::structs::TaskConfig) -> bool {
    matches!(sandbox_hypervisor(task), "firecracker" | "cloud-hypervisor")
}

/// Idle signals a sandbox cannot see, refused at submit rather than
/// accepted and never fired (docs/suspend-and-resume.md, "What each signal
/// sees on a sandbox").
///
/// - `connections` counts connections through the node datapath: ingress
///   it fronts, and egress through a gated allocation's proxy. A sandbox's
///   traffic crosses the datapath only when it is gated, so on `none`,
///   `tap`, or a dev-mode container it would never fire, and a harness
///   waiting on a quiet model call would still be suspended.
/// - `exec`, listed explicitly, on a task pinned to a microVM VMM: a
///   microVM takes no exec sessions. (Listed by default, or on `auto`,
///   it is a warning instead: see [`sandbox_submit_warnings`].)
fn validate_sandbox_idle_signals(
    task: &crate::structs::TaskConfig,
    suspend: &SuspendConfig,
    idle: &ResolvedIdle,
) -> Result<(), String> {
    if task.runtime != "sandbox" {
        return Ok(());
    }
    if idle.signals.contains(&IdleSignal::Connections)
        && !crate::structs::network::task_runs_gated(
            task,
            crate::structs::network::sandbox_gated_by_default(),
        )
    {
        return Err(format!(
            "task {:?}: suspend.idle.signals \"connections\" counts open connections through \
             the node datapath, and this sandbox's traffic never crosses it (it is not \
             network.mode = \"gated\", so it reaches the network on its own NIC or not at all)\n\
             Hint: remove \"connections\", or run the sandbox gated, where a quiet model call \
             through the egress proxy keeps it awake. Otherwise set idle.after longer than the \
             call, or suspend from your platform, which knows when a call is in flight",
            task.name
        ));
    }
    let listed_exec = suspend
        .idle
        .as_ref()
        .and_then(|idle| idle.signals.as_ref())
        .is_some_and(|signals| signals.iter().any(|signal| signal.trim() == "exec"));
    if listed_exec && sandbox_is_always_microvm(task) {
        return Err(format!(
            "task {:?}: suspend.idle.signals lists \"exec\", but this sandbox always runs as a \
             microVM (hypervisor = {:?}), which takes no exec sessions, so the signal would \
             never fire\nHint: remove \"exec\"; \"cpu\" and \"network\" (bytes on the \
             guest's TAP) are what a microVM's idle policy sees",
            task.name,
            sandbox_hypervisor(task)
        ));
    }
    Ok(())
}

/// Advisory lines for a job's sandbox tasks, shown by `job run` and `job
/// validate`: settings that are valid but will not do what the author most
/// likely expects.
pub fn sandbox_submit_warnings(config: &crate::structs::JobConfig) -> Vec<String> {
    let mut warnings = Vec::new();
    for task in &config.tasks {
        if task.runtime != "sandbox" {
            continue;
        }
        // A restart reboots the guest, which throws away the running state a
        // memory checkpoint exists to keep, about every half TTL.
        if crate::structs::snapshot::memory_checkpoint_ask(task).is_some() {
            for identity in &task.identities {
                if identity.change_mode == crate::structs::workload_identity::ChangeMode::Restart {
                    warnings.push(format!(
                        "task {}: identity {:?} sets changeMode = \"restart\", which reboots \
                         this sandbox's guest on every token refresh (about every half TTL) and \
                         loses its running state. Drop changeMode: a memory-checkpointable \
                         sandbox refreshes tokens in place under /legion/identity/ (the env \
                         token's file is /legion/identity/<ENV>). See \
                         docs/workload-identity.md, \"Sandboxes\"",
                        task.name, identity.name
                    ));
                }
            }
        }
        let Some(resolved) = task
            .suspend
            .as_ref()
            .and_then(|suspend| suspend.resolve().ok())
        else {
            continue;
        };
        if let Some(idle) = resolved.idle.as_ref() {
            if idle.signals.contains(&IdleSignal::Exec) && sandbox_hypervisor(task) != "dev" {
                warnings.push(format!(
                    "task {}: suspend.idle counts \"exec\", which never fires while the \
                     sandbox runs as a microVM (firecracker and cloud-hypervisor take no exec \
                     sessions). Only a dev-mode sandbox (macOS Apple container) sees it, so on \
                     Linux nodes \"cpu\" and \"network\" decide. List signals explicitly, \
                     such as [\"network\", \"cpu\"], to drop it",
                    task.name
                ));
            }
        }
    }
    warnings
}

/// Why an instance is (or was last) suspended: SuspendInstance.
pub const SUSPEND_REASON_MANUAL: &str = "manual";
/// The task's idle policy (`suspend.idle`) suspended it.
pub const SUSPEND_REASON_IDLE: &str = "idle";
/// The node's memory pressure response took the `suspend` step
/// (`suspend.onPressure`).
pub const SUSPEND_REASON_PRESSURE: &str = "pressure";
/// The manager suspended it to make room for a resume pinned to its node
/// (manager `suspend.makeRoom`).
pub const SUSPEND_REASON_MAKE_ROOM: &str = "make_room";

/// `instance.pending_op` while a suspend capture is outstanding.
pub const PENDING_SUSPEND: &str = "suspend";
/// `instance.pending_op` while a resume waits for its restore to report.
pub const PENDING_RESUME: &str = "resume";

/// The suspend columns of an `instance` row (plan §7), read apart from the
/// generic instance row so the many instance queries need not change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSuspendState {
    /// The suspend snapshot a resume restores from. Set while `suspended`
    /// and while a resume is in flight.
    pub checkpoint_id: Option<String>,
    /// `manual` (SuspendInstance), `idle` (the idle sweep), `pressure`
    /// (the node's `suspend` pressure step), or `make_room` (a resume
    /// needed its node's capacity). Written when a suspend starts,
    /// so while one is in flight it names that suspend's reason.
    pub suspend_reason: Option<String>,
    /// Unix seconds of the last successful suspend.
    pub suspended_at: Option<i64>,
    /// `suspend` while a capture is outstanding, `resume` while a resume
    /// waits for its restore to report.
    pub pending_op: Option<String>,
    /// The snapshot the instance last resumed or restored from.
    pub restored_from_snapshot_id: Option<String>,
    /// How long the last suspend's capture took.
    pub last_suspend_ms: Option<i64>,
    /// How long the last restore took on the node.
    pub last_restore_ms: Option<i64>,
    /// Unix seconds when the in-flight resume was requested.
    pub resume_requested_at: Option<i64>,
    /// The last suspend or restore failure, shown as the instance `detail`.
    pub lifecycle_error: Option<String>,
    /// Set when the last resume or restore booted cold in place of a
    /// failed restore (`instance.cold_boot`, JSON), until the next resume
    /// or restore starts. Served as `InstanceInfo.cold_boot`.
    pub cold_boot: Option<InstanceColdBoot>,
    /// Set while the instance is archived (`suspend.archive`): the URL of
    /// the archive manifest in the `contentStore`, which holds the
    /// checkpoint a resume restores. The snapshot row is gone until then.
    pub manifest_url: Option<String>,
}

/// A cold boot that replaced a failed restore under `onRestoreFailure =
/// "cold"`: the instance kept its volume but lost its running processes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceColdBoot {
    /// The snapshot the restore was from.
    pub snapshot_id: String,
    /// Why the restore did not happen.
    pub reason: String,
    /// Unix seconds when the manager chose the cold boot.
    pub at: i64,
}

impl InstanceColdBoot {
    /// The JSON the `instance.cold_boot` column holds for a cold boot now.
    pub fn record_now(snapshot_id: &str, reason: &str) -> String {
        serde_json::to_string(&Self {
            snapshot_id: snapshot_id.to_string(),
            reason: reason.to_string(),
            at: chrono::Utc::now().timestamp(),
        })
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(runtime: &str, keyed: bool, suspend: serde_json::Value) -> crate::structs::TaskConfig {
        let mut value = serde_json::json!({
            "name": "workspace",
            "runtime": runtime,
            "config": { "command": ["sleep", "1"] },
            "suspend": suspend,
        });
        if keyed {
            value["stateful"] = serde_json::json!({ "mode": "keyed" });
        }
        serde_json::from_value(value).expect("task parses")
    }

    #[test]
    fn defaults_resolve() {
        let resolved = SuspendConfig::default().resolve().unwrap();
        assert_eq!(resolved, ResolvedSuspend::default());
        assert_eq!(resolved.resume_timeout, Duration::from_secs(30));
        assert_eq!(resolved.on_restore_failure, OnRestoreFailure::Cold);
    }

    #[test]
    fn archive_needs_a_published_memory_checkpoint_and_no_reserve_test() {
        let archived =
            |suspend: serde_json::Value| validate_suspend(&task("sandbox", true, suspend));
        assert!(archived(serde_json::json!({ "durability": "store", "archive": "slim" })).is_ok());
        let resolved = SuspendConfig {
            durability: Some("store".into()),
            archive: Some("manifestOnly".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        assert_eq!(resolved.archive, Some(ArchiveMode::ManifestOnly));

        let err = archived(serde_json::json!({ "archive": "slim" })).unwrap_err();
        assert!(err.contains("durability = \"local\""), "{err}");
        let err = archived(serde_json::json!({
            "durability": "store", "archive": "slim", "checkpoint": "disk",
        }))
        .unwrap_err();
        assert!(err.contains("disk checkpoint"), "{err}");
        let err = archived(serde_json::json!({
            "durability": "store", "archive": "slim", "reserve": 0.5,
        }))
        .unwrap_err();
        assert!(err.contains("nothing to reserve"), "{err}");
        let err =
            archived(serde_json::json!({ "durability": "store", "archive": "cold" })).unwrap_err();
        assert!(
            err.contains("suspend.archive \"cold\" is not valid"),
            "{err}"
        );
    }

    #[test]
    fn suspend_requires_a_keyed_task_with_the_plan_error() {
        let err = validate_suspend(&task("sandbox", false, serde_json::json!({}))).unwrap_err();
        assert!(
            err.starts_with("task \"workspace\" sets `suspend` but is not a keyed stateful task"),
            "{err}"
        );
        assert!(err.contains("stateful.mode = \"keyed\""), "{err}");
        assert!(validate_suspend(&task("sandbox", true, serde_json::json!({}))).is_ok());
    }

    #[test]
    fn later_phase_fields_are_refused_with_a_hint() {
        let field = serde_json::json!({ "idle": { "signals": ["network", "hint"] } });
        let err = validate_suspend(&task("sandbox", true, field)).unwrap_err();
        assert!(err.contains("Phase 8"), "{err}");
        assert!(err.contains("Hint:"), "{err}");
        assert!(validate_suspend(&task(
            "sandbox",
            true,
            serde_json::json!({ "durability": "local" })
        ))
        .is_ok());
    }

    #[test]
    fn idle_and_on_pressure_resolve_with_the_plan_defaults() {
        let resolved = SuspendConfig {
            idle: Some(IdleConfig::default()),
            on_pressure: Some(true),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        let idle = resolved.idle.expect("idle policy");
        assert_eq!(idle.after, Duration::from_secs(180));
        assert_eq!(
            idle.signals,
            vec![IdleSignal::Network, IdleSignal::Cpu, IdleSignal::Exec]
        );
        assert_eq!(idle.cpu_below, 0.02);
        assert!(resolved.on_pressure);
        assert!(SuspendConfig::default().resolve().unwrap().idle.is_none());

        let custom = IdleConfig {
            after: Some("90s".into()),
            signals: Some(vec![
                "connections".into(),
                "network".into(),
                "network".into(),
            ]),
            cpu_below: Some(0.1),
        }
        .resolve()
        .unwrap();
        assert_eq!(custom.after, Duration::from_secs(90));
        assert_eq!(
            custom.signals,
            vec![IdleSignal::Network, IdleSignal::Connections]
        );
    }

    #[test]
    fn bad_idle_values_name_the_field_and_the_fix() {
        for (idle, needle) in [
            (serde_json::json!({ "after": "5s" }), "30s"),
            (serde_json::json!({ "after": "soon" }), "suspend.idle.after"),
            (serde_json::json!({ "signals": [] }), "at least one signal"),
            (
                serde_json::json!({ "signals": ["disk"] }),
                "suspend.idle.signals",
            ),
            (serde_json::json!({ "cpuBelow": 0 }), "cpuBelow"),
            (serde_json::json!({ "cpuBelow": 1.5 }), "cpuBelow"),
        ] {
            let err = validate_suspend(&task("sandbox", true, serde_json::json!({ "idle": idle })))
                .unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
        let unknown: Result<SuspendConfig, _> =
            serde_json::from_value(serde_json::json!({ "idle": { "afterr": "3m" } }));
        assert!(unknown.is_err(), "a misspelled idle field is refused");
    }

    #[test]
    fn store_durability_resolves_and_refuses_a_node_local_volume() {
        let resolved = SuspendConfig {
            durability: Some("store".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        assert_eq!(resolved.durability, SnapshotDurability::Store);
        assert!(validate_suspend(&task(
            "sandbox",
            true,
            serde_json::json!({ "durability": "store" })
        ))
        .is_ok());

        let mut local = task("exec", true, serde_json::json!({ "durability": "store" }));
        local.stateful = Some(
            serde_json::from_value(serde_json::json!({
                "mode": "keyed",
                "storage": { "path": "/data" },
            }))
            .unwrap(),
        );
        let err = validate_suspend(&local).unwrap_err();
        assert!(err.contains("`local-dir`"), "{err}");
        assert!(err.contains("Hint:") && err.contains("nfs"), "{err}");

        let mut network = local.clone();
        network.stateful = Some(
            serde_json::from_value(serde_json::json!({
                "mode": "keyed",
                "storage": { "path": "/data", "driver": "nfs" },
            }))
            .unwrap(),
        );
        assert!(validate_suspend(&network).is_ok());

        let err = SuspendConfig {
            durability: Some("s3".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(err.contains("suspend.durability"), "{err}");
    }

    fn with_config(
        mut task: crate::structs::TaskConfig,
        config: serde_json::Value,
    ) -> crate::structs::TaskConfig {
        task.config = config;
        task
    }

    /// `connections` counts datapath connections, which a sandbox only has
    /// when it is gated. Anywhere else it is refused, with what to do
    /// instead.
    #[test]
    fn connections_on_a_sandbox_off_the_datapath_is_refused() {
        let idle = serde_json::json!({
            "checkpoint": "disk",
            "idle": { "signals": ["cpu", "connections"] },
        });
        let err = validate_suspend(&task("sandbox", true, idle.clone())).unwrap_err();
        assert!(
            err.contains("\"connections\" counts open connections through the node datapath"),
            "{err}"
        );
        assert!(err.contains("Hint: remove \"connections\""), "{err}");

        let mut gated = task("sandbox", true, idle.clone());
        gated.network =
            Some(serde_json::from_value(serde_json::json!({ "mode": "gated" })).unwrap());
        assert!(
            validate_suspend(&gated).is_ok(),
            "a gated sandbox counts connections"
        );
        assert!(validate_suspend(&task("exec", true, idle)).is_ok());
    }

    /// `exec` never fires on a microVM. Listed on a task pinned to one, it
    /// is refused; listed by default, or where the backend may be dev mode,
    /// it is a submit warning.
    #[test]
    fn exec_on_a_microvm_sandbox_is_refused_or_warned() {
        let listed = serde_json::json!({ "idle": { "signals": ["cpu", "exec"] } });
        let pinned = with_config(
            task("sandbox", true, listed.clone()),
            serde_json::json!({ "command": ["x"], "hypervisor": "firecracker" }),
        );
        let err = validate_suspend(&pinned).unwrap_err();
        assert!(err.contains("takes no exec sessions"), "{err}");

        let job = |tasks: Vec<crate::structs::TaskConfig>| -> crate::structs::JobConfig {
            let mut job: crate::structs::JobConfig =
                serde_json::from_value(serde_json::json!({ "name": "agents", "tasks": [] }))
                    .unwrap();
            job.tasks = tasks;
            job
        };
        // Auto: may be dev mode, so a warning, never an error.
        let auto = task("sandbox", true, listed);
        assert!(validate_suspend(&auto).is_ok());
        let defaults = task("sandbox", true, serde_json::json!({ "idle": {} }));
        let warnings = sandbox_submit_warnings(&job(vec![auto, defaults]));
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("never fires while the sandbox runs as a microVM"));

        // Explicitly without exec, or in dev mode: quiet.
        let quiet = task(
            "sandbox",
            true,
            serde_json::json!({ "idle": { "signals": ["network", "cpu"] } }),
        );
        let dev = with_config(
            task("sandbox", true, serde_json::json!({ "idle": {} })),
            serde_json::json!({ "command": ["x"], "hypervisor": "dev" }),
        );
        assert!(sandbox_submit_warnings(&job(vec![quiet, dev])).is_empty());
    }

    /// `changeMode = "restart"` on a memory-checkpointable sandbox reboots
    /// the guest every refresh: valid, and warned.
    #[test]
    fn a_restarting_identity_on_a_checkpointable_sandbox_warns() {
        let mut restarting = task(
            "sandbox",
            true,
            serde_json::json!({ "idle": { "signals": ["network", "cpu"] } }),
        );
        restarting.identities = vec![serde_json::from_value(serde_json::json!({
            "name": "vault",
            "audience": ["vault"],
            "env": "VAULT_TOKEN",
            "changeMode": "restart",
        }))
        .unwrap()];
        let mut job: crate::structs::JobConfig =
            serde_json::from_value(serde_json::json!({ "name": "agents", "tasks": [] })).unwrap();
        job.tasks = vec![restarting.clone()];
        let warnings = sandbox_submit_warnings(&job);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("reboots this sandbox's guest"));

        restarting.identities[0].change_mode = crate::structs::workload_identity::ChangeMode::Noop;
        job.tasks = vec![restarting];
        assert!(sandbox_submit_warnings(&job).is_empty());
    }

    #[test]
    fn memory_on_a_disk_only_runtime_is_refused() {
        let err = validate_suspend(&task(
            "container",
            true,
            serde_json::json!({ "checkpoint": "memory" }),
        ))
        .unwrap_err();
        assert!(
            err.contains("supports \"disk\" only") && err.contains("runtime = \"sandbox\""),
            "{err}"
        );
        assert!(validate_suspend(&task(
            "exec",
            true,
            serde_json::json!({ "checkpoint": "disk" })
        ))
        .is_ok());
    }

    #[test]
    fn on_resume_needs_a_memory_restore() {
        let err = validate_suspend(&task(
            "exec",
            true,
            serde_json::json!({ "onResume": ["true"] }),
        ))
        .unwrap_err();
        assert!(err.contains("onResume"), "{err}");
        assert!(validate_suspend(&task(
            "sandbox",
            true,
            serde_json::json!({ "onResume": ["/bin/reconnect"] })
        ))
        .is_ok());
    }

    #[test]
    fn bad_values_name_the_field() {
        for (value, needle) in [
            (
                serde_json::json!({ "checkpoint": "ram" }),
                "suspend.checkpoint",
            ),
            (
                serde_json::json!({ "onRestoreFailure": "retry" }),
                "suspend.onRestoreFailure",
            ),
            (
                serde_json::json!({ "resumeTimeout": "soon" }),
                "suspend.resumeTimeout",
            ),
            (serde_json::json!({ "resumeTimeout": "10ms" }), "one second"),
        ] {
            let err = validate_suspend(&task("sandbox", true, value)).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }

    fn with_resources(
        mut task: crate::structs::TaskConfig,
        resources: serde_json::Value,
    ) -> crate::structs::TaskConfig {
        task.resources = resources;
        task
    }

    #[test]
    fn reserve_accepts_a_flag_a_ratio_or_an_amount() {
        let resolve = |value: serde_json::Value| {
            serde_json::from_value::<SuspendConfig>(serde_json::json!({ "reserve": value }))
                .map_err(|err| err.to_string())
                .and_then(|config| config.resolve())
                .map(|resolved| resolved.reserve)
        };
        assert_eq!(resolve(serde_json::json!(true)), Ok(SuspendReserve::FULL));
        assert_eq!(resolve(serde_json::json!(false)), Ok(SuspendReserve::NONE));
        assert_eq!(resolve(serde_json::json!(1)), Ok(SuspendReserve::FULL));
        assert_eq!(
            resolve(serde_json::json!(0.25)),
            Ok(SuspendReserve::Ratio(0.25))
        );
        assert_eq!(
            resolve(serde_json::json!({ "memory": "1GiB", "cpu": 100 })),
            Ok(SuspendReserve::Amount {
                memory_mib: 1024.0,
                cpu_mhz: 100.0
            })
        );
        assert_eq!(
            resolve(serde_json::json!({ "memory": 512 })),
            Ok(SuspendReserve::Amount {
                memory_mib: 512.0,
                cpu_mhz: 0.0
            })
        );
        assert!(SuspendReserve::FULL.is_full() && !SuspendReserve::Ratio(0.99).is_full());
        assert_eq!(
            SuspendConfig::default().resolve().unwrap().reserve,
            SuspendReserve::NONE
        );

        // Round-trips through the stored task config in every form.
        for value in [
            serde_json::json!(true),
            serde_json::json!(0.5),
            serde_json::json!({ "memory": "1GiB", "cpu": 100.0 }),
        ] {
            let config: SuspendConfig =
                serde_json::from_value(serde_json::json!({ "reserve": value.clone() })).unwrap();
            assert_eq!(serde_json::to_value(&config).unwrap()["reserve"], value);
        }
    }

    #[test]
    fn a_bad_reserve_fails_loudly_with_the_field_and_a_hint() {
        for (value, needle) in [
            (serde_json::json!(1.5), "out of range"),
            (serde_json::json!(-0.1), "out of range"),
            (
                serde_json::json!({ "memory": "lots" }),
                "suspend.reserve.memory",
            ),
            (serde_json::json!({ "cpu": -5 }), "suspend.reserve.cpu"),
        ] {
            let err = validate_suspend(&task(
                "sandbox",
                true,
                serde_json::json!({ "reserve": value }),
            ))
            .unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
        let err = serde_json::from_value::<SuspendConfig>(
            serde_json::json!({ "reserve": { "disk": 10 } }),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`memory` and `cpu` only"), "{err}");
        let err = serde_json::from_value::<SuspendConfig>(serde_json::json!({ "reserve": "half" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("a ratio between 0 and 1"), "{err}");
    }

    #[test]
    fn a_reserve_amount_must_fit_inside_the_tasks_reservation() {
        let amount = serde_json::json!({ "reserve": { "memory": "1GiB", "cpu": 100 } });
        let fits = with_resources(
            task("sandbox", true, amount.clone()),
            serde_json::json!({ "memory": 2048, "cpu": 500 }),
        );
        assert!(validate_suspend(&fits).is_ok());

        let too_much = with_resources(
            task("sandbox", true, amount.clone()),
            serde_json::json!({ "memory": 512, "cpu": 500 }),
        );
        let err = validate_suspend(&too_much).unwrap_err();
        assert!(
            err.contains("exceeds the task's resources.memory") && err.contains("Hint:"),
            "{err}"
        );

        let undeclared = with_resources(
            task("sandbox", true, amount.clone()),
            serde_json::json!({ "memory": 2048 }),
        );
        let err = validate_suspend(&undeclared).unwrap_err();
        assert!(err.contains("not resources.cpu"), "{err}");

        let cores = with_resources(
            task("sandbox", true, amount),
            serde_json::json!({ "memory": 2048, "cores": 2 }),
        );
        let err = validate_suspend(&cores).unwrap_err();
        assert!(err.contains("exclusive cores"), "{err}");
    }

    #[test]
    fn the_expected_checkpoint_is_the_guest_memory_of_a_memory_suspend() {
        let mut sandbox = task("sandbox", true, serde_json::json!({}));
        sandbox.config["memory_mib"] = serde_json::json!(1024);
        assert_eq!(
            expected_memory_checkpoint_bytes(&sandbox),
            Some(1024 * 1024 * 1024)
        );
        let sandbox = with_resources(
            task("sandbox", true, serde_json::json!({})),
            serde_json::json!({ "memory": 256, "memoryMax": 2048 }),
        );
        assert_eq!(
            expected_memory_checkpoint_bytes(&sandbox),
            Some(2048 * 1024 * 1024)
        );
        assert_eq!(
            expected_memory_checkpoint_bytes(&task("exec", true, serde_json::json!({}))),
            None,
            "a disk suspend stores nothing in the snapshots budget"
        );
        let mut no_suspend = task("sandbox", true, serde_json::json!({}));
        no_suspend.suspend = None;
        assert_eq!(expected_memory_checkpoint_bytes(&no_suspend), None);
    }

    #[test]
    fn default_kind_follows_the_runtime() {
        assert_eq!(
            default_checkpoint_kind(&task("sandbox", true, serde_json::json!({}))),
            SnapshotKind::Memory
        );
        assert_eq!(
            default_checkpoint_kind(&task("exec", true, serde_json::json!({}))),
            SnapshotKind::Disk
        );
        assert_eq!(
            default_checkpoint_kind(&task(
                "sandbox",
                true,
                serde_json::json!({ "checkpoint": "disk" })
            )),
            SnapshotKind::Disk
        );
    }
}
