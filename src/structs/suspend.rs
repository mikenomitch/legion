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
//!     wake = {                      # `legion start gateway` resumes it on a request
//!         port = "http",            # the network.ports label requests go to
//!         timeout = "30s",          # how long the gateway holds a request for the resume
//!     },
//!     park = {                      # the workload says when it waits ($LEGION_PARK_SOCKET)
//!         after = "10s",            # how long a park waits before it suspends
//!     },
//!     # Session tasks only (docs/session-mode.md, "Idle sessions"):
//!     keepFor = "24h",              # how long a node keeps a suspended session's checkpoint
//!     checkpointEvery = "10m",      # checkpoint running sessions periodically (off by default)
//!     onWorkerStop = "checkpoint",  # "keep" (default) | "checkpoint"
//! },
//! ```
//!
//! The workload's own busy/idle report is `suspend.park` (docs/park.md),
//! not an idle signal: a spec that lists the `hint` signal is refused at
//! submit with a pointer at it.

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

/// Default `suspend.wake.timeout`: as long as `resumeTimeout`, so a resume
/// that falls back to a cold boot still answers the held request.
pub const DEFAULT_WAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default `suspend.keepFor`: how long a node keeps a suspended session's
/// local checkpoint, and then remembers its eviction (session durability
/// plan §6.6, open question "keepFor default").
pub const DEFAULT_KEEP_FOR: Duration = Duration::from_secs(24 * 60 * 60);

/// Shortest `suspend.keepFor` accepted. The node sweeps its idle sessions
/// every few seconds, and a shorter window would read as "suspend means
/// end".
pub const MIN_KEEP_FOR: Duration = Duration::from_secs(60);

/// Shortest `suspend.checkpointEvery` accepted. Each round captures a full
/// memory image of every running session of the task on the node, so a
/// shorter interval would keep a node checkpointing and little else.
pub const MIN_CHECKPOINT_EVERY: Duration = Duration::from_secs(60);

/// `suspend.onWorkerStop`: what a gracefully stopping worker does with the
/// task's running sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnWorkerStop {
    /// Nothing: the processes outlive the worker and the restarted worker
    /// re-adopts them (session durability plan Phase 2).
    #[default]
    Keep,
    /// Checkpoint every running session first, leaving it running, so a
    /// session whose process does not outlive the worker is restored from
    /// that checkpoint on the same node.
    Checkpoint,
}

impl OnWorkerStop {
    pub fn as_str(self) -> &'static str {
        match self {
            OnWorkerStop::Keep => "keep",
            OnWorkerStop::Checkpoint => "checkpoint",
        }
    }
}

/// Default `suspend.park.after`: how long a park waits before it suspends.
/// Long enough that a tool call that returns in a few seconds never pays for
/// a suspend and a resume, short enough that a real wait stops costing at
/// once.
pub const DEFAULT_PARK_AFTER: Duration = Duration::from_secs(10);

/// Longest `after` a park takes, in the jobspec or on the socket. A park
/// means "suspend me soon"; a deadline past a day is not one.
pub const MAX_PARK_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

/// Runtimes Legion can hand a park socket to (docs/park.md, "Transports"):
/// a socket in the task directory for `exec` and `container`, the guest
/// agent's vsock relay for `sandbox`.
pub const PARK_RUNTIMES: [&str; 3] = ["exec", "container", "sandbox"];

/// The `suspend.park` block, as written: the task gets a park socket
/// (`$LEGION_PARK_SOCKET`) on which the workload says when it is waiting
/// (docs/park.md).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParkConfig {
    /// How long a park waits before it suspends, unless the `park` request
    /// names its own `after`. Default `"10s"`; `"0s"` suspends at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

/// [`ParkConfig`] with defaults applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedPark {
    pub after: Duration,
}

impl Default for ResolvedPark {
    fn default() -> Self {
        ResolvedPark {
            after: DEFAULT_PARK_AFTER,
        }
    }
}

impl ParkConfig {
    pub fn resolve(&self) -> Result<ResolvedPark, String> {
        let after = match self.after.as_deref().map(str::trim) {
            None | Some("") => DEFAULT_PARK_AFTER,
            Some(raw) => {
                let after = humantime::parse_duration(raw).map_err(|err| {
                    format!("suspend.park.after {raw:?} is not a duration: {err}")
                })?;
                if after > MAX_PARK_AFTER {
                    return Err(format!(
                        "suspend.park.after {raw:?} is longer than 24h: a park suspends the \
                         instance soon after the workload starts waiting\nHint: use a few \
                         seconds, like \"10s\", or \"0s\" to suspend at once"
                    ));
                }
                after
            }
        };
        Ok(ResolvedPark { after })
    }
}

/// Longest `suspend.wake.timeout` accepted. A gateway holds a connection
/// and a socket per waiting request, and clients rarely wait longer.
pub const MAX_WAKE_TIMEOUT: Duration = Duration::from_secs(300);

/// The `suspend.wake` block, as written: the instance accepts requests
/// through `legion start gateway` while it is suspended, and the gateway
/// resumes it and holds the request until it is up (docs/gateway.md).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WakeConfig {
    /// The `network.ports` label the gateway sends requests to.
    pub port: String,
    /// How long the gateway holds a request while the instance resumes.
    /// Default `"30s"`, at most `"5m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

/// [`WakeConfig`] with defaults applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedWake {
    pub port: String,
    pub timeout: Duration,
}

impl WakeConfig {
    pub fn resolve(&self) -> Result<ResolvedWake, String> {
        let port = self.port.trim();
        if port.is_empty() {
            return Err(
                "suspend.wake.port is empty; name the network.ports label the gateway should \
                 send requests to"
                    .to_string(),
            );
        }
        let timeout = match self.timeout.as_deref().map(str::trim) {
            None | Some("") => DEFAULT_WAKE_TIMEOUT,
            Some(raw) => {
                let timeout = humantime::parse_duration(raw).map_err(|err| {
                    format!("suspend.wake.timeout {raw:?} is not a duration: {err}")
                })?;
                if timeout < Duration::from_secs(1) || timeout > MAX_WAKE_TIMEOUT {
                    return Err(format!(
                        "suspend.wake.timeout {raw:?} is out of range: it must be between 1s \
                         and {}s",
                        MAX_WAKE_TIMEOUT.as_secs()
                    ));
                }
                timeout
            }
        };
        Ok(ResolvedWake {
            port: port.to_string(),
            timeout,
        })
    }
}

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

    /// Parse one `signals` entry. `hint` is refused with a pointer at
    /// `suspend.park`, which is how a workload reports itself.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "network" => Ok(IdleSignal::Network),
            "connections" => Ok(IdleSignal::Connections),
            "cpu" => Ok(IdleSignal::Cpu),
            "exec" => Ok(IdleSignal::Exec),
            "hint" => Err(
                "suspend.idle.signals \"hint\" is not a signal: the workload's own busy/idle \
                 report is the park socket\nHint: remove \"hint\" and add `park = {}` to the \
                 suspend block. The task then gets $LEGION_PARK_SOCKET: `busy` on it keeps the \
                 idle policy off through a quiet call, and `park` suspends the instance when \
                 it starts waiting (docs/park.md)"
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
    /// Wake on request through `legion start gateway`. Unset: a suspended
    /// instance resumes only on `legion instance start`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake: Option<WakeConfig>,
    /// Give the task a park socket (`$LEGION_PARK_SOCKET`) on which the
    /// workload says when it is waiting, and the node suspends it then
    /// (docs/park.md). Unset: no socket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub park: Option<ParkConfig>,
    /// Session tasks only: how long a node keeps a suspended session's
    /// local checkpoint before it evicts it, and then how long it remembers
    /// the eviction so the session answers `evicted` rather than `not
    /// found`. Default `"24h"`, at least `"1m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_for: Option<String>,
    /// Session tasks only: checkpoint each running session this often
    /// without stopping it (a pause for the capture). Unset (default): no
    /// periodic checkpoints. Needs a memory checkpoint; at least `"1m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_every: Option<String>,
    /// Session tasks only: `keep` (default) leaves running sessions alone
    /// when the worker stops gracefully; `checkpoint` checkpoints each one
    /// first (within the worker's `edgeBoot.stopCheckpointTimeout`), so a
    /// session whose process does not survive the stop is restored on the
    /// same node when the worker comes back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_worker_stop: Option<String>,
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
    /// `suspend.wake`, when the task accepts wake on request.
    pub wake: Option<ResolvedWake>,
    /// `suspend.park`, when the task has a park socket.
    pub park: Option<ResolvedPark>,
    /// `suspend.keepFor` (session tasks).
    pub keep_for: Duration,
    /// `suspend.checkpointEvery` (session tasks), when set.
    pub checkpoint_every: Option<Duration>,
    /// `suspend.onWorkerStop` (session tasks).
    pub on_worker_stop: OnWorkerStop,
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
            wake: None,
            park: None,
            keep_for: DEFAULT_KEEP_FOR,
            checkpoint_every: None,
            on_worker_stop: OnWorkerStop::Keep,
        }
    }
}

impl SuspendConfig {
    /// Parse and default every field. Shape errors only: the checks that need
    /// the rest of the task live in [`validate_suspend`].
    pub fn resolve(&self) -> Result<ResolvedSuspend, String> {
        let idle = self.idle.as_ref().map(IdleConfig::resolve).transpose()?;
        let wake = self.wake.as_ref().map(WakeConfig::resolve).transpose()?;
        let park = self.park.as_ref().map(ParkConfig::resolve).transpose()?;
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
        let keep_for = match self.keep_for.as_deref().map(str::trim) {
            None | Some("") => DEFAULT_KEEP_FOR,
            Some(raw) => {
                let keep_for = humantime::parse_duration(raw)
                    .map_err(|err| format!("suspend.keepFor {raw:?} is not a duration: {err}"))?;
                if keep_for < MIN_KEEP_FOR {
                    return Err(format!(
                        "suspend.keepFor {raw:?} is shorter than {}s; a suspended session's \
                         checkpoint must outlive at least one sweep of its node",
                        MIN_KEEP_FOR.as_secs()
                    ));
                }
                keep_for
            }
        };
        let checkpoint_every = match self.checkpoint_every.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(raw) => {
                let every = humantime::parse_duration(raw).map_err(|err| {
                    format!("suspend.checkpointEvery {raw:?} is not a duration: {err}")
                })?;
                if every < MIN_CHECKPOINT_EVERY {
                    return Err(format!(
                        "suspend.checkpointEvery {raw:?} is shorter than {}s: each round \
                         captures a full memory image of every running session\nHint: use \
                         \"10m\" or longer, or remove checkpointEvery",
                        MIN_CHECKPOINT_EVERY.as_secs()
                    ));
                }
                Some(every)
            }
        };
        let on_worker_stop = match self.on_worker_stop.as_deref().map(str::trim) {
            None | Some("") | Some("keep") => OnWorkerStop::Keep,
            Some("checkpoint") => OnWorkerStop::Checkpoint,
            Some(other) => {
                return Err(format!(
                    "suspend.onWorkerStop {other:?} is not valid (expected \"keep\" or \
                     \"checkpoint\")"
                ))
            }
        };
        Ok(ResolvedSuspend {
            keep_for,
            checkpoint_every,
            on_worker_stop,
            checkpoint,
            reserve,
            on_restore_failure,
            resume_timeout,
            on_resume,
            idle,
            on_pressure: self.on_pressure.unwrap_or(false),
            durability,
            archive,
            wake,
            park,
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
    validate_session_fields(task, suspend, &resolved)?;
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
        validate_sandbox_idle_signals(task, idle)?;
    }
    if let Some(wake) = resolved.wake.as_ref() {
        validate_wake_port(task, wake)?;
    }
    if resolved.park.is_some() && !PARK_RUNTIMES.contains(&task.runtime.as_str()) {
        return Err(format!(
            "task {:?} sets suspend.park, but runtime {:?} runs no process Legion can hand a \
             park socket to\nHint: suspend.park works on the {} runtimes",
            task.name,
            task.runtime,
            PARK_RUNTIMES.join(", ")
        ));
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

/// `keepFor`, `checkpointEvery`, and `onWorkerStop` say what a node does
/// with the sessions it holds, so they are refused on any other task. A
/// periodic checkpoint needs a memory checkpoint: a disk checkpoint of a
/// session captures nothing (a session has no volume), so the rounds would
/// cost a pause and keep nothing.
fn validate_session_fields(
    task: &crate::structs::TaskConfig,
    suspend: &SuspendConfig,
    resolved: &ResolvedSuspend,
) -> Result<(), String> {
    let sets_session_fields = suspend.keep_for.is_some()
        || suspend.checkpoint_every.is_some()
        || suspend.on_worker_stop.is_some();
    if sets_session_fields && !crate::structs::session::is_session_task(task) {
        return Err(format!(
            "task {:?} sets suspend.keepFor, suspend.checkpointEvery, or suspend.onWorkerStop, \
             which apply to session tasks only (stateful.mode = \"session\"): they say what a \
             node does with the sessions it holds\nHint: remove them; a keyed instance's \
             checkpoint lives as long as its snapshot",
            task.name
        ));
    }
    if resolved.checkpoint_every.is_some() && default_checkpoint_kind(task) != SnapshotKind::Memory
    {
        return Err(format!(
            "task {:?} sets suspend.checkpointEvery, but it checkpoints to disk (runtime {:?}), \
             and a session's disk checkpoint holds nothing to restore from\nHint: remove \
             checkpointEvery, or run the task with runtime = \"sandbox\" and \
             suspend.checkpoint = \"memory\"",
            task.name, task.runtime
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

/// `suspend.wake.port` must name a port the task declares: it is the port
/// the gateway dials once the instance is up.
fn validate_wake_port(
    task: &crate::structs::TaskConfig,
    wake: &ResolvedWake,
) -> Result<(), String> {
    let labels: Vec<&str> = task
        .network
        .as_ref()
        .map(|network| {
            network
                .ports
                .iter()
                .map(|port| port.name.as_str())
                .collect()
        })
        .unwrap_or_default();
    if labels.contains(&wake.port.as_str()) {
        return Ok(());
    }
    let declared = if labels.is_empty() {
        "it declares no network.ports".to_string()
    } else {
        format!("it declares {}", labels.join(", "))
    };
    Err(format!(
        "task {:?} sets suspend.wake.port = {:?}, but {declared}\nHint: add {{ name = {:?} }} to \
         network.ports, or point wake.port at a declared port",
        task.name, wake.port, wake.port
    ))
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

/// Idle signals a sandbox cannot see, refused at submit rather than
/// accepted and never fired (docs/suspend-and-resume.md, "What each signal
/// sees on a sandbox").
///
/// - `connections` counts connections through the node datapath: ingress
///   it fronts, and egress through a gated allocation's proxy. A sandbox's
///   outbound traffic crosses the datapath only when it is gated, so on
///   `none`, `tap`, or a dev-mode container a harness waiting on a quiet
///   model call would still be suspended. (A TAP guest's advertised ports
///   are fronted by the datapath since gated plan Phase 5, but counting
///   only the inbound half would read as a signal that covers the call.)
fn validate_sandbox_idle_signals(
    task: &crate::structs::TaskConfig,
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
             the node datapath, and this sandbox's outbound traffic never crosses it (it is \
             not network.mode = \"gated\", so it reaches the network on its own NIC or not at \
             all)\n\
             Hint: remove \"connections\", or run the sandbox gated, where a quiet model call \
             through the egress proxy keeps it awake. Otherwise set idle.after longer than the \
             call, or suspend from your platform, which knows when a call is in flight",
            task.name
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
/// The workload parked itself on its park socket (`suspend.park`), and its
/// node suspended it at the park's deadline.
pub const SUSPEND_REASON_PARK: &str = "park";

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
    /// (the node's `suspend` pressure step), `park` (the workload parked
    /// itself), or `make_room` (a resume needed its node's capacity). Written when a suspend starts,
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

    /// `hint` was the suspend plan's name for the workload's own report.
    /// That report is the park socket now, and the refusal says so.
    #[test]
    fn the_hint_signal_is_refused_with_a_pointer_at_park() {
        let field = serde_json::json!({ "idle": { "signals": ["network", "hint"] } });
        let err = validate_suspend(&task("sandbox", true, field)).unwrap_err();
        assert!(err.contains("park = {}"), "{err}");
        assert!(err.contains("Hint:"), "{err}");
        assert!(validate_suspend(&task(
            "sandbox",
            true,
            serde_json::json!({ "durability": "local" })
        ))
        .is_ok());
    }

    #[test]
    fn park_resolves_with_its_default_and_accepts_zero() {
        let resolved = SuspendConfig {
            park: Some(ParkConfig::default()),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        assert_eq!(resolved.park, Some(ResolvedPark::default()));
        assert_eq!(resolved.park.unwrap().after, Duration::from_secs(10));

        let now = ParkConfig {
            after: Some("0s".into()),
        }
        .resolve()
        .unwrap();
        assert_eq!(now.after, Duration::ZERO);

        let err = ParkConfig {
            after: Some("soon".into()),
        }
        .resolve()
        .unwrap_err();
        assert!(
            err.starts_with("suspend.park.after \"soon\" is not a duration"),
            "{err}"
        );
        assert!(SuspendConfig::default().resolve().unwrap().park.is_none());
        let err = ParkConfig {
            after: Some("2d".into()),
        }
        .resolve()
        .unwrap_err();
        assert!(err.contains("longer than 24h"), "{err}");
    }

    /// A park socket needs a process Legion can hand it to. Every runtime
    /// that has one is accepted, and the refusal names them.
    #[test]
    fn park_is_refused_on_a_runtime_with_no_process() {
        for runtime in PARK_RUNTIMES {
            let spec = task(runtime, true, serde_json::json!({ "park": {} }));
            assert_eq!(validate_suspend(&spec), Ok(()), "{runtime}");
        }
        let err =
            validate_suspend(&task("noop", true, serde_json::json!({ "park": {} }))).unwrap_err();
        assert_eq!(
            err,
            "task \"workspace\" sets suspend.park, but runtime \"noop\" runs no process Legion \
             can hand a park socket to\nHint: suspend.park works on the exec, container, sandbox \
             runtimes"
        );
        // A misspelled field is refused when the spec parses, not ignored.
        assert!(
            serde_json::from_value::<ParkConfig>(serde_json::json!({ "afterwards": "1s" }))
                .is_err()
        );
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
    fn wake_resolves_with_defaults_and_needs_a_declared_port_test() {
        let with_port = |wake: serde_json::Value| {
            let mut task = task("sandbox", true, serde_json::json!({ "wake": wake }));
            task.network = Some(crate::structs::network::NetworkConfig {
                ports: vec![crate::structs::network::PortConfig {
                    name: "http".into(),
                    static_port: None,
                    to: None,
                    tls: None,
                }],
                ..Default::default()
            });
            task
        };
        let ok = with_port(serde_json::json!({ "port": "http" }));
        validate_suspend(&ok).expect("a declared port is accepted");
        let wake = ok.suspend.unwrap().resolve().unwrap().wake.expect("wake");
        assert_eq!(wake.port, "http");
        assert_eq!(wake.timeout, DEFAULT_WAKE_TIMEOUT);

        let err = validate_suspend(&with_port(serde_json::json!({ "port": "admin" }))).unwrap_err();
        assert!(err.contains("suspend.wake.port = \"admin\""), "{err}");
        assert!(err.contains("it declares http"), "{err}");
        assert!(err.contains("Hint:"), "{err}");

        let err = validate_suspend(&task(
            "sandbox",
            true,
            serde_json::json!({ "wake": { "port": "http" } }),
        ))
        .unwrap_err();
        assert!(err.contains("declares no network.ports"), "{err}");

        for (timeout, needle) in [
            ("0s", "out of range"),
            ("10m", "out of range"),
            ("soon", "not a duration"),
        ] {
            let err = validate_suspend(&with_port(
                serde_json::json!({ "port": "http", "timeout": timeout }),
            ))
            .unwrap_err();
            assert!(err.contains(needle), "{timeout}: {err}");
        }
        let err = validate_suspend(&with_port(serde_json::json!({ "port": " " }))).unwrap_err();
        assert!(err.contains("suspend.wake.port is empty"), "{err}");
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

    /// A microVM takes exec sessions over vsock (sandbox-exec plan §6.1),
    /// so `exec` is a signal it sees: listed on a task pinned to a VMM it is
    /// accepted, and listed by default it draws no warning.
    #[test]
    fn exec_on_a_microvm_sandbox_is_a_live_signal() {
        let listed = serde_json::json!({ "idle": { "signals": ["cpu", "exec"] } });
        let pinned = with_config(
            task("sandbox", true, listed),
            serde_json::json!({ "command": ["x"], "hypervisor": "firecracker" }),
        );
        assert!(validate_suspend(&pinned).is_ok());
        let mut job: crate::structs::JobConfig =
            serde_json::from_value(serde_json::json!({ "name": "agents", "tasks": [] })).unwrap();
        job.tasks = vec![
            pinned,
            task("sandbox", true, serde_json::json!({ "idle": {} })),
        ];
        assert!(sandbox_submit_warnings(&job).is_empty());
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

    /// A normalized session task (keyed, with an envelope), as job parsing
    /// leaves it.
    fn session_task(runtime: &str, suspend: serde_json::Value) -> crate::structs::TaskConfig {
        serde_json::from_value(serde_json::json!({
            "name": "workspace",
            "runtime": runtime,
            "config": { "command": ["sleep", "1"] },
            "resources": { "cpu": 500, "memory": 512 },
            "stateful": {
                "mode": "keyed",
                "envelope": { "resources": { "cpu": 4000, "memory": 4096 } },
            },
            "suspend": suspend,
        }))
        .expect("session task parses")
    }

    #[test]
    fn session_fields_resolve_with_the_plan_defaults_test() {
        let resolved = SuspendConfig::default().resolve().unwrap();
        assert_eq!(resolved.keep_for, DEFAULT_KEEP_FOR);
        assert_eq!(resolved.keep_for, Duration::from_secs(86_400));
        assert_eq!(resolved.checkpoint_every, None);
        assert_eq!(resolved.on_worker_stop, OnWorkerStop::Keep);

        let resolved = SuspendConfig {
            keep_for: Some("2h".into()),
            checkpoint_every: Some("10m".into()),
            on_worker_stop: Some("checkpoint".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap();
        assert_eq!(resolved.keep_for, Duration::from_secs(7_200));
        assert_eq!(resolved.checkpoint_every, Some(Duration::from_secs(600)));
        assert_eq!(resolved.on_worker_stop, OnWorkerStop::Checkpoint);
    }

    #[test]
    fn bad_session_fields_name_the_field_test() {
        let bad = |config: SuspendConfig| config.resolve().unwrap_err();
        let err = bad(SuspendConfig {
            keep_for: Some("soon".into()),
            ..Default::default()
        });
        assert!(err.contains("suspend.keepFor \"soon\""), "{err}");
        let err = bad(SuspendConfig {
            keep_for: Some("10s".into()),
            ..Default::default()
        });
        assert!(err.contains("shorter than 60s"), "{err}");
        let err = bad(SuspendConfig {
            checkpoint_every: Some("5s".into()),
            ..Default::default()
        });
        assert!(err.contains("suspend.checkpointEvery"), "{err}");
        assert!(err.contains("Hint:"), "{err}");
        let err = bad(SuspendConfig {
            on_worker_stop: Some("drain".into()),
            ..Default::default()
        });
        assert!(err.contains("suspend.onWorkerStop \"drain\""), "{err}");
    }

    #[test]
    fn session_fields_are_refused_on_a_keyed_task_test() {
        for field in [
            serde_json::json!({ "keepFor": "1h" }),
            serde_json::json!({ "checkpointEvery": "10m" }),
            serde_json::json!({ "onWorkerStop": "keep" }),
        ] {
            let err = validate_suspend(&task("sandbox", true, field.clone())).unwrap_err();
            assert!(err.contains("session tasks only"), "{field}: {err}");
            assert!(
                validate_suspend(&session_task("sandbox", field.clone())).is_ok(),
                "{field} is valid on a session task"
            );
        }
    }

    #[test]
    fn periodic_session_checkpoints_need_a_memory_checkpoint_test() {
        let err = validate_suspend(&session_task(
            "exec",
            serde_json::json!({ "checkpointEvery": "10m" }),
        ))
        .unwrap_err();
        assert!(err.contains("checkpoints to disk"), "{err}");
        // Checkpoint on stop is accepted on any runtime: an exec session's
        // checkpoint is its spec, and the restart boots it fresh under the
        // same id.
        assert!(validate_suspend(&session_task(
            "exec",
            serde_json::json!({ "onWorkerStop": "checkpoint", "keepFor": "2h" })
        ))
        .is_ok());
        assert!(validate_suspend(&session_task(
            "sandbox",
            serde_json::json!({ "checkpointEvery": "10m", "durability": "local" })
        ))
        .is_ok());
    }
}
