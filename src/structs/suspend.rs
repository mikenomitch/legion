//! The task-level `suspend` jobspec block and the suspend state an instance
//! carries (suspend plan §6.6, §7; docs/suspend-and-resume.md).
//!
//! ```nickel
//! suspend = {
//!     checkpoint = "memory",        # "memory" | "disk"; default: the best the runtime can take
//!     reserve = false,              # hold the reservation while suspended
//!     onRestoreFailure = "cold",    # "cold" | "fail"
//!     resumeTimeout = "30s",        # how long a resume waits for its node before booting cold
//!     onResume = ["/usr/local/bin/reconnect"],  # run in the guest after every restore
//!     durability = "local",         # "local" | "store" (publish to the worker's contentStore)
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

/// Default timeout for `suspend.onResume` inside the guest.
pub const ON_RESUME_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// Any open ingress connection through the datapath, even a quiet one.
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
                 ingress connection counts as activity)"
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

/// The task-level `suspend` block, as written.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SuspendConfig {
    /// `memory` or `disk`. Unset: `memory` for a `sandbox` task, `disk` for
    /// every other runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<String>,
    /// Keep the instance's reservation on its node while it is suspended.
    /// Default false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserve: Option<bool>,
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
    pub reserve: bool,
    pub on_restore_failure: OnRestoreFailure,
    pub resume_timeout: Duration,
    pub on_resume: Vec<String>,
    /// The idle policy, when the task declares `suspend.idle`.
    pub idle: Option<ResolvedIdle>,
    /// `suspend.onPressure`.
    pub on_pressure: bool,
    pub durability: SnapshotDurability,
}

impl Default for ResolvedSuspend {
    fn default() -> Self {
        ResolvedSuspend {
            checkpoint: None,
            reserve: false,
            on_restore_failure: OnRestoreFailure::Cold,
            resume_timeout: DEFAULT_RESUME_TIMEOUT,
            on_resume: Vec::new(),
            idle: None,
            on_pressure: false,
            durability: SnapshotDurability::Local,
        }
    }
}

impl SuspendConfig {
    /// Parse and default every field. Shape errors only: the checks that need
    /// the rest of the task live in [`validate_suspend`].
    pub fn resolve(&self) -> Result<ResolvedSuspend, String> {
        let idle = self.idle.as_ref().map(IdleConfig::resolve).transpose()?;
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
        let on_resume = self.on_resume.clone().unwrap_or_default();
        if self.on_resume.is_some() && on_resume.iter().all(|arg| arg.trim().is_empty()) {
            return Err(
                "suspend.onResume is empty; give it a command (an argv list) or remove it"
                    .to_string(),
            );
        }
        Ok(ResolvedSuspend {
            checkpoint,
            reserve: self.reserve.unwrap_or(false),
            on_restore_failure,
            resume_timeout,
            on_resume,
            idle,
            on_pressure: self.on_pressure.unwrap_or(false),
            durability,
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

/// Submit-time checks for a task's `suspend` block. The gated-network check
/// is shared with snapshots
/// ([`crate::structs::snapshot::validate_memory_checkpoint_network`]).
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
    if resolved.durability == SnapshotDurability::Store {
        crate::structs::snapshot::validate_store_durability_storage(task, "suspend.durability")?;
    }
    if resolved.checkpoint == Some(SnapshotKind::Memory) && task.runtime != "sandbox" {
        return Err(disk_only_runtime_error(
            &format!("task {:?}", task.name),
            &task.runtime,
            true,
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

/// Why an instance is (or was last) suspended: SuspendInstance.
pub const SUSPEND_REASON_MANUAL: &str = "manual";
/// The task's idle policy (`suspend.idle`) suspended it.
pub const SUSPEND_REASON_IDLE: &str = "idle";
/// The node's memory pressure response took the `suspend` step
/// (`suspend.onPressure`).
pub const SUSPEND_REASON_PRESSURE: &str = "pressure";

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
    /// `manual` (SuspendInstance), `idle` (the idle sweep), or `pressure`
    /// (the node's `suspend` pressure step). Written when a suspend starts,
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
