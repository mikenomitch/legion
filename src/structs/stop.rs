//! Why the platform stopped an allocation.
//!
//! Every platform-initiated termination has a reason, and until now that
//! reason existed only in the mind of the caller: allocations are removed by
//! deleting their row, and the worker learns about it as an *absence* in the
//! desired set on its next heartbeat. A drain, a rollout replacement, a
//! `job stop`, and a reschedule all looked identical on the wire.
//!
//! [`StopCause`] is the shared vocabulary that fixes that. The manager
//! records it alongside the removal, serves it to the owning worker in the
//! heartbeat reply, and the worker hands it to the task runtime through
//! [`crate::task_plugins::StopContext`].
//!
//! See `docs/graceful-stop.md` and
//! `internal_docs/plans/preemption-and-eviction-plan.md` § Phase 1.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Why an allocation is being stopped.
///
/// Stable wire vocabulary: the string forms below appear in heartbeat
/// replies, in `allocation.removed` event metadata, and (from Phase 1b) in
/// the on-disk stop notice a task can read. Add variants rather than
/// repurposing existing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StopCause {
    /// The job (or a keyed instance of it) was stopped by a user.
    Stop,
    /// The allocation is being replaced by a newer job config — a rollout
    /// or in-place update.
    Update,
    /// The node is draining and its work is moving elsewhere.
    Drain,
    /// The allocation is being rescheduled (failure recovery, `onLost`
    /// handling, or a scheduler-driven move).
    Reschedule,
    /// The job was destroyed outright.
    Destroy,
    /// The node was removed or declared lost; its allocations were cleared.
    NodeLost,
    /// The task's count was lowered by `legion job scale`, and this
    /// allocation was surplus (docs/autoscaling.md). Distinct from
    /// [`StopCause::Update`] on purpose: a scale-in changes no job config
    /// version and starts no rollout, which is the whole premise of the
    /// count-override surface — reporting it as an update would tell a task
    /// it is being replaced by a new version when it is not.
    ScaleIn,
    /// The allocation was preempted to make room for higher-priority work.
    ///
    /// Reserved for Phase 3 of the preemption plan; nothing emits it yet.
    Preempted,
    /// The node ran short of memory and its memory pressure response stopped
    /// this allocation (`evict-bursting` or `evict-reserved`, see
    /// docs/resource-enforcement.md, "Memory pressure"). The worker acts
    /// first, writing this cause into the task's stop notice itself, and the
    /// manager records it when the heartbeat reports the eviction.
    NodePressure,
    /// The allocation's instance is being rolled back to a snapshot
    /// (`legion snapshot restore`, docs/suspend-and-resume.md). A replacement
    /// allocation restores from the snapshot on the same node.
    Restore,
    /// The allocation's instance was suspended (`legion instance suspend`,
    /// docs/suspend-and-resume.md): the worker captured it and stopped it
    /// first, and the manager removed the row when the capture reported.
    Suspended,
    /// No cause was recorded for this stop.
    ///
    /// This is a legitimate steady-state value, not only a bug: stop causes
    /// are best-effort advisory metadata (see `docs/graceful-stop.md`
    /// § Delivery guarantees), so a worker that reconciles before the notice
    /// reaches it, or one talking to a manager that predates this feature,
    /// stops with `Unknown` rather than blocking.
    #[default]
    Unknown,
}

impl StopCause {
    /// The stable wire/string form.
    pub fn as_str(&self) -> &'static str {
        match self {
            StopCause::Stop => "stop",
            StopCause::Update => "update",
            StopCause::Drain => "drain",
            StopCause::Reschedule => "reschedule",
            StopCause::Destroy => "destroy",
            StopCause::NodeLost => "node_lost",
            StopCause::ScaleIn => "scale_in",
            StopCause::Preempted => "preempted",
            StopCause::NodePressure => "node_pressure",
            StopCause::Restore => "restore",
            StopCause::Suspended => "suspended",
            StopCause::Unknown => "unknown",
        }
    }

    /// Parse a wire/string form. Unrecognized input becomes
    /// [`StopCause::Unknown`] rather than an error: a newer manager may send
    /// a cause an older worker has never heard of, and that must degrade to
    /// "stop it anyway", never to a failed reconcile.
    pub fn from_wire(value: &str) -> Self {
        match value {
            "stop" => StopCause::Stop,
            "update" => StopCause::Update,
            "drain" => StopCause::Drain,
            "reschedule" => StopCause::Reschedule,
            "destroy" => StopCause::Destroy,
            "node_lost" => StopCause::NodeLost,
            "scale_in" => StopCause::ScaleIn,
            "preempted" => StopCause::Preempted,
            "node_pressure" => StopCause::NodePressure,
            "restore" => StopCause::Restore,
            "suspended" => StopCause::Suspended,
            _ => StopCause::Unknown,
        }
    }

    /// Whether this cause carries real information (i.e. is worth recording
    /// or reporting). Used to keep `Unknown` out of event metadata.
    pub fn is_known(&self) -> bool {
        !matches!(self, StopCause::Unknown)
    }
}

impl fmt::Display for StopCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// File name of the stop notice, written into the task directory before the
/// stop signal is delivered. The absolute path is exported to the task at
/// start as `LEGION_STOP_NOTICE_PATH`.
pub const STOP_NOTICE_FILE: &str = "stop-notice.json";

/// Env var naming the stop-notice path, set for every task at start.
pub const STOP_NOTICE_PATH_ENV: &str = "LEGION_STOP_NOTICE_PATH";

/// What a task reads when the platform is shutting it down.
///
/// This is the spot-interruption-notice pattern: the signal is the trigger,
/// the file carries the *why* and the *when*. AWS's two-minute spot notice
/// spawned an entire handler ecosystem precisely because it is uniform and
/// machine-readable; a bare SIGTERM is not something a checkpointing job can
/// branch on.
///
/// Written once, immediately before the signal, and left in place — a task
/// that re-reads it after handling the signal still finds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStopNotice {
    /// Why the platform is stopping this task.
    pub cause: StopCause,
    /// RFC 3339 instant after which the task is SIGKILLed. Computed from the
    /// task's `stop.killAfter` at the moment the notice is written.
    pub deadline: String,
    /// The signal that accompanied this notice (`stop.signal`).
    pub signal: String,
    /// Seconds of grace between the signal and `deadline`, so a task need not
    /// parse and subtract timestamps to find out how long it has.
    pub grace_seconds: u64,
    /// Who displaced this allocation. Present only when `cause` is
    /// [`StopCause::Preempted`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preemptor: Option<StopNoticePreemptor>,
}

/// The job that took this allocation's capacity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StopNoticePreemptor {
    /// Preempting job name.
    pub job: String,
    /// Preempting job's priority band.
    pub band: String,
}

#[cfg(test)]
mod stop_notice_test {
    use super::*;

    #[test]
    fn a_notice_serializes_to_the_documented_shape() -> anyhow::Result<()> {
        let notice = TaskStopNotice {
            cause: StopCause::Preempted,
            deadline: "2026-07-28T21:04:05Z".into(),
            signal: "SIGTERM".into(),
            grace_seconds: 30,
            preemptor: Some(StopNoticePreemptor {
                job: "api".into(),
                band: "high".into(),
            }),
        };
        let json: serde_json::Value = serde_json::from_str(&serde_json::to_string(&notice)?)?;
        assert_eq!(json["cause"], "preempted");
        assert_eq!(json["deadline"], "2026-07-28T21:04:05Z");
        assert_eq!(json["signal"], "SIGTERM");
        assert_eq!(json["graceSeconds"], 30);
        assert_eq!(json["preemptor"]["job"], "api");
        assert_eq!(json["preemptor"]["band"], "high");
        Ok(())
    }

    #[test]
    fn a_non_preemption_notice_omits_the_preemptor() -> anyhow::Result<()> {
        let notice = TaskStopNotice {
            cause: StopCause::Drain,
            deadline: "2026-07-28T21:04:05Z".into(),
            signal: "SIGTERM".into(),
            grace_seconds: 30,
            preemptor: None,
        };
        let encoded = serde_json::to_string(&notice)?;
        assert!(!encoded.contains("preemptor"), "{encoded}");
        Ok(())
    }
}

#[cfg(test)]
mod stop_cause_test {
    use super::*;

    #[test]
    fn wire_forms_round_trip() {
        for cause in [
            StopCause::Stop,
            StopCause::Update,
            StopCause::Drain,
            StopCause::Reschedule,
            StopCause::Destroy,
            StopCause::NodeLost,
            StopCause::ScaleIn,
            StopCause::Preempted,
            StopCause::NodePressure,
            StopCause::Unknown,
        ] {
            assert_eq!(StopCause::from_wire(cause.as_str()), cause);
        }
    }

    #[test]
    fn unrecognized_wire_form_degrades_to_unknown() {
        // A newer manager sending a cause this build has never heard of must
        // not break the worker's reconcile.
        assert_eq!(StopCause::from_wire("teleported"), StopCause::Unknown);
        assert_eq!(StopCause::from_wire(""), StopCause::Unknown);
    }

    #[test]
    fn unknown_is_not_reported_as_known() {
        assert!(!StopCause::Unknown.is_known());
        assert!(StopCause::Drain.is_known());
    }

    #[test]
    fn serde_uses_the_wire_spelling() -> anyhow::Result<()> {
        let encoded = serde_json::to_string(&StopCause::NodeLost)?;
        assert_eq!(encoded, "\"node_lost\"");
        Ok(())
    }
}
