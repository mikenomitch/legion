//! Stable instance identity for stateful workloads.
//!
//! See `internal_docs/plans/stateful-workloads-and-instance-identity-plan.md` and
//! `docs/stateful-workloads.md`. An [`Instance`] is a logical entity one level
//! above an allocation: it owns a stable id that survives reschedules, binds to
//! persistent state via an opaque `storage_handle`, and feeds the
//! reschedule-stable workload-identity `sub` (see
//! `crate::structs::workload_identity`).
//!
//! Allocations are ephemeral placements *of* an instance: an instance has at
//! most one live allocation at a time but many over its life.

use serde::{Deserialize, Serialize};

/// How instance ids are assigned for a stateful task.
///
/// The recycle policy is encoded in the mode on purpose: slot ids are reused
/// within a fixed pool (replica 3 coming back must be replica 3), while keyed
/// ids are never reused so a new sandbox can never inherit a dead one's
/// identity or state.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceMode {
    /// Fixed pool of ordinal slots `0..count.num-1` (StatefulSet style). Slot
    /// ids are deterministic and reused across reschedules and scale cycles.
    Slot,
    /// Instances created on demand via the CreateInstance/DestroyInstance
    /// management RPCs with never-recycled ids (sandbox / dispatch style).
    /// `count.num` does not drive keyed tasks: live instances do.
    Keyed,
}

impl InstanceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceMode::Slot => "slot",
            InstanceMode::Keyed => "keyed",
        }
    }
}

impl std::fmt::Display for InstanceMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for InstanceMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "slot" => Ok(InstanceMode::Slot),
            "keyed" => Ok(InstanceMode::Keyed),
            other => Err(format!("Unknown instance mode {other}")),
        }
    }
}

/// Lifecycle state of an instance.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    /// Created but never placed (no allocation yet).
    Pending,
    /// Has a live allocation fulfilling it.
    Active,
    /// Lost its allocation and is awaiting a replacement that will reuse the
    /// same instance id and storage handle.
    Rescheduling,
    /// Keyed only: paused via StopInstance (or the whole job being stopped).
    /// The allocation is stopped but the row AND storage handle are retained,
    /// and the id stays owned (creating it again still collides). Not desired
    /// by the scheduler; StartInstance moves it back to `Pending`. Unlike
    /// `Terminated` this is fully reversible.
    Stopped,
    /// No longer desired (scaled in, job stopped, or destroyed). Slot-mode
    /// rows are retained so a later scale-out re-materializes the same id and
    /// reattaches the retained storage handle; keyed-mode rows are retained
    /// forever to guarantee ids are never recycled.
    Terminated,
}

impl InstanceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceState::Pending => "pending",
            InstanceState::Active => "active",
            InstanceState::Rescheduling => "rescheduling",
            InstanceState::Stopped => "stopped",
            InstanceState::Terminated => "terminated",
        }
    }
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for InstanceState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(InstanceState::Pending),
            "active" => Ok(InstanceState::Active),
            "rescheduling" => Ok(InstanceState::Rescheduling),
            "stopped" => Ok(InstanceState::Stopped),
            "terminated" => Ok(InstanceState::Terminated),
            other => Err(format!("Unknown instance state {other}")),
        }
    }
}

/// A stable instance of a stateful task. Persisted in the `instance` table;
/// allocations reference it via `Allocation::instance_id`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    /// Stable, unique within (job, group); survives reschedules. In slot mode
    /// this is the deterministic `{job_id}-{group}-{slot}` id (see
    /// [`slot_instance_id`]); in keyed mode it is a caller-supplied key or a
    /// server-minted id.
    pub instance_id: String,
    pub job_id: String,
    pub group: String,
    /// The task this instance currently belongs to (informational; identity is
    /// keyed by `instance_id`).
    pub task_name: String,
    pub mode: InstanceMode,
    pub state: InstanceState,
    /// Ordinal slot `0..count.num-1`; `Some` only in slot mode.
    pub slot_index: Option<u32>,
    /// The allocation currently fulfilling this instance, if any.
    pub current_alloc_id: Option<i64>,
    /// Opaque reference to bound persistent state. Minted on first placement
    /// when the task declares `stateful.storage` and reattached on every
    /// subsequent allocation. Concrete drivers are a follow-up (see
    /// `internal_docs/plans/volume-plugin-system-plan.md`).
    pub storage_handle: Option<String>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 timestamp of the last state/binding change.
    pub updated_at: String,
}

/// Deterministic slot-mode instance id: `{job_id}-{group}-{slot}`.
///
/// Recomputable and human-legible so replica N's identity (and its storage
/// binding and workload-identity `sub`) is stable across reschedules, config
/// updates, and scale cycles.
pub fn slot_instance_id(job_id: &str, group: &str, slot: u32) -> String {
    format!("{job_id}-{group}-{slot}")
}

/// Maximum length of a keyed instance id.
pub const KEYED_INSTANCE_ID_MAX_LEN: usize = 128;

/// Validate a keyed instance id: `[A-Za-z0-9._-]{1,128}`.
///
/// The charset is deliberately conservative — keyed ids flow into env vars
/// (`LEGION_INSTANCE_ID`), workload-identity subjects, file-ish storage
/// handles, and CLI arguments, so anything beyond URL-safe punctuation is a
/// foot-gun. Enforced for caller-supplied ids on CreateInstance; server-minted
/// ids satisfy it by construction.
pub fn validate_keyed_instance_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("keyed instance id cannot be empty".to_string());
    }
    if id.len() > KEYED_INSTANCE_ID_MAX_LEN {
        return Err(format!(
            "keyed instance id is {} characters; max is {KEYED_INSTANCE_ID_MAX_LEN}",
            id.len()
        ));
    }
    if let Some(bad) = id
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(format!(
            "keyed instance id contains invalid character {bad:?}; allowed: [A-Za-z0-9._-]"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::R;

    #[test]
    fn slot_instance_id_is_deterministic_and_stable() {
        let first = slot_instance_id("db", "default", 2);
        let second = slot_instance_id("db", "default", 2);
        assert_eq!(first, "db-default-2");
        assert_eq!(first, second);
        assert_ne!(first, slot_instance_id("db", "default", 3));
        assert_ne!(first, slot_instance_id("db", "replicas", 2));
        assert_ne!(first, slot_instance_id("cache", "default", 2));
    }

    #[test]
    fn keyed_instance_id_validation_enforces_charset_and_length() {
        assert!(validate_keyed_instance_id("sandbox-user.42_A").is_ok());
        assert!(validate_keyed_instance_id(&"a".repeat(KEYED_INSTANCE_ID_MAX_LEN)).is_ok());

        assert!(validate_keyed_instance_id("").is_err());
        assert!(validate_keyed_instance_id(&"a".repeat(KEYED_INSTANCE_ID_MAX_LEN + 1)).is_err());
        for bad in ["has space", "slash/y", "emoji-✨", "colon:id", "at@id"] {
            assert!(
                validate_keyed_instance_id(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn instance_mode_round_trips_serde_and_str() -> R<()> {
        assert_eq!(serde_json::to_string(&InstanceMode::Slot)?, "\"slot\"");
        assert_eq!(serde_json::to_string(&InstanceMode::Keyed)?, "\"keyed\"");
        assert_eq!(
            serde_json::from_str::<InstanceMode>("\"slot\"")?,
            InstanceMode::Slot
        );
        assert_eq!("keyed".parse::<InstanceMode>(), Ok(InstanceMode::Keyed));
        assert_eq!(InstanceMode::Slot.to_string(), "slot");
        assert!("ordinal".parse::<InstanceMode>().is_err());
        Ok(())
    }

    #[test]
    fn instance_state_round_trips_serde_and_str() -> R<()> {
        for state in [
            InstanceState::Pending,
            InstanceState::Active,
            InstanceState::Rescheduling,
            InstanceState::Stopped,
            InstanceState::Terminated,
        ] {
            let json = serde_json::to_string(&state)?;
            assert_eq!(json, format!("\"{state}\""));
            assert_eq!(serde_json::from_str::<InstanceState>(&json)?, state);
            assert_eq!(state.as_str().parse::<InstanceState>(), Ok(state));
        }
        assert!("gone".parse::<InstanceState>().is_err());
        Ok(())
    }
}
