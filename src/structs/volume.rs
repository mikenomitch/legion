//! The manager's record of a volume — what a `storage_handle` points at.
//!
//! An [`Instance`](crate::structs::Instance) owns an opaque
//! `{plugin}/{volume_id}` handle; the `volume` row is the other half of that
//! pair: which plugin, which node it is pinned to, what the driver recorded
//! about it, and where it is in the destroy state machine.
//!
//! Volumes deliberately **outlive** their instances: an instance can be
//! destroyed while its bytes still need reclaiming, so the row sticks around in
//! `destroying` until a worker confirms the driver deleted it. See
//! `docs/volumes.md`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Where a volume is in its lifecycle.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VolumeState {
    /// The row and handle exist; no worker has materialized it yet.
    /// Provisioning is lazy — the first `mount` creates the storage — so this
    /// state holds nothing but a name.
    Pending,
    /// A worker mounted it at least once. `node_id` is set (and, for
    /// node-local plugins, is now a placement constraint).
    Created,
    /// Reclaim requested: the owning instance is gone (or an operator asked).
    /// A worker runs the driver's `destroy` and confirms; until then the
    /// reaper keeps re-dispatching, so a crash mid-destroy never leaks.
    Destroying,
    /// Reclaimed. The row is kept as a tombstone so `legion volume list` can
    /// still explain where a job's storage went.
    Destroyed,
}

impl VolumeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            VolumeState::Pending => "pending",
            VolumeState::Created => "created",
            VolumeState::Destroying => "destroying",
            VolumeState::Destroyed => "destroyed",
        }
    }

    /// True for states that still hold (or may hold) real bytes.
    pub fn is_live(&self) -> bool {
        matches!(self, VolumeState::Pending | VolumeState::Created)
    }
}

impl std::fmt::Display for VolumeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for VolumeState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(VolumeState::Pending),
            "created" => Ok(VolumeState::Created),
            "destroying" => Ok(VolumeState::Destroying),
            "destroyed" => Ok(VolumeState::Destroyed),
            other => Err(format!("Unknown volume state {other}")),
        }
    }
}

/// A volume row.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub volume_id: String,
    /// Declared name within the task (`data` for the instance volume).
    pub name: String,
    pub plugin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// The instance whose lifecycle owns this volume. `None` for task
    /// `volumes[]` claims, which are not reclaimed by instance churn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    pub state: VolumeState,
    /// The node the volume lives on. For node-local plugins this is a hard
    /// placement constraint from the moment it is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    pub node_local: bool,
    /// How this volume is handed to its consumer.
    #[serde(default)]
    pub volume_mode: crate::volumes::VolumeMode,
    /// Block mode: the filesystem the device carries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs: Option<String>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    /// Driver-recorded metadata (an NFS source, a dataset name), handed back
    /// to the driver on every later operation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attrs: BTreeMap<String, Value>,
    /// Host path from the most recent successful mount. Diagnostic only — the
    /// driver is always asked again rather than trusting this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_path: Option<String>,
    /// Last driver failure, so a stuck volume explains itself in
    /// `legion volume list` instead of only in worker logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Volume {
    /// The storage handle this volume backs.
    pub fn handle(&self) -> String {
        format!("{}/{}", self.plugin, self.volume_id)
    }

    /// True when this volume constrains where its instance may be placed.
    pub fn pins_placement(&self) -> bool {
        self.node_local && self.node_id.is_some() && self.state.is_live()
    }
}
