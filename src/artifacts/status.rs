//! Per-artifact staging state reporting (artifact system plan §9).
//!
//! Staging threads write progress into an [`ArtifactStatusBoard`]; the worker
//! heartbeat snapshots it into `AllocationHealth.artifact_states` so the
//! manager, CLI, and UI all see the same per-artifact rows.

use crate::prelude::*;
use crate::structs::ArtifactSpec;
use serde::{Deserialize, Serialize};

/// Lifecycle of one artifact within a staging run.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactState {
    #[default]
    Pending,
    Fetching,
    Verifying,
    Staged,
    Failed,
}

/// One artifact's staging state as reported through heartbeats. Sources are
/// redacted and errors name auth *sources* (KV paths, identity names) —
/// never credential values.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Redacted source (no userinfo, no query).
    pub source: String,
    pub state: ArtifactState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_fetched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ArtifactStatus {
    pub fn pending(spec: &ArtifactSpec) -> Self {
        ArtifactStatus {
            name: spec.name.clone(),
            source: spec.redacted_source(),
            state: ArtifactState::Pending,
            ..ArtifactStatus::default()
        }
    }
}

/// Shared board of per-allocation artifact states. Staging threads update it
/// without holding the worker task-manager lock; the heartbeat loop snapshots
/// it when building `AllocationHealth` entries.
#[derive(Clone, Default)]
pub struct ArtifactStatusBoard {
    inner: Arc<Mutex<HashMap<i64, Vec<ArtifactStatus>>>>,
}

impl ArtifactStatusBoard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds pending rows for every declared artifact of an allocation.
    pub fn init(&self, allocation_id: i64, specs: &[ArtifactSpec]) {
        let rows = specs.iter().map(ArtifactStatus::pending).collect();
        self.inner
            .lock()
            .expect("artifact status board")
            .insert(allocation_id, rows);
    }

    /// Mutates one artifact row in place.
    pub fn update(
        &self,
        allocation_id: i64,
        index: usize,
        apply: impl FnOnce(&mut ArtifactStatus),
    ) {
        let mut guard = self.inner.lock().expect("artifact status board");
        if let Some(row) = guard
            .get_mut(&allocation_id)
            .and_then(|rows| rows.get_mut(index))
        {
            apply(row);
        }
    }

    pub fn snapshot(&self, allocation_id: i64) -> Option<Vec<ArtifactStatus>> {
        self.inner
            .lock()
            .expect("artifact status board")
            .get(&allocation_id)
            .cloned()
    }

    /// JSON for `AllocationHealth.artifact_states`. None when the allocation
    /// has no artifact rows (tasks without artifacts stay byte-identical).
    pub fn snapshot_json(&self, allocation_id: i64) -> Option<String> {
        self.snapshot(allocation_id)
            .filter(|rows| !rows.is_empty())
            .and_then(|rows| serde_json::to_string(&rows).ok())
    }

    pub fn remove(&self, allocation_id: i64) {
        self.inner
            .lock()
            .expect("artifact status board")
            .remove(&allocation_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn board_tracks_state_progression() {
        let board = ArtifactStatusBoard::new();
        let specs = vec![
            crate::structs::ArtifactSpec::from_source("https://example.com/a.tar.gz").named("a"),
            crate::structs::ArtifactSpec::from_source("https://user:pw@example.com/b"),
        ];
        board.init(7, &specs);

        let rows = board.snapshot(7).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].state, ArtifactState::Pending);
        assert!(
            !rows[1].source.contains("pw"),
            "board must hold redacted sources: {}",
            rows[1].source
        );

        board.update(7, 0, |row| {
            row.state = ArtifactState::Fetching;
            row.bytes_fetched = Some(10);
            row.bytes_total = Some(100);
        });
        let json = board.snapshot_json(7).expect("json");
        assert!(json.contains("\"state\":\"fetching\""), "{json}");
        assert!(json.contains("\"bytesFetched\":10"), "{json}");

        board.remove(7);
        assert!(board.snapshot_json(7).is_none());
    }
}
