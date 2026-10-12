//! Archived instances (`suspend.archive`; sandbox-scaling plan, Phase 5;
//! docs/suspend-and-resume.md, "Archived instances").
//!
//! Once a suspended keyed instance's checkpoint is published, its worker
//! writes an **archive manifest** beside the published parts, and the manager
//! drops the snapshot row in the same transaction that marks the snapshot
//! published. The manifest is everything a resume needs to put the snapshot
//! row back (and, for `archive = "manifestOnly"`, the instance row too):
//!
//! ```json
//! {
//!   "version": 1,
//!   "envelope": { "instanceId": "workspace-7k2q…", "jobId": "agents", "group": "agents",
//!                 "task": "workspace", "named": false, "snapshot": { "snapshotId": "snap-…", … } },
//!   "manifest": { "id": "snap-…", "memory": [ { "digest": "sha256:…", "url": "s3://…" } ], … }
//! }
//! ```
//!
//! It lives at `<contentStore.url>/manifests/<job>/<group>/<task>/<id>.json`.

use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::structs::snapshot::{SnapshotRecord, SnapshotState};

/// The archive manifest format this build writes and reads.
pub const ARCHIVE_MANIFEST_VERSION: u32 = 1;

/// The directory under a `contentStore` root that holds archive manifests.
pub const ARCHIVE_PREFIX: &str = "manifests";

/// The object name of an instance's archive manifest, relative to its
/// `contentStore` root.
pub fn archive_object_name(job_id: &str, group: &str, task: &str, instance_id: &str) -> String {
    format!("{ARCHIVE_PREFIX}/{job_id}/{group}/{task}/{instance_id}.json")
}

/// The prefix every archive manifest of `job_id` lives under.
pub fn archive_job_prefix(job_id: &str) -> String {
    format!("{ARCHIVE_PREFIX}/{job_id}/")
}

/// `(job, group, task, instance id)` from an archive object name, as
/// [`archive_object_name`] builds it. `None` for anything else.
pub fn parse_archive_object_name(name: &str) -> Option<(String, String, String, String)> {
    let rest = name.strip_prefix(ARCHIVE_PREFIX)?.strip_prefix('/')?;
    let mut parts = rest.split('/');
    let job = parts.next()?;
    let group = parts.next()?;
    let task = parts.next()?;
    let file = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let id = file.strip_suffix(".json")?;
    if [job, group, task, id].iter().any(|part| part.is_empty()) {
        return None;
    }
    Some((job.into(), group.into(), task.into(), id.into()))
}

/// What the manager hands the worker to write beside the published
/// checkpoint (`CheckpointRequest.manifest_envelope`): the instance's
/// identity and the snapshot row, minus the manifest the worker supplies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveEnvelope {
    pub instance_id: String,
    pub job_id: String,
    /// The job's creation time (Unix seconds). A manifest that outlives its
    /// job is never resumed into a later job that reuses the name.
    pub job_created_at: i64,
    pub group: String,
    pub task: String,
    /// Whether the instance id is a name (`Instance::named`).
    pub named: bool,
    /// RFC 3339 creation time of the instance.
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<String>,
    #[serde(default)]
    pub identity_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspend_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspended_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_suspend_ms: Option<i64>,
    /// The suspend checkpoint's row, with an empty `manifestJson`.
    pub snapshot: SnapshotRecord,
}

/// The document a worker writes: the envelope it was given and the
/// published checkpoint manifest it reported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveManifest {
    pub version: u32,
    pub envelope: ArchiveEnvelope,
    /// The published `SnapshotManifest`, each memory part with its `url`.
    pub manifest: serde_json::Value,
}

impl ArchiveManifest {
    /// The worker's half: the document for `envelope_json` (opaque to the
    /// worker) and the published `manifest_json`, serialized.
    pub fn render(envelope_json: &str, manifest_json: &str) -> R<Vec<u8>> {
        let envelope: serde_json::Value =
            serde_json::from_str(envelope_json).context("archive envelope is not JSON")?;
        let manifest: serde_json::Value =
            serde_json::from_str(manifest_json).context("published manifest is not JSON")?;
        Ok(serde_json::to_vec_pretty(&serde_json::json!({
            "version": ARCHIVE_MANIFEST_VERSION,
            "envelope": envelope,
            "manifest": manifest,
        }))?)
    }

    /// Parse a document read back from the `contentStore`.
    pub fn parse(bytes: &[u8]) -> R<Self> {
        let manifest: ArchiveManifest =
            serde_json::from_slice(bytes).context("archive manifest is not valid")?;
        if manifest.version != ARCHIVE_MANIFEST_VERSION {
            bail!(
                "archive manifest version {} is not supported (this build reads version {})",
                manifest.version,
                ARCHIVE_MANIFEST_VERSION
            );
        }
        Ok(manifest)
    }

    /// The snapshot row a resume puts back: `published`, with the
    /// manifest the parts were uploaded under.
    pub fn snapshot_record(&self) -> SnapshotRecord {
        let mut snapshot = self.envelope.snapshot.clone();
        snapshot.state = SnapshotState::Published;
        snapshot.manifest_json = self.manifest.to_string();
        snapshot.error = None;
        if snapshot.published_at.is_none() {
            snapshot.published_at = Some(chrono::Utc::now().timestamp());
        }
        snapshot
    }
}

#[cfg(test)]
mod archive_test {
    use super::*;
    use crate::structs::checkpoint::SnapshotKind;
    use crate::structs::snapshot::{SnapshotDurability, SnapshotPurpose};

    fn envelope() -> ArchiveEnvelope {
        ArchiveEnvelope {
            instance_id: "workspace-abcdefghijklmnopqrstuvwxyz".into(),
            job_id: "agents".into(),
            job_created_at: 1_700_000_000,
            group: "agents".into(),
            task: "workspace".into(),
            named: false,
            created_at: "2026-10-12T00:00:00Z".into(),
            forked_from: None,
            identity_epoch: 2,
            suspend_reason: Some("idle".into()),
            suspended_at: Some(1_700_000_100),
            last_suspend_ms: Some(40),
            snapshot: SnapshotRecord {
                snapshot_id: "snap-0011223344556677".into(),
                job_id: "agents".into(),
                task_name: "workspace".into(),
                instance_id: Some("workspace-abcdefghijklmnopqrstuvwxyz".into()),
                name: None,
                kind: SnapshotKind::Memory,
                purpose: SnapshotPurpose::Suspend,
                state: SnapshotState::Local,
                durability: SnapshotDurability::Store,
                node_id: Some("n1".into()),
                allocation_id: Some(7),
                manifest_json: String::new(),
                total_bytes: 1024,
                duration_ms: 12,
                error: None,
                created_at: 1_700_000_090,
                expires_at: None,
                pause_ms: 3,
                published_at: None,
            },
        }
    }

    #[test]
    fn a_rendered_manifest_round_trips_to_a_published_snapshot_row_test() -> R<()> {
        let envelope = envelope();
        let bytes = ArchiveManifest::render(
            &serde_json::to_string(&envelope)?,
            r#"{"id":"snap-0011223344556677","memory":[]}"#,
        )?;
        let parsed = ArchiveManifest::parse(&bytes)?;
        assert_eq!(parsed.envelope, envelope);
        let snapshot = parsed.snapshot_record();
        assert_eq!(snapshot.state, SnapshotState::Published);
        assert_eq!(snapshot.snapshot_id, "snap-0011223344556677");
        assert!(snapshot.manifest_json.contains("snap-0011223344556677"));
        assert!(snapshot.published_at.is_some());

        let mut newer: serde_json::Value = serde_json::from_slice(&bytes)?;
        newer["version"] = serde_json::json!(2);
        let err = ArchiveManifest::parse(&serde_json::to_vec(&newer)?).unwrap_err();
        assert!(format!("{err:#}").contains("version 2 is not supported"));
        Ok(())
    }

    #[test]
    fn object_names_parse_back_test() {
        let name = archive_object_name("agents", "g", "workspace", "workspace-x");
        assert_eq!(name, "manifests/agents/g/workspace/workspace-x.json");
        assert!(name.starts_with(&archive_job_prefix("agents")));
        assert_eq!(
            parse_archive_object_name(&name),
            Some((
                "agents".into(),
                "g".into(),
                "workspace".into(),
                "workspace-x".into()
            ))
        );
        assert_eq!(parse_archive_object_name("manifests/agents/g/x.json"), None);
        assert_eq!(parse_archive_object_name("objects/agents/g/t/x.json"), None);
    }
}
