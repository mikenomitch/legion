//! Fleet usage and node content reads for the suspend/resume UI (plan §7.3):
//! ListNodeUsage, the batch form of GetNodeUsage, and GetNodeContent, the
//! read side of the content location index. Both are read-only views over
//! soft state the manager already holds (the usage table and the location
//! index) plus the snapshot table, so neither writes anything.

use super::snapshot_rpcs::{snapshot_info, CHECKPOINT_CAPABILITY, CONTENT_PUBLISH_CAPABILITY};
use super::*;
use crate::structs::snapshot::{SnapshotRecord, SnapshotState};
use crate::structs::{
    GetNodeContentReply, GetNodeContentRequest, ListNodeUsageReply, ListNodeUsageRequest,
    NodeContentEntry, NodeSnapshotContent, SnapshotHolder,
};

/// Most nodes one ListNodeUsage call reports on.
pub(super) const LIST_NODE_USAGE_MAX: usize = 500;

/// The digests of a snapshot's memory parts, with their roles, from its
/// manifest. Empty for a disk snapshot, one still capturing, or a manifest
/// this binary cannot read.
fn memory_parts(snapshot: &SnapshotRecord) -> Vec<(String, String, u64)> {
    snapshot
        .manifest()
        .ok()
        .flatten()
        .map(|manifest| {
            manifest
                .memory
                .into_iter()
                .map(|part| (part.digest, part.role, part.size))
                .collect()
        })
        .unwrap_or_default()
}

/// Which nodes hold a snapshot's memory parts (`SnapshotInfo.holders`),
/// most parts first, then by node id. Empty for a disk snapshot.
pub(super) fn snapshot_holders(store: &Store, snapshot: &SnapshotRecord) -> Vec<SnapshotHolder> {
    let parts = memory_parts(snapshot);
    if parts.is_empty() {
        return Vec::new();
    }
    let digests: Vec<&str> = parts.iter().map(|(digest, _, _)| digest.as_str()).collect();
    let total = parts.len() as u32;
    let mut holders: Vec<SnapshotHolder> = store
        .content_locations()
        .holders_of(&digests)
        .into_iter()
        .map(|(node_id, (parts_here, bytes_here))| SnapshotHolder {
            captured_here: snapshot.node_id.as_deref() == Some(node_id.as_str()),
            node_id,
            parts_here,
            parts_total: total,
            bytes_here,
        })
        .collect();
    holders.sort_by(|a, b| {
        b.parts_here
            .cmp(&a.parts_here)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    holders
}

/// Build the GetNodeContent reply from the node row, its index entries, and
/// the snapshot table. Pure over its inputs (the index is read through
/// `entries` and `holder_count`), so the arithmetic is tested without a
/// store. `may_read` filters snapshot rows by the caller's `snapshot:read`.
pub(super) fn build_node_content_reply(
    node: &Node,
    entries: Option<(u64, Vec<(String, u64)>)>,
    holder_count: impl Fn(&str) -> usize,
    snapshots: Vec<SnapshotRecord>,
    may_read: impl Fn(&str) -> bool,
) -> GetNodeContentReply {
    let index_reported = entries.is_some();
    let (epoch, advertised) = entries.unwrap_or_default();
    let here: HashMap<&str, u64> = advertised
        .iter()
        .map(|(digest, bytes)| (digest.as_str(), *bytes))
        .collect();

    // digest -> (snapshot id, role), for the snapshots the caller may read.
    let mut owners: HashMap<String, (String, String)> = HashMap::new();
    let mut rows: Vec<NodeSnapshotContent> = Vec::new();
    for snapshot in snapshots {
        if snapshot.state == SnapshotState::Lost || !may_read(&snapshot.job_id) {
            continue;
        }
        let parts = memory_parts(&snapshot);
        let captured_here = snapshot.node_id.as_deref() == Some(node.id.as_str());
        let (parts_here, bytes_here) = parts
            .iter()
            .filter_map(|(digest, _, _)| here.get(digest.as_str()))
            .fold((0u32, 0u64), |(count, bytes), held| {
                (count + 1, bytes.saturating_add(*held))
            });
        if !captured_here && parts_here == 0 {
            continue;
        }
        for (digest, role, _) in &parts {
            if here.contains_key(digest.as_str()) {
                owners
                    .entry(digest.clone())
                    .or_insert_with(|| (snapshot.snapshot_id.clone(), role.clone()));
            }
        }
        rows.push(NodeSnapshotContent {
            captured_here,
            parts_here,
            parts_total: parts.len() as u32,
            bytes_here,
            snapshot: Some(snapshot_info(snapshot, false)),
        });
    }
    rows.sort_by(|a, b| {
        let created = |row: &NodeSnapshotContent| {
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.created_at.clone())
                .unwrap_or_default()
        };
        created(b).cmp(&created(a))
    });

    let mut entries: Vec<NodeContentEntry> = advertised
        .into_iter()
        .map(|(digest, bytes)| {
            let (snapshot_id, role) = owners.get(&digest).cloned().unwrap_or_default();
            NodeContentEntry {
                holders: holder_count(&digest) as u32,
                digest,
                bytes,
                snapshot_id,
                role,
            }
        })
        .collect();
    entries.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.digest.cmp(&b.digest)));

    GetNodeContentReply {
        node_id: node.id.clone(),
        index_reported,
        epoch,
        advertised_bytes: entries.iter().map(|entry| entry.bytes).sum(),
        entries,
        can_publish: crate::structs::snapshot::node_advertises(node, CONTENT_PUBLISH_CAPABILITY),
        can_checkpoint: crate::structs::snapshot::node_advertises(node, CHECKPOINT_CAPABILITY),
        snapshots: rows,
    }
}

impl ManagerDaemonService {
    /// One node's GetNodeUsage reply: the store's view of its allocations
    /// and reservations, plus this manager's latest usage report.
    pub(super) fn node_usage_reply(&self, node: &Node) -> Result<GetNodeUsageReply, Status> {
        let allocations = self
            .store
            .allocs_for_node(&node.id)
            .map_err(|e| map_store_error("Error listing node allocations", e))?;
        let mut bands = HashMap::new();
        for allocation in &allocations {
            if !bands.contains_key(&allocation.job_config_id) {
                let band = self
                    .store
                    .cached_band_for_job_config(&allocation.job_config_id)
                    .unwrap_or_default();
                bands.insert(allocation.job_config_id.clone(), band);
            }
        }
        let usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let mut reply = crate::manager::usage::build_node_usage_reply(
            node,
            &allocations,
            &self.store.resource_policy,
            &bands,
            usage.node(&node.id),
            std::time::Instant::now(),
        );
        // Suspended instances and checkpoint headroom. A failed count
        // degrades to zero rather than failing the whole reply.
        crate::manager::usage::fill_suspended_capacity(
            &mut reply,
            self.store.suspended_on_node(&node.id).unwrap_or_default(),
            self.store.content_locations().snapshot_budget(&node.id),
        );
        Ok(reply)
    }

    #[instrument(skip_all, fields(nodes = request.get_ref().node_ids.len()))]
    pub(super) async fn list_node_usage_rpc(
        &self,
        request: Request<ListNodeUsageRequest>,
    ) -> Result<Response<ListNodeUsageReply>, Status> {
        // List endpoint: gate unscoped, then filter each node by its pool.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        if inner.node_ids.len() > LIST_NODE_USAGE_MAX {
            return Err(Status::invalid_argument(format!(
                "{} node_ids requested; at most {LIST_NODE_USAGE_MAX} per call",
                inner.node_ids.len()
            )));
        }
        let (nodes, truncated) = if inner.node_ids.is_empty() {
            let mut nodes = self
                .store
                .get_nodes()
                .map_err(|e| map_store_error("Error listing nodes", e))?;
            nodes.sort_by(|a, b| a.id.cmp(&b.id));
            let truncated = nodes.len() > LIST_NODE_USAGE_MAX;
            nodes.truncate(LIST_NODE_USAGE_MAX);
            (nodes, truncated)
        } else {
            let mut nodes = Vec::with_capacity(inner.node_ids.len());
            for node_id in &inner.node_ids {
                // Unknown ids are left out, not an error: the caller's list
                // may come from a page that has since changed.
                if let Ok(node) = self.store.get_node(node_id) {
                    nodes.push(node);
                }
            }
            (nodes, false)
        };
        let mut replies = Vec::with_capacity(nodes.len());
        for node in nodes.iter().filter(|node| {
            identity.allows(
                AclResource::Node,
                AclAction::Read,
                &AclScope::pool(&node.pool),
            )
        }) {
            let mut reply = self.node_usage_reply(node)?;
            if !inner.include_allocations {
                reply.allocations.clear();
            }
            replies.push(reply);
        }
        Ok(Response::new(ListNodeUsageReply {
            nodes: replies,
            truncated,
        }))
    }

    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id))]
    pub(super) async fn get_node_content_rpc(
        &self,
        request: Request<GetNodeContentRequest>,
    ) -> Result<Response<GetNodeContentReply>, Status> {
        // Coarse before the lookup, pool scope after, as in GetNodeUsage.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let node_id = request.into_inner().node_id;
        let node = self
            .store
            .get_node(&node_id)
            .map_err(|e| map_store_error("Error getting node", e))?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Read,
            &AclScope::pool(&node.pool),
        )?;
        let snapshots = self
            .store
            .list_snapshots(&crate::storage::store::SnapshotFilter::default())
            .map_err(|e| map_store_error("Error listing snapshots", e))?;
        let index = self.store.content_locations();
        let reply = build_node_content_reply(
            &node,
            index.node_entries(&node.id),
            |digest| index.holder_count(digest),
            snapshots,
            |job_id| {
                identity.allows(
                    AclResource::Snapshot,
                    AclAction::Read,
                    &AclScope::job(job_id),
                )
            },
        );
        Ok(Response::new(reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::checkpoint::{MemoryPart, SnapshotKind, SnapshotManifest};
    use crate::structs::snapshot::{SnapshotDurability, SnapshotPurpose};
    use serde_json::json;

    const MIB: u64 = 1024 * 1024;

    fn node(id: &str, capabilities: &[&str]) -> Node {
        serde_json::from_value(json!({
            "id": id,
            "status": "ready",
            "pool": "default",
            "availability": "available",
            "lastHeartbeat": "",
            "healthChecks": {},
            "fingerprintAttrs": {},
            "dynamicAttrs": { "capabilities": capabilities },
            "configAttrs": {},
        }))
        .expect("node parses")
    }

    fn snapshot(id: &str, job: &str, node: &str, parts: &[(&str, &str, u64)]) -> SnapshotRecord {
        let manifest = SnapshotManifest {
            api_version: crate::structs::checkpoint::MANIFEST_API_VERSION,
            id: id.into(),
            job_id: job.into(),
            task: "t".into(),
            origin_instance: Some("i-1".into()),
            origin_alloc_id: 1,
            runtime: "sandbox".into(),
            kind: SnapshotKind::Memory,
            created_at: "2026-10-10T00:00:00Z".into(),
            volumes: Vec::new(),
            memory: parts
                .iter()
                .map(|(digest, role, size)| MemoryPart {
                    role: (*role).into(),
                    file: format!("{role}.bin"),
                    digest: (*digest).into(),
                    size: *size,
                    url: None,
                    key_id: None,
                })
                .collect(),
            compat: Default::default(),
            runtime_state: serde_json::Value::Null,
        };
        SnapshotRecord {
            snapshot_id: id.into(),
            job_id: job.into(),
            task_name: "t".into(),
            instance_id: Some("i-1".into()),
            name: None,
            kind: SnapshotKind::Memory,
            purpose: SnapshotPurpose::Manual,
            state: SnapshotState::Published,
            durability: SnapshotDurability::Store,
            node_id: Some(node.into()),
            allocation_id: Some(1),
            manifest_json: serde_json::to_string(&manifest).expect("manifest serializes"),
            total_bytes: parts.iter().map(|(_, _, size)| size).sum(),
            duration_ms: 1,
            error: None,
            created_at: 1,
            expires_at: None,
            pause_ms: 0,
            published_at: Some(2),
        }
    }

    /// Entries are labeled with the snapshot that owns them, sorted largest
    /// first; snapshots captured here or with parts here are listed, others
    /// and unreadable ones are not.
    #[test]
    fn node_content_labels_entries_and_lists_related_snapshots() {
        let w2 = node("w-2", &["checkpoint", "content-publish"]);
        let entries = Some((
            4,
            vec![
                ("sha256:mem".to_string(), 512 * MIB),
                ("sha256:art".to_string(), 8 * MIB),
            ],
        ));
        let snapshots = vec![
            // Captured on w-1, warmed onto w-2 (one of its two parts).
            snapshot(
                "snap-warm",
                "web",
                "w-1",
                &[
                    ("sha256:mem", "memory", 512 * MIB),
                    ("sha256:state", "vm-state", 2 * MIB),
                ],
            ),
            // Captured on w-2, nothing advertised (parts evicted).
            snapshot(
                "snap-here",
                "web",
                "w-2",
                &[("sha256:gone", "memory", 4 * MIB)],
            ),
            // Unrelated to w-2.
            snapshot(
                "snap-far",
                "web",
                "w-3",
                &[("sha256:far", "memory", 4 * MIB)],
            ),
            // Related, but the caller cannot read its job.
            snapshot(
                "snap-secret",
                "secret",
                "w-2",
                &[("sha256:art", "memory", 8 * MIB)],
            ),
        ];
        let reply = build_node_content_reply(
            &w2,
            entries,
            |digest| if digest == "sha256:mem" { 2 } else { 1 },
            snapshots,
            |job| job != "secret",
        );

        assert!(reply.index_reported);
        assert_eq!(reply.epoch, 4);
        assert!(reply.can_publish && reply.can_checkpoint);
        assert_eq!(reply.advertised_bytes, 520 * MIB);
        let labels: Vec<(&str, &str, &str, u32)> = reply
            .entries
            .iter()
            .map(|e| {
                (
                    e.digest.as_str(),
                    e.snapshot_id.as_str(),
                    e.role.as_str(),
                    e.holders,
                )
            })
            .collect();
        assert_eq!(
            labels,
            vec![
                ("sha256:mem", "snap-warm", "memory", 2),
                // Owned by an unreadable snapshot: digest kept, owner hidden.
                ("sha256:art", "", "", 1),
            ]
        );
        let rows: Vec<(String, bool, u32, u32)> = reply
            .snapshots
            .iter()
            .map(|row| {
                (
                    row.snapshot.as_ref().unwrap().snapshot_id.clone(),
                    row.captured_here,
                    row.parts_here,
                    row.parts_total,
                )
            })
            .collect();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert!(rows.contains(&("snap-warm".into(), false, 1, 2)));
        assert!(rows.contains(&("snap-here".into(), true, 0, 1)));
    }

    /// A node this manager holds no report for says so and lists nothing
    /// from the index, but still lists what it captured.
    #[test]
    fn node_content_without_a_report_still_lists_captured_snapshots() {
        let w1 = node("w-1", &[]);
        let reply = build_node_content_reply(
            &w1,
            None,
            |_| 0,
            vec![snapshot(
                "snap-a",
                "web",
                "w-1",
                &[("sha256:a", "memory", 2 * MIB)],
            )],
            |_| true,
        );
        assert!(!reply.index_reported);
        assert!(reply.entries.is_empty());
        assert!(!reply.can_publish && !reply.can_checkpoint);
        assert_eq!(reply.snapshots.len(), 1);
        assert_eq!(reply.snapshots[0].parts_here, 0);
    }
}
