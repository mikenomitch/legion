//! Usage reporting end to end on the manager: heartbeats with and without a
//! usage report, `GetNodeUsage`, and what a manager restart does to usage
//! (docs/resource-enforcement.md, "Measuring usage").
//!
//! The worker half (cgroup readers and the sampler) is unit-tested in
//! `src/worker/usage.rs` against fixture files; the reply arithmetic in
//! `src/manager/usage.rs`.

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::{
    config::ManagerConfigFull, AllocationUsage, GetNodeUsageRequest, HeartbeatRequest,
    ListNodeUsageRequest, NodeMemoryUsage, NodeMsg, UsageReport,
};
use tonic::{Code, Request};

const NODE_ID: &str = "usage-node";
const MIB: u64 = 1024 * 1024;

fn start_store(dir: &std::path::Path) -> R<Store> {
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })
}

fn insert_node(store: &Store) -> R<()> {
    store.insert_node(NodeMsg {
        id: NODE_ID.into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: format!(r#"{{"memory_total":"{}"}}"#, 4096 * MIB),
        dynamic_attrs: "{}".into(),
        health_checks: "[]".into(),
        last_heartbeat: "2026-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;
    Ok(())
}

fn report() -> UsageReport {
    UsageReport {
        sampled_at_unix_ms: 1_000_000,
        allocations: vec![AllocationUsage {
            allocation_id: 4242,
            memory_current_bytes: 300 * MIB,
            memory_peak_bytes: 310 * MIB,
            ..Default::default()
        }],
        node_memory: Some(NodeMemoryUsage {
            total_bytes: 4096 * MIB,
            available_bytes: 3000 * MIB,
            psi_some_avg10: 0.0,
            psi_available: true,
        }),
        pressure: None,
    }
}

async fn node_usage(svc: &ManagerDaemonService) -> R<legion::structs::GetNodeUsageReply> {
    Ok(svc
        .get_node_usage(Request::new(GetNodeUsageRequest {
            node_id: NODE_ID.into(),
        }))
        .await?
        .into_inner())
}

#[tokio::test(flavor = "multi_thread")]
async fn usage_flows_from_heartbeat_to_get_node_usage_and_not_into_the_store() -> R<()> {
    let dir = tempfile::Builder::new()
        .prefix("legion-node-usage-")
        .tempdir()?;
    let svc = ManagerDaemonService::new(start_store(dir.path())?);
    insert_node(&svc.store)?;

    // An old worker (or one with enforcement off) sends no usage; the
    // heartbeat is accepted and the node reads as unreported, with capacity.
    svc.heartbeat(Request::new(HeartbeatRequest {
        id: NODE_ID.into(),
        ..Default::default()
    }))
    .await?;
    let before = node_usage(&svc).await?;
    assert!(!before.usage_reported);
    assert_eq!(
        before.memory.as_ref().map(|memory| memory.capacity),
        Some(4096.0)
    );

    // A report arrives and is served.
    svc.heartbeat(Request::new(HeartbeatRequest {
        id: NODE_ID.into(),
        usage: Some(report()),
        ..Default::default()
    }))
    .await?;
    let after = node_usage(&svc).await?;
    assert!(after.usage_reported);
    assert_eq!(after.sampled_at_unix_ms, 1_000_000);
    let memory = after.memory.context("memory")?;
    assert_eq!(memory.available_bytes, 3000 * MIB);
    assert!(memory.pressure_available);

    // A heartbeat between reports (no usage) keeps the last report.
    svc.heartbeat(Request::new(HeartbeatRequest {
        id: NODE_ID.into(),
        ..Default::default()
    }))
    .await?;
    assert!(node_usage(&svc).await?.usage_reported);

    // A manager restart over the same store starts with no usage: it was
    // never written there.
    drop(svc);
    let restarted = ManagerDaemonService::new(start_store(dir.path())?);
    let fresh = node_usage(&restarted).await?;
    assert!(!fresh.usage_reported, "usage must not survive a restart");
    assert_eq!(fresh.node_id, NODE_ID);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_node_is_not_found() -> R<()> {
    let dir = tempfile::Builder::new()
        .prefix("legion-node-usage-")
        .tempdir()?;
    let svc = ManagerDaemonService::new(start_store(dir.path())?);
    let status = svc
        .get_node_usage(Request::new(GetNodeUsageRequest {
            node_id: "nope".into(),
        }))
        .await
        .expect_err("no such node");
    assert_eq!(status.code(), Code::NotFound);
    Ok(())
}

/// ListNodeUsage answers with exactly the GetNodeUsage reply per node,
/// skips unknown ids, drops per-allocation rows unless asked, and refuses
/// an oversized id list.
#[tokio::test(flavor = "multi_thread")]
async fn list_node_usage_matches_get_node_usage_per_node() -> R<()> {
    let dir = tempfile::Builder::new()
        .prefix("legion-node-usage-")
        .tempdir()?;
    let svc = ManagerDaemonService::new(start_store(dir.path())?);
    insert_node(&svc.store)?;
    svc.heartbeat(Request::new(HeartbeatRequest {
        id: NODE_ID.into(),
        usage: Some(report()),
        ..Default::default()
    }))
    .await?;
    let single = node_usage(&svc).await?;

    let all = svc
        .list_node_usage(Request::new(ListNodeUsageRequest::default()))
        .await?
        .into_inner();
    assert!(!all.truncated);
    assert_eq!(all.nodes.len(), 1);
    let listed = &all.nodes[0];
    assert_eq!(listed.node_id, NODE_ID);
    assert!(listed.usage_reported);
    assert_eq!(listed.memory, single.memory, "same totals as GetNodeUsage");
    assert!(listed.allocations.is_empty(), "rows omitted by default");

    let named = svc
        .list_node_usage(Request::new(ListNodeUsageRequest {
            node_ids: vec!["nope".into(), NODE_ID.into()],
            include_allocations: true,
        }))
        .await?
        .into_inner();
    assert_eq!(named.nodes.len(), 1, "unknown ids are left out");
    assert_eq!(named.nodes[0].allocations, single.allocations);

    let status = svc
        .list_node_usage(Request::new(ListNodeUsageRequest {
            node_ids: (0..501).map(|n| format!("n-{n}")).collect(),
            include_allocations: false,
        }))
        .await
        .expect_err("too many ids");
    assert_eq!(status.code(), Code::InvalidArgument);
    Ok(())
}
