//! Content locality end to end on the manager: content reports ride the
//! heartbeat into the in-memory location index, a restarted manager asks for
//! a full resync, and nothing reaches the state store
//! (docs/artifacts.md, "Content locality").
//!
//! The worker half (diffing the store against the acknowledged set) is
//! unit-tested in `src/worker/content_report.rs`; the index in
//! `src/storage/content_index.rs`; the score term in the scheduler's tests.

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::{
    config::ManagerConfigFull, ContentLocation, ContentLocationReport, HeartbeatRequest, NodeMsg,
};
use legion::worker::content_report::ContentAdvertiser;
use std::collections::HashMap;
use tonic::Request;

const NODE_ID: &str = "warm-node";
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

/// One worker heartbeat carrying a content report; returns `content_resync`.
async fn beat(
    svc: &ManagerDaemonService,
    worker: &mut ContentAdvertiser,
    current: &[(&str, u64)],
) -> R<bool> {
    let current: HashMap<String, u64> = current.iter().map(|(d, b)| (d.to_string(), *b)).collect();
    let pending = worker.prepare(current);
    let reply = svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: NODE_ID.into(),
            content_locations: Some(pending.report.clone()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    worker.acknowledge(pending, reply.content_resync);
    Ok(reply.content_resync)
}

#[tokio::test(flavor = "multi_thread")]
async fn reports_flow_into_the_index_and_a_restart_resyncs() -> R<()> {
    let dir = tempfile::Builder::new()
        .prefix("legion-content-locality-")
        .tempdir()?;
    let svc = ManagerDaemonService::new(start_store(dir.path())?);
    insert_node(&svc.store)?;
    let mut worker = ContentAdvertiser::new();
    let a = format!("sha256:{}", "aa".repeat(32));
    let b = format!("sha256:{}", "bb".repeat(32));

    // Registration: full set, applied.
    assert!(!beat(&svc, &mut worker, &[(&a, 8 * MIB)]).await?);
    let index = svc.store.content_locations();
    assert_eq!(index.bytes_on(&a, NODE_ID), Some(8 * MIB));

    // A delta.
    assert!(!beat(&svc, &mut worker, &[(&a, 8 * MIB), (&b, 2 * MIB)]).await?);
    assert_eq!(index.bytes_on(&b, NODE_ID), Some(2 * MIB));

    // A heartbeat without a report (an old worker) is never told to resync.
    let reply = svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: NODE_ID.into(),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(!reply.content_resync);

    // Restart over the same store: the index was never persisted, so the
    // node scores 0 until its next report, and the worker's probe is
    // answered with a resync...
    drop(svc);
    let restarted = ManagerDaemonService::new(start_store(dir.path())?);
    assert_eq!(
        restarted.store.content_locations().bytes_on(&a, NODE_ID),
        None
    );
    assert!(beat(&restarted, &mut worker, &[(&a, 8 * MIB), (&b, 2 * MIB)]).await?);
    // ...and the full report that follows restores it.
    assert!(!beat(&restarted, &mut worker, &[(&a, 8 * MIB), (&b, 2 * MIB)]).await?);
    let index = restarted.store.content_locations();
    assert_eq!(index.bytes_on(&a, NODE_ID), Some(8 * MIB));
    assert_eq!(index.bytes_on(&b, NODE_ID), Some(2 * MIB));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delta_the_manager_cannot_place_asks_for_a_full_report() -> R<()> {
    let dir = tempfile::Builder::new()
        .prefix("legion-content-locality-")
        .tempdir()?;
    let svc = ManagerDaemonService::new(start_store(dir.path())?);
    insert_node(&svc.store)?;
    let reply = svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: NODE_ID.into(),
            content_locations: Some(ContentLocationReport {
                epoch: 9,
                base_epoch: 8,
                full: false,
                added: vec![ContentLocation {
                    digest: format!("sha256:{}", "cc".repeat(32)),
                    bytes: 4 * MIB,
                }],
                removed: Vec::new(),
                snapshot_budget: None,
            }),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(reply.content_resync);
    assert_eq!(svc.store.content_locations().size(), (0, 0));
    Ok(())
}
