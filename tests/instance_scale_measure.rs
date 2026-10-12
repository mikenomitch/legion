//! Phase 0 size measurements for the sandbox-scaling plan
//! (`internal_docs/plans/sandbox-scaling-plan.md` §9, Phase 0):
//!
//! - **Bytes per instance row and per snapshot row**, and the store's
//!   snapshot image size at 10^5 instances (10^6 opt-in), measured as the
//!   growth of a `VACUUM INTO` image — exactly what a raft snapshot is, so
//!   the number is what a new manager or a lagging follower receives.
//! - **Heartbeat payload bytes** at 1,000 and 10,000 allocations per node,
//!   request and reply, as the protobuf encodes them, plus the manager's
//!   time to answer one steady beat.
//!
//! Ignored by default (it inserts a lot of rows):
//!
//! ```text
//! cargo test --release --test instance_scale_measure -- --ignored --nocapture
//! LEGION_MEASURE_INSTANCES=100000,1000000 cargo test --release \
//!     --test instance_scale_measure -- --ignored --nocapture
//! LEGION_MEASURE_HEARTBEAT_ALLOCS=1000,10000 ...
//! ```
//!
//! Sizes do not depend on the persistence backend (a raft store holds the
//! same SQLite schema), so these run on a local store. Results print as one
//! JSON line per measurement, tagged `instance_scale_measure`.

use std::time::Instant;

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig};
use legion::structs::manager_server::Manager;
use legion::structs::suspend::SuspendConfig;
use legion::structs::{
    AllocationHealth, HeartbeatRequest, InstanceMode, Node, NodeMsg, SnapshotReport, Submission,
    SuspendInstanceRequest,
};
use prost::Message;
use tonic::Request;

const JOB: &str = "agents";
const TASK: &str = "workspace";
const NODE: &str = "dense-node";

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn store_at(dir: &std::path::Path) -> R<Store> {
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    })
}

/// A node big enough to hold `allocations` 64 MiB, 10 MHz sandboxes.
fn dense_node(allocations: usize) -> Node {
    let memory_mib = (allocations as u64 + 16) * 64;
    // The scheduler reads CPU capacity from the fingerprint: enough 2 GHz
    // cores for every sandbox's 10 MHz.
    let cores = (allocations as u64 * 10).div_ceil(2000) + 1;
    let cpus: serde_json::Map<String, serde_json::Value> = (1..=cores)
        .map(|core| (format!("cpu-{core}"), json!({ "frequency": 2000.0 })))
        .collect();
    Node {
        id: NODE.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": NODE,
            "maxAllocations": allocations + 16,
            "resources": { "memory": memory_mib, "cpu": (allocations as u64 + 16) * 10 },
        }),
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": (memory_mib * 1024 * 1024).to_string(),
            "cpus": cpus,
            "cpu.vendor": "GenuineIntel",
            "cpu.model": "6:106:6",
            "cpu.features": "3f9a0c21",
            "sandbox.vmm": "firecracker",
            "sandbox.vmm.version": "1.12.0",
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": ["checkpoint"] }),
        health_checks: json!({}),
    }
}

fn submit_sandbox_job(store: &Store) -> R<()> {
    let task = TaskConfig {
        config: json!({ "image": "alpine", "command": ["sleep", "3600"] }),
        resources: json!({ "memory": 64, "cpu": 10 }),
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            require_id: false,
            initial_instances: None,
            on_lost: None,
            storage: None,
            envelope: None,
            promote: None,
        }),
        suspend: Some(SuspendConfig {
            checkpoint: Some("memory".into()),
            ..Default::default()
        }),
        ..TaskConfig::new(TASK, "sandbox")
    };
    store.handle_job_submit(
        &JobConfig {
            priority: Default::default(),
            name: JOB.to_string(),
            tasks: vec![task],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        },
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    Ok(())
}

fn run_eval_worker(store: &Store) -> R<()> {
    let limits = EvalBatchLimits {
        max: 16,
        workers: 1,
    };
    let mut batch = store.claim_evals(limits, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, limits, ClaimScope::Any)?;
    }
    Ok(())
}

fn image_bytes(store: &Store, dir: &std::path::Path, label: &str) -> R<u64> {
    let path = dir.join(format!("image-{label}.sqlite"));
    let info = store.state_backup(Some(&path))?;
    let _ = std::fs::remove_file(&path);
    Ok(info.bytes)
}

/// Bytes per instance row (pending, and active with its allocation) and per
/// suspended instance (its snapshot row plus the instance row's checkpoint
/// columns), and the image size at each requested count.
#[test]
#[ignore = "Phase 0 measurement; run with --ignored --nocapture"]
fn instance_and_snapshot_row_bytes() -> R<()> {
    for count in env_list("LEGION_MEASURE_INSTANCES", &[100_000]) {
        let dir = tempfile::tempdir()?;
        let store = store_at(dir.path())?;
        submit_sandbox_job(&store)?;
        let empty = image_bytes(&store, dir.path(), "empty")?;
        let started = Instant::now();
        for _ in 0..count {
            store.create_keyed_instance(JOB, "default", TASK, None)?;
        }
        let insert_secs = started.elapsed().as_secs_f64();
        let pending = image_bytes(&store, dir.path(), "pending")?;
        println!(
            "instance_scale_measure {}",
            json!({
                "measure": "pending_instance_rows",
                "instances": count,
                "image_bytes_empty": empty,
                "image_bytes": pending,
                "bytes_per_instance_row": (pending - empty) as f64 / count as f64,
                "creates_per_sec_store_only": count as f64 / insert_secs,
            })
        );
    }

    // Suspended instances: the snapshot row and the instance row's
    // checkpoint state, measured on a smaller set driven through the real
    // suspend path (the RPC plus the node's capture report).
    let suspended = env_list("LEGION_MEASURE_SUSPENDED", &[5_000])[0];
    let dir = tempfile::tempdir()?;
    let store = store_at(dir.path())?;
    store.insert_node(NodeMsg::from(dense_node(suspended)))?;
    submit_sandbox_job(&store)?;
    run_eval_worker(&store)?;
    let mut ids = Vec::with_capacity(suspended);
    for _ in 0..suspended {
        ids.push(
            store
                .create_keyed_instance(JOB, "default", TASK, None)?
                .instance_id,
        );
    }
    let base = image_bytes(&store, dir.path(), "created")?;
    let config_id = store.get_job_config(JOB)?.id();
    store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    run_eval_worker(&store)?;
    let active = image_bytes(&store, dir.path(), "active")?;
    let svc = ManagerDaemonService::new(store.clone());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let mut reports = Vec::with_capacity(ids.len());
    for id in &ids {
        let instance = store.get_instance(id)?.context("instance")?;
        let alloc = instance.current_alloc_id.context("placed")?;
        let reply = rt
            .block_on(svc.suspend_instance(Request::new(SuspendInstanceRequest {
                instance_id: id.clone(),
                checkpoint: None,
                idempotency_key: None,
            })))?
            .into_inner();
        reports.push(SnapshotReport {
            snapshot_id: reply.snapshot_id.clone(),
            op: "capture".into(),
            ok: true,
            manifest_json: json!({
                "apiVersion": 1, "id": reply.snapshot_id, "jobId": JOB, "task": TASK,
                "originInstance": id, "originAllocId": alloc, "runtime": "sandbox",
                "kind": "memory", "createdAt": "2026-10-10T00:00:00Z",
            })
            .to_string(),
            duration_ms: 400,
            total_bytes: 536_870_912,
            allocation_id: alloc,
            ..Default::default()
        });
    }
    for chunk in reports.chunks(500) {
        rt.block_on(svc.heartbeat(Request::new(HeartbeatRequest {
            id: NODE.into(),
            snapshot_reports: chunk.to_vec(),
            ..Default::default()
        })))?;
    }
    let after = image_bytes(&store, dir.path(), "suspended")?;
    println!(
        "instance_scale_measure {}",
        json!({
            "measure": "suspended_instances",
            "instances": suspended,
            "bytes_per_active_instance_with_allocation": (active as f64 - base as f64) / suspended as f64,
            "bytes_per_suspended_instance_vs_pending": (after as f64 - base as f64) / suspended as f64,
            "image_bytes_pending": base,
            "image_bytes_active": active,
            "image_bytes_suspended": after,
        })
    );
    Ok(())
}

/// Phase 4 acceptance: churn through unnamed (server-minted) instances and
/// check the instance table holds only the live ones. Default 10^5 creates
/// and destroys; `LEGION_MEASURE_CHURN=1000000` runs the plan's 10^6.
#[test]
#[ignore = "Phase 4 measurement; run with --ignored --nocapture"]
fn unnamed_churn_keeps_the_table_at_the_live_count() -> R<()> {
    let churn = env_list("LEGION_MEASURE_CHURN", &[100_000])[0];
    let live = 1_000;
    let dir = tempfile::tempdir()?;
    let store = store_at(dir.path())?;
    submit_sandbox_job(&store)?;
    for _ in 0..live {
        store.create_keyed_instance(JOB, "default", TASK, None)?;
    }
    let before = image_bytes(&store, dir.path(), "live")?;
    let started = Instant::now();
    for _ in 0..churn {
        let id = store
            .create_keyed_instance(JOB, "default", TASK, None)?
            .instance_id;
        store.destroy_keyed_instance(&id)?;
    }
    let secs = started.elapsed().as_secs_f64();
    let rows = store.list_instances(JOB)?.len();
    let after = image_bytes(&store, dir.path(), "churned")?;
    println!(
        "instance_scale_measure {}",
        json!({
            "measure": "unnamed_churn",
            "churned": churn,
            "live": live,
            "rows_after": rows,
            "image_bytes_before": before,
            "image_bytes_after": after,
            "create_destroy_pairs_per_sec_store_only": churn as f64 / secs,
        })
    );
    assert_eq!(rows, live, "only live unnamed instances keep rows");
    Ok(())
}

/// Heartbeat bytes both ways, and the manager's time per beat, for one node
/// running N allocations, steady state (every allocation already reported
/// healthy once).
#[test]
#[ignore = "Phase 0 measurement; run with --ignored --nocapture"]
fn heartbeat_bytes_per_dense_node() -> R<()> {
    for allocations in env_list("LEGION_MEASURE_HEARTBEAT_ALLOCS", &[1_000, 10_000]) {
        let dir = tempfile::tempdir()?;
        let store = store_at(dir.path())?;
        store.insert_node(NodeMsg::from(dense_node(allocations)))?;
        submit_sandbox_job(&store)?;
        run_eval_worker(&store)?;
        for _ in 0..allocations {
            store.create_keyed_instance(JOB, "default", TASK, None)?;
        }
        let config_id = store.get_job_config(JOB)?.id();
        store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
        run_eval_worker(&store)?;
        let ids: Vec<i64> = store.allocs_for_node(NODE)?.iter().map(|a| a.id).collect();
        assert_eq!(ids.len(), allocations, "every instance placed on the node");

        let svc = ManagerDaemonService::new(store.clone());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let request = HeartbeatRequest {
            id: NODE.into(),
            allocation_ids: ids.clone(),
            allocation_health: ids
                .iter()
                .map(|id| AllocationHealth {
                    allocation_id: *id,
                    healthy: true,
                    status: "running".into(),
                    artifact_states: None,
                })
                .collect(),
            ..Default::default()
        };
        // First beat records health; the measured beats are steady.
        rt.block_on(svc.heartbeat(Request::new(request.clone())))?;
        let beats = 5;
        let mut reply_bytes = 0;
        let started = Instant::now();
        for _ in 0..beats {
            let reply = rt
                .block_on(svc.heartbeat(Request::new(request.clone())))?
                .into_inner();
            reply_bytes = reply.encoded_len();
        }
        let per_beat_ms = started.elapsed().as_secs_f64() * 1000.0 / beats as f64;
        println!(
            "instance_scale_measure {}",
            json!({
                "measure": "steady_heartbeat",
                "allocations_per_node": allocations,
                "request_bytes": request.encoded_len(),
                "reply_bytes": reply_bytes,
                "manager_ms_per_beat": per_beat_ms,
            })
        );
    }
    Ok(())
}
