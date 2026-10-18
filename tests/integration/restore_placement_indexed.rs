//! Restoring instances keep their placement rule when the scheduling index
//! is armed (suspend plan Phase 7b; docs/scheduling-indexes.md).
//!
//! An armed index may truncate a task's candidate list to the nearest-fit
//! nodes. A restore from a `local` snapshot is pinned to one node, and a
//! truncated list that left that node out failed the placement as if the
//! node were gone. Stateful tasks now never truncate. The setup mirrors the
//! `restore` group of `benchmarks/micro/scheduler_bench.rs`, which is where
//! this showed up.

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, ContentLocation, ContentLocationReport, JobConfig, NodeMsg,
    Submission, TaskConfig,
};
use serde_json::json;
use tempfile::{Builder, TempDir};
fn store_path(prefix: &str) -> anyhow::Result<(String, TempDir)> {
    let store_dir = Builder::new().prefix(prefix).tempdir()?;
    let path = store_dir
        .path()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Error parsing store path"))?
        .to_string();
    Ok((path, store_dir))
}

fn setup_store_with_settings(
    policy: &str,
    scheduler_settings: Option<serde_json::Value>,
) -> anyhow::Result<(Store, TempDir)> {
    let (store_path, store_dir) = store_path("legion-scheduler-bench-")?;
    let store_config = ManagerConfigFull {
        store_path,
        scheduling_policy: policy.into(),
        task_resource_defaults: Some(json!({
            "cpu": 0,
            "memory": 0,
            "allocations": 1
        })),
        scheduler_settings,
        ..Default::default()
    };

    let store = Store::start_storage(&store_config)?;
    store.reset_all_tables()?;
    Ok((store, store_dir))
}

fn default_fingerprint() -> serde_json::Value {
    json!({
        "memory_total": "8589934592",
        "cpus": {
            "cpu-1": { "frequency": "2400" },
            "cpu-2": { "frequency": "2400" },
        }
    })
}

fn make_node_msg(id: &str, attrs: serde_json::Value) -> NodeMsg {
    NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: attrs.to_string(),
        fingerprint_attrs: default_fingerprint().to_string(),
        // Workers that take checkpoints and fetch published ones, so the
        // published restores below have candidates.
        dynamic_attrs: json!({ "capabilities": ["checkpoint", "checkpoint-fetch"] }).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        heartbeat_source: String::new(),
    }
}

fn submit_jobs(store: &Store, jobs: &[JobConfig]) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::with_capacity(jobs.len());
    let submission = Submission {
        path: "bench.ncl".into(),
        content: "{}".into(),
        is_primary: true,
    };
    for job in jobs {
        store.handle_job_submit(job, std::slice::from_ref(&submission))?;
        ids.push(store.get_job_config(&job.name)?.id());
    }
    Ok(ids)
}

const RESTORE_INSTANCES: usize = 25;
const RESTORE_CPU_MODEL: &str = "6:106:6";

fn restore_digest() -> String {
    format!("sha256:{:064x}", 0xbeef)
}

fn insert_restore_nodes(store: &Store, node_count: usize) -> anyhow::Result<()> {
    for idx in 0..node_count {
        let mut node = make_node_msg(&format!("node-{idx}"), json!({ "tier": "default" }));
        let mut fingerprint = default_fingerprint();
        fingerprint["cpu.vendor"] = json!("GenuineIntel");
        fingerprint["cpu.model"] = json!(RESTORE_CPU_MODEL);
        fingerprint["sandbox.vmm"] = json!("firecracker");
        fingerprint["sandbox.vmm.version"] = json!("1.7.0");
        node.fingerprint_attrs = fingerprint.to_string();
        store.insert_node(node)?;
    }
    Ok(())
}

fn restore_manifest(snapshot_id: &str, allocation_id: i64, published: bool) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": "bench-restore",
        "task": "workspace",
        "originAllocId": allocation_id,
        "runtime": "sandbox",
        "kind": "memory",
        "createdAt": "2026-10-10T00:00:00Z",
        "memory": [{
            "role": "memory", "file": "memory", "digest": restore_digest(),
            "size": 512 * 1024 * 1024,
            "url": if published { Some("s3://bench/sha256/beef") } else { None },
        }],
        "compat": {
            "vmm": { "type": "firecracker", "version": "1.7.0" },
            "cpu": { "vendor": "GenuineIntel", "model": RESTORE_CPU_MODEL },
        },
    })
    .to_string()
}

/// A store with RESTORE_INSTANCES keyed instances pending a restore from
/// their own snapshot, `published` or `local`. Returns the job config id.
fn setup_restores(store: &Store, node_count: usize, published: bool) -> anyhow::Result<String> {
    use legion::storage::store::{NewSnapshot, SnapshotReportRecord};
    use legion::structs::checkpoint::SnapshotKind;
    use legion::structs::snapshot::{SnapshotDurability, SnapshotPurpose};

    insert_restore_nodes(store, node_count)?;
    let job = JobConfig {
        priority: Default::default(),
        name: "bench-restore".into(),
        tasks: vec![TaskConfig {
            stateful: Some(serde_json::from_value(json!({ "mode": "keyed" }))?),
            ..TaskConfig::new("workspace", "noop")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    };
    let job_config_id = submit_jobs(store, std::slice::from_ref(&job))?
        .pop()
        .expect("one id");
    for idx in 0..RESTORE_INSTANCES {
        store.create_keyed_instance(
            "bench-restore",
            "default",
            "workspace",
            Some(&format!("i-{idx}")),
        )?;
    }
    Scheduler::new(store)?.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
    for idx in 0..RESTORE_INSTANCES {
        let instance_id = format!("i-{idx}");
        let instance = store.get_instance(&instance_id)?.expect("instance");
        let allocation_id = instance.current_alloc_id.expect("placed");
        let node_id = store
            .get_allocation_by_id(allocation_id)?
            .expect("allocation")
            .node_id;
        let snapshot_id = format!("snap-bench-{idx}");
        store.create_snapshot(NewSnapshot {
            snapshot_id: snapshot_id.clone(),
            job_id: "bench-restore".into(),
            task_name: "workspace".into(),
            instance_id: instance_id.clone(),
            name: None,
            kind: SnapshotKind::Memory,
            purpose: SnapshotPurpose::Manual,
            node_id: node_id.clone(),
            allocation_id,
            expires_at: None,
            suspend_reason: None,
            durability: if published {
                SnapshotDurability::Store
            } else {
                SnapshotDurability::Local
            },
        })?;
        let report = |op: &str, published_manifest: bool| SnapshotReportRecord {
            snapshot_id: snapshot_id.clone(),
            op: op.into(),
            ok: true,
            error: String::new(),
            manifest_json: restore_manifest(&snapshot_id, allocation_id, published_manifest),
            duration_ms: 1,
            total_bytes: 512 * 1024 * 1024,
            allocation_id,
            local_suspension: false,
            pause_ms: 0,
            manifest_url: String::new(),
        };
        store.apply_snapshot_report(&node_id, &report("capture", false))?;
        if published {
            store.apply_snapshot_report(&node_id, &report("publish", true))?;
        }
        store.restore_keyed_instance(&instance_id, &snapshot_id)?;
    }
    if published {
        let now = std::time::Instant::now();
        for idx in (0..node_count).step_by(4) {
            store.content_locations().ingest(
                &format!("node-{idx}"),
                &ContentLocationReport {
                    epoch: 1,
                    base_epoch: 0,
                    full: true,
                    added: vec![ContentLocation {
                        digest: restore_digest(),
                        bytes: 512 * 1024 * 1024,
                    }],
                    removed: Vec::new(),
                    snapshot_budget: None,
                },
                now,
            );
        }
    }
    Ok(job_config_id)
}

#[test]
fn restores_place_on_their_snapshot_nodes_with_an_armed_index() {
    for published in [false, true] {
        let (store, _dir) =
            setup_store_with_settings("bin_pack", Some(json!({ "index_min_nodes": 1 }))).unwrap();
        let job_config_id = setup_restores(&store, 300, published).unwrap();
        let plan = Scheduler::new(&store)
            .unwrap()
            .plan_only(&job_config_id, PlanMode::Fresh)
            .unwrap_or_else(|err| panic!("published={published}: {err:#}"));
        assert_eq!(plan.len(), RESTORE_INSTANCES, "published={published}");
    }
}
