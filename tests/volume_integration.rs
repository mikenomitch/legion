//! Integration coverage for the volume lifecycle (see docs/volumes.md).
//!
//! These exercise the manager's half — the volume table, node pinning, and the
//! reclaim state machine — against a real store, since that is where the
//! invariants live. The driver half is covered by the conformance harness
//! (`src/volumes/conformance.rs`, also reachable as
//! `legion volume plugin test`), which runs against built-ins and exec plugins
//! alike.

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::prelude::{json, Context, R};
use legion::storage::store::{Store, VolumeFilter};
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::StatefulStorageConfig,
    job::TaskConfig, job::TaskCount, job::VolumeSpec, Eval, InstanceMode, Node, NodeMsg,
    Submission, VolumeState,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use tempfile::Builder;

const NODE_A: &str = "node-a";
const NODE_B: &str = "node-b";

struct Cluster {
    store: Store,
    eval_seq: u64,
    job_config_ids: BTreeMap<String, String>,
}

impl Cluster {
    fn new() -> R<Self> {
        let store_dir = Builder::new()
            .prefix("legion-volume-it-")
            .tempdir()
            .context("temp store dir")?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: store_dir.to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Cluster {
            store,
            eval_seq: 0,
            job_config_ids: BTreeMap::new(),
        })
    }

    /// A node advertising the built-in drivers, so admission and placement see
    /// realistic capability data instead of the pre-fingerprinting fallback.
    fn add_node(&self, id: &str) -> R<()> {
        self.add_node_with_disk(id, 100 * 1024 * 1024 * 1024)
    }

    /// Same, with an explicit free-disk figure for placement-accounting tests.
    fn add_node_with_disk(&self, id: &str, disk_free_bytes: u64) -> R<()> {
        let mut fingerprint = Map::new();
        fingerprint.insert("arch".into(), json!("aarch64"));
        fingerprint.insert(
            "volumeDrivers".into(),
            json!({
                "local-dir": {
                    "apiVersion": 1, "name": "local-dir", "nodeLocal": true,
                    "ephemeral": false, "accessModes": ["single-writer"], "available": true
                },
                "nfs": {
                    "apiVersion": 1, "name": "nfs", "nodeLocal": false,
                    "ephemeral": false, "accessModes": ["single-writer"],
                    "requiredParams": ["server", "export"], "available": true
                }
            }),
        );
        fingerprint.insert("memory_total".into(), json!("68719476736"));
        fingerprint.insert("disk_total_space".into(), json!("214748364800"));
        fingerprint.insert(
            "disk_available_space".into(),
            json!(disk_free_bytes.to_string()),
        );
        fingerprint.insert(
            "cpus".into(),
            json!({ "cpu-1": { "frequency": 2000.0 }, "cpu-2": { "frequency": 2000.0 } }),
        );
        self.store.insert_node(NodeMsg::from(Node {
            id: id.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "node_id": id,
                "maxAllocations": 20,
                "resources": { "memory": 65536, "cpu": 40000 },
            }),
            fingerprint_attrs: Value::Object(fingerprint),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }))?;
        Ok(())
    }

    fn submit(&mut self, config: JobConfig) -> R<()> {
        let name = config.name.clone();
        self.store.handle_job_submit(
            &config,
            &[Submission {
                path: format!("{name}.json"),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        let config_id = self.store.get_job_config(&name)?.id();
        self.job_config_ids.insert(name.clone(), config_id.clone());
        self.eval("job_submit", &name, &config_id)?;
        Ok(())
    }

    fn eval(&mut self, kind: &str, job: &str, config_id: &str) -> R<()> {
        self.eval_seq += 1;
        self.store.insert_eval(&Eval {
            id: self.eval_seq.to_string(),
            status: "pending".into(),
            kind: kind.into(),
            job_id: job.to_string(),
            job_config_id: config_id.to_string(),
            node_name: String::new(),
            priority: 50,
        })?;
        let eval = self.store.get_next_eval()?.context("no eval queued")?;
        let outcome = eval.handle(&self.store);
        self.store.set_eval_status(
            &eval.id,
            if outcome.is_ok() {
                "complete"
            } else {
                "failed"
            },
        )?;
        outcome.map(|_| ())
    }

    fn reschedule(&mut self, job: &str) -> R<()> {
        let config_id = self.job_config_ids.get(job).cloned().context("job")?;
        self.eval("job_reschedule", job, &config_id)
    }

    fn volumes(&self) -> R<Vec<legion::structs::Volume>> {
        self.store.list_volumes(&VolumeFilter::default())
    }

    /// Stand in for a worker mounting the volume and reporting it back.
    fn report_mounted(&self, volume_id: &str, node_id: &str, node_local: bool) -> R<()> {
        self.store.record_volume_mounted(
            volume_id,
            node_id,
            &format!("/var/lib/legion/volumes/{volume_id}"),
            node_local,
            None,
            &BTreeMap::new(),
        )
    }
}

fn stateful_job(name: &str, count: u32, driver: &str, params: Option<Value>) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount {
                num: count,
                per: None,
            },
            config: json!({ "command": ["sleep", "60"] }),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(StatefulStorageConfig {
                    path: "/var/lib/db".into(),
                    driver: Some(driver.to_string()),
                    mode: None,
                    params,
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
            }),
            ..TaskConfig::new("db", "exec")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

#[test]
fn placement_mints_one_volume_per_slot_with_a_matching_handle() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 3, "local-dir", None))?;

    let volumes = cluster.volumes()?;
    assert_eq!(volumes.len(), 3, "one volume per slot");
    for volume in &volumes {
        assert_eq!(volume.driver, "local-dir");
        assert_eq!(volume.state, VolumeState::Pending, "provisioning is lazy");
        assert!(volume.node_local, "local-dir is node-local");
        assert!(volume.node_id.is_none(), "no pin before the first mount");
    }

    // Every handle must have a row and vice versa — that invariant is what
    // lets the reaper treat "unreferenced" as an unambiguous reclaim signal.
    let instances = cluster.store.list_instances("db")?;
    let handles: Vec<String> = instances
        .iter()
        .filter_map(|instance| instance.storage_handle.clone())
        .collect();
    assert_eq!(handles.len(), 3);
    for volume in &volumes {
        assert!(
            handles.contains(&volume.handle()),
            "volume {} has no instance referencing it",
            volume.volume_id
        );
    }
    Ok(())
}

#[test]
fn a_mount_report_pins_the_node_and_survives_a_reschedule() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.add_node(NODE_B)?;
    cluster.submit(stateful_job("db", 1, "local-dir", None))?;

    let volume = cluster.volumes()?.remove(0);
    cluster.report_mounted(&volume.volume_id, NODE_A, true)?;

    let pinned = cluster
        .store
        .get_volume(&volume.volume_id)?
        .context("volume")?;
    assert_eq!(pinned.state, VolumeState::Created);
    assert_eq!(pinned.node_id.as_deref(), Some(NODE_A));
    assert!(pinned.pins_placement(), "a mounted node-local volume pins");

    // The instance keeps the same handle across a reschedule — the whole point
    // of the identity/storage pairing.
    let before = cluster.store.list_instances("db")?[0]
        .storage_handle
        .clone();
    cluster.store.remove_allocs_for_node(NODE_A)?;
    cluster.reschedule("db")?;
    let after = cluster.store.list_instances("db")?[0]
        .storage_handle
        .clone();
    assert_eq!(before, after, "the replacement reattaches the same volume");
    assert_eq!(cluster.volumes()?.len(), 1, "no second volume was minted");
    Ok(())
}

#[test]
fn a_pinned_instance_awaiting_placement_reports_its_node() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 1, "local-dir", None))?;

    let volume = cluster.volumes()?.remove(0);
    cluster.report_mounted(&volume.volume_id, NODE_A, true)?;

    // With the allocation gone, the instance is awaiting placement and the
    // scheduler must be told it can only go back to NODE_A.
    cluster.store.remove_allocs_for_node(NODE_A)?;
    let task_id = cluster
        .store
        .get_task_configs_for_job_config(cluster.job_config_ids.get("db").context("job")?)?
        .first()
        .map(|(id, _)| *id)
        .context("task config")?;
    let pins = cluster.store.pinned_placements_for_task(task_id)?;
    assert_eq!(pins.len(), 1, "the awaiting instance is pinned");
    assert_eq!(pins[0].1, NODE_A);
    Ok(())
}

/// A dry-run plan of a job that was never submitted numbers its tasks 1, 2,
/// ... by position. Its volume pins used to be looked up by that number, so
/// the preview picked up whichever stored task had the same id: here `db`'s
/// pin to node-a, which a `web` task held to node-b could never take, and
/// the preview failed. Pins of an unsubmitted config are found by job and
/// task name instead, and a resubmit preview still sees its own.
#[test]
fn a_dry_run_plan_never_reads_another_jobs_volume_pins() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 1, "local-dir", None))?;
    let volume = cluster.volumes()?.remove(0);
    cluster.report_mounted(&volume.volume_id, NODE_A, true)?;
    cluster.store.remove_allocs_for_node(NODE_A)?;
    cluster.add_node(NODE_B)?;
    let db_task_id = cluster
        .store
        .get_task_configs_for_job_config(cluster.job_config_ids.get("db").context("job")?)?
        .first()
        .map(|(id, _)| *id)
        .context("task config")?;
    assert_eq!(
        db_task_id, 1,
        "the positional id of the preview's first task must collide with db's"
    );
    assert_eq!(
        cluster.store.pinned_placements_for_task(db_task_id)?.len(),
        1
    );

    let web = JobConfig {
        priority: Default::default(),
        name: "web".to_string(),
        tasks: vec![TaskConfig {
            config: json!({ "command": ["sleep", "60"] }),
            constraints: json!({ "node_id": NODE_B }),
            ..TaskConfig::new("web", "exec")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    };
    let scheduler = Scheduler::new(&cluster.store)?;
    let preview = scheduler.plan_for_config(&web, PlanMode::Fresh, None)?;
    assert_eq!(preview.allocations.len(), 1);
    assert_eq!(preview.allocations[0].node_id, NODE_B);

    // A resubmit preview of `db` itself (positional ids again) still honors
    // its own instance's pin.
    let mut db = stateful_job("db", 1, "local-dir", None);
    db.tasks[0].config = json!({ "command": ["sleep", "120"] });
    let preview = scheduler.plan_for_config(&db, PlanMode::Fresh, Some("db"))?;
    assert_eq!(preview.allocations.len(), 1);
    assert_eq!(preview.allocations[0].node_id, NODE_A);
    Ok(())
}

#[test]
fn network_volumes_never_pin() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job(
        "db",
        1,
        "nfs",
        Some(json!({ "server": "10.0.0.5", "export": "/exports/legion" })),
    ))?;

    let volume = cluster.volumes()?.remove(0);
    assert!(!volume.node_local, "nfs is advertised as network storage");
    // Even after mounting somewhere, an nfs volume must not constrain
    // placement — that is the difference between "comes back here" and
    // "comes back".
    cluster.report_mounted(&volume.volume_id, NODE_A, false)?;
    let mounted = cluster
        .store
        .get_volume(&volume.volume_id)?
        .context("volume")?;
    assert!(!mounted.pins_placement());

    let task_id = cluster
        .store
        .get_task_configs_for_job_config(cluster.job_config_ids.get("db").context("job")?)?
        .first()
        .map(|(id, _)| *id)
        .context("task config")?;
    cluster.store.remove_allocs_for_node(NODE_A)?;
    assert!(
        cluster
            .store
            .pinned_placements_for_task(task_id)?
            .is_empty(),
        "network volumes impose no placement constraint"
    );
    Ok(())
}

#[test]
fn scaling_in_reclaims_only_when_retention_is_off() -> R<()> {
    for (retain, expected) in [
        (true, VolumeState::Created),
        (false, VolumeState::Destroying),
    ] {
        let mut cluster = Cluster::new()?;
        cluster.add_node(NODE_A)?;
        let mut job = stateful_job("db", 2, "local-dir", None);
        job.tasks[0]
            .stateful
            .as_mut()
            .unwrap()
            .storage
            .as_mut()
            .unwrap()
            .retain_on_scale_in = Some(retain);
        cluster.submit(job.clone())?;

        for volume in cluster.volumes()? {
            cluster.report_mounted(&volume.volume_id, NODE_A, true)?;
        }

        // Scale 2 -> 1: slot 1's instance is terminated.
        let mut scaled = job.clone();
        scaled.tasks[0].count.num = 1;
        cluster.submit(scaled)?;
        cluster.store.reap_orphaned_volumes()?;

        let states: Vec<VolumeState> = cluster
            .volumes()?
            .iter()
            .map(|volume| volume.state)
            .collect();
        assert!(
            states.contains(&expected),
            "retainOnScaleIn: {retain} should leave a volume {expected:?}, got {states:?}"
        );
    }
    Ok(())
}

#[test]
fn a_destroyed_volume_is_only_reclaimed_after_the_driver_confirms() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 1, "local-dir", None))?;
    let volume = cluster.volumes()?.remove(0);
    cluster.report_mounted(&volume.volume_id, NODE_A, true)?;

    let deleted = cluster.store.delete_volume(&volume.volume_id, false)?;
    assert_eq!(
        deleted.state,
        VolumeState::Destroying,
        "an un-forced delete waits for the driver"
    );
    // Detaching the handle first means the instance mints a fresh volume next
    // placement instead of pointing at storage being reclaimed.
    assert!(cluster.store.list_instances("db")?[0]
        .storage_handle
        .is_none());

    // The manager keeps re-dispatching until a worker confirms.
    let pending = cluster.store.volumes_to_destroy_for_node(NODE_A)?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].volume_id, volume.volume_id);

    cluster.store.mark_volume_destroyed(&volume.volume_id)?;
    let done = cluster
        .store
        .get_volume(&volume.volume_id)?
        .context("volume")?;
    assert_eq!(done.state, VolumeState::Destroyed);
    assert!(done.node_id.is_none());
    assert!(
        cluster
            .store
            .volumes_to_destroy_for_node(NODE_A)?
            .is_empty(),
        "a confirmed destroy stops being re-dispatched"
    );
    Ok(())
}

#[test]
fn force_delete_tombstones_without_driver_confirmation() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 1, "local-dir", None))?;
    let volume = cluster.volumes()?.remove(0);
    cluster.report_mounted(&volume.volume_id, NODE_A, true)?;

    // The escape hatch for a node that is never coming back: the row is
    // settled immediately, so the instance stops being pinned to a dead node.
    let deleted = cluster.store.delete_volume(&volume.volume_id, true)?;
    assert_eq!(deleted.state, VolumeState::Destroyed);
    assert!(deleted.node_id.is_none());
    assert!(!deleted.pins_placement());
    assert!(
        cluster
            .store
            .volumes_to_destroy_for_node(NODE_A)?
            .is_empty(),
        "a forced delete asks no driver to do anything"
    );
    Ok(())
}

/// `legion volume list` answers "what storage exists": a destroyed volume
/// drops out once its reclaim is confirmed (spot check 2026-10-12, RUNBOOK
/// 12.12), and `--all` / `--state destroyed` still show the tombstone.
#[tokio::test(flavor = "multi_thread")]
async fn list_volumes_hides_destroyed_tombstones_unless_asked() -> R<()> {
    use legion::manager::server::ManagerDaemonService;
    use legion::structs::manager_server::Manager;
    use legion::structs::ListVolumesRequest;

    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 2, "local-dir", None))?;
    let volumes = cluster.volumes()?;
    assert_eq!(volumes.len(), 2);
    let gone = &volumes[0].volume_id;
    cluster.store.delete_volume(gone, true)?;

    let service = ManagerDaemonService::new(cluster.store.clone());
    let list = |request: ListVolumesRequest| {
        let service = &service;
        async move {
            let reply = service
                .list_volumes(tonic::Request::new(request))
                .await
                .map_err(|status| anyhow::anyhow!("{status}"))?
                .into_inner();
            R::Ok(
                reply
                    .volumes
                    .into_iter()
                    .map(|volume| volume.volume_id)
                    .collect::<Vec<_>>(),
            )
        }
    };

    let default = list(ListVolumesRequest {
        job_id: Some("db".into()),
        ..Default::default()
    })
    .await?;
    assert_eq!(default.len(), 1);
    assert!(
        !default.contains(gone),
        "the tombstone is hidden by default"
    );

    let all = list(ListVolumesRequest {
        job_id: Some("db".into()),
        include_destroyed: true,
        ..Default::default()
    })
    .await?;
    assert_eq!(all.len(), 2);

    let destroyed = list(ListVolumesRequest {
        state: Some("destroyed".into()),
        ..Default::default()
    })
    .await?;
    assert_eq!(destroyed, vec![gone.clone()]);
    Ok(())
}

#[test]
fn task_volume_claims_are_shared_and_outlive_instance_churn() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    let mut job = stateful_job("db", 2, "local-dir", None);
    job.tasks[0].volumes = vec![VolumeSpec {
        name: "models".into(),
        driver: Some("local-dir".into()),
        mode: None,
        params: None,
        path: Some("/models".into()),
        read_only: Some(true),
    }];
    cluster.submit(job)?;

    let claims: Vec<_> = cluster
        .volumes()?
        .into_iter()
        .filter(|volume| volume.instance_id.is_none())
        .collect();
    assert_eq!(
        claims.len(),
        1,
        "a claim is shared by the whole task, not one per replica"
    );
    assert_eq!(claims[0].name, "models");
    assert!(claims[0].read_only);

    // Instance churn must not reclaim a claim: it is storage Legion does not
    // own the lifecycle of.
    cluster.store.remove_allocs_for_node(NODE_A)?;
    cluster.store.reap_orphaned_volumes()?;
    let claim = cluster
        .store
        .get_volume(&claims[0].volume_id)?
        .context("claim")?;
    assert_eq!(claim.state, VolumeState::Pending);
    Ok(())
}

#[test]
fn stateless_jobs_create_no_volumes() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(JobConfig {
        priority: Default::default(),
        name: "web".into(),
        tasks: vec![TaskConfig {
            count: TaskCount { num: 2, per: None },
            config: json!({ "command": ["sleep", "60"] }),
            ..TaskConfig::new("web", "exec")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    })?;
    assert!(
        cluster.volumes()?.is_empty(),
        "jobs without storage are untouched by the volume system"
    );
    Ok(())
}

#[test]
fn delete_refuses_while_a_live_allocation_mounts_the_volume() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE_A)?;
    cluster.submit(stateful_job("db", 1, "local-dir", None))?;
    let volume = cluster.volumes()?.remove(0);
    cluster.report_mounted(&volume.volume_id, NODE_A, true)?;

    // The instance is bound to a live allocation, so an un-forced delete must
    // refuse: detaching the handle and reclaiming under a running database is
    // the one mistake the lifecycle must never allow.
    let refreshed = cluster
        .store
        .get_volume(&volume.volume_id)?
        .context("volume")?;
    let in_use = cluster.store.volume_in_use(&refreshed)?;
    assert!(
        in_use.is_some(),
        "a bound instance's volume must report in-use"
    );

    // After the workload is gone, delete proceeds.
    cluster.store.remove_allocs_for_node(NODE_A)?;
    let refreshed = cluster
        .store
        .get_volume(&volume.volume_id)?
        .context("volume")?;
    assert!(cluster.store.volume_in_use(&refreshed)?.is_none());
    let deleted = cluster.store.delete_volume(&volume.volume_id, false)?;
    assert_eq!(deleted.state, VolumeState::Destroying);
    Ok(())
}

#[test]
fn node_local_volume_sizes_respect_node_disk() -> R<()> {
    let mut cluster = Cluster::new()?;
    // 5 GiB free: fits one 4G volume, not two.
    cluster.add_node_with_disk(NODE_A, 5 * 1024 * 1024 * 1024)?;

    let err = cluster
        .submit(stateful_job(
            "db",
            2,
            "local-dir",
            Some(json!({ "size": "4G" })),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("free disk"),
        "second replica must fail disk fit, got: {err}"
    );
    Ok(())
}

#[test]
fn remote_volume_sizes_ignore_node_disk() -> R<()> {
    let mut cluster = Cluster::new()?;
    // 1 GiB free — far less than the declared size. An nfs volume's bytes live
    // on the NFS server, so node disk must not gate the placement.
    cluster.add_node_with_disk(NODE_A, 1024 * 1024 * 1024)?;

    cluster.submit(stateful_job(
        "db",
        2,
        "nfs",
        Some(json!({ "server": "10.0.0.5", "export": "/exports/legion", "size": "100G" })),
    ))?;
    assert_eq!(cluster.volumes()?.len(), 2, "both replicas placed");
    Ok(())
}
