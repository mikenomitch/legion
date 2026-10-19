//! Phase C-2 coverage (plan §7 of
//! `docs/plans/durability-and-ops-execution-plan.md`): with
//! `OWNERSHIP_GUARD_ENFORCES` flipped on, EVERY replicated op's changeset
//! must touch only tables owned by its lane (plus commutative inserts) — a
//! violation fails the op before proposal in every topology.
//!
//! `local_disk` mode never captures, so the wider suite cannot exercise the
//! guard; this test is the coverage vehicle: it drives a representative op
//! from every store domain — jobs (submit/stop/destroy), KV (+locks),
//! rollouts (create/advance/prune), nodes (register/heartbeat/lost/drain/
//! remove), allocations (place/remove/plan-apply), instances (slot + keyed
//! lifecycle), statuses/health, ACL, saved queries, signing keys, manager
//! config, idempotency, events — through a single-node raft-mode store,
//! where every mutation runs the full capture → guard → propose → apply
//! path. Any single-writer-lane violation surfaces as an op error and fails
//! the test.

use std::collections::BTreeMap;

use legion::prelude::{json, Context, R};
use legion::storage::events::EventDraft;
use legion::storage::store::{Store, VolumeFilter};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig, VolumeSpec};
use legion::structs::workload_identity::SigningKeyRecord;
use legion::structs::{
    InstanceState, KvValueType, Node, NodeMsg, Submission, TaskStatusState, VolumeState,
};

fn raft_config(root: &std::path::Path) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-guard".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn raft_store(dir: &tempfile::TempDir) -> R<Store> {
    Store::start_storage(&raft_config(dir.path())?)
}

fn test_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn test_job(name: &str) -> JobConfig {
    let mut task = TaskConfig::new("svc", "noop");
    // A desired count so the reconcile/reschedule passes place replacements.
    task.count = legion::structs::job::TaskCount::new(1);
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![task],
        version: None,
    }
}

fn submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.ncl"),
        content: format!("job {name}"),
        is_primary: true,
    }
}

/// Drain the eval queue the way the daemon's eval worker would. Every step
/// runs through the guard: the C-2 composites enqueue their scheduler-lane
/// tails (node_lost_reschedule, node_drain, …) as their own scheduler-lane
/// eval ops (JC-41), the claim/complete are scheduler ops themselves, and
/// the handlers drive further replicated ops.
fn run_pending_evals(store: &Store) -> R<usize> {
    let mut processed = 0;
    while let Some(eval) = store.claim_eval()? {
        eval.handle(store)?;
        store.complete_eval(&eval.id, true)?;
        processed += 1;
    }
    Ok(processed)
}

/// Control-lane domains: KV entries + locks, saved state queries (including
/// a state-store write), signing keys, ACL policies/users/credential
/// pruning, manager config, node admin (dynamic attrs, cordon/uncordon),
/// idempotency records.
#[test]
fn control_domain_ops_pass_the_ownership_guard() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = raft_store(&dir)?;

    // KV entries.
    store.put_kv("guard/kv", "{\"a\":1}", KvValueType::Json, None, None)?;
    store.patch_kv(
        "guard/kv",
        r#"[{"op": "add", "path": "/b", "value": 2}]"#,
        None,
        None,
    )?;
    store.put_kv("guard/tmp", "x", KvValueType::String, None, None)?;
    store.delete_kv("guard/tmp", None, None)?;

    // KV locks.
    let lock = store.acquire_kv_lock("guard/kv", None, Some("guard-test"), 60, false)?;
    store.renew_kv_lock("guard/kv", &lock.lock_id, 60)?;
    store.get_kv_lock("guard/kv")?;
    store.release_kv_lock("guard/kv", &lock.lock_id)?;

    // Saved state queries + a state-store WRITE statement (query_state_store
    // is a control-lane op; its writes must stay inside control-owned
    // tables to pass the guard).
    store.upsert_saved_state_query("guard-query", "SELECT 1;")?;
    store.query_state_store(
        "UPDATE saved_state_query SET sql = 'SELECT 2;' WHERE name = 'guard-query';",
        true,
    )?;
    store.delete_saved_state_query("guard-query")?;

    // Signing keys.
    store.insert_signing_key(&SigningKeyRecord {
        kid: "guard-kid".into(),
        alg: "ES256".into(),
        private_key_pem: "-----BEGIN PRIVATE KEY-----\nguard\n-----END PRIVATE KEY-----".into(),
        public_jwk_x: "x".into(),
        public_jwk_y: "y".into(),
        created_at: 1,
        not_after: None,
        state: "active".into(),
    })?;
    store.update_signing_key_state("guard-kid", "retiring", Some(2))?;
    store.delete_signing_key("guard-kid")?;

    // ACL.
    store.upsert_acl_policy(
        "guard-policy",
        "guard",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
    )?;
    store.upsert_acl_user(
        "guard-user",
        Some("$argon2id$guard"),
        &["guard-policy".to_string()],
    )?;
    store.prune_expired_credentials()?;
    store.delete_acl_user("guard-user")?;
    store.delete_acl_policy("guard-policy")?;

    // Manager config + idempotency + events (commutative insert from the
    // status lane).
    store.upsert_manager_config("guard-manager", &ManagerConfigFull::default())?;
    store.idempotency_record("guard-idem", "guard_op", "done")?;
    store.emit_event(EventDraft::info("guard.test", "guard event"));

    // Node admin: register, dynamic attrs, cordon, uncordon.
    store.insert_node(NodeMsg::from(test_node("guard-node")))?;
    let mut diff = serde_json::Map::new();
    diff.insert("guard".to_string(), json!("yes"));
    store.update_node_dyamic("guard-node", &diff)?;
    store.set_node_availability("guard-node", "drained")?;
    store.set_node_availability("guard-node", "available")?;

    Ok(())
}

/// Job + scheduler + status domains: submit (control + status seed),
/// placement (direct, plan-submission CAS apply, and bulk plan apply),
/// task statuses, allocation health, rollout lifecycle, kv-failure
/// stamping, stop/destroy cascades, slot + keyed instances.
#[test]
fn job_scheduler_and_status_ops_pass_the_ownership_guard() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = raft_store(&dir)?;

    store.insert_node(NodeMsg::from(test_node("node-a")))?;
    store.insert_node(NodeMsg::from(test_node("node-b")))?;
    store.update_node_heartbeat("node-a".to_string())?;
    store.update_node_health_checks("node-a", "{\"worker\": \"pass\"}")?;

    // Submit: the control op plus the status-lane task_status seed must
    // both commit, and the seeded rows must be observable immediately
    // (pre-split behavior preserved).
    let outcome = store.handle_job_submit(&test_job("guard-job"), &[submission("guard-job")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks.first().context("job must have a task config")?;
    let statuses = store.task_statuses_for_job_config(&job_config_id)?;
    assert_eq!(
        statuses.get(&task_id),
        Some(&TaskStatusState::Pending),
        "submit must seed pending task_status rows synchronously"
    );

    // Placement: direct place, plan-submission epoch-CAS apply (the op
    // whose task_status writes moved to a status-lane follow-up), and the
    // bulk plan apply.
    let alloc = store.place_alloc("node-a", task_id)?;
    let epoch = store.allocation_epoch_for_job_config(&job_config_id)?;
    let versions = BTreeMap::from([
        (format!("job_config:{job_config_id}"), epoch),
        ("node:node-b".to_string(), 0),
    ]);
    let plan = store.insert_plan_submission(
        &job_config_id,
        "fresh",
        &[("node-b".to_string(), task_id)],
        &versions,
    )?;
    store.apply_plan_submission(&plan)?;
    let statuses = store.task_statuses_for_job_config(&job_config_id)?;
    assert_eq!(
        statuses.get(&task_id),
        Some(&TaskStatusState::Running),
        "plan apply must still drive lifecycle task_status transitions"
    );
    store.apply_allocation_plan(&[("node-a".to_string(), task_id)])?;

    // Worker-reported state.
    store.set_task_status(task_id, TaskStatusState::Running)?;
    store.update_task_statuses_for_allocations(std::slice::from_ref(&alloc))?;
    store.upsert_allocation_health_snapshots(
        "node-a",
        &[legion::structs::AllocationHealth {
            allocation_id: alloc.id,
            healthy: true,
            status: "healthy".into(),
            artifact_states: None,
        }],
    )?;

    // Rollout lifecycle (consolidated on the scheduler lane in C-2).
    let rollout = store.create_rollout(
        "guard-job",
        None,
        &job_config_id,
        "in_progress",
        Some("rolling"),
        None,
        None,
        None,
        "{}",
    )?;
    store.append_rollout_event(rollout.id, "guard-job", None, "info", "guard tick", None)?;
    store.update_rollout_config_json(rollout.id, "{\"guard\":true}")?;
    store.update_rollout_status(rollout.id, "completed", false, Some("done"))?;
    store.reconcile_incomplete_rollouts()?;
    store.prune_rollouts()?;

    // KV-dependency failure stamping (control job flip + scheduler
    // allocation stamps) and recovery.
    store.mark_job_failed_for_kv("guard-job", "kv gone", None)?;
    assert!(store.clear_job_failed_status("guard-job")?);

    // Instances: slot materialization + state flips, then the keyed
    // lifecycle.
    let slot = store.get_or_create_slot_instance("guard-job", "default", "svc", 0)?;
    store.set_instance_state(&slot.instance_id, InstanceState::Active)?;
    store.set_instance_current_alloc(&slot.instance_id, Some(alloc.id))?;
    store.upsert_instance(&slot)?;
    let keyed = store.create_keyed_instance("guard-job", "default", "svc", Some("svc-guard-1"))?;
    store.stop_keyed_instance(&keyed.instance_id)?;
    store.start_keyed_instance(&keyed.instance_id)?;
    store.stop_keyed_instance(&keyed.instance_id)?;
    store.destroy_keyed_instance(&keyed.instance_id)?;

    // Stop: job status flip (control), allocation removal + instance stop
    // (scheduler — what the job_stop eval runs).
    store.stop_job("guard-job")?;
    store.remove_allocs_for_job("guard-job")?;
    store.stop_instances_for_job("guard-job")?;

    // Destroy: the three-lane composite. Every lane's rows must be gone
    // afterwards — the cascade behavior, now explicit per lane.
    store.destroy_job("guard-job")?;
    assert!(store.get_job("guard-job").is_err(), "job row must be gone");
    assert!(store.allocs_for_job("guard-job")?.is_empty());
    assert!(store.list_rollouts(Some("guard-job"), false)?.is_empty());
    assert!(store
        .task_statuses_for_job_config(&job_config_id)?
        .is_empty());
    assert!(store.list_instances("guard-job")?.is_empty());

    // The status-lane orphan sweeps.
    store.prune_allocation_health_orphans()?;
    store.prune_node_health_orphans()?;

    Ok(())
}

/// Worker exit reports: a batch task's completion and a task failed for
/// good. Regression (AWS spot check, DAG stuck after its first step):
/// `complete_allocation` wrote `task_status` (status lane) from its
/// scheduler-lane op, the guard refused it, and on a raft cluster no batch
/// allocation ever left `placed`, so every `success` dependent waited
/// forever. Drives the whole DAG step: place `fetch`, report its exit,
/// and plan again, which must release `validate`.
#[test]
fn worker_exit_report_ops_pass_the_ownership_guard() -> R<()> {
    use legion::manager::scheduler::{PlanMode, Scheduler};
    use legion::structs::job::{TaskDependency, TaskDependencyCondition, TaskLifecycle};

    let dir = tempfile::tempdir()?;
    let store = raft_store(&dir)?;
    store.insert_node(NodeMsg::from(test_node("node-a")))?;

    let step = |name: &str| {
        let mut task = TaskConfig::new(name, "noop");
        task.lifecycle = TaskLifecycle::Batch;
        task.group = "pipeline".into();
        task
    };
    let mut validate = step("validate");
    validate.depends_on = vec![TaskDependency {
        task: "fetch".into(),
        condition: TaskDependencyCondition::Success,
    }];
    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "guard-dag".into(),
        tasks: vec![step("fetch"), validate],
        version: None,
    };
    let outcome = store.handle_job_submit(&job, &[submission("guard-dag")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let placed = Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
    assert_eq!(placed.held_tasks, vec!["validate".to_string()]);
    let fetch = placed
        .allocations
        .first()
        .context("fetch must be placed")?
        .clone();

    // The batch exit: scheduler-lane row update, status-lane task status,
    // scheduler-lane dependency eval.
    let completed = store
        .complete_allocation(fetch.id, true)?
        .context("fetch allocation exists")?;
    assert_eq!(completed.status, "complete");
    assert_eq!(
        store
            .get_allocation_by_id(fetch.id)?
            .context("completed row is kept")?
            .status,
        "complete"
    );
    assert_eq!(
        store
            .task_statuses_for_job_config(&job_config_id)?
            .get(&fetch.task_config_id),
        Some(&TaskStatusState::Success)
    );
    // A repeated report (the heartbeat retry) converges.
    store.complete_allocation(fetch.id, true)?;
    assert!(store.queue_dependency_progress(&completed)?);
    let released = Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
    assert!(released.held_tasks.is_empty(), "{:?}", released.held_tasks);
    assert_eq!(released.allocations.len(), 1, "validate is placed");

    // A failing exit, and the failure path's writes: the row removal
    // (scheduler), the job flip (control), the task status (status).
    let validate_alloc = released.allocations[0].clone();
    store.complete_allocation(validate_alloc.id, false)?;
    assert_eq!(
        store
            .task_statuses_for_job_config(&job_config_id)?
            .get(&validate_alloc.task_config_id),
        Some(&TaskStatusState::Failed)
    );
    store.remove_allocation_by_id(validate_alloc.id)?;
    store.mark_job_failed("guard-dag")?;
    store.set_task_status(validate_alloc.task_config_id, TaskStatusState::Failed)?;
    Ok(())
}

/// Node lifecycle: ready/lost marks (status + control composites), the
/// eval-mediated lost-node reschedule tail, drain eviction, new-node
/// reconcile, removal + orphan sweeps.
#[test]
fn node_lifecycle_ops_pass_the_ownership_guard() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = raft_store(&dir)?;

    store.insert_node(NodeMsg::from(test_node("life-a")))?;
    store.insert_node(NodeMsg::from(test_node("life-b")))?;

    let outcome = store.handle_job_submit(&test_job("life-job"), &[submission("life-job")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks.first().context("job must have a task config")?;
    store.place_alloc("life-a", task_id)?;

    // Lost-node flow: mark (status), availability flip (control), then the
    // reschedule tail via the node_lost_reschedule eval (scheduler).
    store.mark_nodes_as_lost(&["life-a".to_string()])?;
    assert_eq!(store.get_node("life-a")?.status, "lost");
    assert_eq!(store.get_node("life-a")?.availability, "unavailable");
    let processed = run_pending_evals(&store)?;
    assert!(processed >= 1, "the reschedule eval must have been queued");
    assert!(
        store.allocs_for_node("life-a")?.is_empty(),
        "stateless allocation must move off the lost node"
    );
    assert_eq!(store.allocs_for_node("life-b")?.len(), 1);

    // The recheck tick re-enqueues while the node stays lost.
    store.set_node_lost_at("life-a", "2020-01-01T00:00:00Z")?;
    store.recheck_lost_nodes();
    run_pending_evals(&store)?;

    // Recovery: ready mark + single lost mark round-trip.
    store.mark_node_ready("life-a", "2023-01-01T00:00:00Z")?;
    assert_eq!(store.get_node("life-a")?.availability, "available");
    store.mark_node_lost("life-a")?;
    store.update_node_heartbeat("life-a".to_string())?;
    // Restore schedulability so the drain eviction below has somewhere to
    // place replacements (a heartbeat alone restores liveness, not the
    // availability axis — that is the node_join eval's ready mark).
    store.mark_node_ready("life-a", "2023-01-01T00:00:00Z")?;

    // Drain flow (cordon + evict) and the new-node reconcile pass.
    store.set_node_availability("life-b", "drained")?;
    store.reschedule_allocations_for_drained_node("life-b")?;
    store.set_node_availability("life-b", "available")?;
    store.reschedule_allocations_for_new_node()?;

    // Removal: scheduler-lane allocation cleanup precedes the control-lane
    // node delete; the health orphans are swept by the status lane.
    store.remove_node("life-a")?;
    store.prune_node_health_orphans()?;
    store.prune_allocation_health_orphans()?;

    Ok(())
}

/// Volume domain (scheduler lane). `volume` is scheduler-owned — its
/// lifecycle is bound to the scheduler-owned `instance` and its state machine
/// is driven by placement and the reclaim reaper — so job submit, a CONTROL
/// op, must sync its tasks' claim rows as a SEPARATE scheduler-lane op: doing
/// it inside the control capture is exactly the violation the guard rejects,
/// and submit would fail outright in raft mode.
#[test]
fn volume_ops_pass_the_ownership_guard() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = raft_store(&dir)?;

    store.insert_node(NodeMsg::from(test_node("vol-a")))?;

    let mut job = test_job("vol-job");
    job.tasks[0].volumes = vec![VolumeSpec {
        name: "data".into(),
        driver: Some("local-dir".into()),
        mode: None,
        params: None,
        path: Some("/mnt/data".into()),
        read_only: None,
    }];

    // Submit: the control op, the status-lane task_status seed, and the
    // scheduler-lane claim sync must all commit. The claim row exists from
    // submit — before anything is placed — which is what `legion volume list`
    // shows a user who asked for storage.
    store.handle_job_submit(&job, &[submission("vol-job")])?;
    let claims = store.list_volumes(&VolumeFilter {
        job_id: Some("vol-job".to_string()),
        ..Default::default()
    })?;
    assert_eq!(claims.len(), 1, "submit must sync the task's volume claims");
    let volume_id = claims[0].volume_id.clone();
    assert_eq!(claims[0].state, VolumeState::Pending);

    // A resubmit of the identical config reports `Unchanged` and still runs
    // the scheduler half (JC-40 composite-retry idempotency), refreshing the
    // claim without disturbing it.
    store.handle_job_submit(&job, &[submission("vol-job")])?;
    let claims = store.list_volumes(&VolumeFilter {
        job_id: Some("vol-job".to_string()),
        ..Default::default()
    })?;
    assert_eq!(claims.len(), 1, "the claim id is stable across resubmits");

    // Worker volume reports (scheduler-lane ops driven from the worker plane).
    store.record_volume_mounted(
        &volume_id,
        "vol-a",
        "/mnt/data",
        true,
        None,
        &BTreeMap::from([("size".to_string(), json!("1G"))]),
    )?;
    assert_eq!(
        store.get_volume(&volume_id)?.context("volume")?.state,
        VolumeState::Created
    );
    store.record_volume_error(&volume_id, "driver hiccup")?;

    // The reclaim reaper: the claim's job still exists, so nothing is orphaned.
    assert!(store.reap_orphaned_volumes()?.is_empty());

    // Operator reclaim + the worker's destroy confirmation.
    let deleted = store.delete_volume(&volume_id, false)?;
    assert_eq!(deleted.state, VolumeState::Destroying);
    store.mark_volume_destroyed(&volume_id)?;
    assert_eq!(
        store.get_volume(&volume_id)?.context("volume")?.state,
        VolumeState::Destroyed
    );

    // Destroying the job leaves no live claim for the reaper to find.
    store.stop_job("vol-job")?;
    store.destroy_job("vol-job")?;
    assert!(store.reap_orphaned_volumes()?.is_empty());

    Ok(())
}
