use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, manager_server::Manager, AdvanceRolloutRequest, AllocationHealth,
    Eval, NodeMsg, RunJobRequest, Submission,
};
use tempfile::Builder;
use tokio::runtime::Runtime;
use tonic::Request;

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("legion-rollout-it-")
        .tempdir_in(crate::support::scratch_root())
        .context("error creating temp store directory")?
        .keep();
    let config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("error parsing store path")?
            .to_string(),
        ..Default::default()
    };
    let store = Store::start_storage(&config)?;
    Ok(store)
}

fn insert_node(store: &Store, id: &str) -> R<()> {
    let node = NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: json!({ "worker": "pass" }).to_string(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    };
    store.insert_node(node)?;
    Ok(())
}

fn run_request(jobspec: String, name: &str) -> RunJobRequest {
    RunJobRequest {
        namespace: Default::default(),
        parsed_jobspec: jobspec,
        submissions: vec![Submission {
            path: format!("{name}.ncl"),
            content: name.to_string(),
            is_primary: true,
        }],
        priority: None,
        force_reschedule: false,
        idempotency_key: None,
    }
}

fn process_next_eval(store: &Store) -> R<Option<Eval>> {
    let Some(eval) = store.get_next_eval()? else {
        return Ok(None);
    };
    let id = eval.id.clone();
    let kind = eval.kind.clone();
    match eval.handle(store) {
        Ok(_) => {
            store.set_eval_status(&id, "complete")?;
            Ok(Some(eval))
        }
        Err(err) => {
            store.set_eval_status(&id, "failed")?;
            Err(err).with_context(|| format!("eval {id} ({kind}) failed"))
        }
    }
}

fn mark_active_rollout_replacements_healthy(store: &Store, job_id: &str) -> R<()> {
    let active = store.list_rollouts(Some(job_id), true)?;
    for rollout in active {
        for allocation in store.allocs_for_job_config(&rollout.to_job_config_id)? {
            store.upsert_allocation_health_snapshots(
                &allocation.node_id,
                &[AllocationHealth {
                    allocation_id: allocation.id,
                    healthy: true,
                    status: "integration healthy".into(),
                    artifact_states: None,
                }],
            )?;
        }
    }
    Ok(())
}

fn drain_eval_queue_with_rollout_health(
    store: &Store,
    job_id: &str,
    max_iterations: usize,
) -> R<usize> {
    let mut processed = 0;
    for _ in 0..max_iterations {
        let Some(_eval) = process_next_eval(store)? else {
            break;
        };
        processed += 1;
        mark_active_rollout_replacements_healthy(store, job_id)?;
    }
    Ok(processed)
}

fn latest_rollout_for_job(store: &Store, job_id: &str) -> R<legion::structs::Rollout> {
    let job_config_id = store.get_job_config(job_id)?.id();
    store
        .latest_rollout_for_job_config(&job_config_id)?
        .with_context(|| format!("missing rollout for job {job_id}"))
}

#[test]
fn rollout_auto_progresses_through_percent_steps() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    insert_node(&store, "node-b")?;
    insert_node(&store, "node-c")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let v1 = json!({
        "name": "auto-rollout",
        "tasks": {
            "web": {
                "group": "web",
                "runtime": "exec",
                "count": 4,
                "config": { "command": ["true"], "version": 1 },
                "resources": { "memory": 64 }
            }
        }
    });
    runtime
        .block_on(manager.run_job(Request::new(run_request(v1.to_string(), "auto-rollout-v1"))))?;
    drain_eval_queue_with_rollout_health(&store, "auto-rollout", 20)?;

    let v2 = json!({
        "name": "auto-rollout",
        "tasks": {
            "web": {
                "group": "web",
                "runtime": "exec",
                "count": 4,
                "config": { "command": ["true"], "version": 2 },
                "resources": { "memory": 64 }
            }
        },
        "rollout": {
            "progression": "auto",
            "replacement": "createFirst",
            "percentSteps": [25, 50, 100],
            "allocationStagger": "0s",
            "minHealthyTime": "0s",
            "healthyDeadline": "30s"
        }
    });
    runtime
        .block_on(manager.run_job(Request::new(run_request(v2.to_string(), "auto-rollout-v2"))))?;
    let processed = drain_eval_queue_with_rollout_health(&store, "auto-rollout", 120)?;
    assert!(processed > 0, "expected rollout evals to run");

    let rollout = latest_rollout_for_job(&store, "auto-rollout")?;
    assert_eq!(rollout.status, "completed");
    assert!(!rollout.incomplete);
    let events = store.list_rollout_events(rollout.id, Some(2000))?;
    let advanced_steps = events
        .iter()
        .filter(|event| event.message.contains("advanced to step"))
        .count();
    assert!(
        advanced_steps >= 3,
        "expected multiple stage advances, got {advanced_steps}"
    );
    assert_eq!(
        store
            .allocs_for_job_config(&rollout.to_job_config_id)?
            .len(),
        4
    );
    let from_id = rollout
        .from_job_config_id
        .as_deref()
        .context("expected source config on rollout")?;
    assert!(store.allocs_for_job_config(from_id)?.is_empty());

    Ok(())
}

#[test]
fn rollout_manual_requires_explicit_stage_advances() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    insert_node(&store, "node-b")?;
    insert_node(&store, "node-c")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let v1 = json!({
        "name": "manual-rollout",
        "tasks": {
            "web": {
                "group": "web",
                "runtime": "exec",
                "count": 2,
                "config": { "command": ["true"], "version": 1 },
                "resources": { "memory": 64 }
            },
            "api": {
                "group": "api",
                "runtime": "exec",
                "count": 2,
                "config": { "command": ["true"], "version": 1 },
                "resources": { "memory": 64 }
            }
        }
    });
    runtime.block_on(manager.run_job(Request::new(run_request(
        v1.to_string(),
        "manual-rollout-v1",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, "manual-rollout", 20)?;

    let v2 = json!({
        "name": "manual-rollout",
        "tasks": {
            "web": {
                "group": "web",
                "runtime": "exec",
                "count": 2,
                "config": { "command": ["true"], "version": 2 },
                "resources": { "memory": 64 }
            },
            "api": {
                "group": "api",
                "runtime": "exec",
                "count": 2,
                "config": { "command": ["true"], "version": 2 },
                "resources": { "memory": 64 }
            }
        },
        "rollout": {
            "progression": "manual",
            "replacement": "createFirst",
            "countEachStep": 1,
            "allocationStagger": "0s",
            "minHealthyTime": "0s",
            "healthyDeadline": "30s"
        }
    });
    runtime.block_on(manager.run_job(Request::new(run_request(
        v2.to_string(),
        "manual-rollout-v2",
    ))))?;

    let mut rollout = latest_rollout_for_job(&store, "manual-rollout")?;
    assert_eq!(rollout.status, "waiting_manual_start");
    assert!(store.get_next_eval()?.is_none());

    runtime.block_on(manager.advance_rollout(Request::new(AdvanceRolloutRequest {
        id: rollout.id,
        groups: Vec::new(),
        priority: None,
    })))?;
    drain_eval_queue_with_rollout_health(&store, "manual-rollout", 120)?;
    rollout = store
        .get_rollout(rollout.id)?
        .context("rollout missing after first advance")?;
    assert_eq!(rollout.status, "waiting_manual_advance");

    runtime.block_on(manager.advance_rollout(Request::new(AdvanceRolloutRequest {
        id: rollout.id,
        groups: vec!["web".to_string()],
        priority: None,
    })))?;
    drain_eval_queue_with_rollout_health(&store, "manual-rollout", 120)?;
    rollout = store
        .get_rollout(rollout.id)?
        .context("rollout missing after group advance")?;
    assert_eq!(
        rollout.status, "waiting_manual_advance",
        "single-group advance should leave other groups waiting"
    );

    runtime.block_on(manager.advance_rollout(Request::new(AdvanceRolloutRequest {
        id: rollout.id,
        groups: vec!["api".to_string()],
        priority: None,
    })))?;
    drain_eval_queue_with_rollout_health(&store, "manual-rollout", 120)?;
    rollout = store
        .get_rollout(rollout.id)?
        .context("rollout missing after final advance")?;
    assert_eq!(rollout.status, "completed");

    let events = store.list_rollout_events(rollout.id, Some(2000))?;
    let manual_advance_events = events
        .iter()
        .filter(|event| event.message == "Manual rollout advance queued")
        .count();
    assert!(
        manual_advance_events >= 3,
        "expected queued events for each manual advance"
    );

    Ok(())
}

/// Process every currently-ready eval (delayed `Wait` ticks stay queued in
/// the future), WITHOUT marking anything healthy.
fn drain_ready_evals(store: &Store) -> R<usize> {
    let mut processed = 0;
    while process_next_eval(store)?.is_some() {
        processed += 1;
        assert!(processed < 200, "eval queue did not quiesce");
    }
    Ok(processed)
}

/// Re-trigger rollout processing for the job's latest config (the engine's
/// own next tick may be parked behind a health-wait delay).
fn enqueue_rollout_tick(store: &Store, job_id: &str) -> R<()> {
    let job_config_id = store.get_job_config(job_id)?.id();
    store.enqueue_eval(
        "rollout_tick",
        Some(job_id),
        Some(&job_config_id),
        None,
        None,
    )?;
    Ok(())
}

fn stateful_jobspec(name: &str, count: u32, version: u32, rollout: Option<Value>) -> String {
    let mut spec = json!({
        "name": name,
        "tasks": {
            "db": {
                "group": "data",
                "runtime": "exec",
                "count": count,
                "config": { "command": ["true"], "version": version },
                "resources": { "memory": 64 },
                "stateful": {
                    "mode": "slot",
                    "storage": { "path": "/var/lib/db" }
                }
            }
        }
    });
    if let Some(rollout) = rollout {
        spec["rollout"] = rollout;
    }
    spec.to_string()
}

/// Allocations of one job config keyed by instance id.
fn allocs_by_instance(
    store: &Store,
    job_config_id: &str,
) -> R<std::collections::HashMap<String, i64>> {
    Ok(store
        .allocs_for_job_config(job_config_id)?
        .into_iter()
        .filter_map(|alloc| alloc.instance_id.clone().map(|id| (id, alloc.id)))
        .collect())
}

#[test]
fn sequential_rollout_updates_one_slot_at_a_time_highest_first() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;
    let job = "seq-rollout";

    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(job, 3, 1, None),
        "seq-rollout-v1",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, job, 20)?;
    let v1_id = store.get_job_config(job)?.id();
    let v1_allocs = allocs_by_instance(&store, &v1_id)?;
    assert_eq!(v1_allocs.len(), 3);
    let handles_before: std::collections::HashMap<String, Option<String>> = store
        .list_instances(job)?
        .into_iter()
        .map(|instance| (instance.instance_id.clone(), instance.storage_handle))
        .collect();

    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(
            job,
            3,
            2,
            Some(json!({
                "slots": { "order": "sequential" },
                "allocationStagger": "0s",
                "minHealthyTime": "0s"
            })),
        ),
        "seq-rollout-v2",
    ))))?;
    let v2_id = store.get_job_config(job)?.id();
    assert_ne!(v1_id, v2_id);

    // Drive evals WITHOUT marking replacements healthy: the rollout must
    // replace exactly the highest slot (2) and then halt at the health gate.
    drain_ready_evals(&store)?;
    let v2_allocs = allocs_by_instance(&store, &v2_id)?;
    assert_eq!(
        v2_allocs.keys().collect::<Vec<_>>(),
        vec![&format!("{job}-data-2")],
        "only slot 2 may be replaced before its replacement is healthy"
    );
    let remaining_v1 = allocs_by_instance(&store, &v1_id)?;
    assert_eq!(remaining_v1.len(), 2);
    for slot in [0, 1] {
        let id = format!("{job}-data-{slot}");
        assert_eq!(
            remaining_v1.get(&id),
            v1_allocs.get(&id),
            "slot {slot}'s old allocation must be untouched while slot 2 is unhealthy"
        );
    }

    // An extra tick without health changes nothing: later slots are blocked.
    enqueue_rollout_tick(&store, job)?;
    drain_ready_evals(&store)?;
    assert_eq!(allocs_by_instance(&store, &v2_id)?.len(), 1);
    let rollout = latest_rollout_for_job(&store, job)?;
    assert_eq!(rollout.status, "running");
    let events = store.list_rollout_events(rollout.id, Some(2000))?;
    assert!(
        events.iter().any(|event| event
            .message
            .contains("waiting for slot 2 replacement to become healthy")),
        "halt must be legible via rollout events"
    );

    // Release the gate one slot at a time and observe the 2 -> 1 -> 0 order.
    for _ in 0..3 {
        mark_active_rollout_replacements_healthy(&store, job)?;
        enqueue_rollout_tick(&store, job)?;
        drain_ready_evals(&store)?;
    }
    let rollout = store
        .get_rollout(rollout.id)?
        .context("rollout missing after completion")?;
    assert_eq!(rollout.status, "completed");
    assert_eq!(allocs_by_instance(&store, &v2_id)?.len(), 3);
    assert!(store.allocs_for_job_config(&v1_id)?.is_empty());

    let events = store.list_rollout_events(rollout.id, Some(2000))?;
    let slot_updates: Vec<&str> = events
        .iter()
        .filter_map(|event| {
            ["slot 2 updated", "slot 1 updated", "slot 0 updated"]
                .into_iter()
                .find(|needle| event.message.contains(needle))
        })
        .collect();
    assert_eq!(
        slot_updates,
        vec!["slot 2 updated", "slot 1 updated", "slot 0 updated"],
        "slots must update highest first, one per step"
    );

    // Stateful invariant through the sequential rollout: every slot kept its
    // instance id and storage handle.
    for instance in store.list_instances(job)? {
        assert_eq!(
            handles_before.get(&instance.instance_id),
            Some(&instance.storage_handle),
            "storage handle must survive the sequential rollout"
        );
    }
    Ok(())
}

#[test]
fn update_from_stages_rollout_and_lowering_it_promotes_remaining_slots() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;
    let job = "staged-rollout";

    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(job, 4, 1, None),
        "staged-v1",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, job, 20)?;
    let v1_id = store.get_job_config(job)?.id();
    let v1_allocs = allocs_by_instance(&store, &v1_id)?;
    assert_eq!(v1_allocs.len(), 4);
    let handles_before: std::collections::HashMap<String, Option<String>> = store
        .list_instances(job)?
        .into_iter()
        .map(|instance| (instance.instance_id.clone(), instance.storage_handle))
        .collect();

    // Stage the update at slot 2: only slots 3 and 2 move to v2.
    let staged_rollout = json!({
        "slots": { "order": "sequential", "updateFrom": 2 },
        "allocationStagger": "0s",
        "minHealthyTime": "0s"
    });
    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(job, 4, 2, Some(staged_rollout)),
        "staged-v2",
    ))))?;
    let v2_id = store.get_job_config(job)?.id();
    for _ in 0..3 {
        mark_active_rollout_replacements_healthy(&store, job)?;
        enqueue_rollout_tick(&store, job)?;
        drain_ready_evals(&store)?;
    }

    let rollout = latest_rollout_for_job(&store, job)?;
    assert_eq!(rollout.status, "completed");
    let v2_allocs = allocs_by_instance(&store, &v2_id)?;
    let mut updated: Vec<&String> = v2_allocs.keys().collect();
    updated.sort();
    assert_eq!(
        updated,
        vec![&format!("{job}-data-2"), &format!("{job}-data-3")],
        "only slots >= updateFrom move to the new config"
    );
    let staged_v1 = allocs_by_instance(&store, &v1_id)?;
    for slot in [0, 1] {
        let id = format!("{job}-data-{slot}");
        assert_eq!(
            staged_v1.get(&id),
            v1_allocs.get(&id),
            "slot {slot} must keep its ORIGINAL v1 allocation untouched"
        );
    }

    // Promote: resubmit with updateFrom 0 (same task content). The remaining
    // slots must move to the new config, still ordered and health-gated.
    let promote_rollout = json!({
        "slots": { "order": "sequential", "updateFrom": 0 },
        "allocationStagger": "0s",
        "minHealthyTime": "0s"
    });
    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(job, 4, 2, Some(promote_rollout)),
        "staged-v3",
    ))))?;
    let v3_id = store.get_job_config(job)?.id();
    assert_ne!(v2_id, v3_id);
    for _ in 0..5 {
        mark_active_rollout_replacements_healthy(&store, job)?;
        enqueue_rollout_tick(&store, job)?;
        drain_ready_evals(&store)?;
    }

    let rollout = latest_rollout_for_job(&store, job)?;
    assert_eq!(rollout.status, "completed");
    assert_eq!(allocs_by_instance(&store, &v3_id)?.len(), 4);
    assert!(store.allocs_for_job_config(&v1_id)?.is_empty());
    assert!(store.allocs_for_job_config(&v2_id)?.is_empty());

    // Identity held across stage + promote: same ids, same handles.
    for instance in store.list_instances(job)? {
        assert_eq!(
            handles_before.get(&instance.instance_id),
            Some(&instance.storage_handle),
            "storage handle must survive the staged rollout and its promotion"
        );
    }
    Ok(())
}

#[test]
fn default_parallel_rollout_on_slot_group_is_unchanged() -> R<()> {
    // Regression: without a `slots` block a slot-mode group rolls exactly as
    // before (single full step, stop-then-start), and slot identity is
    // reused by the replacements.
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;
    let job = "parallel-stateful";

    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(job, 3, 1, None),
        "parallel-stateful-v1",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, job, 20)?;
    let v1_id = store.get_job_config(job)?.id();
    let handles_before: std::collections::HashMap<String, Option<String>> = store
        .list_instances(job)?
        .into_iter()
        .map(|instance| (instance.instance_id.clone(), instance.storage_handle))
        .collect();

    runtime.block_on(manager.run_job(Request::new(run_request(
        stateful_jobspec(job, 3, 2, Some(json!({ "minHealthyTime": "0s" }))),
        "parallel-stateful-v2",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, job, 120)?;

    let rollout = latest_rollout_for_job(&store, job)?;
    assert_eq!(rollout.status, "completed");
    let v2_id = store.get_job_config(job)?.id();
    assert_eq!(allocs_by_instance(&store, &v2_id)?.len(), 3);
    assert!(store.allocs_for_job_config(&v1_id)?.is_empty());
    for instance in store.list_instances(job)? {
        assert_eq!(
            handles_before.get(&instance.instance_id),
            Some(&instance.storage_handle)
        );
    }
    Ok(())
}

#[test]
fn rollout_destroy_first_progresses_through_multiple_stages() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    insert_node(&store, "node-b")?;
    insert_node(&store, "node-c")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let v1 = json!({
        "name": "destroy-first-rollout",
        "tasks": {
            "web": {
                "group": "web",
                "runtime": "exec",
                "count": 3,
                "config": { "command": ["true"], "version": 1 },
                "resources": { "memory": 64 }
            }
        }
    });
    runtime.block_on(manager.run_job(Request::new(run_request(
        v1.to_string(),
        "destroy-first-v1",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, "destroy-first-rollout", 20)?;

    let v2 = json!({
        "name": "destroy-first-rollout",
        "tasks": {
            "web": {
                "group": "web",
                "runtime": "exec",
                "count": 3,
                "config": { "command": ["true"], "version": 2 },
                "resources": { "memory": 64 }
            }
        },
        "rollout": {
            "progression": "auto",
            "replacement": "destroyFirst",
            "countSteps": [1, 1],
            "ignoreHealthChecks": true,
            "allocationStagger": "0s"
        }
    });
    runtime.block_on(manager.run_job(Request::new(run_request(
        v2.to_string(),
        "destroy-first-v2",
    ))))?;
    drain_eval_queue_with_rollout_health(&store, "destroy-first-rollout", 120)?;

    let rollout = latest_rollout_for_job(&store, "destroy-first-rollout")?;
    assert_eq!(rollout.status, "completed");
    let events = store.list_rollout_events(rollout.id, Some(2000))?;
    assert!(
        events.iter().any(|event| event.message.contains("retired")),
        "expected retire stage events"
    );
    assert!(
        events.iter().any(|event| event.message.contains("placed")),
        "expected placement stage events"
    );
    let advanced_steps = events
        .iter()
        .filter(|event| event.message.contains("advanced to step"))
        .count();
    assert!(
        advanced_steps >= 3,
        "expected destroy-first rollout to progress through multiple stages"
    );

    Ok(())
}

/// Spot-check P1-2: updating a single-replica static-port service on a
/// one-node cluster. A create-first rollout cannot surge (the new allocation
/// needs the port the old one holds), so it used to sit at "waiting for
/// placement capacity" forever with the service down. Now it stops the old
/// allocation, places the new one, and completes.
#[test]
fn static_port_service_rollout_replaces_instead_of_waiting_forever() -> R<()> {
    let store = setup_store()?;
    insert_node(&store, "node-a")?;
    let manager = ManagerDaemonService::new(store.clone());
    let runtime = Runtime::new()?;

    let spec = |version: u32| {
        json!({
            "name": "static-svc",
            "tasks": {
                "web": {
                    "group": "web",
                    "runtime": "exec",
                    "count": 1,
                    "config": { "command": ["true"], "version": version },
                    "resources": { "memory": 64 },
                    "network": { "ports": [ { "name": "http", "static": 18080 } ] }
                }
            },
            "rollout": {
                "replacement": "createFirst",
                "allocationStagger": "0s",
                "minHealthyTime": "0s"
            }
        })
        .to_string()
    };
    runtime.block_on(manager.run_job(Request::new(run_request(spec(1), "static-svc-v1"))))?;
    drain_eval_queue_with_rollout_health(&store, "static-svc", 20)?;
    let v1_id = store.get_job_config("static-svc")?.id();
    assert_eq!(store.allocs_for_job_config(&v1_id)?.len(), 1);

    runtime.block_on(manager.run_job(Request::new(run_request(spec(2), "static-svc-v2"))))?;
    drain_eval_queue_with_rollout_health(&store, "static-svc", 40)?;

    let rollout = latest_rollout_for_job(&store, "static-svc")?;
    assert_eq!(rollout.status, "completed", "message: {}", rollout.message);
    assert_eq!(
        store
            .allocs_for_job_config(&rollout.to_job_config_id)?
            .len(),
        1
    );
    assert!(store.allocs_for_job_config(&v1_id)?.is_empty());
    let events = store.list_rollout_events(rollout.id, Some(200))?;
    assert!(
        events
            .iter()
            .any(|event| event.message.contains("cannot surge")),
        "expected a destructive-update event, got {:?}",
        events
            .iter()
            .map(|event| &event.message)
            .collect::<Vec<_>>()
    );
    Ok(())
}
