use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
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
        .tempdir()
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
            "memory_total": "8388608",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: json!({ "worker": "pass" }).to_string(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
    };
    store.insert_node(node)?;
    Ok(())
}

fn run_request(jobspec: String, name: &str) -> RunJobRequest {
    RunJobRequest {
        parsed_jobspec: jobspec,
        submissions: vec![Submission {
            path: format!("{name}.jsonnet"),
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
                "kind": "exec",
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
                "kind": "exec",
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
                "kind": "exec",
                "count": 2,
                "config": { "command": ["true"], "version": 1 },
                "resources": { "memory": 64 }
            },
            "api": {
                "group": "api",
                "kind": "exec",
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
                "kind": "exec",
                "count": 2,
                "config": { "command": ["true"], "version": 2 },
                "resources": { "memory": 64 }
            },
            "api": {
                "group": "api",
                "kind": "exec",
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
                "kind": "exec",
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
                "kind": "exec",
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
