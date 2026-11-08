//! The move budget of a keyed instance is its own
//! (docs/jobs-tasks-and-scheduling.md, "Manager-side move logic").
//!
//! Before, `onUnhealthy.moveCount` counted every move of the task inside
//! `movePeriod`, so with it set a few unrelated sandboxes of one keyed job
//! failing failed the whole job. Now each keyed instance counts its own
//! moves, a spent budget stops just that instance, and `instance start`
//! retries it with a fresh budget.

use legion::manager::daemon::resolve_claimed_eval;
use legion::prelude::{json, Context, R};
use legion::storage::store::{
    ClaimScope, EvalBatchLimits, FailedMove, MovePolicy, MoveVerdict, StartInstanceOutcome, Store,
};
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig};
use legion::structs::{InstanceMode, InstanceState, Node, NodeMsg, StopCause, Submission};

const JOB: &str = "agents";
const TASK: &str = "workspace";

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

fn node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "node_id": id, "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn keyed_job() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![TaskConfig {
            resources: json!({ "memory": 256, "cpu": 100 }),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new(TASK, "noop")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

fn run_eval_worker(store: &Store) -> R<()> {
    let mut batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
    }
    Ok(())
}

fn reschedule(store: &Store) -> R<()> {
    let config_id = store.get_job_config(JOB)?.id();
    store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    run_eval_worker(store)
}

fn current_alloc(store: &Store, id: &str) -> R<Option<i64>> {
    Ok(store
        .get_instance(id)?
        .context("instance")?
        .current_alloc_id)
}

/// Fail the instance's current allocation with a budget of one move per
/// hour, as a worker failure report does.
fn fail(store: &Store, id: &str) -> R<MoveVerdict> {
    let allocation_id = current_alloc(store, id)?.context("the instance is placed")?;
    let removal = store
        .remove_failed_allocation(
            allocation_id,
            StopCause::Failed,
            None,
            FailedMove {
                policy: MovePolicy {
                    move_delay_ms: 1_000,
                    move_count: Some(1),
                    move_period_ms: 3_600_000,
                },
                reason: "exited with status 1".into(),
            },
        )?
        .context("removed")?;
    Ok(removal.verdict)
}

/// The backoff is cut short (`instance start` on a rescheduling instance
/// lifts its hold and keeps its move history), and the replacement places.
fn replace(store: &Store, id: &str) -> R<()> {
    assert_eq!(
        store.start_keyed_instance(id)?,
        StartInstanceOutcome::BackoffLifted
    );
    reschedule(store)
}

#[test]
fn a_keyed_instance_spends_its_own_move_budget_test() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.path().to_str().context("store path")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    })?;
    store.insert_node(NodeMsg::from(node("n-1")))?;
    store.handle_job_submit(
        &keyed_job(),
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    run_eval_worker(&store)?;
    for id in ["user-1", "user-2"] {
        store.create_keyed_instance(JOB, "default", TASK, Some(id))?;
    }
    reschedule(&store)?;
    assert!(current_alloc(&store, "user-1")?.is_some());
    assert!(current_alloc(&store, "user-2")?.is_some());

    // user-1's first failure moves it; its second, inside the period,
    // spends its budget and stops just user-1.
    assert!(matches!(fail(&store, "user-1")?, MoveVerdict::Deferred(_)));
    replace(&store, "user-1")?;
    assert!(current_alloc(&store, "user-1")?.is_some(), "moved once");
    match fail(&store, "user-1")? {
        MoveVerdict::InstanceStopped {
            instance_id, moves, ..
        } => {
            assert_eq!(instance_id, "user-1");
            assert_eq!(moves, 1);
        }
        other => panic!("expected the instance to stop, got {other:?}"),
    }
    let stopped = store.get_instance("user-1")?.context("user-1")?;
    assert_eq!(stopped.state, InstanceState::Stopped);
    assert_eq!(stopped.current_alloc_id, None);
    let last_stop = store
        .instance_history("user-1")?
        .last_stop
        .context("last_stop")?;
    assert!(
        last_stop.hint.contains("legion instance start user-1"),
        "the way out is named: {}",
        last_stop.hint
    );
    reschedule(&store)?;
    assert_eq!(
        current_alloc(&store, "user-1")?,
        None,
        "a stopped instance is not placed again"
    );

    // The job, and user-2's budget, are untouched by user-1's failures.
    assert_ne!(store.get_job(JOB)?.status, "failed");
    assert!(current_alloc(&store, "user-2")?.is_some());
    assert!(
        matches!(fail(&store, "user-2")?, MoveVerdict::Deferred(_)),
        "user-2's first failure is a move, not a spent budget"
    );
    replace(&store, "user-2")?;
    assert!(current_alloc(&store, "user-2")?.is_some());

    // An explicit start is a fresh budget: the next failure moves it.
    assert_eq!(
        store.start_keyed_instance("user-1")?,
        StartInstanceOutcome::Started
    );
    reschedule(&store)?;
    assert!(current_alloc(&store, "user-1")?.is_some());
    assert!(matches!(fail(&store, "user-1")?, MoveVerdict::Deferred(_)));
    Ok(())
}
