use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::TaskConfig, job::TaskCount,
    ledger::LedgerEntry, Eval, Submission,
};
use tempfile::Builder;

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("khepri-eval-loop-")
        .tempdir_in(crate::support::scratch_root())
        .context("Error creating temp store directory")?
        .keep();

    let store_config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("Error parsing store path")?
            .to_string(),
        ..Default::default()
    };

    let store = Store::start_storage(&store_config)?;
    Ok(store)
}

fn make_job_config(name: &str, count: u32) -> JobConfig {
    let task = TaskConfig {
        count: TaskCount::new(count),
        config: json!({ "message": "hi" }),
        resources: json!({ "memory": 128 }),
        ..TaskConfig::new(format!("{name}-task"), "service")
    };

    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![task],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

fn primary_submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.json"),
        content: name.to_string(),
        is_primary: true,
    }
}

fn process_next_eval(store: &Store) -> R<Option<Eval>> {
    match store.get_next_eval()? {
        Some(eval) => {
            let result = eval.handle(store);
            match result {
                Ok(_) => store.set_eval_status(&eval.id, "complete")?,
                Err(_) => store.set_eval_status(&eval.id, "failed")?,
            }

            result.map(|_| Some(eval))
        }
        None => Ok(None),
    }
}

fn eval_status(store: &Store, id: &str) -> R<String> {
    let evals = store.get_evals()?;
    evals
        .into_iter()
        .find(|eval| eval.id == id)
        .map(|eval| eval.status)
        .context("missing eval")
}

fn find_node<'a>(ledger: &'a [LedgerEntry], node_id: &str) -> Option<&'a LedgerEntry> {
    ledger.iter().find(|entry| entry.node.id == node_id)
}

#[test]
fn eval_loop_reconciles_ledger_and_evals() -> R<()> {
    let store = setup_store()?;

    let job_name = "job-1";
    let job_config_v1 = make_job_config(job_name, 1);
    let submissions = vec![primary_submission(job_name)];
    store
        .handle_job_submit(&job_config_v1, &submissions)
        .context("first job submit")?;
    let job_config_v1_id = store.get_job_config(job_name)?.id();

    let job_config_v2 = make_job_config(job_name, 2);
    store
        .handle_job_submit(&job_config_v2, &submissions)
        .context("second job submit")?;
    let job_config_v2_id = store.get_job_config(job_name)?.id();

    let evals = vec![
        Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
            priority: 50,
        },
        Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-2".into(),
            priority: 50,
        },
        Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.into(),
            job_config_id: job_config_v1_id.clone(),
            node_name: String::new(),
            priority: 50,
        },
        Eval {
            id: "4".into(),
            status: "pending".into(),
            kind: "job_update".into(),
            job_id: job_name.into(),
            job_config_id: job_config_v2_id.clone(),
            node_name: String::new(),
            priority: 50,
        },
        Eval {
            id: "5".into(),
            status: "pending".into(),
            kind: "node_drop".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
            priority: 50,
        },
        Eval {
            id: "6".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_name.into(),
            job_config_id: job_config_v2_id.clone(),
            node_name: String::new(),
            priority: 50,
        },
    ];

    for eval in evals {
        store.insert_eval(&eval)?;
    }

    process_next_eval(&store)?.context("expected node join eval")?;
    let mut ledger = store.get_ledger()?;
    assert_eq!(ledger.len(), 1);
    let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
    assert!(node_one_entry.allocations.is_empty());
    assert_eq!(eval_status(&store, "1")?, "complete");

    process_next_eval(&store)?.context("expected second node join eval")?;
    ledger = store.get_ledger()?;
    assert_eq!(ledger.len(), 2);
    let node_two_entry = find_node(&ledger, "node-2").context("missing node-2")?;
    assert!(node_two_entry.allocations.is_empty());
    assert_eq!(eval_status(&store, "2")?, "complete");

    process_next_eval(&store)?.context("expected job submit eval")?;
    ledger = store.get_ledger()?;
    let job_one_allocations: Vec<_> = ledger
        .iter()
        .flat_map(|entry| entry.allocations.iter())
        .filter(|alloc| alloc.job_config_id == job_config_v1_id)
        .collect();
    // v2 was submitted before this v1 eval ran, so v1 is superseded and
    // has nothing running: a stale submit for it places nothing.
    assert!(job_one_allocations.is_empty());
    assert_eq!(eval_status(&store, "3")?, "complete");

    process_next_eval(&store)?.context("expected job update eval")?;
    ledger = store.get_ledger()?;
    let updated_allocations: Vec<_> = ledger
        .iter()
        .flat_map(|entry| entry.allocations.iter())
        .filter(|alloc| alloc.job_config_id == job_config_v2_id)
        .collect();
    assert_eq!(updated_allocations.len(), 2);
    assert_eq!(eval_status(&store, "4")?, "complete");

    process_next_eval(&store)?.context("expected node drop eval")?;
    ledger = store.get_ledger()?;
    let node_one_entry = find_node(&ledger, "node-1").context("missing node-1 after drop")?;
    assert_eq!(node_one_entry.allocations.len(), 0);
    let node_two_entry = find_node(&ledger, "node-2").context("missing node-2 after drop")?;
    assert_eq!(node_two_entry.allocations.len(), 2);
    assert_eq!(eval_status(&store, "5")?, "complete");

    process_next_eval(&store)?.context("expected job stop eval")?;
    ledger = store.get_ledger()?;
    let total_allocations: usize = ledger.iter().map(|entry| entry.allocations.len()).sum();
    assert_eq!(total_allocations, 0);
    assert_eq!(eval_status(&store, "6")?, "complete");

    assert!(process_next_eval(&store)?.is_none());

    Ok(())
}

#[test]
fn higher_priority_evals_are_processed_first() -> R<()> {
    let store = setup_store()?;

    let evals = vec![
        Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "low-node".into(),
            priority: 10,
        },
        Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "high-node".into(),
            priority: 90,
        },
    ];

    for eval in evals {
        store.insert_eval(&eval)?;
    }

    let first_eval = store.get_next_eval()?.context("expected a pending eval")?;
    assert_eq!(first_eval.id, "2");
    store.set_eval_status(&first_eval.id, "complete")?;

    let second_eval = store.get_next_eval()?.context("expected remaining eval")?;
    assert_eq!(second_eval.id, "1");

    Ok(())
}

// ---------------------------------------------------------------------------
// DAG dependencies, reschedule avoidance, and job status (spot-check P0-4,
// P1-3, P1-8). These drive the real eval handlers against a store, standing
// in for the worker by reporting task exits the way a heartbeat does.
// ---------------------------------------------------------------------------

use legion::structs::job::{TaskDependency, TaskDependencyCondition, TaskLifecycle};
use legion::structs::TaskStatusState;

/// Run every claimable eval, returning each one's kind and outcome.
fn drain_evals(store: &Store) -> R<Vec<(String, Result<String, String>)>> {
    let mut outcomes = Vec::new();
    while let Some(eval) = store.get_next_eval()? {
        let result = eval.handle(store);
        store.set_eval_status(&eval.id, if result.is_ok() { "complete" } else { "failed" })?;
        outcomes.push((
            eval.kind.clone(),
            result.map_err(|error| format!("{error:#}")),
        ));
    }
    Ok(outcomes)
}

/// Register a worker that reports task exits (protocol 2+), the kind whose
/// batch tasks stay `running` until their exit is reported.
fn add_node(store: &Store, node: &str) -> R<()> {
    store.insert_node(legion::structs::NodeMsg {
        id: node.to_string(),
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
        dynamic_attrs: json!({ "protocolVersion": 2 }).to_string(),
        health_checks: json!({ "worker": "pass" }).to_string(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;
    Ok(())
}

fn batch_step(name: &str, after: Option<&str>) -> TaskConfig {
    TaskConfig {
        lifecycle: TaskLifecycle::Batch,
        group: name.to_string(),
        resources: json!({ "memory": 64 }),
        depends_on: after
            .map(|dependency| {
                vec![TaskDependency {
                    task: dependency.to_string(),
                    condition: TaskDependencyCondition::Success,
                }]
            })
            .unwrap_or_default(),
        ..TaskConfig::new(name, "noop")
    }
}

fn submit(store: &Store, job: &JobConfig) -> R<String> {
    store.handle_job_submit(job, &[primary_submission(&job.name)])?;
    let job_config_id = store.get_job_config(&job.name)?.id();
    store.enqueue_eval(
        "job_submit",
        Some(&job.name),
        Some(&job_config_id),
        None,
        None,
    )?;
    Ok(job_config_id)
}

/// The allocations of `task` in the job, by task name.
fn allocations_of(store: &Store, job: &str, task: &str) -> R<Vec<legion::structs::Allocation>> {
    let config_id = store.get_job_config(job)?.id();
    let task_id = store
        .get_task_configs_for_job_config(&config_id)?
        .into_iter()
        .find(|(_, config)| config.name == task)
        .map(|(id, _)| id)
        .context("task exists")?;
    Ok(store
        .allocs_for_job(job)?
        .into_iter()
        .filter(|allocation| allocation.task_config_id == task_id)
        .collect())
}

/// What a worker's terminal report does on the manager: mark the allocation
/// complete or failed, then queue the eval that places its dependents.
fn report_exit(store: &Store, allocation_id: i64, success: bool) -> R<()> {
    let allocation = store
        .complete_allocation(allocation_id, success)?
        .context("allocation exists")?;
    store.queue_dependency_progress(&allocation)?;
    store.request_capacity_reconcile_if_waiting();
    Ok(())
}

fn task_status(store: &Store, job: &str, task: &str) -> R<TaskStatusState> {
    let config_id = store.get_job_config(job)?.id();
    let task_id = store
        .get_task_configs_for_job_config(&config_id)?
        .into_iter()
        .find(|(_, config)| config.name == task)
        .map(|(id, _)| id)
        .context("task exists")?;
    store
        .task_statuses_for_job_config(&config_id)?
        .get(&task_id)
        .copied()
        .context("task status row")
}

fn dag_job(name: &str) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![
            batch_step("fetch", None),
            batch_step("validate", Some("fetch")),
            TaskConfig {
                lifecycle: TaskLifecycle::Service,
                group: "serve".into(),
                resources: json!({ "memory": 64 }),
                depends_on: vec![TaskDependency {
                    task: "validate".into(),
                    condition: TaskDependencyCondition::Success,
                }],
                ..TaskConfig::new("serve", "noop")
            },
        ],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

/// A three-step chain runs one step at a time: step 2 is not placed until
/// step 1 has reported a successful exit, and step 3 not until step 2 has.
#[test]
fn dag_steps_wait_for_their_dependency_to_succeed() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    submit(&store, &dag_job("pipeline"))?;

    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "pipeline", "fetch")?.len(), 1);
    assert!(allocations_of(&store, "pipeline", "validate")?.is_empty());
    assert!(allocations_of(&store, "pipeline", "serve")?.is_empty());
    // Placed is not finished: re-evaluating changes nothing.
    store.enqueue_eval(
        "job_reschedule",
        Some("pipeline"),
        Some(&store.get_job_config("pipeline")?.id()),
        None,
        None,
    )?;
    drain_evals(&store)?;
    assert!(allocations_of(&store, "pipeline", "validate")?.is_empty());
    assert_eq!(store.get_job_with_status("pipeline")?.status, "running");

    let fetch = allocations_of(&store, "pipeline", "fetch")?[0].id;
    report_exit(&store, fetch, true)?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "pipeline", "validate")?.len(), 1);
    assert!(allocations_of(&store, "pipeline", "serve")?.is_empty());

    let validate = allocations_of(&store, "pipeline", "validate")?[0].id;
    report_exit(&store, validate, true)?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "pipeline", "serve")?.len(), 1);
    assert_eq!(
        task_status(&store, "pipeline", "validate")?,
        TaskStatusState::Success
    );
    Ok(())
}

/// A step that failed never lets its `success` dependent start (the spot
/// check saw `serve` start although `report` had failed).
#[test]
fn a_failed_step_blocks_its_success_dependent() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    submit(&store, &dag_job("broken"))?;
    drain_evals(&store)?;

    let fetch = allocations_of(&store, "broken", "fetch")?[0].id;
    report_exit(&store, fetch, true)?;
    drain_evals(&store)?;
    let validate = allocations_of(&store, "broken", "validate")?[0].id;
    report_exit(&store, validate, false)?;
    drain_evals(&store)?;

    assert_eq!(
        task_status(&store, "broken", "validate")?,
        TaskStatusState::Failed
    );
    assert!(
        allocations_of(&store, "broken", "serve")?.is_empty(),
        "serve must not start after validate failed"
    );
    // Nothing is live, and a task failed for good.
    assert_eq!(store.get_job_with_status("broken")?.status, "failed");
    Ok(())
}

/// A batch task with several replicas is done only when all of them are.
#[test]
fn a_multi_replica_step_succeeds_only_when_every_replica_has() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let mut job = dag_job("fanout");
    job.tasks[0].count = TaskCount::new(2);
    submit(&store, &job)?;
    drain_evals(&store)?;

    let fetches = allocations_of(&store, "fanout", "fetch")?;
    assert_eq!(fetches.len(), 2);
    report_exit(&store, fetches[0].id, true)?;
    drain_evals(&store)?;
    assert_eq!(
        task_status(&store, "fanout", "fetch")?,
        TaskStatusState::Running
    );
    assert!(allocations_of(&store, "fanout", "validate")?.is_empty());

    report_exit(&store, fetches[1].id, true)?;
    drain_evals(&store)?;
    assert_eq!(
        task_status(&store, "fanout", "fetch")?,
        TaskStatusState::Success
    );
    assert_eq!(allocations_of(&store, "fanout", "validate")?.len(), 1);
    Ok(())
}

/// A batch-only job whose steps all succeeded reads `complete`, not
/// `done`; a restart of it runs the chain from the top again.
#[test]
fn a_finished_batch_job_reads_complete_and_reruns_from_the_top() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let job = JobConfig {
        tasks: vec![
            batch_step("fetch", None),
            batch_step("validate", Some("fetch")),
        ],
        ..dag_job("nightly")
    };
    let job_config_id = submit(&store, &job)?;
    drain_evals(&store)?;
    report_exit(
        &store,
        allocations_of(&store, "nightly", "fetch")?[0].id,
        true,
    )?;
    drain_evals(&store)?;
    report_exit(
        &store,
        allocations_of(&store, "nightly", "validate")?[0].id,
        true,
    )?;
    drain_evals(&store)?;
    assert_eq!(store.get_job_with_status("nightly")?.status, "complete");

    // The next run (a schedule tick, a forced reschedule) starts at fetch.
    store.enqueue_eval(
        "job_update",
        Some("nightly"),
        Some(&job_config_id),
        None,
        None,
    )?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "nightly", "fetch")?.len(), 1);
    assert!(
        allocations_of(&store, "nightly", "validate")?.is_empty(),
        "the previous run's success must not satisfy this run"
    );
    Ok(())
}

/// A job nothing could be placed for reads `pending`, not `done`.
#[test]
fn a_job_with_nothing_placed_reads_pending() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let mut job = make_job_config("too-big", 1);
    job.tasks[0].resources = json!({ "memory": 999_999 });
    submit(&store, &job)?;
    let outcomes = drain_evals(&store)?;
    assert!(outcomes
        .iter()
        .any(|(kind, result)| kind == "job_submit" && result.is_err()));
    assert_eq!(store.get_job_with_status("too-big")?.status, "pending");

    let mut fits = make_job_config("fits", 1);
    fits.tasks[0].resources = json!({ "memory": 64 });
    submit(&store, &fits)?;
    drain_evals(&store)?;
    let statuses: std::collections::HashMap<String, String> = store
        .get_jobs_with_status()?
        .into_iter()
        .map(|job| (job.name, job.status))
        .collect();
    assert_eq!(statuses["fits"], "running");
    assert_eq!(statuses["too-big"], "pending");
    Ok(())
}

/// A reschedule after a failure avoids the failed node when another fits,
/// and uses it when it is the only node (it used to fail with "No nodes
/// available to place allocations" and leave the job dead).
#[test]
fn a_reschedule_uses_the_failed_node_when_it_is_the_only_option() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let mut job = make_job_config("solo", 1);
    job.tasks[0].resources = json!({ "memory": 64 });
    let job_config_id = submit(&store, &job)?;
    drain_evals(&store)?;
    let placed = store.allocs_for_job("solo")?;
    assert_eq!(placed.len(), 1);

    // What a failed-allocation report does: drop the row, reschedule
    // away from the node it failed on.
    store.remove_allocation_by_id(placed[0].id)?;
    store.enqueue_eval(
        "job_reschedule",
        Some("solo"),
        Some(&job_config_id),
        Some("node-1"),
        None,
    )?;
    let outcomes = drain_evals(&store)?;
    assert!(
        outcomes.iter().all(|(_, result)| result.is_ok()),
        "{outcomes:?}"
    );
    let replaced = store.allocs_for_job("solo")?;
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].node_id, "node-1");

    // With a second node, the failed one is avoided.
    add_node(&store, "node-2")?;
    store.remove_allocation_by_id(replaced[0].id)?;
    store.enqueue_eval(
        "job_reschedule",
        Some("solo"),
        Some(&job_config_id),
        Some("node-1"),
        None,
    )?;
    drain_evals(&store)?;
    let moved = store.allocs_for_job("solo")?;
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].node_id, "node-2");
    Ok(())
}

/// A scheduled DAG keeps ONE next-run timer. Every step's exit queues a
/// `job_reschedule`, and each placing eval schedules the batch follow-up; if
/// each added its own delayed `job_update`, the job would re-run once per step
/// at the next tick.
#[test]
fn a_scheduled_dag_keeps_one_next_run_timer() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let mut fetch = batch_step("fetch", None);
    fetch.schedule = Some(legion::structs::job::TaskSchedule::Every(
        std::time::Duration::from_secs(3600),
    ));
    let job = JobConfig {
        tasks: vec![fetch, batch_step("validate", Some("fetch"))],
        ..dag_job("hourly")
    };
    let job_config_id = submit(&store, &job)?;
    drain_evals(&store)?;
    report_exit(
        &store,
        allocations_of(&store, "hourly", "fetch")?[0].id,
        true,
    )?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "hourly", "validate")?.len(), 1);

    let timers = store
        .get_evals()?
        .into_iter()
        .filter(|eval| {
            eval.kind == "job_update"
                && eval.job_config_id == job_config_id
                && eval.status == "pending"
        })
        .count();
    assert_eq!(timers, 1);
    Ok(())
}

/// A service task needing `memory` MiB, `count` replicas.
fn service(name: &str, count: u32, memory: u64) -> TaskConfig {
    TaskConfig {
        count: TaskCount::new(count),
        group: name.to_string(),
        resources: json!({ "memory": memory }),
        ..TaskConfig::new(name, "noop")
    }
}

fn job_of(name: &str, tasks: Vec<TaskConfig>) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks,
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

/// A finished step stops holding the node's resources. On one 8 GiB node,
/// each step of a three-step DAG needs 6 GiB, so a step can run only once
/// the one before it has finished and released its share. Before, finished
/// batch allocations kept their resources until the job stopped and the
/// second step never placed.
#[test]
fn a_dag_whose_steps_each_need_most_of_the_node_runs_every_step() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let mut job = dag_job("heavy");
    for task in &mut job.tasks {
        task.resources = json!({ "memory": 6144 });
    }
    submit(&store, &job)?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "heavy", "fetch")?.len(), 1);

    report_exit(
        &store,
        allocations_of(&store, "heavy", "fetch")?[0].id,
        true,
    )?;
    let outcomes = drain_evals(&store)?;
    assert!(
        outcomes.iter().all(|(_, result)| result.is_ok()),
        "{outcomes:?}"
    );
    assert_eq!(allocations_of(&store, "heavy", "validate")?.len(), 1);

    report_exit(
        &store,
        allocations_of(&store, "heavy", "validate")?[0].id,
        true,
    )?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "heavy", "serve")?.len(), 1);
    Ok(())
}

/// Minimal blocked evals: a replica left unplaced for lack of room is placed
/// as soon as room frees up, here by another job stopping, with no node
/// joining.
#[test]
fn an_unplaced_replica_is_placed_when_another_job_stops() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let hog = submit(&store, &job_of("hog", vec![service("hog", 1, 4096)]))?;
    drain_evals(&store)?;
    submit(&store, &job_of("web", vec![service("web", 2, 3072)]))?;
    drain_evals(&store)?;
    assert_eq!(
        allocations_of(&store, "web", "web")?.len(),
        1,
        "one fits beside hog"
    );

    store.stop_job("hog")?;
    store.enqueue_eval("job_stop", Some("hog"), Some(&hog), None, None)?;
    let outcomes = drain_evals(&store)?;
    assert!(
        outcomes.iter().any(|(kind, _)| kind == "node_join"),
        "stopping hog queues a capacity pass: {outcomes:?}"
    );
    assert_eq!(allocations_of(&store, "web", "web")?.len(), 2);
    Ok(())
}

/// The same when the room is freed by a batch task finishing.
#[test]
fn an_unplaced_replica_is_placed_when_a_batch_task_finishes() -> R<()> {
    let store = setup_store()?;
    add_node(&store, "node-1")?;
    let mut crunch = batch_step("crunch", None);
    crunch.resources = json!({ "memory": 4096 });
    submit(&store, &job_of("crunch", vec![crunch]))?;
    drain_evals(&store)?;
    submit(&store, &job_of("web", vec![service("web", 2, 3072)]))?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "web", "web")?.len(), 1);

    report_exit(
        &store,
        allocations_of(&store, "crunch", "crunch")?[0].id,
        true,
    )?;
    drain_evals(&store)?;
    assert_eq!(allocations_of(&store, "web", "web")?.len(), 2);
    // Nobody waits any more: the next freed capacity queues nothing.
    store.request_capacity_reconcile_if_waiting();
    assert!(drain_evals(&store)?.is_empty());
    Ok(())
}
