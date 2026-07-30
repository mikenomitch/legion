//! Phase 1 of the late-binding plan (`internal_docs/plans/
//! late-binding-plan-apply-plan.md`): a plan-conflict loss is transient,
//! so an eval that exhausts its conflict-retry budget is REQUEUED with a
//! delay instead of terminally failed — no work is silently dropped.
//! Terminal failure remains for non-conflict errors.

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{NodeMsg, Submission};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn store_with_settings(settings: serde_json::Value) -> R<Store> {
    let dir = tempfile::Builder::new()
        .prefix("legion-requeue-")
        .tempdir()?
        .keep();
    let config = ManagerConfigFull {
        store_path: dir.to_str().context("store path utf-8")?.to_string(),
        scheduling_policy: "bin_pack".into(),
        scheduler_settings: Some(settings),
        task_resource_defaults: Some(json!({"cpu": 0, "memory": 0, "allocations": 1000})),
        ..Default::default()
    };
    let store = Store::start_storage(&config)?;
    store.reset_all_tables()?;
    Ok(store)
}

fn insert_node(store: &Store, id: &str) -> R<()> {
    store.insert_node(NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({}).to_string(),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } }
        })
        .to_string(),
        dynamic_attrs: json!({}).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
    })?;
    Ok(())
}

fn submit_single_alloc_job(store: &Store, name: &str) -> R<String> {
    let job = JobConfig {
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            resources: json!({ "memory": 2048 }),
            ..TaskConfig::new("sandbox", "noop")
        }],
        version: None,
    };
    store.handle_job_submit(
        &job,
        &[Submission {
            path: format!("{name}.jsonnet"),
            content: name.to_string(),
            is_primary: true,
        }],
    )?;
    Ok(store.get_job_config(name)?.id())
}

/// A contended burst with a retry budget of 1 must drop nothing: every
/// conflict loser requeues (visibly pending) and eventually places.
#[test]
fn conflict_exhaustion_requeues_and_everything_places() -> R<()> {
    let store = store_with_settings(json!({
        "plan_conflict_max_retries": 1,
        "plan_conflict_requeue_delay_ms": 100,
    }))?;

    // Capacity == demand: 16 single-alloc (2 GiB) jobs on 4 8-GiB nodes.
    const JOBS: usize = 16;
    for index in 0..4 {
        insert_node(&store, &format!("node-{index}"))?;
    }
    for index in 0..JOBS {
        let name = format!("requeue-job-{index}");
        let job_config_id = submit_single_alloc_job(&store, &name)?;
        store.enqueue_eval("job_submit", Some(&name), Some(&job_config_id), None, None)?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..8)
        .map(|worker_id| {
            let store = store.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || -> R<()> {
                while !stop.load(Ordering::Relaxed) {
                    match store.claim_eval()? {
                        Some(eval) => {
                            legion::manager::daemon::handle_claimed_eval(&store, worker_id, &eval)
                        }
                        None => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
                Ok(())
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut placed = 0;
    while Instant::now() < deadline {
        placed = store.bench_scalar("SELECT COUNT(*) FROM allocation;")?;
        if placed as usize == JOBS {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        worker.join().expect("worker panicked")?;
    }

    assert_eq!(placed as usize, JOBS, "every job must eventually place");
    let failed = store.bench_scalar("SELECT COUNT(*) FROM eval WHERE status = 'failed';")?;
    assert_eq!(
        failed, 0,
        "no eval may reach `failed` through the conflict path"
    );
    Ok(())
}

/// Non-conflict errors keep today's terminal behavior: a submit eval whose
/// planning genuinely errors (no nodes at all) fails, visibly.
#[test]
fn non_conflict_errors_still_fail_terminally() -> R<()> {
    let store = store_with_settings(json!({
        "plan_conflict_max_retries": 1,
        "plan_conflict_requeue_delay_ms": 100,
    }))?;

    // No nodes registered: planning bails with "No nodes available", which
    // is not a conflict and must not requeue.
    let job_config_id = submit_single_alloc_job(&store, "doomed-job")?;
    store.enqueue_eval(
        "job_submit",
        Some("doomed-job"),
        Some(&job_config_id),
        None,
        None,
    )?;

    let eval = store.claim_eval()?.context("one pending eval")?;
    legion::manager::daemon::handle_claimed_eval(&store, 0, &eval);

    let failed = store.bench_scalar("SELECT COUNT(*) FROM eval WHERE status = 'failed';")?;
    assert_eq!(failed, 1, "a real error must still fail the eval");
    let pending = store.bench_scalar("SELECT COUNT(*) FROM eval WHERE status = 'pending';")?;
    assert_eq!(pending, 0, "a real error must not requeue");
    Ok(())
}
