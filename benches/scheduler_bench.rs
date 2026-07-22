use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use std::time::Duration;
use tempfile::Builder;

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, JobConfig, NodeMsg, Submission, TaskConfig, TaskCount,
};

const SCALE_STEPS: &[usize] = &[10, 100, 1_000, 10_000, 100_000];
const JOB_COUNT: usize = 10;

fn store_path(prefix: &str) -> anyhow::Result<String> {
    let store_dir = Builder::new().prefix(prefix).tempdir()?.keep();
    Ok(store_dir
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Error parsing store path"))?
        .to_string())
}

fn setup_store(policy: &str) -> anyhow::Result<Store> {
    let store_config = ManagerConfigFull {
        store_path: store_path("khepri-scheduler-bench-")?,
        scheduling_policy: policy.into(),
        task_resource_defaults: Some(json!({
            "cpu": 0,
            "memory": 0,
            "allocations": 1
        })),
        ..Default::default()
    };

    let store = Store::start_storage(&store_config)?;
    store.reset_all_tables()?;
    Ok(store)
}

fn default_fingerprint() -> serde_json::Value {
    json!({
        "memory_total": "8388608",
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
        dynamic_attrs: json!({}).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
    }
}

fn insert_uniform_nodes(store: &Store, node_count: usize) -> anyhow::Result<()> {
    for idx in 0..node_count {
        let node = make_node_msg(&format!("node-{idx}"), json!({ "tier": "default" }));
        store.insert_node(node)?;
    }
    Ok(())
}

fn make_jobs(total_allocations: usize) -> Vec<JobConfig> {
    let per_job = std::cmp::max(1, total_allocations / JOB_COUNT);
    let mut jobs = Vec::with_capacity(JOB_COUNT);
    for job_idx in 0..JOB_COUNT {
        let task = TaskConfig {
            count: TaskCount::new(per_job as u32),
            ..TaskConfig::new(format!("task-{job_idx}"), "noop")
        };
        jobs.push(JobConfig {
            name: format!("bench-job-{job_idx}"),
            tasks: vec![task],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        });
    }
    jobs
}

fn submit_jobs(store: &Store, jobs: &[JobConfig]) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::with_capacity(jobs.len());
    let submission = Submission {
        path: "bench.jsonnet".into(),
        content: "{}".into(),
        is_primary: true,
    };
    for job in jobs {
        store.handle_job_submit(job, &[submission.clone()])?;
        ids.push(store.get_job_config(&job.name)?.id());
    }
    Ok(ids)
}

fn scheduler_speed_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler_speed");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(5));

    for policy in ["bin_pack", "spread"] {
        for &node_count in SCALE_STEPS {
            for &allocation_count in SCALE_STEPS {
                let bench_id = BenchmarkId::new(
                    policy,
                    format!("nodes_{node_count}_allocs_{allocation_count}"),
                );
                group.throughput(Throughput::Elements(allocation_count as u64));
                group.bench_function(bench_id, |b| {
                    b.iter_batched(
                        || {
                            let store = setup_store(policy).expect("store");
                            insert_uniform_nodes(&store, node_count).expect("nodes");
                            let jobs = make_jobs(allocation_count);
                            let job_ids = submit_jobs(&store, &jobs).expect("jobs");
                            (store, job_ids)
                        },
                        |(store, job_ids)| {
                            for job_config_id in job_ids {
                                let scheduler = Scheduler::new(&store).expect("scheduler");
                                let outcome = scheduler
                                    .plan_and_apply(&job_config_id, PlanMode::Fresh)
                                    .expect("plan");
                                assert!(!outcome.allocations.is_empty());
                            }
                        },
                        criterion::BatchSize::LargeInput,
                    );
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, scheduler_speed_benchmark);
criterion_main!(benches);
