use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use std::time::Duration;
use tempfile::{Builder, TempDir};

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, JobConfig, NodeMsg, Submission, TaskConfig, TaskCount,
};

const SCALE_STEPS: &[usize] = &[10, 100, 1_000, 10_000, 100_000];
const JOB_COUNT: usize = 10;

/// The `TempDir` guard rides alongside the path so the on-disk store is
/// deleted when the iteration's tuple drops. The previous `.keep()` leaked
/// one store directory per iteration — at the 100k-allocation cells that
/// filled the disk over a run and skewed late samples with filesystem
/// pressure.
fn store_path(prefix: &str) -> anyhow::Result<(String, TempDir)> {
    let store_dir = Builder::new().prefix(prefix).tempdir()?;
    let path = store_dir
        .path()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Error parsing store path"))?
        .to_string();
    Ok((path, store_dir))
}

fn setup_store(policy: &str) -> anyhow::Result<(Store, TempDir)> {
    setup_store_with_settings(policy, None)
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
        dynamic_attrs: json!({}).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        heartbeat_source: String::new(),
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
            priority: Default::default(),
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
                            let (store, dir) = setup_store(policy).expect("store");
                            insert_uniform_nodes(&store, node_count).expect("nodes");
                            let jobs = make_jobs(allocation_count);
                            let job_ids = submit_jobs(&store, &jobs).expect("jobs");
                            (store, dir, job_ids)
                        },
                        // Returning the store and its tempdir moves their
                        // drops (connection close + directory removal)
                        // outside the timed section.
                        |(store, dir, job_ids)| {
                            for job_config_id in job_ids {
                                let scheduler = Scheduler::new(&store).expect("scheduler");
                                let outcome = scheduler
                                    .plan_and_apply(&job_config_id, PlanMode::Fresh)
                                    .expect("plan");
                                assert!(!outcome.allocations.is_empty());
                            }
                            (store, dir)
                        },
                        criterion::BatchSize::LargeInput,
                    );
                });
            }
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Candidate construction (scheduling-index plan, Phase 0)
// ---------------------------------------------------------------------------
//
// Isolates the cost this feature targets: building each task's candidate set
// from constraints over a heterogeneous fleet. `indexes_off` pins the plain
// scan (`index_min_nodes: 0`); `indexes_on` arms a partition index over the
// same fleet. Constraint selectivity is 1/16 (one region of sixteen).

const CANDIDATE_NODE_STEPS: &[usize] = &[1_000, 5_000, 10_000];
const CANDIDATE_REGIONS: usize = 16;

fn insert_regional_nodes(store: &Store, node_count: usize) -> anyhow::Result<()> {
    for idx in 0..node_count {
        let attrs = json!({
            "region": format!("region-{}", idx % CANDIDATE_REGIONS),
            "zone": format!("zone-{}", idx % 4),
            "tier": "default",
        });
        let node = make_node_msg(&format!("node-{idx:06}"), attrs);
        store.insert_node(node)?;
    }
    Ok(())
}

/// One job, `task_count` independently-placed tasks (distinct groups so the
/// single-task candidate path runs per task — the default group would
/// co-place them), each constrained to one region of sixteen. Candidate
/// construction cost scales with tasks × nodes on the scan and with tasks ×
/// shard on the index, while the per-plan snapshot cost stays constant — so
/// more tasks isolates the cost this group exists to measure.
fn make_constrained_job(task_count: usize, per_task: u32) -> JobConfig {
    let tasks = (0..task_count)
        .map(|idx| TaskConfig {
            count: TaskCount::new(per_task),
            constraints: json!({
                "region": format!("region-{}", idx % CANDIDATE_REGIONS),
                "tier": "default",
            }),
            group: format!("group-{idx}"),
            ..TaskConfig::new(format!("task-{idx}"), "noop")
        })
        .collect();
    JobConfig {
        priority: Default::default(),
        name: "bench-candidates".into(),
        tasks,
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn candidate_construction_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("candidate_construction");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    for &node_count in CANDIDATE_NODE_STEPS {
        for (label, settings) in [
            ("indexes_off", json!({ "index_min_nodes": 0 })),
            (
                "indexes_on",
                json!({
                    "index_min_nodes": 1,
                    "indexes": { "topo": ["pool", "region"] },
                }),
            ),
        ] {
            let bench_id = BenchmarkId::new(label, format!("nodes_{node_count}"));
            group.bench_function(bench_id, |b| {
                b.iter_batched(
                    || {
                        let (store, dir) =
                            setup_store_with_settings("bin_pack", Some(settings.clone()))
                                .expect("store");
                        insert_regional_nodes(&store, node_count).expect("nodes");
                        let job = make_constrained_job(12, 2);
                        let ids = submit_jobs(&store, std::slice::from_ref(&job)).expect("job");
                        (store, dir, ids)
                    },
                    |(store, dir, job_ids)| {
                        let scheduler = Scheduler::new(&store).expect("scheduler");
                        for job_config_id in job_ids {
                            let plan = scheduler
                                .plan_only(&job_config_id, PlanMode::Fresh)
                                .expect("plan");
                            assert!(!plan.is_empty());
                        }
                        drop(scheduler);
                        (store, dir)
                    },
                    criterion::BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Planning under cluster fill
// ---------------------------------------------------------------------------
//
// The AWS `dense_fleet` ladder loses ~1.65x throughput per doubling of live
// allocations on a fixed 5,000-node fleet while the planned job stays the
// same size. Neither group above can reproduce that: `scheduler_speed`
// starts from an empty cluster and `candidate_construction` from an
// unallocated one, so any per-plan cost that scales with *other jobs'*
// allocations is invisible to both. This group pre-fills a 5,000-node fleet
// with 0/10/40 allocations per node from filler jobs, then plans a
// six-placement job (the AWS per-eval shape) against it — as a read-only
// plan (`plan_only`, the planning phases alone) and as a full
// plan-and-apply. A cost that is flat across the fill axis is a per-plan
// constant; one that grows with it is the slope the ladder measured.

const FILL_NODES: usize = 5_000;
const FILL_PER_NODE: &[usize] = &[0, 10, 40];
const FILL_JOBS: usize = 10;
const FILL_PLACEMENTS_PER_JOB: u32 = 6;
/// Placements per `apply_allocation_plan` call while filling. Chunked by
/// node range so every node's allocation list is reloaded into the read
/// model once, with all its rows, rather than once per allocation.
const FILL_CHUNK_NODES: usize = 125;

/// Fill every node with `per_node` allocations spread across `FILL_JOBS`
/// filler jobs, through the store's allocation writer rather than the
/// scheduler, so the fill is fast and independent of the code under test.
fn fill_fleet(store: &Store, node_count: usize, per_node: usize) -> anyhow::Result<()> {
    if per_node == 0 {
        return Ok(());
    }
    let mut filler_task_ids = Vec::with_capacity(FILL_JOBS);
    for job_idx in 0..FILL_JOBS {
        let job = JobConfig {
            priority: Default::default(),
            name: format!("filler-{job_idx}"),
            tasks: vec![TaskConfig {
                count: TaskCount::new((node_count * per_node / FILL_JOBS) as u32),
                ..TaskConfig::new("filler", "noop")
            }],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        let job_config_id = submit_jobs(store, std::slice::from_ref(&job))?
            .pop()
            .expect("one id per job");
        let tasks = store.cached_task_configs_for_job_config(&job_config_id)?;
        filler_task_ids.push(tasks[0].0);
    }

    let mut placements = Vec::with_capacity(FILL_CHUNK_NODES * per_node);
    for node_idx in 0..node_count {
        for slot in 0..per_node {
            let task_id = filler_task_ids[(node_idx * per_node + slot) % FILL_JOBS];
            placements.push((format!("node-{node_idx}"), task_id));
        }
        if (node_idx + 1) % FILL_CHUNK_NODES == 0 || node_idx + 1 == node_count {
            store.apply_allocation_plan(&placements)?;
            placements.clear();
        }
    }
    Ok(())
}

fn make_fill_job(name: &str) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(FILL_PLACEMENTS_PER_JOB),
            ..TaskConfig::new("svc", "noop")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn plan_under_fill_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_under_fill");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    for &per_node in FILL_PER_NODE {
        // One filled store per fill level, shared by both cells: the fill
        // is the expensive part of the setup and neither cell's measured
        // work depends on the other's.
        let (store, _dir) = setup_store("bin_pack").expect("store");
        insert_uniform_nodes(&store, FILL_NODES).expect("nodes");
        fill_fleet(&store, FILL_NODES, per_node).expect("fill");

        let planned_job = make_fill_job("planned");
        let planned_id = submit_jobs(&store, std::slice::from_ref(&planned_job))
            .expect("planned job")
            .pop()
            .expect("one id");
        group.bench_function(
            BenchmarkId::new("plan_only", format!("fill_{per_node}")),
            |b| {
                b.iter(|| {
                    let scheduler = Scheduler::new(&store).expect("scheduler");
                    let plan = scheduler
                        .plan_only(&planned_id, PlanMode::Fresh)
                        .expect("plan");
                    assert_eq!(plan.len(), FILL_PLACEMENTS_PER_JOB as usize);
                    plan
                })
            },
        );

        // A fresh job per iteration (submitted in the untimed setup), so
        // every apply is a first placement and the state drift across the
        // run is six allocations per iteration against the fill.
        let mut next_job = 0usize;
        group.bench_function(
            BenchmarkId::new("plan_and_apply", format!("fill_{per_node}")),
            |b| {
                b.iter_batched(
                    || {
                        next_job += 1;
                        let job = make_fill_job(&format!("applied-{next_job}"));
                        submit_jobs(&store, std::slice::from_ref(&job))
                            .expect("job")
                            .pop()
                            .expect("one id")
                    },
                    |job_config_id| {
                        let scheduler = Scheduler::new(&store).expect("scheduler");
                        let outcome = scheduler
                            .plan_and_apply(&job_config_id, PlanMode::Fresh)
                            .expect("plan");
                        assert_eq!(outcome.allocations.len(), FILL_PLACEMENTS_PER_JOB as usize);
                        outcome
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    scheduler_speed_benchmark,
    candidate_construction_benchmark,
    plan_under_fill_benchmark
);
criterion_main!(benches);
