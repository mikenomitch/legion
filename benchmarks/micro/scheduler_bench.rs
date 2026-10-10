use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use std::cell::RefCell;
use std::time::Duration;
use tempfile::{Builder, TempDir};

use legion::manager::scheduler::{PlanMode, PlanTimings, Scheduler};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, ArtifactSpec, ContentLocation, ContentLocationReport, JobConfig,
    NodeMsg, Submission, TaskConfig, TaskCount,
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
        // Workers that take checkpoints and fetch published ones, so the
        // published restores below have candidates.
        dynamic_attrs: json!({ "capabilities": ["checkpoint", "checkpoint-fetch"] }).to_string(),
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
        // run is six allocations per iteration against the fill. The
        // per-phase `PlanTimings` of every measured plan are averaged and
        // printed after the cell, so a run also says WHERE the time went
        // at each fill level (the same phases as the
        // `legion_scheduler_plan_phase_seconds` metric).
        let mut next_job = 0usize;
        let timings: RefCell<Vec<PlanTimings>> = RefCell::new(Vec::new());
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
                        timings.borrow_mut().push(outcome.timings.clone());
                        outcome
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
        print_phase_means(
            &format!("plan_under_fill/plan_and_apply/fill_{per_node}"),
            &timings.borrow(),
        );
    }

    group.finish();
}

// --- plan_fleet_scaling (plan-path-fleet-scaling-plan.md § 6.4) ---------
//
// Reproduces the per-eval cost growth the 2026-10-09 AWS campaign measured
// between 5,000 and 10,000 nodes, with node count and fill varied
// independently (the campaign doubled both at once). The fleet is the
// `web_fleet` mix: 5 of every 6 nodes `class = web`, the rest `class =
// util`, one pool, so the default `fit` index and the per-node constraint
// re-verification run exactly as on the rig. The planned job is the
// `web_fleet` api shape: two single-task groups of 4, cpu 500, memory
// 1024, constrained to `class = web`.
//
// The concurrent cells are the ones that matter: four planners (the
// 4-core default) plus a background writer publishing to the read model,
// which is what makes the lazy version sample fall back on the rig. Single
// threaded, no plan ever falls back.
const SCALING_NODES: &[usize] = &[1_000, 2_000, 5_000, 10_000, 20_000, 50_000];
const SCALING_FILL: &[usize] = &[0, 5];
/// Evals each `eval_cycle` cell runs (single-threaded, fill 5 only).
const SCALING_EVAL_CYCLES: usize = 150;

/// The node counts to run, overridable because the 50k cells take minutes
/// to set up: `LEGION_BENCH_SCALING_NODES=20000,50000 cargo bench ...`.
fn scaling_nodes() -> Vec<usize> {
    std::env::var("LEGION_BENCH_SCALING_NODES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect::<Vec<usize>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| SCALING_NODES.to_vec())
}
const SCALING_PLANNERS: usize = 4;
const SCALING_CONCURRENT_SECS: u64 = 4;

fn scaling_node_resources() -> serde_json::Value {
    // 8 cores x 3000 MHz, 32 GiB: the rig's `web` profile.
    let cpus: serde_json::Map<String, serde_json::Value> = (1..=8)
        .map(|idx| (format!("cpu-{idx}"), json!({ "frequency": "3000" })))
        .collect();
    json!({ "memory_total": (32u64 << 30).to_string(), "cpus": cpus })
}

fn insert_web_fleet(store: &Store, node_count: usize) -> anyhow::Result<()> {
    for idx in 0..node_count {
        let class = if idx % 6 == 5 { "util" } else { "web" };
        let mut node = make_node_msg(&format!("node-{idx}"), json!({ "class": class }));
        node.fingerprint_attrs = scaling_node_resources().to_string();
        store.insert_node(node)?;
    }
    Ok(())
}

fn make_api_job(name: &str) -> JobConfig {
    let task = |idx: usize| TaskConfig {
        count: TaskCount::new(4),
        group: format!("g{idx}"),
        resources: json!({ "cpu": 500, "memory": 1024 }),
        constraints: json!({ "class": "web" }),
        ..TaskConfig::new(format!("t{idx}"), "noop")
    };
    JobConfig {
        priority: Default::default(),
        name: name.to_string(),
        tasks: vec![task(0), task(1)],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn plan_fleet_scaling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_fleet_scaling");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(4));
    group.sample_size(15);

    for nodes in scaling_nodes() {
        for &fill in SCALING_FILL {
            let (store, _dir) = setup_store("bin_pack").expect("store");
            insert_web_fleet(&store, nodes).expect("nodes");
            fill_fleet(&store, nodes, fill).expect("fill");
            let cell = format!("nodes_{nodes}/fill_{fill}");

            // Single-threaded plan_and_apply: a fresh api job per iteration.
            // The previous iteration's 8 allocations are removed in the
            // untimed setup, so the fleet stays at its configured fill
            // instead of filling up over a few thousand iterations.
            let mut next_job = 0usize;
            let timings: RefCell<Vec<PlanTimings>> = RefCell::new(Vec::new());
            let placed: RefCell<Vec<i64>> = RefCell::new(Vec::new());
            group.bench_function(BenchmarkId::new("plan_and_apply", &cell), |b| {
                b.iter_batched(
                    || {
                        for id in placed.borrow_mut().drain(..) {
                            store.remove_allocation_by_id(id).expect("cleanup");
                        }
                        next_job += 1;
                        let job = make_api_job(&format!("api-{next_job}"));
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
                        assert_eq!(outcome.allocations.len(), 8);
                        timings.borrow_mut().push(outcome.timings.clone());
                        placed
                            .borrow_mut()
                            .extend(outcome.allocations.iter().map(|a| a.id));
                        outcome
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
            print_phase_means(
                &format!("plan_fleet_scaling/plan_and_apply/{cell}"),
                &timings.borrow(),
            );

            if fill > 0 {
                eval_cycle_cell(&store, &cell);
            }
            // `LEGION_BENCH_SKIP_CONCURRENT=1` skips these cells: at 20k+
            // nodes their 12,000 job submissions dominate the setup.
            if fill > 0
                && nodes >= 5_000
                && std::env::var_os("LEGION_BENCH_SKIP_CONCURRENT").is_none()
            {
                concurrent_scaling_cell(&store, &cell);
            }
        }
    }
    group.finish();
}

/// Four planners racing `plan_and_apply` on distinct jobs while a
/// background writer places and removes a filler allocation every few
/// milliseconds. Not a Criterion measurement (the interesting output is the
/// per-phase breakdown and the fallback count, not one duration), so it
/// prints its own summary line.
fn concurrent_scaling_cell(store: &Store, cell: &str) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    // Enough that no planner runs out before the deadline, so plans/s is
    // a measurement rather than the job count divided by the window.
    let jobs_per_planner = 3_000;
    let mut job_ids: Vec<Vec<String>> = Vec::new();
    for planner in 0..SCALING_PLANNERS {
        let jobs: Vec<JobConfig> = (0..jobs_per_planner)
            .map(|idx| make_api_job(&format!("conc-{cell}-{planner}-{idx}")))
            .collect();
        job_ids.push(submit_jobs(store, &jobs).expect("jobs"));
    }
    // The background writer's own job: one filler allocation placed and
    // removed on a rotating node, so every iteration publishes to the read
    // model the way unrelated applies do on the rig.
    let churn_id = submit_jobs(store, &[make_fill_job("churn")])
        .expect("churn job")
        .pop()
        .expect("one id");
    let churn_task = store
        .cached_task_configs_for_job_config(&churn_id)
        .expect("task")[0]
        .0;

    let metrics = legion::manager::telemetry::metrics();
    let fallback_before = metrics.scheduler_lazy_sample_fallback_total.get();
    let stop = Arc::new(AtomicBool::new(false));
    let planned = Arc::new(AtomicUsize::new(0));
    let samples: Arc<Mutex<Vec<PlanTimings>>> = Arc::new(Mutex::new(Vec::new()));
    let deadline = std::time::Instant::now() + Duration::from_secs(SCALING_CONCURRENT_SECS);

    std::thread::scope(|scope| {
        {
            let store = store.clone();
            let stop = stop.clone();
            scope.spawn(move || {
                let mut node = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let placed = store
                        .apply_allocation_plan(&[(format!("node-{node}"), churn_task)])
                        .expect("churn place");
                    for allocation in placed {
                        store
                            .remove_allocation_by_id(allocation.id)
                            .expect("churn remove");
                    }
                    node = (node + 7) % 1_000;
                    std::thread::sleep(Duration::from_millis(2));
                }
            });
        }
        for ids in &job_ids {
            let store = store.clone();
            let planned = planned.clone();
            let samples = samples.clone();
            scope.spawn(move || {
                for id in ids {
                    if std::time::Instant::now() >= deadline {
                        break;
                    }
                    let scheduler = Scheduler::new(&store).expect("scheduler");
                    // A fenced reject is a correct outcome under contention
                    // (the eval would retry); count only successes.
                    if let Ok(outcome) = scheduler.plan_and_apply(id, PlanMode::Fresh) {
                        samples.lock().unwrap().push(outcome.timings.clone());
                        planned.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        while std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        // Planners stop at the deadline on their own; then the writer.
        std::thread::sleep(Duration::from_millis(200));
        stop.store(true, Ordering::Relaxed);
    });

    let samples = samples.lock().unwrap();
    let fallbacks = metrics.scheduler_lazy_sample_fallback_total.get() - fallback_before;
    let plans = planned.load(Ordering::Relaxed);
    eprintln!(
        "plan_fleet_scaling/concurrent/{cell}: {plans} plans in {SCALING_CONCURRENT_SECS}s \
         ({:.1}/s), {fallbacks} lazy-sample fallbacks ({:.0}%)",
        plans as f64 / SCALING_CONCURRENT_SECS as f64,
        if plans > 0 {
            100.0 * fallbacks as f64 / plans as f64
        } else {
            0.0
        },
    );
    print_phase_means(&format!("plan_fleet_scaling/concurrent/{cell}"), &samples);
}

/// The whole per-eval cycle an eval worker runs on the leader, not just the
/// plan: claim the eval, run its handler (`resolve_claimed_eval`: scheduler
/// construction, previous-config lookup, plan with conflict retries,
/// dispatch, follow-ups), and settle it. `plan_and_apply` alone misses
/// everything the handler does around the plan, which is where the AWS
/// 20k-node cell spent most of each eval. Single-threaded, so it measures
/// cost rather than contention. Prints wall-time means per step and the
/// eval-phase breakdown the handler records.
fn eval_cycle_cell(store: &Store, cell: &str) {
    use legion::manager::daemon::resolve_claimed_eval;
    use legion::storage::store::{EvalBatchLimits, EvalCompletion};

    let limits = EvalBatchLimits { max: 1, workers: 1 };
    // Drain whatever the setup's job submissions enqueued (the filler jobs
    // and the plan_and_apply cell's api jobs), so every claim below takes
    // exactly the eval this cell enqueued.
    while let Some(eval) = store.claim_eval().expect("drain claim") {
        store.complete_eval(&eval.id, true).expect("drain complete");
    }

    let mut placed: Vec<i64> = Vec::new();
    let (mut claim_us, mut handle_us, mut settle_us) = (0u128, 0u128, 0u128);
    // The per-plan `node_lookup` map every plan built before the
    // available-node set cached its own by-id lookup, timed on its own so
    // its share of a plan is a measurement rather than a subtraction.
    {
        let nodes = store.available_node_arcs().expect("nodes");
        let rounds = 50u32;
        let started = std::time::Instant::now();
        for _ in 0..rounds {
            let lookup: std::collections::HashMap<&str, &legion::structs::Node> = nodes
                .iter()
                .map(|node| (node.id.as_str(), &**node))
                .collect();
            std::hint::black_box(lookup);
        }
        eprintln!(
            "plan_fleet_scaling/eval_cycle/{cell}: per-plan node_lookup map build (pre-Phase-4 shape) {:.0} us, node list clone {:.0} us",
            started.elapsed().as_micros() as f64 / rounds as f64,
            {
                let started = std::time::Instant::now();
                for _ in 0..rounds {
                    std::hint::black_box(store.available_node_arcs().expect("nodes"));
                }
                started.elapsed().as_micros() as f64 / rounds as f64
            }
        );
    }
    let eval_phases_before = histogram_sums("legion_eval_phase_seconds");
    let plan_phases_before = histogram_sums("legion_scheduler_plan_phase_seconds");
    for idx in 0..SCALING_EVAL_CYCLES {
        // Untimed: keep the fleet at its configured fill, and enqueue the
        // eval (a job submit, as the RunJob RPC does).
        for id in placed.drain(..) {
            store.remove_allocation_by_id(id).expect("cleanup");
        }
        let job_name = format!("eval-{cell}-{idx}");
        let job_config_id = submit_jobs(store, &[make_api_job(&job_name)])
            .expect("job")
            .pop()
            .expect("one id");
        store
            .enqueue_eval(
                "job_submit",
                Some(&job_name),
                Some(&job_config_id),
                None,
                None,
            )
            .expect("enqueue");

        let started = std::time::Instant::now();
        let batch = store.claim_evals(limits).expect("claim");
        claim_us += started.elapsed().as_micros();
        assert_eq!(batch.len(), 1, "exactly the eval this cycle enqueued");
        let eval = &batch[0];
        assert_eq!(eval.kind, "job_submit");

        let started = std::time::Instant::now();
        let completion = resolve_claimed_eval(store, 0, eval);
        handle_us += started.elapsed().as_micros();
        assert!(
            matches!(
                completion,
                EvalCompletion::Finished {
                    succeeded: true,
                    ..
                }
            ),
            "eval failed: {completion:?}"
        );

        let started = std::time::Instant::now();
        let next = store
            .complete_evals_and_claim_next(vec![(eval.id.clone(), completion)], limits)
            .expect("settle");
        settle_us += started.elapsed().as_micros();
        assert!(next.is_empty());

        placed.extend(
            store
                .allocs_for_job_config(&job_config_id)
                .expect("allocations")
                .iter()
                .map(|allocation| allocation.id),
        );
    }
    let n = SCALING_EVAL_CYCLES as f64;
    eprintln!(
        "plan_fleet_scaling/eval_cycle/{cell}: {SCALING_EVAL_CYCLES} evals, mean us: \
         claim {:.0}, handle {:.0}, settle {:.0}, cycle {:.0}",
        claim_us as f64 / n,
        handle_us as f64 / n,
        settle_us as f64 / n,
        (claim_us + handle_us + settle_us) as f64 / n,
    );
    // Where the handler's time went, from the metrics the eval path records
    // (`legion_eval_phase_seconds`, `legion_scheduler_plan_phase_seconds`).
    for (family, before) in [
        ("legion_eval_phase_seconds", eval_phases_before),
        ("legion_scheduler_plan_phase_seconds", plan_phases_before),
    ] {
        let after = histogram_sums(family);
        let mut phases: Vec<String> = after
            .iter()
            .map(|(labels, (sum, count))| {
                let (sum0, count0) = before.get(labels).copied().unwrap_or((0.0, 0));
                let evals = (count - count0).max(1) as f64;
                (labels, (sum - sum0) * 1e6 / evals, count - count0)
            })
            .filter(|(_, _, count)| *count > 0)
            .map(|(labels, mean_us, _)| format!("{labels} {mean_us:.0}"))
            .collect();
        phases.sort();
        eprintln!(
            "plan_fleet_scaling/eval_cycle/{cell}: {family} mean us: {}",
            phases.join(", ")
        );
    }
}

/// `(sum seconds, count)` per label set of one histogram family, read from
/// the process's metrics exposition.
fn histogram_sums(family: &str) -> std::collections::HashMap<String, (f64, u64)> {
    let text = legion::manager::telemetry::render();
    let mut out: std::collections::HashMap<String, (f64, u64)> = Default::default();
    for line in text.lines() {
        for (suffix, is_sum) in [("_sum", true), ("_count", false)] {
            let Some(rest) = line.strip_prefix(&format!("{family}{suffix}")) else {
                continue;
            };
            let Some((labels, value)) = rest.rsplit_once(' ') else {
                continue;
            };
            let labels = labels.trim_start_matches('{').trim_end_matches('}');
            let entry = out.entry(labels.to_string()).or_default();
            if is_sum {
                entry.0 = value.parse().unwrap_or(0.0);
            } else {
                entry.1 = value.parse().unwrap_or(0);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Content locality (suspend plan Phase 7)
// ---------------------------------------------------------------------------
//
// The cost of the `locality` score term. `content_absent` plans tasks with no
// digest-pinned artifacts, so the term is skipped (the baseline every
// ordinary job pays). `content_present` gives each task two pinned artifacts
// and seeds the location index so half the fleet holds one and a quarter the
// other: one index lookup per task, one score read per candidate.

const LOCALITY_NODE_STEPS: &[usize] = &[1_000, 5_000];
const LOCALITY_TASKS: usize = 4;
const LOCALITY_PER_TASK: u32 = 25;

fn locality_digest(idx: usize) -> String {
    format!("sha256:{:064x}", idx + 1)
}

fn make_locality_job(with_content: bool) -> JobConfig {
    let tasks = (0..LOCALITY_TASKS)
        .map(|idx| {
            let artifacts = if with_content {
                (0..2)
                    .map(|n| ArtifactSpec {
                        checksum: Some(locality_digest(n)),
                        ..ArtifactSpec::from_source(format!("https://example.com/{idx}-{n}.bin"))
                    })
                    .collect()
            } else {
                Vec::new()
            };
            TaskConfig {
                count: TaskCount::new(LOCALITY_PER_TASK),
                group: format!("group-{idx}"),
                artifacts,
                ..TaskConfig::new(format!("task-{idx}"), "noop")
            }
        })
        .collect();
    JobConfig {
        priority: Default::default(),
        name: format!("bench-locality-{with_content}"),
        tasks,
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn seed_locality_index(store: &Store, node_count: usize) {
    let now = std::time::Instant::now();
    for idx in 0..node_count {
        let mut added = Vec::new();
        if idx % 2 == 0 {
            added.push(ContentLocation {
                digest: locality_digest(0),
                bytes: 512 * 1024 * 1024,
            });
        }
        if idx % 4 == 0 {
            added.push(ContentLocation {
                digest: locality_digest(1),
                bytes: 64 * 1024 * 1024,
            });
        }
        store.content_locations().ingest(
            &format!("node-{idx}"),
            &ContentLocationReport {
                epoch: 1,
                base_epoch: 0,
                full: true,
                added,
                removed: Vec::new(),
            },
            now,
        );
    }
}

fn locality_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("locality");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    for &node_count in LOCALITY_NODE_STEPS {
        let (store, _dir) = setup_store("bin_pack").expect("store");
        insert_uniform_nodes(&store, node_count).expect("nodes");
        seed_locality_index(&store, node_count);
        for (label, with_content) in [("content_absent", false), ("content_present", true)] {
            let job = make_locality_job(with_content);
            let job_id = submit_jobs(&store, std::slice::from_ref(&job))
                .expect("job")
                .pop()
                .expect("one id");
            group.bench_function(
                BenchmarkId::new(label, format!("nodes_{node_count}")),
                |b| {
                    b.iter(|| {
                        let scheduler = Scheduler::new(&store).expect("scheduler");
                        let plan = scheduler.plan_only(&job_id, PlanMode::Fresh).expect("plan");
                        assert_eq!(plan.len(), LOCALITY_TASKS * LOCALITY_PER_TASK as usize);
                        plan
                    })
                },
            );
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Restore placement (suspend plan Phases 3c, 4, 7b)
// ---------------------------------------------------------------------------
//
// The cost of placing keyed instances that restore from a snapshot.
// `local_pinned` restores from `local` snapshots: each instance is pinned to
// its snapshot's node and `compat` is checked there once. `published_floating`
// restores from `published` snapshots: no pin, `compat` filters every
// candidate, and the locality term scores the manifest parts (seeded on a
// quarter of the fleet). Both plan RESTORE_INSTANCES placements.

const RESTORE_NODE_STEPS: &[usize] = &[1_000, 5_000];
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
                },
                now,
            );
        }
    }
    Ok(job_config_id)
}

fn restore_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("restore");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    for &node_count in RESTORE_NODE_STEPS {
        for (label, published) in [("local_pinned", false), ("published_floating", true)] {
            let (store, _dir) = setup_store("bin_pack").expect("store");
            let job_config_id = setup_restores(&store, node_count, published).expect("restores");
            group.bench_function(
                BenchmarkId::new(label, format!("nodes_{node_count}")),
                |b| {
                    b.iter(|| {
                        let scheduler = Scheduler::new(&store).expect("scheduler");
                        let plan = scheduler
                            .plan_only(&job_config_id, PlanMode::Fresh)
                            .expect("plan");
                        assert_eq!(plan.len(), RESTORE_INSTANCES);
                        plan
                    })
                },
            );
        }
    }

    group.finish();
}

/// Mean per-phase microseconds over every plan a cell measured, in the
/// order the phases run.
fn print_phase_means(cell: &str, samples: &[PlanTimings]) {
    if samples.is_empty() {
        return;
    }
    let mean = |pick: fn(&PlanTimings) -> u64| {
        samples.iter().map(pick).sum::<u64>() as f64 / samples.len() as f64
    };
    eprintln!(
        "{cell} phase means over {} plans (us): job {:.0}, nodes {:.0}, tasks {:.0}, \
         capacity {:.0}, build {:.0}, versions {:.0}, speculative {:.0}, apply {:.0}, \
         post {:.0}, total {:.0}",
        samples.len(),
        mean(|t| t.job_lookup_us),
        mean(|t| t.available_nodes_us),
        mean(|t| t.task_fetch_us),
        mean(|t| t.capacity_snapshot_us),
        mean(|t| t.build_plan_us),
        mean(|t| t.version_sample_us),
        mean(|t| t.speculative_dispatch_us),
        mean(|t| t.apply_us),
        mean(|t| t.post_apply_us),
        mean(|t| t.total_us),
    );
}

criterion_group!(
    benches,
    scheduler_speed_benchmark,
    candidate_construction_benchmark,
    plan_under_fill_benchmark,
    plan_fleet_scaling_benchmark,
    locality_benchmark,
    restore_benchmark
);
criterion_main!(benches);
