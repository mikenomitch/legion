use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Instant;

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::storage::errors::StorageError;
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, JobConfig, NodeMsg, Submission, TaskConfig, TaskCount,
};
use serde::Deserialize;
use serde_json::json;
use tempfile::Builder;

#[derive(Debug, Deserialize)]
struct SchedulerScenario {
    name: String,
    scheduling_policy: String,
    node_count: usize,
    job_count: usize,
    tasks_per_job: usize,
    task_count_per_task: u32,
    iterations: usize,
    churn_every: usize,
    churn_nodes: usize,
}

#[derive(Debug, Deserialize)]
struct SchedulerSlo {
    min_evals_per_sec: f64,
    max_p99_latency_ms: f64,
    max_conflict_rejection_rate: f64,
    min_correctness_ratio: f64,
}

#[derive(Debug)]
struct ScenarioMetrics {
    evals_per_sec: f64,
    p99_latency_ms: f64,
    conflict_rejection_rate: f64,
    correctness_ratio: f64,
}

fn store_path(prefix: &str) -> anyhow::Result<String> {
    let store_dir = Builder::new().prefix(prefix).tempdir()?.keep();
    Ok(store_dir
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Error parsing store path"))?
        .to_string())
}

fn setup_store(policy: &str) -> anyhow::Result<Store> {
    let store_config = ManagerConfigFull {
        store_path: store_path("legion-scheduler-benchmark-")?,
        scheduling_policy: policy.to_string(),
        task_resource_defaults: Some(json!({
            "cpu": 0,
            "memory": 0,
            "allocations": 1000
        })),
        ..Default::default()
    };

    let store = Store::start_storage(&store_config)?;
    store.reset_all_tables()?;
    Ok(store)
}

fn load_scenarios() -> anyhow::Result<Vec<SchedulerScenario>> {
    let base = Path::new("benchmarks/scheduler/scenarios");
    let mut scenarios: Vec<SchedulerScenario> = Vec::new();
    for entry in fs::read_dir(base)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let raw = fs::read_to_string(path)?;
        scenarios.push(serde_json::from_str(&raw)?);
    }
    scenarios.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(scenarios)
}

fn load_slo() -> anyhow::Result<SchedulerSlo> {
    let raw = fs::read_to_string("benchmarks/scheduler/slo.json")?;
    Ok(serde_json::from_str(&raw)?)
}

fn default_fingerprint() -> serde_json::Value {
    json!({
        "memory_total": "34359738368",
        "cpus": {
            "cpu-1": { "frequency": "3000" },
            "cpu-2": { "frequency": "3000" },
            "cpu-3": { "frequency": "3000" },
            "cpu-4": { "frequency": "3000" }
        }
    })
}

fn insert_node(store: &Store, id: &str) -> anyhow::Result<()> {
    let node = NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({ "tier": "benchmark" }).to_string(),
        fingerprint_attrs: default_fingerprint().to_string(),
        dynamic_attrs: json!({}).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
    };
    store.insert_node(node)?;
    Ok(())
}

fn submit_jobs(store: &Store, scenario: &SchedulerScenario) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::with_capacity(scenario.job_count);
    let submission = Submission {
        path: "benchmark.jsonnet".into(),
        content: "{}".into(),
        is_primary: true,
    };

    for job_idx in 0..scenario.job_count {
        let mut tasks = Vec::with_capacity(scenario.tasks_per_job);
        for task_idx in 0..scenario.tasks_per_job {
            let mut task = TaskConfig::new(format!("task-{job_idx}-{task_idx}"), "noop");
            task.count = TaskCount::new(scenario.task_count_per_task);
            tasks.push(task);
        }

        let job = JobConfig {
            name: format!("benchmark-job-{job_idx}"),
            tasks,
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        store.handle_job_submit(&job, &[submission.clone()])?;
        ids.push(store.get_job_config(&job.name)?.id());
    }

    Ok(ids)
}

fn pctl(latencies_ms: &[f64], percentile: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }
    let mut ordered = latencies_ms.to_vec();
    ordered.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (((ordered.len() - 1) as f64) * percentile).round() as usize;
    ordered[idx]
}

fn apply_churn(
    store: &Store,
    scenario: &SchedulerScenario,
    iteration: usize,
) -> anyhow::Result<()> {
    if scenario.churn_every == 0 || scenario.churn_nodes == 0 {
        return Ok(());
    }
    if iteration == 0 || iteration % scenario.churn_every != 0 {
        return Ok(());
    }

    let mut active_nodes = store
        .get_nodes()?
        .into_iter()
        .filter(|node| node.status == "active" && node.availability == "available")
        .collect::<Vec<_>>();
    active_nodes.sort_by(|a, b| a.id.cmp(&b.id));

    for node in active_nodes.iter().take(scenario.churn_nodes) {
        store.mark_node_lost(&node.id)?;
        store.remove_node(&node.id)?;
    }

    for idx in 0..scenario.churn_nodes {
        let node_id = format!("node-{}-replacement-{iteration}-{idx}", scenario.name);
        insert_node(store, &node_id)?;
    }

    Ok(())
}

fn validate_correctness(
    store: &Store,
    job_config_ids: &[String],
    scenario: &SchedulerScenario,
) -> anyhow::Result<f64> {
    let expected_per_job = (scenario.tasks_per_job as u32) * scenario.task_count_per_task;
    let mut valid_jobs = 0u64;

    let active_nodes: HashSet<String> = store
        .get_nodes()?
        .into_iter()
        .filter(|node| node.status == "active" && node.availability == "available")
        .map(|node| node.id)
        .collect();

    for job_config_id in job_config_ids {
        let allocations = store.allocs_for_job_config(job_config_id)?;
        if (allocations.len() as u32) < expected_per_job {
            continue;
        }
        let all_on_active_nodes = allocations
            .iter()
            .all(|allocation| active_nodes.contains(&allocation.node_id));
        if all_on_active_nodes {
            valid_jobs += 1;
        }
    }

    Ok(valid_jobs as f64 / job_config_ids.len() as f64)
}

fn run_scenario(scenario: &SchedulerScenario) -> anyhow::Result<ScenarioMetrics> {
    let store = setup_store(&scenario.scheduling_policy)?;

    for idx in 0..scenario.node_count {
        insert_node(&store, &format!("node-{idx}"))?;
    }

    let job_config_ids = submit_jobs(&store, scenario)?;

    let mut latencies_ms = Vec::new();
    let mut conflicts = 0u64;
    let mut attempts = 0u64;
    let benchmark_started = Instant::now();

    for iteration in 0..scenario.iterations {
        apply_churn(&store, scenario, iteration)?;

        for job_config_id in &job_config_ids {
            attempts += 1;
            let scheduler = Scheduler::new(&store)?;
            let started = Instant::now();
            match scheduler.plan_and_apply(job_config_id, PlanMode::Fresh) {
                Ok(_) => {
                    latencies_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
                }
                Err(err) => {
                    if let Some(StorageError::Conflict { .. }) = err.downcast_ref::<StorageError>()
                    {
                        conflicts += 1;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    let elapsed_secs = benchmark_started.elapsed().as_secs_f64().max(0.001);
    let evals_per_sec = attempts as f64 / elapsed_secs;
    let conflict_rejection_rate = conflicts as f64 / attempts.max(1) as f64;
    let correctness_ratio = validate_correctness(&store, &job_config_ids, scenario)?;

    Ok(ScenarioMetrics {
        evals_per_sec,
        p99_latency_ms: pctl(&latencies_ms, 0.99),
        conflict_rejection_rate,
        correctness_ratio,
    })
}

#[test]
#[ignore = "benchmark gate for scheduler-impacting changes"]
fn scheduler_benchmark_slo_gate() -> anyhow::Result<()> {
    let scenarios = load_scenarios()?;
    let slo = load_slo()?;
    assert!(!scenarios.is_empty(), "at least one scenario is required");

    for scenario in scenarios {
        let metrics = run_scenario(&scenario)?;
        println!(
            "scenario={} evals_per_sec={:.2} p99_latency_ms={:.2} conflict_rejection_rate={:.4} correctness_ratio={:.3}",
            scenario.name,
            metrics.evals_per_sec,
            metrics.p99_latency_ms,
            metrics.conflict_rejection_rate,
            metrics.correctness_ratio,
        );

        assert!(
            metrics.evals_per_sec >= slo.min_evals_per_sec,
            "scenario={} evals/sec {:.2} below target {:.2}",
            scenario.name,
            metrics.evals_per_sec,
            slo.min_evals_per_sec
        );
        assert!(
            metrics.p99_latency_ms <= slo.max_p99_latency_ms,
            "scenario={} p99 latency {:.2}ms exceeds target {:.2}ms",
            scenario.name,
            metrics.p99_latency_ms,
            slo.max_p99_latency_ms
        );
        assert!(
            metrics.conflict_rejection_rate <= slo.max_conflict_rejection_rate,
            "scenario={} conflict rejection rate {:.4} exceeds target {:.4}",
            scenario.name,
            metrics.conflict_rejection_rate,
            slo.max_conflict_rejection_rate
        );
        assert!(
            metrics.correctness_ratio >= slo.min_correctness_ratio,
            "scenario={} correctness ratio {:.3} below target {:.3}",
            scenario.name,
            metrics.correctness_ratio,
            slo.min_correctness_ratio
        );
    }

    Ok(())
}
