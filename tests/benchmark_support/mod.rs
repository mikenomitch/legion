//! Shared scenario schema and runner for the scheduler benchmarks.
//!
//! Used via `#[path = "benchmark_support/mod.rs"]` from
//! `tests/scheduler_benchmark_slo.rs` (the CI gate, local_disk only) and
//! `tests/scheduler_scale_benchmark.rs` (the scenario × backend matrix).
//! See `docs/testing-and-benchmarks.md` and
//! `internal_docs/plans/scheduler-performance-plan.md`.
//!
//! Scenario files live in `benchmarks/scheduler/scenarios/*.json`. Two
//! schema generations parse into the same struct:
//!
//! - v1: uniform `node_count` + `job_count`/`tasks_per_job`/
//!   `task_count_per_task` (no resources, no constraints).
//! - v2: `node_profiles` (heterogeneous fleets: per-profile count,
//!   memory/cpus, attrs) + `job_types` (mixed workloads: per-type count,
//!   shape, `resources`, `constraints`) + optional `scheduler_settings`
//!   passthrough (indexes) + optional per-scenario `slo` overrides.
//!
//! A v1 file is exactly a v2 file with one uniform profile and one job
//! type, so every legacy scenario keeps working unchanged.

// Each including binary uses a different subset of this module; dead-code
// analysis runs per binary, so unused-in-this-binary is expected.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::time::Instant;

use legion::manager::scheduler::{PlanMode, PlanTimings, Scheduler};
use legion::storage::errors::StorageError;
use legion::storage::store::Store;
use legion::structs::{
    config::{ManagerConfigFull, PersistenceType},
    JobConfig, NodeMsg, Submission, TaskConfig, TaskCount,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Scenario schema
// ---------------------------------------------------------------------------

/// One class of node in the benchmark fleet.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeProfile {
    pub name: String,
    pub count: usize,
    /// Fingerprinted memory, GiB (default 32).
    #[serde(default = "default_memory_gb")]
    pub memory_gb: u64,
    /// Fingerprinted CPU count (default 4), 3 GHz each.
    #[serde(default = "default_cpus")]
    pub cpus: u32,
    /// Extra `config_attrs` (region, zone, class, gpu, ...).
    #[serde(default)]
    pub attrs: BTreeMap<String, Value>,
}

fn default_memory_gb() -> u64 {
    32
}
fn default_cpus() -> u32 {
    4
}

/// One class of job in the benchmark workload.
#[derive(Debug, Clone, Deserialize)]
pub struct JobType {
    pub name: String,
    /// How many jobs of this type.
    pub count: usize,
    pub tasks_per_job: usize,
    pub task_count_per_task: u32,
    /// `TaskConfig.resources`, in pool units: `cpu` is MHz, `memory` is
    /// MiB (e.g. `{ "cpu": 500, "memory": 1024 }` = 0.5 GHz + 1 GiB).
    #[serde(default)]
    pub resources: Option<Value>,
    /// `TaskConfig.constraints` (attr equality, e.g. `{ "class": "web" }`).
    #[serde(default)]
    pub constraints: Option<Value>,
}

/// Per-scenario SLO overrides; absent fields fall back to the global
/// `benchmarks/scheduler/slo.json`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SloOverride {
    pub min_evals_per_sec: Option<f64>,
    pub max_p99_latency_ms: Option<f64>,
    pub max_conflict_rejection_rate: Option<f64>,
    pub min_correctness_ratio: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct SchedulerScenario {
    pub name: String,
    pub scheduling_policy: String,
    pub iterations: usize,
    // ---- v1 (uniform) ----
    #[serde(default)]
    pub node_count: Option<usize>,
    #[serde(default)]
    pub job_count: Option<usize>,
    #[serde(default)]
    pub tasks_per_job: Option<usize>,
    #[serde(default)]
    pub task_count_per_task: Option<u32>,
    // ---- v2 (heterogeneous) ----
    #[serde(default)]
    pub node_profiles: Option<Vec<NodeProfile>>,
    #[serde(default)]
    pub job_types: Option<Vec<JobType>>,
    /// Passed through to `ManagerConfigFull.scheduler_settings` (this is
    /// how a scenario arms scheduling indexes; docs/scheduling-indexes.md).
    #[serde(default)]
    pub scheduler_settings: Option<Value>,
    // ---- churn ----
    #[serde(default)]
    pub churn_every: usize,
    #[serde(default)]
    pub churn_nodes: usize,
    // ---- SLO override ----
    #[serde(default)]
    pub slo: Option<SloOverride>,
}

#[derive(Debug, Deserialize)]
pub struct SchedulerSlo {
    pub min_evals_per_sec: f64,
    pub max_p99_latency_ms: f64,
    pub max_conflict_rejection_rate: f64,
    pub min_correctness_ratio: f64,
}

impl SchedulerSlo {
    /// Global thresholds with this scenario's overrides applied.
    pub fn for_scenario(&self, scenario: &SchedulerScenario) -> SchedulerSlo {
        let ovr = scenario.slo.clone().unwrap_or_default();
        SchedulerSlo {
            min_evals_per_sec: ovr.min_evals_per_sec.unwrap_or(self.min_evals_per_sec),
            max_p99_latency_ms: ovr.max_p99_latency_ms.unwrap_or(self.max_p99_latency_ms),
            max_conflict_rejection_rate: ovr
                .max_conflict_rejection_rate
                .unwrap_or(self.max_conflict_rejection_rate),
            min_correctness_ratio: ovr
                .min_correctness_ratio
                .unwrap_or(self.min_correctness_ratio),
        }
    }
}

impl SchedulerScenario {
    /// The fleet, as profiles — a v1 scenario becomes one uniform profile.
    pub fn effective_profiles(&self) -> Vec<NodeProfile> {
        if let Some(profiles) = &self.node_profiles {
            assert!(
                self.node_count.is_none(),
                "scenario {}: node_profiles and node_count are mutually exclusive",
                self.name
            );
            return profiles.clone();
        }
        vec![NodeProfile {
            name: "default".into(),
            count: self.node_count.unwrap_or_else(|| {
                panic!("scenario {}: need node_count or node_profiles", self.name)
            }),
            memory_gb: default_memory_gb(),
            cpus: default_cpus(),
            attrs: BTreeMap::new(),
        }]
    }

    /// The workload, as job types — a v1 scenario becomes one type.
    pub fn effective_job_types(&self) -> Vec<JobType> {
        if let Some(types) = &self.job_types {
            assert!(
                self.job_count.is_none(),
                "scenario {}: job_types and job_count are mutually exclusive",
                self.name
            );
            return types.clone();
        }
        vec![JobType {
            name: "uniform".into(),
            count: self
                .job_count
                .unwrap_or_else(|| panic!("scenario {}: need job_count or job_types", self.name)),
            tasks_per_job: self.tasks_per_job.unwrap_or(1),
            task_count_per_task: self.task_count_per_task.unwrap_or(1),
            resources: None,
            constraints: None,
        }]
    }

    pub fn total_nodes(&self) -> usize {
        self.effective_profiles().iter().map(|p| p.count).sum()
    }

    pub fn total_desired_allocations(&self) -> usize {
        self.effective_job_types()
            .iter()
            .map(|t| t.count * t.tasks_per_job * t.task_count_per_task as usize)
            .sum()
    }
}

pub fn load_scenarios() -> anyhow::Result<Vec<SchedulerScenario>> {
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

pub fn load_slo() -> anyhow::Result<SchedulerSlo> {
    let raw = fs::read_to_string("benchmarks/scheduler/slo.json")?;
    Ok(serde_json::from_str(&raw)?)
}

// ---------------------------------------------------------------------------
// Fleet + workload construction
// ---------------------------------------------------------------------------

fn fingerprint_for(profile: &NodeProfile) -> Value {
    let mut cpus = serde_json::Map::new();
    for idx in 1..=profile.cpus {
        cpus.insert(format!("cpu-{idx}"), json!({ "frequency": "3000" }));
    }
    json!({
        "memory_total": (profile.memory_gb * 1024 * 1024 * 1024).to_string(),
        "cpus": cpus,
    })
}

fn config_attrs_for(profile: &NodeProfile) -> Value {
    let mut attrs = serde_json::Map::new();
    attrs.insert("tier".into(), json!("benchmark"));
    for (key, value) in &profile.attrs {
        attrs.insert(key.clone(), value.clone());
    }
    Value::Object(attrs)
}

/// Insert one node of `profile`. Node ids embed the profile name
/// (`node-<profile>-...`) so churn replacements can preserve the profile.
pub fn insert_profile_node(store: &Store, profile: &NodeProfile, id: &str) -> anyhow::Result<()> {
    let node = NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: config_attrs_for(profile).to_string(),
        fingerprint_attrs: fingerprint_for(profile).to_string(),
        dynamic_attrs: json!({}).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
    };
    store.insert_node(node)?;
    Ok(())
}

pub fn insert_fleet(store: &Store, scenario: &SchedulerScenario) -> anyhow::Result<()> {
    for profile in scenario.effective_profiles() {
        for idx in 0..profile.count {
            insert_profile_node(store, &profile, &format!("node-{}-{idx}", profile.name))?;
        }
    }
    Ok(())
}

pub fn submit_jobs(store: &Store, scenario: &SchedulerScenario) -> anyhow::Result<Vec<String>> {
    let submission = Submission {
        path: "benchmark.ncl".into(),
        content: "{}".into(),
        is_primary: true,
    };
    let mut ids = Vec::new();

    for job_type in scenario.effective_job_types() {
        for job_idx in 0..job_type.count {
            let mut tasks = Vec::with_capacity(job_type.tasks_per_job);
            for task_idx in 0..job_type.tasks_per_job {
                let mut task = TaskConfig::new(
                    format!("task-{}-{job_idx}-{task_idx}", job_type.name),
                    "noop",
                );
                task.count = TaskCount::new(job_type.task_count_per_task);
                if let Some(resources) = &job_type.resources {
                    task.resources = resources.clone();
                }
                if let Some(constraints) = &job_type.constraints {
                    task.constraints = constraints.clone();
                }
                // Distinct groups: each task places independently (the
                // default group would co-place a job's tasks on one node).
                task.group = format!("group-{}-{job_idx}-{task_idx}", job_type.name);
                tasks.push(task);
            }

            let job = JobConfig {
                priority: Default::default(),
                name: format!("bench-{}-{job_idx}", job_type.name),
                tasks,
                rollout: None,
                rollout_by_group: Default::default(),
                version: None,
            };
            store.handle_job_submit(&job, std::slice::from_ref(&submission))?;
            ids.push(store.get_job_config(&job.name)?.id());
        }
    }

    Ok(ids)
}

// ---------------------------------------------------------------------------
// Churn
// ---------------------------------------------------------------------------

/// Drop `churn_nodes` active nodes and add profile-preserving replacements
/// (the profile is recovered from the node-id prefix).
pub fn apply_churn(
    store: &Store,
    scenario: &SchedulerScenario,
    iteration: usize,
) -> anyhow::Result<()> {
    if scenario.churn_every == 0 || scenario.churn_nodes == 0 {
        return Ok(());
    }
    if iteration == 0 || !iteration.is_multiple_of(scenario.churn_every) {
        return Ok(());
    }

    let profiles: HashMap<String, NodeProfile> = scenario
        .effective_profiles()
        .into_iter()
        .map(|profile| (profile.name.clone(), profile))
        .collect();

    let mut active_nodes = store
        .get_nodes()?
        .into_iter()
        .filter(|node| node.status == "active" && node.availability == "available")
        .collect::<Vec<_>>();
    active_nodes.sort_by(|a, b| a.id.cmp(&b.id));

    let removed: Vec<_> = active_nodes
        .iter()
        .take(scenario.churn_nodes)
        .cloned()
        .collect();
    for node in &removed {
        store.mark_node_lost(&node.id)?;
        store.remove_node(&node.id)?;
    }

    for (idx, node) in removed.iter().enumerate() {
        // `node-<profile>-...` — recover the profile; unknown prefixes get
        // the first profile (covers v1's single "default").
        let profile = profiles
            .values()
            .find(|profile| {
                node.id
                    .strip_prefix("node-")
                    .is_some_and(|rest| rest.starts_with(profile.name.as_str()))
            })
            .or_else(|| profiles.values().next())
            .expect("at least one profile");
        let id = format!("node-{}-replacement-{iteration}-{idx}", profile.name);
        insert_profile_node(store, profile, &id)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Metrics + correctness
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ScenarioMetrics {
    pub evals_per_sec: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub conflict_rejection_rate: f64,
    pub correctness_ratio: f64,
    /// Mean per-phase µs across all applied plans (PlanTimings order).
    pub phase_means_us: PhaseMeans,
}

/// Mean per-phase timings across a run, µs. Field order matches
/// `PlanTimings`.
#[derive(Debug, Default)]
pub struct PhaseMeans {
    pub available_nodes_us: u64,
    pub version_sample_us: u64,
    pub task_fetch_us: u64,
    pub capacity_snapshot_us: u64,
    pub build_plan_us: u64,
    pub apply_us: u64,
    pub total_us: u64,
}

impl PhaseMeans {
    fn from_samples(samples: &[PlanTimings]) -> PhaseMeans {
        if samples.is_empty() {
            return PhaseMeans::default();
        }
        let n = samples.len() as u64;
        let sum = |f: fn(&PlanTimings) -> u64| samples.iter().map(f).sum::<u64>() / n;
        PhaseMeans {
            available_nodes_us: sum(|t| t.available_nodes_us),
            version_sample_us: sum(|t| t.version_sample_us),
            task_fetch_us: sum(|t| t.task_fetch_us),
            capacity_snapshot_us: sum(|t| t.capacity_snapshot_us),
            build_plan_us: sum(|t| t.build_plan_us),
            apply_us: sum(|t| t.apply_us),
            total_us: sum(|t| t.total_us),
        }
    }

    pub fn render(&self) -> String {
        format!(
            "nodes={}us versions={}us tasks={}us capacity={}us build={}us apply={}us total={}us",
            self.available_nodes_us,
            self.version_sample_us,
            self.task_fetch_us,
            self.capacity_snapshot_us,
            self.build_plan_us,
            self.apply_us,
            self.total_us,
        )
    }
}

pub fn pctl(latencies_ms: &[f64], percentile: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }
    let mut ordered = latencies_ms.to_vec();
    ordered.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (((ordered.len() - 1) as f64) * percentile).round() as usize;
    ordered[idx]
}

/// A job is correct when it has its full desired allocation count, every
/// allocation sits on an active node, and every allocation of a constrained
/// task sits on a node whose `config_attrs` satisfy the constraints
/// (equality semantics — what the scenarios use).
pub fn validate_correctness(
    store: &Store,
    job_config_ids: &[String],
    scenario: &SchedulerScenario,
) -> anyhow::Result<f64> {
    let job_types = scenario.effective_job_types();
    let nodes = store.get_nodes()?;
    let active: HashMap<String, Value> = nodes
        .into_iter()
        .filter(|node| node.status == "active" && node.availability == "available")
        .map(|node| (node.id, node.config_attrs))
        .collect();

    let expected_for = |job_config_id: &str, job_name: &str| -> (u32, Option<Value>) {
        // `bench-<type>-<idx>` (v2 runner) or legacy names: match by prefix.
        for job_type in &job_types {
            if job_name.starts_with(&format!("bench-{}-", job_type.name)) {
                return (
                    (job_type.tasks_per_job as u32) * job_type.task_count_per_task,
                    job_type.constraints.clone(),
                );
            }
        }
        let _ = job_config_id;
        (
            job_types
                .first()
                .map(|t| (t.tasks_per_job as u32) * t.task_count_per_task)
                .unwrap_or(0),
            None,
        )
    };

    let mut valid_jobs = 0u64;
    for job_config_id in job_config_ids {
        let config = store.get_job_config_by_id(job_config_id)?;
        let (expected, constraints) = expected_for(job_config_id, &config.name);
        let allocations = store.allocs_for_job_config(job_config_id)?;
        if (allocations.len() as u32) < expected {
            continue;
        }
        let all_ok = allocations.iter().all(|allocation| {
            let Some(attrs) = active.get(&allocation.node_id) else {
                return false;
            };
            let Some(Value::Object(required)) = &constraints else {
                return true;
            };
            required
                .iter()
                .all(|(key, want)| attrs.get(key).is_some_and(|have| have == want))
        });
        if all_ok {
            valid_jobs += 1;
        }
    }

    Ok(valid_jobs as f64 / job_config_ids.len().max(1) as f64)
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Run one scenario against an already-populated-config store: inserts the
/// fleet, submits the workload, then re-plans every job for `iterations`
/// rounds with churn applied between rounds.
pub fn run_scenario_on(
    store: &Store,
    scenario: &SchedulerScenario,
) -> anyhow::Result<ScenarioMetrics> {
    insert_fleet(store, scenario)?;
    let job_config_ids = submit_jobs(store, scenario)?;

    let mut latencies_ms = Vec::new();
    let mut timing_samples: Vec<PlanTimings> = Vec::new();
    let mut conflicts = 0u64;
    let mut attempts = 0u64;
    let benchmark_started = Instant::now();

    for iteration in 0..scenario.iterations {
        apply_churn(store, scenario, iteration)?;

        for job_config_id in &job_config_ids {
            attempts += 1;
            let scheduler = Scheduler::new(store)?;
            let started = Instant::now();
            match scheduler.plan_and_apply(job_config_id, PlanMode::Fresh) {
                Ok(outcome) => {
                    latencies_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
                    timing_samples.push(outcome.timings);
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
    let correctness_ratio = validate_correctness(store, &job_config_ids, scenario)?;

    Ok(ScenarioMetrics {
        evals_per_sec,
        p50_latency_ms: pctl(&latencies_ms, 0.50),
        p95_latency_ms: pctl(&latencies_ms, 0.95),
        p99_latency_ms: pctl(&latencies_ms, 0.99),
        conflict_rejection_rate,
        correctness_ratio,
        phase_means_us: PhaseMeans::from_samples(&timing_samples),
    })
}

/// `run_scenario_on` with N concurrent submitters — the driver shape that
/// exercises the plan-apply group commit (plan-path scale levers, D5).
/// Each iteration partitions the jobs across `workers` threads; every
/// thread plans and applies its jobs with bounded conflict retries (what
/// the production eval path does), so contention shows up as conflict
/// rate and retry latency rather than missing allocations.
pub fn run_scenario_concurrent(
    store: &Store,
    scenario: &SchedulerScenario,
    workers: usize,
) -> anyhow::Result<ScenarioMetrics> {
    const CONFLICT_RETRIES: usize = 8;
    if workers <= 1 {
        return run_scenario_on(store, scenario);
    }
    insert_fleet(store, scenario)?;
    let job_config_ids = submit_jobs(store, scenario)?;

    let mut latencies_ms: Vec<f64> = Vec::new();
    let mut timing_samples: Vec<PlanTimings> = Vec::new();
    let mut conflicts = 0u64;
    let mut attempts = 0u64;
    let benchmark_started = Instant::now();

    for iteration in 0..scenario.iterations {
        apply_churn(store, scenario, iteration)?;

        let chunk = job_config_ids.len().div_ceil(workers);
        let handles: Vec<_> = job_config_ids
            .chunks(chunk.max(1))
            .map(|jobs| {
                let store = store.clone();
                let jobs = jobs.to_vec();
                std::thread::spawn(
                    move || -> anyhow::Result<(Vec<f64>, Vec<PlanTimings>, u64, u64)> {
                        let mut latencies = Vec::new();
                        let mut timings = Vec::new();
                        let mut conflicts = 0u64;
                        let mut attempts = 0u64;
                        for job_config_id in &jobs {
                            attempts += 1;
                            let mut retries = 0usize;
                            loop {
                                let scheduler = Scheduler::new(&store)?;
                                let started = Instant::now();
                                match scheduler.plan_and_apply(job_config_id, PlanMode::Fresh) {
                                    Ok(outcome) => {
                                        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
                                        timings.push(outcome.timings);
                                        break;
                                    }
                                    Err(err) => {
                                        if let Some(StorageError::Conflict { .. }) =
                                            err.downcast_ref::<StorageError>()
                                        {
                                            conflicts += 1;
                                            retries += 1;
                                            if retries <= CONFLICT_RETRIES {
                                                continue;
                                            }
                                            // Exhausted: leave the gap for the
                                            // correctness check to surface.
                                            break;
                                        }
                                        return Err(err);
                                    }
                                }
                            }
                        }
                        Ok((latencies, timings, conflicts, attempts))
                    },
                )
            })
            .collect();
        for handle in handles {
            let (worker_latencies, worker_timings, worker_conflicts, worker_attempts) =
                handle.join().expect("scenario worker panicked")?;
            latencies_ms.extend(worker_latencies);
            timing_samples.extend(worker_timings);
            conflicts += worker_conflicts;
            attempts += worker_attempts;
        }
    }

    let elapsed_secs = benchmark_started.elapsed().as_secs_f64().max(0.001);
    let evals_per_sec = attempts as f64 / elapsed_secs;
    let conflict_rejection_rate = conflicts as f64 / attempts.max(1) as f64;
    let correctness_ratio = validate_correctness(store, &job_config_ids, scenario)?;

    Ok(ScenarioMetrics {
        evals_per_sec,
        p50_latency_ms: pctl(&latencies_ms, 0.50),
        p95_latency_ms: pctl(&latencies_ms, 0.95),
        p99_latency_ms: pctl(&latencies_ms, 0.99),
        conflict_rejection_rate,
        correctness_ratio,
        phase_means_us: PhaseMeans::from_samples(&timing_samples),
    })
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Plain local SQLite store (today's default persistence).
    LocalDisk,
    /// Raft, single bootstrap node: propose → commit → apply, no network.
    Raft1,
    /// Raft, 3-node in-process cluster over real tonic transport: quorum
    /// commit on the scheduler write path.
    Raft3,
    /// Streaming persistence (rustyriver-managed Db, `file://` replica).
    Streaming,
}

impl Backend {
    pub fn label(&self) -> &'static str {
        match self {
            Backend::LocalDisk => "local_disk",
            Backend::Raft1 => "raft1",
            Backend::Raft3 => "raft3",
            Backend::Streaming => "streaming",
        }
    }

    pub fn parse_list(raw: &str) -> Vec<Backend> {
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| match value {
                "local_disk" => Backend::LocalDisk,
                "raft1" => Backend::Raft1,
                "raft3" => Backend::Raft3,
                "streaming" => Backend::Streaming,
                other => panic!("unknown backend {other} (local_disk|raft1|raft3|streaming)"),
            })
            .collect()
    }
}

/// A member of an in-process raft cluster: its store plus the tonic
/// transport serving its `RaftTransport` (same harness shape as
/// `tests/raft_lanes.rs`).
struct ClusterNode {
    store: Store,
    _server: (
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    ),
}

/// Owns everything a benchmark backend needs to stay alive: the store (or
/// cluster), the tokio runtime serving raft transports, and the tempdirs.
pub struct BackendHandle {
    pub backend: Backend,
    single: Option<Store>,
    cluster: Vec<ClusterNode>,
    _runtime: Option<tokio::runtime::Runtime>,
    _dir: TempDir,
}

impl BackendHandle {
    /// The store scheduling should drive. For `raft3` that is the current
    /// scheduler-lane leader (writes on a follower fail `NotRaftLeader`).
    pub fn store(&self) -> &Store {
        if let Some(store) = &self.single {
            return store;
        }
        self.cluster
            .iter()
            .find(|node| {
                node.store.raft_status().is_some_and(|status| {
                    status
                        .lanes
                        .iter()
                        .any(|lane| lane.role.as_str() == "primary")
                })
            })
            .map(|node| &node.store)
            .expect("raft3 cluster has a leader")
    }
}

fn base_config(dir: &Path, scenario: &SchedulerScenario) -> anyhow::Result<ManagerConfigFull> {
    // `LEGION_BENCH_SCHEDULER_SETTINGS` overlays keys onto the scenario's
    // schedulerSettings — how an A/B toggles a lever (e.g.
    // '{"plan_apply_coalescing":false}') without editing scenario files.
    let mut scheduler_settings = scenario.scheduler_settings.clone();
    if let Ok(raw) = std::env::var("LEGION_BENCH_SCHEDULER_SETTINGS") {
        let overlay: Value = serde_json::from_str(&raw)
            .map_err(|err| anyhow::anyhow!("bad LEGION_BENCH_SCHEDULER_SETTINGS: {err}"))?;
        let mut merged = scheduler_settings
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Value::Object(overlay) = overlay {
            merged.extend(overlay);
        }
        scheduler_settings = Some(Value::Object(merged));
    }
    Ok(ManagerConfigFull {
        store_path: dir
            .join("store")
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("store path is not utf-8"))?
            .to_string(),
        scheduling_policy: scenario.scheduling_policy.clone(),
        task_resource_defaults: Some(json!({
            "cpu": 0,
            "memory": 0,
            "allocations": 1000
        })),
        scheduler_settings,
        ..Default::default()
    })
}

fn raft_node_config(
    root: &Path,
    scenario: &SchedulerScenario,
    name: &str,
    port: u16,
    bootstrap: bool,
    join_token: Option<String>,
) -> anyhow::Result<ManagerConfigFull> {
    let dir = root.join(name);
    fs::create_dir_all(&dir)?;
    let mut config = base_config(&dir, scenario)?;
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    // Relaxed election timing for an in-process multi-node cluster (see
    // tests/raft_lanes.rs for the reasoning): benchmark load stalls threads
    // long enough to trip production timeouts and flap leadership mid-run.
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 2000;
    config.persistence.raft.election_timeout_max_ms = 3000;
    config.persistence.raft.data_dir = dir
        .join("raft")
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("raft data dir is not utf-8"))?
        .to_string();
    Ok(config)
}

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn start_transport(
    rt: &tokio::runtime::Runtime,
    store: &Store,
    port: u16,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let service = store.raft_transport_server();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = rt.spawn(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("bind transport port");
        let listener = socket.listen(64).expect("listen");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
        {
            eprintln!("benchmark raft transport server exited: {err}");
        }
    });
    // Let the listener reach accept() before anyone dials it.
    std::thread::sleep(std::time::Duration::from_millis(50));
    (handle, shutdown_tx)
}

fn wait_for(timeout: std::time::Duration, what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if probe() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("timed out waiting for {what}");
}

/// Stand up one backend for one scenario run. The returned handle owns all
/// resources; drop it to tear the backend down.
pub fn setup_backend(
    backend: Backend,
    scenario: &SchedulerScenario,
) -> anyhow::Result<BackendHandle> {
    let dir = tempfile::Builder::new()
        .prefix(&format!("legion-bench-{}-", backend.label()))
        .tempdir()?;

    match backend {
        Backend::LocalDisk => {
            let store = Store::start_storage(&base_config(dir.path(), scenario)?)?;
            store.reset_all_tables()?;
            Ok(BackendHandle {
                backend,
                single: Some(store),
                cluster: Vec::new(),
                _runtime: None,
                _dir: dir,
            })
        }
        Backend::Streaming => {
            let mut config = base_config(dir.path(), scenario)?;
            config.persistence.r#type = PersistenceType::Streaming;
            config.persistence.streaming.replica_url = format!(
                "file://{}",
                dir.path().join("replica").to_str().expect("utf-8 path")
            );
            config.persistence.streaming.sync_interval = "1s".to_string();
            let store = Store::start_storage(&config)?;
            store.reset_all_tables()?;
            Ok(BackendHandle {
                backend,
                single: Some(store),
                cluster: Vec::new(),
                _runtime: None,
                _dir: dir,
            })
        }
        Backend::Raft1 => {
            let mut config = base_config(dir.path(), scenario)?;
            config.persistence.r#type = PersistenceType::Raft;
            config.persistence.raft.bootstrap = true;
            config.persistence.raft.node_id = "bench-raft1".to_string();
            config.persistence.raft.data_dir = dir
                .path()
                .join("raft")
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("raft data dir is not utf-8"))?
                .to_string();
            let store = Store::start_storage(&config)?;
            Ok(BackendHandle {
                backend,
                single: Some(store),
                cluster: Vec::new(),
                _runtime: None,
                _dir: dir,
            })
        }
        Backend::Raft3 => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;

            let port_a = reserve_port();
            let config_a = raft_node_config(dir.path(), scenario, "bench-a", port_a, true, None)?;
            let store_a = Store::start_storage(&config_a)?;
            let server_a = start_transport(&rt, &store_a, port_a);

            let (token, _expires) = store_a.issue_raft_join_token("benchmark")?;

            let port_b = reserve_port();
            let config_b = raft_node_config(
                dir.path(),
                scenario,
                "bench-b",
                port_b,
                false,
                Some(token.clone()),
            )?;
            let store_b = Store::start_storage(&config_b)?;
            let server_b = start_transport(&rt, &store_b, port_b);

            let port_c = reserve_port();
            let config_c =
                raft_node_config(dir.path(), scenario, "bench-c", port_c, false, Some(token))?;
            let store_c = Store::start_storage(&config_c)?;
            let server_c = start_transport(&rt, &store_c, port_c);

            let nodes = vec![
                ClusterNode {
                    store: store_a,
                    _server: server_a,
                },
                ClusterNode {
                    store: store_b,
                    _server: server_b,
                },
                ClusterNode {
                    store: store_c,
                    _server: server_c,
                },
            ];

            // Same gate as tests/raft_lanes.rs::start_cluster: every node a
            // SETTLED voter on every lane and every lane led — a joint
            // membership left mid-join would poison latency samples with
            // old-quorum waits.
            wait_for(
                std::time::Duration::from_secs(120),
                "3-node benchmark cluster settled (all voters, all lanes led)",
                || {
                    nodes.iter().all(|node| {
                        node.store
                            .raft_status()
                            .map(|status| {
                                let voters_ok = ["bench-a", "bench-b", "bench-c"]
                                    .iter()
                                    .all(|name| status.voters.iter().any(|voter| voter == name));
                                voters_ok
                                    && !status.lanes.is_empty()
                                    && status.lanes.iter().all(|lane| lane.leader_id.is_some())
                            })
                            .unwrap_or(false)
                    })
                },
            );

            Ok(BackendHandle {
                backend,
                single: None,
                cluster: nodes,
                _runtime: Some(rt),
                _dir: dir,
            })
        }
    }
}
