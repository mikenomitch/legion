//! Edge ratification throughput (edge ratify throughput plan, Phase 1;
//! `internal_docs/plans/edge-ratify-throughput-plan.md`).
//!
//! Every edge start boots without a manager but still needs one durable
//! ratification. This harness drives the real `RatifyClaim` handler
//! in-process — ticket re-verification, the ticket spend, the
//! create-and-bind, the one-placement plan apply, the event, and the
//! committed payload — from many concurrent callers, and reports what a
//! platform cares about: how many starts per second are *confirmed*, how
//! many booted sandboxes were refused (and why), and the latency a node
//! waits for its verdict.
//!
//! What it does not include: the network hop and HTTP/2 framing between a
//! node and the manager, and a multi-node raft quorum. The `raft` backend is
//! a single-node bootstrap cluster, so it pays the raft pipeline and the
//! log fsync but not a follower round trip.
//!
//! Two fleet shapes, both one job config (the common sandbox platform: one
//! sandbox job, thousands of keyed instances):
//!
//! - `fleet`: every claim from a different node (256 nodes, round robin),
//!   the shape of thousands of edge nodes starting sandboxes at once.
//! - `one_node`: every claim from one node, the shape of a dense node
//!   starting many sandboxes per second.
//!
//! The manager's runtime is a multi-thread tokio runtime with
//! `LEGION_RATIFY_WORKERS` worker threads (default 4, a 4-vCPU manager).
//!
//! Ignored by default. Run with:
//!
//! ```text
//! cargo test --release --test integration ratify_throughput_measure:: -- --ignored --nocapture
//! LEGION_RATIFY_CLAIMS=4000 LEGION_RATIFY_CONCURRENCY=1,64,256 \
//!   LEGION_RATIFY_BACKENDS=local,raft cargo test --release \
//!   --test integration ratify_throughput_measure:: -- --ignored --nocapture
//! ```
//!
//! Results print as a table and as one JSON line per cell, tagged
//! `ratify_throughput_measure`. Numbers are recorded in the plan (§8) and
//! in `docs/edge-boot.md` ("Ratification under load").

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::WorkloadIdentityState;
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::edge_boot::{RefusalReason, SpeculativeStartConfig};
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig};
use legion::structs::manager_server::Manager;
use legion::structs::{
    EdgeBootConfigFull, GateLevel, InstanceMode, IssueBootTicketRequest, NodeMsg,
    RatifyClaimRequest, Submission, WorkloadIdentityConfigFull,
};
use tonic::Request;

const JOB: &str = "edge-sandboxes";
const TASK: &str = "workspace";
const POOL: &str = "edge";
const ISSUER: &str = "https://manager.test:4747";

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(default)
}

fn store_config(root: &std::path::Path, backend: &str) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    if backend == "raft" {
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-ratify".to_string();
        config.persistence.raft.data_dir = root
            .join("raft")
            .to_str()
            .context("raft data dir is not utf-8")?
            .to_string();
    }
    Ok(config)
}

/// A node large enough that capacity never refuses a claim: the
/// measurement is about the commit path, not about running out of room.
fn edge_node(id: &str) -> NodeMsg {
    NodeMsg {
        id: id.into(),
        status: "active".into(),
        pool: POOL.into(),
        availability: "available".into(),
        last_heartbeat: "now".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "workerAddress": format!("http://{id}:4848"),
            "protocolVersion": 2,
            "resources": { "memory": 4_194_304, "cores": 65_536 },
        })
        .to_string(),
        fingerprint_attrs: json!({
            "memory_mb": 4_194_304,
            "cpu": { "cores": 65_536, "mhz": 2400 },
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
    }
}

fn sandbox_job() -> JobConfig {
    let task = TaskConfig {
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
        speculative_start: Some(SpeculativeStartConfig {
            mode: Some("boot".into()),
            ratify_timeout: Some("5s".into()),
            on_refused: None,
            unknown_fields: Default::default(),
        }),
        resources: json!({ "memory": 64, "cores": 1 }),
        permit: json!({ "pool": POOL }),
        ..TaskConfig::new(TASK, "noop")
    };
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: JOB.into(),
        tasks: vec![task],
        version: None,
        pending_extensions: Default::default(),
    }
}

/// `sum` and `count` of `legion_store_coalesce_batch_size` for one
/// coalescer path, read from the process-wide registry.
fn batch_size_totals(path: &str) -> (f64, f64) {
    let rendered = legion::manager::telemetry::render();
    let pick = |suffix: &str| {
        let prefix = format!("legion_store_coalesce_batch_size_{suffix}{{path=\"{path}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .and_then(|value| value.trim().parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    (pick("sum"), pick("count"))
}

/// User plus system CPU this process has used, in milliseconds. Everything
/// here is the manager's work: the callers only format requests.
fn process_cpu_ms() -> f64 {
    // SAFETY: `getrusage` writes into the zeroed struct we hand it and
    // reads nothing else.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let ms = |time: libc::timeval| time.tv_sec as f64 * 1_000.0 + time.tv_usec as f64 / 1_000.0;
    ms(usage.ru_utime) + ms(usage.ru_stime)
}

fn pctl(sorted_ms: &[f64], percentile: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = (((sorted_ms.len() - 1) as f64) * percentile).round() as usize;
    sorted_ms[idx]
}

struct Cell {
    backend: &'static str,
    shape: &'static str,
    nodes: usize,
    concurrency: usize,
    claims: usize,
    workers: usize,
}

#[derive(Default)]
struct Tally {
    confirmed: usize,
    refused: BTreeMap<String, usize>,
    errors: usize,
    first_refusal: Option<String>,
    latencies_ms: Vec<f64>,
}

fn run_cell(cell: &Cell) -> R<serde_json::Value> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&store_config(dir.path(), cell.backend)?)?;
    for index in 0..cell.nodes {
        store.insert_node(edge_node(&format!("edge-{index}")))?;
    }
    store.handle_job_submit(
        &sandbox_job(),
        &[Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }],
    )?;
    let job_config_id = store.get_job_config(JOB)?.id();

    let identity = Arc::new(WorkloadIdentityState::initialize(
        &store,
        &WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some(ISSUER.into()),
            ..Default::default()
        },
        ISSUER,
        std::time::SystemTime::now(),
    )?);
    let service = Arc::new(
        ManagerDaemonService::new(store.clone())
            .with_workload_identity(Some(identity))
            .with_edge_boot(EdgeBootConfigFull {
                enabled: true,
                ..Default::default()
            }),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cell.workers)
        .enable_all()
        .build()?;

    // One multi-use prefix ticket covers every claim in the cell, the way a
    // platform hands an edge pool a capability for a family of sandboxes.
    let ticket = runtime
        .block_on(
            service.issue_boot_ticket(Request::new(IssueBootTicketRequest {
                job: JOB.into(),
                task: TASK.into(),
                job_config_id: job_config_id.clone(),
                instance_id: String::new(),
                instance_prefix: "sbx-".into(),
                pool: POOL.into(),
                gate: GateLevel::GateBoot as i32,
                ttl: "30m".into(),
                uses: cell.claims as u32,
                idempotency_key: None,
                scope: String::new(),
            })),
        )?
        .into_inner()
        .ticket;

    let (sum_before, count_before) = batch_size_totals("instance_group_commit");
    let next = Arc::new(AtomicUsize::new(0));
    let tally = Arc::new(Mutex::new(Tally::default()));
    let cpu_before = process_cpu_ms();
    let started = Instant::now();
    runtime.block_on(async {
        let mut callers = Vec::with_capacity(cell.concurrency);
        for _ in 0..cell.concurrency {
            let service = service.clone();
            let next = next.clone();
            let tally = tally.clone();
            let ticket = ticket.clone();
            let job_config_id = job_config_id.clone();
            let nodes = cell.nodes;
            let claims = cell.claims;
            callers.push(tokio::spawn(async move {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= claims {
                        break;
                    }
                    let request = RatifyClaimRequest {
                        node_id: format!("edge-{}", index % nodes),
                        claim_id: format!("clm-{index}"),
                        job_config_id: job_config_id.clone(),
                        task_name: TASK.into(),
                        instance_id: format!("sbx-{index}"),
                        gate: GateLevel::GateBoot as i32,
                        ticket: ticket.clone(),
                    };
                    let sent = Instant::now();
                    let reply = service.ratify_claim(Request::new(request)).await;
                    let elapsed_ms = sent.elapsed().as_secs_f64() * 1_000.0;
                    let mut tally = tally.lock().expect("tally");
                    tally.latencies_ms.push(elapsed_ms);
                    match reply {
                        Ok(reply) => {
                            let reply = reply.into_inner();
                            if reply.confirmed {
                                tally.confirmed += 1;
                            } else {
                                let reason = RefusalReason::from_wire(reply.reason);
                                *tally.refused.entry(reason.to_string()).or_default() += 1;
                                if tally.first_refusal.is_none() {
                                    tally.first_refusal = Some(reply.detail);
                                }
                            }
                        }
                        Err(_) => tally.errors += 1,
                    }
                }
            }));
        }
        for caller in callers {
            caller.await.expect("caller task");
        }
    });
    let wall = started.elapsed();
    let cpu_ms = process_cpu_ms() - cpu_before;
    let (sum_after, count_after) = batch_size_totals("instance_group_commit");
    let batches = count_after - count_before;
    let mean_batch = if batches > 0.0 {
        (sum_after - sum_before) / batches
    } else {
        0.0
    };

    let mut tally = std::mem::take(&mut *tally.lock().expect("tally"));
    tally
        .latencies_ms
        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let refused_total: usize = tally.refused.values().sum();
    let confirmed_per_sec = tally.confirmed as f64 / wall.as_secs_f64().max(1e-9);
    // The store is the source of truth: every confirmation must be exactly
    // one live allocation, or the measurement is measuring a bug.
    let allocations = store.get_allocs()?.len();
    assert_eq!(
        allocations, tally.confirmed,
        "confirmed verdicts and live allocations disagree"
    );
    drop(runtime);
    Ok(json!({
        "tag": "ratify_throughput_measure",
        "backend": cell.backend,
        "shape": cell.shape,
        "nodes": cell.nodes,
        "workers": cell.workers,
        "concurrency": cell.concurrency,
        "claims": cell.claims,
        "confirmed": tally.confirmed,
        "refused": refused_total,
        "refused_by_reason": tally.refused,
        "errors": tally.errors,
        "first_refusal": tally.first_refusal,
        "wall_s": wall.as_secs_f64(),
        "confirmed_per_sec": confirmed_per_sec,
        "p50_ms": pctl(&tally.latencies_ms, 0.50),
        "p99_ms": pctl(&tally.latencies_ms, 0.99),
        "max_ms": tally.latencies_ms.last().copied().unwrap_or_default(),
        "mean_batch": mean_batch,
        // Per ratification attempted, confirmed or not.
        "cpu_us_per_claim": cpu_ms * 1_000.0 / cell.claims.max(1) as f64,
    }))
}

#[test]
#[ignore = "throughput measurement; run with --ignored --nocapture"]
fn ratify_throughput() -> R<()> {
    let claims = env_usize("LEGION_RATIFY_CLAIMS", 2_000);
    let workers = env_usize("LEGION_RATIFY_WORKERS", 4);
    let concurrencies = env_list("LEGION_RATIFY_CONCURRENCY", &[1, 16, 64, 256]);
    let backends: Vec<&'static str> = std::env::var("LEGION_RATIFY_BACKENDS")
        .unwrap_or_else(|_| "local,raft".into())
        .split(',')
        .filter_map(|backend| match backend.trim() {
            "local" => Some("local"),
            "raft" => Some("raft"),
            _ => None,
        })
        .collect();
    let shapes: [(&'static str, usize); 2] = [("fleet", 256), ("one_node", 1)];

    println!(
        "{:<6} {:<9} {:>5} {:>9} {:>11} {:>9} {:>8} {:>8} {:>8} {:>6} {:>8}",
        "store",
        "shape",
        "conc",
        "confirmed",
        "confirmed/s",
        "refused",
        "p50_ms",
        "p99_ms",
        "max_ms",
        "batch",
        "cpu_us"
    );
    let mut rows = Vec::new();
    for backend in &backends {
        for (shape, nodes) in shapes {
            for &concurrency in &concurrencies {
                let row = run_cell(&Cell {
                    backend,
                    shape,
                    nodes,
                    concurrency,
                    claims,
                    workers,
                })?;
                println!(
                    "{:<6} {:<9} {:>5} {:>9} {:>11.0} {:>9} {:>8.2} {:>8.2} {:>8.2} {:>6.1} {:>8.0}",
                    backend,
                    shape,
                    concurrency,
                    row["confirmed"],
                    row["confirmed_per_sec"].as_f64().unwrap_or_default(),
                    row["refused"],
                    row["p50_ms"].as_f64().unwrap_or_default(),
                    row["p99_ms"].as_f64().unwrap_or_default(),
                    row["max_ms"].as_f64().unwrap_or_default(),
                    row["mean_batch"].as_f64().unwrap_or_default(),
                    row["cpu_us_per_claim"].as_f64().unwrap_or_default(),
                );
                rows.push(row);
            }
        }
    }
    for row in &rows {
        println!("{row}");
    }
    // Let a raft driver's background threads wind down before the process
    // exits.
    std::thread::sleep(Duration::from_millis(50));
    Ok(())
}
