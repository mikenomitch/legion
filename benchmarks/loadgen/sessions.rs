//! `legion-loadgen --mode sessions`: session-mode start throughput and
//! latency (docs/session-mode.md), against real nodes.
//!
//! Each start is one unnamed session of an existing session job, through one
//! of two paths:
//!
//! - `--via manager`: `CreateInstance` with no id on a manager, which routes
//!   it to a node's envelope (the session router). Spread over every
//!   `--manager` given, round robin.
//! - `--via edge`: `Worker.StartSession` straight to a node's worker plane
//!   with a session ticket (`--ticket`), no manager on the path. Nodes come
//!   from `ListSessionNodes` and are picked round robin; a refused start
//!   (`RESOURCE_EXHAUSTED`) moves to the next node, up to three.
//!
//! Starts go open loop at `--rate` per second (start i is due at
//! begin + i/rate, and its latency is measured from when it was due, so a
//! slow cluster cannot hide queueing behind a slowed sender), or as fast as
//! `--concurrency` allows when the rate is 0.
//!
//! What a start's latency covers depends on `--wait`:
//!
//! - `accepted` (the default for `manager`): the start RPC's reply. For the
//!   manager path that is "the node admitted it"; the runtime boots after.
//! - `running`: until the session runs. The edge path asks the node to wait
//!   (`StartSession.wait`); the manager path polls `GetInstance` every
//!   `--poll-ms`.
//!
//! `--hold-ms` stops each session that long after it started (0 keeps them
//! running, for a density run), and `--stop-at-end` stops whatever is still
//! running once every start is in. Stops are not timed.
//!
//! The summary is JSON: counts by outcome, the achieved start rate over the
//! window from the first start to the last reply, p50/p90/p99/max latency,
//! and completions per second, so a burst's ramp and plateau are visible.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use legion::cmd::helpers::{connect_manager_client_at, AuthedManagerClient, ClientTimeouts};
use legion::structs::worker_client::WorkerClient;
use legion::structs::{
    CreateInstanceRequest, GetInstanceRequest, ListSessionNodesRequest, StartSessionRequest,
    StopInstanceRequest, StopSessionRequest,
};
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tonic::transport::Channel;

/// How many nodes an edge start tries before it counts as refused: the same
/// bound the manager's router uses.
const EDGE_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    Manager,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    Accepted,
    Running,
}

#[derive(Debug, Clone)]
pub struct SessionsOptions {
    pub managers: Vec<String>,
    pub job: String,
    pub task: String,
    pub via: Via,
    pub wait: Wait,
    pub count: usize,
    pub rate: f64,
    pub concurrency: usize,
    pub connections: usize,
    pub ticket: String,
    pub ca: Option<String>,
    pub hold: Duration,
    pub stop_at_end: bool,
    pub poll: Duration,
    pub timeout: Duration,
    pub quiet: bool,
}

/// Where a started session lives, for stopping it.
#[derive(Debug, Clone)]
enum Started {
    Manager(String),
    Edge { session_id: String, node: usize },
}

#[derive(Default)]
struct Tally {
    latencies_us: Vec<u64>,
    /// Completion second (from the first start) -> successful starts.
    per_second: BTreeMap<u64, u64>,
    refused: u64,
    failed: u64,
    errors: BTreeMap<String, u64>,
    started: Vec<Started>,
    last_reply: Option<Instant>,
}

struct Edge {
    nodes: Vec<(String, WorkerClient<Channel>)>,
    next: AtomicUsize,
}

pub async fn run(opts: SessionsOptions) -> Result<Value> {
    if opts.managers.is_empty() {
        bail!("--manager names no manager");
    }
    if opts.count == 0 {
        bail!("--count must be at least 1");
    }
    let timeouts = ClientTimeouts::from_env()?;
    let mut managers: Vec<AuthedManagerClient> = Vec::new();
    for _ in 0..opts.connections.max(1) {
        for address in &opts.managers {
            managers.push(connect_manager_client_at(address, timeouts)?);
        }
    }
    let managers = Arc::new(managers);

    let edge = if opts.via == Via::Edge {
        if opts.ticket.is_empty() {
            bail!("--via edge needs --ticket (legion boot-ticket create <job> --scope session)");
        }
        let mut client = managers[0].clone();
        let reply = client
            .list_session_nodes(ListSessionNodesRequest {
                job_id: opts.job.clone(),
                task_name: opts.task.clone(),
            })
            .await
            .context("ListSessionNodes")?
            .into_inner();
        // The worker plane is verified against the cluster CA. Fetched once
        // from the first manager unless --ca names a bundle.
        let ca = match opts.ca.clone() {
            Some(path) => path,
            None => {
                let bundle = client
                    .get_mesh_ca(legion::structs::GetMeshCaRequest {})
                    .await
                    .context("GetMeshCa (pass --ca to skip)")?
                    .into_inner()
                    .bundle_pem;
                let path = std::env::temp_dir()
                    .join(format!("legion-loadgen-ca-{}.pem", std::process::id()));
                std::fs::write(&path, bundle)?;
                path.display().to_string()
            }
        };
        let mut nodes = Vec::new();
        for node in reply.nodes.iter().filter(|node| node.accepting) {
            // One channel per node, opened up front so no dial lands in the
            // measured window.
            let client = legion::cmd::helpers::worker_plane_client(
                &node.address,
                Some(&node.node_id),
                Some(&ca),
            )
            .await
            .with_context(|| format!("dial {} at {}", node.node_id, node.address))?;
            nodes.push((node.node_id.clone(), client));
        }
        if nodes.is_empty() {
            bail!(
                "no node accepts edge starts for {}/{} ({} listed)",
                opts.job,
                opts.task,
                reply.nodes.len()
            );
        }
        if !opts.quiet {
            eprintln!("legion-loadgen: {} edge node(s)", nodes.len());
        }
        Some(Arc::new(Edge {
            nodes,
            next: AtomicUsize::new(0),
        }))
    } else {
        None
    };

    let tally = Arc::new(Mutex::new(Tally::default()));
    let issued = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let limit = Arc::new(Semaphore::new(opts.concurrency.max(1)));
    let opts = Arc::new(opts);
    let begin = Instant::now();

    let progress = {
        let (done, quiet, total) = (done.clone(), opts.quiet, opts.count);
        tokio::spawn(async move {
            let mut last = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = done.load(Ordering::Relaxed);
                if !quiet {
                    eprintln!(
                        "legion-loadgen: {now}/{total} starts done ({}/s)",
                        now.saturating_sub(last)
                    );
                }
                last = now;
                if now as usize >= total {
                    break;
                }
            }
        })
    };

    let mut handles = Vec::with_capacity(opts.count);
    for index in 0..opts.count {
        let due = if opts.rate > 0.0 {
            let due = begin + Duration::from_secs_f64(index as f64 / opts.rate);
            tokio::time::sleep_until(due.into()).await;
            due
        } else {
            Instant::now()
        };
        // Open loop still caps what is in flight, so a wedged cluster ends
        // in timeouts rather than in an unbounded pile of tasks.
        let permit = limit.clone().acquire_owned().await.expect("semaphore");
        let due = if opts.rate > 0.0 { due } else { Instant::now() };
        let (managers, edge, tally, opts, issued, done) = (
            managers.clone(),
            edge.clone(),
            tally.clone(),
            opts.clone(),
            issued.clone(),
            done.clone(),
        );
        handles.push(tokio::spawn(async move {
            let attempt = issued.fetch_add(1, Ordering::Relaxed) as usize;
            let outcome = start_one(&opts, &managers, edge.as_deref(), attempt).await;
            let finished = Instant::now();
            {
                let mut tally = tally.lock().expect("tally");
                tally.last_reply = Some(finished);
                match &outcome {
                    Ok(started) => {
                        tally
                            .latencies_us
                            .push(finished.duration_since(due).as_micros() as u64);
                        *tally
                            .per_second
                            .entry(finished.duration_since(begin).as_secs())
                            .or_default() += 1;
                        if opts.hold.is_zero() {
                            tally.started.push(started.clone());
                        }
                    }
                    Err((refused, message)) => {
                        if *refused {
                            tally.refused += 1;
                        } else {
                            tally.failed += 1;
                        }
                        *tally.errors.entry(error_class(message)).or_default() += 1;
                    }
                }
            }
            done.fetch_add(1, Ordering::Relaxed);
            drop(permit);
            if let Ok(started) = outcome {
                if !opts.hold.is_zero() {
                    tokio::time::sleep(opts.hold).await;
                    stop_one(&managers, edge.as_deref(), &opts.ticket, &started).await;
                }
            }
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }
    progress.abort();

    let mut tally = std::mem::take(&mut *tally.lock().expect("tally"));
    let window = tally
        .last_reply
        .map(|last| last.duration_since(begin))
        .unwrap_or_default();
    let ok = tally.latencies_us.len();
    tally.latencies_us.sort_unstable();
    let pct = |p: f64| -> Value {
        if tally.latencies_us.is_empty() {
            return Value::Null;
        }
        let rank = ((p / 100.0) * (tally.latencies_us.len() - 1) as f64).round() as usize;
        json!((tally.latencies_us[rank] as f64 / 1000.0 * 100.0).round() / 100.0)
    };
    let summary = json!({
        "via": match opts.via { Via::Manager => "manager", Via::Edge => "edge" },
        "wait": match opts.wait { Wait::Accepted => "accepted", Wait::Running => "running" },
        "job": opts.job,
        "task": opts.task,
        "requested": opts.count,
        "rate_target": opts.rate,
        "concurrency": opts.concurrency,
        "managers": opts.managers.len(),
        "started": ok,
        "refused": tally.refused,
        "failed": tally.failed,
        "window_s": (window.as_secs_f64() * 1000.0).round() / 1000.0,
        "starts_per_sec": if window.is_zero() { 0.0 } else {
            ((ok as f64 / window.as_secs_f64()) * 10.0).round() / 10.0
        },
        "latency_ms": { "p50": pct(50.0), "p90": pct(90.0), "p99": pct(99.0), "max": pct(100.0) },
        "per_second": tally.per_second.values().collect::<Vec<_>>(),
        "errors": tally.errors,
    });

    if opts.stop_at_end && !tally.started.is_empty() {
        if !opts.quiet {
            eprintln!(
                "legion-loadgen: stopping {} session(s)",
                tally.started.len()
            );
        }
        let stops = Arc::new(Semaphore::new(opts.concurrency.max(1)));
        let mut handles = Vec::new();
        for started in tally.started.drain(..) {
            let permit = stops.clone().acquire_owned().await.expect("semaphore");
            let (managers, edge, ticket) = (managers.clone(), edge.clone(), opts.ticket.clone());
            handles.push(tokio::spawn(async move {
                stop_one(&managers, edge.as_deref(), &ticket, &started).await;
                drop(permit);
            }));
        }
        for handle in handles {
            let _ = handle.await;
        }
    }
    Ok(summary)
}

/// One start. `Err((refused, message))`: `refused` for a retryable
/// no-room answer, as opposed to a failure.
async fn start_one(
    opts: &SessionsOptions,
    managers: &[AuthedManagerClient],
    edge: Option<&Edge>,
    attempt: usize,
) -> std::result::Result<Started, (bool, String)> {
    let deadline = tokio::time::Instant::now() + opts.timeout;
    match edge {
        None => {
            let mut client = managers[attempt % managers.len()].clone();
            let reply = tokio::time::timeout_at(
                deadline,
                client.create_instance(CreateInstanceRequest {
                    job_id: opts.job.clone(),
                    task: Some(opts.task.clone()),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|_| (false, "timeout: create".to_string()))?
            .map_err(status_outcome)?
            .into_inner();
            if opts.wait == Wait::Running {
                loop {
                    let info = tokio::time::timeout_at(
                        deadline,
                        client.get_instance(GetInstanceRequest {
                            instance_id: reply.instance_id.clone(),
                        }),
                    )
                    .await
                    .map_err(|_| (false, "timeout: waiting for running".to_string()))?
                    .map_err(status_outcome)?
                    .into_inner();
                    let state = info.instance.map(|i| i.state).unwrap_or_default();
                    match state.as_str() {
                        "running" | "active" => break,
                        "failed" | "terminated" | "lost" => {
                            return Err((false, format!("session ended: {state}")))
                        }
                        _ => tokio::time::sleep(opts.poll).await,
                    }
                }
            }
            Ok(Started::Manager(reply.instance_id))
        }
        Some(edge) => {
            let first = edge.next.fetch_add(1, Ordering::Relaxed);
            let mut last = (true, String::new());
            for hop in 0..EDGE_ATTEMPTS.min(edge.nodes.len()) {
                let node = (first + hop) % edge.nodes.len();
                let mut client = edge.nodes[node].1.clone();
                let reply = tokio::time::timeout_at(
                    deadline,
                    client.start_session(StartSessionRequest {
                        ticket: opts.ticket.clone(),
                        job_id: opts.job.clone(),
                        task_name: opts.task.clone(),
                        wait: Some(opts.wait == Wait::Running),
                        ..Default::default()
                    }),
                )
                .await
                .map_err(|_| (false, "timeout: start".to_string()))?;
                match reply {
                    Ok(reply) => {
                        return Ok(Started::Edge {
                            session_id: reply.into_inner().session_id,
                            node,
                        })
                    }
                    Err(status) => {
                        last = status_outcome(status);
                        if !last.0 {
                            return Err(last);
                        }
                    }
                }
            }
            Err(last)
        }
    }
}

async fn stop_one(
    managers: &[AuthedManagerClient],
    edge: Option<&Edge>,
    ticket: &str,
    started: &Started,
) {
    match started {
        Started::Manager(id) => {
            let mut client = managers[0].clone();
            let _ = client
                .stop_instance(StopInstanceRequest {
                    instance_id: id.clone(),
                    idempotency_key: None,
                })
                .await;
        }
        Started::Edge { session_id, node } => {
            if let Some(edge) = edge {
                let mut client = edge.nodes[*node].1.clone();
                let _ = client
                    .stop_session(StopSessionRequest {
                        ticket: ticket.to_string(),
                        session_id: session_id.clone(),
                    })
                    .await;
            }
        }
    }
}

fn status_outcome(status: tonic::Status) -> (bool, String) {
    (
        status.code() == tonic::Code::ResourceExhausted,
        format!("{:?}: {}", status.code(), status.message()),
    )
}

/// The first few words of an error, so a summary groups thousands of the
/// same refusal into one line instead of thousands.
fn error_class(message: &str) -> String {
    message
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ")
}
