//! Raft-mode performance smoke (plan §2 gate support, alongside the
//! `scheduler_benchmark_slo` local_disk gate): a single-node bootstrap raft
//! cluster runs `put_kv` writes through the full propose → quorum-commit →
//! state-machine-apply pipeline and the caller-observed round-trip latency
//! must stay within a generous smoke bound.
//!
//! This is a *smoke*, not a benchmark: the p99 < 250ms assertion exists to
//! catch order-of-magnitude regressions in the replication pipeline (capture
//! mutex contention, per-apply read-model projection cost, fsync pileups),
//! not to track fine-grained performance. Measured numbers are printed and
//! recorded in `docs/multi-raft.md`'s performance section.
//!
//! Also here: the **lanes-vs-single isolation A/B**
//! ([`lanes_vs_single_scheduler_isolation_ab`]) — the measurement behind the
//! multi-raft plan's §Performance claim that independent lanes keep
//! scheduler-lane latency flat while the control and status lanes are
//! saturated. A 3-node in-process cluster (real tonic `RaftTransport`
//! listeners, everything else in-process — the `tests/raft_lanes.rs` harness
//! shape) measures `place_alloc` propose→apply latency on the scheduler lane
//! twice per topology: unloaded, then under a control-lane job-submit burst
//! plus a status-lane heartbeat flood. The same workload runs under
//! `topology = single` for the baseline comparison. Numbers are printed as a
//! table and recorded in `docs/multi-raft.md`'s performance section; the
//! test only asserts sanity (writes really committed and replicated), not a
//! lanes-must-win comparison — the recorded numbers carry that argument.
//!
//! `#[ignore]`d by default, like the SLO gate. Run with:
//! `cargo test --test raft_perf_smoke --locked -- --ignored --nocapture`

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{KvValueType, Node, NodeMsg, Submission};

/// Serializes the perf tests in this binary: latency samples must never be
/// taken while a sibling perf test is saturating the same machine.
static PERF_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn perf_gate() -> std::sync::MutexGuard<'static, ()> {
    PERF_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Manager config for a single-node bootstrap raft cluster rooted at `root`
/// (same shape as `tests/raft_apply_roundtrip.rs`).
fn raft_config(root: &std::path::Path) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-perf".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

/// Percentile over an unsorted latency sample, nearest-rank on a sorted copy
/// (same convention as `tests/scheduler_benchmark_slo.rs`).
fn pctl(latencies_ms: &[f64], percentile: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }
    let mut ordered = latencies_ms.to_vec();
    ordered.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (((ordered.len() - 1) as f64) * percentile).round() as usize;
    ordered[idx]
}

/// Generous smoke bound on caller-observed propose→apply p99 latency.
const MAX_P99_MS: f64 = 250.0;

const SEQUENTIAL_OPS: usize = 1000;
const BURST_THREADS: usize = 8;
const BURST_OPS_PER_THREAD: usize = 100;

#[test]
#[ignore = "performance smoke for raft-mode changes; run with --ignored --nocapture"]
fn raft_single_node_put_kv_latency_smoke() -> R<()> {
    let _serial = perf_gate();
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path())?)?;

    // Sanity: we really are measuring the raft path, not local_disk.
    let status = store.raft_status().context("raft driver must be active")?;
    assert!(status.enabled, "smoke requires persistence.type = raft");
    assert_eq!(status.leader_id.as_deref(), Some("mgr-perf"));

    // Warmup: let the engine settle (first proposals pay one-time costs —
    // log-store page allocation, lazy runtime init) so the measured sample
    // reflects steady state.
    for i in 0..50 {
        store.put_kv(
            &format!("smoke/warmup/{i}"),
            "w",
            KvValueType::String,
            None,
            None,
        )?;
    }

    // Phase 1: 1000 sequential put_kv round-trips. Each call blocks until
    // capture + quorum commit (self-quorum here) + local apply, so the
    // elapsed time per call is exactly the propose→apply round-trip the
    // caller experiences.
    let mut seq_latencies_ms = Vec::with_capacity(SEQUENTIAL_OPS);
    let seq_started = Instant::now();
    for i in 0..SEQUENTIAL_OPS {
        let started = Instant::now();
        store.put_kv(
            &format!("smoke/seq/{i}"),
            "v",
            KvValueType::String,
            None,
            None,
        )?;
        seq_latencies_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
    }
    let seq_elapsed = seq_started.elapsed().as_secs_f64().max(0.000_001);
    let seq_p50 = pctl(&seq_latencies_ms, 0.50);
    let seq_p99 = pctl(&seq_latencies_ms, 0.99);
    let seq_throughput = SEQUENTIAL_OPS as f64 / seq_elapsed;
    println!(
        "raft_perf_smoke sequential ops={SEQUENTIAL_OPS} p50_ms={seq_p50:.3} \
         p99_ms={seq_p99:.3} throughput_ops_per_sec={seq_throughput:.1}"
    );

    // Phase 2: a concurrent burst — 8 threads each firing 100 put_kv ops as
    // fast as they can. This is the shape that exposes capture-mutex /
    // pipeline-queue contention: all callers funnel into the single capture
    // seam, so per-op latency here includes queueing behind peers.
    let burst_started = Instant::now();
    let handles: Vec<_> = (0..BURST_THREADS)
        .map(|thread_idx| {
            let store = store.clone();
            std::thread::spawn(move || -> R<Vec<f64>> {
                let mut latencies_ms = Vec::with_capacity(BURST_OPS_PER_THREAD);
                for i in 0..BURST_OPS_PER_THREAD {
                    let started = Instant::now();
                    store.put_kv(
                        &format!("smoke/burst/{thread_idx}/{i}"),
                        "b",
                        KvValueType::String,
                        None,
                        None,
                    )?;
                    latencies_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
                }
                Ok(latencies_ms)
            })
        })
        .collect();

    let mut burst_latencies_ms = Vec::with_capacity(BURST_THREADS * BURST_OPS_PER_THREAD);
    for handle in handles {
        let latencies = handle
            .join()
            .map_err(|panic| anyhow::anyhow!("burst thread panicked: {panic:?}"))??;
        burst_latencies_ms.extend(latencies);
    }
    let burst_elapsed = burst_started.elapsed().as_secs_f64().max(0.000_001);
    let burst_ops = burst_latencies_ms.len();
    let burst_p50 = pctl(&burst_latencies_ms, 0.50);
    let burst_p99 = pctl(&burst_latencies_ms, 0.99);
    let burst_throughput = burst_ops as f64 / burst_elapsed;
    println!(
        "raft_perf_smoke burst threads={BURST_THREADS} ops={burst_ops} \
         p50_ms={burst_p50:.3} p99_ms={burst_p99:.3} \
         throughput_ops_per_sec={burst_throughput:.1}"
    );

    // Every write really committed: read-your-writes + advancing applied
    // index guard against the smoke silently measuring a no-op path.
    assert_eq!(
        store
            .get_kv(&format!("smoke/seq/{}", SEQUENTIAL_OPS - 1))?
            .context("last sequential write must be readable")?
            .value,
        "v"
    );
    let status_after = store.raft_status().context("status after writes")?;
    assert!(
        status_after.applied_index >= status.applied_index + (SEQUENTIAL_OPS + burst_ops) as u64,
        "applied index must advance by at least one entry per op ({} -> {})",
        status.applied_index,
        status_after.applied_index
    );

    assert!(
        seq_p99 < MAX_P99_MS,
        "sequential p99 {seq_p99:.3}ms exceeds smoke bound {MAX_P99_MS}ms"
    );
    assert!(
        burst_p99 < MAX_P99_MS,
        "burst p99 {burst_p99:.3}ms exceeds smoke bound {MAX_P99_MS}ms"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Lanes-vs-single isolation A/B (multi-raft plan §Performance)
// ─────────────────────────────────────────────────────────────────────────

/// Nodes registered for the A/B (heartbeat-flood targets and placement
/// targets).
const AB_NODES: usize = 6;
/// Scheduler-lane placements measured per phase (baseline / under load).
const AB_MEASURED_OPS: usize = 200;
/// Unmeasured placements to settle the engine before the baseline sample.
const AB_WARMUP_OPS: usize = 20;
/// Control-lane load: threads issuing back-to-back job resubmits.
const AB_SUBMIT_THREADS: usize = 2;
/// Status-lane load: threads issuing back-to-back heartbeats.
const AB_HEARTBEAT_THREADS: usize = 2;

/// One cluster member of the A/B harness: config, store (drop = kill), and
/// the tonic task serving its `RaftTransport` (the `tests/raft_lanes.rs`
/// harness shape, without the kill/restart machinery this smoke never uses).
struct AbNode {
    store: Option<Store>,
    server: Option<(
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    )>,
}

impl AbNode {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("node is running")
    }
}

impl Drop for AbNode {
    fn drop(&mut self) {
        if let Some((handle, shutdown)) = self.server.take() {
            let _ = shutdown.send(());
            handle.abort();
        }
        self.store = None;
    }
}

/// Whether `store`'s node currently leads the group carrying `lane`
/// (`single` topology maps every lane to the one `control` group).
fn leads_lane(store: &Store, lane: &str) -> bool {
    store
        .raft_status()
        .map(|status| {
            let target = if status.lanes.iter().any(|l| l.lane == lane) {
                lane
            } else {
                "control"
            };
            status
                .lanes
                .iter()
                .any(|l| l.lane == target && l.role.as_str() == "primary")
        })
        .unwrap_or(false)
}

/// Run a raft-proposing operation against whichever node currently leads
/// `lane`, with client-style retries (the `tests/raft_durability.rs`
/// CI-hardening posture): under CPU starvation (debug builds, three
/// in-process nodes, 150-300ms election timeouts) a spurious election can
/// move leadership mid-phase, and a real client re-resolves the leader and
/// retries. Only transient raft errors retry; application-level errors
/// return immediately. Returns the value plus how many retries it took, so
/// the measurement can report a polluted sample.
fn with_lane_leader<T>(
    stores: &[Store],
    lane: &str,
    patience: Duration,
    mut op: impl FnMut(&Store) -> R<T>,
) -> R<(T, usize)> {
    let end = Instant::now() + patience;
    let mut retries = 0usize;
    loop {
        let leader = stores.iter().find(|s| leads_lane(s, lane));
        if let Some(store) = leader {
            match op(store) {
                Ok(v) => return Ok((v, retries)),
                Err(err) => {
                    let msg = format!("{err:#}").to_lowercase();
                    let transient = msg.contains("raft leader")
                        || msg.contains("retry shortly")
                        || msg.contains("replication pipeline")
                        || msg.contains("unreachable")
                        || (msg.contains("raft") && msg.contains("timeout"));
                    if !transient || Instant::now() >= end {
                        return Err(err);
                    }
                }
            }
        } else if Instant::now() >= end {
            return Err(anyhow::anyhow!(
                "no node leads the {lane} lane within {patience:?}"
            ));
        }
        retries += 1;
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// Poll `probe` every 100ms until it returns true or `timeout` elapses.
fn wait_for(timeout: Duration, what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if probe() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}

/// Reserve an ephemeral loopback port (rebound by the transport server with
/// SO_REUSEADDR).
fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn ab_node_config(
    root: &Path,
    name: &str,
    port: u16,
    topology: &str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<ManagerConfigFull> {
    let dir = root.join(name);
    let mut config = ManagerConfigFull {
        store_path: dir
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    // Relaxed election timing for an in-process MULTI-NODE cluster. Every
    // node here shares one process and, in CI, as few as two cores in a
    // debug build, so a node can be descheduled for far longer than the
    // production defaults (250 / 600-1200 ms) tolerate: followers time out
    // against a leader that is merely starved, campaign, and the cluster
    // flaps between terms instead of converging. These values absorb
    // multi-hundred-millisecond scheduling stalls while keeping failover
    // fast enough that the tests stay quick.
    //
    // The window is WIDENED, not just inflated: `electionTimeoutMinMs` is
    // what buys starvation tolerance (it doubles the default's patience
    // before a follower campaigns), while `electionTimeoutMaxMs` is also
    // the follower leader-lease and is therefore kept close to the
    // minimum. Pushing the max to 3s made deliberate transfers
    // (`move_lane_leadership`) time out: a candidate cannot win while the
    // incumbent's lease is live, and the incumbent refreshes it on every
    // heartbeat.
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 1200;
    config.persistence.raft.election_timeout_max_ms = 2000;
    config.persistence.raft.topology = topology.to_string();
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    // Far above anything this smoke writes: compaction/snapshot churn must
    // not pollute the latency samples.
    config.persistence.raft.snapshot_entries = 100_000;
    config.persistence.raft.data_dir = dir
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn ab_start_node(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    name: &str,
    topology: &str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<AbNode> {
    let port = reserve_port();
    let config = ab_node_config(root, name, port, topology, bootstrap, join_token)?;
    let store = Store::start_storage(&config)?;
    let service = store.raft_transport_server();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = rt.spawn(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("bind transport port");
        let listener = socket.listen(64).expect("listen");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    // Let the listener reach accept() before anyone dials it.
    std::thread::sleep(Duration::from_millis(50));
    Ok(AbNode {
        store: Some(store),
        server: Some((handle, shutdown_tx)),
    })
}

/// Bring up a 3-node cluster under `topology` and wait until both joiners
/// are voters and every active lane has a leader on every node.
fn ab_start_cluster(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    topology: &str,
) -> R<(AbNode, AbNode, AbNode)> {
    let a = ab_start_node(rt, root, "mgr-a", topology, true, None)?;
    let (token, _expires) = a.store().issue_raft_join_token("perf-ab")?;
    let b = ab_start_node(rt, root, "mgr-b", topology, false, Some(token.clone()))?;
    let c = ab_start_node(rt, root, "mgr-c", topology, false, Some(token))?;
    wait_for(
        Duration::from_secs(120),
        "both joiners promoted to voter and every lane led on every node",
        || {
            let voters_ok = a
                .store()
                .raft_status()
                .map(|s| {
                    s.voters.contains(&"mgr-b".to_string())
                        && s.voters.contains(&"mgr-c".to_string())
                })
                .unwrap_or(false);
            voters_ok
                && [&a, &b, &c].iter().all(|node| {
                    node.store()
                        .raft_status()
                        .map(|s| {
                            !s.lanes.is_empty()
                                && s.lanes.iter().all(|lane| lane.leader_id.is_some())
                        })
                        .unwrap_or(false)
                })
        },
    );
    Ok((a, b, c))
}

fn ab_node_msg(id: &str) -> NodeMsg {
    NodeMsg::from(Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        config_attrs: json!({ "maxAllocations": 10000, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    })
}

fn ab_job(name: &str) -> JobConfig {
    let mut task = TaskConfig::new("svc", "noop");
    task.count = legion::structs::job::TaskCount::new(1);
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![task],
        version: None,
    }
}

fn ab_submission(name: &str, content: &str) -> Submission {
    Submission {
        path: format!("{name}.jsonnet"),
        content: content.to_string(),
        is_primary: true,
    }
}

/// One phase's latency sample: measured `place_alloc` round-trips against
/// the scheduler lane's current leader, rotating placements across the
/// registered nodes. Leadership blips retry client-style; an op that
/// needed a retry is EXCLUDED from the latency vector (its elapsed time is
/// dominated by the client-failover backoff sleep, not the propose→apply
/// pipeline this smoke measures) and counted, so the caller can report how
/// many samples were dropped.
fn measure_placements(
    stores: &[Store],
    nodes: &[String],
    task_id: i64,
    ops: usize,
) -> R<(Vec<f64>, f64, usize)> {
    let mut latencies_ms = Vec::with_capacity(ops);
    let mut retried_ops = 0usize;
    let started = Instant::now();
    for i in 0..ops {
        let node = &nodes[i % nodes.len()];
        let op_started = Instant::now();
        let (_, retries) = with_lane_leader(stores, "scheduler", Duration::from_secs(45), |s| {
            s.place_alloc(node, task_id)
        })?;
        if retries == 0 {
            latencies_ms.push(op_started.elapsed().as_secs_f64() * 1_000.0);
        } else {
            retried_ops += 1;
        }
    }
    Ok((
        latencies_ms,
        started.elapsed().as_secs_f64().max(0.000_001),
        retried_ops,
    ))
}

/// The numbers one topology produces.
struct AbOutcome {
    topology: &'static str,
    baseline_p50_ms: f64,
    baseline_p99_ms: f64,
    baseline_ops_per_sec: f64,
    loaded_p50_ms: f64,
    loaded_p99_ms: f64,
    loaded_ops_per_sec: f64,
    /// Control-lane job resubmits committed while the loaded sample ran.
    submit_ops: usize,
    /// Status-lane heartbeats committed while the loaded sample ran.
    heartbeat_ops: usize,
    loaded_elapsed_s: f64,
    /// Measured ops excluded from the samples because a leadership blip
    /// forced a client-style retry (non-zero = churn during the run).
    measure_retries: usize,
}

/// Run the full scenario under one topology: bring up a 3-node cluster,
/// sample scheduler-lane placement latency unloaded, then again while the
/// control lane (job-submit burst) and status lane (heartbeat flood) are
/// saturated, and verify every write really committed and replicated.
fn run_isolation_scenario(topology: &'static str) -> R<AbOutcome> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = ab_start_cluster(&rt, dir.path(), topology)?;
    // Every proposal targets whichever node currently leads the op's lane
    // (leadership can move under CPU starvation; a real client re-resolves).
    let stores: Vec<Store> = [&a, &b, &c].iter().map(|n| n.store().clone()).collect();

    // Seed: placement/heartbeat targets and the measured job's task config.
    // (These are cross-lane wrapper composites, proposed from the node
    // leading the control lane — right after bring-up that node leads every
    // lane, which the composite's status-lane half needs too.)
    let nodes: Vec<String> = (0..AB_NODES).map(|i| format!("ab-node-{i}")).collect();
    for node in &nodes {
        with_lane_leader(&stores, "control", Duration::from_secs(45), |s| {
            s.insert_node(ab_node_msg(node))
        })?;
    }
    let (outcome, _) = with_lane_leader(&stores, "control", Duration::from_secs(45), |s| {
        s.handle_job_submit(
            &ab_job("ab-sched-job"),
            &[ab_submission("ab-sched-job", "v1")],
        )
    })?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = a.store().get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks
        .first()
        .context("measured job must have a task config")?;

    // Warmup + unloaded baseline.
    measure_placements(&stores, &nodes, task_id, AB_WARMUP_OPS)?;
    let (baseline_ms, baseline_elapsed, baseline_retries) =
        measure_placements(&stores, &nodes, task_id, AB_MEASURED_OPS)?;

    // Cross-lane load: control-lane job-submit burst + status-lane
    // heartbeat flood, all proposed on the leader, running until the
    // measured sample completes.
    // Load threads follow their lane's leader too, and are best-effort: a
    // thread that exhausts its retry patience stops early and reports how
    // far it got (the sanity check below still requires real committed
    // load), instead of failing the whole A/B.
    let stop = Arc::new(AtomicBool::new(false));
    let mut submit_handles = Vec::new();
    for thread_idx in 0..AB_SUBMIT_THREADS {
        let stores = stores.clone();
        let stop = stop.clone();
        submit_handles.push(std::thread::spawn(move || -> (usize, Option<String>) {
            let job_name = format!("ab-burst-{thread_idx}");
            let mut committed = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let version = committed + 1;
                let result = with_lane_leader(&stores, "control", Duration::from_secs(45), |s| {
                    s.handle_job_submit(
                        &ab_job(&job_name),
                        &[ab_submission(&job_name, &format!("{job_name} v{version}"))],
                    )
                });
                match result {
                    Ok(_) => committed += 1,
                    Err(err) => return (committed, Some(format!("{err:#}"))),
                }
            }
            (committed, None)
        }));
    }
    let mut heartbeat_handles = Vec::new();
    for thread_idx in 0..AB_HEARTBEAT_THREADS {
        let stores = stores.clone();
        let stop = stop.clone();
        let nodes = nodes.clone();
        heartbeat_handles.push(std::thread::spawn(move || -> (usize, Option<String>) {
            let mut committed = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let node = &nodes[(thread_idx + committed) % nodes.len()];
                let result = with_lane_leader(&stores, "status", Duration::from_secs(45), |s| {
                    s.update_node_heartbeat(node.clone())
                });
                match result {
                    Ok(_) => committed += 1,
                    Err(err) => return (committed, Some(format!("{err:#}"))),
                }
            }
            (committed, None)
        }));
    }
    // Let the load reach a steady state before sampling.
    std::thread::sleep(Duration::from_millis(500));

    let (loaded_ms, loaded_elapsed, loaded_retries) =
        measure_placements(&stores, &nodes, task_id, AB_MEASURED_OPS)?;

    stop.store(true, Ordering::Relaxed);
    let mut submit_ops = 0usize;
    for handle in submit_handles {
        let (committed, err) = handle
            .join()
            .map_err(|panic| anyhow::anyhow!("submit-burst thread panicked: {panic:?}"))?;
        submit_ops += committed;
        if let Some(err) = err {
            println!("WARNING [{topology}]: submit-burst thread stopped early after {committed} ops: {err}");
        }
    }
    let mut heartbeat_ops = 0usize;
    for handle in heartbeat_handles {
        let (committed, err) = handle
            .join()
            .map_err(|panic| anyhow::anyhow!("heartbeat-flood thread panicked: {panic:?}"))?;
        heartbeat_ops += committed;
        if let Some(err) = err {
            println!("WARNING [{topology}]: heartbeat-flood thread stopped early after {committed} ops: {err}");
        }
    }

    // Sanity: every measured placement really committed and replicates —
    // the smoke must never silently measure a rejected or no-op path. A
    // leadership-blip retry can double-place (the retried op may have
    // committed before erroring), so the count is `>=` and the real check
    // is that all three replicas CONVERGE on the same count.
    let expected_allocs = AB_WARMUP_OPS + 2 * AB_MEASURED_OPS;
    let count_allocs = |store: &Store| -> R<usize> {
        let mut total = 0;
        for node in &nodes {
            total += store.allocs_for_node(node)?.len();
        }
        Ok(total)
    };
    {
        let counts = || -> R<Vec<usize>> { stores.iter().map(&count_allocs).collect() };
        wait_for(
            Duration::from_secs(120),
            "all replicas converge on the same placement count",
            || {
                counts()
                    .map(|c| c[0] >= expected_allocs && c.iter().all(|n| *n == c[0]))
                    .unwrap_or(false)
            },
        );
    }

    Ok(AbOutcome {
        topology,
        baseline_p50_ms: pctl(&baseline_ms, 0.50),
        baseline_p99_ms: pctl(&baseline_ms, 0.99),
        baseline_ops_per_sec: AB_MEASURED_OPS as f64 / baseline_elapsed,
        loaded_p50_ms: pctl(&loaded_ms, 0.50),
        loaded_p99_ms: pctl(&loaded_ms, 0.99),
        loaded_ops_per_sec: AB_MEASURED_OPS as f64 / loaded_elapsed,
        submit_ops,
        heartbeat_ops,
        loaded_elapsed_s: loaded_elapsed,
        measure_retries: baseline_retries + loaded_retries,
    })
}

/// The lane-isolation A/B (multi-raft plan §Performance): scheduler-lane
/// placement latency, unloaded vs under control+status cross-lane load,
/// under `topology = lanes` and `topology = single`. Prints the comparison
/// table; the recorded numbers (docs/multi-raft.md) carry the verdict —
/// this test asserts only sanity, not a lanes-must-win comparison.
#[test]
#[ignore = "performance smoke for raft-mode changes; run with --ignored --nocapture"]
fn lanes_vs_single_scheduler_isolation_ab() -> R<()> {
    let _serial = perf_gate();
    let single = run_isolation_scenario("single")?;
    let lanes = run_isolation_scenario("lanes")?;

    println!(
        "\nraft_perf_smoke lanes-vs-single A/B: scheduler-lane place_alloc \
         propose→apply latency, 3-node in-process cluster\n\
         (cross-lane load: {AB_SUBMIT_THREADS} job-submit burst threads on the control lane + \
         {AB_HEARTBEAT_THREADS} heartbeat flood threads on the status lane; \
         {AB_MEASURED_OPS} measured placements per phase)\n"
    );
    println!(
        "| topology | phase      | p50 (ms) | p99 (ms) | placements/s | cross-lane load committed (load window) |"
    );
    println!(
        "| -------- | ---------- | -------- | -------- | ------------ | --------------------------------------- |"
    );
    for outcome in [&single, &lanes] {
        println!(
            "| {:<8} | baseline   | {:>8.3} | {:>8.3} | {:>12.1} | (none)                                  |",
            outcome.topology,
            outcome.baseline_p50_ms,
            outcome.baseline_p99_ms,
            outcome.baseline_ops_per_sec,
        );
        println!(
            "| {:<8} | cross-load | {:>8.3} | {:>8.3} | {:>12.1} | {} submits + {} heartbeats (sample {:.1}s) |",
            outcome.topology,
            outcome.loaded_p50_ms,
            outcome.loaded_p99_ms,
            outcome.loaded_ops_per_sec,
            outcome.submit_ops,
            outcome.heartbeat_ops,
            outcome.loaded_elapsed_s,
        );
    }
    let single_degradation = single.loaded_p99_ms / single.baseline_p99_ms.max(0.001);
    let lanes_degradation = lanes.loaded_p99_ms / lanes.baseline_p99_ms.max(0.001);
    println!(
        "\nscheduler p99 degradation under cross-lane load: single {single_degradation:.2}x, \
         lanes {lanes_degradation:.2}x"
    );
    for outcome in [&single, &lanes] {
        if outcome.measure_retries > 0 {
            println!(
                "NOTE: {} run excluded {} measured op(s) that hit a leadership-blip retry \
                 (leadership churned during the run; percentiles cover the clean samples)",
                outcome.topology, outcome.measure_retries
            );
        }
    }

    // The load threads must have produced real pressure in both topologies,
    // or the A/B compared nothing.
    for outcome in [&single, &lanes] {
        assert!(
            outcome.submit_ops > 0 && outcome.heartbeat_ops > 0,
            "{}: cross-lane load threads must commit ops during the sample",
            outcome.topology
        );
    }
    Ok(())
}
