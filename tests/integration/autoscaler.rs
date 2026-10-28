//! Task autoscaling end to end (docs/autoscaling.md; autoscaler-v2 plan §9,
//! Phase 2).
//!
//! Most tests run the autoscaler's engine in process against a real
//! `ManagerDaemonService` served over gRPC, with fake usage heartbeated in
//! for every allocation and the rollup tick run by hand. The engine's
//! clock is driven by the test (one tick per 15 virtual seconds), so the
//! S1 scenario's minutes of stabilization run in seconds.
//!
//! One test runs the real binary: a fresh single manager whose system
//! worker runs the bundled autoscaler app with no worker joined, and with
//! nothing configured for its credentials. It reports running; frozen, it
//! reads NOT RUNNING within three intervals.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use legion::apps::autoscaler::{Engine, EngineOptions};
use legion::cmd::helpers::{connect_manager_client_at, AuthedManagerClient, ClientTimeouts};
use legion::manager::scaling_inputs::RollupTicker;
use legion::manager::server::ManagerDaemonService;
use legion::manager::usage::UsageTable;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::manager_server::ManagerServer;
use legion::structs::{
    AllocationHealth, AllocationUsage, ApplyScalingDefaultsRequest, GetScaleHistoryRequest,
    GetScalingStatusRequest, HeartbeatRequest, NodeMsg, PauseScalingRequest, RunJobRequest,
    ScaleTaskRequest, UsageReport, WatchScalingPoliciesRequest,
};

const MIB: u64 = 1024 * 1024;
/// 500 MHz on a 2400 MHz core, in millicores.
const RESERVED_MILLICORES: f64 = 500.0 / 2400.0 * 1000.0;
/// One virtual tick: the autoscaler's default interval.
const TICK_MS: i64 = 15_000;
/// Where virtual time starts.
const T0: i64 = 1_800_000_000_000;

/// A node with two 2400 MHz cores.
fn node(id: &str) -> NodeMsg {
    NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({ "maxAllocations": 20 }).to_string(),
        fingerprint_attrs: json!({
            "memory_total": (16384 * MIB).to_string(),
            "cpus": {
                "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" },
                "cpu-3": { "frequency": "2400" }, "cpu-4": { "frequency": "2400" }
            }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

/// S1's jobspec: `web`, count 2, 500 MHz, `autoscale` as given.
fn s1(autoscale: Value, count: u32) -> Value {
    json!({
        "name": "web",
        "task": {
            "name": "web",
            "runtime": "exec",
            "count": count,
            "config": { "command": ["sleep", "3600"] },
            "resources": { "cpu": 500, "memory": 64 },
            "autoscale": autoscale,
        }
    })
}

struct Harness {
    _dir: tempfile::TempDir,
    store: Store,
    client: AuthedManagerClient,
    usage: Arc<Mutex<UsageTable>>,
    ticker: RollupTicker,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    async fn start(nodes: usize) -> R<Self> {
        let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir
                .path()
                .join("store")
                .to_str()
                .context("utf-8")?
                .to_string(),
            ..Default::default()
        })?;
        for index in 0..nodes {
            store.insert_node(node(&format!("n{index}")))?;
        }
        let service = ManagerDaemonService::new(store.clone());
        let usage = service.usage_handle();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        let incoming = legion::manager::daemon::manager_incoming(&store, listener, None)?;
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(ManagerServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });
        let client = connect_manager_client_at(
            &addr,
            ClientTimeouts {
                connect: Some(Duration::from_secs(5)),
                request: Some(Duration::from_secs(30)),
            },
        )?;
        Ok(Self {
            _dir: dir,
            store,
            client,
            usage,
            ticker: RollupTicker::new("local"),
            server,
        })
    }

    async fn run(&mut self, spec: Value) -> R<String> {
        let reply = self
            .client
            .run_job(RunJobRequest {
                parsed_jobspec: spec.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|status| anyhow::anyhow!("RunJob: {}", status.message()))?
            .into_inner();
        self.drain_evals()?;
        Ok(reply.message)
    }

    /// Run every claimable eval, as the daemon's eval workers would.
    fn drain_evals(&self) -> R<()> {
        while let Some(eval) = self.store.claim_eval()? {
            let ok = eval.handle(&self.store).is_ok();
            self.store.complete_eval(&eval.id, ok)?;
        }
        Ok(())
    }

    fn allocation_ids(&self, job: &str) -> R<Vec<i64>> {
        let mut ids: Vec<i64> = self
            .store
            .allocs_for_job(job)?
            .into_iter()
            .map(|allocation| allocation.id)
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    fn count(&self, job: &str) -> R<u32> {
        let config = self.store.get_job_config(job)?;
        let task = &config.tasks[0];
        self.store
            .effective_task_count(job, &task.group, &task.name, task.count.num)
    }

    /// Every allocation of `job` reports `utilization` of its reservation
    /// and healthy; then one rollup tick folds it.
    async fn report(&mut self, job: &str, utilization: f64, at_ms: i64) -> R<()> {
        let allocations = self.store.allocs_for_job(job)?;
        let mut nodes: std::collections::BTreeMap<String, Vec<i64>> = Default::default();
        for allocation in &allocations {
            nodes
                .entry(allocation.node_id.clone())
                .or_default()
                .push(allocation.id);
        }
        for (node_id, ids) in nodes {
            self.client
                .heartbeat(HeartbeatRequest {
                    id: node_id,
                    usage: Some(UsageReport {
                        sampled_at_unix_ms: at_ms,
                        allocations: ids
                            .iter()
                            .map(|id| AllocationUsage {
                                allocation_id: *id,
                                cpu_millicores: (utilization * RESERVED_MILLICORES).round() as u64,
                                memory_current_bytes: 32 * MIB,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }),
                    allocation_health: ids
                        .iter()
                        .map(|id| AllocationHealth {
                            allocation_id: *id,
                            healthy: true,
                            status: "running".into(),
                            artifact_states: None,
                        })
                        .collect(),
                    ..Default::default()
                })
                .await
                .map_err(|status| anyhow::anyhow!("heartbeat: {}", status.message()))?;
        }
        self.ticker.tick(&self.store, &self.usage).await;
        Ok(())
    }

    async fn engine(&self, options: EngineOptions) -> R<(Engine, tokio::task::JoinHandle<()>)> {
        let engine = Engine::new(self.client.clone(), options, T0);
        let watch = engine.spawn_watch();
        let deadline = Instant::now() + Duration::from_secs(20);
        while engine.policy_count().unwrap_or(0) == 0 {
            anyhow::ensure!(Instant::now() < deadline, "no policy arrived on the watch");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok((engine, watch))
    }

    async fn history(&mut self, job: &str) -> R<Vec<legion::structs::ScaleHistoryEntry>> {
        Ok(self
            .client
            .get_scale_history(GetScaleHistoryRequest {
                job: job.into(),
                limit: 1000,
                detail: true,
                ..Default::default()
            })
            .await
            .map_err(|status| anyhow::anyhow!("history: {}", status.message()))?
            .into_inner()
            .entries)
    }
}

/// S1 (plan §3): `autoscale = true` on `web` (count 2, 500 MHz). Under load
/// the count rises within a minute, step-limited, to the ceiling of 8; when
/// the load falls it holds for the 5-minute stabilization window, then
/// falls one replica per minute back to the floor of 2. Every change is in
/// the history with the autoscaler's summary, and status says the app runs.
#[tokio::test(flavor = "multi_thread")]
async fn s1_scales_out_under_load_and_back_in_after_the_window_test() -> R<()> {
    let mut h = Harness::start(4).await?;
    h.run(s1(json!(true), 2)).await?;
    assert_eq!(h.allocation_ids("web")?.len(), 2);
    let (mut engine, watch) = h.engine(EngineOptions::default()).await?;

    // Demand in replica-reservations: idle, a spike of 5.6, then quiet.
    let demand = |tick: i64| match tick {
        0..=1 => 1.0,
        2..=19 => 5.6,
        _ => 0.4,
    };
    let mut counts = Vec::new();
    for tick in 0..72 {
        let at = T0 + tick * TICK_MS;
        let replicas = h.allocation_ids("web")?.len().max(1) as f64;
        h.report("web", demand(tick) / replicas, at).await?;
        let report = engine.tick(at).await;
        assert!(report.errors.is_empty(), "tick {tick}: {:?}", report.errors);
        h.drain_evals()?;
        counts.push(h.count("web")?);
    }
    watch.abort();

    // Up within a minute of the spike (ticks 2..=5), to the ceiling.
    assert!(counts[2..=5].iter().any(|count| *count > 2), "{counts:?}");
    assert_eq!(*counts[2..20].iter().max().unwrap_or(&0), 8, "{counts:?}");
    // No scale-in for the 5-minute window after the load fell (tick 20).
    assert!(counts[20..39].iter().all(|count| *count == 8), "{counts:?}");
    // Then down by one replica at a time, at most one per minute.
    let downs: Vec<usize> = (1..counts.len())
        .filter(|&tick| counts[tick] < counts[tick - 1])
        .collect();
    assert!(!downs.is_empty(), "{counts:?}");
    for &tick in &downs {
        assert_eq!(counts[tick - 1] - counts[tick], 1, "{counts:?}");
    }
    for pair in downs.windows(2) {
        assert!(pair[1] - pair[0] >= 4, "faster than 1/min: {counts:?}");
    }
    assert_eq!(
        *counts.last().unwrap_or(&0),
        2,
        "back to the floor: {counts:?}"
    );
    assert_eq!(h.allocation_ids("web")?.len(), 2);

    // The durable record: the autoscaler's writes, each with its summary
    // and a digest that resolves.
    let history = h.history("web").await?;
    let writes: Vec<_> = history
        .iter()
        .filter(|entry| entry.source == "autoscaler" && entry.kind == "scaled")
        .collect();
    assert!(writes.len() >= 8, "{history:?}");
    assert!(writes.iter().all(|entry| entry.summary.starts_with("cpu ")));
    assert!(writes.iter().all(|entry| !entry.policy_json.is_empty()));

    // Status: the app is running, the target is at its floor.
    let status = h
        .client
        .get_scaling_status(GetScalingStatusRequest {
            job: "web".into(),
            detail: true,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(
        status.app.as_ref().map(|app| app.state.as_str()),
        Some("running")
    );
    let target = &status.targets[0];
    assert_eq!((target.now, target.min, target.max), (2, 2, 8));
    assert!(!target.decision_json.is_empty());
    assert!(target
        .policy
        .iter()
        .any(|row| row.field == "cpu" && row.origin == "built-in"));
    Ok(())
}

/// A resubmit that changes only `autoscale`, or the `count` of an
/// autoscaled task, moves the running allocations to the new version: the
/// same allocation ids, no rollout. The submit arbitration keeps the
/// override, rebases it, and clamps it into the new bounds.
#[tokio::test(flavor = "multi_thread")]
async fn in_place_resubmits_keep_allocation_ids_test() -> R<()> {
    let mut h = Harness::start(2).await?;
    h.run(s1(json!(true), 2)).await?;
    // The autoscaler (with ACLs off, any caller may claim it) scales to 3.
    h.client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(3),
            source: "autoscaler".into(),
            ..Default::default()
        })
        .await?;
    h.drain_evals()?;
    let before = h.allocation_ids("web")?;
    assert_eq!(before.len(), 3);

    // Only autoscale changes: in place.
    let message = h.run(s1(json!({ "max": 20 }), 2)).await?;
    assert!(message.contains("in place"), "{message}");
    assert_eq!(h.allocation_ids("web")?, before);
    assert!(h.store.list_rollouts(Some("web"), true)?.is_empty());
    assert_eq!(h.count("web")?, 3);

    // Raising count lifts the floor at once: 3 -> 4, in place.
    let message = h.run(s1(json!({ "max": 20 }), 4)).await?;
    assert!(message.contains("in place"), "{message}");
    assert_eq!(h.count("web")?, 4);
    let after = h.allocation_ids("web")?;
    assert_eq!(after.len(), 4);
    assert!(
        before.iter().all(|id| after.contains(id)),
        "{before:?} {after:?}"
    );

    // Lowering count only lowers the floor: the count stays.
    h.run(s1(json!({ "max": 20 }), 2)).await?;
    assert_eq!(h.count("web")?, 4);

    // A manual count without --for on an autoscaled task is refused with
    // the hint; with it, it is a pin.
    let refused = h
        .client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(6),
            ..Default::default()
        })
        .await
        .expect_err("a person's count on an autoscaled task needs --for");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(refused.message().contains("--for"), "{}", refused.message());
    let pinned = h
        .client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(6),
            pin_for_seconds: Some(3600),
            reason: Some("bad deploy".into()),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(pinned.pinned_until_unix_ms.is_some());
    assert!(
        pinned.message.contains("autoscaling pinned until"),
        "{}",
        pinned.message
    );
    assert_eq!(h.count("web")?, 6);
    // The autoscaler may not move a pinned count.
    let blocked = h
        .client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(5),
            source: "autoscaler".into(),
            ..Default::default()
        })
        .await
        .expect_err("pinned");
    assert!(
        blocked.message().contains("pinned at 6"),
        "{}",
        blocked.message()
    );

    // A change outside autoscale and count still rolls out.
    let mut changed = s1(json!({ "max": 20 }), 2);
    changed["task"]["resources"]["cpu"] = json!(600);
    let message = h.run(changed).await?;
    assert!(!message.contains("in place"), "{message}");
    Ok(())
}

/// Compare-and-set: a stale autoscaler write is refused and changes
/// nothing. A cluster-wide pause stops every autoscaler write (the engine
/// holds, and the server refuses one sent anyway) while people can still
/// scale; resuming lets the engine act again.
#[tokio::test(flavor = "multi_thread")]
async fn cas_conflicts_and_pause_all_stop_writes_test() -> R<()> {
    let mut h = Harness::start(2).await?;
    h.run(s1(json!(true), 2)).await?;
    let first = h
        .client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(3),
            source: "autoscaler".into(),
            expected_version: Some(0),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(first.version > 0);
    let stale = h
        .client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(4),
            source: "autoscaler".into(),
            expected_version: Some(0),
            ..Default::default()
        })
        .await
        .expect_err("a stale version loses");
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    assert!(
        stale.message().contains("version conflict"),
        "{}",
        stale.message()
    );
    h.drain_evals()?;
    assert_eq!(h.count("web")?, 3);

    h.client
        .pause_scaling(PauseScalingRequest {
            all: true,
            reason: "incident".into(),
            ..Default::default()
        })
        .await?;
    let (mut engine, watch) = h.engine(EngineOptions::default()).await?;
    for tick in 0..4 {
        let at = T0 + tick * TICK_MS;
        h.report("web", 2.0, at).await?;
        let report = engine.tick(at).await;
        assert_eq!(report.scaled, 0);
        assert_eq!(
            report.states.get("web/default/web").map(String::as_str),
            Some("paused")
        );
    }
    let refused = h
        .client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(5),
            source: "autoscaler".into(),
            ..Default::default()
        })
        .await
        .expect_err("paused");
    assert!(
        refused.message().contains("paused"),
        "{}",
        refused.message()
    );
    assert_eq!(h.count("web")?, 3);

    h.client
        .pause_scaling(PauseScalingRequest {
            all: true,
            resume: true,
            ..Default::default()
        })
        .await?;
    let at = T0 + 4 * TICK_MS;
    h.report("web", 2.0, at).await?;
    let report = engine.tick(at).await;
    watch.abort();
    assert_eq!(report.scaled, 1, "{report:?}");
    h.drain_evals()?;
    assert!(h.count("web")? > 3);
    Ok(())
}

/// A dry-run policy changes nothing and records what it would have done
/// once (not every tick), as a history row with `applied = false`.
#[tokio::test(flavor = "multi_thread")]
async fn dry_run_records_would_rows_test() -> R<()> {
    let mut h = Harness::start(2).await?;
    h.run(s1(json!({ "mode": "dry-run" }), 2)).await?;
    let (mut engine, watch) = h.engine(EngineOptions::default()).await?;
    for tick in 0..3 {
        let at = T0 + tick * TICK_MS;
        // 130% of the reservation against a 70% target: 2 -> 4 (1.4 would
        // round up past 4.0 in millicores).
        h.report("web", 1.3, at).await?;
        let report = engine.tick(at).await;
        assert_eq!(
            report.states.get("web/default/web").map(String::as_str),
            Some("dry-run")
        );
    }
    watch.abort();
    assert_eq!(h.count("web")?, 2);
    let history = h.history("web").await?;
    let would: Vec<_> = history
        .iter()
        .filter(|entry| entry.kind == "would")
        .collect();
    assert_eq!(would.len(), 1, "{history:?}");
    assert!(!would[0].applied);
    assert_eq!((would[0].from, would[0].to), (2, 4));
    Ok(())
}

/// The cluster's scaling defaults apply live: the policy stream re-sends
/// the new effective policy with no resubmit. A change that would leave a
/// policy invalid is refused (and listed by a dry run); forced, the policy
/// is held `invalid`. A history digest keeps resolving to the policy that
/// made the decision.
#[tokio::test(flavor = "multi_thread")]
async fn defaults_apply_live_and_refuse_invalidating_changes_test() -> R<()> {
    let mut h = Harness::start(2).await?;
    h.run(s1(json!(true), 2)).await?;
    h.client
        .scale_task(ScaleTaskRequest {
            name: "web".into(),
            count: Some(3),
            source: "autoscaler".into(),
            decision: Some(legion::structs::ScaleDecision {
                summary: "test".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    let mut stream = h
        .client
        .watch_scaling_policies(WatchScalingPoliciesRequest::default())
        .await?
        .into_inner();
    let max_of = |set: &legion::structs::ScalingPolicySet| -> Value {
        serde_json::from_str::<Value>(&set.policies[0].policy_json).unwrap_or_default()["max"]
            .clone()
    };
    let first = stream.message().await?.context("a first set")?;
    assert_eq!(max_of(&first), json!(8));

    let applied = h
        .client
        .apply_scaling_defaults(ApplyScalingDefaultsRequest {
            defaults_json: json!({ "max": "6x", "cpu": 0.6 }).to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(applied.applied, "{applied:?}");
    assert_eq!(applied.changes.len(), 1);
    assert!(applied.changes[0].fields.contains(&"max".to_string()));
    let next = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await??
        .context("a second set")?;
    assert_eq!(max_of(&next), json!(12), "live, no resubmit");

    // A cluster floor of 7x (14) without the 6x ceiling: above the built-in
    // ceiling of 4x (8).
    let invalidating = json!({ "cpu": 0.6, "min": "7x" }).to_string();
    let dry = h
        .client
        .apply_scaling_defaults(ApplyScalingDefaultsRequest {
            defaults_json: invalidating.clone(),
            dry_run: true,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(dry.invalid, 1);
    assert!(!dry.applied);
    let refused = h
        .client
        .apply_scaling_defaults(ApplyScalingDefaultsRequest {
            defaults_json: invalidating.clone(),
            ..Default::default()
        })
        .await
        .expect_err("invalidating");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains("web/default/web"),
        "{}",
        refused.message()
    );
    h.client
        .apply_scaling_defaults(ApplyScalingDefaultsRequest {
            defaults_json: invalidating,
            force: true,
            ..Default::default()
        })
        .await?;
    let held = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await??
        .context("a third set")?;
    assert_eq!(held.policies[0].state, "invalid");
    assert!(
        held.policies[0].error.contains("min"),
        "{}",
        held.policies[0].error
    );
    let status = h
        .client
        .get_scaling_status(GetScalingStatusRequest {
            job: "web".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(status.targets[0].state, "invalid");
    Ok(())
}

/// With the app gone, status says NOT RUNNING once three of its intervals
/// pass without a report, and its targets read `stale`.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_app_reads_not_running_after_three_intervals_test() -> R<()> {
    let mut h = Harness::start(1).await?;
    h.run(s1(json!(true), 2)).await?;
    let options = EngineOptions {
        interval: Duration::from_secs(1),
        ..Default::default()
    };
    let (mut engine, watch) = h.engine(options).await?;
    h.report("web", 0.7, T0).await?;
    engine.tick(T0).await;
    watch.abort();
    let state = |reply: &legion::structs::GetScalingStatusReply| {
        reply
            .app
            .as_ref()
            .map(|app| app.state.clone())
            .unwrap_or_default()
    };
    let status = h
        .client
        .get_scaling_status(GetScalingStatusRequest::default())
        .await?
        .into_inner();
    assert_eq!(state(&status), "running");
    tokio::time::sleep(Duration::from_millis(3200)).await;
    let status = h
        .client
        .get_scaling_status(GetScalingStatusRequest::default())
        .await?
        .into_inner();
    assert_eq!(state(&status), "not-running");
    let message = status
        .app
        .as_ref()
        .map(|app| app.message.clone())
        .unwrap_or_default();
    assert!(message.contains("NOT RUNNING"), "{message}");
    assert_eq!(status.targets[0].state, "stale");
    Ok(())
}

/// The real binary: a fresh manager with no worker joined runs the bundled
/// autoscaler on its system worker, which reaches Legion with its own
/// identity with nothing configured (workload identity is on by default,
/// ACLs too) and reports running. Frozen (SIGSTOP), it reads NOT RUNNING
/// within three intervals; thawed, running again.
#[cfg(target_os = "linux")]
#[test]
fn the_app_runs_on_a_fresh_cluster_and_reads_not_running_when_frozen_test() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipped: an exec task in legion-system runs as `nobody`, which needs root");
        return;
    }
    let _serial = crate::system_worker::SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ports = crate::support::DYNAMIC_PORT_GATE.blocking_lock();
    let cluster = crate::system_worker::Cluster::start_with_autoscaler(false);
    let config = cluster.dir().join("autoscaler.json");
    std::fs::write(
        &config,
        json!({ "plugins": {}, "interval": "1s" }).to_string(),
    )
    .expect("write config");
    let set = cluster.cli_json(&[
        "app",
        "config",
        "set",
        "autoscaler",
        "-f",
        config.to_str().expect("utf-8"),
    ]);
    assert_eq!(set["ok"], true, "{set}");
    let app_state = || {
        let status = cluster.cli_json(&["scaling", "status"]);
        (
            status["data"]["app"]["state"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            status["data"]["app"]["intervalSeconds"]
                .as_u64()
                .unwrap_or_default(),
            status["data"]["app"]["message"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        )
    };
    cluster.wait_for("the autoscaler to report running at a 1s interval", || {
        let (state, interval, _) = app_state();
        state == "running" && interval == 1
    });

    let pids = crate::support::task_processes_under(cluster.dir());
    assert!(!pids.is_empty(), "the app runs as a task process");
    for pid in &pids {
        // SAFETY: plain kill(2) on a task process this test started.
        unsafe { libc::kill(*pid, libc::SIGSTOP) };
    }
    let frozen = Instant::now();
    cluster.wait_for("the frozen app to read NOT RUNNING", || {
        app_state().0 == "not-running"
    });
    assert!(frozen.elapsed() < Duration::from_secs(30));
    assert!(app_state().2.contains("NOT RUNNING"));
    for pid in &pids {
        // SAFETY: as above.
        unsafe { libc::kill(*pid, libc::SIGCONT) };
    }
    cluster.wait_for("the thawed app to report running again", || {
        app_state().0 == "running"
    });
}
