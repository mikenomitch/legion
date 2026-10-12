//! `legion-loadgen --mode instances`: the keyed-instance lifecycle load
//! generator (sandbox-scaling plan, Phase 0).
//!
//! Each *session* is one keyed sandbox driven through `CreateInstance`, its
//! placement, `SuspendInstance`, `StartInstance` (a resume from the
//! checkpoint), and `DestroyInstance`. Sessions start open loop at `--rate`
//! per second (session i at start + i/rate, whatever the manager's latency),
//! or as fast as `--concurrency` allows when the rate is 0.
//!
//! The worker side is a simulated fleet embedded in this process
//! (`--simulate N`, the same code as `legion start worker --simulate`) with
//! the checkpoint channel faked: every node advertises `checkpoint`, answers
//! capture and delete requests, and reports restores as done. Embedding it
//! is what lets one process time the whole path: the manager's commit (from
//! `StreamEvents`) and the moment the allocation reached a node (from the
//! fleet's observer), on one clock.
//!
//! What it reports, per stage, at p50/p90/p99/max:
//!
//! - `create_rpc`: the `CreateInstance` call.
//! - `placement`: create sent → `allocation.placed` received (the placement
//!   decision committed, as the event stream saw it).
//! - `dispatch`: `allocation.placed` received → the node holds the payload.
//! - `create_to_running`: create sent → the node holds the payload (the
//!   control plane's share of a cold boot; the simulated node "boots" in
//!   `--ready-after`).
//! - `suspend`: `SuspendInstance` sent → `instance.suspended`. The capture
//!   request and its report each ride a heartbeat, so this includes up to
//!   two heartbeat intervals by design.
//! - `resume_dispatch`: `StartInstance` sent → the node holds the restore
//!   payload.
//! - `resume`: `StartInstance` sent → `instance.resumed`.
//! - `destroy_rpc`: the `DestroyInstance` call.
//!
//! plus sustained sessions per second and the error count. Numbers and the
//! method behind them are recorded in `benchmarks/scheduler/README.md`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::stream::StreamExt;
use legion::cmd::helpers::{connect_manager_client, AuthedManagerClient};
use legion::structs::{
    CreateInstanceRequest, DestroyInstanceRequest, RunJobRequest, StartInstanceRequest,
    StreamEventsRequest, Submission, SuspendInstanceRequest,
};
use legion::worker::simulate::{launch_simulated_fleet, SimArrival, SimObserver, SimulateOptions};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::Notify;

/// Options for one instances-mode run.
#[derive(Debug, Clone)]
pub struct InstancesOptions {
    pub manager: String,
    pub job: String,
    pub sessions: usize,
    pub rate: f64,
    pub concurrency: usize,
    pub connections: usize,
    pub simulate: usize,
    pub worker_type: String,
    pub join_secret: Option<String>,
    pub hold: Duration,
    pub ready_after: Duration,
    pub stage_timeout: Duration,
    pub submit_job: bool,
    pub quiet: bool,
    /// Address (`host` or `host:port`) the manager dials for `PostWork` to
    /// the embedded fleet; `None` auto-detects it. Only a worker off the
    /// work stream is pushed to, so this matters only while a stream is
    /// down; on the AWS rig it is the loadgen box's private IP.
    pub advertise_addr: Option<String>,
    /// Node-id prefix for the embedded fleet. A campaign running several
    /// cells passes a fixed one so every cell re-registers the same nodes
    /// instead of leaving the previous cell's to go lost.
    pub sim_name_prefix: String,
    /// `node_profiles` JSON for the embedded fleet (the benchmark scenario
    /// schema); `None` is the simulator's single default profile.
    pub sim_profiles: Option<String>,
}

/// What the run saw for one instance, filled in by the event stream and the
/// fleet observer as they arrive.
#[derive(Debug, Default, Clone)]
struct Track {
    /// Allocation ids the node received for this instance, in order, with
    /// when.
    arrivals: Vec<(i64, Instant)>,
    suspended: Option<Instant>,
    resumed: Option<Instant>,
}

/// Shared observation state: per-instance tracks, and when each allocation's
/// `allocation.placed` landed (keyed by allocation id, because the event
/// carries no instance).
#[derive(Default)]
struct Observed {
    tracks: Mutex<HashMap<String, Track>>,
    placed_at: Mutex<HashMap<i64, Instant>>,
    changed: Notify,
}

impl Observed {
    fn update(&self, instance_id: &str, f: impl FnOnce(&mut Track)) {
        {
            let mut tracks = self.tracks.lock().expect("tracks lock");
            f(tracks.entry(instance_id.to_string()).or_default());
        }
        self.changed.notify_waiters();
    }

    /// Wait until `probe` returns something for `instance_id`.
    async fn wait<T>(
        &self,
        instance_id: &str,
        timeout: Duration,
        what: &str,
        probe: impl Fn(&Track) -> Option<T>,
    ) -> Result<T> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Registered before the check, so a change between the check and
            // the wait still wakes this waiter.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(found) = self
                .tracks
                .lock()
                .expect("tracks lock")
                .get(instance_id)
                .and_then(&probe)
            {
                return Ok(found);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                bail!("{instance_id}: timed out after {timeout:?} waiting for {what}");
            }
        }
    }

    fn placed_at(&self, allocation_id: i64) -> Option<Instant> {
        self.placed_at
            .lock()
            .expect("placed lock")
            .get(&allocation_id)
            .copied()
    }
}

/// Stage latencies for one completed session, in milliseconds.
#[derive(Debug, Default, Clone)]
struct SessionTimes {
    create_rpc: f64,
    placement: Option<f64>,
    dispatch: Option<f64>,
    create_to_running: f64,
    suspend: f64,
    resume_dispatch: f64,
    resume: f64,
    destroy_rpc: f64,
}

#[derive(Debug, Serialize)]
struct Distribution {
    count: usize,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn distribution(mut samples: Vec<f64>) -> Distribution {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        let rank = ((p / 100.0) * samples.len() as f64).ceil() as usize;
        samples[rank.clamp(1, samples.len()) - 1]
    };
    Distribution {
        count: samples.len(),
        p50_ms: round2(pct(50.0)),
        p90_ms: round2(pct(90.0)),
        p99_ms: round2(pct(99.0)),
        max_ms: round2(samples.last().copied().unwrap_or(0.0)),
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// The keyed sandbox job every session is an instance of. Memory
/// checkpoints, small reservations so the fleet's size, not its capacity,
/// bounds the run.
fn jobspec(job: &str) -> String {
    json!({
        "name": job,
        "tasks": [{
            "name": "workspace",
            "runtime": "sandbox",
            "stateful": { "mode": "keyed" },
            "config": { "image": "alpine", "command": ["sleep", "3600"] },
            "resources": { "cpu": 10, "memory": 64 },
            "suspend": { "checkpoint": "memory" },
        }],
    })
    .to_string()
}

pub async fn run(opts: InstancesOptions) -> Result<Value> {
    if opts.concurrency == 0 || opts.connections == 0 || opts.sessions == 0 {
        bail!("--concurrency, --connections, and --sessions must all be at least 1");
    }
    if !opts.rate.is_finite() || opts.rate < 0.0 {
        bail!("--rate must be a non-negative number of sessions/sec");
    }

    let mut clients = Vec::with_capacity(opts.connections);
    for _ in 0..opts.connections {
        clients.push(connect_manager_client().await?);
    }
    let clients = Arc::new(clients);

    if opts.submit_job {
        let spec = jobspec(&opts.job);
        clients[0]
            .clone()
            .run_job(tonic::Request::new(RunJobRequest {
                parsed_jobspec: spec.clone(),
                submissions: vec![Submission {
                    path: format!("{}.json", opts.job),
                    content: spec,
                    is_primary: true,
                }],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
            .await
            .context("submitting the instances job")?;
    }

    let observed = Arc::new(Observed::default());

    // The event stream: placement commits, suspends, and resumes.
    {
        let mut client = clients[0].clone();
        let observed = observed.clone();
        let job = opts.job.clone();
        let mut stream = client
            .stream_events(tonic::Request::new(StreamEventsRequest {
                after_id: None,
                kind: None,
                job_id: Some(job),
                node_id: None,
                level: None,
                ..Default::default()
            }))
            .await
            .context("opening StreamEvents")?
            .into_inner();
        tokio::spawn(async move {
            while let Some(Ok(event)) = stream.next().await {
                let now = Instant::now();
                let meta: Value = serde_json::from_str(&event.metadata_json).unwrap_or(Value::Null);
                match event.kind.as_str() {
                    "allocation.placed" => {
                        let mut placed = observed.placed_at.lock().expect("placed lock");
                        for placement in meta["placements"].as_array().into_iter().flatten() {
                            if let Some(id) = placement["allocationId"].as_i64() {
                                placed.entry(id).or_insert(now);
                            }
                        }
                        drop(placed);
                        observed.changed.notify_waiters();
                    }
                    "instance.suspended" => {
                        if let Some(id) = meta["instanceId"].as_str() {
                            observed.update(id, |track| track.suspended = Some(now));
                        }
                    }
                    "instance.resumed" => {
                        if let Some(id) = meta["instanceId"].as_str() {
                            observed.update(id, |track| track.resumed = Some(now));
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    // The embedded fleet. The observer records each allocation's arrival
    // under its instance.
    let fleet = {
        let observed = observed.clone();
        let observer = SimObserver(Arc::new(move |arrival: SimArrival| {
            if arrival.instance_id.is_empty() {
                return;
            }
            observed.update(&arrival.instance_id, |track| {
                track.arrivals.push((arrival.allocation_id, arrival.at));
            });
        }));
        launch_simulated_fleet(
            opts.manager.clone(),
            opts.worker_type.clone(),
            opts.join_secret.clone(),
            SimulateOptions {
                count: opts.simulate,
                profiles_path: opts.sim_profiles.clone(),
                name_prefix: opts.sim_name_prefix.clone(),
                ready_after: opts.ready_after,
                register_rate: 1000.0,
                advertise_addr: opts.advertise_addr.clone(),
                checkpoints: true,
                observer: Some(observer),
                ..Default::default()
            },
        )
        .await?
    };
    let fleet_deadline = Instant::now() + Duration::from_secs(60);
    while (fleet.registered() as usize) < opts.simulate {
        if Instant::now() > fleet_deadline {
            bail!(
                "only {} of {} simulated nodes registered within 60s",
                fleet.registered(),
                opts.simulate
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // One heartbeat round so every node is `ready` and schedulable.
    tokio::time::sleep(Duration::from_secs(3)).await;

    if !opts.quiet {
        eprintln!(
            "legion-loadgen instances: {} sessions, rate {}/s, concurrency {}, {} sim nodes",
            opts.sessions, opts.rate, opts.concurrency, opts.simulate
        );
    }

    let completed = Arc::new(AtomicU64::new(0));
    let running = Arc::new(Running::default());
    let started = Instant::now();
    let progress = if opts.quiet {
        None
    } else {
        let completed = completed.clone();
        let total = opts.sessions;
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.tick().await;
            loop {
                tick.tick().await;
                eprintln!(
                    "legion-loadgen instances: {}/{total} sessions done ({:.1}s)",
                    completed.load(Ordering::Relaxed),
                    started.elapsed().as_secs_f64()
                );
            }
        }))
    };

    let outcomes: Vec<Result<SessionTimes, String>> = futures::stream::iter(0..opts.sessions)
        .map(|index| {
            let mut client = clients[index % clients.len()].clone();
            let observed = observed.clone();
            let opts = opts.clone();
            let completed = completed.clone();
            let running = running.clone();
            async move {
                if opts.rate > 0.0 {
                    let due = started + Duration::from_secs_f64(index as f64 / opts.rate);
                    tokio::time::sleep_until(due.into()).await;
                }
                let outcome = session(&mut client, &observed, &opts, &running)
                    .await
                    .map_err(|err| format!("{err:#}"));
                completed.fetch_add(1, Ordering::Relaxed);
                outcome
            }
        })
        .buffer_unordered(if opts.rate > 0.0 {
            // Open loop: never let the in-flight cap hold back an arrival.
            opts.sessions.max(1)
        } else {
            opts.concurrency
        })
        .collect()
        .await;
    let elapsed = started.elapsed();
    if let Some(progress) = progress {
        progress.abort();
    }
    fleet.abort();

    let mut errors: HashMap<String, usize> = HashMap::new();
    let mut times = Vec::new();
    for outcome in outcomes {
        match outcome {
            Ok(session) => times.push(session),
            Err(err) => {
                let key = err.chars().take(160).collect::<String>();
                *errors.entry(key).or_insert(0) += 1;
            }
        }
    }
    let ok = times.len();
    let column = |f: &dyn Fn(&SessionTimes) -> Option<f64>| -> Distribution {
        distribution(times.iter().filter_map(f).collect())
    };
    let mut top_errors: Vec<(String, usize)> = errors.into_iter().collect();
    top_errors.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    top_errors.truncate(5);
    Ok(json!({
        "mode": "instances",
        "ok": top_errors.is_empty(),
        "sessions": opts.sessions,
        "completed": ok,
        "failed": opts.sessions - ok,
        "elapsed_secs": round2(elapsed.as_secs_f64()),
        "sessions_per_sec": round2(ok as f64 / elapsed.as_secs_f64().max(0.001)),
        "target_rate_per_sec": opts.rate,
        "concurrency": opts.concurrency,
        "simulated_nodes": opts.simulate,
        "hold_ms": ms(opts.hold),
        "peak_running": running.peak.load(Ordering::Relaxed),
        "stages": {
            "create_rpc": column(&|t| Some(t.create_rpc)),
            "placement": column(&|t| t.placement),
            "dispatch": column(&|t| t.dispatch),
            "create_to_running": column(&|t| Some(t.create_to_running)),
            "suspend": column(&|t| Some(t.suspend)),
            "resume_dispatch": column(&|t| Some(t.resume_dispatch)),
            "resume": column(&|t| Some(t.resume)),
            "destroy_rpc": column(&|t| Some(t.destroy_rpc)),
        },
        "errors": top_errors.iter().map(|(e, n)| json!({ "error": e, "count": n })).collect::<Vec<_>>(),
    }))
}

/// One session, start to finish.
/// Sessions between "running" and their suspend, and the most there ever
/// were at once: the resident count a density cell holds.
#[derive(Default)]
struct Running {
    now: AtomicU64,
    peak: AtomicU64,
}

impl Running {
    fn enter(&self) {
        let now = self.now.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    fn leave(&self) {
        self.now.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn session(
    client: &mut AuthedManagerClient,
    observed: &Observed,
    opts: &InstancesOptions,
    running: &Running,
) -> Result<SessionTimes> {
    let mut times = SessionTimes::default();

    let create_sent = Instant::now();
    let instance_id = client
        .create_instance(tonic::Request::new(CreateInstanceRequest {
            job_id: opts.job.clone(),
            ..Default::default()
        }))
        .await
        .context("CreateInstance")?
        .into_inner()
        .instance_id;
    times.create_rpc = ms(create_sent.elapsed());

    let (first_alloc, received) = observed
        .wait(&instance_id, opts.stage_timeout, "placement", |track| {
            track.arrivals.first().copied()
        })
        .await?;
    times.create_to_running = ms(received - create_sent);
    if let Some(placed) = observed.placed_at(first_alloc) {
        times.placement = Some(ms(placed.saturating_duration_since(create_sent)));
        times.dispatch = Some(ms(received.saturating_duration_since(placed)));
    }

    running.enter();
    if !opts.hold.is_zero() {
        tokio::time::sleep(opts.hold).await;
    }
    running.leave();

    let suspend_sent = Instant::now();
    client
        .suspend_instance(tonic::Request::new(SuspendInstanceRequest {
            instance_id: instance_id.clone(),
            checkpoint: None,
            idempotency_key: None,
        }))
        .await
        .context("SuspendInstance")?;
    let suspended = observed
        .wait(
            &instance_id,
            opts.stage_timeout,
            "instance.suspended",
            |track| track.suspended,
        )
        .await?;
    times.suspend = ms(suspended.saturating_duration_since(suspend_sent));

    let resume_sent = Instant::now();
    client
        .start_instance(tonic::Request::new(StartInstanceRequest {
            instance_id: instance_id.clone(),
            idempotency_key: None,
            cold: false,
        }))
        .await
        .context("StartInstance")?;
    let (_, resume_received) = observed
        .wait(
            &instance_id,
            opts.stage_timeout,
            "the resume's allocation",
            |track| track.arrivals.get(1).copied(),
        )
        .await?;
    times.resume_dispatch = ms(resume_received.saturating_duration_since(resume_sent));
    let resumed = observed
        .wait(
            &instance_id,
            opts.stage_timeout,
            "instance.resumed",
            |track| track.resumed,
        )
        .await?;
    times.resume = ms(resumed.saturating_duration_since(resume_sent));

    let destroy_sent = Instant::now();
    client
        .destroy_instance(tonic::Request::new(DestroyInstanceRequest {
            instance_id: instance_id.clone(),
            idempotency_key: None,
        }))
        .await
        .context("DestroyInstance")?;
    times.destroy_rpc = ms(destroy_sent.elapsed());
    observed
        .tracks
        .lock()
        .expect("tracks lock")
        .remove(&instance_id);
    Ok(times)
}
