//! The bundled autoscaler app's body (`legion app run autoscaler`;
//! autoscaler-v2 plan §6.1, §6.5, §6.9; docs/autoscaling.md).
//!
//! The manager owns state and the scheduling rules; this app owns decisions.
//! Every call it makes is a public RPC, as its own `legion` identity through
//! the allocation's API socket:
//!
//! 1. **Policies** arrive on `WatchScalingPolicies`: the effective policy of
//!    every autoscaled task, re-sent whenever a job, the cluster's scaling
//!    defaults, or a namespace policy changes. Nothing is polled.
//! 2. **Every interval** (`interval` in the app's config, default 15s), one
//!    `GetScalingInputs` call (per 1000 targets) reads every target's
//!    replicas, measured usage, pins, pauses, rollouts, and override
//!    version. Each check's window average is kept here, by the app's
//!    clock, like the stabilization windows.
//! 3. Each policy runs through the engine's safeguard pipeline
//!    ([`crate::autoscaler::evaluate`]), with the per-target windows kept
//!    here between ticks.
//! 4. A change is written with `ScaleTask` (`source = "autoscaler"`,
//!    compare-and-set on the version it read, the decision attached); a
//!    dry-run change is recorded with `RecordScalingAction`; and every
//!    evaluation is reported with one `ReportScalingStatus`, which is also
//!    the app's heartbeat.
//!
//! The source is `legion` (Legion's own measurements), with the `cpu` and
//! `memory` shorthands and the fixed query vocabulary
//! ([`crate::manager::autoscale::LEGION_QUERIES`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::time::Duration;

use crate::apps::{AppContext, AppFuture};
use crate::autoscaler::defaults::EffectiveCheck;
use crate::autoscaler::{
    evaluate, CapacityGuard, Decision, DecisionKind, EffectivePolicy, Flag, Guards, Mode,
    Observation, Pause, Pin, PolicyState, Reading, Stage,
};
use crate::cmd::helpers::AuthedManagerClient;
use crate::prelude::*;
use crate::structs::{
    GetScalingInputsRequest, RecordScalingActionRequest, ReportScalingStatusRequest, ScaleDecision,
    ScaleTaskRequest, ScalingEventFlag, ScalingInputs, ScalingPolicy, ScalingTargetReport,
    ScalingTargetSelector, WatchScalingPoliciesRequest,
};

/// The default evaluation interval (plan §6.9).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(15);

/// Most selectors one `GetScalingInputs` call takes.
const INPUT_BATCH: usize = 1000;

/// How long the policy watch waits before reconnecting, at most.
const WATCH_BACKOFF_MAX: Duration = Duration::from_secs(15);

/// The app body [`crate::apps::BUNDLED`] registers.
pub fn run(context: AppContext) -> AppFuture {
    Box::pin(run_body(context))
}

/// The app's config (its manifest's schema): `interval`, `mode`.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineOptions {
    /// How often every policy is evaluated.
    pub interval: Duration,
    /// `Some(DryRun)` forces every policy into dry-run.
    pub force_mode: Option<Mode>,
    /// Names this instance in reports (its allocation, or a host name).
    pub instance: String,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            force_mode: None,
            instance: "autoscaler".to_string(),
        }
    }
}

impl EngineOptions {
    /// Read the app config.
    pub fn from_config(config: &Value) -> R<Self> {
        let mut options = Self::default();
        if let Some(interval) = config.get("interval").and_then(Value::as_str) {
            let parsed = humantime::parse_duration(interval.trim())
                .with_context(|| format!("interval {interval:?} is not a duration"))?;
            if parsed.is_zero() {
                bail!("interval must be longer than 0s");
            }
            options.interval = parsed;
        }
        options.force_mode = match config.get("mode").and_then(Value::as_str) {
            Some("dry-run") => Some(Mode::DryRun),
            Some("enforce") | None => None,
            Some(other) => bail!("mode {other:?}: use enforce or dry-run"),
        };
        options.instance = std::env::var("LEGION_ALLOC_ID")
            .map(|alloc| format!("alloc {alloc}"))
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "autoscaler".to_string());
        Ok(options)
    }
}

async fn run_body(mut context: AppContext) -> R<()> {
    let options = EngineOptions::from_config(context.config())?;
    info!(
        app = context.name(),
        interval = ?options.interval,
        dry_run = options.force_mode.is_some(),
        legion = context.legion_addr().is_some(),
        "The autoscaler app is running"
    );
    let client = loop {
        match context.client().await {
            Ok(client) => break client,
            Err(err) => {
                // No credentials: nothing to do but say so, and wait.
                warn!("The autoscaler cannot reach Legion: {err:#}");
                context.set_health(true, format!("idle: cannot reach Legion: {err:#}"));
                tokio::select! {
                    _ = context.stopped() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        }
    };
    let engine = Engine::new(client, options.clone(), now_ms());
    let watch = engine.spawn_watch();
    context.set_health(true, "ok: evaluating");
    let mut ticker = tokio::time::interval(options.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut engine = engine;
    loop {
        tokio::select! {
            _ = context.stopped() => break,
            _ = ticker.tick() => {
                let summary = engine.tick(now_ms()).await;
                context.set_health(true, summary);
            }
        }
    }
    watch.abort();
    info!(app = context.name(), "The autoscaler app is stopping");
    Ok(())
}

/// Wall-clock unix milliseconds.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// One target: `job/group/task`, where `task` carries `autoscale`.
type TargetKey = (String, String, String);

/// Warmup bookkeeping: replicas that became ready recently are warming.
#[derive(Debug, Default)]
struct Readiness {
    last_ready: Option<u32>,
    rises: VecDeque<(i64, u32)>,
}

impl Readiness {
    /// Record `ready` at `now_ms`; returns how many of them are warming.
    fn observe(&mut self, ready: u32, now_ms: i64, warmup: Duration) -> u32 {
        if let Some(last) = self.last_ready {
            if ready > last {
                self.rises.push_back((now_ms, ready - last));
            } else if ready < last {
                // Replicas went away: the newest rises go first.
                let mut gone = last - ready;
                while gone > 0 {
                    let Some(back) = self.rises.back_mut() else {
                        break;
                    };
                    let take = back.1.min(gone);
                    back.1 -= take;
                    gone -= take;
                    if back.1 == 0 {
                        self.rises.pop_back();
                    }
                }
            }
        }
        self.last_ready = Some(ready);
        let since = now_ms - i64::try_from(warmup.as_millis()).unwrap_or(i64::MAX / 4);
        while self.rises.front().is_some_and(|(at, _)| *at <= since) {
            self.rises.pop_front();
        }
        self.rises.iter().map(|(_, n)| *n).sum::<u32>().min(ready)
    }
}

/// What the engine keeps per target between ticks.
#[derive(Debug)]
struct TargetState {
    policy_state: PolicyState,
    readiness: Readiness,
    /// Hash of the last decision reported in full.
    reported: Option<u64>,
    /// Checks without data in the last evaluation (for `scale.check_failed`
    /// episodes).
    missing: BTreeSet<String>,
    /// The last dry-run "would" recorded, so an unchanged one is not
    /// recorded every tick.
    last_would: Option<(u32, u32)>,
    /// Each check's readings inside its window, by the engine's clock: the
    /// window average (`window`, default 1m) is the mean of these. Kept in
    /// the app, like the stabilization windows, so a manager failover does
    /// not reset them.
    samples: BTreeMap<String, VecDeque<(i64, f64)>>,
}

impl TargetState {
    /// Add `value` at `now_ms` and return the mean over `window`.
    fn windowed(&mut self, check: &str, value: f64, now_ms: i64, window: Duration) -> f64 {
        let ring = self.samples.entry(check.to_string()).or_default();
        ring.push_back((now_ms, value));
        let since = now_ms - i64::try_from(window.as_millis()).unwrap_or(i64::MAX / 4);
        while ring.front().is_some_and(|(at, _)| *at <= since) {
            ring.pop_front();
        }
        mean(ring.iter().map(|(_, value)| *value)).unwrap_or(value)
    }
}

/// The engine: the policy cache, per-target state, and one client.
pub struct Engine {
    client: AuthedManagerClient,
    options: EngineOptions,
    started_ms: i64,
    policies: std::sync::Arc<RwLock<Option<Vec<ScalingPolicy>>>>,
    targets: HashMap<TargetKey, TargetState>,
    /// The manager that took the last report. Reports leave out unchanged
    /// decisions, so when another manager takes over the status board
    /// (which starts empty), the next report sends every decision again.
    served_by: Option<String>,
}

/// What one tick did, for tests and the app's health detail.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickReport {
    /// Targets evaluated.
    pub evaluated: usize,
    /// Count writes that landed.
    pub scaled: usize,
    /// Dry-run decisions recorded.
    pub would: usize,
    /// Per target, the state reported.
    pub states: BTreeMap<String, String>,
    /// Errors (inputs, writes, the report).
    pub errors: Vec<String>,
}

impl std::fmt::Display for TickReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ok: {} target(s) evaluated, {} scaled, {} dry-run",
            self.evaluated, self.scaled, self.would
        )?;
        if let Some(error) = self.errors.first() {
            write!(f, "; last error: {error}")?;
        }
        Ok(())
    }
}

impl From<TickReport> for String {
    fn from(report: TickReport) -> Self {
        report.to_string()
    }
}

fn hash_of(value: &impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn mean(values: impl Iterator<Item = f64>) -> Option<f64> {
    let (sum, n) = values.fold((0.0, 0usize), |(sum, n), value| (sum + value, n + 1));
    (n > 0).then(|| sum / n as f64)
}

/// One member's current value for a `legion` query. The window average is
/// the engine's own (see `TargetState::samples`), by the engine's clock.
fn member_reading(input: &ScalingInputs, query: &str) -> Result<f64, String> {
    let fresh = input
        .freshness
        .as_ref()
        .map(|freshness| freshness.state.as_str())
        .unwrap_or("missing");
    match query {
        "task.unplaced" => return Ok(f64::from(input.unplaced_replicas)),
        "service.rps" | "service.errors" | "service.latency_ms.mean" | "service.latency_ms.max" => {
            let requests = input
                .requests
                .as_ref()
                .ok_or_else(|| "the task backs no service".to_string())?;
            if fresh != "current" {
                return Err(format!("service data is {fresh}"));
            }
            return Ok(match query {
                "service.rps" => requests.requests_per_second,
                "service.errors" => requests.errors_per_second,
                "service.latency_ms.mean" => requests.latency_ms_mean,
                _ => requests.latency_ms_max as f64,
            });
        }
        _ => {}
    }
    if fresh != "current" {
        return Err(format!("usage is {fresh}"));
    }
    let (measured, current) = if query == "task.cpu.utilization" {
        let cpu = input.cpu.as_ref();
        (
            cpu.map_or(0, |cpu| {
                cpu.measured_replicas
                    .saturating_sub(cpu.unmeasurable_replicas)
            }),
            cpu.map_or(0.0, |cpu| cpu.utilization),
        )
    } else {
        let memory = input.memory.as_ref();
        (
            memory.map_or(0, |memory| memory.measured_replicas),
            memory.map_or(0.0, |memory| memory.utilization),
        )
    };
    if measured == 0 {
        return Err("no replica reports measurable usage".to_string());
    }
    Ok(current)
}

/// A target's reading for one check: one member's value, or for a group
/// target, the members' utilizations weighted by their reservations (sums
/// for counts and rates).
fn check_reading(check: &EffectiveCheck, members: &[&ScalingInputs]) -> Reading {
    let mut values = Vec::new();
    for input in members {
        match member_reading(input, &check.query) {
            Ok(value) => {
                let weight = match check.query.as_str() {
                    "task.cpu.utilization" => {
                        input.cpu.as_ref().map_or(0.0, |cpu| cpu.reserved_mhz)
                    }
                    "task.memory.utilization" => input
                        .memory
                        .as_ref()
                        .map_or(0.0, |memory| memory.reserved_mib),
                    _ => 1.0,
                };
                values.push((value, weight));
            }
            Err(why) => return Reading::Missing(why),
        }
    }
    if values.is_empty() {
        return Reading::Missing("no inputs".to_string());
    }
    if check.query.ends_with(".utilization") {
        let total: f64 = values.iter().map(|(_, weight)| weight).sum();
        if total > 0.0 {
            return Reading::Value(values.iter().map(|(v, w)| v * w).sum::<f64>() / total);
        }
        return Reading::Value(mean(values.iter().map(|(v, _)| *v)).unwrap_or_default());
    }
    if check.query.starts_with("service.") {
        // Every task backing a service reports the service's totals.
        return Reading::Value(values[0].0);
    }
    Reading::Value(values.iter().map(|(v, _)| v).sum())
}

/// The state word a decision reports (see `ScalingTargetReport.state`).
fn state_of(decision: &Decision) -> &'static str {
    match decision.kind {
        DecisionKind::Applied => return "scaled",
        DecisionKind::Would => return "dry-run",
        DecisionKind::Held => {}
    }
    let changed = |stage: Stage| decision.stage(stage).is_some_and(|note| note.changed());
    let noted = |stage: Stage| {
        decision
            .stage(stage)
            .is_some_and(|note| !note.note.is_empty())
    };
    if decision
        .stage(Stage::Pause)
        .is_some_and(|note| note.note.starts_with("paused"))
    {
        return "paused";
    }
    if decision
        .stage(Stage::Pin)
        .is_some_and(|note| note.note.contains("pinned"))
    {
        return "pinned";
    }
    if changed(Stage::Rollout) {
        return "deferred";
    }
    if changed(Stage::Bounds) || changed(Stage::Quota) || changed(Stage::Capacity) {
        return "limited";
    }
    if changed(Stage::Stabilize) || changed(Stage::Hold) || changed(Stage::Step) {
        return "held";
    }
    if decision.checks.iter().all(|check| check.missing.is_some()) && !decision.checks.is_empty() {
        return "missing-data";
    }
    if noted(Stage::Hold) && decision.checks.iter().any(|check| check.missing.is_some()) {
        return "missing-data";
    }
    "ok"
}

fn decision_id(key: &TargetKey, at_ms: i64) -> String {
    format!("d-{:08x}", hash_of(&(key, at_ms)) as u32)
}

impl Engine {
    /// An engine that writes through `client`, started at `started_ms` (the
    /// cold-start hold counts from here).
    pub fn new(client: AuthedManagerClient, options: EngineOptions, started_ms: i64) -> Self {
        Self {
            client,
            options,
            started_ms,
            policies: std::sync::Arc::new(RwLock::new(None)),
            targets: HashMap::new(),
            served_by: None,
        }
    }

    /// Keep the policy cache current from `WatchScalingPolicies`,
    /// reconnecting with backoff, until the returned task is aborted.
    pub fn spawn_watch(&self) -> tokio::task::JoinHandle<()> {
        let mut client = self.client.clone();
        let policies = self.policies.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match client
                    .watch_scaling_policies(WatchScalingPoliciesRequest::default())
                    .await
                {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        backoff = Duration::from_millis(500);
                        loop {
                            match stream.message().await {
                                Ok(Some(set)) => {
                                    debug!(
                                        policies = set.policies.len(),
                                        generation = set.generation,
                                        "Scaling policies updated"
                                    );
                                    if let Ok(mut cache) = policies.write() {
                                        *cache = Some(set.policies);
                                    }
                                }
                                Ok(None) => break,
                                Err(status) => {
                                    warn!("The scaling policy watch ended: {}", status.message());
                                    break;
                                }
                            }
                        }
                    }
                    Err(status) => {
                        warn!("Cannot watch scaling policies: {}", status.message());
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCH_BACKOFF_MAX);
            }
        })
    }

    /// How many policies the cache holds (`None` before the first set).
    pub fn policy_count(&self) -> Option<usize> {
        self.policies
            .read()
            .ok()
            .and_then(|cache| cache.as_ref().map(Vec::len))
    }

    /// Replace the policy cache (tests, or a caller with its own source).
    pub fn set_policies(&self, policies: Vec<ScalingPolicy>) {
        if let Ok(mut cache) = self.policies.write() {
            *cache = Some(policies);
        }
    }

    /// One evaluation of every policy at `now_ms`: read, decide, write,
    /// report.
    #[instrument(skip_all, fields(now_ms))]
    pub async fn tick(&mut self, now_ms: i64) -> TickReport {
        let mut report = TickReport::default();
        let policies = self
            .policies
            .read()
            .ok()
            .and_then(|cache| cache.clone())
            .unwrap_or_default();
        let mut reports = Vec::with_capacity(policies.len());
        let live: BTreeSet<TargetKey> = policies
            .iter()
            .map(|p| (p.job_id.clone(), p.task_group.clone(), p.task_name.clone()))
            .collect();
        self.targets.retain(|key, _| live.contains(key));

        // Parse the valid policies; report the invalid ones as held.
        let mut valid: Vec<(&ScalingPolicy, EffectivePolicy)> = Vec::new();
        for policy in &policies {
            if policy.state != "ok" {
                reports.push(self.plain_report(policy, "invalid", 0, &policy.error, now_ms));
                continue;
            }
            match serde_json::from_str::<EffectivePolicy>(&policy.policy_json) {
                Ok(mut effective) => {
                    if let Some(mode) = self.options.force_mode {
                        effective.mode = mode;
                    }
                    valid.push((policy, effective));
                }
                Err(err) => reports.push(self.plain_report(
                    policy,
                    "error",
                    0,
                    &format!("the policy does not parse in this build: {err}"),
                    now_ms,
                )),
            }
        }

        // One batched read of every target's inputs.
        let mut selectors: Vec<ScalingTargetSelector> = Vec::new();
        for (policy, _) in &valid {
            let selector = if policy.members.len() > 1 {
                ScalingTargetSelector {
                    job: policy.job_id.clone(),
                    task_group: Some(policy.task_group.clone()),
                    task_name: None,
                }
            } else {
                ScalingTargetSelector {
                    job: policy.job_id.clone(),
                    task_group: Some(policy.task_group.clone()),
                    task_name: Some(policy.task_name.clone()),
                }
            };
            if !selectors.contains(&selector) {
                selectors.push(selector);
            }
        }
        let mut inputs: HashMap<TargetKey, ScalingInputs> = HashMap::new();
        let mut input_error: Option<String> = None;
        for batch in selectors.chunks(INPUT_BATCH) {
            match self
                .client
                .get_scaling_inputs(GetScalingInputsRequest {
                    targets: batch.to_vec(),
                    namespaces: Vec::new(),
                    history_seconds: 0,
                })
                .await
            {
                Ok(reply) => {
                    for input in reply.into_inner().inputs {
                        if input.error.is_empty() {
                            inputs.insert(
                                (
                                    input.job_id.clone(),
                                    input.task_group.clone(),
                                    input.task_name.clone(),
                                ),
                                input,
                            );
                        }
                    }
                }
                Err(status) => {
                    input_error = Some(format!("reading scaling inputs: {}", status.message()));
                }
            }
        }
        if let Some(error) = &input_error {
            report.errors.push(error.clone());
        }

        for (policy, effective) in valid {
            let key: TargetKey = (
                policy.job_id.clone(),
                policy.task_group.clone(),
                policy.task_name.clone(),
            );
            let members: Vec<&ScalingInputs> = policy
                .members
                .iter()
                .filter_map(|member| {
                    inputs.get(&(
                        policy.job_id.clone(),
                        policy.task_group.clone(),
                        member.clone(),
                    ))
                })
                .collect();
            let Some(owner) = inputs.get(&key) else {
                let why = input_error
                    .clone()
                    .unwrap_or_else(|| "no inputs for the target".to_string());
                reports.push(self.plain_report(policy, "error", 0, &why, now_ms));
                continue;
            };
            let target_report = self
                .evaluate_target(
                    policy,
                    &effective,
                    &key,
                    owner,
                    &members,
                    now_ms,
                    &mut report,
                )
                .await;
            report.evaluated += 1;
            reports.push(target_report);
        }

        for target in &reports {
            report.states.insert(
                format!(
                    "{}/{}/{}",
                    target.job_id, target.task_group, target.task_name
                ),
                target.state.clone(),
            );
        }
        let request = ReportScalingStatusRequest {
            instance: self.options.instance.clone(),
            interval_seconds: self.options.interval.as_secs().max(1) as u32,
            mode: match self.options.force_mode {
                Some(Mode::DryRun) => "dry-run".to_string(),
                _ => "enforce".to_string(),
            },
            targets: reports,
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        match self.client.report_scaling_status(request).await {
            Ok(reply) => {
                let served_by = reply.into_inner().served_by;
                if self.served_by.as_deref() != Some(served_by.as_str()) {
                    for state in self.targets.values_mut() {
                        state.reported = None;
                    }
                    self.served_by = Some(served_by);
                }
            }
            Err(status) => {
                // The report may not have landed: send every decision in
                // full next time.
                for state in self.targets.values_mut() {
                    state.reported = None;
                }
                report
                    .errors
                    .push(format!("reporting status: {}", status.message()));
            }
        }
        report
    }

    fn plain_report(
        &self,
        policy: &ScalingPolicy,
        state: &str,
        current: u32,
        summary: &str,
        now_ms: i64,
    ) -> ScalingTargetReport {
        ScalingTargetReport {
            job_id: policy.job_id.clone(),
            task_group: policy.task_group.clone(),
            task_name: policy.task_name.clone(),
            state: state.to_string(),
            current,
            want: current,
            evaluated_at_unix_ms: now_ms,
            summary: summary.to_string(),
            decision_json: String::new(),
            policy_digest: policy.digest.clone(),
            events: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn evaluate_target(
        &mut self,
        policy: &ScalingPolicy,
        effective: &EffectivePolicy,
        key: &TargetKey,
        owner: &ScalingInputs,
        members: &[&ScalingInputs],
        now_ms: i64,
        report: &mut TickReport,
    ) -> ScalingTargetReport {
        let started_ms = self.started_ms;
        let state = self
            .targets
            .entry(key.clone())
            .or_insert_with(|| TargetState {
                policy_state: PolicyState::new(started_ms),
                readiness: Readiness::default(),
                reported: None,
                missing: BTreeSet::new(),
                last_would: None,
                samples: BTreeMap::new(),
            });
        let current = owner.desired_replicas;
        let warming = state
            .readiness
            .observe(owner.ready_replicas, now_ms, effective.warmup);
        let mut observation = Observation {
            current,
            ready: owner.ready_replicas.saturating_sub(warming),
            warming,
            readings: BTreeMap::new(),
            guards: Guards {
                quota: None,
                capacity: (owner.unplaced_replicas > 0).then_some(CapacityGuard {
                    unplaced: owner.unplaced_replicas,
                    node_group_fits: false,
                    last_scale_out: None,
                }),
                rollout: owner.rollout_active.then(|| "in progress".to_string()),
            },
            pin: owner.pin.as_ref().map(|pin| match (pin.min, pin.max) {
                (Some(min), Some(max)) if min == max => Pin {
                    count: Some(min),
                    min: None,
                    max: None,
                    until_ms: pin.until_unix_ms,
                    by: pin.by.clone(),
                    reason: pin.reason.clone(),
                },
                (min, max) => Pin {
                    count: None,
                    min,
                    max,
                    until_ms: pin.until_unix_ms,
                    by: pin.by.clone(),
                    reason: pin.reason.clone(),
                },
            }),
            pause: owner.pause.as_ref().map(|pause| Pause {
                until_ms: pause.until_unix_ms,
                by: pause.by.clone(),
                reason: pause.reason.clone(),
            }),
        };
        for check in effective.all_checks() {
            let reading = match check_reading(&check, members) {
                Reading::Value(value) => {
                    Reading::Value(state.windowed(&check.name, value, now_ms, check.window))
                }
                // Missing data is missing now: a window of old samples does
                // not stand in for it.
                missing => missing,
            };
            observation.readings.insert(check.name.clone(), reading);
        }
        let mut decision = evaluate(effective, &mut state.policy_state, &observation, now_ms);

        // State changes worth an event: the engine's flags, and a check
        // that newly lost its data.
        let mut events: Vec<ScalingEventFlag> = decision
            .flags
            .iter()
            .map(|flag| match flag {
                Flag::Limited { reason } => ScalingEventFlag {
                    kind: "limited".to_string(),
                    message: reason.clone(),
                },
                Flag::Flapping {
                    reversals,
                    until_ms,
                } => ScalingEventFlag {
                    kind: "flapping".to_string(),
                    message: format!(
                        "flapping: {reversals} direction changes in 10m; scale-in held until {}",
                        crate::cmd::scaling::clock(*until_ms)
                    ),
                },
            })
            .collect();
        let missing: BTreeSet<String> = decision
            .checks
            .iter()
            .filter(|check| check.missing.is_some())
            .map(|check| check.name.clone())
            .collect();
        for check in decision
            .checks
            .iter()
            .filter(|check| check.missing.is_some() && !state.missing.contains(&check.name))
        {
            events.push(ScalingEventFlag {
                kind: "check_failed".to_string(),
                message: format!("check {} has {}", check.name, check.detail),
            });
        }
        state.missing = missing;

        let mut state_word = state_of(&decision).to_string();
        let mut summary = decision.summary.clone();
        let policy_json = effective.canonical_json();
        let scale_decision = ScaleDecision {
            id: decision_id(key, now_ms),
            summary: decision.summary.clone(),
            policy_digest: decision.policy_digest.clone(),
            policy_json,
            detail_json: serde_json::to_string(&decision).unwrap_or_default(),
            evaluated_at_unix_ms: now_ms,
        };
        match decision.kind {
            DecisionKind::Applied => {
                let request = ScaleTaskRequest {
                    name: key.0.clone(),
                    task_group: Some(key.1.clone()),
                    task_name: Some(key.2.clone()),
                    count: Some(decision.to),
                    reason: Some(decision.summary.clone()),
                    expected_version: Some(owner.override_version),
                    decision: Some(scale_decision),
                    source: "autoscaler".to_string(),
                    ..Default::default()
                };
                match self.client.scale_task(request).await {
                    Ok(reply) => {
                        let reply = reply.into_inner();
                        if reply.deferred {
                            state_word = "deferred".to_string();
                            summary = format!("{summary}; {}", reply.message);
                        } else {
                            state
                                .policy_state
                                .record_applied(now_ms, decision.from, decision.to);
                            report.scaled += 1;
                        }
                    }
                    Err(status) => {
                        // A version conflict or a pause that raced this
                        // tick: nothing changed; the next tick re-reads.
                        let held = status.code() == tonic::Code::FailedPrecondition;
                        state_word = if held { "held" } else { "error" }.to_string();
                        summary = format!("write refused: {}", status.message());
                        decision.to = decision.from;
                        report.errors.push(summary.clone());
                    }
                }
            }
            DecisionKind::Would => {
                if state.last_would != Some((decision.from, decision.to)) {
                    let request = RecordScalingActionRequest {
                        job_id: key.0.clone(),
                        task_group: key.1.clone(),
                        task_name: key.2.clone(),
                        kind: "would".to_string(),
                        from: decision.from,
                        to: decision.to,
                        decision: Some(scale_decision),
                    };
                    match self.client.record_scaling_action(request).await {
                        Ok(_) => {
                            state.last_would = Some((decision.from, decision.to));
                            report.would += 1;
                        }
                        Err(status) => report.errors.push(format!(
                            "recording a dry-run decision: {}",
                            status.message()
                        )),
                    }
                }
            }
            DecisionKind::Held => {
                if decision.from == decision.to {
                    state.last_would = None;
                }
            }
        }

        // The full decision only when it changed (ignoring its time).
        let mut stable = decision.clone();
        stable.at_ms = 0;
        let digest = hash_of(&serde_json::to_string(&stable).unwrap_or_default());
        let decision_json = if state.reported == Some(digest) {
            String::new()
        } else {
            state.reported = Some(digest);
            serde_json::to_string(&decision).unwrap_or_default()
        };
        ScalingTargetReport {
            job_id: key.0.clone(),
            task_group: key.1.clone(),
            task_name: key.2.clone(),
            state: state_word,
            current,
            want: decision.to,
            evaluated_at_unix_ms: now_ms,
            summary,
            decision_json,
            policy_digest: policy.digest.clone(),
            events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{ScalingCpuInputs, ScalingDataFreshness};

    #[test]
    fn options_read_the_app_config_test() -> R<()> {
        let options = EngineOptions::from_config(&json!({ "interval": "2s", "mode": "dry-run" }))?;
        assert_eq!(options.interval, Duration::from_secs(2));
        assert_eq!(options.force_mode, Some(Mode::DryRun));
        let defaults = EngineOptions::from_config(&json!({ "plugins": {} }))?;
        assert_eq!(defaults.interval, DEFAULT_INTERVAL);
        assert_eq!(defaults.force_mode, None);
        assert!(EngineOptions::from_config(&json!({ "interval": "0s" })).is_err());
        assert!(EngineOptions::from_config(&json!({ "mode": "yolo" })).is_err());
        Ok(())
    }

    /// Replicas that became ready within `warmup` are warming; replicas
    /// that go away take the newest rises with them.
    #[test]
    fn readiness_counts_warming_replicas_test() {
        let warmup = Duration::from_secs(30);
        let mut readiness = Readiness::default();
        assert_eq!(
            readiness.observe(2, 0, warmup),
            0,
            "the first sight is not a rise"
        );
        assert_eq!(readiness.observe(4, 10_000, warmup), 2);
        assert_eq!(readiness.observe(5, 20_000, warmup), 3);
        assert_eq!(
            readiness.observe(5, 40_001, warmup),
            1,
            "the first rise warmed up"
        );
        assert_eq!(
            readiness.observe(4, 41_000, warmup),
            0,
            "the newest rise left"
        );
        assert_eq!(readiness.observe(4, 60_000, warmup), 0);
    }

    fn input(task: &str, utilization: f64, reserved: f64) -> ScalingInputs {
        ScalingInputs {
            job_id: "web".into(),
            task_group: "default".into(),
            task_name: task.into(),
            desired_replicas: 2,
            ready_replicas: 2,
            cpu: Some(ScalingCpuInputs {
                measured_replicas: 2,
                utilization,
                reserved_mhz: reserved,
                ..Default::default()
            }),
            freshness: Some(ScalingDataFreshness {
                state: "current".into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn cpu_check() -> EffectiveCheck {
        let policy = EffectivePolicy::from_record(&crate::autoscaler::builtin_defaults(), 2)
            .expect("builtin resolves");
        policy.all_checks().remove(0)
    }

    /// A group weighs its members by their reservations; stale data is
    /// missing, not zero; the window averages by the engine's clock.
    #[test]
    fn readings_weigh_groups_and_average_the_window_test() {
        let check = cpu_check();
        let a = input("a", 1.0, 3000.0);
        let b = input("b", 0.2, 1000.0);
        assert_eq!(
            check_reading(&check, &[&a, &b]),
            Reading::Value((3.0 + 0.2) / 4.0)
        );
        let mut stale = input("web", 0.5, 1000.0);
        stale.freshness = Some(ScalingDataFreshness {
            state: "resyncing".into(),
            ..Default::default()
        });
        assert!(
            matches!(check_reading(&check, &[&stale]), Reading::Missing(why) if why.contains("resyncing"))
        );
        let mut unmeasured = input("web", 0.0, 0.0);
        unmeasured.cpu = Some(ScalingCpuInputs::default());
        assert!(matches!(
            check_reading(&check, &[&unmeasured]),
            Reading::Missing(_)
        ));

        let mut state = TargetState {
            policy_state: PolicyState::new(0),
            readiness: Readiness::default(),
            reported: None,
            missing: BTreeSet::new(),
            last_would: None,
            samples: BTreeMap::new(),
        };
        let window = Duration::from_secs(60);
        assert_eq!(state.windowed("cpu", 0.1, 0, window), 0.1);
        assert_eq!(state.windowed("cpu", 0.5, 30_000, window), 0.3);
        // The first sample left the window.
        assert_eq!(state.windowed("cpu", 0.9, 60_000, window), 0.7);
    }

    /// The state word follows the stage that decided.
    #[test]
    fn state_words_follow_the_deciding_stage_test() {
        let policy = EffectivePolicy::from_record(&crate::autoscaler::builtin_defaults(), 2)
            .expect("builtin resolves");
        let mut state = PolicyState::new(0);
        // Hot: scales out at once.
        let hot = Observation::new(2).reading("cpu", 1.4);
        let decision = evaluate(&policy, &mut state, &hot, 1_000);
        assert_eq!(state_of(&decision), "scaled");
        // At the ceiling: limited.
        let mut state = PolicyState::new(0);
        let capped = Observation::new(8).reading("cpu", 2.0);
        assert_eq!(
            state_of(&evaluate(&policy, &mut state, &capped, 1_000)),
            "limited"
        );
        // Quiet right after a start: held by the cold-start hold.
        let mut state = PolicyState::new(0);
        let quiet = Observation::new(4).reading("cpu", 0.1);
        assert_eq!(
            state_of(&evaluate(&policy, &mut state, &quiet, 1_000)),
            "held"
        );
        // Within tolerance: ok.
        let mut state = PolicyState::new(0);
        let steady = Observation::new(4).reading("cpu", 0.7);
        assert_eq!(
            state_of(&evaluate(&policy, &mut state, &steady, 1_000)),
            "ok"
        );
        // No data at all.
        let mut state = PolicyState::new(0);
        let blind = Observation::new(4).missing("cpu", "usage is missing");
        assert_eq!(
            state_of(&evaluate(&policy, &mut state, &blind, 1_000)),
            "missing-data"
        );
        // Paused.
        let mut state = PolicyState::new(0);
        let mut paused = Observation::new(2).reading("cpu", 1.4);
        paused.pause = Some(Pause {
            until_ms: None,
            by: "alice".into(),
            reason: String::new(),
        });
        assert_eq!(
            state_of(&evaluate(&policy, &mut state, &paused, 1_000)),
            "paused"
        );
    }
}
