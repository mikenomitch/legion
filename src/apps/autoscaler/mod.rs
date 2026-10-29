//! The bundled autoscaler app's body (`legion app run autoscaler`;
//! autoscaler-v2 plan §6.1, §6.5, §6.8, §6.9; docs/autoscaling.md).
//!
//! The manager owns state and the scheduling rules; this app owns decisions.
//! Every call it makes is a public RPC, as its own `legion` identity through
//! the allocation's API socket:
//!
//! 1. **Policies** arrive on two streams. `WatchScalingPolicies` sends the
//!    effective policy of every autoscaled task, re-sent whenever a job, the
//!    cluster's scaling defaults, a namespace policy, or the plugins
//!    installed in this app's config change, and, for this app alone, the
//!    layered defaults, the cluster-wide pause, and each of its objects'
//!    pause and action row. `WatchObjects` sends the app's own objects
//!    ([`objects`]): `autoscaler/ScalingPolicy` (a target outside Legion) and
//!    `autoscaler/NodeGroup`, which this app resolves against those layers
//!    itself, enforcing the plugin allowlist. Nothing is polled.
//! 2. **Every interval** (`interval` in the app's config, default 15s), one
//!    `GetScalingInputs` call (per 1000 targets) reads every task target's
//!    replicas, measured usage, pins, pauses, rollouts, and override
//!    version. Checks on other sources ask the [plugin host](host): the
//!    built-in `prometheus` and `http` sources, or an external plugin over
//!    JSON-RPC, each distinct query once per tick. A standalone policy's
//!    target reports its count through `target/status`.
//! 3. Each policy runs through the engine's safeguard pipeline
//!    ([`crate::autoscaler::evaluate`]), with the per-target windows kept
//!    here between ticks. A check whose strategy is a plugin asks it for
//!    its desired count first.
//! 4. A task's change is written with `ScaleTask` (`source = "autoscaler"`,
//!    compare-and-set on the version it read, the decision attached). A
//!    scaling policy's change is an **action intent** first
//!    (`RecordScalingAction` `intent`, compare-and-set on the object's
//!    action version, which a pause refuses on the server), then made with
//!    `target/scale`, then closed with its outcome. A dry-run change is
//!    recorded with `RecordScalingAction` `would`, and every evaluation is
//!    reported with one `ReportScalingStatus`, which is also the app's
//!    heartbeat.
//! 5. Node groups ([`nodegroups`]) run after the task and external
//!    policies: one `GetCapacityDemand` for every group, the expander, the
//!    node-group engine, and one action intent per acting group. A deleted
//!    group (its object in its cleanup window) drains and terminates every
//!    member, then is released.
//! 6. Each object's durable verdict (accepted, ready, its resolved target,
//!    its last action) is written to its status with `UpdateObjectStatus`,
//!    only when it changed; the per-tick numbers stay in the report.
//!
//! **Replicas** ([`shards`]; docs/autoscaling.md, "Replicas"). Policies
//! hash to 64 shards; each replica holds KV leases on the shards it
//! evaluates, watches only their policies (as deltas), and evaluates only
//! them. A lone replica holds one lease for every shard. Leases are not
//! fences: every write above is compare-and-set, so two replicas that
//! briefly both own a shard still act once per decision.
//!
//! Module map: [`rpc`] (the JSON-RPC transports), [`host`] (every installed
//! plugin, ready to call), [`builtins`] (`prometheus`, `http`, `exec`,
//! `noop`), [`conformance`] (`legion scaling plugin test`), [`nodegroups`]
//! (node groups), [`objects`] (the app's object kinds, their status, and the
//! validate call), [`shards`] (the shard leases).

pub mod builtins;
pub mod conformance;
pub mod host;
pub mod nodegroups;
pub mod objects;
pub mod rpc;
pub mod shards;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::apps::objects::{run_object_watch, ObjectCache, ShardSelection, StatusBook};
use crate::apps::{AppContext, AppFuture};
use crate::autoscaler::defaults::EffectiveCheck;
use crate::autoscaler::plugins::PluginCatalog;
use crate::autoscaler::shard::{self, SHARDS};
use crate::autoscaler::signals::{Readiness, Windows};
use crate::autoscaler::strategies::StrategyConfig;
use crate::autoscaler::{
    evaluate, CapacityGuard, Decision, DecisionKind, EffectiveNodeGroup, EffectivePolicy, Flag,
    Guards, Mode, Observation, Pause, Pin, PluginRecommendation, PolicyState, Reading, Stage,
};
use crate::cmd::helpers::AuthedManagerClient;
use crate::prelude::*;
use crate::structs::{
    GetScalingInputsRequest, RecordScalingActionRequest, ReleaseObjectRequest,
    ReportScalingStatusRequest, ScaleDecision, ScaleTaskRequest, ScalingActionIntent,
    ScalingAppContext, ScalingEventFlag, ScalingInputs, ScalingPauseInfo, ScalingPolicy,
    ScalingPolicySet, ScalingTargetReport, ScalingTargetSelector, WatchScalingPoliciesRequest,
};
use host::{PluginHost, RecommendCall, SourceCall, SourceReading};
use objects::{ObjectTarget, Plan};
use shards::{LeaseTiming, Ownership, Shards};

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

/// The app's config (its manifest's schema): `interval`, `mode`, `plugins`.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineOptions {
    /// How often every policy is evaluated.
    pub interval: Duration,
    /// `Some(DryRun)` forces every policy into dry-run.
    pub force_mode: Option<Mode>,
    /// Names this instance in reports (its allocation, or a host name).
    pub instance: String,
    /// The plugins the operator installed.
    pub plugins: PluginCatalog,
    /// Where artifact plugins were fetched: the task directory's `code`.
    pub code_root: Option<PathBuf>,
    /// One timeout for every plugin call, instead of the protocol's (tests).
    pub plugin_timeout: Option<Duration>,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            force_mode: None,
            instance: "autoscaler".to_string(),
            plugins: PluginCatalog::default(),
            code_root: None,
            plugin_timeout: None,
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
        options.plugins = PluginCatalog::from_app_config(config).map_err(|err| anyhow!(err))?;
        options.code_root = std::env::var("LEGION_TASK_DIR")
            .ok()
            .filter(|dir| !dir.trim().is_empty())
            .map(|dir| PathBuf::from(dir).join("code"));
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
        plugins = options.plugins.entries().count(),
        legion = context.legion_addr().is_some(),
        "The autoscaler app is running"
    );
    // Registered first, so the views answer "starting" rather than "no such
    // view" until the app reaches Legion.
    let ui = crate::apps::autoscaler_ui::AutoscalerUi::new(now_ms());
    crate::apps::autoscaler_ui::register(context.ui(), ui.clone());
    let connection = loop {
        match context.connection() {
            Ok(connection) => break connection,
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
    let engine = Engine::new(connection.client(), options.clone(), now_ms());
    // The views read the engine's last hour of evaluations and call Legion
    // as the app; the actions write as the viewer, over the same connection
    // (docs/autoscaling.md, "In the UI").
    ui.connect(connection, engine.evaluations());
    // The validate call for the app's kinds, from the layers the policy
    // watch keeps (advice; the engine's own checks are the control).
    objects::register_validate(context.ui(), engine.validator());
    // Shard leases first, so the first watch and tick know what this
    // replica owns (everything, when it is alone).
    let mut leases = engine.shards(LeaseTiming::default());
    leases.step().await;
    let (stop_leases, leases_stopped) = tokio::sync::oneshot::channel();
    let leases = leases.spawn(leases_stopped);
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
    // Give the shards back, so the other replicas take them now rather
    // than after the leases expire.
    let _ = stop_leases.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), leases).await;
    info!(app = context.name(), "The autoscaler app is stopping");
    Ok(())
}

/// Wall-clock unix milliseconds.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// One target: `job/group/task` for a task, where `task` carries
/// `autoscale`; `(object:<uid>, "", "")` for one of the app's objects
/// ([`ObjectTarget::target_key`]).
type TargetKey = (String, String, String);

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
    /// Each check's readings inside its window, by the engine's clock (calls
    /// entry 39). Kept in the app, like the stabilization windows, so a
    /// manager failover does not reset them.
    windows: Windows,
    /// The count the engine expects to see next: after its own write, the
    /// count it wrote; otherwise the count it last saw. A different count
    /// was changed by someone else (a person, a pin, a finished rollout's
    /// deferred scale-in), and spends the step budget when it is seen.
    expected: Option<u32>,
    /// Why the policy was invalid at the last evaluation (for one
    /// `scale.check_failed` per episode).
    invalid: Option<String>,
    /// A scaling policy's action version, as last seen (the compare-and-set
    /// token of its next intent).
    action_version: u64,
}

impl TargetState {
    fn new(started_ms: i64) -> Self {
        Self {
            policy_state: PolicyState::new(started_ms),
            readiness: Readiness::default(),
            reported: None,
            missing: BTreeSet::new(),
            last_would: None,
            windows: Windows::default(),
            expected: None,
            invalid: None,
            action_version: 0,
        }
    }

    /// Account for a count change the engine did not make.
    fn observe_count(&mut self, current: u32, now_ms: i64) {
        if let Some(expected) = self.expected {
            if expected != current {
                self.policy_state.record_applied(now_ms, expected, current);
            }
        }
        self.expected = Some(current);
    }
}

/// Everything `WatchScalingPolicies` sent, by the key a delta names an
/// entry with (`job/group/task`, or `object:<uid>`).
#[derive(Debug, Clone, Default)]
struct PolicyCache {
    tasks: BTreeMap<String, ScalingPolicy>,
    /// The app's objects' pauses and action rows.
    objects: objects::ObjectStates,
    /// The layered defaults and the cluster-wide pause.
    context: Option<ScalingAppContext>,
}

impl PolicyCache {
    /// Take one message: a full set replaces the cache, a delta updates it.
    fn apply(&mut self, set: ScalingPolicySet) {
        if set.full {
            self.tasks.clear();
            self.objects.clear();
        }
        for key in &set.removed {
            self.tasks.remove(key);
            self.objects.remove(key);
        }
        for policy in set.policies {
            self.tasks.insert(
                shard::task_key(&policy.job_id, &policy.task_group, &policy.task_name),
                policy,
            );
        }
        for state in set.objects {
            self.objects.insert(state.key.clone(), state);
        }
        if set.context.is_some() {
            self.context = set.context;
        }
    }
}

/// Which ScalingPolicies a replica watches: those of the shards it holds.
fn policy_selection(owned: &Ownership) -> ShardSelection {
    match &owned.shards {
        None => ShardSelection::All,
        Some(shards) => ShardSelection::Shards {
            count: SHARDS,
            shards: shards.iter().copied().collect(),
        },
    }
}

/// Which NodeGroups a replica watches: every one, on the replica that holds
/// the `nodegroups` shard (the expander must see every group); none
/// elsewhere.
fn group_selection(owned: &Ownership) -> ShardSelection {
    if owned.owns(shard::shard_of(shard::NODE_GROUPS_KEY, SHARDS)) {
        ShardSelection::All
    } else {
        ShardSelection::Nothing
    }
}

/// How long each part of a tick took.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TickTimings {
    /// Reading: external sources and the batched `GetScalingInputs`.
    pub inputs: Duration,
    /// Deciding and acting: every policy's pipeline and its writes, the
    /// standalone targets, and the node groups.
    pub decide: Duration,
    /// The `ReportScalingStatus` call.
    pub report: Duration,
}

/// The engine: the policy cache, per-target state, the plugins, and one
/// client.
pub struct Engine {
    client: AuthedManagerClient,
    options: EngineOptions,
    started_ms: i64,
    policies: Arc<RwLock<Option<PolicyCache>>>,
    /// The layered defaults the policy watch last sent, which the validate
    /// call reads too.
    context: Arc<RwLock<Option<ScalingAppContext>>>,
    /// The app's ScalingPolicies of this replica's shards, and (on the
    /// replica holding the `nodegroups` shard) every NodeGroup, from
    /// `WatchObjects`.
    policy_objects: Arc<RwLock<ObjectCache>>,
    group_objects: Arc<RwLock<ObjectCache>>,
    /// The status each object last had written, so only a change writes.
    statuses: StatusBook,
    /// A deleting node group's cleanup progress this tick, by key.
    cleanups: HashMap<String, String>,
    targets: HashMap<TargetKey, TargetState>,
    host: Arc<PluginHost>,
    /// The manager that took the last report. Reports leave out unchanged
    /// decisions, so when another manager takes over the status board
    /// (which starts empty), the next report sends every decision again.
    served_by: Option<String>,
    /// The last hour of every target's evaluations, for the job panel.
    log: Arc<crate::apps::autoscaler_ui::EvaluationLog>,
    /// What the engine keeps per node group between ticks.
    groups: HashMap<String, nodegroups::GroupRuntime>,
    /// The shards this replica evaluates, which its lease controller
    /// ([`Engine::shards`]) keeps current. Everything until one runs.
    ownership: Arc<RwLock<Ownership>>,
    /// Whether a tick has run: a target first seen after that (a new job,
    /// or a shard this replica just took over) starts its cold-start hold
    /// when it is first seen, since its windows start empty then.
    ticked: bool,
}

/// What one tick did, for tests and the app's health detail.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickReport {
    /// Targets evaluated.
    pub evaluated: usize,
    /// Count writes that landed (task writes and target scales).
    pub scaled: usize,
    /// Dry-run decisions recorded.
    pub would: usize,
    /// Per target, the state reported.
    pub states: BTreeMap<String, String>,
    /// Per target, the one-line summary reported.
    pub summaries: BTreeMap<String, String>,
    /// Errors (inputs, writes, plugins, the report).
    pub errors: Vec<String>,
    /// How long each part took.
    pub timings: TickTimings,
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
/// the engine's own (see `TargetState::windows`), by the engine's clock.
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

/// A target's reading for one `legion` check: one member's value, or for a
/// group target, the members' utilizations weighted by their reservations
/// (sums for counts and rates).
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

/// One distinct external query in a tick: (source, query, window ms).
type QueryKey = (String, String, i64);

fn query_key(check: &EffectiveCheck) -> QueryKey {
    (
        check.source.clone(),
        check.query.clone(),
        i64::try_from(check.window.as_millis()).unwrap_or(i64::MAX),
    )
}

/// Whether a check reads from a source other than `legion`.
fn reads_external(check: &EffectiveCheck) -> bool {
    check.source != crate::autoscaler::defaults::LEGION_SOURCE && !check.query.trim().is_empty()
}

fn pause_of(info: &ScalingPauseInfo) -> Pause {
    Pause {
        until_ms: info.until_unix_ms,
        by: info.by.clone(),
        reason: info.reason.clone(),
    }
}

/// The inputs an evaluation starts from, whatever the target.
struct Inputs<'a> {
    key: TargetKey,
    /// The target's label for plugins and messages.
    label: String,
    effective: &'a EffectivePolicy,
    current: u32,
    healthy: u32,
    /// A task target's members' inputs (for `legion` checks).
    members: Vec<&'a ScalingInputs>,
    guards: Guards,
    pin: Option<Pin>,
    pause: Option<Pause>,
}

impl Engine {
    /// An engine that writes through `client`, started at `started_ms` (the
    /// cold-start hold counts from here).
    pub fn new(client: AuthedManagerClient, options: EngineOptions, started_ms: i64) -> Self {
        let host = match options.plugin_timeout {
            Some(timeout) => PluginHost::with_timeout(
                options.plugins.clone(),
                options.code_root.as_deref(),
                timeout,
            ),
            None => PluginHost::new(options.plugins.clone(), options.code_root.as_deref()),
        };
        Self {
            client,
            options,
            started_ms,
            policies: Arc::new(RwLock::new(None)),
            context: Arc::new(RwLock::new(None)),
            policy_objects: Arc::new(RwLock::new(ObjectCache::default())),
            group_objects: Arc::new(RwLock::new(ObjectCache::default())),
            statuses: StatusBook::default(),
            cleanups: HashMap::new(),
            targets: HashMap::new(),
            host: Arc::new(host),
            served_by: None,
            log: Default::default(),
            groups: HashMap::new(),
            ownership: Arc::new(RwLock::new(Ownership::default())),
            ticked: false,
        }
    }

    /// When a target first seen at `now_ms` starts its cold-start hold:
    /// the engine's start on the first tick, then when it is first seen.
    fn cold_start_ms(&self, now_ms: i64) -> i64 {
        if self.ticked {
            now_ms.max(self.started_ms)
        } else {
            self.started_ms
        }
    }

    /// A shard-lease controller for this engine's instance, publishing what
    /// it owns into the engine (docs/autoscaling.md, "Replicas"). Run it
    /// with [`Shards::spawn`], or step it by hand (tests).
    pub fn shards(&self, timing: LeaseTiming) -> Shards {
        Shards::new(
            self.client.clone(),
            &self.options.instance,
            timing,
            self.ownership.clone(),
        )
    }

    /// The shards this replica evaluates now.
    pub fn ownership(&self) -> Ownership {
        self.ownership
            .read()
            .map(|ownership| ownership.clone())
            .unwrap_or_default()
    }

    /// The engine's last hour of evaluations, which the UI views draw.
    pub fn evaluations(&self) -> Arc<crate::apps::autoscaler_ui::EvaluationLog> {
        self.log.clone()
    }

    /// The plugin host.
    pub fn host(&self) -> &PluginHost {
        &self.host
    }

    /// What the validate call for the app's kinds reads.
    pub fn validator(&self) -> objects::Validator {
        objects::Validator {
            context: self.context.clone(),
            plugins: self.options.plugins.clone(),
        }
    }

    /// Keep the policy cache and the object caches current until the
    /// returned task is aborted: `WatchScalingPolicies` (deltas, filtered to
    /// the shards this replica owns), and `WatchObjects` for the app's
    /// ScalingPolicies (the same shards) and NodeGroups (all of them, on the
    /// replica holding the `nodegroups` shard), each reconnecting with
    /// backoff and starting over whenever the owned shards change.
    pub fn spawn_watch(&self) -> tokio::task::JoinHandle<()> {
        let policy_watch = self.policy_watch();
        let owned = self.ownership();
        let (policy_tx, policy_rx) = tokio::sync::watch::channel(policy_selection(&owned));
        let (group_tx, group_rx) = tokio::sync::watch::channel(group_selection(&owned));
        let ownership = self.ownership.clone();
        // The lease controller publishes ownership in place; follow it into
        // the object watches' selections.
        let follow = async move {
            let mut every = tokio::time::interval(Duration::from_millis(250));
            loop {
                every.tick().await;
                let owned = ownership
                    .read()
                    .map(|ownership| ownership.clone())
                    .unwrap_or_default();
                policy_tx.send_if_modified(|current| {
                    let next = policy_selection(&owned);
                    let changed = *current != next;
                    *current = next;
                    changed
                });
                group_tx.send_if_modified(|current| {
                    let next = group_selection(&owned);
                    let changed = *current != next;
                    *current = next;
                    changed
                });
            }
        };
        let app = crate::autoscaler::policy::APP.to_string();
        let policies = run_object_watch(
            self.client.clone(),
            app.clone(),
            vec![crate::autoscaler::policy::SCALING_POLICY_KIND.to_string()],
            policy_rx,
            self.policy_objects.clone(),
        );
        let groups = run_object_watch(
            self.client.clone(),
            app,
            vec![crate::autoscaler::policy::NODE_GROUP_KIND.to_string()],
            group_rx,
            self.group_objects.clone(),
        );
        tokio::spawn(async move {
            tokio::join!(policy_watch, follow, policies, groups);
        })
    }

    /// The `WatchScalingPolicies` loop (see [`Self::spawn_watch`]).
    fn policy_watch(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut client = self.client.clone();
        let policies = self.policies.clone();
        let context = self.context.clone();
        let ownership = self.ownership.clone();
        async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                let owned = ownership
                    .read()
                    .map(|ownership| ownership.clone())
                    .unwrap_or_default();
                let request = WatchScalingPoliciesRequest {
                    namespaces: Vec::new(),
                    shard_count: if owned.shards.is_some() { SHARDS } else { 0 },
                    shards: owned.list(),
                    deltas: true,
                };
                match client.watch_scaling_policies(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        backoff = Duration::from_millis(500);
                        let mut check = tokio::time::interval(Duration::from_millis(250));
                        // The parts of a change split across messages
                        // (`more`), merged before the engine sees any.
                        let mut staging: Option<PolicyCache> = None;
                        loop {
                            let message = tokio::select! {
                                message = stream.message() => message,
                                _ = check.tick() => {
                                    let epoch = ownership.read().map(|o| o.epoch).unwrap_or(0);
                                    if epoch != owned.epoch {
                                        debug!("The owned shards changed: restarting the policy watch");
                                        break;
                                    }
                                    continue;
                                }
                            };
                            match message {
                                Ok(Some(set)) => {
                                    debug!(
                                        policies = set.policies.len(),
                                        objects = set.objects.len(),
                                        removed = set.removed.len(),
                                        full = set.full,
                                        more = set.more,
                                        generation = set.generation,
                                        "Scaling policies updated"
                                    );
                                    if let Some(sent) = &set.context {
                                        if let Ok(mut context) = context.write() {
                                            *context = Some(sent.clone());
                                        }
                                    }
                                    let more = set.more;
                                    if staging.is_none() && !set.full && !more {
                                        // A whole delta: apply it in place.
                                        if let Ok(mut cache) = policies.write() {
                                            cache
                                                .get_or_insert_with(PolicyCache::default)
                                                .apply(set);
                                        }
                                        continue;
                                    }
                                    let mut next = match staging.take() {
                                        Some(staged) => staged,
                                        None if set.full => PolicyCache::default(),
                                        None => policies
                                            .read()
                                            .ok()
                                            .and_then(|cache| cache.clone())
                                            .unwrap_or_default(),
                                    };
                                    next.apply(set);
                                    if more {
                                        staging = Some(next);
                                    } else if let Ok(mut cache) = policies.write() {
                                        *cache = Some(next);
                                    }
                                }
                                Ok(None) => break,
                                Err(status) => {
                                    warn!("The scaling policy watch ended: {}", status.message());
                                    break;
                                }
                            }
                        }
                        // An epoch change restarts at once, with a full set.
                        let epoch = ownership.read().map(|o| o.epoch).unwrap_or(0);
                        if epoch != owned.epoch {
                            continue;
                        }
                    }
                    Err(status) => {
                        warn!("Cannot watch scaling policies: {}", status.message());
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCH_BACKOFF_MAX);
            }
        }
    }

    /// How many policies the caches hold: task policies, and the app's
    /// ScalingPolicies and NodeGroups (`None` before the first policy set).
    pub fn policy_count(&self) -> Option<usize> {
        let tasks = self
            .policies
            .read()
            .ok()
            .and_then(|cache| cache.as_ref().map(|cache| cache.tasks.len()))?;
        let objects: usize = [&self.policy_objects, &self.group_objects]
            .iter()
            .filter_map(|cache| cache.read().ok().map(|cache| cache.len()))
            .sum();
        Some(tasks + objects)
    }

    /// Whether both object watches (ScalingPolicies, NodeGroups) have
    /// completed their first snapshot since they last (re)connected.
    pub fn objects_synced(&self) -> bool {
        [&self.policy_objects, &self.group_objects]
            .iter()
            .all(|cache| cache.read().is_ok_and(|cache| cache.synced()))
    }

    /// Replace the policy cache's task policies (tests, or a caller with
    /// its own source).
    pub fn set_policies(&self, policies: Vec<ScalingPolicy>) {
        if let Ok(mut cache) = self.policies.write() {
            let (objects, context) = cache
                .as_ref()
                .map(|cache| (cache.objects.clone(), cache.context.clone()))
                .unwrap_or_default();
            *cache = Some(PolicyCache {
                tasks: policies
                    .into_iter()
                    .map(|policy| {
                        (
                            shard::task_key(&policy.job_id, &policy.task_group, &policy.task_name),
                            policy,
                        )
                    })
                    .collect(),
                objects,
                context,
            });
        }
    }

    /// The app's objects this replica runs this tick, resolved: its
    /// ScalingPolicies (of the shards it holds) and, when it holds the
    /// `nodegroups` shard, every NodeGroup. Empty before the policy watch
    /// has sent the layered defaults.
    fn object_targets(&self, cache: &PolicyCache, owned: &Ownership) -> Vec<ObjectTarget> {
        let Some(layers) = objects::layers_of(cache.context.as_ref(), &self.options.plugins) else {
            return Vec::new();
        };
        let paused_all = cache
            .context
            .as_ref()
            .and_then(|context| context.paused_all.as_ref());
        let force_dry_run = self.options.force_mode == Some(Mode::DryRun);
        let mut out = Vec::new();
        let groups_here = owned.owns(shard::shard_of(shard::NODE_GROUPS_KEY, SHARDS));
        for (cache_lock, groups) in [(&self.policy_objects, false), (&self.group_objects, true)] {
            if groups && !groups_here {
                continue;
            }
            let Ok(objects) = cache_lock.read() else {
                continue;
            };
            for object in objects.objects() {
                // The watch is filtered to the shards held, but lags a
                // rebalance by a moment.
                if !groups
                    && !owned.owns(crate::structs::app_object::object_shard(
                        &object.app,
                        &object.kind,
                        &object.namespace,
                        &object.name,
                        SHARDS,
                    ))
                {
                    continue;
                }
                let state = cache
                    .objects
                    .get(&crate::manager::autoscale::object_target_key(object.uid));
                if let Some(target) =
                    objects::resolve_target(object, &layers, state, paused_all, force_dry_run)
                {
                    out.push(target);
                }
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out
    }

    /// One evaluation of every policy at `now_ms`: read, decide, write,
    /// report.
    #[instrument(skip_all, fields(now_ms))]
    pub async fn tick(&mut self, now_ms: i64) -> TickReport {
        let mut report = TickReport::default();
        let started = Instant::now();
        let cache = self
            .policies
            .read()
            .ok()
            .and_then(|cache| cache.clone())
            .unwrap_or_default();
        // Only the shards this replica holds: the watch is filtered to them
        // too, but lags a rebalance by a moment.
        let owned = self.ownership();
        let tasks: Vec<&ScalingPolicy> = cache
            .tasks
            .iter()
            .filter(|(key, _)| owned.owns(shard::shard_of(key, SHARDS)))
            .map(|(_, policy)| policy)
            .collect();
        let object_targets = self.object_targets(&cache, &owned);
        let mut reports = Vec::with_capacity(tasks.len() + object_targets.len());
        let live: BTreeSet<TargetKey> = tasks
            .iter()
            .map(|p| (p.job_id.clone(), p.task_group.clone(), p.task_name.clone()))
            .chain(object_targets.iter().map(ObjectTarget::target_key))
            .collect();
        self.targets.retain(|key, _| live.contains(key));
        self.log.retain(&live);
        self.cleanups.clear();

        // Parse the valid policies; report the invalid ones as held.
        let mut valid: Vec<(&ScalingPolicy, EffectivePolicy)> = Vec::new();
        for policy in tasks.iter().copied() {
            if policy.state != "ok" {
                reports.push(plain_report(
                    task_report(policy),
                    "invalid",
                    0,
                    &policy.error,
                    now_ms,
                ));
                continue;
            }
            match serde_json::from_str::<EffectivePolicy>(&policy.policy_json) {
                Ok(mut effective) => {
                    if let Some(mode) = self.options.force_mode {
                        effective.mode = mode;
                    }
                    valid.push((policy, effective));
                }
                Err(err) => reports.push(plain_report(
                    task_report(policy),
                    "error",
                    0,
                    &format!("the policy does not parse in this build: {err}"),
                    now_ms,
                )),
            }
        }
        // The app's objects: one that does not resolve (its spec, the
        // layered defaults, a plugin closed to its namespace) is held
        // `invalid`, never run.
        let mut standalone: Vec<(&ObjectTarget, EffectivePolicy)> = Vec::new();
        let mut node_groups: Vec<(&ObjectTarget, EffectiveNodeGroup)> = Vec::new();
        for target in &object_targets {
            match &target.plan {
                Err(err) => reports.push(plain_report(
                    object_report(target),
                    "invalid",
                    0,
                    err,
                    now_ms,
                )),
                Ok(Plan::External(effective)) => {
                    standalone.push((target, effective.as_ref().clone()))
                }
                Ok(Plan::Nodes(group)) => node_groups.push((target, group.as_ref().clone())),
            }
        }

        // Every distinct external query, once.
        let external = self
            .read_external(
                valid
                    .iter()
                    .map(|(policy, effective)| {
                        (
                            crate::manager::autoscale::target_label(
                                &policy.job_id,
                                &policy.task_group,
                                &policy.task_name,
                            ),
                            effective,
                        )
                    })
                    .chain(
                        standalone
                            .iter()
                            .map(|(target, effective)| (target.id.clone(), effective)),
                    ),
                now_ms,
            )
            .await;

        // One batched read of every task target's inputs.
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
        let read_at = Instant::now();
        report.timings.inputs = read_at - started;

        for (policy, effective) in &valid {
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
                reports.push(plain_report(task_report(policy), "error", 0, &why, now_ms));
                continue;
            };
            let target_inputs = Inputs {
                key: key.clone(),
                label: crate::manager::autoscale::target_label(&key.0, &key.1, &key.2),
                effective,
                current: owner.desired_replicas,
                healthy: owner.ready_replicas,
                members,
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
                pause: owner.pause.as_ref().map(pause_of),
            };
            let (decision, scale_decision, mut target_report) = match self
                .decide(
                    &target_inputs,
                    &external,
                    task_report(policy),
                    now_ms,
                    &mut report,
                )
                .await
            {
                Ok(decided) => decided,
                Err(held) => {
                    report.evaluated += 1;
                    reports.push(*held);
                    continue;
                }
            };
            self.act_task(
                &key,
                owner,
                &decision,
                scale_decision,
                &mut target_report,
                now_ms,
                &mut report,
            )
            .await;
            report.evaluated += 1;
            reports.push(target_report);
        }

        for (policy, effective) in &standalone {
            let target_report = self
                .evaluate_standalone(policy, effective, &external, now_ms, &mut report)
                .await;
            report.evaluated += 1;
            reports.push(target_report);
        }

        let group_reports = self
            .evaluate_node_groups(&node_groups, now_ms, &mut report)
            .await;
        report.evaluated += group_reports.len();
        reports.extend(group_reports);

        for target in &reports {
            let label = match &target.object {
                None => format!(
                    "{}/{}/{}",
                    target.job_id, target.task_group, target.task_name
                ),
                Some(object) => object_label(object),
            };
            report.states.insert(label.clone(), target.state.clone());
            report.summaries.insert(label, target.summary.clone());
        }
        let decided_at = Instant::now();
        report.timings.decide = decided_at - read_at;
        // Each object's durable verdict, written only when it changed.
        self.write_statuses(&object_targets, &reports).await;
        let request = ReportScalingStatusRequest {
            instance: self.options.instance.clone(),
            interval_seconds: self.options.interval.as_secs().max(1) as u32,
            mode: match self.options.force_mode {
                Some(Mode::DryRun) => "dry-run".to_string(),
                _ => "enforce".to_string(),
            },
            targets: reports,
            version: env!("CARGO_PKG_VERSION").to_string(),
            shards: owned.list(),
            shard_count: if owned.sharded { SHARDS } else { 0 },
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
        report.timings.report = decided_at.elapsed();
        self.ticked = true;
        report
    }

    /// Run every distinct external query of these policies once, at once.
    async fn read_external<'a>(
        &self,
        policies: impl Iterator<Item = (String, &'a EffectivePolicy)>,
        now_ms: i64,
    ) -> HashMap<QueryKey, Result<SourceReading, String>> {
        let mut wanted: BTreeMap<QueryKey, (String, String)> = BTreeMap::new();
        for (label, effective) in policies {
            for check in effective
                .all_checks()
                .iter()
                .filter(|check| reads_external(check))
            {
                wanted
                    .entry(query_key(check))
                    .or_insert_with(|| (label.clone(), check.name.clone()));
            }
        }
        let host = self.host.clone();
        let calls = wanted.into_iter().map(|(key, (label, check))| {
            let host = host.clone();
            async move {
                let window = Duration::from_millis(u64::try_from(key.2).unwrap_or(0));
                let call = SourceCall {
                    policy: &label,
                    check: &check,
                    query: &key.1,
                    window,
                };
                let result = host.query(&key.0, &call, now_ms).await;
                (key, result)
            }
        });
        futures::future::join_all(calls).await.into_iter().collect()
    }

    /// Build the observation and run the pipeline. `Err` carries the report
    /// of a target that could not be evaluated (a plugin config its schema
    /// refuses).
    async fn decide(
        &mut self,
        inputs: &Inputs<'_>,
        external: &HashMap<QueryKey, Result<SourceReading, String>>,
        base: ScalingTargetReport,
        now_ms: i64,
        report: &mut TickReport,
    ) -> Result<(Decision, ScaleDecision, ScalingTargetReport), Box<ScalingTargetReport>> {
        let started_ms = self.cold_start_ms(now_ms);
        let host = self.host.clone();
        let effective = inputs.effective;
        let state = self
            .targets
            .entry(inputs.key.clone())
            .or_insert_with(|| TargetState::new(started_ms));
        state.observe_count(inputs.current, now_ms);
        let warming = state
            .readiness
            .observe(inputs.healthy, now_ms, effective.warmup);
        let mut observation = Observation {
            current: inputs.current,
            ready: inputs.healthy.saturating_sub(warming),
            warming,
            readings: BTreeMap::new(),
            guards: inputs.guards.clone(),
            pin: inputs.pin.clone(),
            pause: inputs.pause.clone(),
            recommendations: BTreeMap::new(),
        };
        let checks = effective.all_checks();
        for check in &checks {
            let reading = if check.source == crate::autoscaler::defaults::LEGION_SOURCE {
                if check.query.trim().is_empty() {
                    continue;
                }
                match check_reading(check, &inputs.members) {
                    Reading::Value(value) => {
                        Reading::Value(state.windows.add(&check.name, value, now_ms, check.window))
                    }
                    // Missing data is missing now: a window of old samples
                    // does not stand in for it.
                    missing => missing,
                }
            } else if reads_external(check) {
                match external.get(&query_key(check)) {
                    Some(Ok(SourceReading::Value {
                        value,
                        windowed: true,
                    })) => Reading::Value(*value),
                    Some(Ok(SourceReading::Value {
                        value,
                        windowed: false,
                    })) => {
                        Reading::Value(state.windows.add(&check.name, *value, now_ms, check.window))
                    }
                    Some(Ok(SourceReading::Missing(why))) => Reading::Missing(why.clone()),
                    Some(Err(err)) => Reading::Missing(err.clone()),
                    None => Reading::Missing("the source was not read".to_string()),
                }
            } else {
                continue;
            };
            observation.readings.insert(check.name.clone(), reading);
        }

        // Plugin strategies answer before the pipeline runs (it is pure).
        let mut invalid: Option<String> = None;
        for check in &checks {
            let StrategyConfig::Plugin(plugin) = &check.strategy else {
                continue;
            };
            let reading = match observation.readings.get(&check.name) {
                Some(Reading::Value(value)) => Some(*value),
                // The pipeline reports the missing reading itself.
                Some(Reading::Missing(_)) => continue,
                None => None,
            };
            let config = plugin.config_value();
            let call = RecommendCall {
                policy: &inputs.label,
                check: &check.name,
                current: observation.current,
                ready: observation.ready,
                reading,
                config: config.clone(),
            };
            if let Err(host::Problem::Invalid(err)) = host
                .check(
                    &plugin.name,
                    crate::autoscaler::plugins::Kind::Strategy,
                    &config,
                )
                .await
            {
                invalid = Some(format!("check {}: {err}", check.name));
                break;
            }
            let answer = match host.recommend(&plugin.name, &call).await {
                Ok((desired, note)) => PluginRecommendation {
                    desired: Some(desired),
                    note,
                    error: None,
                },
                Err(err) => {
                    report.errors.push(err.clone());
                    PluginRecommendation {
                        desired: None,
                        note: String::new(),
                        error: Some(err),
                    }
                }
            };
            observation
                .recommendations
                .insert(check.name.clone(), answer);
        }
        if let Some(why) = invalid {
            let mut held = base;
            held.state = "invalid".to_string();
            held.current = inputs.current;
            held.want = inputs.current;
            held.evaluated_at_unix_ms = now_ms;
            held.summary = why.clone();
            if state.invalid.as_deref() != Some(why.as_str()) {
                held.events.push(ScalingEventFlag {
                    kind: "check_failed".to_string(),
                    message: why.clone(),
                    outcome: String::new(),
                });
            }
            state.invalid = Some(why);
            return Err(Box::new(held));
        }
        state.invalid = None;

        let decision = evaluate(effective, &mut state.policy_state, &observation, now_ms);

        // State changes worth an event: the engine's flags, and a check
        // that newly lost its data.
        let mut events: Vec<ScalingEventFlag> = decision
            .flags
            .iter()
            .map(|flag| match flag {
                Flag::Limited { reason } => ScalingEventFlag {
                    kind: "limited".to_string(),
                    message: reason.clone(),
                    outcome: String::new(),
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
                    outcome: String::new(),
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
                outcome: String::new(),
            });
        }
        state.missing = missing;
        let scale_decision = ScaleDecision {
            id: decision_id(&inputs.key, now_ms),
            summary: decision.summary.clone(),
            policy_digest: decision.policy_digest.clone(),
            policy_json: effective.canonical_json(),
            detail_json: serde_json::to_string(&decision).unwrap_or_default(),
            evaluated_at_unix_ms: now_ms,
        };
        let mut target_report = base;
        target_report.state = state_of(&decision).to_string();
        target_report.current = inputs.current;
        target_report.want = decision.to;
        target_report.evaluated_at_unix_ms = now_ms;
        target_report.summary = decision.summary.clone();
        target_report.events = events;
        self.log.record(
            &inputs.key.0,
            &inputs.key.1,
            &inputs.key.2,
            effective,
            &decision,
            inputs.current,
            decision.to,
            now_ms,
        );
        Ok((decision, scale_decision, target_report))
    }

    /// Record the reported decision in full only when it changed (ignoring
    /// its time).
    fn finish_report(
        &mut self,
        key: &TargetKey,
        decision: &Decision,
        target_report: &mut ScalingTargetReport,
    ) {
        let Some(state) = self.targets.get_mut(key) else {
            return;
        };
        let mut stable = decision.clone();
        stable.at_ms = 0;
        let digest = hash_of(&serde_json::to_string(&stable).unwrap_or_default());
        target_report.decision_json = if state.reported == Some(digest) {
            String::new()
        } else {
            state.reported = Some(digest);
            serde_json::to_string(decision).unwrap_or_default()
        };
    }

    #[allow(clippy::too_many_arguments)]
    async fn act_task(
        &mut self,
        key: &TargetKey,
        owner: &ScalingInputs,
        decision: &Decision,
        scale_decision: ScaleDecision,
        target_report: &mut ScalingTargetReport,
        now_ms: i64,
        report: &mut TickReport,
    ) {
        let mut decision = decision.clone();
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
                let outcome = self.client.scale_task(request).await;
                let state = self.targets.get_mut(key).expect("decided above");
                match outcome {
                    Ok(reply) => {
                        let reply = reply.into_inner();
                        if reply.deferred {
                            target_report.state = "deferred".to_string();
                            target_report.summary =
                                format!("{}; {}", target_report.summary, reply.message);
                        } else {
                            state
                                .policy_state
                                .record_applied(now_ms, decision.from, decision.to);
                            state.expected = Some(decision.to);
                            report.scaled += 1;
                        }
                    }
                    Err(status) => {
                        // A version conflict or a pause that raced this
                        // tick: nothing changed; the next tick re-reads.
                        let held = status.code() == tonic::Code::FailedPrecondition;
                        target_report.state = if held { "held" } else { "error" }.to_string();
                        target_report.summary = format!("write refused: {}", status.message());
                        decision.to = decision.from;
                        target_report.want = decision.from;
                        report.errors.push(target_report.summary.clone());
                    }
                }
            }
            DecisionKind::Would => {
                let state = self.targets.get_mut(key).expect("decided above");
                if state.last_would != Some((decision.from, decision.to)) {
                    let request = RecordScalingActionRequest {
                        job_id: key.0.clone(),
                        task_group: key.1.clone(),
                        task_name: key.2.clone(),
                        kind: "would".to_string(),
                        from: decision.from,
                        to: decision.to,
                        decision: Some(scale_decision),
                        instance: self.options.instance.clone(),
                        ..Default::default()
                    };
                    match self.client.record_scaling_action(request).await {
                        Ok(_) => {
                            if let Some(state) = self.targets.get_mut(key) {
                                state.last_would = Some((decision.from, decision.to));
                            }
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
                let state = self.targets.get_mut(key).expect("decided above");
                if decision.from == decision.to {
                    state.last_would = None;
                }
            }
        }
        self.finish_report(key, &decision, target_report);
    }

    /// Evaluate one standalone policy: read its target, decide, record, and
    /// scale the target.
    async fn evaluate_standalone(
        &mut self,
        policy: &ObjectTarget,
        effective: &EffectivePolicy,
        external: &HashMap<QueryKey, Result<SourceReading, String>>,
        now_ms: i64,
        report: &mut TickReport,
    ) -> ScalingTargetReport {
        let key = policy.target_key();
        let base = object_report(policy);
        let config = policy.target_config.clone();
        let host = self.host.clone();
        if let Err(problem) = host
            .check(
                &policy.target_plugin,
                crate::autoscaler::plugins::Kind::Target,
                &config,
            )
            .await
        {
            let started_ms = self.cold_start_ms(now_ms);
            let state = self
                .targets
                .entry(key.clone())
                .or_insert_with(|| TargetState::new(started_ms));
            return match problem {
                host::Problem::Invalid(err) => {
                    let mut held =
                        plain_report(base, "invalid", 0, &format!("target: {err}"), now_ms);
                    if state.invalid.as_deref() != Some(err.as_str()) {
                        held.events.push(ScalingEventFlag {
                            kind: "check_failed".to_string(),
                            message: format!("target: {err}"),
                            outcome: String::new(),
                        });
                        state.invalid = Some(err);
                    }
                    held
                }
                host::Problem::Unavailable(err) => {
                    report.errors.push(format!("{}: {err}", policy.id));
                    plain_report(base, "error", 0, &format!("target: {err}"), now_ms)
                }
            };
        }
        let status = match host
            .status(&policy.target_plugin, &policy.id, &config, effective.min)
            .await
        {
            Ok(status) => status,
            Err(err) => {
                report.errors.push(format!("{}: {err}", policy.id));
                return plain_report(base, "error", 0, &format!("target status: {err}"), now_ms);
            }
        };
        let inputs = Inputs {
            key: key.clone(),
            label: policy.id.clone(),
            effective,
            current: status.count,
            healthy: status.ready,
            members: Vec::new(),
            guards: Guards::default(),
            pin: None,
            pause: policy.pause.as_ref().map(pause_of),
        };
        let (mut decision, scale_decision, mut target_report) =
            match self.decide(&inputs, external, base, now_ms, report).await {
                Ok(decided) => decided,
                Err(held) => return *held,
            };
        match decision.kind {
            DecisionKind::Applied => {
                // An intent first: a pause refuses it on the server, and of
                // two replicas whose leases overlap only one wins the
                // compare-and-set and calls the target.
                let known = policy.action.as_ref().map_or(0, |action| action.version);
                let expected = {
                    let state = self.targets.get_mut(&key).expect("decided above");
                    state.action_version = state.action_version.max(known);
                    state.action_version
                };
                let opened = self
                    .client
                    .record_scaling_action(RecordScalingActionRequest {
                        kind: "intent".to_string(),
                        object: Some(policy.reference.clone()),
                        expected_action_version: expected,
                        intent: Some(ScalingActionIntent {
                            decision_id: scale_decision.id.clone(),
                            from: decision.from,
                            desired: Some(decision.to),
                            summary: decision.summary.clone(),
                            ..Default::default()
                        }),
                        decision: Some(scale_decision.clone()),
                        instance: self.options.instance.clone(),
                        ..Default::default()
                    })
                    .await;
                let won = match opened {
                    Err(status) => {
                        let held = status.code() == tonic::Code::FailedPrecondition;
                        target_report.state = if held { "held" } else { "error" }.to_string();
                        target_report.summary = format!("write refused: {}", status.message());
                        target_report.want = decision.from;
                        decision.to = decision.from;
                        report.errors.push(target_report.summary.clone());
                        None
                    }
                    Ok(reply) => {
                        let reply = reply.into_inner();
                        let state = self.targets.get_mut(&key).expect("decided above");
                        if let Some(action) = &reply.action {
                            state.action_version = action.version;
                        }
                        if reply.won {
                            Some(state.action_version)
                        } else {
                            // Another replica acted on this policy first.
                            target_report.state = "held".to_string();
                            target_report.summary = format!(
                                "{} acted on this policy first; following its intent",
                                reply
                                    .action
                                    .as_ref()
                                    .map(|action| action.owner.as_str())
                                    .unwrap_or("another replica")
                            );
                            target_report.want = decision.from;
                            decision.to = decision.from;
                            None
                        }
                    }
                };
                if let Some(version) = won {
                    let scaled = host
                        .scale(
                            &policy.target_plugin,
                            &policy.id,
                            &config,
                            decision.to,
                            &scale_decision.id,
                        )
                        .await;
                    let (outcome, detail) = match &scaled {
                        Ok(()) => ("succeeded", String::new()),
                        Err(err) => ("failed", err.clone()),
                    };
                    let closed = self
                        .client
                        .record_scaling_action(RecordScalingActionRequest {
                            kind: "outcome".to_string(),
                            object: Some(policy.reference.clone()),
                            expected_action_version: version,
                            outcome: outcome.to_string(),
                            detail,
                            instance: self.options.instance.clone(),
                            ..Default::default()
                        })
                        .await;
                    let state = self.targets.get_mut(&key).expect("decided above");
                    match closed {
                        Ok(reply) => {
                            if let Some(action) = reply.into_inner().action {
                                state.action_version = action.version;
                            }
                        }
                        Err(status) => report.errors.push(format!(
                            "{}: closing the intent: {}",
                            policy.id,
                            status.message()
                        )),
                    }
                    match scaled {
                        Ok(()) => {
                            state
                                .policy_state
                                .record_applied(now_ms, decision.from, decision.to);
                            state.expected = Some(decision.to);
                            report.scaled += 1;
                        }
                        Err(err) => {
                            target_report.state = "error".to_string();
                            target_report.summary = format!("target/scale failed: {err}");
                            target_report.want = decision.from;
                            report.errors.push(format!("{}: {err}", policy.id));
                            decision.to = decision.from;
                        }
                    }
                }
            }
            DecisionKind::Would => {
                let state = self.targets.get_mut(&key).expect("decided above");
                if state.last_would != Some((decision.from, decision.to)) {
                    let recorded = self
                        .client
                        .record_scaling_action(RecordScalingActionRequest {
                            kind: "would".to_string(),
                            from: decision.from,
                            to: decision.to,
                            decision: Some(scale_decision),
                            object: Some(policy.reference.clone()),
                            ..Default::default()
                        })
                        .await;
                    match recorded {
                        Ok(_) => {
                            if let Some(state) = self.targets.get_mut(&key) {
                                state.last_would = Some((decision.from, decision.to));
                            }
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
                let state = self.targets.get_mut(&key).expect("decided above");
                if decision.from == decision.to {
                    state.last_would = None;
                }
            }
        }
        self.finish_report(&key, &decision, &mut target_report);
        target_report
    }
}

impl Engine {
    /// Write each object's durable verdict ([`objects::verdict`]) to its
    /// status when it differs from what is stored: a handful of writes
    /// when the world changes, none on a quiet tick. A write that fails is
    /// tried again next tick.
    async fn write_statuses(&mut self, targets: &[ObjectTarget], reports: &[ScalingTargetReport]) {
        let live: std::collections::HashSet<u64> =
            targets.iter().map(|target| target.object.uid).collect();
        self.statuses.retain(&live);
        let by_uid: HashMap<u64, &ScalingTargetReport> = reports
            .iter()
            .filter_map(|tick| tick.object.as_ref().map(|object| (object.uid, tick)))
            .collect();
        let mut writes = Vec::new();
        for target in targets {
            let Some(tick) = by_uid.get(&target.object.uid) else {
                continue;
            };
            let outcome = objects::verdict(
                target,
                &tick.state,
                &tick.summary,
                self.cleanups.get(&target.key).cloned(),
            );
            if self.statuses.differs(&target.object, &outcome) {
                writes.push((target.object.clone(), outcome));
            }
        }
        if writes.is_empty() {
            return;
        }
        use futures::StreamExt;
        let client = self.client.clone();
        let results: Vec<_> = futures::stream::iter(writes.into_iter().map(|(object, outcome)| {
            let client = client.clone();
            async move {
                let written =
                    crate::apps::objects::write_object_status(&client, &object, &outcome).await;
                (object, outcome, written)
            }
        }))
        .buffer_unordered(8)
        .collect()
        .await;
        for (object, outcome, written) in results {
            match written {
                Ok(_) => self.statuses.record(&object, &outcome),
                Err(status) => {
                    // A deleted object, or one whose generation this manager
                    // has not seen yet: the next tick tries again.
                    debug!(
                        kind = %object.qualified_kind,
                        object = %object.id,
                        uid = object.uid,
                        "Writing the object's status: {}",
                        status.message()
                    );
                    self.statuses.forget(object.uid);
                }
            }
        }
    }

    /// Release a deleting object whose cleanup is done.
    async fn release(&mut self, target: &ObjectTarget, report: &mut TickReport) {
        match self
            .client
            .release_object(ReleaseObjectRequest {
                kind: target.reference.kind.clone(),
                namespace: target.reference.namespace.clone(),
                name: target.reference.name.clone(),
                uid: target.reference.uid,
                idempotency_key: None,
            })
            .await
        {
            Ok(_) => {
                info!(object = %target.label, uid = target.reference.uid, "Released a deleted object: its cleanup is done")
            }
            Err(status) if status.code() == tonic::Code::NotFound => {}
            Err(status) => report.errors.push(format!(
                "{}: releasing it: {}",
                target.label,
                status.message()
            )),
        }
    }
}

/// A report that carries no decision: an invalid policy, an error.
fn plain_report(
    mut base: ScalingTargetReport,
    state: &str,
    current: u32,
    summary: &str,
    now_ms: i64,
) -> ScalingTargetReport {
    base.state = state.to_string();
    base.current = current;
    base.want = current;
    base.evaluated_at_unix_ms = now_ms;
    base.summary = summary.to_string();
    base
}

/// A report naming a task target.
fn task_report(policy: &ScalingPolicy) -> ScalingTargetReport {
    ScalingTargetReport {
        job_id: policy.job_id.clone(),
        task_group: policy.task_group.clone(),
        task_name: policy.task_name.clone(),
        policy_digest: policy.digest.clone(),
        ..Default::default()
    }
}

/// A report naming one of the app's objects.
fn object_report(target: &ObjectTarget) -> ScalingTargetReport {
    ScalingTargetReport {
        object: Some(target.reference.clone()),
        policy_digest: target.digest.clone(),
        ..Default::default()
    }
}

/// `ScalingPolicy media/render-farm`: how a tick report names an object.
fn object_label(object: &crate::structs::ScalingObjectRef) -> String {
    let bare = object
        .kind
        .split_once('/')
        .map_or(object.kind.as_str(), |(_, bare)| bare);
    format!(
        "{bare} {}",
        crate::autoscaler::policy::object_label(&object.namespace, &object.name)
    )
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
        assert!(defaults.plugins.is_empty());
        let installed = EngineOptions::from_config(&json!({ "plugins": {
            "q": { "builtin": "http", "config": { "url": "http://q.internal/depth" } },
        } }))?;
        assert!(installed.plugins.get("q").is_some());
        assert!(EngineOptions::from_config(&json!({ "interval": "0s" })).is_err());
        assert!(EngineOptions::from_config(&json!({ "mode": "yolo" })).is_err());
        assert!(EngineOptions::from_config(&json!({ "plugins": { "q": {} } })).is_err());
        Ok(())
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
    /// missing, not zero.
    #[test]
    fn readings_weigh_groups_test() {
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
    }

    /// A count the engine did not write spends the step budget when seen.
    #[test]
    fn changes_by_others_spend_the_step_budget_test() {
        let mut state = TargetState::new(0);
        state.observe_count(2, 1_000);
        assert_eq!(
            state.policy_state.applied().count(),
            0,
            "the first sight is not a change"
        );
        state.observe_count(6, 16_000);
        let changes: Vec<_> = state.policy_state.applied().collect();
        assert_eq!(changes.len(), 1);
        assert_eq!(
            (changes[0].from, changes[0].to, changes[0].at_ms),
            (2, 6, 16_000)
        );
        // The engine's own write is expected, and not counted twice.
        state.policy_state.record_applied(31_000, 6, 8);
        state.expected = Some(8);
        state.observe_count(8, 46_000);
        assert_eq!(state.policy_state.applied().count(), 2);
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
