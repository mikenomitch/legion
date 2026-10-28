//! The bundled autoscaler's UI, built as app views (autoscaler-v2 plan
//! §6.14 and Phase 6; docs/autoscaling.md, "In the UI"; docs/apps.md,
//! "Views and actions").
//!
//! Nothing here is a hand-written SPA page: the autoscaler's manifest
//! (`bundled/autoscaler.ncl`) registers its slots, and these handlers answer
//! them with `legion-view/1` documents, which the SPA, `legion app view`, and
//! the `legion_app_view` MCP tool all render:
//!
//! - **`job`**: the Autoscaling panel on a job's page: now, want, and bounds
//!   per autoscaled task, an hour of replicas against the signal the policy
//!   reads, a timeline of the job's scaling decisions, and the Pause,
//!   Resume, and Release actions. Hidden for a job with no autoscaled task.
//! - **`task-badge`**: the `auto 6 · 2..8` badge on an autoscaled task's row.
//! - **`policies`**: the sidebar's Autoscaling page, every policy the viewer
//!   may read (`Viewer::sees_namespace`, from the viewer's `scale:read`
//!   namespaces), with the app's own health.
//! - **`node`**: the node-group panel on a node's page: the group its join
//!   token put it in, the group's policy state (mode, bounds, the count the
//!   provider holds and the count wanted, the last evaluation, drains and
//!   protected members), the group's members in its pool, and how many
//!   nodes like it the pending work would need. Hidden for a node in no
//!   group.
//!
//! The manager has already checked the viewer's declared permission
//! (`scale:read` on the job, `node:read` on the node's pool) before any of
//! this runs. The data comes from the app's own state, the last hour of its
//! evaluations ([`EvaluationLog`], kept by the engine), and from RPCs the app
//! makes with its own grant (`scale:read`/`scale:write` in every namespace,
//! `node:read`): `GetScalingStatus`, `GetScaleHistory`, `PauseScaling`,
//! `ScaleTask`, `GetNode`, `GetNodes`, and `GetCapacityDemand`. Actions act
//! only on the job in their context, and record the viewer in the reason.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use crate::apps::ui::{ActionOutcome, ActionRequest, Refused, UiRegistry, ViewRequest, Viewer};
use crate::autoscaler::strategies::StrategyConfig;
use crate::autoscaler::{Decision, EffectivePolicy};
use crate::cmd::helpers::AuthedManagerClient;
use crate::prelude::*;
use crate::structs::app_ui::{
    ActionButton, ActionsBlock, AppView, Badge, Block, Cell, Column, ColumnType, KeyValueBlock,
    KeyValueItem, NoticeBlock, RichText, Series, SeriesBlock, Stat, StatsBlock, TableBlock,
    TimelineBlock, TimelineItem, Tone, ViewContext,
};
use crate::structs::{
    GetCapacityDemandRequest, GetNodeRequest, GetNodesRequest, GetScaleHistoryRequest,
    GetScalingStatusReply, GetScalingStatusRequest, NodeGroupTemplate, NodeMsg,
    PauseScalingRequest, ScaleHistoryEntry, ScaleTaskRequest, ScalingTargetStatus, JOIN_ATTR,
    NODEGROUP_ATTR, PROVIDER_ID_ATTR, SCALE_IN_ATTR,
};

/// How much of its evaluations the app keeps per target.
pub const LOG_WINDOW: Duration = Duration::from_secs(3600);
/// Most evaluations kept per target (an hour at a 2-second interval).
const LOG_MAX_POINTS: usize = 1800;
/// Most decisions the job panel's timeline shows.
const TIMELINE_ROWS: u32 = 20;
/// How long a node group's capacity demand is reused between views: the
/// simulation is not free, and a node page re-fetches every 10 seconds.
const DEMAND_TTL: Duration = Duration::from_secs(30);
/// Most members the node-group panel lists.
const MEMBER_ROWS: usize = 50;

/// One target: `job/group/task`.
type TargetKey = (String, String, String);

/// What one check reads, and what it holds the reading at.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckInfo {
    pub name: String,
    pub source: String,
    pub query: String,
    /// The `target-value` strategy's target, when the check has one.
    pub target: Option<f64>,
}

/// One evaluation of one target, as the job panel draws it.
#[derive(Debug, Clone, PartialEq)]
pub struct Evaluation {
    pub at_ms: i64,
    /// The count when evaluated.
    pub current: u32,
    /// What the evaluation wanted.
    pub want: u32,
    /// Each check's reading, in [`CheckInfo`] order (`None`: no data).
    pub readings: Vec<Option<f64>>,
}

#[derive(Debug, Default)]
struct TargetLog {
    checks: Vec<CheckInfo>,
    points: VecDeque<Evaluation>,
}

/// The app's memory of its own evaluations: the last hour of each target's,
/// kept by the engine and drawn by the job panel (plan §6.6: "evaluations
/// that held are live, not stored"). A restart starts it empty.
#[derive(Debug, Default)]
pub struct EvaluationLog {
    targets: RwLock<HashMap<TargetKey, TargetLog>>,
}

impl EvaluationLog {
    /// Record one evaluation of `job/group/task` at `now_ms`.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        job: &str,
        group: &str,
        task: &str,
        policy: &EffectivePolicy,
        decision: &Decision,
        current: u32,
        want: u32,
        now_ms: i64,
    ) {
        let checks: Vec<CheckInfo> = policy
            .all_checks()
            .into_iter()
            .map(|check| CheckInfo {
                target: match &check.strategy {
                    StrategyConfig::TargetValue(value) => Some(value.target),
                    _ => None,
                },
                name: check.name,
                source: check.source,
                query: check.query,
            })
            .collect();
        let readings = checks
            .iter()
            .map(|check| {
                decision
                    .checks
                    .iter()
                    .find(|outcome| outcome.name == check.name)
                    .and_then(|outcome| outcome.reading)
            })
            .collect();
        let Ok(mut targets) = self.targets.write() else {
            return;
        };
        let log = targets
            .entry((job.to_string(), group.to_string(), task.to_string()))
            .or_default();
        if log.checks != checks {
            // A policy change that changed the checks: the old readings
            // measured something else.
            log.points.clear();
            log.checks = checks;
        }
        log.points.push_back(Evaluation {
            at_ms: now_ms,
            current,
            want,
            readings,
        });
        let since = now_ms - LOG_WINDOW.as_millis() as i64;
        while log
            .points
            .front()
            .is_some_and(|point| point.at_ms < since || log.points.len() > LOG_MAX_POINTS)
        {
            log.points.pop_front();
        }
    }

    /// Forget targets whose policy is gone.
    pub fn retain(&self, live: &BTreeSet<TargetKey>) {
        if let Ok(mut targets) = self.targets.write() {
            targets.retain(|key, _| live.contains(key));
        }
    }

    /// The checks and evaluations of `job/group/task`, oldest first.
    pub fn of(&self, job: &str, group: &str, task: &str) -> (Vec<CheckInfo>, Vec<Evaluation>) {
        self.targets
            .read()
            .ok()
            .and_then(|targets| {
                targets
                    .get(&(job.to_string(), group.to_string(), task.to_string()))
                    .map(|log| (log.checks.clone(), log.points.iter().cloned().collect()))
            })
            .unwrap_or_default()
    }
}

/// What the views share: the app's client (once it has one), its
/// evaluation log, when it started, and recent capacity demand.
pub struct AutoscalerUi {
    connected: OnceLock<(AuthedManagerClient, Arc<EvaluationLog>)>,
    started_ms: i64,
    demand: tokio::sync::Mutex<HashMap<String, (Instant, Demand)>>,
}

/// One node group's capacity demand, as the node panel shows it.
#[derive(Debug, Clone, Default)]
struct Demand {
    /// Nodes like this one the pending work this group can host needs.
    nodes: u32,
    /// Replicas of that work.
    replicas: u32,
    /// Replicas pending cluster-wide.
    pending: u32,
    /// Replicas no node like this one can host.
    unfittable: u32,
}

impl AutoscalerUi {
    /// The views' shared state, for an app started at `started_ms`. Views
    /// answer "starting" until [`AutoscalerUi::connect`].
    pub fn new(started_ms: i64) -> Arc<AutoscalerUi> {
        Arc::new(AutoscalerUi {
            connected: OnceLock::new(),
            started_ms,
            demand: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Hand the views the app's client and the engine's evaluation log.
    pub fn connect(&self, client: AuthedManagerClient, log: Arc<EvaluationLog>) {
        let _ = self.connected.set((client, log));
    }

    fn parts(&self) -> R<(AuthedManagerClient, Arc<EvaluationLog>)> {
        self.connected.get().cloned().ok_or_else(|| {
            anyhow!(
                "the autoscaler is starting and cannot reach Legion yet (it needs its API \
                 socket: workload identity on the managers)"
            )
        })
    }
}

/// Register the autoscaler's views and actions on `registry`.
pub fn register(registry: &UiRegistry, ui: Arc<AutoscalerUi>) {
    let shared = ui.clone();
    registry.view("job", move |request: ViewRequest| {
        let ui = shared.clone();
        async move { job_view(&ui, &request.context).await }
    });
    let shared = ui.clone();
    registry.view("task-badge", move |request: ViewRequest| {
        let ui = shared.clone();
        async move { task_badge(&ui, &request.context).await }
    });
    let shared = ui.clone();
    registry.view("policies", move |request: ViewRequest| {
        let ui = shared.clone();
        async move { policies_view(&ui, &request.viewer).await }
    });
    let shared = ui.clone();
    registry.view("node", move |request: ViewRequest| {
        let ui = shared.clone();
        async move { node_view(&ui, &request.context).await }
    });
    for action in ["pause", "resume", "release"] {
        let shared = ui.clone();
        registry.action(action, move |request: ActionRequest| {
            let ui = shared.clone();
            async move { run_action(&ui, request).await }
        });
    }
}

// ── Reading ─────────────────────────────────────────────────────────────

/// The status of `job`'s autoscaled tasks, or `None` when it has none the
/// app may read (no such job, not autoscaled, or outside the app's grant,
/// as `legion-system` is).
async fn job_status(
    client: &mut AuthedManagerClient,
    job: &str,
) -> R<Option<GetScalingStatusReply>> {
    match client
        .get_scaling_status(GetScalingStatusRequest {
            job: job.to_string(),
            ..Default::default()
        })
        .await
    {
        Ok(reply) => {
            let reply = reply.into_inner();
            Ok((!reply.targets.is_empty()).then_some(reply))
        }
        Err(status)
            if matches!(
                status.code(),
                tonic::Code::NotFound | tonic::Code::PermissionDenied
            ) =>
        {
            Ok(None)
        }
        Err(status) => bail!("reading the scaling status of {job}: {}", status.message()),
    }
}

/// `2..8`.
fn bounds(target: &ScalingTargetStatus) -> String {
    format!("{}..{}", target.min, target.max)
}

fn state_tone(state: &str) -> Tone {
    match state {
        "ok" | "scaled" => Tone::Ok,
        "dry-run" | "held" | "deferred" | "starting" => Tone::Info,
        "limited" | "pinned" | "paused" | "missing-data" | "stale" => Tone::Warn,
        "invalid" | "error" => Tone::Danger,
        _ => Tone::Neutral,
    }
}

/// `14:05 UTC`, or `until resumed`.
fn until(until_ms: Option<i64>) -> String {
    match until_ms {
        Some(at) if at > 0 => format!("until {}", crate::cmd::scaling::clock(at)),
        _ => "until resumed".to_string(),
    }
}

/// How a check's reading is shown: a factor and a unit.
fn signal_scale(check: &CheckInfo) -> (f64, Option<&'static str>) {
    if check.source != crate::manager::autoscale::LEGION_SOURCE {
        return (1.0, None);
    }
    match check.query.as_str() {
        query if query.ends_with(".utilization") => (100.0, Some("%")),
        "service.rps" => (1.0, Some("req/s")),
        "service.errors" => (1.0, Some("err/s")),
        "service.latency_ms.mean" | "service.latency_ms.max" => (1.0, Some("ms")),
        _ => (1.0, None),
    }
}

/// `value` as one path segment of an in-app route.
fn segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn seconds(at_ms: i64) -> f64 {
    at_ms as f64 / 1000.0
}

fn target_label(target: &ScalingTargetStatus) -> String {
    if target.task_group == target.task_name {
        target.task_name.clone()
    } else {
        format!("{}/{}", target.task_group, target.task_name)
    }
}

/// The hour of one target's evaluations as a series: replicas on the left,
/// its first check's reading (and target) on the right.
fn series_block(
    target: &ScalingTargetStatus,
    log: &EvaluationLog,
    many: bool,
    started_ms: i64,
) -> Block {
    let (checks, points) = log.of(&target.job_id, &target.task_group, &target.task_name);
    if points.is_empty() {
        return Block::Notice(NoticeBlock {
            tone: Tone::Info,
            title: None,
            text: format!(
                "No evaluations of {} yet: the autoscaler keeps the last hour of them in memory, \
                 and has made none since it started at {}.",
                target_label(target),
                crate::cmd::scaling::clock(started_ms)
            ),
        });
    }
    let mut lines = vec![Series {
        name: "replicas".to_string(),
        axis: Default::default(),
        unit: None,
        points: points
            .iter()
            .map(|point| (seconds(point.at_ms), f64::from(point.current)))
            .collect(),
    }];
    let mut title = "Replicas (1h)".to_string();
    if let Some(check) = checks.first() {
        let (factor, unit) = signal_scale(check);
        let readings: Vec<(f64, f64)> = points
            .iter()
            .filter_map(|point| {
                point
                    .readings
                    .first()
                    .copied()
                    .flatten()
                    .map(|value| (seconds(point.at_ms), value * factor))
            })
            .collect();
        title = format!("Replicas vs {} (1h)", check.name);
        lines.push(Series {
            name: check.name.clone(),
            axis: crate::structs::app_ui::Axis::Right,
            unit: unit.map(str::to_string),
            points: readings,
        });
        if let (Some(goal), Some(first), Some(last)) = (check.target, points.first(), points.last())
        {
            lines.push(Series {
                name: format!("{} target", check.name),
                axis: crate::structs::app_ui::Axis::Right,
                unit: unit.map(str::to_string),
                points: vec![
                    (seconds(first.at_ms), goal * factor),
                    (seconds(last.at_ms), goal * factor),
                ],
            });
        }
    }
    if many {
        title = format!("{}: {title}", target_label(target));
    }
    Block::Series(SeriesBlock {
        title: Some(title),
        series: lines,
    })
}

fn history_item(entry: &ScaleHistoryEntry, many: bool) -> TimelineItem {
    let change = format!("{} → {}", entry.from, entry.to);
    let (title, tone) = match entry.kind.as_str() {
        "would" => (format!("would {change}"), None),
        "deferred" => (format!("{change} deferred"), Some(Tone::Warn)),
        "pinned" => (format!("pinned at {}", entry.to), Some(Tone::Warn)),
        "released" => ("pin released".to_string(), None),
        "reset" => (format!("reset to the spec ({})", entry.to), None),
        _ => (change, Some(Tone::Info)),
    };
    let by = if entry.source == "autoscaler" {
        "autoscaler".to_string()
    } else if entry.principal.is_empty() {
        entry.source.clone()
    } else {
        entry.principal.clone()
    };
    let mut detail = if entry.summary.is_empty() {
        format!("by {by}")
    } else {
        format!("{} (by {by})", entry.summary)
    };
    if many {
        detail = format!("{}/{}: {detail}", entry.task_group, entry.task_name);
    }
    TimelineItem {
        at: entry.at_unix_ms / 1000,
        title,
        detail: Some(detail),
        tone,
    }
}

/// The Autoscaling panel on a job's page.
async fn job_view(ui: &AutoscalerUi, context: &ViewContext) -> R<AppView> {
    let Some(job) = context.job_id() else {
        return Ok(AppView::hidden());
    };
    let (mut client, log) = ui.parts()?;
    let Some(status) = job_status(&mut client, &job).await? else {
        return Ok(AppView::hidden());
    };
    let history = client
        .get_scale_history(GetScaleHistoryRequest {
            job: job.clone(),
            limit: TIMELINE_ROWS,
            ..Default::default()
        })
        .await
        .map(|reply| reply.into_inner().entries)
        .unwrap_or_default();
    Ok(render_job(&job, &status, &history, &log, ui.started_ms))
}

/// The job panel's document, from what the app read.
fn render_job(
    job: &str,
    status: &GetScalingStatusReply,
    history: &[ScaleHistoryEntry],
    log: &EvaluationLog,
    started_ms: i64,
) -> AppView {
    let many = status.targets.len() > 1;
    let mut blocks = Vec::new();
    if let Some(app) = &status.app {
        if app.state != "running" {
            blocks.push(Block::Notice(NoticeBlock {
                tone: if app.state == "starting" {
                    Tone::Info
                } else {
                    Tone::Danger
                },
                title: Some("Autoscaler".to_string()),
                text: app.message.clone(),
            }));
        }
        if let Some(all) = &app.paused_all {
            blocks.push(Block::Notice(NoticeBlock {
                tone: Tone::Warn,
                title: Some("Paused cluster-wide".to_string()),
                text: format!(
                    "by {} {}{}. `legion scaling resume --all` resumes it.",
                    all.by,
                    until(all.until_unix_ms),
                    if all.reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", all.reason)
                    }
                ),
            }));
        }
    }
    let mut paused = false;
    let mut job_pause = false;
    let mut pinned = false;
    for target in status.targets.iter().take(8) {
        let label = target_label(target);
        if !target.error.is_empty() {
            blocks.push(Block::Notice(NoticeBlock {
                tone: Tone::Danger,
                title: Some(format!("{label}: the policy does not resolve")),
                text: target.error.clone(),
            }));
        }
        blocks.push(Block::Stats(StatsBlock {
            title: many.then(|| label.clone()),
            items: vec![
                Stat {
                    label: "Now".to_string(),
                    value: target.now.to_string(),
                    tone: None,
                    hint: None,
                },
                Stat {
                    label: "Want".to_string(),
                    value: target.want.to_string(),
                    tone: (target.want != target.now).then_some(Tone::Info),
                    hint: None,
                },
                Stat {
                    label: "Bounds".to_string(),
                    value: bounds(target),
                    tone: None,
                    hint: (!target.policy_summary.is_empty())
                        .then(|| target.policy_summary.clone()),
                },
                Stat {
                    label: "State".to_string(),
                    value: target.state.clone(),
                    tone: Some(state_tone(&target.state)),
                    hint: (!target.summary.is_empty()).then(|| target.summary.clone()),
                },
                Stat {
                    label: "Mode".to_string(),
                    value: target.mode.clone(),
                    tone: (target.mode == "dry-run").then_some(Tone::Info),
                    hint: None,
                },
            ],
        }));
        if let Some(pause) = &target.pause {
            paused = true;
            job_pause |= pause.scope != "all";
            if pause.scope != "all" {
                blocks.push(Block::Notice(NoticeBlock {
                    tone: Tone::Warn,
                    title: Some(format!("{label}: paused")),
                    text: format!(
                        "by {} {}{}",
                        pause.by,
                        until(pause.until_unix_ms),
                        if pause.reason.is_empty() {
                            String::new()
                        } else {
                            format!(": {}", pause.reason)
                        }
                    ),
                }));
            }
        }
        if let Some(pin) = &target.pin {
            pinned = true;
            let what = match (pin.min, pin.max) {
                (Some(min), Some(max)) if min == max => format!("at {min}"),
                (Some(min), Some(max)) => format!("between {min} and {max}"),
                (Some(min), None) => format!("at {min} or more"),
                (None, Some(max)) => format!("at {max} or fewer"),
                (None, None) => format!("at {}", target.now),
            };
            blocks.push(Block::Notice(NoticeBlock {
                tone: Tone::Warn,
                title: Some(format!("{label}: pinned")),
                text: format!(
                    "{what} by {} {}{}",
                    pin.by,
                    until(Some(pin.until_unix_ms)),
                    if pin.reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", pin.reason)
                    }
                ),
            }));
        }
        blocks.push(series_block(target, log, many, started_ms));
    }
    if status.targets.len() > 8 {
        blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Info,
            title: None,
            text: format!(
                "{} more autoscaled tasks: `legion scaling status --job {job}`",
                status.targets.len() - 8
            ),
        }));
    }
    blocks.push(Block::Timeline(TimelineBlock {
        title: Some("Decisions".to_string()),
        items: history
            .iter()
            .map(|entry| history_item(entry, many))
            .collect(),
    }));
    let mut buttons = Vec::new();
    if !paused {
        buttons.push(ActionButton {
            action: "pause".to_string(),
            label: Some("Pause…".to_string()),
            inputs: Map::new(),
        });
    }
    if job_pause {
        buttons.push(ActionButton {
            action: "resume".to_string(),
            label: Some("Resume".to_string()),
            inputs: Map::new(),
        });
    }
    if pinned {
        buttons.push(ActionButton {
            action: "release".to_string(),
            label: Some("Release the pin".to_string()),
            inputs: Map::new(),
        });
    }
    if !buttons.is_empty() {
        blocks.push(Block::Actions(ActionsBlock { items: buttons }));
    }
    let mut view = AppView::new(blocks);
    view.title = Some("Autoscaling".to_string());
    view
}

/// The badge on an autoscaled task's row: `auto 6 · 2..8`.
async fn task_badge(ui: &AutoscalerUi, context: &ViewContext) -> R<AppView> {
    let Some(job) = context.job_id() else {
        return Ok(AppView::hidden());
    };
    let (mut client, _) = ui.parts()?;
    let Some(status) = job_status(&mut client, &job).await? else {
        return Ok(AppView::hidden());
    };
    let target = status.targets.iter().find(|target| {
        (context.group.is_empty() || target.task_group == context.group)
            && (target.task_name == context.task || target.members.contains(&context.task))
    });
    Ok(match target {
        Some(target) => {
            let mut view = AppView::new(Vec::new());
            view.badge = Some(badge(target));
            view
        }
        None => AppView::hidden(),
    })
}

fn badge(target: &ScalingTargetStatus) -> Badge {
    let bounds = bounds(target);
    let now = if target.want != target.now && target.mode != "dry-run" {
        format!("{}→{}", target.now, target.want)
    } else {
        target.now.to_string()
    };
    let (text, tone) = match target.state.as_str() {
        "invalid" => ("autoscale invalid".to_string(), Tone::Danger),
        _ if target.pause.is_some() => (format!("paused {now} · {bounds}"), Tone::Warn),
        _ if target.pin.is_some() => (format!("pinned {now} · {bounds}"), Tone::Warn),
        _ if target.mode == "dry-run" => {
            let would = if target.want != target.now {
                format!(" (would {})", target.want)
            } else {
                String::new()
            };
            (format!("dry-run {now}{would} · {bounds}"), Tone::Neutral)
        }
        "stale" | "error" | "missing-data" => (format!("auto {now} · {bounds}"), Tone::Warn),
        _ => (format!("auto {now} · {bounds}"), Tone::Info),
    };
    Badge {
        text,
        tone: Some(tone),
    }
}

/// The sidebar's Autoscaling page: every policy the viewer may read.
async fn policies_view(ui: &AutoscalerUi, viewer: &Viewer) -> R<AppView> {
    let (mut client, _) = ui.parts()?;
    let status = client
        .get_scaling_status(GetScalingStatusRequest {
            namespaces: vec!["*".to_string()],
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow!("reading the scaling status: {}", status.message()))?
        .into_inner();
    Ok(render_policies(&status, viewer))
}

/// The policies page's document. Rows are filtered by the namespaces the
/// viewer holds `scale:read` in (the manager checked it on any scope, and
/// the assertion says where).
fn render_policies(status: &GetScalingStatusReply, viewer: &Viewer) -> AppView {
    let visible: Vec<&ScalingTargetStatus> = status
        .targets
        .iter()
        .filter(|target| {
            viewer.sees_namespace(crate::structs::namespace::job_namespace(&target.job_id))
        })
        .collect();
    let mut blocks = Vec::new();
    if let Some(app) = &status.app {
        blocks.push(Block::Notice(NoticeBlock {
            tone: match app.state.as_str() {
                "running" => Tone::Ok,
                "starting" => Tone::Info,
                _ => Tone::Danger,
            },
            title: Some("Autoscaler".to_string()),
            text: app.message.clone(),
        }));
        if let Some(all) = &app.paused_all {
            blocks.push(Block::Notice(NoticeBlock {
                tone: Tone::Warn,
                title: Some("Paused cluster-wide".to_string()),
                text: format!(
                    "by {} {}{}",
                    all.by,
                    until(all.until_unix_ms),
                    if all.reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", all.reason)
                    }
                ),
            }));
        }
    }
    let count = |predicate: &dyn Fn(&ScalingTargetStatus) -> bool| {
        visible.iter().filter(|target| predicate(target)).count()
    };
    let attention = count(&|target| {
        matches!(
            target.state.as_str(),
            "invalid" | "error" | "stale" | "missing-data"
        )
    });
    blocks.push(Block::Stats(StatsBlock {
        title: None,
        items: vec![
            Stat {
                label: "Policies".to_string(),
                value: visible.len().to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Want a change".to_string(),
                value: count(&|target| target.want != target.now).to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Paused".to_string(),
                value: count(&|target| target.pause.is_some()).to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Pinned".to_string(),
                value: count(&|target| target.pin.is_some()).to_string(),
                tone: None,
                hint: None,
            },
            Stat {
                label: "Need attention".to_string(),
                value: attention.to_string(),
                tone: (attention > 0).then_some(Tone::Warn),
                hint: Some("invalid, error, stale, or missing data".to_string()),
            },
        ],
    }));
    let column = |key: &str, label: &str, kind: ColumnType| Column {
        key: key.to_string(),
        label: label.to_string(),
        kind,
    };
    let rows = visible
        .iter()
        .take(crate::structs::app_ui::MAX_ROWS)
        .map(|target| {
            let (namespace, name) = crate::structs::namespace::split_job_id(&target.job_id);
            let mut row = std::collections::BTreeMap::new();
            row.insert(
                "target".to_string(),
                Cell::Rich(RichText {
                    text: format!(
                        "{}/{}/{}",
                        target.job_id, target.task_group, target.task_name
                    ),
                    route: Some(format!("/jobs/{}/{}", segment(namespace), segment(name))),
                    href: None,
                    tone: None,
                }),
            );
            row.insert("mode".to_string(), Cell::Text(target.mode.clone()));
            row.insert("now".to_string(), Cell::Number(f64::from(target.now)));
            row.insert("want".to_string(), Cell::Number(f64::from(target.want)));
            row.insert("bounds".to_string(), Cell::Text(bounds(target)));
            row.insert(
                "state".to_string(),
                Cell::Rich(RichText {
                    text: target.state.clone(),
                    route: None,
                    href: None,
                    tone: Some(state_tone(&target.state)),
                }),
            );
            if let Some(change) = &target.last_change {
                if change.at_unix_ms > 0 {
                    row.insert(
                        "last".to_string(),
                        Cell::Number((change.at_unix_ms / 1000) as f64),
                    );
                }
            }
            row.insert(
                "why".to_string(),
                Cell::Text(if target.error.is_empty() {
                    target.summary.clone()
                } else {
                    target.error.clone()
                }),
            );
            row
        })
        .collect();
    blocks.push(Block::Table(TableBlock {
        title: Some("Policies".to_string()),
        columns: vec![
            column("target", "Target", ColumnType::Text),
            column("mode", "Mode", ColumnType::Text),
            column("now", "Now", ColumnType::Number),
            column("want", "Want", ColumnType::Number),
            column("bounds", "Bounds", ColumnType::Text),
            column("state", "State", ColumnType::Text),
            column("last", "Last change", ColumnType::Time),
            column("why", "Why", ColumnType::Text),
        ],
        rows,
        empty: Some(
            "No autoscaled tasks you may see. Put `autoscale = true` on a task to start."
                .to_string(),
        ),
    }));
    let mut view = AppView::new(blocks);
    view.title = Some("Autoscaling".to_string());
    view
}

/// A node's config attributes.
fn attributes(node: &NodeMsg) -> Value {
    serde_json::from_str(&node.config_attrs).unwrap_or(Value::Null)
}

fn node_group(node: &NodeMsg) -> Option<String> {
    attributes(node)
        .get(NODEGROUP_ATTR)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Every node in `pool` (the app reads at most 2,000).
async fn nodes_in_pool(client: &mut AuthedManagerClient, pool: &str) -> R<Vec<NodeMsg>> {
    let mut nodes = Vec::new();
    for page in 1..=20u32 {
        let reply = client
            .get_nodes(GetNodesRequest {
                query: Some(pool.to_string()),
                page: Some(page),
                page_size: Some(100),
                ..Default::default()
            })
            .await
            .map_err(|status| anyhow!("listing nodes: {}", status.message()))?
            .into_inner();
        let total = reply.total_count as usize;
        let fetched = reply.nodes.len();
        nodes.extend(reply.nodes.into_iter().filter(|node| node.pool == pool));
        if fetched == 0 || (page as usize) * 100 >= total {
            break;
        }
    }
    Ok(nodes)
}

/// The capacity demand for nodes like `node`, reused for [`DEMAND_TTL`].
async fn demand(
    ui: &AutoscalerUi,
    client: &mut AuthedManagerClient,
    group: &str,
    node: &str,
) -> R<Demand> {
    let key = format!("{group}\u{0}{node}");
    if let Some((at, demand)) = ui.demand.lock().await.get(&key) {
        if at.elapsed() < DEMAND_TTL {
            return Ok(demand.clone());
        }
    }
    let reply = client
        .get_capacity_demand(GetCapacityDemandRequest {
            templates: vec![NodeGroupTemplate {
                name: group.to_string(),
                like_node: Some(node.to_string()),
                ..Default::default()
            }],
        })
        .await
        .map_err(|status| anyhow!("reading capacity demand: {}", status.message()))?
        .into_inner();
    let group_demand = reply.groups.first();
    let demand = Demand {
        nodes: group_demand.map_or(0, |demand| demand.nodes),
        replicas: group_demand.map_or(0, |demand| demand.replicas),
        pending: reply.pending.iter().map(|pending| pending.pending).sum(),
        unfittable: reply
            .unfittable
            .iter()
            .map(|unfittable| unfittable.replicas)
            .sum(),
    };
    let mut cache = ui.demand.lock().await;
    cache.retain(|_, (at, _)| at.elapsed() < DEMAND_TTL);
    cache.insert(key, (Instant::now(), demand.clone()));
    Ok(demand)
}

/// The node-group panel on a node's page.
async fn node_view(ui: &AutoscalerUi, context: &ViewContext) -> R<AppView> {
    let (mut client, _) = ui.parts()?;
    let node = match client
        .get_node(GetNodeRequest {
            id: context.node.clone(),
        })
        .await
    {
        Ok(reply) => reply.into_inner().node,
        Err(status) if status.code() == tonic::Code::NotFound => None,
        Err(status) => bail!("reading node {}: {}", context.node, status.message()),
    };
    let Some(node) = node else {
        return Ok(AppView::hidden());
    };
    let Some(group) = node_group(&node) else {
        return Ok(AppView::hidden());
    };
    let members: Vec<NodeMsg> = nodes_in_pool(&mut client, &node.pool)
        .await?
        .into_iter()
        .filter(|member| node_group(member).as_deref() == Some(group.as_str()))
        .collect();
    let demand = demand(ui, &mut client, &group, &node.id).await;
    // The group's policy, as the autoscaler last reported it.
    let policy = match client
        .get_scaling_status(GetScalingStatusRequest {
            policy: group.clone(),
            detail: true,
            ..Default::default()
        })
        .await
    {
        Ok(reply) => reply
            .into_inner()
            .targets
            .into_iter()
            .find(|target| target.kind == "nodes"),
        Err(status) => {
            if status.code() != tonic::Code::NotFound {
                debug!(group = %group, "Reading the node group's status: {}", status.message());
            }
            None
        }
    };
    Ok(render_node(&node, &group, &members, demand, policy.as_ref()))
}

/// The node group's policy state, as blocks.
fn policy_blocks(group: &str, policy: &ScalingTargetStatus) -> Vec<Block> {
    let mut items = vec![
        KeyValueItem {
            key: "Policy".to_string(),
            value: format!("{} ({})", policy.policy_summary, policy.mode),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "Nodes".to_string(),
            value: format!(
                "{} held, {} wanted, bounds {}..{}",
                policy.now, policy.want, policy.min, policy.max
            ),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "State".to_string(),
            value: policy.state.clone(),
            tone: Some(state_tone(&policy.state)),
            route: None,
        },
    ];
    if !policy.summary.is_empty() {
        items.push(KeyValueItem {
            key: "Last evaluation".to_string(),
            value: policy.summary.clone(),
            tone: None,
            route: None,
        });
    }
    let mut blocks = vec![Block::KeyValue(KeyValueBlock {
        title: Some(format!("Autoscaling {group}")),
        items,
    })];
    if let Some(pause) = &policy.pause {
        blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Warn,
            title: Some("Paused".to_string()),
            text: format!(
                "paused by {}{}: no node is added and no drain starts",
                pause.by,
                until(pause.until_unix_ms)
            ),
        }));
    }
    if let Ok(decision) =
        serde_json::from_str::<crate::autoscaler::nodes::NodeDecision>(&policy.decision_json)
    {
        let mut lines: Vec<String> = decision
            .victims
            .iter()
            .map(|victim| format!("draining {} ({})", victim.node, victim.reason))
            .collect();
        lines.extend(
            decision
                .protected
                .iter()
                .map(|p| format!("{} is never a victim: {}", p.node, p.protection)),
        );
        if !lines.is_empty() {
            blocks.push(Block::Notice(NoticeBlock {
                tone: Tone::Info,
                title: Some("Scale-in".to_string()),
                text: lines.join("; "),
            }));
        }
    }
    blocks
}

fn render_node(
    node: &NodeMsg,
    group: &str,
    members: &[NodeMsg],
    demand: R<Demand>,
    policy: Option<&ScalingTargetStatus>,
) -> AppView {
    let attrs = attributes(node);
    let text = |key: &str| attrs.get(key).and_then(Value::as_str).map(str::to_string);
    let token = attrs
        .get(JOIN_ATTR)
        .and_then(|join| join.get("token"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut items = vec![
        KeyValueItem {
            key: "Group".to_string(),
            value: group.to_string(),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "Pool".to_string(),
            value: node.pool.clone(),
            tone: None,
            route: None,
        },
        KeyValueItem {
            key: "Joined with token".to_string(),
            value: token.unwrap_or_else(|| "-".to_string()),
            tone: None,
            route: None,
        },
    ];
    if let Some(provider) = text(PROVIDER_ID_ATTR) {
        items.push(KeyValueItem {
            key: "Provider id".to_string(),
            value: provider,
            tone: None,
            route: None,
        });
    }
    let protected = text(SCALE_IN_ATTR).as_deref() == Some("never");
    items.push(KeyValueItem {
        key: "Scale-in".to_string(),
        value: if protected {
            format!("never ({SCALE_IN_ATTR})")
        } else {
            "allowed".to_string()
        },
        tone: protected.then_some(Tone::Info),
        route: None,
    });
    let ready = members
        .iter()
        .filter(|member| {
            matches!(member.status.as_str(), "ready" | "active")
                && member.availability == "available"
        })
        .count();
    let mut stats = vec![
        Stat {
            label: "Members".to_string(),
            value: members.len().to_string(),
            tone: None,
            hint: Some(format!("in pool {}", node.pool)),
        },
        Stat {
            label: "Ready".to_string(),
            value: ready.to_string(),
            tone: (ready < members.len()).then_some(Tone::Warn),
            hint: None,
        },
    ];
    let mut blocks = Vec::new();
    match &demand {
        Ok(demand) => {
            stats.push(Stat {
                label: "Pending replicas".to_string(),
                value: demand.pending.to_string(),
                tone: None,
                hint: Some("cluster-wide, waiting for room".to_string()),
            });
            stats.push(Stat {
                label: "Nodes like this needed".to_string(),
                value: demand.nodes.to_string(),
                tone: (demand.nodes > 0).then_some(Tone::Info),
                hint: Some(format!(
                    "for {} replica(s); {} fit no node like this",
                    demand.replicas, demand.unfittable
                )),
            });
        }
        Err(err) => blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Warn,
            title: Some("Capacity demand".to_string()),
            text: format!("{err:#}"),
        })),
    }
    blocks.insert(0, Block::KeyValue(KeyValueBlock { title: None, items }));
    blocks.insert(
        1,
        Block::Stats(StatsBlock {
            title: None,
            items: stats,
        }),
    );
    let rows = members
        .iter()
        .take(MEMBER_ROWS)
        .map(|member| {
            let mut row = std::collections::BTreeMap::new();
            row.insert(
                "node".to_string(),
                Cell::Rich(RichText {
                    text: member.id.clone(),
                    route: Some(format!("/nodes/{}", segment(&member.id))),
                    href: None,
                    tone: None,
                }),
            );
            row.insert("status".to_string(), Cell::Text(member.status.clone()));
            row.insert(
                "availability".to_string(),
                Cell::Text(member.availability.clone()),
            );
            row
        })
        .collect();
    blocks.push(Block::Table(TableBlock {
        title: Some(format!("Members of {group}")),
        columns: vec![
            Column {
                key: "node".to_string(),
                label: "Node".to_string(),
                kind: ColumnType::Text,
            },
            Column {
                key: "status".to_string(),
                label: "Status".to_string(),
                kind: ColumnType::Text,
            },
            Column {
                key: "availability".to_string(),
                label: "Availability".to_string(),
                kind: ColumnType::Text,
            },
        ],
        rows,
        empty: None,
    }));
    match policy {
        Some(policy) => blocks.splice(1..1, policy_blocks(group, policy)).for_each(drop),
        None => blocks.push(Block::Notice(NoticeBlock {
            tone: Tone::Info,
            title: None,
            text: format!(
                "No node-group policy runs {group}: nothing adds or removes its nodes on its \
                 own. `legion scaling apply` a policy named {group} to scale it. The demand \
                 above is what `legion scaling demand --like-node {}` reports.",
                node.id
            ),
        })),
    }
    let mut view = AppView::new(blocks);
    view.title = Some(format!("Node group {group}"));
    view
}

// ── Acting ──────────────────────────────────────────────────────────────

/// A refusal the viewer sees for a status the RPC answered with.
fn refused_or_failed(what: &str, status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::FailedPrecondition
        | tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::PermissionDenied => {
            anyhow::Error::new(Refused(format!("{what}: {}", status.message())))
        }
        _ => anyhow!("{what}: {}", status.message()),
    }
}

/// Pause, resume, or release the job in the action's context, and answer
/// with the refreshed panel.
async fn run_action(ui: &AutoscalerUi, request: ActionRequest) -> R<ActionOutcome> {
    let Some(job) = request.context.job_id() else {
        return Err(Refused("autoscaling actions are pressed on a job".into()).into());
    };
    let (mut client, log) = ui.parts()?;
    let Some(status) = job_status(&mut client, &job).await? else {
        return Err(Refused(format!(
            "{job} has no autoscaled task: there is nothing to {}",
            request.action
        ))
        .into());
    };
    let by = &request.viewer.principal;
    let message = match request.action.as_str() {
        "pause" => {
            let for_seconds = match request.inputs.get("for").and_then(Value::as_str) {
                Some(text) if !text.trim().is_empty() => Some(
                    humantime::parse_duration(text.trim())
                        .map_err(|err| Refused(format!("for {text:?}: {err}")))?
                        .as_secs(),
                ),
                _ => None,
            };
            client
                .pause_scaling(PauseScalingRequest {
                    job: job.clone(),
                    for_seconds,
                    reason: format!("paused from the UI by {by}"),
                    ..Default::default()
                })
                .await
                .map_err(|status| refused_or_failed("pausing", status))?
                .into_inner()
                .message
        }
        "resume" => {
            if !status
                .targets
                .iter()
                .any(|target| target.pause.as_ref().is_some_and(|p| p.scope != "all"))
            {
                return Err(Refused(format!(
                    "{job} is not paused on its own (a cluster-wide pause is resumed with \
                     `legion scaling resume --all`)"
                ))
                .into());
            }
            client
                .pause_scaling(PauseScalingRequest {
                    job: job.clone(),
                    resume: true,
                    reason: format!("resumed from the UI by {by}"),
                    ..Default::default()
                })
                .await
                .map_err(|status| refused_or_failed("resuming", status))?
                .into_inner()
                .message
        }
        "release" => {
            let pinned: Vec<&ScalingTargetStatus> = status
                .targets
                .iter()
                .filter(|target| target.pin.is_some())
                .collect();
            if pinned.is_empty() {
                return Err(
                    Refused(format!("no task of {job} is pinned: nothing to release")).into(),
                );
            }
            let mut said = Vec::new();
            for target in pinned {
                let reply = client
                    .scale_task(ScaleTaskRequest {
                        name: job.clone(),
                        task_group: Some(target.task_group.clone()),
                        task_name: Some(target.task_name.clone()),
                        release: true,
                        reason: Some(format!("released from the UI by {by}")),
                        ..Default::default()
                    })
                    .await
                    .map_err(|status| refused_or_failed("releasing the pin", status))?;
                said.push(reply.into_inner().message);
            }
            said.join("; ")
        }
        other => return Err(Refused(format!("the autoscaler has no action {other:?}")).into()),
    };
    let refreshed = match job_status(&mut client, &job).await? {
        Some(status) => {
            let history = client
                .get_scale_history(GetScaleHistoryRequest {
                    job: job.clone(),
                    limit: TIMELINE_ROWS,
                    ..Default::default()
                })
                .await
                .map(|reply| reply.into_inner().entries)
                .unwrap_or_default();
            render_job(&job, &status, &history, &log, ui.started_ms)
        }
        None => AppView::hidden(),
    };
    Ok(ActionOutcome::done(message).with_view(refreshed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::app_ui::{validate_view_value, AssertionScope, Slot};
    use crate::structs::{ScalePin, ScalingAppStatus, ScalingPauseInfo};

    fn target(job: &str, now: u32, want: u32) -> ScalingTargetStatus {
        ScalingTargetStatus {
            job_id: job.into(),
            task_group: "web".into(),
            task_name: "web".into(),
            members: vec!["web".into()],
            mode: "enforce".into(),
            now,
            want,
            min: 2,
            max: 8,
            state: "ok".into(),
            summary: "cpu 0.71 vs 0.70 (6 ready)".into(),
            policy_summary: "2..8, cpu 70%, enforce".into(),
            ..Default::default()
        }
    }

    fn status(targets: Vec<ScalingTargetStatus>) -> GetScalingStatusReply {
        GetScalingStatusReply {
            app: Some(ScalingAppStatus {
                state: "running".into(),
                message: "running (alloc 4), last evaluation 3s ago".into(),
                ..Default::default()
            }),
            targets,
        }
    }

    fn manifest_actions() -> Vec<crate::structs::app_ui::ActionDecl> {
        crate::apps::bundled_manifest("autoscaler")
            .expect("bundled")
            .ui_spec()
            .expect("ui")
            .actions
    }

    /// Every document the views build passes the manager's validation for
    /// its slot, with the manifest's actions.
    fn valid(view: &AppView, slot: Slot) {
        let value = serde_json::to_value(view).expect("serializes");
        validate_view_value(&value, Some(&manifest_actions()), Some(slot))
            .unwrap_or_else(|err| panic!("{err}: {value}"));
    }

    #[test]
    fn the_job_panel_shows_bounds_the_hour_and_the_decisions_test() {
        let log = EvaluationLog::default();
        let policy = EffectivePolicy::from_record(&crate::autoscaler::builtin_defaults(), 2)
            .expect("builtin resolves");
        let decision = crate::autoscaler::evaluate(
            &policy,
            &mut crate::autoscaler::PolicyState::new(0),
            &crate::autoscaler::Observation::new(4).reading("cpu", 0.5),
            1_000,
        );
        for (index, current) in [4u32, 4, 6].iter().enumerate() {
            log.record(
                "web",
                "web",
                "web",
                &policy,
                &decision,
                *current,
                6,
                1_000_000 + index as i64 * 15_000,
            );
        }
        let history = vec![ScaleHistoryEntry {
            job_id: "web".into(),
            task_group: "web".into(),
            task_name: "web".into(),
            at_unix_ms: 1_030_000,
            from: 4,
            to: 6,
            source: "autoscaler".into(),
            principal: "legion-system/autoscaler".into(),
            summary: "cpu 0.91 vs 0.70 (4 ready)".into(),
            applied: true,
            kind: "scaled".into(),
            ..Default::default()
        }];
        let view = render_job("web", &status(vec![target("web", 6, 6)]), &history, &log, 0);
        valid(&view, Slot::Job);
        let value = serde_json::to_value(&view).unwrap();
        assert_eq!(value["title"], "Autoscaling");
        let stats = &value["blocks"][0]["items"];
        assert_eq!(stats[0]["value"], "6");
        assert_eq!(stats[2]["value"], "2..8");
        let series = &value["blocks"][1];
        assert_eq!(series["type"], "series");
        assert_eq!(series["title"], "Replicas vs cpu (1h)");
        assert_eq!(series["series"][0]["points"].as_array().unwrap().len(), 3);
        assert_eq!(series["series"][1]["unit"], "%");
        assert_eq!(series["series"][1]["points"][0][1], 50.0);
        assert_eq!(series["series"][2]["name"], "cpu target");
        assert_eq!(value["blocks"][2]["items"][0]["title"], "4 → 6");
        let actions: Vec<&str> = value["blocks"][3]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|button| button["action"].as_str().unwrap())
            .collect();
        assert_eq!(actions, vec!["pause"]);

        // Paused and pinned: Resume and Release instead of Pause.
        let mut held = target("web", 5, 5);
        held.pause = Some(ScalingPauseInfo {
            scope: "job".into(),
            by: "alice".into(),
            ..Default::default()
        });
        held.pin = Some(ScalePin {
            until_unix_ms: 2_000_000,
            min: Some(5),
            max: Some(5),
            by: "bob".into(),
            reason: "launch".into(),
        });
        let view = render_job(
            "web",
            &status(vec![held]),
            &[],
            &EvaluationLog::default(),
            0,
        );
        valid(&view, Slot::Job);
        let text = serde_json::to_string(&view).unwrap();
        assert!(
            text.contains("\"resume\"") && text.contains("\"release\""),
            "{text}"
        );
        assert!(!text.contains("\"pause\""), "{text}");
        assert!(text.contains("at 5 by bob"), "{text}");
        assert!(text.contains("No evaluations of web yet"), "{text}");
    }

    #[test]
    fn the_badge_reads_auto_now_and_bounds_test() {
        assert_eq!(badge(&target("web", 6, 6)).text, "auto 6 · 2..8");
        assert_eq!(badge(&target("web", 6, 8)).text, "auto 6→8 · 2..8");
        let mut dry = target("web", 4, 7);
        dry.mode = "dry-run".into();
        assert_eq!(badge(&dry).text, "dry-run 4 (would 7) · 2..8");
        let mut paused = target("web", 4, 4);
        paused.pause = Some(ScalingPauseInfo::default());
        assert_eq!(badge(&paused).text, "paused 4 · 2..8");
        assert_eq!(badge(&paused).tone, Some(Tone::Warn));
    }

    /// The policies page shows only the namespaces the viewer holds
    /// `scale:read` in.
    #[test]
    fn the_policies_page_filters_by_the_viewers_namespaces_test() {
        let reply = status(vec![
            target("web", 2, 2),
            target("team-a/api", 4, 6),
            target("payments/db", 3, 3),
        ]);
        let viewer = Viewer {
            principal: "alice".into(),
            accessor: None,
            permission: Some("scale:read".into()),
            scope: AssertionScope::Any,
            namespaces: vec!["team-*".into()],
        };
        let view = render_policies(&reply, &viewer);
        valid(&view, Slot::Nav);
        let value = serde_json::to_value(&view).unwrap();
        let table = value["blocks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|block| block["type"] == "table")
            .unwrap();
        let rows = table["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1, "{table}");
        assert_eq!(rows[0]["target"]["text"], "team-a/api/web/web");
        assert_eq!(rows[0]["target"]["route"], "/jobs/team-a/api");
        let everyone = Viewer {
            namespaces: vec!["*".into()],
            ..viewer
        };
        let value = serde_json::to_value(render_policies(&reply, &everyone)).unwrap();
        assert!(value.to_string().contains("/jobs/default/web"));
    }

    #[test]
    fn the_node_panel_shows_the_group_and_its_members_test() {
        let node = |id: &str, group: Option<&str>| NodeMsg {
            id: id.into(),
            pool: "general".into(),
            status: "ready".into(),
            availability: "available".into(),
            config_attrs: match group {
                Some(group) => json!({ NODEGROUP_ATTR: group, PROVIDER_ID_ATTR: "i-0abc",
                    JOIN_ATTR: { "token": "jt-1", "attributes": { NODEGROUP_ATTR: group } } }),
                None => json!({}),
            }
            .to_string(),
            ..Default::default()
        };
        let one = node("w-1", Some("general"));
        assert_eq!(node_group(&one).as_deref(), Some("general"));
        assert!(node_group(&node("w-3", None)).is_none());
        let demand = || {
            Ok(Demand {
                nodes: 2,
                replicas: 7,
                pending: 9,
                unfittable: 2,
            })
        };
        let members = [one.clone(), node("w-2", Some("general"))];
        let view = render_node(&one, "general", &members, demand(), None);
        valid(&view, Slot::Node);
        let text = serde_json::to_string(&view).unwrap();
        for needle in ["jt-1", "i-0abc", "/nodes/w-2", "No node-group policy runs general"] {
            assert!(text.contains(needle), "{needle}: {text}");
        }

        // With a policy: its state, and why a member is kept.
        let decision = json!({
            "atMs": 1, "from": 3, "to": 3, "kind": "held", "stages": [], "victims": [],
            "protected": [{ "node": "w-1", "protection": { "rule": "system-allocation", "job": "legion-system/autoscaler" } }],
            "intents": [], "flags": [], "summary": "steady at 3",
        });
        let status = ScalingTargetStatus {
            policy_id: "general".into(),
            kind: "nodes".into(),
            mode: "enforce".into(),
            now: 3,
            want: 3,
            min: 1,
            max: 10,
            state: "ok".into(),
            summary: "steady at 3".into(),
            policy_summary: "nodes 1..10 via aws-asg".into(),
            decision_json: decision.to_string(),
            ..Default::default()
        };
        let view = render_node(&one, "general", &members, demand(), Some(&status));
        valid(&view, Slot::Node);
        let text = serde_json::to_string(&view).unwrap();
        for needle in [
            "Autoscaling general",
            "3 held, 3 wanted, bounds 1..10",
            "w-1 is never a victim: runs legion-system job legion-system/autoscaler",
        ] {
            assert!(text.contains(needle), "{needle}: {text}");
        }
        assert!(!text.contains("No node-group policy"), "{text}");
    }
}
