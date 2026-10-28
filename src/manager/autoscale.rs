//! Task autoscaling, the manager's half (docs/autoscaling.md; autoscaler-v2
//! plan §6.3, §7).
//!
//! The engine (`crate::autoscaler`) is pure: it resolves a policy from four
//! records and evaluates it against an observation. This module is what
//! connects it to the cluster:
//!
//! - [`AutoscaleLayers`]: the cluster's scaling defaults and every namespace
//!   policy's `autoscale`, loaded from the store (or a transaction), and the
//!   resolution of one task against them.
//! - [`validate_static`]: the `autoscale` rules that need only the jobspec
//!   (`validate_job_config`), and [`validate_resolved`], the ones that need
//!   the layered defaults (`RunJob`, `PlanJob`).
//! - [`targets_for_config`]: the autoscaling targets of a job, with each
//!   policy resolved (or the reason it does not resolve). What
//!   `WatchScalingPolicies` streams and `GetScalingStatus` reports.
//! - [`describe`] and [`summary_line`]: the words `legion job plan` and
//!   `legion scaling status` print for a policy.
//! - [`in_place_change`]: whether a resubmit differs only in `autoscale`
//!   and the `count` of autoscaled tasks, so `RunJob` can move allocations
//!   to the new version instead of rolling out.

use rusqlite::Connection;

use crate::autoscaler::defaults::{EffectiveDefaults, NamespaceDefaults, ResolvedPolicy};
use crate::autoscaler::spec::format_duration;
use crate::autoscaler::strategies::fmt_num;
use crate::autoscaler::{
    resolve, AutoscaleRecord, CountBound, DefaultsLayers, EffectivePolicy, Mode,
};
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::namespace::job_namespace;
use crate::structs::namespace_policy::NamespacePolicy;
use crate::structs::{InstanceMode, JobConfig, TaskConfig, TaskLifecycle};

/// Runtimes whose replicas report measured usage (docs/resource-enforcement.md,
/// "Measuring usage"). A policy whose signals read the task's own usage needs
/// one of them.
pub const USAGE_RUNTIMES: [&str; 3] = ["exec", "container", "sandbox"];

/// The only metric source in this build: Legion's own measurements.
pub const LEGION_SOURCE: &str = crate::autoscaler::defaults::LEGION_SOURCE;

/// The `legion` source's fixed vocabulary (docs/autoscaling.md, "Checks").
pub const LEGION_QUERIES: [&str; 7] = [
    "task.cpu.utilization",
    "task.memory.utilization",
    "task.unplaced",
    "service.rps",
    "service.errors",
    "service.latency_ms.mean",
    "service.latency_ms.max",
];

/// `job/group/task`, the way every autoscaling surface names a target.
pub fn target_label(job_id: &str, group: &str, task: &str) -> String {
    format!("{job_id}/{group}/{task}")
}

/// The inherited levels of every autoscale policy: the cluster's scaling
/// defaults and the namespace policies that set `autoscale`.
#[derive(Debug, Clone, Default)]
pub struct AutoscaleLayers {
    /// The cluster's scaling defaults, when set.
    pub cluster: Option<AutoscaleRecord>,
    /// Their version (0: never applied).
    pub cluster_version: u64,
    /// The namespace policies with an `autoscale` record.
    pub policies: Vec<NamespacePolicy>,
}

impl AutoscaleLayers {
    /// Build from the stored pieces. A stored record that no longer parses
    /// is an error rather than silently ignored.
    pub fn from_parts(
        cluster_json: Option<&str>,
        cluster_version: u64,
        policies: impl IntoIterator<Item = NamespacePolicy>,
    ) -> R<Self> {
        let cluster = match cluster_json {
            Some(json) => {
                let record: AutoscaleRecord = serde_json::from_str(json)
                    .context("stored cluster scaling defaults do not parse")?;
                (record != AutoscaleRecord::default()).then_some(record)
            }
            None => None,
        };
        Ok(Self {
            cluster,
            cluster_version,
            policies: policies
                .into_iter()
                .filter(|policy| policy.autoscale.is_some())
                .collect(),
        })
    }

    /// Load from the store's committed state.
    pub fn load(store: &Store) -> R<Self> {
        let defaults = store.scaling_defaults()?;
        let policies = store
            .list_namespace_policies()?
            .into_iter()
            .map(|record| record.policy);
        Self::from_parts(
            defaults.as_ref().map(|row| row.record.as_str()),
            defaults.as_ref().map_or(0, |row| row.version),
            policies,
        )
    }

    /// Load through a connection or transaction the caller holds (the submit
    /// arbitration runs inside the submit's own transaction).
    pub fn load_tx(conn: &Connection) -> R<Self> {
        use rusqlite::OptionalExtension;
        let defaults: Option<(String, i64)> = conn
            .query_row(
                "SELECT record, version FROM scaling_defaults WHERE id = 1;",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let mut stmt = conn.prepare_cached("SELECT object FROM namespace_policy;")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut policies = Vec::new();
        for row in rows {
            let raw = row?;
            if let Ok(policy) = serde_json::from_str::<NamespacePolicy>(&raw) {
                policies.push(policy);
            }
        }
        Self::from_parts(
            defaults.as_ref().map(|(record, _)| record.as_str()),
            defaults.as_ref().map_or(0, |(_, version)| (*version).max(0) as u64),
            policies,
        )
    }

    /// The `autoscale` records of the namespace policies governing
    /// `namespace`.
    pub fn namespace_defaults(&self, namespace: &str) -> Vec<NamespaceDefaults> {
        self.policies
            .iter()
            .filter(|policy| policy.matches(namespace))
            .filter_map(|policy| {
                policy.autoscale.as_ref().map(|record| NamespaceDefaults {
                    policy: policy.name.clone(),
                    record: record.clone(),
                })
            })
            .collect()
    }

    /// Resolve one task's `autoscale` against these layers. `Ok(None)` for a
    /// task without autoscaling.
    pub fn resolve(&self, job_id: &str, task: &TaskConfig) -> Result<Option<ResolvedPolicy>, String> {
        let Some(spec) = task.autoscale.as_ref().filter(|spec| spec.is_enabled()) else {
            return Ok(None);
        };
        let namespace = job_namespace(job_id);
        let defaults = self.namespace_defaults(namespace);
        let layers = DefaultsLayers {
            cluster: self.cluster.as_ref(),
            namespace,
            policies: &defaults,
        };
        resolve(
            spec,
            task.count.num,
            &layers,
            &target_label(job_id, &task.group, &task.name),
        )
    }

    /// The effective defaults for `namespace`, with no task (`legion scaling
    /// defaults get -n`).
    pub fn effective_defaults(&self, namespace: &str) -> Result<EffectiveDefaults, String> {
        let defaults = self.namespace_defaults(namespace);
        crate::autoscaler::effective_defaults(&DefaultsLayers {
            cluster: self.cluster.as_ref(),
            namespace,
            policies: &defaults,
        })
    }

    /// The names of the namespace policies governing `namespace` that set
    /// `autoscale`.
    pub fn policy_names(&self, namespace: &str) -> Vec<String> {
        self.namespace_defaults(namespace)
            .into_iter()
            .map(|defaults| defaults.policy)
            .collect()
    }
}

/// Whether a task has autoscaling on.
pub fn is_autoscaled(task: &TaskConfig) -> bool {
    task.autoscale.as_ref().is_some_and(|spec| spec.is_enabled())
}

/// The tasks a policy on `task` moves: the task alone, or every task of its
/// group (plan §6.3, "Multi-task groups"), in spec order.
pub fn group_members(config: &JobConfig, task: &TaskConfig) -> Vec<String> {
    config
        .tasks
        .iter()
        .filter(|other| other.group == task.group)
        .map(|other| other.name.clone())
        .collect()
}

/// Whether a record's signals read the task's own usage. A record that names
/// no signal inherits the default signal, which is always a utilization
/// (defaults may set only `cpu` and `memory`).
pub fn needs_usage(record: &AutoscaleRecord) -> bool {
    if !record.names_signal() {
        return true;
    }
    record.cpu.is_some()
        || record.memory.is_some()
        || record.checks.iter().flatten().any(|check| {
            check.source == LEGION_SOURCE && check.query.ends_with(".utilization")
        })
}

/// The `autoscale` rules that need only the jobspec. Run by
/// `validate_job_config`, so `legion job validate` catches them offline.
pub fn validate_static(config: &JobConfig) -> R<()> {
    for task in config.tasks.iter().filter(|task| is_autoscaled(task)) {
        let spec = task.autoscale.as_ref().expect("filtered on autoscale");
        let at = target_label(&config.name, &task.group, &task.name);
        spec.validate().map_err(|err| anyhow!("{at}: {err}"))?;
        let record = spec.record().unwrap_or_default();
        if task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed)
        {
            bail!(
                "{at}: autoscale cannot be set on a keyed stateful task: its instances come from \
                 the instance API, not a count. Hint: remove autoscale, or use slot mode"
            );
        }
        if task.count.per.is_some() {
            bail!(
                "{at}: autoscale cannot be set on a `count.per` task: its count is per node, set \
                 by where it runs. Hint: remove autoscale, or use a plain count"
            );
        }
        if task.lifecycle == TaskLifecycle::Batch || task.schedule.is_some() {
            bail!(
                "{at}: autoscale applies to long-running tasks; a batch or scheduled task \
                 finishes by design. Hint: remove autoscale"
            );
        }
        let siblings: Vec<&TaskConfig> = config
            .tasks
            .iter()
            .filter(|other| other.group == task.group && other.name != task.name)
            .collect();
        if let Some(other) = siblings.iter().find(|other| is_autoscaled(other)) {
            bail!(
                "{at}: tasks {} and {} share group '{}' and both set autoscale; a group scales as \
                 one unit. Hint: set autoscale on one task of the group",
                task.name,
                other.name,
                task.group
            );
        }
        if let Some(other) = siblings
            .iter()
            .find(|other| other.count.num != task.count.num)
        {
            bail!(
                "{at}: autoscale on a task in group '{}' moves every task of the group to one \
                 count, but task {} has count {} and task {} has count {}. Hint: give the group's \
                 tasks the same count",
                task.group,
                task.name,
                task.count.num,
                other.name,
                other.count.num
            );
        }
        if let Some(other) = siblings
            .iter()
            .find(|other| other.count.per.is_some() || other.stateful.as_ref().is_some_and(|s| s.mode == InstanceMode::Keyed))
        {
            bail!(
                "{at}: task {} in the same group is keyed or `count.per`, and a group scales as \
                 one unit. Hint: move it to its own group",
                other.name
            );
        }
        if matches!(record.min, Some(CountBound::Replicas(0)))
            || matches!(record.min, Some(CountBound::Multiple(m)) if m <= 0.0)
        {
            bail!(
                "{at}: autoscale.min = 0 (scale to zero) is not supported: nothing could wake the \
                 task again. Hint: set min >= 1"
            );
        }
        if task.count.num == 0 {
            bail!(
                "{at}: count = 0 with autoscale; count is the starting size and the default \
                 floor. Hint: set count >= 1"
            );
        }
        for (index, check) in record.checks.iter().flatten().enumerate() {
            let label = format!("autoscale.checks[{index}] ({})", check.name);
            if check.source != LEGION_SOURCE {
                bail!(
                    "{at}: {label}: unknown source '{}'. Hint: the only source in this version \
                     is `legion`, which reads {}",
                    check.source,
                    LEGION_QUERIES.join(", ")
                );
            }
            if !check.query.is_empty() && !LEGION_QUERIES.contains(&check.query.as_str()) {
                bail!(
                    "{at}: {label}: the `legion` source has no query '{}'. Hint: use one of {}",
                    check.query,
                    LEGION_QUERIES.join(", ")
                );
            }
            if check.activation.is_some() {
                bail!(
                    "{at}: {label}: activation is for scaling from zero, which is not supported. \
                     Hint: remove activation"
                );
            }
            if check.query.starts_with("service.") && task.service.is_none() {
                bail!(
                    "{at}: {label}: '{}' reads the service the task backs, but the task has no \
                     `service` block. Hint: add one, or use a task.* query",
                    check.query
                );
            }
        }
        if needs_usage(&record) {
            let members: Vec<&TaskConfig> = std::iter::once(task)
                .chain(siblings.iter().copied())
                .collect();
            if let Some(blind) = members
                .iter()
                .find(|member| !USAGE_RUNTIMES.contains(&member.runtime.as_str()))
            {
                let whose = if blind.name == task.name {
                    String::new()
                } else {
                    format!(" (task {} of its group)", blind.name)
                };
                bail!(
                    "{at}: autoscale needs measured usage, and runtime '{}'{whose} does not \
                     report it. Hint: only {} report usage; use checks on another `legion` \
                     query (task.unplaced, service.*), or remove autoscale",
                    blind.runtime,
                    USAGE_RUNTIMES.join(", ")
                );
            }
        }
    }
    Ok(())
}

/// One autoscaled task's resolution in a plan or a submit.
#[derive(Debug, Clone)]
pub struct ResolvedTask {
    /// `job/group/task`.
    pub target: String,
    /// The resolved policy.
    pub resolved: ResolvedPolicy,
    /// What the defaults cannot know, said at submit (the default CPU
    /// reservation).
    pub warnings: Vec<String>,
    /// The human summary ([`describe`]).
    pub summary: Vec<String>,
}

/// The `autoscale` rules that need the layered defaults: every autoscaled
/// task must resolve (`min <= count <= max` and the rest), and a gang's
/// floor must hold its quorum. Returns each task's resolution with its
/// warnings and summary.
pub fn validate_resolved(
    config: &JobConfig,
    layers: &AutoscaleLayers,
) -> Result<Vec<ResolvedTask>, String> {
    let mut out = Vec::new();
    for task in config.tasks.iter().filter(|task| is_autoscaled(task)) {
        let target = target_label(&config.name, &task.group, &task.name);
        let Some(resolved) = layers.resolve(&config.name, task)? else {
            continue;
        };
        if let Some(gang) = task.gang.as_ref() {
            let quorum = gang.min.unwrap_or(task.count.num);
            if resolved.policy.min < quorum {
                return Err(format!(
                    "{target}: autoscale.min = {} is below the gang's quorum of {quorum}: a gang \
                     short of its quorum places nothing. Hint: set autoscale.min >= {quorum}, or \
                     lower gang.min",
                    resolved.policy.min
                ));
            }
        }
        let mut warnings = Vec::new();
        if resolved.policy.cpu.is_some() && cpu_reservation(task).is_none() {
            warnings.push(format!(
                "{target}: autoscale holds cpu at {} of the reservation, but this task uses the \
                 default cpu reservation. Set resources.cpu to what one replica needs",
                percent(resolved.policy.cpu.unwrap_or_default())
            ));
        }
        let summary = describe(&resolved, task);
        out.push(ResolvedTask {
            target,
            resolved,
            warnings,
            summary,
        });
    }
    Ok(out)
}

/// One autoscaling target of a stored job, as `WatchScalingPolicies` and
/// `GetScalingStatus` report it.
#[derive(Debug, Clone)]
pub struct TargetPolicy {
    pub job_id: String,
    pub task_group: String,
    pub task_name: String,
    /// Every task the policy moves.
    pub members: Vec<String>,
    /// The spec count.
    pub count: u32,
    pub runtime: String,
    /// The resolved policy, or why it does not resolve.
    pub resolved: Result<ResolvedPolicy, String>,
}

impl TargetPolicy {
    /// `job/group/task`.
    pub fn label(&self) -> String {
        target_label(&self.job_id, &self.task_group, &self.task_name)
    }
}

/// The autoscaling targets of one stored job config.
pub fn targets_for_config(config: &JobConfig, layers: &AutoscaleLayers) -> Vec<TargetPolicy> {
    config
        .tasks
        .iter()
        .filter(|task| is_autoscaled(task))
        .map(|task| TargetPolicy {
            job_id: config.name.clone(),
            task_group: task.group.clone(),
            task_name: task.name.clone(),
            members: group_members(config, task),
            count: task.count.num,
            runtime: task.runtime.clone(),
            resolved: match layers.resolve(&config.name, task) {
                Ok(Some(resolved)) => Ok(resolved),
                Ok(None) => Err("autoscale is off".to_string()),
                Err(err) => Err(err),
            },
        })
        .collect()
}

fn percent(fraction: f64) -> String {
    format!("{}%", fmt_num(fraction * 100.0))
}

/// The task's CPU reservation as people write it (`500MHz`, `2 cores`), when
/// its `resources` set one.
pub fn cpu_reservation(task: &TaskConfig) -> Option<String> {
    let resources = &task.resources;
    if let Some(cores) = resources.get("cores").and_then(Value::as_f64) {
        return Some(format!(
            "{} core{}",
            fmt_num(cores),
            if (cores - 1.0).abs() < f64::EPSILON { "" } else { "s" }
        ));
    }
    resources
        .get("cpu")
        .and_then(Value::as_f64)
        .map(|mhz| format!("{}MHz", fmt_num(mhz)))
}

fn memory_reservation(task: &TaskConfig) -> Option<String> {
    task.resources
        .get("memory")
        .and_then(|value| match value {
            Value::Number(number) => number.as_f64().map(|mib| format!("{}MiB", fmt_num(mib))),
            Value::String(text) => Some(text.clone()),
            _ => None,
        })
}

fn bound_words(bound: Option<CountBound>, name: &str) -> Option<String> {
    match bound {
        Some(CountBound::Multiple(m)) if (m - 1.0).abs() < 1e-9 => Some(format!("{name} = count")),
        Some(CountBound::Multiple(m)) => Some(format!("{name} = {} × count", fmt_num(m))),
        _ => None,
    }
}

fn per_every(every: std::time::Duration) -> String {
    if every.as_secs() == 60 {
        "per minute".to_string()
    } else {
        format!("per {}", format_duration(every))
    }
}

/// The policy in one line, for status tables: `2..8, cpu 70%, enforce`.
pub fn summary_line(policy: &EffectivePolicy) -> String {
    let mut parts = vec![format!("{}..{}", policy.min, policy.max)];
    if let Some(cpu) = policy.cpu {
        parts.push(format!("cpu {}", percent(cpu)));
    }
    if let Some(memory) = policy.memory {
        parts.push(format!("memory {}", percent(memory)));
    }
    for check in &policy.checks {
        parts.push(format!("{} ({})", check.name, check.strategy));
    }
    parts.push(policy.mode.to_string());
    parts.join(", ")
}

/// The human summary `legion job plan` prints for a policy (plan §6.3):
///
/// ```text
/// hold cpu at 70% of 500MHz per replica, 1m average over ready replicas
/// bounds 2..8 (min = count, max = 4 × count)
/// up:   immediately, at most +100% or +4 per minute (whichever is larger)
/// down: after 5m below target, at most −10% or −1 per minute
/// ```
pub fn describe(resolved: &ResolvedPolicy, task: &TaskConfig) -> Vec<String> {
    let policy = &resolved.policy;
    let mut lines = Vec::new();
    let window = format_duration(policy.window);
    if let Some(cpu) = policy.cpu {
        let of = cpu_reservation(task)
            .map(|reservation| format!("{reservation} per replica"))
            .unwrap_or_else(|| "the default cpu reservation".to_string());
        lines.push(format!(
            "hold cpu at {} of {of}, {window} average over ready replicas",
            percent(cpu)
        ));
    }
    if let Some(memory) = policy.memory {
        let of = memory_reservation(task)
            .map(|reservation| format!("{reservation} per replica"))
            .unwrap_or_else(|| "the default memory reservation".to_string());
        lines.push(format!(
            "hold memory at {} of {of}, {window} average over ready replicas",
            percent(memory)
        ));
    }
    for check in &policy.checks {
        lines.push(format!(
            "check {}: {} on {} {}, {} window",
            check.name,
            check.strategy,
            check.source,
            check.query,
            format_duration(check.window)
        ));
    }
    let explained: Vec<String> = [
        bound_words(resolved.record.min, "min"),
        bound_words(resolved.record.max, "max"),
    ]
    .into_iter()
    .flatten()
    .collect();
    lines.push(if explained.is_empty() {
        format!("bounds {}..{}", policy.min, policy.max)
    } else {
        format!(
            "bounds {}..{} ({})",
            policy.min,
            policy.max,
            explained.join(", ")
        )
    });
    let step = |step: &crate::autoscaler::defaults::Step, sign: char| -> String {
        let minus = if sign == '+' { '+' } else { '−' };
        match (step.percent > 0.0, step.replicas > 0) {
            (true, true) => format!(
                "{minus}{}% or {minus}{}",
                fmt_num(step.percent),
                step.replicas
            ),
            (true, false) => format!("{minus}{}%", fmt_num(step.percent)),
            (false, _) => format!("{minus}{}", step.replicas),
        }
    };
    let both = |s: &crate::autoscaler::defaults::Step| {
        if s.percent > 0.0 && s.replicas > 0 {
            " (whichever is larger)"
        } else {
            ""
        }
    };
    let up_when = if policy.up.stabilize.is_zero() {
        "immediately".to_string()
    } else {
        format!("after {} above target", format_duration(policy.up.stabilize))
    };
    lines.push(format!(
        "up:   {up_when}, at most {} {}{}",
        step(&policy.up.step, '+'),
        per_every(policy.up.every),
        both(&policy.up.step)
    ));
    let down_when = if policy.down.stabilize.is_zero() {
        "immediately".to_string()
    } else {
        format!("after {} below target", format_duration(policy.down.stabilize))
    };
    lines.push(format!(
        "down: {down_when}, at most {} {}{}",
        step(&policy.down.step, '-'),
        per_every(policy.down.every),
        both(&policy.down.step)
    ));
    let mut extra = vec![
        format!("warmup {}", format_duration(policy.warmup)),
        format!("tolerance ±{}", percent(policy.tolerance)),
    ];
    if policy.headroom > 0 {
        extra.push(format!("headroom +{}", policy.headroom));
    }
    if !policy.schedule.is_empty() {
        extra.push(format!("{} schedule window(s)", policy.schedule.len()));
    }
    extra.push(match policy.mode {
        Mode::Enforce => "enforce".to_string(),
        Mode::DryRun => "dry-run (records what it would do, changes nothing)".to_string(),
    });
    lines.push(extra.join(", "));
    lines
}

/// Whether `next` differs from `previous` only in `autoscale`, and in the
/// `count` of tasks that are autoscaled in `next` (plan §6.3, "Editing the
/// policy restarts nothing"). Such a resubmit moves the running
/// allocations to the new version instead of rolling out.
pub fn in_place_change(previous: &JobConfig, next: &JobConfig) -> bool {
    if previous.tasks.len() != next.tasks.len() {
        return false;
    }
    if !next.tasks.iter().any(is_autoscaled) && !previous.tasks.iter().any(is_autoscaled) {
        return false;
    }
    let normalize = |config: &JobConfig, scaled: &dyn Fn(&TaskConfig) -> bool| {
        let mut config = config.clone();
        config.version = None;
        for task in config.tasks.iter_mut() {
            if scaled(task) {
                task.count.num = 0;
            }
            task.autoscale = None;
        }
        config
    };
    // A task's count may differ only when the new version autoscales it.
    let autoscaled_next = |task: &TaskConfig| {
        next.tasks
            .iter()
            .any(|n| n.group == task.group && n.name == task.name && is_autoscaled(n))
    };
    let before = normalize(previous, &autoscaled_next);
    let after = normalize(next, &autoscaled_next);
    before == after
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(tasks: Vec<Value>) -> JobConfig {
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "web".into(),
            tasks: tasks
                .into_iter()
                .map(|task| serde_json::from_value(task).expect("task parses"))
                .collect(),
            version: None,
            pending_extensions: Default::default(),
        }
    }

    fn web(extra: Value) -> Value {
        let mut task = json!({
            "name": "web", "runtime": "exec", "count": 2,
            "config": { "command": ["sleep", "1"] },
            "resources": { "cpu": 500, "memory": 512 },
            "autoscale": true,
        });
        if let (Value::Object(task), Value::Object(extra)) = (&mut task, extra) {
            task.extend(extra);
        }
        task
    }

    #[test]
    fn static_rules_table_test() {
        let cases: Vec<(Vec<Value>, Option<&str>)> = vec![
            (vec![web(json!({}))], None),
            (
                vec![web(json!({ "runtime": "noop", "config": {} }))],
                Some("runtime 'noop' does not report it"),
            ),
            (
                vec![web(json!({ "count": { "num": 1, "per": "node" } }))],
                Some("`count.per` task"),
            ),
            (
                vec![web(json!({ "stateful": { "mode": "keyed" }, "count": 1 }))],
                Some("keyed stateful task"),
            ),
            (
                vec![web(json!({ "autoscale": { "min": 0 } }))],
                Some("scale to zero"),
            ),
            (
                vec![web(json!({ "lifecycle": "batch" }))],
                Some("long-running tasks"),
            ),
            (
                vec![
                    web(json!({})),
                    web(json!({ "name": "side", "autoscale": false, "count": 3 })),
                ],
                Some("has count 2 and task side has count 3"),
            ),
            (
                vec![web(json!({})), web(json!({ "name": "side" }))],
                Some("both set autoscale"),
            ),
            (
                vec![web(json!({})), web(json!({ "name": "side", "autoscale": false }))],
                None,
            ),
            (
                vec![
                    web(json!({})),
                    web(json!({ "name": "side", "autoscale": false, "runtime": "noop", "config": {} })),
                ],
                Some("(task side of its group) does not report it"),
            ),
            (
                vec![web(json!({ "autoscale": { "checks": [{
                    "name": "lag", "source": "team-prom", "query": "q",
                    "strategy": { "name": "per-replica", "value": 5 } }] } }))],
                Some("unknown source 'team-prom'"),
            ),
            (
                vec![web(json!({ "autoscale": { "checks": [{
                    "name": "lag", "source": "legion", "query": "task.lag",
                    "strategy": { "name": "per-replica", "value": 5 } }] } }))],
                Some("has no query 'task.lag'"),
            ),
            (
                vec![web(json!({ "runtime": "noop", "config": {}, "autoscale": { "checks": [{
                    "name": "unplaced", "source": "legion", "query": "task.unplaced",
                    "strategy": { "name": "pass-through" } }] } }))],
                None,
            ),
            (
                vec![web(json!({ "autoscale": { "checks": [{
                    "name": "rps", "source": "legion", "query": "service.rps",
                    "strategy": { "name": "per-replica", "value": 100 } }] } }))],
                Some("has no `service` block"),
            ),
        ];
        for (tasks, expected) in cases {
            let config = job(tasks.clone());
            let result = validate_static(&config);
            match expected {
                None => assert!(result.is_ok(), "{tasks:?}: {result:?}"),
                Some(text) => {
                    let err = format!("{:#}", result.unwrap_err());
                    assert!(err.contains(text), "{tasks:?}: {err}");
                    assert!(err.starts_with("web/default/web: "), "{err}");
                }
            }
        }
    }

    #[test]
    fn resolved_rules_and_summary_test() -> R<()> {
        let layers = AutoscaleLayers::default();
        let resolved = validate_resolved(&job(vec![web(json!({}))]), &layers)
            .map_err(|err| anyhow!(err))?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved[0].summary,
            vec![
                "hold cpu at 70% of 500MHz per replica, 1m average over ready replicas",
                "bounds 2..8 (min = count, max = 4 × count)",
                "up:   immediately, at most +100% or +4 per minute (whichever is larger)",
                "down: after 5m below target, at most −10% or −1 per minute (whichever is larger)",
                "warmup 30s, tolerance ±10%, enforce",
            ]
        );
        assert!(resolved[0].warnings.is_empty());
        assert_eq!(summary_line(&resolved[0].resolved.policy), "2..8, cpu 70%, enforce");

        // No cpu reservation: a warning, not an error.
        let bare = job(vec![web(json!({ "resources": { "memory": 64 } }))]);
        let resolved = validate_resolved(&bare, &layers).map_err(|err| anyhow!(err))?;
        assert!(resolved[0].warnings[0].contains("default cpu reservation"));

        // count above an explicit max.
        let err = validate_resolved(&job(vec![web(json!({ "autoscale": { "max": 1 } }))]), &layers)
            .unwrap_err();
        assert!(err.contains("count 2 is above autoscale.max = 1"), "{err}");

        // A gang's floor must hold its quorum.
        let gang = job(vec![web(json!({
            "count": 4, "gang": { "min": 3 }, "autoscale": { "min": 2 }
        }))]);
        let err = validate_resolved(&gang, &layers).unwrap_err();
        assert!(err.contains("below the gang's quorum of 3"), "{err}");

        // Inherited layers apply live: a cluster default changes the bounds.
        let layers = AutoscaleLayers::from_parts(Some(r#"{"max":"6x","cpu":0.6}"#), 2, Vec::new())?;
        let resolved = validate_resolved(&job(vec![web(json!({}))]), &layers)
            .map_err(|err| anyhow!(err))?;
        assert_eq!(summary_line(&resolved[0].resolved.policy), "2..12, cpu 60%, enforce");
        Ok(())
    }

    #[test]
    fn namespace_policies_layer_by_glob_test() -> R<()> {
        let policy: NamespacePolicy = serde_json::from_value(json!({
            "name": "payments",
            "namespaces": ["payments-*"],
            "autoscale": { "mode": "dry-run" },
        }))?;
        let layers = AutoscaleLayers::from_parts(None, 0, vec![policy])?;
        let task: TaskConfig = serde_json::from_value(web(json!({})))?;
        let mine = layers
            .resolve("payments-prod/api", &task)
            .map_err(|err| anyhow!(err))?
            .context("resolves")?;
        assert_eq!(mine.policy.mode, Mode::DryRun);
        let other = layers
            .resolve("web", &task)
            .map_err(|err| anyhow!(err))?
            .context("resolves")?;
        assert_eq!(other.policy.mode, Mode::Enforce);
        Ok(())
    }

    #[test]
    fn in_place_change_matrix_test() {
        let base = job(vec![web(json!({}))]);
        let with = |extra: Value| job(vec![web(extra)]);
        // Only autoscale changes: in place.
        assert!(in_place_change(&base, &with(json!({ "autoscale": { "max": 20 } }))));
        // Only the count of an autoscaled task: in place.
        assert!(in_place_change(&base, &with(json!({ "count": 4 }))));
        // Both.
        assert!(in_place_change(&base, &with(json!({ "count": 3, "autoscale": { "max": 9 } }))));
        // Removing autoscale with the same count: in place.
        assert!(in_place_change(&base, &with(json!({ "autoscale": false }))));
        // Removing autoscale and changing count: not (Phase 0's rollout).
        assert!(!in_place_change(&base, &with(json!({ "autoscale": false, "count": 5 }))));
        // Anything else: a rollout.
        assert!(!in_place_change(&base, &with(json!({ "resources": { "cpu": 600, "memory": 512 } }))));
        // No autoscale on either side: never in place.
        let plain = job(vec![web(json!({ "autoscale": false }))]);
        let plain_more = job(vec![web(json!({ "autoscale": false, "count": 4 }))]);
        assert!(!in_place_change(&plain, &plain_more));
    }
}
