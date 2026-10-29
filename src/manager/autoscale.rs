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
//!   and the `count` of autoscaled tasks, or in installed apps' jobspec
//!   extension values (apps plan §6.6), so `RunJob` can move allocations to
//!   the new version instead of rolling out.

use rusqlite::Connection;

use crate::autoscaler::defaults::{EffectiveDefaults, NamespaceDefaults, ResolvedPolicy};
use crate::autoscaler::plugins::{Kind, PluginCatalog};
use crate::autoscaler::policy::ExternalPolicy;
use crate::autoscaler::spec::format_duration;
use crate::autoscaler::strategies::{fmt_num, StrategyConfig};
use crate::autoscaler::{
    resolve, AutoscaleRecord, AutoscaleSpec, CountBound, DefaultsLayers, EffectivePolicy, Mode,
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

/// The built-in metric source: Legion's own measurements. Every other source
/// is a plugin installed in the autoscaler's app config.
pub const LEGION_SOURCE: &str = crate::autoscaler::defaults::LEGION_SOURCE;

/// The bundled autoscaler app, whose config installs the scaling plugins.
pub const AUTOSCALER_APP: &str = "autoscaler";

/// The key a standalone policy's history, pauses and status live under in
/// the tables task targets use (`job_id`, with empty group and task):
/// `policy:<id>`, which no job id can be.
pub fn policy_target_key(id: &str) -> String {
    format!("policy:{id}")
}

/// The standalone policy id a target key names, if it names one.
pub fn policy_of_target_key(job_id: &str) -> Option<&str> {
    job_id.strip_prefix("policy:")
}

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
    /// The plugins installed in the autoscaler's app config, when loaded:
    /// a policy naming a plugin that is not installed, or one closed to its
    /// namespace, does not resolve. `None` skips the check (the submit
    /// arbitration, which only needs bounds).
    pub plugins: Option<PluginCatalog>,
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
            plugins: None,
        })
    }

    /// With the installed plugins checked on every resolution.
    pub fn with_plugins(mut self, plugins: PluginCatalog) -> Self {
        self.plugins = Some(plugins);
        self
    }

    /// Load from the store's committed state, the installed plugins
    /// included.
    pub fn load(store: &Store) -> R<Self> {
        let defaults = store.scaling_defaults()?;
        let policies = store
            .list_namespace_policies()?
            .into_iter()
            .map(|record| record.policy);
        Ok(Self::from_parts(
            defaults.as_ref().map(|row| row.record.as_str()),
            defaults.as_ref().map_or(0, |row| row.version),
            policies,
        )?
        .with_plugins(installed_plugins(store)?))
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
            defaults
                .as_ref()
                .map_or(0, |(_, version)| (*version).max(0) as u64),
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
    pub fn resolve(
        &self,
        job_id: &str,
        task: &TaskConfig,
    ) -> Result<Option<ResolvedPolicy>, String> {
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
        let label = target_label(job_id, &task.group, &task.name);
        let resolved = resolve(spec, task.count.num, &layers, &label)?;
        if let (Some(catalog), Some(resolved)) = (&self.plugins, &resolved) {
            check_plugin_use(&resolved.policy, catalog, namespace)
                .map_err(|err| format!("{label}: {err}"))?;
        }
        Ok(resolved)
    }

    /// Resolve a node group: its own defaults (a node group inherits none
    /// of the task layers), and a target plugin that is installed and may
    /// run node groups.
    pub fn resolve_node_group(
        &self,
        policy: &crate::autoscaler::nodes::NodeGroupPolicy,
    ) -> Result<crate::autoscaler::nodes::EffectiveNodeGroup, String> {
        let group = policy.resolve()?;
        if let Some(catalog) = &self.plugins {
            catalog
                .check_node_target(&group.plugin)
                .map_err(|err| format!("{}: {err}", group.name))?;
        }
        Ok(group)
    }

    /// Resolve a standalone external policy against these layers: its
    /// record over the cluster defaults and its namespace's policies, with
    /// `min` standing in for the `count` a task would have (both bounds are
    /// replica counts), then every plugin it names checked.
    pub fn resolve_standalone(&self, policy: &ExternalPolicy) -> Result<ResolvedPolicy, String> {
        let label = crate::autoscaler::policy::policy_id(&policy.namespace, &policy.name);
        let count = match policy.record.min {
            Some(CountBound::Replicas(min)) => min,
            _ => 0,
        };
        let defaults = self.namespace_defaults(&policy.namespace);
        let layers = DefaultsLayers {
            cluster: self.cluster.as_ref(),
            namespace: &policy.namespace,
            policies: &defaults,
        };
        let spec = AutoscaleSpec::Policy(Box::new(policy.record.clone()));
        let resolved = resolve(&spec, count, &layers, &label)?
            .ok_or_else(|| format!("{label}: the policy resolves to nothing"))?;
        if let Some(catalog) = &self.plugins {
            crate::autoscaler::policy::check_external(policy, catalog)?;
        }
        Ok(resolved)
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

/// The plugins installed in the autoscaler's app config. No app record (a
/// cluster that never seeded it) is an empty catalog; a stored config that
/// no longer parses is too, with a warning: `legion app config set` refuses
/// such a config, so only an older build's could.
pub fn installed_plugins(store: &Store) -> R<PluginCatalog> {
    let Some(record) = store.get_app(AUTOSCALER_APP)? else {
        return Ok(PluginCatalog::default());
    };
    Ok(match PluginCatalog::from_app_config(&record.config) {
        Ok(catalog) => catalog,
        Err(err) => {
            warn!("The autoscaler's stored app config does not parse as plugins: {err}");
            PluginCatalog::default()
        }
    })
}

/// Check every plugin an effective policy names (each check's source, and
/// each plugin strategy) is installed and open to `namespace`. The error
/// names the check and the plugin, as calls entry 5 shows.
pub fn check_plugin_use(
    policy: &EffectivePolicy,
    catalog: &PluginCatalog,
    namespace: &str,
) -> Result<(), String> {
    for check in &policy.checks {
        if check.source != LEGION_SOURCE {
            catalog.check_use(
                &check.source,
                Kind::Source,
                namespace,
                &format!("check '{}' uses source '{}'", check.name, check.source),
            )?;
        }
        if let StrategyConfig::Plugin(plugin) = &check.strategy {
            catalog.check_use(
                &plugin.name,
                Kind::Strategy,
                namespace,
                &format!("check '{}' uses strategy '{}'", check.name, plugin.name),
            )?;
        }
    }
    Ok(())
}

/// Whether a task has autoscaling on.
pub fn is_autoscaled(task: &TaskConfig) -> bool {
    task.autoscale
        .as_ref()
        .is_some_and(|spec| spec.is_enabled())
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
        || record
            .checks
            .iter()
            .flatten()
            .any(|check| check.source == LEGION_SOURCE && check.query.ends_with(".utilization"))
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
        if let Some(other) = siblings.iter().find(|other| {
            other.count.per.is_some()
                || other
                    .stateful
                    .as_ref()
                    .is_some_and(|s| s.mode == InstanceMode::Keyed)
        }) {
            bail!(
                "{at}: task {} in the same group is keyed or `count.per`, and a group scales as \
                 one unit. Hint: move it to its own group",
                other.name
            );
        }
        if task.count.num == 0 {
            bail!(
                "{at}: count = 0 with autoscale; count is the starting size (with min = 0 the \
                 task sleeps once its activation checks go quiet). Hint: set count >= 1"
            );
        }
        for (index, check) in record.checks.iter().flatten().enumerate() {
            let label = format!("autoscale.checks[{index}] ({})", check.name);
            // Other sources are plugins, which only the manager can see
            // (`legion job plan` and `job run` check them).
            if check.source != LEGION_SOURCE {
                continue;
            }
            if !check.query.is_empty() && !LEGION_QUERIES.contains(&check.query.as_str()) {
                bail!(
                    "{at}: {label}: the `legion` source has no query '{}'. Hint: use one of {}",
                    check.query,
                    LEGION_QUERIES.join(", ")
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
            if (cores - 1.0).abs() < f64::EPSILON {
                ""
            } else {
                "s"
            }
        ));
    }
    resources
        .get("cpu")
        .and_then(Value::as_f64)
        .map(|mhz| format!("{}MHz", fmt_num(mhz)))
}

fn memory_reservation(task: &TaskConfig) -> Option<String> {
    task.resources.get("memory").and_then(|value| match value {
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
        format!(
            "after {} above target",
            format_duration(policy.up.stabilize)
        )
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
        format!(
            "after {} below target",
            format_duration(policy.down.stabilize)
        )
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

/// What an in-place resubmit changed: the parts of a job version that can
/// move to a new version without restarting anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InPlaceChange {
    /// `autoscale`, or the `count` of tasks the new version autoscales
    /// (autoscaler-v2 plan §6.3, "Editing the policy restarts nothing").
    pub autoscale: bool,
    /// Installed apps' jobspec extension values, or pending keys, at any
    /// point (apps plan §6.6, "Changing only extension values restarts
    /// nothing"): no runtime reads them, the apps see them through their
    /// watch.
    pub extensions: bool,
}

/// Whether `next` differs from `previous` only in what [`InPlaceChange`]
/// names. Such a resubmit moves the running allocations to the new version
/// instead of rolling out.
pub fn in_place_change(previous: &JobConfig, next: &JobConfig) -> bool {
    classify_in_place(previous, next).is_some()
}

/// [`in_place_change`], saying what changed. `None` when anything else
/// changed, or nothing did.
pub fn classify_in_place(previous: &JobConfig, next: &JobConfig) -> Option<InPlaceChange> {
    if previous.tasks.len() != next.tasks.len() {
        return None;
    }
    let autoscaled =
        next.tasks.iter().any(is_autoscaled) || previous.tasks.iter().any(is_autoscaled);
    // A task's count may differ only when the new version autoscales it.
    let autoscaled_next = |task: &TaskConfig| {
        next.tasks
            .iter()
            .any(|n| n.group == task.group && n.name == task.name && is_autoscaled(n))
    };
    let strip_autoscale = |config: &mut JobConfig| {
        if !autoscaled {
            return;
        }
        for task in config.tasks.iter_mut() {
            if autoscaled_next(task) {
                task.count.num = 0;
            }
            task.autoscale = None;
        }
    };
    let strip_extensions = |config: &mut JobConfig| {
        config.extensions.clear();
        config.pending_extensions.clear();
        for task in config.tasks.iter_mut() {
            task.pending_extensions.clear();
            for volume in task.volumes.iter_mut() {
                volume.pending_extensions.clear();
            }
            if let Some(network) = task.network.as_mut() {
                for port in network.ports.iter_mut() {
                    port.pending_extensions.clear();
                }
            }
            if let Some(service) = task.service.as_mut() {
                service.pending_extensions.clear();
            }
        }
    };
    let mut before = previous.clone();
    let mut after = next.clone();
    before.version = None;
    after.version = None;
    strip_extensions(&mut before);
    strip_extensions(&mut after);
    // Everything but the extensions equal: autoscale did not change.
    let same_but_extensions = before == after;
    strip_autoscale(&mut before);
    strip_autoscale(&mut after);
    if before != after {
        return None;
    }
    let change = InPlaceChange {
        autoscale: !same_but_extensions,
        extensions: {
            let mut a = previous.clone();
            let mut b = next.clone();
            a.version = None;
            b.version = None;
            strip_autoscale(&mut a);
            strip_autoscale(&mut b);
            a != b
        },
    };
    (change.autoscale || change.extensions).then_some(change)
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
            extensions: Default::default(),
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
                // Scale to zero needs a wake check: the engine's rule, at
                // resolution (validate_resolved).
                vec![web(json!({ "autoscale": { "min": 0, "checks": [{
                    "name": "lag", "source": "team-prom", "query": "q",
                    "strategy": { "name": "per-replica", "value": 5 }, "activation": 1 }] } }))],
                None,
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
                vec![
                    web(json!({})),
                    web(json!({ "name": "side", "autoscale": false })),
                ],
                None,
            ),
            (
                vec![
                    web(json!({})),
                    web(
                        json!({ "name": "side", "autoscale": false, "runtime": "noop", "config": {} }),
                    ),
                ],
                Some("(task side of its group) does not report it"),
            ),
            (
                // Plugins are checked where they are known (the manager).
                vec![web(json!({ "autoscale": { "checks": [{
                    "name": "lag", "source": "team-prom", "query": "q",
                    "strategy": { "name": "per-replica", "value": 5 } }] } }))],
                None,
            ),
            (
                vec![web(json!({ "autoscale": { "checks": [{
                    "name": "lag", "source": "legion", "query": "task.lag",
                    "strategy": { "name": "per-replica", "value": 5 } }] } }))],
                Some("has no query 'task.lag'"),
            ),
            (
                vec![web(
                    json!({ "runtime": "noop", "config": {}, "autoscale": { "checks": [{
                    "name": "unplaced", "source": "legion", "query": "task.unplaced",
                    "strategy": { "name": "pass-through" } }] } }),
                )],
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
        let resolved =
            validate_resolved(&job(vec![web(json!({}))]), &layers).map_err(|err| anyhow!(err))?;
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
        assert_eq!(
            summary_line(&resolved[0].resolved.policy),
            "2..8, cpu 70%, enforce"
        );

        // No cpu reservation: a warning, not an error.
        let bare = job(vec![web(json!({ "resources": { "memory": 64 } }))]);
        let resolved = validate_resolved(&bare, &layers).map_err(|err| anyhow!(err))?;
        assert!(resolved[0].warnings[0].contains("default cpu reservation"));

        // count above an explicit max.
        let err = validate_resolved(
            &job(vec![web(json!({ "autoscale": { "max": 1 } }))]),
            &layers,
        )
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
        let resolved =
            validate_resolved(&job(vec![web(json!({}))]), &layers).map_err(|err| anyhow!(err))?;
        assert_eq!(
            summary_line(&resolved[0].resolved.policy),
            "2..12, cpu 60%, enforce"
        );
        Ok(())
    }

    /// A policy may name only installed plugins open to its namespace, and
    /// `min = 0` needs a check that can wake it (plan §6.12).
    #[test]
    fn plugins_and_scale_to_zero_resolve_at_submit_test() -> R<()> {
        let catalog = PluginCatalog::from_app_config(&json!({ "plugins": {
            "team-prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" },
                           "namespaces": ["payments-*"] },
            "calendar": { "command": ["python3", "calendar.py"] },
        } }))
        .map_err(|err| anyhow!(err))?;
        let layers = AutoscaleLayers::default().with_plugins(catalog);
        let lag = |source: &str| {
            json!({ "autoscale": { "min": 0, "max": 10, "checks": [{
                "name": "lag", "source": source, "query": "sum(lag)",
                "strategy": { "name": "per-replica", "value": 500 }, "activation": 1 }] } })
        };
        let task: TaskConfig = serde_json::from_value(web(lag("team-prom")))?;
        let resolved = layers
            .resolve("payments-prod/api", &task)
            .map_err(|err| anyhow!(err))?
            .context("resolves")?;
        assert_eq!(resolved.policy.min, 0);
        let err = layers.resolve("web", &task).unwrap_err();
        assert_eq!(
            err,
            "web/default/web: check 'lag' uses source 'team-prom', which is not available to \
             namespace default (allowed: payments-*)"
        );
        let missing: TaskConfig = serde_json::from_value(web(lag("other-prom")))?;
        let err = layers.resolve("payments-prod/api", &missing).unwrap_err();
        assert!(
            err.contains("uses source 'other-prom', which is not installed"),
            "{err}"
        );
        let strategy: TaskConfig =
            serde_json::from_value(web(json!({ "autoscale": { "checks": [{
            "name": "batch", "source": "legion",
            "strategy": { "name": "calendar", "replicas": 6 }, "scaleIn": false }] } })))?;
        assert!(layers.resolve("web", &strategy).is_ok());
        let unknown: TaskConfig = serde_json::from_value(web(json!({ "autoscale": { "checks": [{
            "name": "batch", "source": "legion",
            "strategy": { "name": "perreplica", "value": 2 } }] } })))?;
        let err = layers.resolve("web", &unknown).unwrap_err();
        assert!(
            err.contains("uses strategy 'perreplica', which is not installed"),
            "{err}"
        );
        // Without the catalog (the submit arbitration), plugins are not
        // checked.
        assert!(AutoscaleLayers::default().resolve("web", &missing).is_ok());

        // Scale to zero on the task's own usage cannot wake: refused with
        // the documented message.
        let sleepy: TaskConfig = serde_json::from_value(web(json!({ "autoscale": { "min": 0 } })))?;
        let err = layers.resolve("web", &sleepy).unwrap_err();
        assert!(
            err.starts_with(
                "web/default/web: autoscale.min = 0 needs a check that can wake it from zero; \
                 'cpu' reads the task's own usage, which is empty at 0 replicas. Hint: add a \
                 check with `activation`"
            ),
            "{err}"
        );
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
        assert!(in_place_change(
            &base,
            &with(json!({ "autoscale": { "max": 20 } }))
        ));
        // Only the count of an autoscaled task: in place.
        assert!(in_place_change(&base, &with(json!({ "count": 4 }))));
        // Both.
        assert!(in_place_change(
            &base,
            &with(json!({ "count": 3, "autoscale": { "max": 9 } }))
        ));
        // Removing autoscale with the same count: in place.
        assert!(in_place_change(&base, &with(json!({ "autoscale": false }))));
        // Removing autoscale and changing count: not (Phase 0's rollout).
        assert!(!in_place_change(
            &base,
            &with(json!({ "autoscale": false, "count": 5 }))
        ));
        // Anything else: a rollout.
        assert!(!in_place_change(
            &base,
            &with(json!({ "resources": { "cpu": 600, "memory": 512 } }))
        ));
        // No autoscale on either side: a count change is never in place.
        let plain = job(vec![web(json!({ "autoscale": false }))]);
        let plain_more = job(vec![web(json!({ "autoscale": false, "count": 4 }))]);
        assert!(!in_place_change(&plain, &plain_more));
    }

    #[test]
    fn extension_only_changes_are_in_place_test() {
        use crate::structs::extensions::{ExtensionPath, ExtensionValue};
        use crate::structs::spec_fields::ExtensionPoint;
        let plain = job(vec![web(json!({ "autoscale": false }))]);
        let value = |keep: u32| ExtensionValue {
            app: "backup".into(),
            point: ExtensionPoint::Task,
            path: ExtensionPath {
                group: "web".into(),
                task: "web".into(),
                ..Default::default()
            },
            key: "backup".into(),
            value: json!({ "keep": keep }),
            validated_by: "backup@1".into(),
        };
        let mut set = plain.clone();
        set.extensions = vec![value(3)];
        let mut changed = plain.clone();
        changed.extensions = vec![value(4)];
        assert_eq!(
            classify_in_place(&plain, &set),
            Some(InPlaceChange {
                autoscale: false,
                extensions: true
            })
        );
        assert!(in_place_change(&set, &changed));
        // Removing the value is in place too.
        assert!(in_place_change(&changed, &plain));
        // A pending key alone (calls entry 62's restart, now in place).
        let mut pending = plain.clone();
        pending.tasks[0]
            .pending_extensions
            .insert("uptime".into(), json!(true));
        assert!(in_place_change(&plain, &pending));
        // `validated_by` alone is no change at all.
        let mut stamped = set.clone();
        stamped.extensions[0].validated_by = "backup@2".into();
        assert_eq!(classify_in_place(&set, &stamped), None);
        // An extension change with any other change rolls out.
        let mut both = changed.clone();
        both.tasks[0].resources = json!({ "cpu": 600, "memory": 512 });
        assert!(!in_place_change(&set, &both));
        // Autoscale and extensions together are in place, and say so.
        let base = job(vec![web(json!({}))]);
        let mut scaled = job(vec![web(json!({ "autoscale": { "max": 20 } }))]);
        scaled.extensions = vec![value(3)];
        assert_eq!(
            classify_in_place(&base, &scaled),
            Some(InPlaceChange {
                autoscale: true,
                extensions: true
            })
        );
    }
}
