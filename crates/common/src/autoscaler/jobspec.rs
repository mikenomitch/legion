//! The `autoscale` rules that need only the jobspec: what is checked
//! offline by `legion job validate` and again at submit. The rules that
//! need the cluster's scaling defaults or its installed plugins are the
//! manager's (`manager::autoscale`).

use super::defaults::LEGION_SOURCE;
use super::AutoscaleRecord;
use crate::prelude::*;
use crate::structs::{InstanceMode, JobConfig, TaskConfig, TaskLifecycle};

/// Runtimes whose replicas report measured usage (docs/resource-enforcement.md,
/// "Measuring usage"). A policy whose signals read the task's own usage needs
/// one of them.
pub const USAGE_RUNTIMES: [&str; 3] = ["exec", "container", "sandbox"];

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

/// Whether a task has autoscaling on.
pub fn is_autoscaled(task: &TaskConfig) -> bool {
    task.autoscale
        .as_ref()
        .is_some_and(|spec| spec.is_enabled())
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
