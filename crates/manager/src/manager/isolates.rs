//! The manager's half of the `isolate` runtime (docs/task-runtime-isolate.md):
//! the latest isolate aggregates each node sent on its heartbeat, held in
//! memory beside usage, and the pure helpers the isolate RPCs build on.
//!
//! Managers keep no record of isolates (isolate-runtime calls entry 3): a
//! node reports one aggregate per isolate task (`HeartbeatRequest.
//! isolate_tasks`) and one row per isolate host process (`isolate_hosts`),
//! and this table keeps only the latest of each per node. Like usage, it is
//! high-churn and worthless after a restart (the next beat replaces it), so
//! it never touches the state store.

use crate::structs::{IsolateHostStats, IsolateNodeStats, IsolateTaskStats, TaskConfig};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// Reports older than this are dropped by [`IsolateStatsTable::sweep`]: a
/// node that stopped heartbeating stops showing numbers that look current.
/// The same horizon as usage reports.
pub const ISOLATE_STATS_STALE_AFTER: Duration = crate::manager::usage::USAGE_STALE_AFTER;

/// How often the heartbeat path sweeps the table.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// One node's latest isolate report and when this manager received it.
#[derive(Debug, Clone)]
pub struct NodeIsolateEntry {
    pub tasks: Vec<IsolateTaskStats>,
    pub hosts: Vec<IsolateHostStats>,
    pub received_at: Instant,
}

/// The latest isolate report per node.
#[derive(Debug, Default)]
pub struct IsolateStatsTable {
    nodes: HashMap<String, NodeIsolateEntry>,
    last_sweep: Option<Instant>,
}

impl IsolateStatsTable {
    /// Replace a node's report. A beat with neither tasks nor hosts means
    /// the node holds no isolate task any more (or never ran the runtime),
    /// and forgets the node.
    pub fn ingest(
        &mut self,
        node_id: &str,
        tasks: Vec<IsolateTaskStats>,
        hosts: Vec<IsolateHostStats>,
        now: Instant,
    ) {
        if tasks.is_empty() && hosts.is_empty() {
            self.nodes.remove(node_id);
            return;
        }
        self.nodes.insert(
            node_id.to_string(),
            NodeIsolateEntry {
                tasks,
                hosts,
                received_at: now,
            },
        );
    }

    pub fn node(&self, node_id: &str) -> Option<&NodeIsolateEntry> {
        self.nodes.get(node_id)
    }

    /// Drop reports older than [`ISOLATE_STATS_STALE_AFTER`], at most once
    /// per [`SWEEP_EVERY`]. Returns the node ids dropped.
    pub fn sweep(&mut self, now: Instant) -> Vec<String> {
        if self
            .last_sweep
            .is_some_and(|last| now.duration_since(last) < SWEEP_EVERY)
        {
            return Vec::new();
        }
        self.last_sweep = Some(now);
        let stale: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.received_at) >= ISOLATE_STATS_STALE_AFTER)
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node_id in &stale {
            self.nodes.remove(node_id);
        }
        stale
    }

    /// One entry per node that reported any of `task_names` of `job_id`,
    /// sorted by node id: the matching task aggregates, the host processes
    /// whose cordon serves them, and how old the report is at `now`.
    pub fn for_job(
        &self,
        job_id: &str,
        task_names: &[String],
        now: Instant,
    ) -> Vec<IsolateNodeStats> {
        let mut nodes: BTreeMap<&str, IsolateNodeStats> = BTreeMap::new();
        for (node_id, entry) in &self.nodes {
            if let Some(stats) = node_stats(node_id, entry, job_id, task_names, now) {
                nodes.insert(node_id.as_str(), stats);
            }
        }
        nodes.into_values().collect()
    }
}

/// One node's part of a reply, or `None` when it reported none of the
/// tasks.
pub fn node_stats(
    node_id: &str,
    entry: &NodeIsolateEntry,
    job_id: &str,
    task_names: &[String],
    now: Instant,
) -> Option<IsolateNodeStats> {
    let tasks = filter_tasks(&entry.tasks, job_id, task_names);
    if tasks.is_empty() {
        return None;
    }
    let hosts = hosts_serving(&entry.hosts, &tasks);
    Some(IsolateNodeStats {
        node_id: node_id.to_string(),
        tasks,
        hosts,
        age_ms: now
            .saturating_duration_since(entry.received_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    })
}

/// The aggregates of `task_names` of `job_id` among `tasks`, in task-name
/// order.
pub fn filter_tasks(
    tasks: &[IsolateTaskStats],
    job_id: &str,
    task_names: &[String],
) -> Vec<IsolateTaskStats> {
    let mut found: Vec<IsolateTaskStats> = tasks
        .iter()
        .filter(|stats| stats.job_id == job_id && task_names.contains(&stats.task_name))
        .cloned()
        .collect();
    found.sort_by(|a, b| a.task_name.cmp(&b.task_name));
    found
}

/// The host processes among `hosts` whose cordon serves one of `tasks`, by
/// cordon then replica.
pub fn hosts_serving(
    hosts: &[IsolateHostStats],
    tasks: &[IsolateTaskStats],
) -> Vec<IsolateHostStats> {
    let mut found: Vec<IsolateHostStats> = hosts
        .iter()
        .filter(|host| tasks.iter().any(|task| task.cordon == host.cordon))
        .cloned()
        .collect();
    found.sort_by(|a, b| (&a.cordon, a.replica).cmp(&(&b.cordon, b.replica)));
    found
}

/// The isolate tasks a stats request covers: the one `task_name` names, or
/// every isolate task of the job when it is empty. The error is the
/// FAILED_PRECONDITION text for a name that is no isolate task of the job.
pub fn stats_tasks(
    job_id: &str,
    tasks: &[TaskConfig],
    task_name: &str,
) -> Result<Vec<String>, String> {
    let isolates: Vec<&TaskConfig> = tasks
        .iter()
        .filter(|task| crate::structs::isolate::is_isolate_task(task))
        .collect();
    let task_name = task_name.trim();
    if task_name.is_empty() {
        return Ok(isolates.iter().map(|task| task.name.clone()).collect());
    }
    if isolates.iter().any(|task| task.name == task_name) {
        return Ok(vec![task_name.to_string()]);
    }
    let names: Vec<&str> = isolates.iter().map(|task| task.name.as_str()).collect();
    Err(if names.is_empty() {
        format!("job {job_id} has no isolate task (task {task_name} is not one)")
    } else {
        format!(
            "task {task_name} of job {job_id} is not an isolate task; its isolate tasks: {}",
            names.join(", ")
        )
    })
}

/// The tiers a run of an isolate task may ask for (`RunSessionRequest.tier`).
pub const TIERS: [&str; 3] = ["auto", "isolate", "vm"];

/// Check the isolate-only fields of a run request against its task, before
/// anything is routed. `Err((code, message))` is the documented refusal.
pub fn check_run_fields(
    job_id: &str,
    task: &TaskConfig,
    tier: &str,
    tenant: &str,
    session_id: &str,
    command: &[String],
) -> Result<(), (tonic::Code, String)> {
    let isolate = crate::structs::isolate::is_isolate_task(task);
    if !isolate {
        let set: Vec<&str> = [
            ("tier", tier),
            ("tenant", tenant),
            ("session_id", session_id),
        ]
        .into_iter()
        .filter(|(_, value)| !value.trim().is_empty())
        .map(|(name, _)| name)
        .collect();
        if !set.is_empty() {
            return Err((
                tonic::Code::InvalidArgument,
                format!(
                    "{} applies only to tasks of the isolate runtime; task {} of job {job_id} \
                     runs on {}\nHint: drop {}",
                    set.join(", "),
                    task.name,
                    task.runtime,
                    match set.as_slice() {
                        [one] => format!("{one}"),
                        _ => "them".to_string(),
                    }
                ),
            ));
        }
        return Ok(());
    }
    if !command.is_empty() {
        return Err((
            tonic::Code::InvalidArgument,
            format!(
                "task {} of job {job_id} runs on the isolate runtime, which has no command to \
                 replace: a run executes the files you send (main.<ext>, else the first)\nHint: \
                 drop --command (command in the API), or use --tier vm with the VM task's own \
                 run.command",
                task.name
            ),
        ));
    }
    let tier = tier.trim();
    if !tier.is_empty() && !TIERS.contains(&tier) {
        return Err((
            tonic::Code::InvalidArgument,
            format!("unknown tier {tier:?}; use auto, isolate, or vm"),
        ));
    }
    if !tenant.is_empty() {
        crate::structs::isolate::validate_tenant_key(tenant)
            .map_err(|err| (tonic::Code::InvalidArgument, format!("tenant: {err}")))?;
    }
    let config =
        crate::structs::isolate::IsolateTaskConfig::from_value(&task.config).map_err(|err| {
            (
                tonic::Code::FailedPrecondition,
                format!("task {}: {err}", task.name),
            )
        })?;
    if tier == "vm" && crate::structs::isolate::vm_task_name(task, &config).is_none() {
        return Err((
            tonic::Code::FailedPrecondition,
            format!(
                "task {} of job {job_id} has no VM tier (no config.vm), so tier = \"vm\" has \
                 nowhere to run\nHint: add config.vm with a sandbox session task that has a run \
                 block, or run with tier auto or isolate",
                task.name
            ),
        ));
    }
    if !session_id.trim().is_empty()
        && config.lifetime != crate::structs::isolate::IsolateLifetime::Session
    {
        return Err((
            tonic::Code::FailedPrecondition,
            format!(
                "task {} of job {job_id} has config.lifetime = \"{}\", so it has no session \
                 isolates to run in\nHint: drop session_id (--session), or set config.lifetime \
                 = \"session\"",
                task.name,
                config.lifetime.as_str()
            ),
        ));
    }
    Ok(())
}

/// The most arguments one run of an isolate task takes
/// (`RunSessionRequest.args`).
pub const MAX_RUN_ARGS: usize = 64;

/// The most bytes a run's arguments take together.
pub const MAX_RUN_ARGS_BYTES: usize = 64 * 1024;

/// Check `RunSessionRequest.args`: isolate tasks only, at most
/// [`MAX_RUN_ARGS`] arguments and [`MAX_RUN_ARGS_BYTES`] together. Every
/// refusal is INVALID_ARGUMENT.
pub fn check_run_args(job_id: &str, task: &TaskConfig, args: &[String]) -> Result<(), String> {
    if args.is_empty() {
        return Ok(());
    }
    if !crate::structs::isolate::is_isolate_task(task) {
        return Err(format!(
            "args applies only to tasks of the isolate runtime; task {} of job {job_id} runs on \
             {}\nHint: drop args (--arg), or replace the command with --command",
            task.name, task.runtime
        ));
    }
    if args.len() > MAX_RUN_ARGS {
        return Err(format!(
            "{} args, over the {MAX_RUN_ARGS}-argument maximum for one run\nHint: pass large or \
             many values in --input or a --file instead",
            args.len()
        ));
    }
    let bytes: usize = args.iter().map(String::len).sum();
    if bytes > MAX_RUN_ARGS_BYTES {
        return Err(format!(
            "args take {bytes} bytes together, over the {MAX_RUN_ARGS_BYTES}-byte maximum for one \
             run\nHint: pass large values in --input or a --file instead"
        ));
    }
    Ok(())
}

/// The isolate task's VM task, when it has one and it is in `tasks`.
pub fn vm_task<'a>(task: &TaskConfig, tasks: &'a [TaskConfig]) -> Option<&'a TaskConfig> {
    let config = crate::structs::isolate::IsolateTaskConfig::of(task).ok()??;
    let name = crate::structs::isolate::vm_task_name(task, &config)?;
    tasks.iter().find(|candidate| candidate.name == name)
}

/// The deadline the manager gives a run of `task` on its node: the isolate's
/// `limits.wall`, or, when the run may go to the VM tier (`vm`, or `auto`
/// on a task with a VM task), the longer of that and the VM task's
/// `run.wall`.
pub fn run_wall(tier: &str, isolate_wall: Duration, vm: Option<&TaskConfig>) -> Duration {
    if tier.trim() == "isolate" {
        return isolate_wall;
    }
    let vm_wall = vm
        .and_then(|vm| vm.run.as_ref())
        .and_then(|run| run.resolve().ok())
        .map(|run| run.wall)
        .unwrap_or_default();
    isolate_wall.max(vm_wall)
}

/// The task an empty `task_name` picks for a run among a job's session
/// tasks: the only one that takes runs. An isolate task wins over its own
/// VM task (which takes runs too, through its `run` block, but exists to
/// serve the isolate task). `None` when the choice is ambiguous or empty.
pub fn default_run_task<'a>(
    session_tasks: &[&'a TaskConfig],
    all: &[TaskConfig],
) -> Vec<&'a TaskConfig> {
    let runnable: Vec<&TaskConfig> = session_tasks
        .iter()
        .copied()
        .filter(|task| crate::structs::run::takes_runs(task))
        .collect();
    let vm_tasks: Vec<String> = runnable
        .iter()
        .filter(|task| crate::structs::isolate::is_isolate_task(task))
        .filter_map(|task| vm_task(task, all).map(|vm| vm.name.clone()))
        .collect();
    runnable
        .into_iter()
        .filter(|task| !vm_tasks.contains(&task.name))
        .collect()
}

/// Whether the manager should refuse a run of `task` up front because its
/// runtime is disabled. An isolate run that may go to a VM (`tier` empty,
/// `auto`, or `vm`, on a task with a VM task) is the node's to decide: it
/// runs in the VM while the isolate runtime is disabled (plan §13).
pub fn refuse_disabled_up_front(task: &TaskConfig, tier: &str, has_vm: bool) -> bool {
    if !crate::structs::isolate::is_isolate_task(task) {
        return true;
    }
    !(has_vm && matches!(tier.trim(), "" | "auto" | "vm"))
}

#[cfg(test)]
mod isolates_test {
    use super::*;
    use serde_json::json;

    fn task_stats(job: &str, task: &str, cordon: &str, resident: u32) -> IsolateTaskStats {
        IsolateTaskStats {
            job_id: job.into(),
            task_name: task.into(),
            cordon: cordon.into(),
            resident,
            ..Default::default()
        }
    }

    fn host(cordon: &str, replica: u32) -> IsolateHostStats {
        IsolateHostStats {
            cordon: cordon.into(),
            replica,
            rss_bytes: 1 << 20,
            ..Default::default()
        }
    }

    fn task(name: &str, runtime: &str, config: serde_json::Value) -> TaskConfig {
        serde_json::from_value(json!({
            "name": name,
            "runtime": runtime,
            "config": config,
        }))
        .expect("task")
    }

    fn isolate_config(lifetime: &str, vm: bool) -> serde_json::Value {
        let mut config = json!({
            "image": "python",
            "lifetime": lifetime,
            "limits": { "cpu": "100ms", "wall": "30s" },
        });
        if vm {
            config["vm"] = json!({ "task": "machine" });
        }
        config
    }

    #[test]
    fn isolate_stats_ingest_replaces_and_empty_forgets() {
        let mut table = IsolateStatsTable::default();
        let now = Instant::now();
        table.ingest(
            "w-1",
            vec![task_stats("j", "run", "default", 3)],
            vec![host("default", 0)],
            now,
        );
        table.ingest(
            "w-1",
            vec![task_stats("j", "run", "default", 5)],
            vec![],
            now,
        );
        assert_eq!(table.node("w-1").map(|e| e.tasks[0].resident), Some(5));
        assert!(table.node("w-1").is_some_and(|e| e.hosts.is_empty()));
        table.ingest("w-1", vec![], vec![], now);
        assert!(
            table.node("w-1").is_none(),
            "an empty beat forgets the node"
        );
    }

    #[test]
    fn isolate_stats_filter_by_job_and_task_with_hosts_and_age() {
        let mut table = IsolateStatsTable::default();
        let start = Instant::now();
        table.ingest(
            "w-2",
            vec![
                task_stats("j", "run", "default", 7),
                task_stats("j", "other", "job:j", 1),
                task_stats("k", "run", "default", 9),
            ],
            vec![
                host("default", 1),
                host("default", 0),
                host("job:j", 0),
                host("shared", 0),
            ],
            start,
        );
        table.ingest(
            "w-1",
            vec![task_stats("k", "run", "default", 2)],
            vec![host("default", 0)],
            start,
        );
        let later = start + Duration::from_millis(1_500);

        let all = table.for_job("j", &["run".into(), "other".into()], later);
        assert_eq!(all.len(), 1, "w-1 reported no task of j");
        let node = &all[0];
        assert_eq!(node.node_id, "w-2");
        assert_eq!(node.age_ms, 1_500);
        let names: Vec<&str> = node.tasks.iter().map(|t| t.task_name.as_str()).collect();
        assert_eq!(names, ["other", "run"]);
        let hosts: Vec<(&str, u32)> = node
            .hosts
            .iter()
            .map(|h| (h.cordon.as_str(), h.replica))
            .collect();
        assert_eq!(
            hosts,
            [("default", 0), ("default", 1), ("job:j", 0)],
            "shared serves neither"
        );

        let one = table.for_job("j", &["other".into()], later);
        assert_eq!(one[0].tasks.len(), 1);
        assert_eq!(one[0].hosts.len(), 1);
        assert_eq!(one[0].hosts[0].cordon, "job:j");

        let k = table.for_job("k", &["run".into()], later);
        let nodes: Vec<&str> = k.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(nodes, ["w-1", "w-2"], "sorted by node");
    }

    #[test]
    fn isolate_stats_sweep_drops_stale_reports_at_most_once_a_minute() {
        let mut table = IsolateStatsTable::default();
        let start = Instant::now();
        table.ingest("old", vec![task_stats("j", "run", "d", 1)], vec![], start);
        assert!(table.sweep(start).is_empty());
        let later = start + ISOLATE_STATS_STALE_AFTER;
        table.ingest("fresh", vec![task_stats("j", "run", "d", 1)], vec![], later);
        assert_eq!(
            table.sweep(later + Duration::from_secs(1)),
            vec!["old".to_string()]
        );
        assert!(table.node("old").is_none());
        assert!(table.node("fresh").is_some());
        table.ingest(
            "stale-again",
            vec![task_stats("j", "run", "d", 1)],
            vec![],
            start,
        );
        assert!(table.sweep(later + Duration::from_secs(2)).is_empty());
    }

    #[test]
    fn isolate_stats_tasks_resolve_names_and_refuse_non_isolates() {
        let tasks = vec![
            task("run", "isolate", isolate_config("request", false)),
            task("web", "exec", json!({})),
        ];
        assert_eq!(
            stats_tasks("j", &tasks, "").unwrap(),
            vec!["run".to_string()]
        );
        assert_eq!(
            stats_tasks("j", &tasks, "run").unwrap(),
            vec!["run".to_string()]
        );
        let err = stats_tasks("j", &tasks, "web").unwrap_err();
        assert!(
            err.contains("not an isolate task") && err.contains("run"),
            "{err}"
        );
        let err = stats_tasks("j", &tasks[1..], "web").unwrap_err();
        assert!(err.contains("has no isolate task"), "{err}");
    }

    #[test]
    fn isolate_run_fields_are_checked_against_the_runtime() {
        let isolate = task("run", "isolate", isolate_config("request", false));
        let session = task("run", "isolate", isolate_config("session", true));
        let exec = task("web", "exec", json!({}));
        assert!(check_run_fields("j", &isolate, "", "", "", &[]).is_ok());
        assert!(check_run_fields("j", &isolate, "auto", "t-1", "", &[]).is_ok());

        let (code, message) = check_run_fields("j", &exec, "vm", "t", "", &[]).unwrap_err();
        assert_eq!(code, tonic::Code::InvalidArgument);
        assert!(
            message.starts_with("tier, tenant applies only"),
            "{message}"
        );

        let (code, message) =
            check_run_fields("j", &isolate, "", "", "", &["sh".into()]).unwrap_err();
        assert_eq!(code, tonic::Code::InvalidArgument);
        assert!(message.contains("no command to replace"), "{message}");

        let (code, _) = check_run_fields("j", &isolate, "gpu", "", "", &[]).unwrap_err();
        assert_eq!(code, tonic::Code::InvalidArgument);
        let (code, _) = check_run_fields("j", &isolate, "", "has space", "", &[]).unwrap_err();
        assert_eq!(code, tonic::Code::InvalidArgument);

        let (code, message) = check_run_fields("j", &isolate, "vm", "", "", &[]).unwrap_err();
        assert_eq!(code, tonic::Code::FailedPrecondition);
        assert!(message.contains("no VM tier"), "{message}");
        assert!(check_run_fields("j", &session, "vm", "", "", &[]).is_ok());

        let (code, message) =
            check_run_fields("j", &isolate, "", "", "run-abc.def", &[]).unwrap_err();
        assert_eq!(code, tonic::Code::FailedPrecondition);
        assert!(message.contains("lifetime = \"request\""), "{message}");
        assert!(check_run_fields("j", &session, "", "", "run-abc.def", &[]).is_ok());
    }

    #[test]
    fn isolate_run_args_are_bounded_and_isolate_only() {
        let isolate = task("run", "isolate", isolate_config("request", false));
        let exec = task("web", "exec", json!({}));
        assert!(check_run_args("j", &exec, &[]).is_ok());
        assert!(check_run_args("j", &isolate, &["--rows".into(), "10".into()]).is_ok());
        let err = check_run_args("j", &exec, &["x".into()]).unwrap_err();
        assert!(
            err.starts_with("args applies only to tasks of the isolate runtime"),
            "{err}"
        );
        let many: Vec<String> = (0..=MAX_RUN_ARGS).map(|i| i.to_string()).collect();
        assert!(check_run_args("j", &isolate, &many[..MAX_RUN_ARGS]).is_ok());
        let err = check_run_args("j", &isolate, &many).unwrap_err();
        assert!(
            err.starts_with("65 args, over the 64-argument maximum"),
            "{err}"
        );
        let big = vec!["x".repeat(MAX_RUN_ARGS_BYTES / 2); 2];
        assert!(check_run_args("j", &isolate, &big).is_ok());
        let bigger = vec!["x".repeat(MAX_RUN_ARGS_BYTES / 2 + 1); 2];
        let err = check_run_args("j", &isolate, &bigger).unwrap_err();
        assert!(err.contains("over the 65536-byte maximum"), "{err}");
    }

    #[test]
    fn isolate_task_is_the_default_run_task_over_its_vm_task() {
        let mut machine = task("machine", "sandbox", json!({}));
        machine.run =
            serde_json::from_value(json!({ "command": ["python3", "main.py"], "wall": "5m" })).ok();
        let all = vec![
            task("run", "isolate", isolate_config("request", true)),
            machine,
        ];
        let refs: Vec<&TaskConfig> = all.iter().collect();
        let picked = default_run_task(&refs, &all);
        let names: Vec<&str> = picked.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["run"]);

        // Without config.vm the two are unrelated and the choice is ambiguous.
        let mut unrelated = all.clone();
        unrelated[0] = task("run", "isolate", isolate_config("request", false));
        let refs: Vec<&TaskConfig> = unrelated.iter().collect();
        assert_eq!(default_run_task(&refs, &unrelated).len(), 2);
    }

    #[test]
    fn isolate_run_wall_covers_the_vm_tier_unless_pinned_to_the_isolate() {
        let mut machine = task("machine", "sandbox", json!({}));
        machine.run = serde_json::from_value(json!({ "command": ["x"], "wall": "5m" })).ok();
        let wall = Duration::from_secs(30);
        assert_eq!(run_wall("isolate", wall, Some(&machine)), wall);
        assert_eq!(run_wall("", wall, Some(&machine)), Duration::from_secs(300));
        assert_eq!(
            run_wall("vm", wall, Some(&machine)),
            Duration::from_secs(300)
        );
        assert_eq!(run_wall("auto", wall, None), wall);
    }

    #[test]
    fn isolate_runtime_disable_is_the_nodes_call_when_a_vm_can_take_the_run() {
        let isolate = task("run", "isolate", isolate_config("request", true));
        let exec = task("web", "exec", json!({}));
        assert!(refuse_disabled_up_front(&exec, "", false));
        assert!(!refuse_disabled_up_front(&isolate, "", true));
        assert!(!refuse_disabled_up_front(&isolate, "auto", true));
        assert!(!refuse_disabled_up_front(&isolate, "vm", true));
        assert!(refuse_disabled_up_front(&isolate, "isolate", true));
        assert!(refuse_disabled_up_front(&isolate, "", false));
    }
}
