//! `legion isolate` — observe the `isolate` runtime
//! (docs/task-runtime-isolate.md, "Observing").
//!
//! Isolates are never listed one by one: a node reports one aggregate per
//! isolate task on its heartbeat, and `stats` shows the latest from every
//! node (`Manager.GetIsolateStats`). `get` describes one session isolate by
//! asking the node its id names (`Manager.GetIsolate`).
//!
//! Runs on an isolate task are `legion session run` (with `--tier`,
//! `--tenant`, `--session`, `--fallback vm`).

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetIsolateReply, GetIsolateRequest, GetIsolateStatsReply, GetIsolateStatsRequest,
    IsolateHostStats, IsolateNodeStats, IsolateTaskStats,
};
use tabled::Tabled;

pub fn config() -> Command {
    let stats_cmd = Command::new("stats")
        .about("Show how many isolates each node holds for a job's isolate tasks")
        .long_about(
            "Show how many isolates each node holds for a job's isolate tasks, and what they \
             are doing: resident, active, paged out, and hibernated isolates, invocation and CPU \
             rates, VM runs, child VMs, stops by reason, refusals, and the host processes that \
             serve them.\n\nThe numbers come from each node's latest heartbeat (ageMs says how \
             old); counters are cumulative since the node's isolate host started, and rates are \
             over the node's last reporting interval. A node whose heartbeats go to another \
             manager is asked directly.\n\nWith -j the data is {\"task\": ..., \"nodes\": [...]} \
             for one isolate task, or {\"tasks\": [{\"task\": ..., \"nodes\": [...]}]} when the \
             job has several and --task names none.",
        )
        .arg(Arg::new("job").value_name("JOB").required(true))
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .help("One isolate task (default: every isolate task of the job)"),
        )
        .arg(json_flag());
    let get_cmd = Command::new("get")
        .about("Describe one session isolate, asking the node its id names")
        .long_about(
            "Describe one session isolate (an isolate task with config.lifetime = \"session\") \
             by asking the node its id names: its state (hot, paged_out, hibernated, or empty), \
             invocations, memory, state bytes, checkpoint, and child VMs. Managers keep no \
             record of isolates, so a node that cannot be reached answers UNAVAILABLE rather \
             than not found.",
        )
        .arg(Arg::new("id").value_name("SESSION_ID").required(true))
        .arg(Arg::new("on-node").long("on-node").value_name("NODE").help(
            "The node to ask, for an id that carries no node tag (one derived from an \
                     idempotency key)",
        ))
        .arg(json_flag());
    Command::new("isolate")
        .about("Observe the isolate runtime: per-node aggregates and session isolates")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(stats_cmd)
        .subcommand(get_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("stats", matches)) => stats(matches).await,
        Some(("get", matches)) => get(matches).await,
        _ => bail!("Unsupported isolate subcommand"),
    }
}

async fn stats(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let job_id = matches
        .get_one::<String>("job")
        .map(|job| crate::cmd::helpers::job_ref(matches, job))
        .transpose()?
        .unwrap_or_default();
    let task = matches
        .get_one::<String>("task")
        .cloned()
        .unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to read isolate stats",
        client
            .get_isolate_stats(tonic::Request::new(GetIsolateStatsRequest {
                job_id: job_id.clone(),
                task_name: task.clone(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&stats_json(&reply, &task));
        return Ok(());
    }
    let rows = stats_rows(&reply);
    if rows.is_empty() {
        println!(
            "No node reports isolates for {job_id}{} yet (nodes report on their heartbeat once \
             an envelope for the task is placed; see `legion job status {job_id}`)",
            if task.is_empty() {
                String::new()
            } else {
                format!("/{task}")
            }
        );
        return Ok(());
    }
    print_table(rows);
    for node in &reply.nodes {
        for stats in node.tasks.iter().filter(|stats| !stats.problem.is_empty()) {
            eprintln!(
                "legion: {} on {}: {}",
                stats.task_name, node.node_id, stats.problem
            );
        }
    }
    Ok(())
}

async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let session_id = matches.get_one::<String>("id").cloned().unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to read the session isolate",
        client
            .get_isolate(tonic::Request::new(GetIsolateRequest {
                session_id,
                node_id: matches
                    .get_one::<String>("on-node")
                    .cloned()
                    .unwrap_or_default(),
                ticket: String::new(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&isolate_json(&reply));
        return Ok(());
    }
    for line in isolate_lines(&reply) {
        println!("{line}");
    }
    Ok(())
}

fn text_or_null(text: &str) -> Value {
    if text.is_empty() {
        Value::Null
    } else {
        json!(text)
    }
}

fn host_json(host: &IsolateHostStats) -> Value {
    json!({
        "cordon": host.cordon,
        "replica": host.replica,
        "rssBytes": host.rss_bytes,
        "slots": host.slots,
        "slotsUsed": host.slots_used,
        "shards": host.shards,
        "engine": host.engine_version,
        "pid": host.pid,
        "startedUnixMs": host.started_unix_ms,
        "layout": host.layout,
        "confinement": host.confinement,
        "restarts": host.restarts,
    })
}

/// One node's row for one task: the task's aggregate, and the host
/// processes of its cordon on that node.
fn node_row_json(node: &IsolateNodeStats, stats: &IsolateTaskStats) -> Value {
    let hosts: Vec<&IsolateHostStats> = node
        .hosts
        .iter()
        .filter(|host| host.cordon == stats.cordon)
        .collect();
    json!({
        "node": node.node_id,
        "cordon": stats.cordon,
        "resident": stats.resident,
        "active": stats.active,
        "pagedOut": stats.paged_out,
        "hibernated": stats.hibernated,
        "queued": stats.queued,
        "invocations": stats.invocations,
        "invocationsPerSec": stats.invocations_per_second,
        "cpuMicros": stats.cpu_micros,
        "cpuSecondsPerSec": stats.cpu_seconds_per_second,
        "vmRuns": stats.vm_runs,
        "vmRunsPerMin": stats.vm_runs_per_minute,
        "childVms": stats.child_vms,
        "stopped": stats.stopped,
        "limits": stats.limits,
        "refusals": stats.refusals,
        "memoryBytes": stats.memory_bytes,
        "reservationBytes": stats.reservation_bytes,
        "hibernations": stats.hibernations,
        "problem": text_or_null(&stats.problem),
        "hostRssBytes": hosts.iter().map(|host| host.rss_bytes).sum::<u64>(),
        "hosts": hosts.iter().map(|host| host_json(host)).collect::<Vec<_>>(),
        "ageMs": node.age_ms,
    })
}

/// The JSON data of `legion isolate stats`: `{"task", "nodes"}` for one
/// isolate task (the one `--task` named, or the only one the nodes report),
/// `{"tasks": [{"task", "nodes"}]}` for several.
pub fn stats_json(reply: &GetIsolateStatsReply, task: &str) -> Value {
    let mut by_task: std::collections::BTreeMap<String, Vec<Value>> = Default::default();
    if !task.is_empty() {
        by_task.entry(task.to_string()).or_default();
    }
    for node in &reply.nodes {
        for stats in &node.tasks {
            by_task
                .entry(stats.task_name.clone())
                .or_default()
                .push(node_row_json(node, stats));
        }
    }
    let entry = |(task, nodes): (String, Vec<Value>)| json!({ "task": task, "nodes": nodes });
    match by_task.len() {
        0 => json!({ "task": Value::Null, "nodes": [] }),
        1 => entry(by_task.into_iter().next().expect("one task")),
        _ => json!({ "tasks": by_task.into_iter().map(entry).collect::<Vec<_>>() }),
    }
}

#[derive(Tabled)]
pub struct StatsRow {
    #[tabled(rename = "NODE")]
    node: String,
    #[tabled(rename = "TASK")]
    task: String,
    #[tabled(rename = "CORDON")]
    cordon: String,
    #[tabled(rename = "RESIDENT")]
    resident: u32,
    #[tabled(rename = "ACTIVE")]
    active: u32,
    #[tabled(rename = "PAGED-OUT")]
    paged_out: u32,
    #[tabled(rename = "HIBERNATED")]
    hibernated: u32,
    #[tabled(rename = "QUEUED")]
    queued: u32,
    #[tabled(rename = "INV/S")]
    invocations: String,
    #[tabled(rename = "CPU-S/S")]
    cpu: String,
    #[tabled(rename = "VM/MIN")]
    vm_runs: String,
    #[tabled(rename = "CHILD-VMS")]
    child_vms: u32,
    #[tabled(rename = "STOPPED")]
    stopped: u64,
    #[tabled(rename = "REFUSED")]
    refusals: u64,
    #[tabled(rename = "HOST-RSS")]
    host_rss: String,
    #[tabled(rename = "AGE")]
    age: String,
}

/// One table row per node and task.
pub fn stats_rows(reply: &GetIsolateStatsReply) -> Vec<StatsRow> {
    let mut rows = Vec::new();
    for node in &reply.nodes {
        for stats in &node.tasks {
            let rss: u64 = node
                .hosts
                .iter()
                .filter(|host| host.cordon == stats.cordon)
                .map(|host| host.rss_bytes)
                .sum();
            rows.push(StatsRow {
                node: node.node_id.clone(),
                task: stats.task_name.clone(),
                cordon: stats.cordon.clone(),
                resident: stats.resident,
                active: stats.active,
                paged_out: stats.paged_out,
                hibernated: stats.hibernated,
                queued: stats.queued,
                invocations: format!("{:.1}", stats.invocations_per_second),
                cpu: format!("{:.2}", stats.cpu_seconds_per_second),
                vm_runs: format!("{:.1}", stats.vm_runs_per_minute),
                child_vms: stats.child_vms,
                stopped: stats.stopped.values().sum(),
                refusals: stats.refusals,
                host_rss: format!("{}MiB", crate::cmd::helpers::usage_display::mib(rss)),
                age: format!("{:.1}s", node.age_ms as f64 / 1000.0),
            });
        }
    }
    rows
}

/// The JSON data of `legion isolate get`.
pub fn isolate_json(reply: &GetIsolateReply) -> Value {
    json!({
        "sessionId": reply.session_id,
        "node": reply.node_id,
        "job": reply.job_id,
        "task": reply.task_name,
        "tenant": text_or_null(&reply.tenant),
        "state": reply.state,
        "startedUnixMs": reply.started_unix_ms,
        "lastActiveUnixMs": reply.last_active_unix_ms,
        "invocations": reply.invocations,
        "memoryBytes": reply.memory_bytes,
        "stateBytes": reply.state_bytes,
        "checkpointGeneration": reply.checkpoint_generation,
        "checkpointBytes": reply.checkpoint_bytes,
        "children": reply.children.iter().map(|child| json!({
            "sessionId": child.session_id,
            "task": child.task_name,
            "node": child.node_id,
            "state": child.state,
            "reason": child.reason,
        })).collect::<Vec<_>>(),
    })
}

fn unix_ms_text(unix_ms: i64) -> String {
    if unix_ms <= 0 {
        return "-".into();
    }
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(unix_ms)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "-".into())
}

/// The text form of `legion isolate get`.
pub fn isolate_lines(reply: &GetIsolateReply) -> Vec<String> {
    let mut lines = vec![
        format!("Session isolate {}", reply.session_id),
        format!("  job/task:      {}/{}", reply.job_id, reply.task_name),
        format!("  node:          {}", reply.node_id),
        format!("  state:         {}", reply.state),
        format!(
            "  tenant:        {}",
            if reply.tenant.is_empty() {
                "-"
            } else {
                &reply.tenant
            }
        ),
        format!("  started:       {}", unix_ms_text(reply.started_unix_ms)),
        format!(
            "  last active:   {}",
            unix_ms_text(reply.last_active_unix_ms)
        ),
        format!("  invocations:   {}", reply.invocations),
        format!(
            "  memory:        {} MiB (state {} bytes)",
            crate::cmd::helpers::usage_display::mib(reply.memory_bytes),
            reply.state_bytes
        ),
    ];
    if reply.checkpoint_generation > 0 {
        lines.push(format!(
            "  checkpoint:    generation {}, {} bytes",
            reply.checkpoint_generation, reply.checkpoint_bytes
        ));
    }
    for child in &reply.children {
        lines.push(format!(
            "  child:         {} ({}, {} on {})",
            child.session_id, child.task_name, child.state, child.node_id
        ));
    }
    lines
}

#[cfg(test)]
mod isolate_cmd_test {
    use super::*;

    fn task(name: &str, cordon: &str, resident: u32) -> IsolateTaskStats {
        IsolateTaskStats {
            job_id: "code-runner".into(),
            task_name: name.into(),
            cordon: cordon.into(),
            resident,
            active: 3,
            invocations_per_second: 8410.0,
            stopped: [("limits".to_string(), 2u64), ("trap".to_string(), 1)]
                .into_iter()
                .collect(),
            ..Default::default()
        }
    }

    fn reply(tasks: Vec<IsolateTaskStats>) -> GetIsolateStatsReply {
        GetIsolateStatsReply {
            nodes: vec![IsolateNodeStats {
                node_id: "w-17".into(),
                tasks,
                hosts: vec![
                    IsolateHostStats {
                        cordon: "ns:default".into(),
                        replica: 0,
                        rss_bytes: 3 << 20,
                        ..Default::default()
                    },
                    IsolateHostStats {
                        cordon: "ns:default".into(),
                        replica: 1,
                        rss_bytes: 1 << 20,
                        ..Default::default()
                    },
                    IsolateHostStats {
                        cordon: "job:code-runner".into(),
                        rss_bytes: 7 << 20,
                        ..Default::default()
                    },
                ],
                age_ms: 1200,
            }],
        }
    }

    #[test]
    fn isolate_stats_json_is_one_task_or_several_test() {
        let one = stats_json(&reply(vec![task("run", "ns:default", 48213)]), "");
        assert_eq!(one["task"], "run");
        let node = &one["nodes"][0];
        assert_eq!(node["node"], "w-17");
        assert_eq!(node["resident"], 48213);
        assert_eq!(node["invocationsPerSec"], 8410.0);
        assert_eq!(node["hostRssBytes"], 4u64 << 20, "both default replicas");
        assert_eq!(node["hosts"].as_array().map(Vec::len), Some(2));
        assert_eq!(node["stopped"]["limits"], 2);
        assert_eq!(node["problem"], Value::Null);
        assert_eq!(node["ageMs"], 1200);

        let several = stats_json(
            &reply(vec![
                task("run", "ns:default", 1),
                task("web", "job:code-runner", 2),
            ]),
            "",
        );
        assert!(several.get("task").is_none());
        let tasks = several["tasks"].as_array().expect("tasks");
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[1]["task"], "web");
        assert_eq!(tasks[1]["nodes"][0]["hostRssBytes"], 7u64 << 20);

        // A named task nobody reports yet: its name and no nodes.
        let empty = stats_json(&GetIsolateStatsReply::default(), "run");
        assert_eq!(empty, json!({ "task": "run", "nodes": [] }));
        let unknown = stats_json(&GetIsolateStatsReply::default(), "");
        assert_eq!(unknown, json!({ "task": null, "nodes": [] }));
    }

    #[test]
    fn isolate_stats_table_has_a_row_per_node_and_task_test() {
        let rows = stats_rows(&reply(vec![
            task("run", "ns:default", 5),
            task("web", "x", 1),
        ]));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].stopped, 3);
        assert_eq!(rows[0].host_rss, "4MiB");
        assert_eq!(rows[1].host_rss, "0MiB");
        assert_eq!(rows[0].age, "1.2s");
    }

    #[test]
    fn isolate_get_renders_json_and_text_test() {
        let reply = GetIsolateReply {
            session_id: "run-abcd.xyz".into(),
            node_id: "w-17".into(),
            job_id: "code-runner".into(),
            task_name: "run".into(),
            state: "hibernated".into(),
            checkpoint_generation: 3,
            checkpoint_bytes: 4096,
            ..Default::default()
        };
        let data = isolate_json(&reply);
        assert_eq!(data["state"], "hibernated");
        assert_eq!(data["tenant"], Value::Null);
        assert_eq!(data["checkpointGeneration"], 3);
        let lines = isolate_lines(&reply);
        assert!(lines.iter().any(|line| line.contains("generation 3")));
        assert!(lines.iter().any(|line| line.contains("tenant:        -")));
    }

    #[test]
    fn isolate_commands_parse_test() {
        let matches = config()
            .try_get_matches_from(["isolate", "stats", "code-runner", "--task", "run", "-j"])
            .expect("parses");
        let (name, stats) = matches.subcommand().expect("stats");
        assert_eq!(name, "stats");
        assert_eq!(
            stats.get_one::<String>("task").map(String::as_str),
            Some("run")
        );
        assert!(config()
            .try_get_matches_from(["isolate", "get", "run-x", "--on-node", "w-1"])
            .is_ok());
        assert!(config().try_get_matches_from(["isolate", "get"]).is_err());
    }
}
