//! `legion operator` — cluster-wide incident controls.
//!
//! `legion operator runtime disable|enable|status` is the switch for a task
//! runtime (docs/runtime-disable.md): when a sandbox boundary has a
//! published escape, stop every new start on that runtime, cluster-wide or
//! for named namespaces, optionally drain what is running, and turn it back
//! on after upgrading. Writing needs `operator:write`, reading
//! `operator:read`. Every subcommand takes `--json`/`-j` and emits the
//! standard [`crate::cmd::output`] envelope.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    ListRuntimeStatesRequest, RuntimeDrainSummary, RuntimeRunning, RuntimeState,
    RuntimeStateStatus, SetRuntimeEnabledReply, SetRuntimeEnabledRequest,
};
use tabled::Tabled;

#[derive(Tabled)]
struct StateRow {
    #[tabled(rename = "RUNTIME")]
    runtime: String,
    #[tabled(rename = "SCOPE")]
    scope: String,
    #[tabled(rename = "SINCE")]
    since: String,
    #[tabled(rename = "BY")]
    by: String,
    #[tabled(rename = "DRAIN")]
    drain: String,
    #[tabled(rename = "RUNNING")]
    running: String,
    #[tabled(rename = "BLOCKED")]
    blocked: String,
    #[tabled(rename = "REFUSED")]
    refused: String,
    #[tabled(rename = "REASON")]
    reason: String,
}

pub fn config() -> Command {
    let runtime_arg = Arg::new("runtime")
        .required(true)
        .value_name("RUNTIME")
        .help("Task runtime, as a jobspec's `runtime` names it (exec, container, sandbox, ...)");
    let namespace_arg = Arg::new("namespace")
        .long("namespace")
        .short('n')
        .value_name("GLOB")
        .action(ArgAction::Append)
        .value_delimiter(',')
        .help(
            "Limit to namespaces matching this glob (repeatable, or comma-separated); \
             without it the whole cluster",
        );
    let runtime = Command::new("runtime")
        .about(
            "Disable or enable a task runtime across the cluster: the switch for a runtime \
             with a published escape (needs operator:write / operator:read)",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("disable")
                .about(
                    "Refuse every new start of a runtime: placements, session starts, edge \
                     claims, restarts. Running work is untouched unless --drain",
                )
                .arg(runtime_arg.clone())
                .arg(namespace_arg.clone())
                .arg(
                    Arg::new("reason")
                        .long("reason")
                        .value_name("TEXT")
                        .help("Why (an advisory id, usually); shown in every refusal"),
                )
                .arg(
                    Arg::new("drain")
                        .long("drain")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Also take running work off the runtime: suspend what has a \
                             `suspend` block (checkpoints kept), gracefully stop the rest",
                        ),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("enable")
                .about(
                    "Enable a runtime again; blocked work places on its own and refused \
                     callers succeed on retry",
                )
                .arg(runtime_arg)
                .arg(namespace_arg)
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("status")
                .about("List disabled runtimes, with what is running on and waiting for each")
                .arg(json_flag()),
        );
    Command::new("operator")
        .about("Cluster-wide incident controls (needs the operator ACL resource)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(runtime)
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("runtime", sub)) => match sub.subcommand() {
            Some(("disable", sub)) => set(sub, false).await,
            Some(("enable", sub)) => set(sub, true).await,
            Some(("status", sub)) => status(sub).await,
            _ => unreachable!("subcommand_required"),
        },
        _ => unreachable!("subcommand_required"),
    }
}

/// `cluster`, or the globs joined with `, `.
fn scope(state: &RuntimeState) -> String {
    if state.namespaces.is_empty() {
        "cluster".to_string()
    } else {
        state.namespaces.join(", ")
    }
}

fn running_json(running: Option<&RuntimeRunning>) -> Value {
    let running = running.cloned().unwrap_or_default();
    json!({ "allocations": running.allocations, "sessions": running.sessions })
}

fn drained_json(drained: &RuntimeDrainSummary) -> Value {
    json!({
        "suspending": drained.suspending,
        "stopping": drained.stopping,
        "failed": drained.failed,
    })
}

/// The `data` of a disable or enable reply.
fn set_json(runtime: &str, enabled: bool, reply: &SetRuntimeEnabledReply) -> Value {
    let mut data = match reply.state.as_ref() {
        Some(state) => json!({
            "runtime": state.runtime,
            "enabled": false,
            "scope": scope(state),
            "namespaces": state.namespaces,
            "since": state.since,
            "by": state.by,
            "reason": state.reason,
            "drain": state.drain,
        }),
        None => json!({ "runtime": runtime, "enabled": true, "scope": "cluster" }),
    };
    data["requested"] = json!(if enabled { "enable" } else { "disable" });
    data["changed"] = json!(reply.changed);
    data["deduplicated"] = json!(reply.deduplicated);
    if reply.state.is_some() {
        data["running"] = running_json(reply.running.as_ref());
    }
    if let Some(drained) = reply.drained.as_ref() {
        data["drained"] = drained_json(drained);
    }
    data
}

fn status_json(status: &RuntimeStateStatus) -> Value {
    let state = status.state.clone().unwrap_or_default();
    json!({
        "runtime": state.runtime,
        "scope": scope(&state),
        "namespaces": state.namespaces,
        "since": state.since,
        "by": state.by,
        "reason": state.reason,
        "drain": state.drain,
        "running": running_json(status.running.as_ref()),
        "blocked": {
            "evals": status.blocked_evals,
            "edgeRefusals": status.edge_refusals,
        },
        "unawareNodes": status.unaware_nodes,
    })
}

async fn set(matches: &ArgMatches, enabled: bool) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let runtime = matches
        .get_one::<String>("runtime")
        .expect("required")
        .clone();
    let namespaces: Vec<String> = matches
        .get_many::<String>("namespace")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    let (reason, drain) = if enabled {
        (String::new(), false)
    } else {
        (
            matches
                .get_one::<String>("reason")
                .cloned()
                .unwrap_or_default(),
            matches.get_flag("drain"),
        )
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        if enabled {
            "enable runtime"
        } else {
            "disable runtime"
        },
        client
            .set_runtime_enabled(SetRuntimeEnabledRequest {
                runtime: runtime.clone(),
                enabled,
                namespaces,
                reason,
                drain,
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&set_json(&runtime, enabled, &reply));
        return Ok(());
    }
    match reply.state.as_ref() {
        None if reply.changed => println!("Runtime {runtime} enabled."),
        None => println!("Runtime {runtime} is not disabled; nothing changed."),
        Some(state) => {
            let verb = match (enabled, reply.changed) {
                (true, _) => "still disabled",
                (false, true) => "disabled",
                (false, false) => "already disabled",
            };
            println!(
                "Runtime {} {verb} for {} since {}{}.",
                state.runtime,
                scope(state),
                state.since,
                if state.reason.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", state.reason)
                }
            );
            let running = reply.running.unwrap_or_default();
            println!(
                "Running on it in scope: {} allocation(s), {} session(s){}.",
                running.allocations,
                running.sessions,
                if state.drain {
                    ""
                } else {
                    "; they keep running (pass --drain to take them off)"
                }
            );
            if let Some(drained) = reply.drained.as_ref() {
                println!(
                    "Drain: suspending {} instance(s), stopping {} allocation(s); each node \
                     drains its own sessions.",
                    drained.suspending, drained.stopping
                );
                for failure in &drained.failed {
                    println!("  not drained: {failure}");
                }
            }
        }
    }
    Ok(())
}

async fn status(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list runtime states",
        client
            .list_runtime_states(ListRuntimeStatesRequest {})
            .await,
    )?
    .into_inner();
    if format.is_json() {
        let disabled: Vec<Value> = reply.disabled.iter().map(status_json).collect();
        output::print_success(&json!({ "disabled": disabled }));
        return Ok(());
    }
    if reply.disabled.is_empty() {
        println!("No runtime is disabled.");
        return Ok(());
    }
    print_table(
        reply
            .disabled
            .iter()
            .map(|status| {
                let state = status.state.clone().unwrap_or_default();
                let running = status.running.unwrap_or_default();
                StateRow {
                    scope: scope(&state),
                    runtime: state.runtime,
                    since: state.since,
                    by: state.by,
                    drain: if state.drain { "yes" } else { "no" }.to_string(),
                    running: format!(
                        "{} alloc, {} session",
                        running.allocations, running.sessions
                    ),
                    blocked: format!("{} eval", status.blocked_evals),
                    refused: status.edge_refusals.to_string(),
                    reason: state.reason,
                }
            })
            .collect::<Vec<_>>(),
    );
    for status in &reply.disabled {
        if !status.unaware_nodes.is_empty() {
            println!(
                "\n{} ready node(s) run a worker that predates runtime disables and will not \
                 refuse edge starts: {}",
                status.unaware_nodes.len(),
                status.unaware_nodes.join(", ")
            );
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod operator_cmd_test {
    use super::*;

    #[test]
    fn config_parses_disable_with_scope_reason_and_drain() {
        let matches = config()
            .try_get_matches_from([
                "operator",
                "runtime",
                "disable",
                "sandbox",
                "--namespace",
                "tenants-*,ci",
                "-n",
                "batch",
                "--reason",
                "firecracker GHSA-1",
                "--drain",
                "-j",
            ])
            .unwrap();
        let (_, runtime) = matches.subcommand().unwrap();
        let (name, sub) = runtime.subcommand().unwrap();
        assert_eq!(name, "disable");
        assert_eq!(sub.get_one::<String>("runtime").unwrap(), "sandbox");
        let namespaces: Vec<&String> = sub.get_many::<String>("namespace").unwrap().collect();
        assert_eq!(namespaces, ["tenants-*", "ci", "batch"]);
        assert!(sub.get_flag("drain"));
        assert!(sub.get_flag("json"));
    }

    #[test]
    fn enable_takes_no_reason_or_drain() {
        assert!(config()
            .try_get_matches_from(["operator", "runtime", "enable", "sandbox", "--drain"])
            .is_err());
        assert!(config()
            .try_get_matches_from(["operator", "runtime", "enable", "sandbox"])
            .is_ok());
        assert!(config()
            .try_get_matches_from(["operator", "runtime", "status", "-j"])
            .is_ok());
    }

    #[test]
    fn replies_render_the_documented_json() {
        let state = RuntimeState {
            runtime: "container".into(),
            namespaces: vec![],
            reason: "CVE-2024-21626, runc < 1.1.12".into(),
            since: "2026-11-07T18:04:11Z".into(),
            by: "accessor:7f3c".into(),
            drain: false,
        };
        let data = set_json(
            "container",
            false,
            &SetRuntimeEnabledReply {
                state: Some(state.clone()),
                changed: true,
                running: Some(RuntimeRunning {
                    allocations: 412,
                    sessions: 9031,
                }),
                drained: None,
                deduplicated: false,
            },
        );
        assert_eq!(data["runtime"], "container");
        assert_eq!(data["scope"], "cluster");
        assert_eq!(data["running"]["allocations"], 412);
        assert_eq!(data["running"]["sessions"], 9031);
        assert_eq!(data["drain"], false);
        assert!(data.get("drained").is_none());

        let enabled = set_json(
            "container",
            true,
            &SetRuntimeEnabledReply {
                changed: true,
                ..Default::default()
            },
        );
        assert_eq!(enabled["enabled"], true);
        assert!(enabled.get("running").is_none());

        let listed = status_json(&RuntimeStateStatus {
            state: Some(RuntimeState {
                namespaces: vec!["tenants-*".into()],
                ..state
            }),
            blocked_evals: 12,
            edge_refusals: 340,
            running: None,
            unaware_nodes: vec![],
        });
        assert_eq!(listed["scope"], "tenants-*");
        assert_eq!(listed["blocked"]["evals"], 12);
        assert_eq!(listed["blocked"]["edgeRefusals"], 340);
    }
}
