//! `legion net` — network policy, upstreams, and the datapath
//! (docs/network-handlers.md).
//!
//! `net policy` manages stored policy objects and answers the two questions
//! an operator or an agent actually has: *what does this task get* (`show`,
//! the merged policy with every rule naming its source) and *would this be
//! allowed* (`test`, a verdict without sending anything). Every subcommand
//! takes `--json`/`-j` and emits the standard [`crate::cmd::output`] envelope.

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::netpolicy::{EffectivePolicy, NetPolicyObject};
use crate::structs::{
    DeleteNetPolicyRequest, GetNetPolicyObjectRequest, ListNetPoliciesRequest, NetPolicy,
    ShowNetPolicyRequest, TestNetPolicyRequest, UpsertNetPolicyRequest,
};
use std::path::Path;
use tabled::{Table, Tabled};

#[derive(Tabled)]
struct PolicyRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "ENFORCEMENT")]
    enforcement: String,
    #[tabled(rename = "PRIORITY")]
    priority: i64,
    #[tabled(rename = "SELECTOR")]
    selector: String,
    #[tabled(rename = "EGRESS")]
    egress: String,
    #[tabled(rename = "INGRESS")]
    ingress: String,
    #[tabled(rename = "MODE")]
    mode: String,
}

pub fn config() -> Command {
    let job_scope = |cmd: Command| {
        cmd.arg(
            Arg::new("job")
                .required(true)
                .help("Job id")
                .value_hint(ValueHint::Other),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .short('t')
                .help("Task name (optional when the job has one task)"),
        )
        .arg(
            Arg::new("pool")
                .long("pool")
                .help("Node pool to match selectors against (optional)"),
        )
    };
    Command::new("net")
        .about("Network policy, upstreams, and the datapath (docs/network-handlers.md)")
        .subcommand_required(true)
        .subcommand(
            Command::new("policy")
                .about("Stored network policy objects and a task's effective policy")
                .subcommand_required(true)
                .subcommand(
                    Command::new("apply")
                        .about("Create or replace a policy object from a Nickel or JSON file")
                        .arg(
                            Arg::new("file")
                                .long("file")
                                .short('f')
                                .required(true)
                                .help("Policy object file (.ncl or .json)")
                                .value_hint(ValueHint::FilePath),
                        )
                        .arg(
                            Arg::new("dry-run")
                                .long("dry-run")
                                .action(ArgAction::SetTrue)
                                .help("Validate and list affected allocations without writing"),
                        )
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("list")
                        .about("List stored policies in merge order")
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("describe")
                        .about("Print one stored policy object")
                        .arg(Arg::new("name").required(true).help("Policy name"))
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("delete")
                        .about("Delete a stored policy")
                        .arg(Arg::new("name").required(true).help("Policy name"))
                        .arg(json_flag()),
                )
                .subcommand(
                    job_scope(Command::new("show"))
                        .about("Show a task's effective policy, each rule naming its source")
                        .arg(json_flag()),
                )
                .subcommand(
                    job_scope(Command::new("test"))
                        .about("Dry-run a verdict: may this task reach --to, or be reached from --from?")
                        .arg(Arg::new("to").long("to").help("Egress destination: host[:port], IP, or URL"))
                        .arg(Arg::new("from").long("from").help("Ingress peer: host[:port] or IP"))
                        .arg(Arg::new("identity").long("identity").help("Peer workload identity (ingress)"))
                        .arg(Arg::new("method").long("method").help("HTTP method (request-time rules)"))
                        .arg(Arg::new("path").long("path").help("HTTP path (request-time rules)"))
                        .arg(json_flag()),
                ),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("policy", policy)) => match policy.subcommand() {
            Some(("apply", sub)) => apply(sub).await,
            Some(("list", sub)) => list(sub).await,
            Some(("describe", sub)) => describe(sub).await,
            Some(("delete", sub)) => delete(sub).await,
            Some(("show", sub)) => show(sub).await,
            Some(("test", sub)) => test(sub).await,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

/// Evaluate a `.ncl` (or read a `.json`) policy file to its JSON text.
fn load_policy_file(path: &Path) -> R<String> {
    crate::config_lang::eval_file(path)
        .with_context(|| format!("failed to evaluate {}", path.display()))
}

fn policy_json(policy: &NetPolicy) -> Value {
    let object: Value = serde_json::from_str(&policy.object_json).unwrap_or(Value::Null);
    json!({
        "name": policy.name,
        "priority": policy.priority,
        "enforcement": policy.enforcement,
        "description": policy.description,
        "createdAt": policy.created_at,
        "updatedAt": policy.updated_at,
        "object": object,
    })
}

fn summarize_selector(object: &NetPolicyObject) -> String {
    let mut parts = Vec::new();
    if !object.selector.node_pools.is_empty() {
        parts.push(format!("pools={}", object.selector.node_pools.join(",")));
    }
    if !object.selector.namespaces.is_empty() {
        parts.push(format!("ns={}", object.selector.namespaces.join(",")));
    }
    if !object.selector.runtimes.is_empty() {
        parts.push(format!("runtimes={}", object.selector.runtimes.join(",")));
    }
    if !object.selector.jobs.is_empty() {
        parts.push(format!("jobs={}", object.selector.jobs.join(",")));
    }
    if !object.selector.labels.is_empty() {
        let labels: Vec<String> = object
            .selector
            .labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        parts.push(format!("labels={}", labels.join(",")));
    }
    if parts.is_empty() {
        "(every task)".to_string()
    } else {
        parts.join(" ")
    }
}

fn summarize_chain(chain: Option<&crate::structs::netpolicy::ChainConfig>) -> String {
    match chain {
        None => "-".to_string(),
        Some(chain) => format!(
            "{} {} {}r {}h",
            chain.protocol_name(),
            chain.policy.as_deref().unwrap_or("allow"),
            chain.rules.len(),
            chain.handlers.len()
        ),
    }
}

fn policy_row(policy: &NetPolicy) -> PolicyRow {
    let object: Option<NetPolicyObject> = serde_json::from_str(&policy.object_json).ok();
    PolicyRow {
        name: policy.name.clone(),
        enforcement: policy.enforcement.clone(),
        priority: policy.priority,
        selector: object.as_ref().map(summarize_selector).unwrap_or_default(),
        egress: object
            .as_ref()
            .map(|o| summarize_chain(o.egress.as_ref()))
            .unwrap_or_default(),
        ingress: object
            .as_ref()
            .map(|o| summarize_chain(o.ingress.as_ref()))
            .unwrap_or_default(),
        mode: object
            .as_ref()
            .and_then(|o| o.mode.clone())
            .unwrap_or_else(|| "-".to_string()),
    }
}

async fn apply(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = Path::new(matches.get_one::<String>("file").expect("required"));
    let dry_run = matches.get_flag("dry-run");
    let object_json = match load_policy_file(path) {
        Ok(text) => text,
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local("invalid_policy_file", err.to_string(), None),
            ));
        }
    };
    // Validate locally first so a bad file fails with the field name before
    // a round trip; the manager validates again.
    let mut object: NetPolicyObject = match serde_json::from_str(&object_json) {
        Ok(object) => object,
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local("invalid_policy", format!("{}: {err}", path.display()), None),
            ));
        }
    };
    if let Err(err) = object.normalize_and_validate() {
        return Err(output::fail_local(
            format,
            CliError::local("invalid_policy", err.to_string(), None),
        ));
    }
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "apply net policy",
        client
            .upsert_net_policy(UpsertNetPolicyRequest {
                object_json: serde_json::to_string(&object)?,
                dry_run,
            })
            .await,
    )?
    .into_inner();
    let policy = reply.policy.unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({
            "dryRun": dry_run,
            "policy": policy_json(&policy),
            "affectedAllocations": reply.affected_allocations,
        }));
        return Ok(());
    }
    let verb = if dry_run { "Would apply" } else { "Applied" };
    println!(
        "{verb} policy {} ({}, priority {}) — selector: {}",
        policy.name,
        policy.enforcement,
        policy.priority,
        serde_json::from_str::<NetPolicyObject>(&policy.object_json)
            .map(|o| summarize_selector(&o))
            .unwrap_or_default()
    );
    if reply.affected_allocations.is_empty() {
        println!("No running allocation is affected right now.");
    } else {
        println!(
            "{} running allocation(s) affected on their next heartbeat:",
            reply.affected_allocations.len()
        );
        for entry in &reply.affected_allocations {
            println!("  {entry}");
        }
    }
    Ok(())
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list net policies",
        client.list_net_policies(ListNetPoliciesRequest {}).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "generation": reply.generation,
            "policies": reply.policies.iter().map(policy_json).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    if reply.policies.is_empty() {
        println!("No network policies. Apply one with `legion net policy apply -f <file.ncl>`.");
        return Ok(());
    }
    let rows: Vec<PolicyRow> = reply.policies.iter().map(policy_row).collect();
    println!("{}", Table::new(rows));
    Ok(())
}

async fn describe(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "describe net policy",
        client
            .get_net_policy_object(GetNetPolicyObjectRequest { name })
            .await,
    )?
    .into_inner();
    let policy = reply.policy.unwrap_or_default();
    if format.is_json() {
        output::print_success(&policy_json(&policy));
        return Ok(());
    }
    let object: Value = serde_json::from_str(&policy.object_json).unwrap_or(Value::Null);
    println!(
        "{} ({}, priority {}) updated {}",
        policy.name, policy.enforcement, policy.priority, policy.updated_at
    );
    if !policy.description.is_empty() {
        println!("{}", policy.description);
    }
    println!("{}", serde_json::to_string_pretty(&object)?);
    Ok(())
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    output::resolve(
        format,
        "delete net policy",
        client
            .delete_net_policy(DeleteNetPolicyRequest { name: name.clone() })
            .await,
    )?;
    if format.is_json() {
        output::print_success(&json!({ "deleted": name }));
    } else {
        println!("Deleted policy {name}; matching allocations lose it on their next heartbeat.");
    }
    Ok(())
}

fn job_scope_args(matches: &ArgMatches) -> (String, String, String) {
    (
        matches.get_one::<String>("job").expect("required").clone(),
        matches
            .get_one::<String>("task")
            .cloned()
            .unwrap_or_default(),
        matches
            .get_one::<String>("pool")
            .cloned()
            .unwrap_or_default(),
    )
}

async fn show(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let (job_id, task_name, node_pool) = job_scope_args(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "show net policy",
        client
            .show_net_policy(ShowNetPolicyRequest {
                job_id: job_id.clone(),
                task_name: task_name.clone(),
                node_pool,
            })
            .await,
    )?
    .into_inner();
    let effective: EffectivePolicy =
        serde_json::from_str(&reply.effective_json).unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({
            "job": job_id,
            "task": task_name,
            "declaredMode": reply.declared_mode,
            "effectiveMode": reply.effective_mode,
            "matchedPolicies": reply.matched_policies,
            "effective": effective,
        }));
        return Ok(());
    }
    print_effective(
        &job_id,
        &task_name,
        &reply.declared_mode,
        &reply.effective_mode,
        &reply.matched_policies,
        &effective,
    );
    Ok(())
}

fn print_effective(
    job_id: &str,
    task_name: &str,
    declared_mode: &str,
    effective_mode: &str,
    matched: &[String],
    effective: &EffectivePolicy,
) {
    let task = if task_name.is_empty() {
        String::new()
    } else {
        format!("/{task_name}")
    };
    println!("Effective network policy for {job_id}{task}");
    if declared_mode == effective_mode {
        println!("  mode: {effective_mode}");
    } else {
        println!("  mode: {effective_mode} (task declared {declared_mode})");
    }
    if matched.is_empty() {
        println!("  stored policies: none matched");
    } else {
        println!("  stored policies: {}", matched.join(" → "));
    }
    if effective.is_empty() {
        println!("  no policy applies; everything is allowed");
        return;
    }
    for (direction, chain) in [
        ("egress", effective.egress.as_ref()),
        ("ingress", effective.ingress.as_ref()),
    ] {
        let Some(chain) = chain else {
            continue;
        };
        println!(
            "  {direction}: protocol {} · fallthrough {} · onError {}{}",
            chain.protocol,
            chain.policy,
            chain.on_error,
            if chain.access_log {
                " · accessLog"
            } else {
                ""
            }
        );
        if let Some(tls) = chain.tls.as_ref() {
            println!("    tls: {}", tls.mode);
        }
        for (index, rule) in chain.rules.iter().enumerate() {
            let m = &rule.rule.matcher;
            let mut parts = Vec::new();
            if let Some(host) = m.host.as_deref() {
                parts.push(format!("host={host}"));
            }
            if let Some(port) = m.port {
                parts.push(format!("port={port}"));
            }
            if let Some(cidr) = m.cidr.as_deref() {
                parts.push(format!("cidr={cidr}"));
            }
            if let Some(path) = m.path.as_deref() {
                parts.push(format!("path={path}"));
            }
            if !m.method.is_empty() {
                parts.push(format!("method={}", m.method.join("|")));
            }
            for (k, v) in &m.header {
                parts.push(format!("header[{k}]={v}"));
            }
            if let Some(identity) = m.identity.as_deref() {
                parts.push(format!("identity={identity}"));
            }
            let reason = rule
                .rule
                .reason
                .as_deref()
                .map(|r| format!(" — {r}"))
                .unwrap_or_default();
            println!(
                "    {index:>2}. {:<5} {}  [{}]{reason}",
                rule.rule.action,
                if parts.is_empty() {
                    "(anything)".to_string()
                } else {
                    parts.join(" ")
                },
                rule.source
            );
        }
        for handler in &chain.handlers {
            let h = &handler.handler;
            let target = h
                .url
                .clone()
                .or(h.service.clone().map(|s| format!("service {s}")))
                .or(h.port.clone().map(|p| format!("port {p}")))
                .or(h.node.clone().map(|n| format!("node {n}")))
                .unwrap_or_default();
            println!(
                "    handler {} {}  timeout {} onError {}  [{}]",
                h.handler,
                target,
                h.timeout.as_deref().unwrap_or("2s"),
                h.on_error.as_deref().unwrap_or(&chain.on_error),
                handler.source
            );
        }
    }
    println!("  hash: {}", effective.hash);
}

async fn test(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let (job_id, task_name, node_pool) = job_scope_args(matches);
    let get = |key: &str| matches.get_one::<String>(key).cloned().unwrap_or_default();
    let (to, from) = (get("to"), get("from"));
    if to.is_empty() && from.is_empty() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "missing_target",
                "pass --to <destination> (egress) or --from <peer> (ingress)",
                None,
            ),
        ));
    }
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "test net policy",
        client
            .test_net_policy(TestNetPolicyRequest {
                job_id: job_id.clone(),
                task_name: task_name.clone(),
                node_pool,
                to: to.clone(),
                from: from.clone(),
                identity: get("identity"),
                method: get("method"),
                path: get("path"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "job": job_id,
            "task": task_name,
            "direction": reply.direction,
            "protocol": reply.protocol,
            "verdict": reply.verdict,
            "source": reply.source,
            "reason": reply.reason,
            "explanation": reply.explanation,
        }));
        return Ok(());
    }
    let mark = match reply.verdict.as_str() {
        "allow" => "✓",
        "deny" => "✗",
        _ => "?",
    };
    println!("{mark} {} {}", reply.verdict, reply.explanation);
    Ok(())
}
