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
    DeleteNetPolicyRequest, GetMeshCaRequest, GetNetPolicyObjectRequest, ListNetPoliciesRequest,
    NetPolicy, RotateMeshCaRequest, ShowNetPolicyRequest, TestNetPolicyRequest,
    UpsertNetPolicyRequest,
};
use serde::Serialize;
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
        .subcommand(
            job_scope(Command::new("doctor"))
                .about("Walk a task's network layers — policy, mode, datapath, handlers, TLS, gate — and name the first one that is wrong")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("handler")
                .about("External handlers: authz webhooks and proxies")
                .subcommand_required(true)
                .subcommand(
                    Command::new("test")
                        .about("Conformance-probe a handler: an authz webhook URL (http[s]://...) or a proxy at host:port")
                        .arg(Arg::new("target").required(true).help("Webhook URL or proxy host:port"))
                        .arg(
                            Arg::new("timeout")
                                .long("timeout")
                                .default_value("2s")
                                .help("Per-probe timeout"),
                        )
                        .arg(json_flag()),
                ),
        )
        .subcommand(
            Command::new("ca")
                .about("The cluster CA: the worker plane, the worker-to-manager channel, and certFrom = \"legion\"")
                .subcommand_required(true)
                .subcommand(
                    Command::new("show")
                        .about("Print the cluster CA trust bundle (PEM), its pin, and where a rotation stands")
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("rotate")
                        .about("Rotate the cluster CA in two steps: the new CA signs now, the old one stays trusted for 24h")
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
        Some(("doctor", sub)) => doctor(sub).await,
        Some(("handler", handler)) => match handler.subcommand() {
            Some(("test", sub)) => handler_test(sub).await,
            _ => unreachable!(),
        },
        Some(("ca", ca)) => match ca.subcommand() {
            Some(("show", sub)) => ca_show(sub).await,
            Some(("rotate", sub)) => ca_rotate(sub).await,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

/// One check of a handler probe.
#[derive(Serialize, Clone, Debug)]
pub(crate) struct ProbeCheck {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

/// What `net handler test` found.
#[derive(Serialize, Clone, Debug)]
pub(crate) struct HandlerProbe {
    pub kind: &'static str,
    pub target: String,
    pub ok: bool,
    pub latency_ms: u64,
    pub checks: Vec<ProbeCheck>,
}

/// Probe a handler the way the datapath would use it: an `authz` webhook
/// gets a sample facts payload and must answer a verdict; a `proxy` gets a
/// connection that starts with a PROXY v2 header and must keep it open.
/// Sends one synthetic request; never touches the cluster.
pub(crate) async fn probe_handler(target: &str, timeout: std::time::Duration) -> HandlerProbe {
    use crate::worker::netd::proxyproto::{self, tlv};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let started = std::time::Instant::now();
    let mut checks = Vec::new();
    if target.starts_with("http://") || target.starts_with("https://") {
        let body = json!({
            "allocation": 0,
            "job": "legion-handler-test",
            "task": "probe",
            "identity": "auto:legion-handler-test:default:probe",
            "labels": { "probe": "true" },
            "direction": "egress",
            "protocol": "http",
            "destination": "example.com:443",
            "host": "example.com",
            "port": 443,
            "method": "GET",
            "path": "/",
            "headers": [],
        });
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        let response = client
            .post(target)
            .timeout(timeout)
            .header("content-type", "application/json")
            .header("legion-handler", "authz")
            .body(body.to_string())
            .send()
            .await;
        let mut ok = false;
        match response {
            Ok(response) => {
                let status = response.status();
                checks.push(ProbeCheck {
                    name: "reachable",
                    ok: true,
                    detail: format!(
                        "answered {} in {} ms",
                        status.as_u16(),
                        started.elapsed().as_millis()
                    ),
                });
                checks.push(ProbeCheck {
                    name: "status_2xx",
                    ok: status.is_success(),
                    detail: format!("HTTP {}", status.as_u16()),
                });
                let text = response.text().await.unwrap_or_default();
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(value) => {
                        let verdict = value
                            .get("verdict")
                            .and_then(|v| v.as_str())
                            .map(|v| v.trim().to_ascii_lowercase());
                        let valid = matches!(verdict.as_deref(), Some("allow") | Some("deny"));
                        checks.push(ProbeCheck {
                            name: "verdict",
                            ok: valid,
                            detail: match verdict {
                                Some(v) if valid => format!("verdict = {v}"),
                                Some(v) => format!("verdict {v:?} is not allow|deny"),
                                None => "reply has no \"verdict\" field".into(),
                            },
                        });
                        if let Some(ttl) = value.get("ttl").and_then(|t| t.as_str()) {
                            let parsed = crate::structs::netpolicy::parse_duration(ttl).is_ok();
                            checks.push(ProbeCheck {
                                name: "ttl",
                                ok: parsed,
                                detail: if parsed {
                                    format!("ttl = {ttl}")
                                } else {
                                    format!("ttl {ttl:?} is not a duration")
                                },
                            });
                        }
                        ok = status.is_success() && valid;
                    }
                    Err(err) => checks.push(ProbeCheck {
                        name: "json",
                        ok: false,
                        detail: format!("reply is not JSON: {err}"),
                    }),
                }
            }
            Err(err) => checks.push(ProbeCheck {
                name: "reachable",
                ok: false,
                detail: format!("{err}"),
            }),
        }
        return HandlerProbe {
            kind: "authz",
            target: target.to_string(),
            ok,
            latency_ms: started.elapsed().as_millis() as u64,
            checks,
        };
    }

    let mut ok = false;
    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(target)).await {
        Ok(Ok(mut stream)) => {
            checks.push(ProbeCheck {
                name: "reachable",
                ok: true,
                detail: format!("connected in {} ms", started.elapsed().as_millis()),
            });
            let header = proxyproto::encode(
                None,
                "127.0.0.1:443".parse().ok(),
                &[
                    (tlv::IDENTITY, b"auto:legion-handler-test:default:probe"),
                    (tlv::DESTINATION, b"example.com:443"),
                    (tlv::ALLOCATION, b"0"),
                    (tlv::DIRECTION, b"egress"),
                ],
            );
            match stream.write_all(&header).await {
                Ok(()) => {
                    // A proxy that rejects the header closes at once; one that
                    // accepts it waits for payload.
                    let mut buf = [0u8; 1];
                    let accepted = !matches!(
                        tokio::time::timeout(
                            std::time::Duration::from_millis(500),
                            stream.read(&mut buf),
                        )
                        .await,
                        Ok(Ok(0)) | Ok(Err(_))
                    );
                    checks.push(ProbeCheck {
                        name: "proxy_protocol",
                        ok: accepted,
                        detail: if accepted {
                            "kept the connection open after a PROXY v2 header".into()
                        } else {
                            "closed the connection on the PROXY v2 header (set proxyProtocol = false, or enable PROXY protocol on the proxy)".into()
                        },
                    });
                    ok = accepted;
                }
                Err(err) => checks.push(ProbeCheck {
                    name: "proxy_protocol",
                    ok: false,
                    detail: format!("write failed: {err}"),
                }),
            }
        }
        Ok(Err(err)) => checks.push(ProbeCheck {
            name: "reachable",
            ok: false,
            detail: format!("{err}"),
        }),
        Err(_) => checks.push(ProbeCheck {
            name: "reachable",
            ok: false,
            detail: "dial timed out".into(),
        }),
    }
    HandlerProbe {
        kind: "proxy",
        target: target.to_string(),
        ok,
        latency_ms: started.elapsed().as_millis() as u64,
        checks,
    }
}

async fn handler_test(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let target = matches
        .get_one::<String>("target")
        .expect("required")
        .clone();
    let timeout = matches
        .get_one::<String>("timeout")
        .and_then(|t| crate::structs::netpolicy::parse_duration(t).ok())
        .unwrap_or(std::time::Duration::from_secs(2));
    let probe = probe_handler(&target, timeout).await;
    if format.is_json() {
        output::print_success(&probe);
    } else {
        println!(
            "{} {} {} ({} ms)",
            if probe.ok { "✓" } else { "✗" },
            probe.kind,
            probe.target,
            probe.latency_ms
        );
        for check in &probe.checks {
            println!(
                "  {} {:<15} {}",
                if check.ok { "✓" } else { "✗" },
                check.name,
                check.detail
            );
        }
    }
    if !probe.ok {
        return Err(output::fail_local(
            format,
            CliError::local(
                "handler_test_failed",
                "the handler did not pass the conformance probe",
                Some("fix the checks marked ✗ above; the datapath would fail this hop closed"),
            ),
        ));
    }
    Ok(())
}

async fn ca_show(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "cluster ca",
        client.get_mesh_ca(GetMeshCaRequest {}).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "certPem": reply.cert_pem,
            "bundlePem": reply.bundle_pem,
            "pin": reply.pin,
            "phase": reply.phase,
            "overlapUntil": reply.overlap_until,
            "previousCertPem": reply.previous_cert_pem,
            "createdAt": reply.created_at,
            "exists": !reply.cert_pem.is_empty(),
        }));
        return Ok(());
    }
    if reply.cert_pem.is_empty() {
        println!("no cluster CA yet: it is created with the first join token or node certificate");
        return Ok(());
    }
    // Human output goes to stderr so `legion net ca show > ca.pem` writes a
    // bundle a client can trust as is.
    eprintln!("Pin:     {}", reply.pin);
    if reply.phase == "overlap" {
        eprintln!(
            "Phase:   rotating. The new CA signs; the previous CA stays trusted until {}",
            reply.overlap_until
        );
        eprintln!("Bundle:  2 CAs (the signing CA first, then the previous one)");
    } else {
        eprintln!("Phase:   stable (one CA)");
    }
    print!("{}", reply.bundle_pem);
    Ok(())
}

async fn ca_rotate(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "rotate cluster ca",
        client.rotate_mesh_ca(RotateMeshCaRequest {}).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "certPem": reply.cert_pem,
            "createdAt": reply.created_at,
            "pin": reply.pin,
            "overlapUntil": reply.overlap_until,
        }));
        return Ok(());
    }
    println!(
        "Rotated the cluster CA. The new CA (pin {}) signs every certificate from now on.\n\
         The previous CA stays trusted until {}, so no worker-plane or mesh connection breaks;\n\
         nodes re-issue their certificates within one heartbeat. Join tokens minted before\n\
         the rotation work until then; mint new ones after.",
        reply.pin, reply.overlap_until
    );
    Ok(())
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

/// One layer of the `doctor` walk.
#[derive(Serialize, Clone)]
pub(crate) struct DoctorStep {
    layer: &'static str,
    status: &'static str,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

/// A node's datapath status for one allocation (`ShowNetPolicyReply.node_status_json`).
#[derive(serde::Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct NodeStatus {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    allocation_id: i64,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    capture_ready: bool,
    #[serde(default)]
    policy_hash: String,
    #[serde(default)]
    upstreams_bound: Vec<String>,
    #[serde(default)]
    ingress_bound: Vec<String>,
    #[serde(default)]
    resolver_ready: bool,
    #[serde(default)]
    proxy_ready: bool,
    #[serde(default)]
    gate_shut: bool,
    #[serde(default)]
    problems: Vec<String>,
    #[serde(default)]
    handlers: Vec<NodeHandlerStatus>,
    #[serde(default = "default_true")]
    tls_ready: bool,
    #[serde(default)]
    tls_error: String,
    #[serde(default)]
    mesh_identity: bool,
}

fn default_true() -> bool {
    true
}

/// One external hop as the node reports it.
#[derive(serde::Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct NodeHandlerStatus {
    #[serde(default)]
    source: String,
    #[serde(default)]
    handler: String,
    #[serde(default)]
    target: String,
    #[serde(default)]
    healthy: bool,
    #[serde(default)]
    last_error: String,
    #[serde(default)]
    calls: u64,
    #[serde(default)]
    errors: u64,
}

/// The ladder: policy resolved → mode and whether it is enforced → datapath
/// ready (namespace, listeners, resolver, proxy) → gate → problems. Stops
/// at the first failure with a verdict, a cause, and a fix hint — the
/// `legion service doctor` contract.
pub(crate) fn diagnose_network(
    job_id: &str,
    task_name: &str,
    reply: &crate::structs::ShowNetPolicyReply,
) -> Vec<DoctorStep> {
    let effective: EffectivePolicy =
        serde_json::from_str(&reply.effective_json).unwrap_or_default();
    let statuses: Vec<NodeStatus> = reply
        .node_status_json
        .iter()
        .filter_map(|json| serde_json::from_str(json).ok())
        .collect();
    let mut steps = Vec::new();

    // 1. Policy.
    if effective.is_empty() {
        steps.push(DoctorStep {
            layer: "policy",
            status: "info",
            detail: format!("no network policy applies to {job_id}/{task_name}; everything is allowed"),
            hint: Some("apply one with `legion net policy apply -f <file.ncl>` or add `network.egress` to the task".into()),
        });
    } else {
        let sources = if reply.matched_policies.is_empty() {
            "the task's own block".to_string()
        } else {
            format!(
                "stored {} + the task's own block",
                reply.matched_policies.join(", ")
            )
        };
        let summary = |chain: Option<&crate::structs::netpolicy::EffectiveChain>| match chain {
            Some(chain) => format!(
                "{} {} {} rules {} handlers",
                chain.protocol,
                chain.policy,
                chain.rules.len(),
                chain.handlers.len()
            ),
            None => "none".to_string(),
        };
        steps.push(DoctorStep {
            layer: "policy",
            status: "ok",
            detail: format!(
                "policy resolved from {sources}: egress {}, ingress {}",
                summary(effective.egress.as_ref()),
                summary(effective.ingress.as_ref())
            ),
            hint: None,
        });
        if let Some(ingress) = effective.ingress.as_ref() {
            if ingress.denies_by_default() && ingress.rules.iter().all(|r| r.rule.action == "deny")
            {
                steps.push(DoctorStep {
                    layer: "policy",
                    status: "warn",
                    detail: "the ingress chain denies everything, including this task's own health checks".into(),
                    hint: Some("add an allow rule for the checker (e.g. `when = { cidr = \"127.0.0.0/8\" }`) or the consumers".into()),
                });
            }
        }
    }

    // 2. Mode and enforcement.
    let mode = reply.effective_mode.as_str();
    let enforced = mode == "gated";
    steps.push(DoctorStep {
        layer: "mode",
        status: if enforced { "ok" } else { "info" },
        detail: if enforced {
            "network.mode is gated: the task's namespace has no route except the datapath (enforced)".into()
        } else {
            format!("network.mode is {mode}: egress policy is cooperative (upstreams, resolver, proxy env); a raw dial bypasses it")
        },
        hint: (!enforced && effective.egress.is_some())
            .then(|| "for an unbypassable boundary set `network.mode = \"gated\"` (Linux, container/exec) or impose it with a `require` policy".to_string()),
    });

    // 3. Datapath readiness, per running allocation.
    if statuses.is_empty() {
        steps.push(DoctorStep {
            layer: "datapath",
            status: if effective.is_empty() { "info" } else { "warn" },
            detail: "no running allocation has reported datapath status yet".into(),
            hint: Some("the node reports it on its next heartbeat; if the task is not running, that is the problem to fix first (`legion job status`)".into()),
        });
        return steps;
    }
    for status in &statuses {
        let alloc = if status.node_id.is_empty() {
            status.allocation_id.to_string()
        } else {
            format!("{} on {}", status.allocation_id, status.node_id)
        };
        if status.mode == "gated" && !status.capture_ready {
            steps.push(DoctorStep {
                layer: "datapath",
                status: "fail",
                detail: format!(
                    "allocation {alloc}: gated capture is not ready — the task has no network"
                ),
                hint: Some(status.problems.join("; ")),
            });
            return steps;
        }
        if !status.problems.is_empty() {
            steps.push(DoctorStep {
                layer: "datapath",
                status: "warn",
                detail: format!("allocation {alloc}: {}", status.problems.join("; ")),
                hint: None,
            });
        }
        if !effective.is_empty() && status.policy_hash.is_empty() {
            steps.push(DoctorStep {
                layer: "datapath",
                status: if status.mode == "gated" { "fail" } else { "warn" },
                detail: format!("allocation {alloc}: the node has not received the policy body yet (fail-closed while gated)"),
                hint: Some("the worker fetches it by hash on its next heartbeat; persistent means the manager could not serve FetchNetPolicy — check manager logs".into()),
            });
            return steps;
        }
        if !status.policy_hash.is_empty()
            && !effective.hash.is_empty()
            && status.policy_hash != effective.hash
        {
            steps.push(DoctorStep {
                layer: "datapath",
                status: "info",
                detail: format!("allocation {alloc}: enforcing an older policy ({}…) than the manager computes ({}…); converges on the next heartbeat", &status.policy_hash[..8.min(status.policy_hash.len())], &effective.hash[..8.min(effective.hash.len())]),
                hint: None,
            });
        }
        if status.gate_shut {
            steps.push(DoctorStep {
                layer: "gate",
                status: "warn",
                detail: format!("allocation {alloc}: the edge-boot gate is shut — every connection is denied until the placement is ratified"),
                hint: Some("normal for a `boot`-gated provisional start; see docs/edge-boot.md".into()),
            });
        }
        // TLS material: an ingress chain that terminates TLS needs its
        // files (or the mesh certificate) before it accepts anything.
        let kv_cert = effective
            .ingress
            .as_ref()
            .and_then(|c| c.tls.as_ref())
            .and_then(|t| t.cert_kv_path())
            .map(str::to_string);
        if !status.tls_ready {
            let hint = match &kv_cert {
                Some(path) => format!("put the chain followed by the private key, as one PEM value: legion kv put {path} \"$(cat fullchain.pem privkey.pem)\""),
                None => "for certPath/keyPath, put the files on the node (or stage them as artifacts); for certFrom = \"legion\", the node issues its certificate on its next heartbeat".into(),
            };
            steps.push(DoctorStep {
                layer: "tls",
                status: "fail",
                detail: format!(
                    "allocation {alloc}: ingress TLS is not ready — {}",
                    status.tls_error
                ),
                hint: Some(hint),
            });
            return steps;
        }
        // Ready, with an error: a KV renewal was rejected and the previous
        // certificate is still being served.
        if let (Some(path), false) = (&kv_cert, status.tls_error.is_empty()) {
            steps.push(DoctorStep {
                layer: "tls",
                status: "warn",
                detail: format!("allocation {alloc}: serving the previous certificate; the current kv {path} value was rejected — {}", status.tls_error),
                hint: Some(format!("fix the value at {path}; the node picks it up on its next heartbeat")),
            });
        }
        let wants_mesh = effective
            .ingress
            .as_ref()
            .and_then(|c| c.tls.as_ref())
            .is_some_and(|t| t.cert_from_mesh());
        if wants_mesh && !status.mesh_identity {
            steps.push(DoctorStep {
                layer: "tls",
                status: "warn",
                detail: format!("allocation {alloc}: the node holds no mesh certificate yet; mesh peers are refused until it does"),
                hint: Some("issued on the node's next heartbeat (IssueNodeCertificate); persistent means the manager could not create the mesh CA — check manager logs".into()),
            });
        }
        // External handlers: each hop the datapath has run, with its health.
        for hop in &status.handlers {
            steps.push(DoctorStep {
                layer: "handler",
                status: if hop.healthy { "ok" } else { "warn" },
                detail: format!(
                    "allocation {alloc}: {} {} → {} ({} calls, {} errors){}",
                    hop.source,
                    hop.handler,
                    hop.target,
                    hop.calls,
                    hop.errors,
                    if hop.last_error.is_empty() { String::new() } else { format!(" — last error: {}", hop.last_error) }
                ),
                hint: (!hop.healthy).then(|| "the chain fails closed on this hop unless it says onError = \"allow\"; `legion net handler test <target>` probes it directly".to_string()),
            });
        }
        if status.proxy_ready {
            steps.push(DoctorStep {
                layer: "host-guard",
                status: "info",
                detail: format!(
                    "allocation {alloc}: the egress proxy refuses loopback, this node's own addresses, link-local, and cloud metadata, whatever policy says; a refusal reads `Legion-Denied-By: host-guard`"
                ),
                hint: Some("only the node's operator can open one service: hostGuard.allow = [\"ip:port\"] in the worker config".into()),
            });
        }
        steps.push(DoctorStep {
            layer: "datapath",
            status: "ok",
            detail: format!(
                "allocation {alloc}: mode {}, {} upstream listener(s), {} ingress listener(s), resolver {}, proxy {}",
                status.mode,
                status.upstreams_bound.len(),
                status.ingress_bound.len(),
                if status.resolver_ready { "up" } else { "n/a" },
                if status.proxy_ready { "up" } else { "n/a" },
            ),
            hint: None,
        });
    }
    steps
}

/// The overall verdict: the worst status any step reached.
pub(crate) fn doctor_verdict(steps: &[DoctorStep]) -> &'static str {
    if steps.iter().any(|s| s.status == "fail") {
        "fail"
    } else if steps.iter().any(|s| s.status == "warn") {
        "warn"
    } else {
        "ok"
    }
}

async fn doctor(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let (job_id, task_name, node_pool) = job_scope_args(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "net doctor",
        client
            .show_net_policy(ShowNetPolicyRequest {
                job_id: job_id.clone(),
                task_name: task_name.clone(),
                node_pool,
            })
            .await,
    )?
    .into_inner();
    let steps = diagnose_network(&job_id, &task_name, &reply);
    let worst = doctor_verdict(&steps);
    if format.is_json() {
        output::print_success(&json!({
            "job": job_id,
            "task": task_name,
            "verdict": worst,
            "steps": steps,
        }));
        return Ok(());
    }
    for step in &steps {
        let mark = match step.status {
            "ok" => "✓",
            "info" => "·",
            "warn" => "!",
            _ => "✗",
        };
        println!("{mark} {:<9} {}", step.layer, step.detail);
        if let Some(hint) = step.hint.as_deref().filter(|h| !h.is_empty()) {
            println!("  → {hint}");
        }
    }
    Ok(())
}
