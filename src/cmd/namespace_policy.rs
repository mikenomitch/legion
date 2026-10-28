//! `legion namespace policy` — namespace policies (docs/namespaces.md,
//! "Namespace policies").
//!
//! A namespace policy selects namespaces by glob and governs the jobs in
//! them: an aggregate quota per priority band, job and namespace counts,
//! allowed and default pools, allowed runtimes and host volumes. Reading
//! needs `acl:read` and writing `acl:write`; a tenant sees the limits that
//! bind it through `legion namespace get`. Every subcommand takes
//! `--json`/`-j` and emits the standard [`crate::cmd::output`] envelope.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    ApplyNamespacePolicyRequest, BandUsage, DeleteNamespacePolicyRequest,
    GetNamespacePolicyRequest, ListNamespacePoliciesRequest, NamespacePolicy,
    NamespacePolicyStatus,
};
use std::path::Path;
use tabled::Tabled;

#[derive(Tabled)]
struct PolicyRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "NAMESPACES")]
    namespaces: String,
    #[tabled(rename = "RULES")]
    rules: String,
    #[tabled(rename = "UPDATED")]
    updated: String,
}

#[derive(Tabled)]
struct UsageRow {
    #[tabled(rename = "BAND")]
    band: String,
    #[tabled(rename = "CPU (MHz)")]
    cpu: String,
    #[tabled(rename = "MEMORY (MiB)")]
    memory: String,
    #[tabled(rename = "CORES")]
    cores: String,
}

pub fn config() -> Command {
    let name_arg = Arg::new("name")
        .required(true)
        .value_name("POLICY")
        .help("Namespace policy name");
    Command::new("policy")
        .about(
            "Namespace policies: quota, counts, pools, runtimes and host volumes for every \
             namespace a glob matches (needs acl:read / acl:write)",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List namespace policies")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one policy, its limits, and usage across the namespaces it matches")
                .arg(name_arg.clone())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("apply")
                .about("Create or replace a namespace policy from a Nickel or JSON file")
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .required(true)
                        .value_name("FILE")
                        .help("Policy file (.ncl or .json)")
                        .value_hint(ValueHint::FilePath),
                )
                .arg(
                    Arg::new("force")
                        .long("force")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Apply even when the policy's autoscale leaves some autoscaled \
                             tasks' policies invalid; they are held until fixed",
                        ),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Validate and list what the policy would have refused among \
                             existing jobs and counts, without writing",
                        ),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a namespace policy")
                .arg(name_arg)
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("get", sub)) => get(sub).await,
        Some(("apply", sub)) => apply(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        _ => unreachable!("subcommand_required"),
    }
}

/// The policy object as JSON, with the stored bookkeeping beside it.
pub fn policy_json(policy: &NamespacePolicy) -> Value {
    let object: Value = serde_json::from_str(&policy.object_json).unwrap_or(Value::Null);
    json!({
        "name": policy.name,
        "namespaces": policy.namespaces,
        "description": policy.description,
        "object": object,
        "createdAt": policy.created_at,
        "updatedAt": policy.updated_at,
    })
}

pub fn usage_json(usage: &[BandUsage]) -> Value {
    Value::Array(
        usage
            .iter()
            .map(|band| {
                json!({
                    "band": band.band,
                    "cpu": band.cpu,
                    "memory": band.memory,
                    "cores": band.cores,
                    "allocations": band.allocations,
                })
            })
            .collect(),
    )
}

pub fn status_json(status: &NamespacePolicyStatus) -> Value {
    json!({
        "policy": status.policy.as_ref().map(policy_json),
        "usage": usage_json(&status.usage),
        "jobs": status.jobs,
        "namespaces": status.namespaces,
    })
}

/// `quota, maxJobs, pools`: which rules a policy sets, for the list view.
fn rules_summary(policy: &NamespacePolicy) -> String {
    let object: Value = serde_json::from_str(&policy.object_json).unwrap_or(Value::Null);
    [
        "quota",
        "maxJobs",
        "maxNamespaces",
        "pools",
        "runtimes",
        "hostVolumes",
    ]
    .iter()
    .filter(|key| object.get(**key).is_some())
    .copied()
    .collect::<Vec<_>>()
    .join(", ")
}

/// One line per limit with its usage: `high cpu 61500 / 64000 MHz`.
pub fn print_status(status: &NamespacePolicyStatus, indent: &str) {
    let Some(policy) = status.policy.as_ref() else {
        return;
    };
    let object: Value = serde_json::from_str(&policy.object_json).unwrap_or(Value::Null);
    println!(
        "{indent}{} (namespaces {})",
        policy.name,
        policy.namespaces.join(", ")
    );
    if let Some(quota) = object.get("quota").and_then(Value::as_object) {
        for (band, limits) in quota {
            let used = status.usage.iter().find(|usage| &usage.band == band);
            for (dimension, unit, value) in [
                ("cpu", "MHz", used.map(|u| u.cpu)),
                ("memory", "MiB", used.map(|u| u.memory)),
                ("cores", "cores", used.map(|u| u.cores)),
            ] {
                if let Some(cap) = limits.get(dimension).and_then(Value::as_f64) {
                    println!(
                        "{indent}  quota {band} {dimension}: {:.0} / {cap:.0} {unit}",
                        value.unwrap_or(0.0)
                    );
                }
            }
        }
    }
    if let Some(max) = object.get("maxJobs").and_then(Value::as_u64) {
        println!("{indent}  jobs: {} / {max}", status.jobs);
    }
    if let Some(max) = object.get("maxNamespaces").and_then(Value::as_u64) {
        println!("{indent}  namespaces: {} / {max}", status.namespaces);
    }
    for (key, label) in [("runtimes", "runtimes"), ("hostVolumes", "host volumes")] {
        if let Some(list) = object.get(key).and_then(Value::as_array) {
            let names: Vec<&str> = list.iter().filter_map(Value::as_str).collect();
            println!("{indent}  {label}: [{}]", names.join(", "));
        }
    }
    if let Some(pools) = object.get("pools") {
        if let Some(list) = pools.get("allowed").and_then(Value::as_array) {
            let names: Vec<&str> = list.iter().filter_map(Value::as_str).collect();
            println!("{indent}  pools: [{}]", names.join(", "));
        }
        if let Some(default) = pools.get("default").and_then(Value::as_str) {
            println!("{indent}  default pool: {default}");
        }
    }
}

/// The charged usage table for one namespace or policy.
pub fn print_usage(usage: &[BandUsage]) {
    if usage.is_empty() {
        println!("  No charged usage.");
        return;
    }
    print_table(
        usage
            .iter()
            .map(|band| UsageRow {
                band: band.band.clone(),
                cpu: format!("{:.0}", band.cpu),
                memory: format!("{:.0}", band.memory),
                cores: format!("{}", band.cores),
            })
            .collect::<Vec<_>>(),
    );
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list namespace policies",
        client
            .list_namespace_policies(ListNamespacePoliciesRequest {})
            .await,
    )?
    .into_inner();
    if format.is_json() {
        let policies: Vec<Value> = reply.policies.iter().map(policy_json).collect();
        output::print_success(&json!({ "policies": policies }));
        return Ok(());
    }
    if reply.policies.is_empty() {
        println!("No namespace policies.");
        return Ok(());
    }
    print_table(
        reply
            .policies
            .iter()
            .map(|policy| PolicyRow {
                name: policy.name.clone(),
                namespaces: policy.namespaces.join(", "),
                rules: rules_summary(policy),
                updated: policy.updated_at.clone(),
            })
            .collect::<Vec<_>>(),
    );
    Ok(())
}

async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "get namespace policy",
        client
            .get_namespace_policy(GetNamespacePolicyRequest { name })
            .await,
    )?
    .into_inner();
    let status = reply.status.unwrap_or_default();
    if format.is_json() {
        let mut data = status_json(&status);
        data["byNamespace"] = reply
            .by_namespace
            .iter()
            .map(|(namespace, usage)| (namespace.clone(), usage_json(&usage.usage)))
            .collect::<serde_json::Map<_, _>>()
            .into();
        output::print_success(&data);
        return Ok(());
    }
    print_status(&status, "");
    let mut namespaces: Vec<_> = reply.by_namespace.iter().collect();
    namespaces.sort_by(|a, b| a.0.cmp(b.0));
    for (namespace, usage) in namespaces {
        println!("\n{namespace}:");
        print_usage(&usage.usage);
    }
    Ok(())
}

async fn apply(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = Path::new(matches.get_one::<String>("file").expect("required"));
    let text = match crate::config_lang::eval_file(path) {
        Ok(text) => text,
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "invalid_namespace_policy_file",
                    format!("failed to evaluate {}: {err:#}", path.display()),
                    None,
                ),
            ));
        }
    };
    // Validated here too, so a typo fails before any network call.
    if let Err(err) = crate::structs::namespace_policy::NamespacePolicy::from_json(&text) {
        return Err(output::fail_local(
            format,
            CliError::local(
                "invalid_namespace_policy",
                format!("{}: {err}", path.display()),
                Some("see docs/namespaces.md, \"Namespace policies\""),
            ),
        ));
    }
    let dry_run = matches.get_flag("dry-run");
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "apply namespace policy",
        client
            .apply_namespace_policy(ApplyNamespacePolicyRequest {
                object_json: text,
                dry_run,
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
                force: matches.get_flag("force"),
            })
            .await,
    )?
    .into_inner();
    let policy = reply.policy.unwrap_or_default();
    if format.is_json() {
        let violations: Vec<Value> = reply
            .violations
            .iter()
            .map(|violation| {
                json!({
                    "namespace": violation.namespace,
                    "jobId": violation.job_id,
                    "rule": violation.rule,
                    "message": violation.message,
                })
            })
            .collect();
        output::print_success(&json!({
            "dryRun": dry_run,
            "created": reply.created,
            "deduplicated": reply.deduplicated,
            "policy": policy_json(&policy),
            "violations": violations,
        }));
        return Ok(());
    }
    let verb = match (dry_run, reply.created) {
        (true, true) => "Would create",
        (true, false) => "Would update",
        (false, true) => "Created",
        (false, false) => "Updated",
    };
    println!(
        "{verb} namespace policy {} (namespaces {}).",
        policy.name,
        policy.namespaces.join(", ")
    );
    if dry_run {
        if reply.violations.is_empty() {
            println!("Nothing existing would have been refused.");
        } else {
            println!("Had it been in force, it would have refused (admitted work keeps running):");
            for violation in &reply.violations {
                let what = if violation.job_id.is_empty() {
                    violation.namespace.clone()
                } else {
                    violation.job_id.clone()
                };
                println!("  [{}] {what}: {}", violation.rule, violation.message);
            }
        }
    }
    Ok(())
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "delete namespace policy",
        client
            .delete_namespace_policy(DeleteNamespacePolicyRequest {
                name: name.clone(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "name": name,
            "deleted": reply.deleted,
            "deduplicated": reply.deduplicated,
        }));
        return Ok(());
    }
    println!("Deleted namespace policy {name}.");
    Ok(())
}

#[cfg(test)]
mod namespace_policy_cmd_test {
    use super::*;

    #[test]
    fn config_parses_apply_with_dry_run() {
        let matches = config()
            .try_get_matches_from(["policy", "apply", "-f", "p.ncl", "--dry-run", "-j"])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "apply");
        assert_eq!(sub.get_one::<String>("file").unwrap(), "p.ncl");
        assert!(sub.get_flag("dry-run"));
        assert!(sub.get_flag("json"));
    }

    #[test]
    fn rules_summary_names_what_is_set() {
        let policy = NamespacePolicy {
            object_json: r#"{"name":"p","namespaces":["a"],"maxJobs":3,"runtimes":[]}"#.into(),
            ..Default::default()
        };
        assert_eq!(rules_summary(&policy), "maxJobs, runtimes");
    }
}
