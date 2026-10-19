//! `legion namespace` — namespace objects (docs/namespaces.md).
//!
//! A namespace is a name scope, a permission scope and a policy anchor. It is
//! not an isolation boundary: creating one moves no workload onto other
//! machines, networks or kernels (node pools, `gated` networking and the
//! `sandbox` runtime do that). Every subcommand takes `--json`/`-j` and emits
//! the standard [`crate::cmd::output`] envelope.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::namespace::NamespaceInput;
use crate::structs::{
    ApplyNamespaceRequest, DeleteNamespaceRequest, GetNamespaceRequest, ListNamespacesRequest,
    Namespace, NamespaceReach,
};
use std::path::Path;
use tabled::Tabled;

const ABOUT: &str = "Namespaces: the name scope, permission scope and policy anchor jobs live \
in (docs/namespaces.md). Not an isolation boundary: node pools, gated networking and the \
sandbox runtime isolate; a namespace separates names and scopes ACL rules.";

#[derive(Tabled)]
struct NamespaceRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "JOBS")]
    jobs: u32,
    #[tabled(rename = "DESCRIPTION")]
    description: String,
    #[tabled(rename = "CREATED")]
    created: String,
}

#[derive(Tabled)]
struct ReachRow {
    #[tabled(rename = "POLICY")]
    policy: String,
    #[tabled(rename = "RULE")]
    rule: u32,
    #[tabled(rename = "GRANTS")]
    grants: String,
    #[tabled(rename = "NAMESPACES")]
    namespaces: String,
}

pub fn config() -> Command {
    let name_arg = Arg::new("name")
        .required(true)
        .value_name("NAME")
        .help("Namespace name: lowercase letters, digits and '-', at most 63 characters");
    Command::new("namespace")
        .about(ABOUT)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List the namespaces you can see, with job counts")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one namespace, what it holds, and the ACL rules that reach it")
                .arg(name_arg.clone())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("create")
                .about("Create a namespace")
                .arg(name_arg.clone())
                .arg(
                    Arg::new("description")
                        .long("description")
                        .short('d')
                        .value_name("TEXT")
                        .help("Free-form description"),
                )
                .arg(
                    Arg::new("meta")
                        .long("meta")
                        .value_name("KEY=VALUE")
                        .action(ArgAction::Append)
                        .help("Free-form metadata; repeatable (`--meta owner=payments`)"),
                )
                .arg(dry_run_flag(
                    "Validate and show which ACL policies would reach it, without creating it",
                ))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("apply")
                .about("Create or update a namespace from a Nickel or JSON file")
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .required(true)
                        .value_name("FILE")
                        .help("Namespace file (.ncl or .json): { name, description, meta }")
                        .value_hint(ValueHint::FilePath),
                )
                .arg(dry_run_flag("Validate without writing"))
                .arg(json_flag()),
        )
        .subcommand(crate::cmd::namespace_policy::config())
        .subcommand(
            Command::new("delete")
                .about(
                    "Delete a namespace. Refused while it holds anything; --cascade stops and \
                     destroys its contents first, synchronously",
                )
                .arg(name_arg)
                .arg(
                    Arg::new("cascade")
                        .long("cascade")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Stop and destroy every job, then delete its volumes and snapshots, \
                             then the namespace (needs job:write there)",
                        ),
                )
                .arg(dry_run_flag(
                    "List what the namespace holds, without deleting anything",
                ))
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
}

fn dry_run_flag(help: &'static str) -> Arg {
    Arg::new("dry-run")
        .long("dry-run")
        .action(ArgAction::SetTrue)
        .help(help)
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("get", sub)) => get(sub).await,
        Some(("create", sub)) => create(sub).await,
        Some(("apply", sub)) => apply(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        Some(("policy", sub)) => crate::cmd::namespace_policy::handle(sub).await,
        _ => unreachable!("subcommand_required"),
    }
}

fn namespace_json(namespace: &Namespace) -> Value {
    let counts = namespace.counts.clone().unwrap_or_default();
    json!({
        "name": namespace.name,
        "description": namespace.description,
        "meta": namespace.meta,
        "createdAt": namespace.created_at,
        "updatedAt": namespace.updated_at,
        "counts": {
            "jobs": counts.jobs,
            "volumes": counts.volumes,
            "snapshots": counts.snapshots,
        },
    })
}

fn reach_json(reach: &[NamespaceReach]) -> Value {
    Value::Array(
        reach
            .iter()
            .map(|rule| {
                json!({
                    "policy": rule.policy,
                    "ruleIndex": rule.rule_index,
                    "grants": rule.grants,
                    "namespaces": rule.namespaces,
                })
            })
            .collect(),
    )
}

fn print_reach(reach: &[NamespaceReach], lead: &str) {
    if reach.is_empty() {
        return;
    }
    println!("{lead}");
    let rows = reach
        .iter()
        .map(|rule| ReachRow {
            policy: rule.policy.clone(),
            rule: rule.rule_index,
            grants: rule.grants.join(","),
            namespaces: if rule.namespaces.is_empty() {
                "(every namespace)".into()
            } else {
                rule.namespaces.join(",")
            },
        })
        .collect::<Vec<_>>();
    print_table(rows);
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list namespaces",
        client.list_namespaces(ListNamespacesRequest {}).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "namespaces": reply.namespaces.iter().map(namespace_json).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    if reply.namespaces.is_empty() {
        println!("No namespaces visible to this token.");
        return Ok(());
    }
    let rows = reply
        .namespaces
        .iter()
        .map(|namespace| NamespaceRow {
            name: namespace.name.clone(),
            jobs: namespace
                .counts
                .as_ref()
                .map(|c| c.jobs)
                .unwrap_or_default(),
            description: namespace.description.clone(),
            created: namespace.created_at.chars().take(10).collect(),
        })
        .collect::<Vec<_>>();
    print_table(rows);
    Ok(())
}

async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "get namespace",
        client.get_namespace(GetNamespaceRequest { name }).await,
    )?
    .into_inner();
    let namespace = reply.namespace.unwrap_or_default();
    if format.is_json() {
        let mut data = namespace_json(&namespace);
        data["reachingRules"] = reach_json(&reply.reaching_rules);
        data["usage"] = crate::cmd::namespace_policy::usage_json(&reply.usage);
        data["policies"] = Value::Array(
            reply
                .policies
                .iter()
                .map(crate::cmd::namespace_policy::status_json)
                .collect(),
        );
        output::print_success(&data);
        return Ok(());
    }
    let counts = namespace.counts.clone().unwrap_or_default();
    println!("{}", namespace.name);
    if !namespace.description.is_empty() {
        println!("  {}", namespace.description);
    }
    println!(
        "  {} job(s), {} volume(s), {} snapshot(s); created {}",
        counts.jobs, counts.volumes, counts.snapshots, namespace.created_at
    );
    let mut meta: Vec<_> = namespace.meta.iter().collect();
    meta.sort();
    for (key, value) in meta {
        println!("  {key}={value}");
    }
    if !reply.usage.is_empty() || !reply.policies.is_empty() {
        println!("\nCharged usage:");
        crate::cmd::namespace_policy::print_usage(&reply.usage);
    }
    if !reply.policies.is_empty() {
        println!("\nNamespace policies governing it (usage across every namespace they match):");
        for status in &reply.policies {
            crate::cmd::namespace_policy::print_status(status, "  ");
        }
    }
    print_reach(&reply.reaching_rules, "ACL rules that reach it:");
    Ok(())
}

fn parse_meta(
    matches: &ArgMatches,
    format: OutputFormat,
) -> R<std::collections::HashMap<String, String>> {
    let mut meta = std::collections::HashMap::new();
    for entry in matches.get_many::<String>("meta").into_iter().flatten() {
        let Some((key, value)) = entry.split_once('=') else {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "invalid_meta",
                    format!("--meta {entry:?} must be KEY=VALUE"),
                    None,
                ),
            ));
        };
        meta.insert(key.trim().to_string(), value.to_string());
    }
    Ok(meta)
}

async fn send_apply(format: OutputFormat, namespace: Namespace, dry_run: bool) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "apply namespace",
        client
            .apply_namespace(ApplyNamespaceRequest {
                namespace: Some(namespace),
                dry_run,
            })
            .await,
    )?
    .into_inner();
    let namespace = reply.namespace.unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({
            "dryRun": dry_run,
            "created": reply.created,
            "namespace": namespace_json(&namespace),
            "reachingRules": reach_json(&reply.reaching_rules),
        }));
        return Ok(());
    }
    let verb = match (dry_run, reply.created) {
        (true, true) => "Would create",
        (true, false) => "Would update",
        (false, true) => "Created",
        (false, false) => "Updated",
    };
    println!("{verb} namespace {}.", namespace.name);
    if reply.created {
        // The blast-radius report: the grants that take effect the moment
        // the namespace exists, including through globs written long ago.
        print_reach(
            &reply.reaching_rules,
            if dry_run {
                "These ACL rules would reach it:"
            } else {
                "These ACL rules reach it:"
            },
        );
    }
    Ok(())
}

async fn create(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    if let Err(err) = crate::structs::namespace::validate_new_namespace_name(&name) {
        return Err(output::fail_local(
            format,
            CliError::local("invalid_namespace", err, None),
        ));
    }
    let namespace = Namespace {
        name,
        description: matches
            .get_one::<String>("description")
            .cloned()
            .unwrap_or_default(),
        meta: parse_meta(matches, format)?,
        ..Default::default()
    };
    send_apply(format, namespace, matches.get_flag("dry-run")).await
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
                    "invalid_namespace_file",
                    format!("failed to evaluate {}: {err:#}", path.display()),
                    None,
                ),
            ));
        }
    };
    let input: NamespaceInput = match serde_json::from_str(&text) {
        Ok(input) => input,
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "invalid_namespace_file",
                    format!("{}: {err}", path.display()),
                    Some("a namespace file is { name, description, meta }"),
                ),
            ));
        }
    };
    if let Err(err) = crate::structs::namespace::validate_namespace_name(&input.name) {
        return Err(output::fail_local(
            format,
            CliError::local("invalid_namespace", err, None),
        ));
    }
    let namespace = Namespace {
        name: input.name,
        description: input.description,
        meta: input.meta.into_iter().collect(),
        ..Default::default()
    };
    send_apply(format, namespace, matches.get_flag("dry-run")).await
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let dry_run = matches.get_flag("dry-run");
    let cascade = matches.get_flag("cascade");
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "delete namespace",
        client
            .delete_namespace(DeleteNamespaceRequest {
                name: name.clone(),
                cascade,
                dry_run,
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            })
            .await,
    )?
    .into_inner();
    let contents = reply.contents.clone().unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({
            "name": name,
            "dryRun": dry_run,
            "deleted": reply.deleted,
            "deduplicated": reply.deduplicated,
            "contents": {
                "jobs": contents.jobs,
                "volumes": contents.volumes,
                "snapshots": contents.snapshots,
            },
            "steps": reply.steps,
        }));
        return Ok(());
    }
    if dry_run {
        if contents.jobs.is_empty() && contents.volumes.is_empty() && contents.snapshots.is_empty()
        {
            println!("Namespace {name} is empty; deleting it removes nothing else.");
            return Ok(());
        }
        println!("Namespace {name} holds:");
        for job in &contents.jobs {
            println!(
                "  job       {}",
                crate::structs::namespace::qualified_display(job)
            );
        }
        for volume in &contents.volumes {
            println!("  volume    {volume}");
        }
        for snapshot in &contents.snapshots {
            println!("  snapshot  {snapshot}");
        }
        println!("`legion namespace delete {name} --cascade` removes all of it.");
        return Ok(());
    }
    for step in &reply.steps {
        println!("{step}");
    }
    Ok(())
}
