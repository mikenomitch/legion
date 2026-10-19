//! `legion exec-key` — delegated exec minting keys
//! (docs/exec-and-actions.md, "Delegated minting keys").
//!
//! A platform registers an ES256 public key scoped to a job glob. Workers
//! then accept exec tickets the platform signs with the private half, for
//! matching jobs on non-host runtimes, with no manager call per session.
//! `legion exec-ticket sign` signs one offline.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{CreateExecKeyRequest, DeleteExecKeyRequest, ExecKey, ListExecKeysRequest};
use tabled::Tabled;

pub fn config() -> Command {
    let create_cmd = Command::new("create")
        .about("Register a platform's public key for minting exec tickets")
        .arg(
            Arg::new("jobs")
                .long("jobs")
                .value_name("PATTERN")
                .required(true)
                .help(
                    "Jobs the key's tickets may reach: 'agents-*' (names in default) or \
                     'agents/*' (namespace-glob/name-glob)",
                ),
        )
        .arg(
            Arg::new("public-key")
                .long("public-key")
                .value_name("FILE")
                .required(true)
                .value_hint(ValueHint::FilePath)
                .help("P-256 public key, PEM (openssl ec -in key.pem -pubout)"),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("A label for people"),
        )
        .arg(
            Arg::new("max-ttl")
                .long("max-ttl")
                .value_name("DURATION")
                .help("Ceiling on a ticket's redeem window (default: exec.tickets.maxTtl)"),
        )
        .arg(
            Arg::new("max-session")
                .long("max-session")
                .value_name("DURATION")
                .help("Ceiling on a ticket's session length (default: exec.tickets.maxSession)"),
        )
        .arg(
            Arg::new("idempotency-key")
                .long("idempotency-key")
                .value_name("KEY")
                .help("Retry-safe create: a repeat with the same key returns the same key"),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list")
        .about("List delegated exec keys")
        .arg(json_flag());

    let delete_cmd = Command::new("delete")
        .about("Delete a delegated exec key; workers stop accepting its tickets")
        .arg(Arg::new("id").value_name("ID").required(true))
        .arg(
            Arg::new("idempotency-key")
                .long("idempotency-key")
                .value_name("KEY")
                .help("Retry-safe delete"),
        )
        .arg(json_flag());

    Command::new("exec-key")
        .about("Manage delegated exec minting keys")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(create_cmd)
        .subcommand(list_cmd)
        .subcommand(delete_cmd)
}

#[instrument(skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("create", sub)) => create(sub).await,
        Some(("list", sub)) => list(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        _ => bail!("unknown exec-key subcommand"),
    }
}

fn key_json(key: &ExecKey) -> Value {
    json!({
        "keyId": key.id,
        "name": key.name,
        "jobs": key.jobs,
        "maxTtl": humantime::format_duration(std::time::Duration::from_secs(key.max_ttl_seconds)).to_string(),
        "maxSession": humantime::format_duration(std::time::Duration::from_secs(key.max_session_seconds)).to_string(),
        "createdBy": key.created_by,
        "createdAt": key.created_at,
        "publicKeyPem": key.public_key_pem,
    })
}

async fn create(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = matches
        .get_one::<String>("public-key")
        .context("Must provide --public-key")?;
    let public_key_pem = match std::fs::read_to_string(path) {
        Ok(pem) => pem,
        Err(err) => {
            return Err(output::fail_local(
                format,
                output::CliError {
                    code: "InvalidArgument".into(),
                    message: format!("could not read {path}: {err}"),
                    hint: None,
                    retryable: false,
                },
            ))
        }
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to create exec key",
        client
            .create_exec_key(CreateExecKeyRequest {
                name: matches
                    .get_one::<String>("name")
                    .cloned()
                    .unwrap_or_default(),
                jobs: matches
                    .get_one::<String>("jobs")
                    .cloned()
                    .unwrap_or_default(),
                public_key_pem,
                max_ttl: matches
                    .get_one::<String>("max-ttl")
                    .cloned()
                    .unwrap_or_default(),
                max_session: matches
                    .get_one::<String>("max-session")
                    .cloned()
                    .unwrap_or_default(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            })
            .await,
    )?
    .into_inner();
    let key = reply.key.context("the manager returned no key")?;
    if format.is_json() {
        let mut data = key_json(&key);
        data["deduplicated"] = json!(reply.deduplicated);
        output::print_success(&data);
    } else {
        println!("Exec key {} created for jobs {}.", key.id, key.jobs);
        println!(
            "Sign tickets with the private key and kid {}; workers accept them within one \
             heartbeat.",
            key.id
        );
    }
    Ok(())
}

#[derive(Tabled)]
struct ExecKeyPrint {
    id: String,
    name: String,
    jobs: String,
    max_ttl: String,
    max_session: String,
    created_by: String,
    created_at: String,
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let keys = output::resolve(
        format,
        "Failed to list exec keys",
        client.list_exec_keys(ListExecKeysRequest {}).await,
    )?
    .into_inner()
    .keys;
    if format.is_json() {
        output::print_success(&json!({
            "keys": keys.iter().map(key_json).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    if keys.is_empty() {
        println!("No exec keys.");
        return Ok(());
    }
    let rows: Vec<ExecKeyPrint> = keys
        .into_iter()
        .map(|key| ExecKeyPrint {
            id: key.id,
            name: key.name,
            jobs: key.jobs,
            max_ttl: humantime::format_duration(std::time::Duration::from_secs(
                key.max_ttl_seconds,
            ))
            .to_string(),
            max_session: humantime::format_duration(std::time::Duration::from_secs(
                key.max_session_seconds,
            ))
            .to_string(),
            created_by: key.created_by,
            created_at: key.created_at,
        })
        .collect();
    print_table(rows);
    Ok(())
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let id = matches.get_one::<String>("id").cloned().unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to delete exec key",
        client
            .delete_exec_key(DeleteExecKeyRequest {
                id: id.clone(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({ "keyId": id, "deduplicated": reply.deduplicated }));
    } else {
        println!("Exec key {id} deleted. Workers drop it, and end its sessions, on their next heartbeat.");
    }
    Ok(())
}
