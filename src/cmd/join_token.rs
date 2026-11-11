//! `legion join-token` — manage worker join tokens.
//!
//! `create` (the default action) asks the manager to mint a join token and
//! prints the composite string a worker passes to `legion join <TOKEN>`. The
//! manager stores only a hash of the embedded secret, so tokens are listable
//! and revocable but never recoverable.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    CreateJoinTokenRequest, DeleteJoinTokenRequest, JoinTokenInfo, ListJoinTokensRequest,
};
use tabled::Tabled;

pub fn config() -> Command {
    let create_cmd = Command::new("create")
        .about("Create a join token for introducing a worker")
        .arg(
            Arg::new("type")
                .long("type")
                .value_name("TYPE")
                .help("Worker type the token introduces (default: default)"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("TTL")
                .help("Token lifetime, e.g. 1h or 30d (default: 24h; 'none' disables expiry)"),
        )
        .arg(
            Arg::new("description")
                .long("description")
                .value_name("TEXT")
                .help("Free-form note shown in `join-token list`"),
        )
        .arg(
            Arg::new("max-uses")
                .long("max-uses")
                .value_name("N")
                .value_parser(clap::value_parser!(u32).range(1..))
                .help(
                    "Admit at most N new nodes; a node restarting under its own name does not \
                     count (default: no limit)",
                ),
        )
        .arg(
            Arg::new("attr")
                .long("attr")
                .value_name("KEY=VALUE")
                .action(ArgAction::Append)
                .help(
                    "Node attribute stamped on every node that joins with the token, \
                     repeatable (--attr legion.nodegroup=workers-general). Wins over the \
                     worker's --attr and its worker type's attributes",
                ),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list")
        .about("List join tokens (secrets are never shown)")
        .arg(json_flag());

    let delete_cmd = Command::new("delete")
        .about("Revoke a join token by ID")
        .arg(Arg::new("id").value_name("ID").required(true))
        .arg(json_flag());

    Command::new("join-token")
        .about("Manage worker join tokens")
        .subcommand(create_cmd)
        .subcommand(list_cmd)
        .subcommand(delete_cmd)
        // Bare `legion join-token` behaves like `create` for backwards
        // compatibility with the original single-purpose command.
        .arg(
            Arg::new("type")
                .long("type")
                .value_name("TYPE")
                .help("Worker type the token introduces (default: default)"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("TTL")
                .help("Token lifetime, e.g. 1h or 30d (default: 24h; 'none' disables expiry)"),
        )
        .arg(json_flag())
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("create", arg_matches)) => create(arg_matches).await,
        Some(("list", arg_matches)) => list(OutputFormat::from_matches(arg_matches)).await,
        Some(("delete", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide token ID")?;
            delete(id, OutputFormat::from_matches(arg_matches)).await
        }
        None => create(sub_matches).await,
        _ => unreachable!(),
    }
}

async fn create(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let worker_type = arg_matches
        .get_one::<String>("type")
        .cloned()
        .unwrap_or_default();
    let ttl = arg_matches.get_one::<String>("ttl").cloned();
    let description = arg_matches
        .get_one::<String>("description")
        .cloned()
        .unwrap_or_default();
    // The bare `legion join-token` form has neither flag.
    let max_uses = arg_matches
        .try_get_one::<u32>("max-uses")
        .ok()
        .flatten()
        .copied();
    let mut attributes = std::collections::HashMap::new();
    for raw in arg_matches
        .try_get_many::<String>("attr")
        .ok()
        .flatten()
        .into_iter()
        .flatten()
    {
        let (key, value) = raw
            .split_once('=')
            .with_context(|| format!("--attr {raw:?} is not KEY=VALUE"))?;
        if attributes
            .insert(key.to_string(), value.to_string())
            .is_some()
        {
            bail!("--attr {key} is given twice");
        }
    }

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to create join token",
        client
            .create_join_token(tonic::Request::new(CreateJoinTokenRequest {
                worker_type,
                ttl,
                description,
                max_uses,
                attributes,
            }))
            .await,
    )?;
    let reply = response.into_inner();
    let info = reply.info.unwrap_or_default();

    if format.is_json() {
        let mut data = json!({
            "id": info.id,
            "token": reply.token,
            "workerType": info.worker_type,
            "expiresAt": info.expires_at,
            "maxUses": info.max_uses,
            "attributes": sorted(&info.attributes),
        });
        if !reply.warning.is_empty() {
            data["warning"] = json!(reply.warning);
        }
        output::print_success(&data);
    } else {
        if !reply.warning.is_empty() {
            eprintln!("Warning: {}\n", reply.warning);
        }
        println!("Join token created (id {}).\n", info.id);
        println!("Run this on the worker host:\n");
        println!("  legion join {}\n", reply.token);
        println!(
            "For a worker that keeps running, save the token to a 0600 file and pass \
             `--token-file <PATH>` instead: a token on the command line is visible to every \
             local user.\n"
        );
        if let Some(expires) = info.expires_at {
            println!("Expires: {expires}");
        }
        if let Some(max) = info.max_uses {
            println!("Admits at most {max} new node(s).");
        }
        if !info.attributes.is_empty() {
            println!("Stamps: {}", attributes_text(&info.attributes));
        }
    }
    Ok(())
}

fn sorted(
    attributes: &std::collections::HashMap<String, String>,
) -> std::collections::BTreeMap<&str, &str> {
    attributes
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

fn attributes_text(attributes: &std::collections::HashMap<String, String>) -> String {
    sorted(attributes)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Tabled)]
struct JoinTokenPrint {
    id: String,
    worker_type: String,
    description: String,
    uses: String,
    attributes: String,
    expires_at: String,
}

fn info_json(info: &JoinTokenInfo) -> Value {
    json!({
        "id": info.id,
        "workerType": info.worker_type,
        "description": info.description,
        "expiresAt": info.expires_at,
        "createdAt": info.created_at,
        "uses": info.uses,
        "maxUses": info.max_uses,
        "attributes": sorted(&info.attributes),
    })
}

async fn list(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to list join tokens",
        client
            .list_join_tokens(tonic::Request::new(ListJoinTokensRequest {}))
            .await,
    )?;
    let tokens = response.into_inner().tokens;

    if format.is_json() {
        output::print_success(&tokens.iter().map(info_json).collect::<Vec<_>>());
    } else if tokens.is_empty() {
        println!("No join tokens");
    } else {
        print_table(
            tokens
                .into_iter()
                .map(|info| JoinTokenPrint {
                    uses: match info.max_uses {
                        Some(max) => format!("{}/{max}", info.uses),
                        None => info.uses.to_string(),
                    },
                    attributes: attributes_text(&info.attributes),
                    id: info.id,
                    worker_type: info.worker_type,
                    description: info.description,
                    expires_at: info.expires_at.unwrap_or_else(|| "never".to_string()),
                })
                .collect(),
        );
    }
    Ok(())
}

async fn delete(id: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to delete join token",
        client
            .delete_join_token(tonic::Request::new(DeleteJoinTokenRequest {
                id: id.to_string(),
            }))
            .await,
    )?;
    let message = response.into_inner().message;
    if format.is_json() {
        output::print_success(&json!({ "message": message }));
    } else {
        println!("{message}");
    }
    Ok(())
}
