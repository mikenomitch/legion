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

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to create join token",
        client
            .create_join_token(tonic::Request::new(CreateJoinTokenRequest {
                worker_type,
                ttl,
                description,
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
    }
    Ok(())
}

#[derive(Tabled)]
struct JoinTokenPrint {
    id: String,
    worker_type: String,
    description: String,
    expires_at: String,
}

fn info_json(info: &JoinTokenInfo) -> Value {
    json!({
        "id": info.id,
        "workerType": info.worker_type,
        "description": info.description,
        "expiresAt": info.expires_at,
        "createdAt": info.created_at,
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
