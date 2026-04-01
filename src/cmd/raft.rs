use crate::cmd::helpers::{connect_manager_client, friendly_status};
use crate::prelude::*;

pub fn config() -> Command {
    Command::new("raft")
        .about("Inspect and operate manager Raft control-plane state")
        .subcommand(Command::new("status").about("Show local manager raft status"))
        .subcommand(
            Command::new("history")
                .about("Show recent raft events")
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
                        .value_parser(clap::value_parser!(u32))
                        .default_value("25")
                        .help("Maximum number of events to return"),
                ),
        )
        .subcommand(
            Command::new("command")
                .about("Issue a raft command")
                .arg(
                    Arg::new("name")
                        .required(true)
                        .value_name("COMMAND")
                        .help("Command name (for example trigger-election, transfer-leader)"),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .value_name("NODE_ID")
                        .help("Optional target node id"),
                )
                .arg(
                    Arg::new("actor")
                        .long("actor")
                        .value_name("ACTOR")
                        .default_value("cli")
                        .help("Actor tag written into raft history"),
                ),
        )
        .subcommand(
            Command::new("join-token")
                .about("Issue a raft manager join token")
                .arg(
                    Arg::new("actor")
                        .long("actor")
                        .value_name("ACTOR")
                        .default_value("cli")
                        .help("Actor tag written into raft history"),
                ),
        )
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;
    match sub_matches.subcommand() {
        Some(("status", _)) => {
            let reply = client
                .get_raft_status(crate::structs::GetRaftStatusRequest {})
                .await
                .map_err(|status| friendly_status("failed to fetch raft status", status))?
                .into_inner();
            let status = reply.status.context("raft status missing from response")?;
            println!(
                "enabled={} backend={} node={} role={} leader={} term={} commit={} applied={}",
                status.enabled,
                status.backend,
                status.node_id,
                status.role,
                status.leader_id.unwrap_or_else(|| "-".to_string()),
                status.term,
                status.commit_index,
                status.applied_index
            );
            println!(
                "voters={} learners={}",
                status.voters.join(","),
                status.learners.join(",")
            );
        }
        Some(("history", args)) => {
            let limit = args.get_one::<u32>("limit").copied().unwrap_or(25);
            let reply = client
                .list_raft_history(crate::structs::ListRaftHistoryRequest { limit: Some(limit) })
                .await
                .map_err(|status| friendly_status("failed to fetch raft history", status))?
                .into_inner();
            for event in reply.events {
                println!(
                    "#{} {} [{}] actor={} target={} term={} leader={} {}",
                    event.event_id,
                    event.created_at,
                    event.event_type,
                    event.actor,
                    event.target.unwrap_or_else(|| "-".to_string()),
                    event.term,
                    event.leader_id.unwrap_or_else(|| "-".to_string()),
                    event.message,
                );
            }
        }
        Some(("command", args)) => {
            let command = args
                .get_one::<String>("name")
                .context("missing command")?
                .to_string();
            let target_node_id = args.get_one::<String>("target").cloned();
            let actor = args
                .get_one::<String>("actor")
                .cloned()
                .unwrap_or_else(|| "cli".to_string());
            let reply = client
                .issue_raft_command(crate::structs::IssueRaftCommandRequest {
                    command,
                    target_node_id,
                    actor: Some(actor),
                })
                .await
                .map_err(|status| friendly_status("failed to issue raft command", status))?
                .into_inner();
            let status = reply.status.context("raft status missing from response")?;
            let event = reply.event.context("raft event missing from response")?;
            println!(
                "{} -> role={} leader={} term={} ({})",
                reply.command,
                status.role,
                status.leader_id.unwrap_or_else(|| "-".to_string()),
                status.term,
                event.message,
            );
        }
        Some(("join-token", args)) => {
            let actor = args
                .get_one::<String>("actor")
                .cloned()
                .unwrap_or_else(|| "cli".to_string());
            let reply = client
                .get_raft_join_token(crate::structs::GetRaftJoinTokenRequest { actor: Some(actor) })
                .await
                .map_err(|status| friendly_status("failed to issue raft join token", status))?
                .into_inner();
            println!("token={}", reply.token);
            println!("expires_at={}", reply.expires_at);
        }
        _ => {}
    }
    Ok(())
}
