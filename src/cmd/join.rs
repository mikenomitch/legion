use crate::cmd::helpers::{connect_manager_client, friendly_status};
use crate::prelude::*;
use crate::structs::CreateJoinTokenRequest;
use crate::worker::agent::run_worker;

pub fn config() -> Command {
    Command::new("join")
        .about("Join a cluster")
        .arg(
            Arg::new("token")
                .value_name("TOKEN")
                .help("Join token issued by the manager")
                .required(false)
                .conflicts_with("auto"),
        )
        .arg(
            Arg::new("auto")
                .short('a')
                .long("auto")
                .help("Request a join token then start the worker")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("type")
                .long("type")
                .value_name("TYPE")
                .help("Worker type to request (default: default)")
                .requires("auto"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("TTL")
                .help("Join token TTL (e.g. 1h); 0 disables expiry")
                .requires("auto"),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("Override the worker name/ID"),
        )
}

#[instrument(err, skip_all)]
pub async fn handle(arg_matches: &ArgMatches) -> R<()> {
    let token = if arg_matches.get_flag("auto") {
        let ttl = arg_matches
            .get_one::<String>("ttl")
            .map_or("1h", String::as_str);

        let worker_type = arg_matches
            .get_one::<String>("type")
            .map_or("default", String::as_str);

        request_join_token(ttl, worker_type).await?
    } else {
        arg_matches
            .get_one::<String>("token")
            .context("Must provide token or use --auto/-a")?
            .to_owned()
    };

    let name = arg_matches.get_one::<String>("name").map(String::as_str);
    run_worker(&token, name).await
}

/// `--auto` convenience: mint a join token through the normal authenticated
/// API and immediately join with it (requires `node:write` when ACLs are on).
#[instrument(err, skip_all)]
async fn request_join_token(ttl: &str, worker_type: &str) -> R<String> {
    let mut client = connect_manager_client().await?;

    let request = tonic::Request::new(CreateJoinTokenRequest {
        worker_type: worker_type.to_string(),
        ttl: Some(ttl.to_string()),
        description: "created by `legion join --auto`".to_string(),
    });

    let response = client
        .create_join_token(request)
        .await
        .map_err(|status| friendly_status("failed to request join token", status))?;

    Ok(response.get_ref().token.clone())
}
