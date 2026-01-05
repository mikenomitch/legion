use crate::prelude::*;
use crate::structs::insecure_manager_client::InsecureManagerClient;
use crate::structs::GetJoinTokenRequest;
use crate::worker::agent::run_worker;

use crate::constants::DEF_INSECURE_ADDR;

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

    run_worker(&token).await
}

#[instrument(err, skip_all)]
async fn request_join_token(ttl: &str, worker_type: &str) -> R<String> {
    let address = std::env::var("LEGION_ADDR").unwrap_or(DEF_INSECURE_ADDR.to_owned());

    let mut client = InsecureManagerClient::connect(address.clone())
        .await
        .with_context(|| format!("failed to connect to manager at {address}"))?;

    let req_ttl: Option<String> = if ttl == "0" || ttl == "none" || ttl == "skip" {
        None
    } else {
        Some(ttl.to_owned())
    };

    let request = tonic::Request::new(GetJoinTokenRequest {
        ttl: req_ttl,
        worker_type: Some(worker_type.into()),
    });

    let response = client
        .get_join_token(request)
        .await
        .context("failed to request join token")?;

    Ok(response.get_ref().token.clone())
}
