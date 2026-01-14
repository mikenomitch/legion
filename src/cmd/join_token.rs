use crate::cmd::helpers::friendly_status;
use crate::prelude::*;

use crate::structs::insecure_manager_client::InsecureManagerClient;
use crate::structs::GetJoinTokenRequest;
pub fn config() -> Command {
    Command::new("join-token")
        .about("Creates a join token for a worker node")
        .arg(
            Arg::new("type")
                .long("type")
                .short('t')
                .value_name("TYPE")
                .help("Worker type to request a join token for"),
        )
        .arg(
            Arg::new("ttl").long("ttl").value_name("TTL").help(
                "TTL of the join token. Accepts chrono expressions like 1h. 0 means no expiry.",
            ),
        )
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    let ttl = sub_matches
        .get_one::<String>("ttl")
        .map_or("1h", String::as_str);

    let worker_type = sub_matches
        .get_one::<String>("type")
        .map_or("default", String::as_str);

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
        .map_err(|status| friendly_status("failed to request join token", status))?;
    let t = &response.get_ref().token;

    println!("To launch a client with this token, run: ");
    println!("cr start worker {t}");

    Ok(())
}
