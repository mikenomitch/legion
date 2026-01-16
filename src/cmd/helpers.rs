use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use tonic::transport::Certificate;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Status};

use crate::prelude::{Arg, ArgAction};

pub fn print_table<T>(printable_list: Vec<T>)
where
    T: Tabled,
{
    // let style = Style::modern()
    //     .horizontals([(1, HorizontalLine::inherit(Style::modern()).horizontal('═'))])
    //     .verticals([(1, VerticalLine::inherit(Style::modern()))])
    //     .remove_horizontal()
    //     .remove_vertical();
    let mut table = Table::new(&printable_list);
    println!("{}", table.with(Style::psql()));
}

#[instrument(err, skip_all)]
pub async fn connect_manager_client() -> R<ManagerClient<Channel>> {
    let address = std::env::var("LEGION_ADDR").unwrap_or_else(|_| DEF_MANAGER_ADDRESS.to_owned());

    let endpoint = Endpoint::from_shared(address.clone())
        .with_context(|| format!("invalid manager address: {address}"))?
        .connect_timeout(std::time::Duration::from_secs(5));

    let scheme = endpoint.uri().scheme_str().map(str::to_owned);
    let host = endpoint.uri().host().map(str::to_owned);

    let endpoint = if matches!(scheme.as_deref(), Some("https" | "grpcs")) {
        let domain = host.unwrap_or_else(|| "localhost".to_string());
        let ca_bytes = match std::env::var("LEGION_TLS_CA_CERT") {
            Ok(path) => std::fs::read(&path)
                .with_context(|| format!("failed to read TLS CA certificate from {path}"))?,
            Err(_) => {
                let default_path = "configs/tls/rootca.pem";
                std::fs::read(default_path).with_context(|| {
                    format!("failed to read TLS CA certificate from {default_path}")
                })?
            }
        };

        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_bytes))
            .domain_name(domain);

        endpoint
            .tls_config(tls)
            .with_context(|| format!("failed to configure TLS for manager at {address}"))?
    } else {
        endpoint
    };

    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to manager at {address}"))?;

    Ok(ManagerClient::new(channel))
}

pub fn json_flag() -> Arg {
    Arg::new("json")
        .long("json")
        .short('j')
        .help("Return values in JSON format")
        .action(ArgAction::SetTrue)
}

pub fn friendly_status(action: &str, status: Status) -> anyhow::Error {
    let code = status.code();
    let message = status.message().trim();
    let mut description = if message.is_empty() {
        action.to_string()
    } else {
        format!("{action}: {message}")
    };

    let hint = match code {
        Code::Unavailable => Some("manager unavailable; check LEGION_ADDR or start the manager"),
        Code::NotFound => Some("resource not found"),
        Code::AlreadyExists => Some("resource already exists"),
        Code::InvalidArgument => Some("request was invalid; check CLI arguments"),
        Code::FailedPrecondition => Some("request precondition failed"),
        Code::PermissionDenied | Code::Unauthenticated => Some("authentication failed"),
        _ => None,
    };

    if let Some(hint) = hint {
        description.push_str(&format!(" ({hint})"));
    }
    description.push_str(&format!(" [code={code}]"));

    anyhow!(description)
}
