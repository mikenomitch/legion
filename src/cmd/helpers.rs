use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
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

/// Manager client with the caller's ACL token attached to every request.
pub type AuthedManagerClient = ManagerClient<InterceptedService<Channel, AuthInterceptor>>;

/// Attaches `authorization: Bearer <token>` to outgoing requests when a
/// token is available (no-op otherwise, matching ACL-disabled clusters).
#[derive(Clone)]
pub struct AuthInterceptor {
    token: Option<MetadataValue<Ascii>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        Ok(request)
    }
}

/// Path of the CLI token file written by `legion login`.
///
/// Override with `LEGION_TOKEN_FILE`; defaults to `~/.legion/token`.
pub fn cli_token_path() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("LEGION_TOKEN_FILE") {
        return std::path::PathBuf::from(path);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".legion").join("token")
}

/// Resolves the ACL token for outgoing requests: `LEGION_TOKEN` env var
/// first (CI-friendly), then the `legion login` token file.
pub fn load_cli_token() -> Option<String> {
    if let Ok(token) = std::env::var("LEGION_TOKEN") {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Some(token);
        }
    }
    std::fs::read_to_string(cli_token_path())
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|token| !token.is_empty())
}

/// Persists a token for future CLI invocations (0600 on unix).
pub fn save_cli_token(secret: &str) -> R<std::path::PathBuf> {
    let path = cli_token_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&path, format!("{secret}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    Ok(path)
}

fn auth_interceptor() -> AuthInterceptor {
    let token = load_cli_token().and_then(|token| {
        match format!("Bearer {token}").parse::<MetadataValue<Ascii>>() {
            Ok(value) => Some(value),
            Err(_) => {
                warn!("Ignoring LEGION_TOKEN with non-ASCII characters");
                None
            }
        }
    });
    AuthInterceptor { token }
}

#[instrument(err, skip_all)]
pub async fn connect_manager_client() -> R<AuthedManagerClient> {
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

    Ok(ManagerClient::with_interceptor(channel, auth_interceptor()))
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
        // Raft non-leader rejections carry the leader's name+address in the
        // message itself; the hint tells the operator what to do with it.
        Code::FailedPrecondition if message.contains("not the raft leader") => Some(
            "writes go to the raft leader; point LEGION_ADDR at the leader shown above and retry",
        ),
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
