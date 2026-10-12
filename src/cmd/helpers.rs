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

/// Split a server message into its text and the actionable hint it carries
/// on a final `\nHint: ...` line, when it carries one. Servers put a hint in
/// the message when the fix is specific to the failure (for example
/// "use --disk"), which beats the generic per-code hint.
pub fn split_hint(message: &str) -> (&str, Option<&str>) {
    match message.split_once("\nHint: ") {
        Some((text, hint)) => (text.trim(), Some(hint.trim())),
        None => (message, None),
    }
}

pub fn friendly_status(action: &str, status: Status) -> anyhow::Error {
    let code = status.code();
    let (message, server_hint) = split_hint(status.message().trim());
    let mut description = if message.is_empty() {
        action.to_string()
    } else {
        format!("{action}: {message}")
    };
    if let Some(hint) = server_hint {
        return anyhow!("{description} [code={code}]\nHint: {hint}");
    }

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

/// Usage columns shared by `legion node usage` and `legion instance list`
/// (docs/resource-enforcement.md, "Measuring usage").
pub mod usage_display {
    use crate::structs::AllocationUsage;

    /// Seconds of inactivity below which an allocation reads as active: one
    /// report interval, so "idle" never flickers between reports.
    const IDLE_AFTER_SECONDS: i64 = 10;

    /// Bytes as whole MiB.
    pub fn mib(bytes: u64) -> String {
        (bytes / (1024 * 1024)).to_string()
    }

    /// Thousandths of a core as cores with two decimals.
    pub fn cores(millicores: u64) -> String {
        format!("{:.2}", millicores as f64 / 1000.0)
    }

    /// An idle span as `2m41s`, or `-` when active within the last interval.
    pub fn idle(seconds: i64) -> String {
        if seconds < IDLE_AFTER_SECONDS {
            return "-".into();
        }
        let (hours, minutes, secs) = (seconds / 3600, (seconds % 3600) / 60, seconds % 60);
        if hours > 0 {
            format!("{hours}h{minutes}m")
        } else if minutes > 0 {
            format!("{minutes}m{secs}s")
        } else {
            format!("{secs}s")
        }
    }

    /// `(used MiB, cpu cores, idle)` for a table row, dashes when unmeasured.
    /// `now_unix` is the reader's clock, so idle keeps counting for a report
    /// that is a few seconds old.
    pub fn columns(usage: Option<&AllocationUsage>, now_unix: i64) -> (String, String, String) {
        match usage {
            // With an idle policy, "idle" is what the policy counts (its
            // signals), so the column matches what the idle sweep acts on.
            Some(usage) => (
                mib(usage.memory_current_bytes),
                cores(usage.cpu_millicores),
                idle(
                    now_unix
                        - if usage.idle_since_unix > 0 {
                            usage.idle_since_unix
                        } else {
                            usage.last_activity_unix
                        },
                ),
            ),
            None => ("-".into(), "-".into(), "-".into()),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn idle_spans_read_like_durations() {
            assert_eq!(idle(0), "-");
            assert_eq!(idle(9), "-");
            assert_eq!(idle(42), "42s");
            assert_eq!(idle(161), "2m41s");
            assert_eq!(idle(7260), "2h1m");
        }

        #[test]
        fn unmeasured_rows_are_dashes() {
            assert_eq!(
                columns(None, 0),
                ("-".to_string(), "-".to_string(), "-".to_string())
            );
            let usage = AllocationUsage {
                memory_current_bytes: 1830 * 1024 * 1024,
                cpu_millicores: 250,
                last_activity_unix: 100,
                ..Default::default()
            };
            assert_eq!(
                columns(Some(&usage), 261),
                ("1830".to_string(), "0.25".to_string(), "2m41s".to_string())
            );
        }
    }
}
