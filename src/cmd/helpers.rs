use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use std::time::Duration;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Certificate;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Status;

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
/// token is available (no-op otherwise, matching ACL-disabled clusters),
/// and gives each bounded request its deadline.
#[derive(Clone)]
pub struct AuthInterceptor {
    token: Option<MetadataValue<Ascii>>,
    request_timeout: Option<Duration>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        if let Some(timeout) = self.request_timeout {
            let unbounded = request
                .extensions()
                .get::<tonic::GrpcMethod>()
                .is_some_and(|method| UNBOUNDED_METHODS.contains(&method.method()));
            // `set_timeout` sends `grpc-timeout`: the channel enforces it
            // locally, and the manager learns the caller's deadline too.
            if !unbounded && request.metadata().get("grpc-timeout").is_none() {
                request.set_timeout(timeout);
            }
        }
        Ok(request)
    }
}

/// Default for `LEGION_CONNECT_TIMEOUT`.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default for `LEGION_REQUEST_TIMEOUT`: far above any ordinary call, short
/// enough that a manager that accepts connections but never answers (one
/// with no raft leader, say) fails the command instead of hanging it.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Manager RPCs that carry no request deadline. Streams stay open as long as
/// the caller wants (`legion exec`, `legion events follow`), and the
/// whole-store operations take time that grows with the cluster (`legion
/// state backup`, `legion keyring rewrap`). Commands that wait by polling
/// (`job logs -f`, `job wait`, `--wait`) are not here: each poll is a short
/// call that should fail fast like any other.
const UNBOUNDED_METHODS: &[&str] = &[
    "ExecAllocation",
    "StreamEvents",
    "GetStateBackup",
    "RewrapKv",
];

/// How long the CLI waits on a manager, from `LEGION_CONNECT_TIMEOUT` and
/// `LEGION_REQUEST_TIMEOUT`. `None` waits forever (the variable set to `0`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientTimeouts {
    /// Bound on opening the TCP (and TLS) connection.
    pub connect: Option<Duration>,
    /// Bound on each request outside [`UNBOUNDED_METHODS`], from send to
    /// the reply's headers (all of a unary call).
    pub request: Option<Duration>,
}

impl ClientTimeouts {
    pub fn from_env() -> R<Self> {
        Self::parse(
            std::env::var("LEGION_CONNECT_TIMEOUT").ok().as_deref(),
            std::env::var("LEGION_REQUEST_TIMEOUT").ok().as_deref(),
        )
    }

    fn parse(connect: Option<&str>, request: Option<&str>) -> R<Self> {
        Ok(ClientTimeouts {
            connect: parse_timeout("LEGION_CONNECT_TIMEOUT", connect, DEFAULT_CONNECT_TIMEOUT)?,
            request: parse_timeout("LEGION_REQUEST_TIMEOUT", request, DEFAULT_REQUEST_TIMEOUT)?,
        })
    }
}

/// A timeout variable: a duration (`30s`, `2m`), bare seconds (`30`), or
/// `0` for none. Unset or blank takes `default`.
fn parse_timeout(var: &str, raw: Option<&str>, default: Duration) -> R<Option<Duration>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(Some(default));
    };
    let duration = match raw.parse::<u64>() {
        Ok(secs) => Duration::from_secs(secs),
        Err(_) => humantime::parse_duration(raw)
            .with_context(|| format!("invalid {var} {raw:?}: expected a duration like 30s"))?,
    };
    Ok((!duration.is_zero()).then_some(duration))
}

/// The manager address the CLI talks to: `LEGION_ADDR`, else
/// `http://127.0.0.1:4747`. Inside a task that declares the `legion`
/// identity the worker sets it to the allocation's API socket,
/// `unix:///…/legion.sock` (docs/workload-identity.md, "The API socket").
pub fn manager_address() -> String {
    std::env::var("LEGION_ADDR").unwrap_or_else(|_| DEF_MANAGER_ADDRESS.to_owned())
}

/// The socket path of a `unix://` manager address (`unix:///path/legion.sock`
/// or `unix:relative.sock`), or `None` for a network address.
pub fn unix_socket_path(address: &str) -> Option<std::path::PathBuf> {
    let rest = address
        .strip_prefix("unix://")
        .or_else(|| address.strip_prefix("unix:"))?;
    (!rest.is_empty()).then(|| std::path::PathBuf::from(rest))
}

/// A channel to the gRPC server listening on the unix socket at `path`.
/// The connection opens on the first call. A path longer than a socket
/// address holds (about 100 bytes, which a deep task directory can exceed)
/// is reached through its directory's `/proc/self/fd` entry on Linux.
pub fn unix_socket_channel(path: std::path::PathBuf, connect_timeout: Option<Duration>) -> Channel {
    // The URI is a placeholder: the connector dials the socket.
    let mut endpoint = Endpoint::from_static("http://legion.sock")
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10));
    if let Some(connect) = connect_timeout {
        endpoint = endpoint.connect_timeout(connect);
    }
    endpoint.connect_with_connector_lazy(tower::service_fn(move |_: http::Uri| {
        let path = path.clone();
        async move {
            let stream = connect_unix(&path).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
        }
    }))
}

async fn connect_unix(path: &std::path::Path) -> std::io::Result<tokio::net::UnixStream> {
    /// `sun_path` is 108 bytes on Linux (104 on macOS), NUL included.
    const MAX_SOCKET_PATH: usize = 100;
    if path.as_os_str().len() <= MAX_SOCKET_PATH || !cfg!(target_os = "linux") {
        return tokio::net::UnixStream::connect(path).await;
    }
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return tokio::net::UnixStream::connect(path).await;
    };
    let dir = std::fs::File::open(dir)?;
    let via = std::path::Path::new("/proc/self/fd")
        .join(std::os::fd::AsRawFd::as_raw_fd(&dir).to_string())
        .join(name);
    let stream = tokio::net::UnixStream::connect(&via).await;
    drop(dir);
    stream
}

/// A manager address as a token-file name: `host_port`, so
/// `http://127.0.0.1:4747` is `127.0.0.1_4747`. The scheme is dropped (http
/// and https on one port are one manager), a missing port is the scheme's
/// default, and anything outside `[A-Za-z0-9.-]` becomes `_`, so an IPv6
/// address or a typo can never name a path outside the tokens directory.
pub fn token_file_key(address: &str) -> String {
    if let Some(path) = unix_socket_path(address) {
        // A socket has no host and port; its path names it. `unix_` keeps
        // it apart from every network address's key.
        return format!("unix_{}", path.display())
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
    }
    let host_port = match address.parse::<http::Uri>() {
        Ok(uri) if uri.host().is_some() => {
            let host = uri.host().unwrap_or_default().trim_matches(['[', ']']);
            let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
                Some("https" | "grpcs") => 443,
                _ => 80,
            });
            format!("{host}_{port}")
        }
        _ => address.to_string(),
    };
    host_port
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Path of the stored CLI token for the manager at `address`: the file
/// `legion login` writes, and the one a manager's first start writes the
/// initial management token to.
///
/// Tokens are kept per manager address (`~/.legion/tokens/<host>_<port>`) so
/// two clusters on one machine never present each other's token.
/// `LEGION_TOKEN_FILE` names one file for every address instead.
pub fn cli_token_path(address: &str) -> std::path::PathBuf {
    if let Ok(path) = std::env::var("LEGION_TOKEN_FILE") {
        return std::path::PathBuf::from(path);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home)
        .join(".legion")
        .join("tokens")
        .join(token_file_key(address))
}

/// Where the token the CLI presents came from, kept so an `Unauthenticated`
/// reply can say which credential the manager rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenSource {
    /// The `LEGION_TOKEN` environment variable.
    Env,
    /// The stored token file for the manager's address.
    File(std::path::PathBuf),
    /// No token: `LEGION_TOKEN` is unset and this file does not exist.
    Missing(std::path::PathBuf),
}

/// The manager address and token source of the last client this process
/// built, for [`presented_token_note`].
static PRESENTED_TOKEN: std::sync::Mutex<Option<(String, TokenSource)>> =
    std::sync::Mutex::new(None);

/// Resolves the ACL token for requests to `address`: the `LEGION_TOKEN` env
/// var first (CI-friendly), then the stored token for that address.
pub fn load_cli_token(address: &str) -> (Option<String>, TokenSource) {
    if let Ok(token) = std::env::var("LEGION_TOKEN") {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return (Some(token), TokenSource::Env);
        }
    }
    let path = cli_token_path(address);
    match std::fs::read_to_string(&path)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|token| !token.is_empty())
    {
        Some(token) => (Some(token), TokenSource::File(path)),
        None => (None, TokenSource::Missing(path)),
    }
}

/// Persists a token for future CLI calls to `address` (0600 on unix).
///
/// Never over the `legion` identity's token file
/// (`LEGION_IDENTITY_TOKEN_FILE`): inside a task that file is the worker's,
/// refreshed on its own schedule, and a login token written there would be
/// overwritten, or would replace the identity the task's other clients read.
pub fn save_cli_token(address: &str, secret: &str) -> R<std::path::PathBuf> {
    let path = cli_token_path(address);
    refuse_identity_token_file(
        &path,
        std::env::var_os(crate::structs::workload_identity::IDENTITY_TOKEN_FILE_ENV)
            .map(std::path::PathBuf::from)
            .as_deref(),
    )?;
    if let Some(parent) = path.parent() {
        crate::util::create_private_dir_all(parent)?;
    }
    crate::util::write_private_file(&path, format!("{secret}\n").as_bytes())?;
    Ok(path)
}

/// Refuse to store a login token at `path` when it is the task's identity
/// token file (`identity_file`, from `LEGION_IDENTITY_TOKEN_FILE`), by name
/// or through a link.
fn refuse_identity_token_file(
    path: &std::path::Path,
    identity_file: Option<&std::path::Path>,
) -> R<()> {
    let Some(identity_file) = identity_file else {
        return Ok(());
    };
    let same = path == identity_file
        || matches!(
            (std::fs::canonicalize(path), std::fs::canonicalize(identity_file)),
            (Ok(a), Ok(b)) if a == b
        );
    if same {
        bail!(
            "refusing to store the login token at {}: it is this task's workload identity token \
             file ({}), which the worker refreshes. Point LEGION_TOKEN_FILE somewhere else, or \
             leave it unset",
            path.display(),
            crate::structs::workload_identity::IDENTITY_TOKEN_FILE_ENV
        );
    }
    Ok(())
}

/// What the CLI presented to the manager, for the hint on an
/// `Unauthenticated` reply; `None` when no client was built.
pub fn presented_token_note() -> Option<String> {
    let presented = PRESENTED_TOKEN.lock().ok()?.clone()?;
    Some(token_note(&presented.0, &presented.1))
}

/// The hint for an `Unauthenticated` reply from `address`, naming the
/// credential the CLI sent: a token from another cluster and a revoked one
/// look the same from the manager's side.
fn token_note(address: &str, source: &TokenSource) -> String {
    match source {
        TokenSource::Env => format!(
            "the CLI sent LEGION_TOKEN to {address}, which does not accept it; unset it to use \
             the stored token, or set it to a token this cluster issued"
        ),
        TokenSource::File(path) => format!(
            "the CLI sent the token stored for {address} in {}, which this manager does not \
             accept (revoked, expired, or issued by another cluster); run `legion login`, set \
             LEGION_TOKEN, or on the manager's machine use <dataDir>/bootstrap-token",
            path.display()
        ),
        TokenSource::Missing(_) if unix_socket_path(address).is_some() => format!(
            "the CLI sent no token to {address}, so the worker behind that socket attached this \
             task's `legion` identity, which the manager does not accept (workload identity \
             disabled, or the job's grant revoked); `legion acl whoami` shows what it holds"
        ),
        TokenSource::Missing(path) => format!(
            "the CLI has no token for {address}: LEGION_TOKEN is unset and {} does not exist; \
             run `legion login`, set LEGION_TOKEN, or on the manager's machine use \
             <dataDir>/bootstrap-token",
            path.display()
        ),
    }
}

fn auth_interceptor(address: &str, request_timeout: Option<Duration>) -> AuthInterceptor {
    let (token, source) = load_cli_token(address);
    if let Ok(mut presented) = PRESENTED_TOKEN.lock() {
        *presented = Some((address.to_string(), source));
    }
    let token =
        token.and_then(
            |token| match format!("Bearer {token}").parse::<MetadataValue<Ascii>>() {
                Ok(value) => Some(value),
                Err(_) => {
                    warn!("Ignoring an ACL token with non-ASCII characters");
                    None
                }
            },
        );
    AuthInterceptor {
        token,
        request_timeout,
    }
}

#[instrument(skip_all)]
pub async fn connect_manager_client() -> R<AuthedManagerClient> {
    connect_manager_client_at(&manager_address(), ClientTimeouts::from_env()?)
}

/// A client for the manager at `address`. The connection opens on the first
/// call, so a manager that cannot be reached fails that call with
/// `Unavailable`, which the `--json` envelope reports like any other error.
pub fn connect_manager_client_at(
    address: &str,
    timeouts: ClientTimeouts,
) -> R<AuthedManagerClient> {
    let address = address.to_owned();
    if let Some(path) = unix_socket_path(&address) {
        // A local socket (a task's API socket): no TLS, and the worker
        // behind it attaches the task's identity when the CLI sends no
        // token of its own.
        return Ok(ManagerClient::with_interceptor(
            unix_socket_channel(path, timeouts.connect),
            auth_interceptor(&address, timeouts.request),
        ));
    }
    let mut endpoint = Endpoint::from_shared(address.clone())
        .with_context(|| format!("invalid manager address: {address}"))?
        // Notice a manager that vanished under a long stream (`exec`,
        // `events follow`), which has no request deadline to do it.
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10));
    if let Some(connect) = timeouts.connect {
        endpoint = endpoint.connect_timeout(connect);
    }

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

    let channel = endpoint.connect_lazy();
    Ok(ManagerClient::with_interceptor(
        channel,
        auth_interceptor(&address, timeouts.request),
    ))
}

/// A client for one worker's plane (port 4848), for the commands that talk
/// to a worker directly instead of through a manager (`legion sandbox
/// start`, `legion exec --direct`). The worker plane is always TLS; the
/// worker's certificate is verified against the cluster CA bundle, read from
/// `ca_file` or fetched from the manager with the caller's token
/// (`GetMeshCa`, `net:read`). With `node_name`, the certificate must name
/// that node (`<node>.node.legion`); without it, any node certificate of the
/// cluster is accepted. The client presents no certificate: these callers
/// authenticate with tickets.
pub async fn worker_plane_client(
    address: &str,
    node_name: Option<&str>,
    ca_file: Option<&str>,
) -> R<crate::structs::worker_client::WorkerClient<tonic::transport::Channel>> {
    use crate::cluster_tls::{
        client_config, tls_channel, AnyNodeVerifier, NamedServerVerifier, SwappableRoots,
    };
    let bundle = match ca_file {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("failed to read the cluster CA bundle from {path}"))?,
        None => {
            let mut manager = connect_manager_client().await?;
            let reply = manager
                .get_mesh_ca(tonic::Request::new(crate::structs::GetMeshCaRequest {}))
                .await
                .map_err(|status| {
                    friendly_status(
                        "failed to fetch the cluster CA to verify the worker (pass --ca to \
                         skip the manager)",
                        status,
                    )
                })?
                .into_inner();
            if reply.bundle_pem.is_empty() {
                bail!("the cluster has no CA yet, so no worker has a certificate to verify");
            }
            reply.bundle_pem
        }
    };
    let roots = SwappableRoots::from_pem(&bundle)?;
    let (verifier, sni): (Arc<dyn rustls::client::danger::ServerCertVerifier>, String) =
        match node_name {
            Some(node) => {
                let name = crate::manager::mesh_ca::node_dns_name(node);
                (NamedServerVerifier::new(roots, &name)?, name)
            }
            None => (AnyNodeVerifier::new(roots), "node.legion".to_string()),
        };
    let channel = tls_channel(address, client_config(verifier, None)?, &sni)?;
    Ok(crate::structs::worker_client::WorkerClient::new(channel))
}

pub fn json_flag() -> Arg {
    Arg::new("json")
        .long("json")
        .short('j')
        .help("Return values in JSON format")
        .action(ArgAction::SetTrue)
}

/// `-n/--namespace`: the namespace a command acts in (docs/namespaces.md).
/// Attached only to commands that act on namespaced things, so on a
/// cluster-scoped command (`legion node list -n prod`) it is a parse error
/// rather than a flag that looks like a scope and is silently ignored.
pub fn namespace_flag() -> Arg {
    Arg::new("namespace")
        .long("namespace")
        .short('n')
        .value_name("NAMESPACE")
        .help("Namespace to act in (default: $LEGION_NAMESPACE, else `default`)")
}

/// `-n` and `-A` for list commands: `-n` takes names or `*`-globs,
/// comma-separated (`-n 'payments-*,shared'`); `-A` lists every namespace
/// the caller can read.
pub fn list_namespace_flags() -> [Arg; 2] {
    [
        Arg::new("namespace")
            .long("namespace")
            .short('n')
            .value_name("NAMESPACES")
            .help(
                "Namespaces to list: names or globs, comma-separated (default: \
                 $LEGION_NAMESPACE, else `default`)",
            ),
        Arg::new("all-namespaces")
            .long("all-namespaces")
            .short('A')
            .action(ArgAction::SetTrue)
            .conflicts_with("namespace")
            .help("List every namespace you can read"),
    ]
}

/// The active namespace: `-n`, else `LEGION_NAMESPACE`, else `None`
/// (which every caller treats as `default`). Safe on commands that do not
/// define `-n`.
pub fn active_namespace(matches: &ArgMatches) -> Option<String> {
    matches
        .try_get_one::<String>("namespace")
        .ok()
        .flatten()
        .cloned()
        .or_else(|| std::env::var(crate::structs::namespace::NAMESPACE_ENV).ok())
        .map(|namespace| namespace.trim().to_string())
        .filter(|namespace| !namespace.is_empty())
}

/// Resolves a typed job reference (`web`, or `payments-prod/web`) against
/// the active namespace to the canonical id the API takes. A qualified name
/// that disagrees with `-n` fails here, through the output envelope.
pub fn job_ref(matches: &ArgMatches, raw: &str) -> R<String> {
    let format = crate::cmd::output::OutputFormat::from_matches(matches);
    crate::structs::namespace::resolve_job_ref(raw, active_namespace(matches).as_deref()).map_err(
        |message| {
            crate::cmd::output::fail_local(
                format,
                crate::cmd::output::CliError::local(
                    "invalid_job_ref",
                    message,
                    Some("name a job as `name` or `namespace/name`"),
                ),
            )
        },
    )
}

/// The job argument `key` resolved against the active namespace (see
/// [`job_ref`]), when it was given.
pub fn job_arg(matches: &ArgMatches, key: &str) -> R<Option<String>> {
    match matches.try_get_one::<String>(key).ok().flatten() {
        Some(raw) => job_ref(matches, raw).map(Some),
        None => Ok(None),
    }
}

/// [`job_arg`] for a required argument.
pub fn required_job_arg(matches: &ArgMatches, key: &str) -> R<String> {
    job_arg(matches, key)?.with_context(|| format!("missing required argument {key}"))
}

/// Resolves a typed instance reference (`user-42`, `payments/user-42`, or a
/// minted id) against the active namespace to the canonical instance id the
/// API takes (see `crate::structs::namespace::resolve_instance_ref`).
pub fn instance_ref(matches: &ArgMatches, raw: &str) -> R<String> {
    let format = crate::cmd::output::OutputFormat::from_matches(matches);
    // Beside a qualified job (`--job payments/web`), a bare key lives in
    // that job's namespace.
    let active = active_namespace(matches).or_else(|| {
        NAMESPACED_REF_ARGS[..3].iter().find_map(|key| {
            let job = matches.try_get_one::<String>(key).ok().flatten()?;
            job.split_once('/')
                .map(|(namespace, _)| namespace.to_string())
        })
    });
    crate::structs::namespace::resolve_instance_ref(raw, active.as_deref()).map_err(|message| {
        crate::cmd::output::fail_local(
            format,
            crate::cmd::output::CliError::local(
                "invalid_instance_ref",
                message,
                Some("name an instance as `id` or `namespace/id`"),
            ),
        )
    })
}

/// The instance argument `key` resolved against the active namespace (see
/// [`instance_ref`]), when it was given.
pub fn instance_arg(matches: &ArgMatches, key: &str) -> R<Option<String>> {
    match matches.try_get_one::<String>(key).ok().flatten() {
        Some(raw) => instance_ref(matches, raw).map(Some),
        None => Ok(None),
    }
}

/// [`instance_arg`] for a required argument.
pub fn required_instance_arg(matches: &ArgMatches, key: &str) -> R<String> {
    instance_arg(matches, key)?.with_context(|| format!("missing required argument {key}"))
}

/// Argument ids that name a job or an instance: a command taking one gets
/// `-n` from [`with_namespace_flags`].
const NAMESPACED_REF_ARGS: &[&str] = &["job", "job-id", "job_id", "instance", "instance-id"];

/// Attaches `-n/--namespace` to every subcommand that takes a `job` or
/// `instance` argument and does not already define `namespace`, so a
/// command that acts on one always resolves it in a namespace, and one that
/// does not never accepts the flag. Where `-n` is already taken (`--count`
/// on `instance fork`), only `--namespace` is added.
pub fn with_namespace_flags(command: Command) -> Command {
    let takes_job = command
        .get_arguments()
        .any(|arg| NAMESPACED_REF_ARGS.contains(&arg.get_id().as_str()));
    let has_namespace = command
        .get_arguments()
        .any(|arg| arg.get_id().as_str() == "namespace");
    let short_taken = command
        .get_arguments()
        .any(|arg| arg.get_short() == Some('n'));
    let mut command = command;
    if takes_job && !has_namespace {
        let flag = if short_taken {
            Arg::new("namespace")
                .long("namespace")
                .value_name("NAMESPACE")
                .help("Namespace to act in (default: $LEGION_NAMESPACE, else `default`)")
        } else {
            namespace_flag()
        };
        command = command.arg(flag);
    }
    let names: Vec<String> = command
        .get_subcommands()
        .map(|sub| sub.get_name().to_string())
        .collect();
    for name in names {
        command = command.mut_subcommand(name, with_namespace_flags);
    }
    command
}

/// The namespace selection a list command sends: `["*"]` for `-A`, the
/// `-n` value (or `LEGION_NAMESPACE`) otherwise, empty (= `default`) when
/// neither is set.
pub fn namespace_selection(matches: &ArgMatches) -> Vec<String> {
    if matches
        .try_get_one::<bool>("all-namespaces")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
    {
        return vec!["*".into()];
    }
    active_namespace(matches).into_iter().collect()
}

/// Whether a list command was asked for more than one namespace, so its
/// table should carry a NAMESPACE column.
pub fn lists_many_namespaces(matches: &ArgMatches) -> bool {
    namespace_selection(matches)
        .iter()
        .any(|selector| selector.contains('*') || selector.contains(','))
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

/// For an `Unauthenticated` reply, the hint naming the token the CLI sent
/// (and so which file to fix). It replaces the server's hint: the manager
/// cannot know which file the token came from.
pub fn unauthenticated_hint(code: tonic::Code) -> Option<String> {
    (code == tonic::Code::Unauthenticated)
        .then(presented_token_note)
        .flatten()
}

pub fn friendly_status(action: &str, status: Status) -> anyhow::Error {
    let status = crate::cmd::output::client_side_failure(&status).unwrap_or(status);
    let code_raw = status.code();
    // The stable name (`FailedPrecondition`), the same `code` the JSON
    // envelope carries — tonic's `Display` for a code is a prose sentence.
    let code = crate::cmd::output::code_name(code_raw);
    let (message, server_hint) = split_hint(status.message().trim());
    let mut description = if message.is_empty() {
        action.to_string()
    } else {
        format!("{action}: {message}")
    };
    if let Some(hint) = unauthenticated_hint(code_raw).or(server_hint.map(str::to_string)) {
        return anyhow!("{description} [code={code}]\nHint: {hint}");
    }

    let hint = crate::cmd::output::hint_for(code_raw, message);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::output::CliError;
    use crate::structs::ListJobsRequest;
    use std::convert::Infallible;
    use std::task::{Context, Poll};

    #[test]
    fn token_files_are_keyed_by_manager_address_test() {
        assert_eq!(token_file_key("http://127.0.0.1:4747"), "127.0.0.1_4747");
        // http and https on one port are one manager.
        assert_eq!(token_file_key("https://127.0.0.1:4747"), "127.0.0.1_4747");
        assert_eq!(
            token_file_key("http://manager.example.com:4757/"),
            "manager.example.com_4757"
        );
        assert_eq!(
            token_file_key("https://legion.internal"),
            "legion.internal_443"
        );
        assert_eq!(token_file_key("http://[::1]:4747"), "__1_4747");
        // Whatever the input, the key stays one path component.
        let odd = token_file_key("../../etc/passwd");
        assert!(!odd.contains('/'), "{odd}");
    }

    #[test]
    fn unix_addresses_name_a_socket_and_a_token_file_of_their_own_test() {
        assert_eq!(
            unix_socket_path("unix:///alloc/task/secrets/legion.sock"),
            Some(std::path::PathBuf::from("/alloc/task/secrets/legion.sock"))
        );
        assert_eq!(
            unix_socket_path("unix:legion.sock"),
            Some(std::path::PathBuf::from("legion.sock"))
        );
        assert_eq!(unix_socket_path("unix://"), None);
        assert_eq!(unix_socket_path("http://127.0.0.1:4747"), None);
        let key = token_file_key("unix:///alloc/task/secrets/legion.sock");
        assert_eq!(key, "unix__alloc_task_secrets_legion.sock");
        assert!(!key.contains('/'), "{key}");
        // A client for one opens nothing until its first call.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            connect_manager_client_at(
                "unix:///nonexistent/legion.sock",
                ClientTimeouts {
                    connect: Some(Duration::from_secs(1)),
                    request: None,
                },
            )
            .expect("a unix client builds lazily");
        });
    }

    /// `legion login` inside a task never writes over the workload's
    /// identity token file, by name or through a link.
    #[test]
    fn login_never_stores_over_the_identity_token_file_test() -> R<()> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let identity = dir.path().join("legion.token");
        std::fs::write(&identity, "identity")?;
        let link = dir.path().join("token-link");
        std::os::unix::fs::symlink(&identity, &link)?;
        let refused = refuse_identity_token_file(&identity, Some(&identity))
            .expect_err("the identity token file itself");
        assert!(
            refused.to_string().contains("LEGION_IDENTITY_TOKEN_FILE"),
            "{refused}"
        );
        assert!(refuse_identity_token_file(&link, Some(&identity)).is_err());
        assert!(refuse_identity_token_file(&dir.path().join("login"), Some(&identity)).is_ok());
        assert!(refuse_identity_token_file(&identity, None).is_ok());
        assert_eq!(std::fs::read_to_string(&identity)?, "identity");
        Ok(())
    }

    #[test]
    fn the_unauthenticated_hint_names_the_token_file_and_address_test() {
        let path = std::path::PathBuf::from("/home/me/.legion/tokens/127.0.0.1_4757");
        let sent = token_note("http://127.0.0.1:4757", &TokenSource::File(path.clone()));
        assert!(
            sent.contains("/home/me/.legion/tokens/127.0.0.1_4757"),
            "{sent}"
        );
        assert!(sent.contains("http://127.0.0.1:4757"), "{sent}");
        assert!(sent.contains("legion login"), "{sent}");

        let missing = token_note("http://127.0.0.1:4757", &TokenSource::Missing(path));
        assert!(missing.contains("does not exist"), "{missing}");
        assert!(missing.contains("bootstrap-token"), "{missing}");

        let env = token_note("http://127.0.0.1:4757", &TokenSource::Env);
        assert!(env.contains("LEGION_TOKEN"), "{env}");
    }

    /// A manager whose handlers never return: the shape of one whose raft
    /// core died, where reads wait on a leader that never comes.
    #[derive(Clone)]
    struct NeverAnswers;

    impl tonic::server::NamedService for NeverAnswers {
        const NAME: &'static str = "legion.Manager";
    }

    impl tower::Service<http::Request<tonic::body::BoxBody>> for NeverAnswers {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = std::future::Pending<Result<Self::Response, Infallible>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: http::Request<tonic::body::BoxBody>) -> Self::Future {
            std::future::pending()
        }
    }

    async fn serve_never_answers() -> R<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NeverAnswers)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        Ok(format!("http://{addr}"))
    }

    fn short(request: Duration) -> ClientTimeouts {
        ClientTimeouts {
            connect: Some(Duration::from_secs(2)),
            request: Some(request),
        }
    }

    /// Spot-check P2: `legion job list` against a leaderless cluster hung
    /// forever. It must fail at the request deadline with a retryable
    /// `DeadlineExceeded` in the envelope.
    #[tokio::test]
    async fn a_manager_that_never_answers_is_a_retryable_deadline_test() -> R<()> {
        let address = serve_never_answers().await?;
        let mut client = connect_manager_client_at(&address, short(Duration::from_millis(300)))?;
        let status = tokio::time::timeout(
            Duration::from_secs(10),
            client.list_jobs(ListJobsRequest::default()),
        )
        .await
        .expect("the request deadline never fired")
        .expect_err("a handler that never returns cannot succeed");

        let err = CliError::from_status("failed to list jobs", &status);
        assert_eq!(err.code, "DeadlineExceeded", "{err:?}");
        assert!(err.retryable);
        assert!(err.message.contains("LEGION_REQUEST_TIMEOUT"), "{err:?}");
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("raft")),
            "{err:?}"
        );

        let human = friendly_status("failed to list jobs", status).to_string();
        assert!(human.contains("[code=DeadlineExceeded]"), "{human}");
        Ok(())
    }

    #[tokio::test]
    async fn an_unreachable_manager_is_a_retryable_unavailable_test() -> R<()> {
        // Bind, then drop, to get a port nothing listens on.
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let mut client = connect_manager_client_at(
            &format!("http://127.0.0.1:{port}"),
            short(Duration::from_secs(5)),
        )?;
        let status = client
            .list_jobs(ListJobsRequest::default())
            .await
            .expect_err("nothing listens there");
        let err = CliError::from_status("failed to list jobs", &status);
        assert_eq!(err.code, "Unavailable", "{err:?}");
        assert!(err.retryable);
        assert!(
            err.message.contains("cannot connect to the manager"),
            "{err:?}"
        );
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("LEGION_ADDR")),
            "{err:?}"
        );
        Ok(())
    }

    fn intercept(method: &'static str, timeout: Option<Duration>) -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new("legion.Manager", method));
        let mut interceptor = AuthInterceptor {
            token: None,
            request_timeout: timeout,
        };
        interceptor
            .call(request)
            .expect("the interceptor never refuses")
    }

    #[test]
    fn streams_and_whole_store_calls_carry_no_deadline_test() {
        let timeout = Some(Duration::from_secs(30));
        assert!(intercept("ListJobs", timeout)
            .metadata()
            .get("grpc-timeout")
            .is_some());
        for method in UNBOUNDED_METHODS {
            assert!(
                intercept(method, timeout)
                    .metadata()
                    .get("grpc-timeout")
                    .is_none(),
                "{method} must not carry a deadline"
            );
        }
        assert!(intercept("ListJobs", None)
            .metadata()
            .get("grpc-timeout")
            .is_none());
    }

    #[test]
    fn timeout_variables_parse_test() -> R<()> {
        let defaults = ClientTimeouts::parse(None, Some("  "))?;
        assert_eq!(defaults.connect, Some(DEFAULT_CONNECT_TIMEOUT));
        assert_eq!(defaults.request, Some(DEFAULT_REQUEST_TIMEOUT));

        let set = ClientTimeouts::parse(Some("2s"), Some("90"))?;
        assert_eq!(set.connect, Some(Duration::from_secs(2)));
        assert_eq!(set.request, Some(Duration::from_secs(90)));

        let off = ClientTimeouts::parse(Some("0"), Some("0s"))?;
        assert_eq!(off.connect, None);
        assert_eq!(off.request, None);

        let err = ClientTimeouts::parse(None, Some("soon")).expect_err("not a duration");
        assert!(
            format!("{err:#}").contains("LEGION_REQUEST_TIMEOUT"),
            "{err:#}"
        );
        Ok(())
    }
}
