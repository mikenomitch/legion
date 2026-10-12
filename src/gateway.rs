//! `legion start gateway`: wake on request (docs/gateway.md).
//!
//! A stateless front door for keyed instances whose task declares
//! `suspend.wake`. A request names an instance (by hostname,
//! `<instance>.<domain>`, or the `Legion-Instance` header). When the instance
//! is running, the gateway splices the connection to its port. When it is
//! suspended, the gateway asks the manager to resume it (`StartInstance`),
//! holds the request until the instance accepts a TCP connection on that
//! port, and then splices. Past `wake.timeout` it answers `503` with
//! `Retry-After`.
//!
//! The gateway keeps no durable state. Everything it knows comes from
//! `GetInstance`, plus a few seconds of cached endpoints so a burst of
//! requests does not cost a manager call each, and so a running instance
//! stays reachable while the manager is briefly unavailable.
//!
//! Routing reads only the first request head on a connection. The rest of
//! the connection (keep-alive requests, a WebSocket upgrade, a request body)
//! is spliced byte for byte, so the gateway works with any HTTP/1.1 client
//! and, through `CONNECT <instance>:<port>`, with any TCP protocol.

use crate::prelude::*;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::{Code, Status};

/// Largest request head the gateway reads before refusing the request.
const MAX_HEAD_BYTES: usize = 16 * 1024;
/// How long a client has to send its request head.
const HEAD_TIMEOUT: Duration = Duration::from_secs(10);
/// Between `GetInstance` polls while a resume runs.
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// One dial attempt to an instance's port.
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a running instance's endpoints are trusted without asking the
/// manager again.
const ENDPOINT_CACHE_TTL: Duration = Duration::from_secs(5);
/// A `StartInstance` this gateway sent for an instance suppresses another for
/// this long, so concurrent requests share one resume.
const START_DEDUPE: Duration = Duration::from_secs(10);
/// `Retry-After` on a held request that timed out, and when the manager is
/// unreachable.
const RETRY_AFTER_SECS: u64 = 5;

/// How the gateway is started (`legion start gateway`).
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Where the gateway listens.
    pub listen: SocketAddr,
    /// The wildcard domain: `<instance>.<domain>` names an instance. `None`
    /// routes by the `Legion-Instance` header only.
    pub domain: Option<String>,
    /// Terminate TLS with this certificate chain and key (PEM file paths).
    /// `None` serves plain HTTP, for a gateway behind a load balancer that
    /// terminates TLS itself.
    pub tls: Option<(String, String)>,
}

/// A rustls acceptor from a PEM chain and key on disk.
fn tls_acceptor(cert_path: &str, key_path: &str) -> R<tokio_rustls::TlsAcceptor> {
    use crate::worker::netd::tls::{certs_from_pem, key_from_pem};
    let chain =
        certs_from_pem(&std::fs::read(cert_path).with_context(|| format!("reading {cert_path}"))?)?;
    if chain.is_empty() {
        bail!("{cert_path} holds no certificate");
    }
    let key =
        key_from_pem(&std::fs::read(key_path).with_context(|| format!("reading {key_path}"))?)?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|err| anyhow::anyhow!("{cert_path} and {key_path} do not match: {err}"))?;
    // The gateway reads one HTTP/1.1 head and splices; it does not speak h2.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// What the gateway reads from `GetInstance`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceView {
    /// `active`, `pending`, `suspended`, `stopped`, ...
    pub state: String,
    /// The task's `suspend.wake` block; `None` is never routed.
    pub wake: Option<WakeView>,
    /// `(port label, address, number)` for the current allocation.
    pub endpoints: Vec<(String, String, u16)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeView {
    pub port: String,
    pub timeout: Duration,
}

impl InstanceView {
    /// The `host:port` serving `port` (a label, or a port number), when the
    /// current allocation has one with an address.
    fn endpoint(&self, port: &str) -> Option<String> {
        self.endpoints
            .iter()
            .find(|(label, _, number)| label == port || number.to_string() == port)
            .filter(|(_, address, number)| !address.is_empty() && *number != 0)
            .map(|(_, address, number)| join_host_port(address, *number))
    }
}

fn join_host_port(address: &str, port: u16) -> String {
    if address.contains(':') && !address.starts_with('[') {
        format!("[{address}]:{port}")
    } else {
        format!("{address}:{port}")
    }
}

/// The manager calls the gateway makes. A trait so tests can drive the
/// gateway without a manager.
#[tonic::async_trait]
pub trait Control: Send + Sync + 'static {
    /// `GetInstance`; `Ok(None)` when no such instance exists.
    async fn get(&self, instance_id: &str) -> Result<Option<InstanceView>, Status>;
    /// `StartInstance`: resume a suspended instance.
    async fn start(&self, instance_id: &str) -> Result<(), Status>;
}

/// [`Control`] over a manager connection (`LEGION_ADDR`, `LEGION_TOKEN`).
pub struct ManagerControl {
    client: crate::cmd::helpers::AuthedManagerClient,
}

impl ManagerControl {
    pub fn new(client: crate::cmd::helpers::AuthedManagerClient) -> Self {
        ManagerControl { client }
    }
}

#[tonic::async_trait]
impl Control for ManagerControl {
    async fn get(&self, instance_id: &str) -> Result<Option<InstanceView>, Status> {
        let reply = self
            .client
            .clone()
            .get_instance(crate::structs::GetInstanceRequest {
                instance_id: instance_id.to_string(),
            })
            .await;
        let info = match reply {
            Ok(reply) => reply.into_inner().instance,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(status),
        };
        Ok(info.map(|info| InstanceView {
            state: info.state,
            wake: info.wake.map(|wake| WakeView {
                port: wake.port,
                timeout: Duration::from_millis(wake.timeout_ms),
            }),
            endpoints: info
                .endpoints
                .into_iter()
                .map(|endpoint| {
                    (
                        endpoint.port,
                        endpoint.address,
                        u16::try_from(endpoint.number).unwrap_or(0),
                    )
                })
                .collect(),
        }))
    }

    async fn start(&self, instance_id: &str) -> Result<(), Status> {
        self.client
            .clone()
            .start_instance(crate::structs::StartInstanceRequest {
                instance_id: instance_id.to_string(),
                ..Default::default()
            })
            .await
            .map(|_| ())
    }
}

/// Run the gateway until the process is stopped.
pub async fn run_gateway(config: GatewayConfig) -> R<()> {
    let acceptor = config
        .tls
        .as_ref()
        .map(|(cert, key)| tls_acceptor(cert, key))
        .transpose()?;
    let client = crate::cmd::helpers::connect_manager_client().await?;
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to listen on {}", config.listen))?;
    info!(
        listen = %config.listen,
        domain = config.domain.as_deref().unwrap_or(""),
        tls = acceptor.is_some(),
        "Gateway listening"
    );
    let gateway = Arc::new(Gateway::new(ManagerControl::new(client), config.domain));
    tokio::select! {
        result = gateway.serve(listener, acceptor) => result,
        _ = tokio::signal::ctrl_c() => {
            info!("Gateway stopping");
            Ok(())
        }
    }
}

/// The gateway's routing and wake logic over any [`Control`].
pub struct Gateway<C> {
    control: C,
    domain: Option<String>,
    /// Endpoints of instances seen running, and when.
    cache: Mutex<HashMap<String, (InstanceView, Instant)>>,
    /// When this gateway last sent `StartInstance` for an instance.
    started: Mutex<HashMap<String, Instant>>,
}

/// Where a request goes, from its first head.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    /// Splice to an instance's port. `connect` answers `200` first instead
    /// of forwarding the head.
    Instance {
        id: String,
        port: Option<String>,
        connect: bool,
    },
    /// `POST /v1/wake/<instance>`: resume and report, no splice.
    Wake { id: String },
    /// `GET /v1/health`.
    Health,
    /// Nothing names an instance.
    Unroutable(String),
}

/// A request head as read off the wire.
#[derive(Debug)]
struct Head {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    /// Everything read so far: the head and any bytes after it.
    raw: Vec<u8>,
    /// Where the head ends in `raw`.
    head_len: usize,
}

impl Head {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Why a request could not be routed, as the client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayError {
    /// 404: no instance by that name accepts wake on request here.
    NotFound(String),
    /// 404: the instance does not serve that port.
    BadPort { id: String, port: String },
    /// 503: the instance is stopped or cannot be resumed.
    NotRunnable { id: String, reason: String },
    /// 503 + Retry-After: still resuming when the hold ran out.
    Timeout { id: String, waited: Duration },
    /// 502 + Retry-After: the manager could not be asked.
    Control(String),
    /// 400 / 431: the request itself.
    BadRequest(u16, String),
}

impl GatewayError {
    fn status(&self) -> (u16, &'static str) {
        match self {
            GatewayError::NotFound(_) | GatewayError::BadPort { .. } => (404, "Not Found"),
            GatewayError::NotRunnable { .. } | GatewayError::Timeout { .. } => {
                (503, "Service Unavailable")
            }
            GatewayError::Control(_) => (502, "Bad Gateway"),
            GatewayError::BadRequest(431, _) => (431, "Request Header Fields Too Large"),
            GatewayError::BadRequest(..) => (400, "Bad Request"),
        }
    }

    fn code(&self) -> &'static str {
        match self {
            GatewayError::NotFound(_) => "not_found",
            GatewayError::BadPort { .. } => "unknown_port",
            GatewayError::NotRunnable { .. } => "not_runnable",
            GatewayError::Timeout { .. } => "wake_timeout",
            GatewayError::Control(_) => "manager_unavailable",
            GatewayError::BadRequest(..) => "bad_request",
        }
    }

    fn retry_after(&self) -> Option<u64> {
        matches!(
            self,
            GatewayError::Timeout { .. } | GatewayError::Control(_)
        )
        .then_some(RETRY_AFTER_SECS)
    }

    fn message(&self) -> String {
        match self {
            GatewayError::NotFound(id) => {
                format!("no instance {id:?} accepts wake on request through this gateway")
            }
            GatewayError::BadPort { id, port } => {
                format!("instance {id:?} serves no port {port:?}")
            }
            GatewayError::NotRunnable { id, reason } => {
                format!("instance {id:?} cannot be woken: {reason}")
            }
            GatewayError::Timeout { id, waited } => format!(
                "instance {id:?} was still resuming after {}ms",
                waited.as_millis()
            ),
            GatewayError::Control(message) => {
                format!("the manager could not be reached: {message}")
            }
            GatewayError::BadRequest(_, message) => message.clone(),
        }
    }

    fn hint(&self) -> Option<String> {
        match self {
            GatewayError::NotFound(_) => Some(
                "name a keyed instance whose task sets suspend.wake, by host \
                 (<instance>.<domain>) or the Legion-Instance header"
                    .to_string(),
            ),
            GatewayError::BadPort { .. } => Some(
                "set Legion-Port to a network.ports label the task declares, or omit it to use \
                 suspend.wake.port"
                    .to_string(),
            ),
            GatewayError::NotRunnable { id, .. } => Some(format!(
                "a stopped instance is never woken by a request; run `legion instance start {id}`"
            )),
            GatewayError::Timeout { .. } => Some(
                "retry after Retry-After seconds; raise suspend.wake.timeout if resumes \
                 routinely take longer"
                    .to_string(),
            ),
            GatewayError::Control(_) => None,
            GatewayError::BadRequest(..) => None,
        }
    }

    /// The standard error envelope (docs/agent-readiness.md), as an HTTP
    /// response that closes the connection.
    fn response(&self) -> Vec<u8> {
        let (status, reason) = self.status();
        let body = serde_json::json!({
            "ok": false,
            "error": {
                "code": self.code(),
                "message": self.message(),
                "hint": self.hint(),
                "retryable": self.retry_after().is_some(),
            },
        })
        .to_string();
        let retry = self
            .retry_after()
            .map(|secs| format!("Retry-After: {secs}\r\n"))
            .unwrap_or_default();
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
             Legion-Gateway-Error: {}\r\n{retry}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.code(),
            body.len()
        )
        .into_bytes()
    }
}

/// What a successful resolve found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub address: String,
    pub port: String,
    /// Whether this request had to wait for a resume.
    pub woke: bool,
    pub waited: Duration,
}

impl<C: Control> Gateway<C> {
    pub fn new(control: C, domain: Option<String>) -> Self {
        Gateway {
            control,
            domain: domain
                .map(|domain| domain.trim().trim_matches('.').to_ascii_lowercase())
                .filter(|domain| !domain.is_empty()),
            cache: Mutex::new(HashMap::new()),
            started: Mutex::new(HashMap::new()),
        }
    }

    /// Accept connections until the listener fails.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        tls: Option<tokio_rustls::TlsAcceptor>,
    ) -> R<()> {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    warn!(error = %err, "Gateway accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let gateway = self.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                match tls {
                    None => gateway.handle(stream, peer).await,
                    Some(acceptor) => {
                        match tokio::time::timeout(HEAD_TIMEOUT, acceptor.accept(stream)).await {
                            Ok(Ok(stream)) => gateway.handle(stream, peer).await,
                            Ok(Err(err)) => {
                                debug!(%peer, error = %err, "Gateway TLS handshake failed")
                            }
                            Err(_) => debug!(%peer, "Gateway TLS handshake timed out"),
                        }
                    }
                }
            });
        }
    }

    async fn handle<S>(&self, mut client: S, peer: SocketAddr)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let head = match tokio::time::timeout(HEAD_TIMEOUT, read_head(&mut client)).await {
            Ok(Ok(Some(head))) => head,
            Ok(Ok(None)) | Err(_) => return,
            Ok(Err(err)) => {
                let _ = client.write_all(&err.response()).await;
                return;
            }
        };
        match self.route(&head) {
            Route::Health => {
                let _ = client
                    .write_all(&json_response(
                        200,
                        "OK",
                        &serde_json::json!({ "ok": true }),
                    ))
                    .await;
            }
            Route::Unroutable(message) => {
                let err = GatewayError::NotFound(message);
                let _ = client.write_all(&err.response()).await;
            }
            Route::Wake { id } => {
                let response = match self.connect(&id, None).await {
                    Ok((_, resolved)) => json_response(
                        200,
                        "OK",
                        &serde_json::json!({
                            "ok": true,
                            "data": {
                                "instance": id,
                                "address": resolved.address,
                                "port": resolved.port,
                                "woke": resolved.woke,
                                "waitedMs": resolved.waited.as_millis() as u64,
                            },
                        }),
                    ),
                    Err(err) => err.response(),
                };
                let _ = client.write_all(&response).await;
            }
            Route::Instance { id, port, connect } => {
                let (mut upstream, resolved) = match self.connect(&id, port.as_deref()).await {
                    Ok(connected) => connected,
                    Err(err) => {
                        debug!(instance_id = %id, %peer, code = err.code(), "Gateway refused request");
                        let _ = client.write_all(&err.response()).await;
                        return;
                    }
                };
                debug!(
                    instance_id = %id,
                    %peer,
                    upstream = %resolved.address,
                    woke = resolved.woke,
                    "Gateway splicing connection"
                );
                let forward = if connect {
                    if client
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await
                        .is_err()
                    {
                        return;
                    }
                    &head.raw[head.head_len..]
                } else {
                    &head.raw[..]
                };
                if upstream.write_all(forward).await.is_err() {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            }
        }
    }

    /// Decide where a head goes. An instance named by the
    /// `Legion-Instance` header or the host wins, so an app's own
    /// `/v1/health` is never shadowed by the gateway's.
    fn route(&self, head: &Head) -> Route {
        let port = head
            .header("legion-port")
            .map(|port| port.trim().to_string())
            .filter(|port| !port.is_empty());
        if head.method.eq_ignore_ascii_case("CONNECT") {
            let Some((host, target_port)) = split_authority(&head.target) else {
                return Route::Unroutable(format!(
                    "CONNECT target {:?} is not <instance>:<port>",
                    head.target
                ));
            };
            let id = self.instance_from_host(&host).unwrap_or(host);
            return Route::Instance {
                id,
                port: Some(target_port),
                connect: true,
            };
        }
        if let Some(id) = head
            .header("legion-instance")
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
        {
            return Route::Instance {
                id,
                port,
                connect: false,
            };
        }
        if let Some(id) = head
            .header("host")
            .and_then(|host| self.instance_from_host(host))
        {
            return Route::Instance {
                id,
                port,
                connect: false,
            };
        }
        let path = head.target.split('?').next().unwrap_or_default();
        if head.method.eq_ignore_ascii_case("POST") {
            if let Some(id) = path.strip_prefix("/v1/wake/") {
                let id = id.trim_end_matches('/');
                if !id.is_empty() && !id.contains('/') {
                    return Route::Wake {
                        id: percent_decode(id),
                    };
                }
            }
        }
        if head.method.eq_ignore_ascii_case("GET") && path == "/v1/health" {
            return Route::Health;
        }
        Route::Unroutable(match &self.domain {
            Some(domain) => {
                format!("the request names no instance (host <instance>.{domain} or the Legion-Instance header)")
            }
            None => "the request names no instance (the Legion-Instance header)".to_string(),
        })
    }

    /// `<instance>.<domain>` → `<instance>`, ignoring a port and a trailing
    /// dot. Only one label deep, so `a.b.<domain>` is not an instance.
    fn instance_from_host(&self, host: &str) -> Option<String> {
        let domain = self.domain.as_deref()?;
        let host = strip_port(host.trim())
            .trim_end_matches('.')
            .to_ascii_lowercase();
        let id = host.strip_suffix(domain)?.strip_suffix('.')?;
        (!id.is_empty() && !id.contains('.')).then(|| id.to_string())
    }

    /// Find the instance's port, resuming it first when it is suspended, and
    /// return an open connection to it.
    pub async fn connect(
        &self,
        id: &str,
        port: Option<&str>,
    ) -> Result<(TcpStream, Resolved), GatewayError> {
        let started_at = Instant::now();
        // A running instance seen moments ago: dial it without asking.
        if let Some((view, port)) = self.cached(id, port, false) {
            if let Some(address) = view.endpoint(&port) {
                if let Ok(stream) = dial(&address).await {
                    return Ok((
                        stream,
                        Resolved {
                            address,
                            port,
                            woke: false,
                            waited: started_at.elapsed(),
                        },
                    ));
                }
            }
            self.forget(id);
        }

        let mut view = match self.control.get(id).await {
            Ok(Some(view)) => view,
            Ok(None) => return Err(GatewayError::NotFound(id.to_string())),
            Err(status) => return self.fail_static(id, port, started_at, status).await,
        };
        let Some(wake) = view.wake.clone() else {
            return Err(GatewayError::NotFound(id.to_string()));
        };
        let port = port.map(str::to_string).unwrap_or(wake.port.clone());
        let deadline = started_at + wake.timeout;
        let mut woke = false;
        loop {
            match view.state.as_str() {
                "active" => {
                    if let Some(address) = view.endpoint(&port) {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if let Ok(stream) = dial_within(&address, remaining).await {
                            self.remember(id, &view);
                            if woke {
                                info!(
                                    instance_id = %id,
                                    waited_ms = started_at.elapsed().as_millis() as u64,
                                    "Gateway woke instance"
                                );
                            }
                            return Ok((
                                stream,
                                Resolved {
                                    address,
                                    port,
                                    woke,
                                    waited: started_at.elapsed(),
                                },
                            ));
                        }
                    } else if !view.endpoints.is_empty() {
                        return Err(GatewayError::BadPort {
                            id: id.to_string(),
                            port,
                        });
                    }
                }
                "suspended" => {
                    woke = true;
                    self.start_once(id).await?;
                }
                // A resume or (re)placement in flight, ours or anyone's.
                "pending" | "rescheduling" => woke = true,
                other => {
                    return Err(GatewayError::NotRunnable {
                        id: id.to_string(),
                        reason: format!("it is {other}"),
                    })
                }
            }
            if Instant::now() >= deadline {
                return Err(GatewayError::Timeout {
                    id: id.to_string(),
                    waited: started_at.elapsed(),
                });
            }
            tokio::time::sleep(
                POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
            view = match self.control.get(id).await {
                Ok(Some(view)) => view,
                Ok(None) => return Err(GatewayError::NotFound(id.to_string())),
                Err(status) if is_transient(&status) => continue,
                Err(status) => return Err(GatewayError::Control(status.message().to_string())),
            };
        }
    }

    /// `StartInstance`, at most once per [`START_DEDUPE`] per instance from
    /// this gateway. A start for an instance that is already starting is a
    /// no-op on the manager too, so gateways never need to coordinate.
    async fn start_once(&self, id: &str) -> Result<(), GatewayError> {
        {
            let mut started = lock(&self.started);
            let now = Instant::now();
            started.retain(|_, at| now.duration_since(*at) < START_DEDUPE);
            if started.contains_key(id) {
                return Ok(());
            }
            started.insert(id.to_string(), now);
        }
        info!(instance_id = %id, "Gateway resuming instance for a held request");
        match self.control.start(id).await {
            Ok(()) => Ok(()),
            Err(status) => {
                lock(&self.started).remove(id);
                match status.code() {
                    Code::NotFound => Err(GatewayError::NotFound(id.to_string())),
                    Code::FailedPrecondition | Code::InvalidArgument => {
                        Err(GatewayError::NotRunnable {
                            id: id.to_string(),
                            reason: crate::cmd::helpers::split_hint(status.message())
                                .0
                                .to_string(),
                        })
                    }
                    // Let the next poll try again.
                    _ if is_transient(&status) => Ok(()),
                    _ => Err(GatewayError::Control(status.message().to_string())),
                }
            }
        }
    }

    /// The manager could not answer: a running instance seen before (at any
    /// age) is still dialed, so a manager blip does not take running
    /// instances off the gateway. Nothing can be woken until it is back.
    async fn fail_static(
        &self,
        id: &str,
        port: Option<&str>,
        started_at: Instant,
        status: Status,
    ) -> Result<(TcpStream, Resolved), GatewayError> {
        if let Some((view, port)) = self.cached(id, port, true) {
            if let Some(address) = view.endpoint(&port) {
                if let Ok(stream) = dial(&address).await {
                    return Ok((
                        stream,
                        Resolved {
                            address,
                            port,
                            woke: false,
                            waited: started_at.elapsed(),
                        },
                    ));
                }
            }
        }
        warn!(instance_id = %id, code = ?status.code(), "Gateway could not ask the manager");
        Err(GatewayError::Control(status.message().to_string()))
    }

    fn cached(
        &self,
        id: &str,
        port: Option<&str>,
        stale_ok: bool,
    ) -> Option<(InstanceView, String)> {
        let cache = lock(&self.cache);
        let (view, at) = cache.get(id)?;
        if !stale_ok && at.elapsed() > ENDPOINT_CACHE_TTL {
            return None;
        }
        let port = port
            .map(str::to_string)
            .or_else(|| view.wake.as_ref().map(|wake| wake.port.clone()))?;
        Some((view.clone(), port))
    }

    fn remember(&self, id: &str, view: &InstanceView) {
        let mut cache = lock(&self.cache);
        // Bounded by instances recently served; drop the stale ones as we go.
        if cache.len() > 4096 {
            cache.retain(|_, (_, at)| at.elapsed() < ENDPOINT_CACHE_TTL * 12);
        }
        cache.insert(id.to_string(), (view.clone(), Instant::now()));
    }

    fn forget(&self, id: &str) {
        lock(&self.cache).remove(id);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn is_transient(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::ResourceExhausted
    )
}

async fn dial(address: &str) -> std::io::Result<TcpStream> {
    match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(address)).await {
        Ok(result) => {
            let stream = result?;
            let _ = stream.set_nodelay(true);
            Ok(stream)
        }
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "dial timed out",
        )),
    }
}

/// Dial until the port accepts or `budget` runs out: a restored guest is
/// listening at once, a cold boot takes a moment to bind.
async fn dial_within(address: &str, budget: Duration) -> std::io::Result<TcpStream> {
    let deadline = Instant::now() + budget;
    loop {
        match dial(address).await {
            Ok(stream) => return Ok(stream),
            Err(err) if Instant::now() + POLL_INTERVAL >= deadline => return Err(err),
            Err(_) => tokio::time::sleep(POLL_INTERVAL).await,
        }
    }
}

/// Read one HTTP/1.x request head. `Ok(None)` when the client closed
/// before sending anything.
async fn read_head<S: AsyncRead + Unpin>(client: &mut S) -> Result<Option<Head>, GatewayError> {
    let mut raw = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    let head_len = loop {
        let read = client
            .read(&mut chunk)
            .await
            .map_err(|err| GatewayError::BadRequest(400, err.to_string()))?;
        if read == 0 {
            if raw.is_empty() {
                return Ok(None);
            }
            return Err(GatewayError::BadRequest(
                400,
                "the connection closed inside the request head".into(),
            ));
        }
        let scan_from = raw.len().saturating_sub(3);
        raw.extend_from_slice(&chunk[..read]);
        if let Some(end) = find_head_end(&raw[scan_from..]) {
            break scan_from + end;
        }
        if raw.len() > MAX_HEAD_BYTES {
            return Err(GatewayError::BadRequest(
                431,
                format!("the request head is larger than {MAX_HEAD_BYTES} bytes"),
            ));
        }
    };
    let text = std::str::from_utf8(&raw[..head_len])
        .map_err(|_| GatewayError::BadRequest(400, "the request head is not UTF-8".into()))?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(GatewayError::BadRequest(
            400,
            format!("malformed request line {request_line:?}"),
        ));
    };
    if !version.starts_with("HTTP/1.") {
        return Err(GatewayError::BadRequest(
            400,
            format!("{version} is not supported; the gateway speaks HTTP/1.x"),
        ));
    }
    let headers = lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();
    Ok(Some(Head {
        method: method.to_string(),
        target: target.to_string(),
        headers,
        raw,
        head_len,
    }))
}

/// The index just past `\r\n\r\n`.
fn find_head_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| at + 4)
}

fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return host;
    }
    match host.rsplit_once(':') {
        Some((name, port)) if port.chars().all(|c| c.is_ascii_digit()) => name,
        _ => host,
    }
}

/// `host:port` → (host, port); the port may be a label or a number.
fn split_authority(target: &str) -> Option<(String, String)> {
    let (host, port) = target.trim().rsplit_once(':')?;
    let host = host.trim_end_matches('.').to_string();
    (!host.is_empty() && !port.is_empty()).then(|| (host, port.to_string()))
}

/// Instance ids are plain, but a client may percent-encode one in a path.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&raw[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn json_response(status: u16, reason: &str, body: &serde_json::Value) -> Vec<u8> {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[cfg(test)]
mod gateway_test {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A manager stand-in: one instance whose state the test drives. A
    /// `start` flips a suspended instance to `pending`, then to `active` on
    /// `backend` after `resume_after`.
    struct FakeControl {
        view: Arc<Mutex<Option<InstanceView>>>,
        starts: Arc<AtomicUsize>,
        backend: u16,
        resume_after: Duration,
    }

    #[tonic::async_trait]
    impl Control for FakeControl {
        async fn get(&self, id: &str) -> Result<Option<InstanceView>, Status> {
            Ok(lock(&self.view).clone().filter(|_| id == "ws-1"))
        }

        async fn start(&self, _id: &str) -> Result<(), Status> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            if let Some(view) = lock(&self.view).as_mut() {
                view.state = "pending".into();
            }
            let view = self.view.clone();
            let (backend, after) = (self.backend, self.resume_after);
            tokio::spawn(async move {
                tokio::time::sleep(after).await;
                if let Some(view) = lock(&view).as_mut() {
                    view.state = "active".into();
                    view.endpoints = vec![("http".into(), "127.0.0.1".into(), backend)];
                }
            });
            Ok(())
        }
    }

    fn suspended(timeout: Duration) -> InstanceView {
        InstanceView {
            state: "suspended".into(),
            wake: Some(WakeView {
                port: "http".into(),
                timeout,
            }),
            endpoints: Vec::new(),
        }
    }

    /// An origin that answers every request with its request line.
    async fn origin() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let Ok(Some(head)) = read_head(&mut stream).await else {
                        return;
                    };
                    let body = format!("{} {}", head.method, head.target);
                    let _ = stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                });
            }
        });
        port
    }

    async fn start_gateway(control: FakeControl) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let gateway = Arc::new(Gateway::new(control, Some("gw.test".into())));
        tokio::spawn(gateway.serve(listener, None));
        port
    }

    async fn send(port: u16, raw: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        out
    }

    fn fake(
        view: Option<InstanceView>,
        backend: u16,
        resume_after: Duration,
    ) -> (FakeControl, Arc<AtomicUsize>) {
        let starts = Arc::new(AtomicUsize::new(0));
        (
            FakeControl {
                view: Arc::new(Mutex::new(view)),
                starts: starts.clone(),
                backend,
                resume_after,
            },
            starts,
        )
    }

    fn head(raw: &str) -> Head {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut bytes = raw.as_bytes();
        rt.block_on(read_head(&mut bytes)).unwrap().unwrap()
    }

    #[test]
    fn routes_by_header_host_connect_and_gateway_paths_test() {
        let (control, _) = fake(None, 0, Duration::ZERO);
        let gateway = Gateway::new(control, Some("GW.test.".into()));
        let instance = |id: &str, port: Option<&str>, connect: bool| Route::Instance {
            id: id.into(),
            port: port.map(str::to_string),
            connect,
        };
        assert_eq!(
            gateway.route(&head("GET / HTTP/1.1\r\nHost: ws-1.gw.test:4780\r\n\r\n")),
            instance("ws-1", None, false)
        );
        assert_eq!(
            gateway.route(&head(
                "GET /v1/health HTTP/1.1\r\nHost: gw.test\r\nLegion-Instance: Ws-1\r\nLegion-Port: admin\r\n\r\n"
            )),
            instance("Ws-1", Some("admin"), false),
            "the header wins over gateway paths and keeps the id's case"
        );
        assert_eq!(
            gateway.route(&head(
                "GET /v1/health HTTP/1.1\r\nHost: ws-1.gw.test\r\n\r\n"
            )),
            instance("ws-1", None, false),
            "an app's own /v1/health is not shadowed"
        );
        assert_eq!(
            gateway.route(&head("CONNECT ws-1.gw.test:5432 HTTP/1.1\r\n\r\n")),
            instance("ws-1", Some("5432"), true)
        );
        assert_eq!(
            gateway.route(&head("CONNECT ws-1:db HTTP/1.1\r\n\r\n")),
            instance("ws-1", Some("db"), true)
        );
        assert_eq!(
            gateway.route(&head(
                "POST /v1/wake/ws%2D1 HTTP/1.1\r\nHost: gw.test\r\n\r\n"
            )),
            Route::Wake { id: "ws-1".into() }
        );
        assert_eq!(
            gateway.route(&head("GET /v1/health HTTP/1.1\r\nHost: gw.test\r\n\r\n")),
            Route::Health
        );
        assert!(matches!(
            gateway.route(&head("GET / HTTP/1.1\r\nHost: a.b.gw.test\r\n\r\n")),
            Route::Unroutable(_)
        ));
        assert!(matches!(
            gateway.route(&head("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")),
            Route::Unroutable(_)
        ));
    }

    #[tokio::test]
    async fn suspended_instance_is_resumed_once_for_concurrent_requests_test() {
        let backend = origin().await;
        let (control, starts) = fake(
            Some(suspended(Duration::from_secs(10))),
            backend,
            Duration::from_millis(300),
        );
        let port = start_gateway(control).await;
        let requests = (0..3).map(|n| {
            tokio::spawn(async move {
                send(
                    port,
                    &format!("GET /x/{n} HTTP/1.1\r\nHost: ws-1.gw.test\r\n\r\n"),
                )
                .await
            })
        });
        for (n, request) in requests.enumerate() {
            let response = request.await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
            assert!(response.ends_with(&format!("GET /x/{n}")), "{response}");
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1, "one resume for the burst");

        // Running now: served from the endpoint cache, no new start.
        let response = send(port, "GET /again HTTP/1.1\r\nLegion-Instance: ws-1\r\n\r\n").await;
        assert!(response.ends_with("GET /again"), "{response}");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn held_request_times_out_with_retry_after_test() {
        let (control, starts) = fake(
            Some(suspended(Duration::from_millis(600))),
            1,
            Duration::from_secs(60),
        );
        let port = start_gateway(control).await;
        let response = send(port, "GET / HTTP/1.1\r\nHost: ws-1.gw.test\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.contains("Retry-After: 5"), "{response}");
        assert!(
            response.contains("Legion-Gateway-Error: wake_timeout"),
            "{response}"
        );
        assert!(response.contains("\"retryable\":true"), "{response}");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn instances_without_wake_and_unknown_ids_are_not_found_test() {
        let mut view = suspended(Duration::from_secs(1));
        view.wake = None;
        let (control, starts) = fake(Some(view), 1, Duration::ZERO);
        let port = start_gateway(control).await;
        for host in ["ws-1.gw.test", "nope.gw.test"] {
            let response = send(port, &format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n")).await;
            assert!(response.starts_with("HTTP/1.1 404"), "{response}");
            assert!(
                response.contains("accepts wake on request through this gateway"),
                "{response}"
            );
        }
        assert_eq!(starts.load(Ordering::SeqCst), 0, "nothing is resumed");
    }

    #[tokio::test]
    async fn stopped_instance_is_not_woken_test() {
        let mut view = suspended(Duration::from_secs(1));
        view.state = "stopped".into();
        let (control, starts) = fake(Some(view), 1, Duration::ZERO);
        let port = start_gateway(control).await;
        let response = send(port, "GET / HTTP/1.1\r\nHost: ws-1.gw.test\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(!response.contains("Retry-After"), "{response}");
        assert!(
            response.contains("legion instance start ws-1"),
            "{response}"
        );
        assert_eq!(starts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_webhook_resumes_and_reports_the_address_test() {
        let backend = origin().await;
        let (control, starts) = fake(
            Some(suspended(Duration::from_secs(10))),
            backend,
            Duration::from_millis(100),
        );
        let port = start_gateway(control).await;
        let response = send(port, "POST /v1/wake/ws-1 HTTP/1.1\r\nHost: gw.test\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        let body: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["data"]["address"], format!("127.0.0.1:{backend}"));
        assert_eq!(body["data"]["port"], "http");
        assert_eq!(body["data"]["woke"], true);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn connect_splices_any_declared_port_test() {
        let backend = origin().await;
        let mut view = suspended(Duration::from_secs(5));
        view.state = "active".into();
        view.endpoints = vec![("http".into(), "127.0.0.1".into(), backend)];
        let (control, _) = fake(Some(view), backend, Duration::ZERO);
        let port = start_gateway(control).await;
        let response = send(
            port,
            "CONNECT ws-1:http HTTP/1.1\r\n\r\nGET /inner HTTP/1.1\r\nHost: x\r\n\r\n",
        )
        .await;
        assert!(
            response.starts_with("HTTP/1.1 200 Connection Established\r\n\r\nHTTP/1.1 200 OK"),
            "{response}"
        );
        assert!(response.ends_with("GET /inner"), "{response}");

        let response = send(port, "CONNECT ws-1:22 HTTP/1.1\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        assert!(response.contains("unknown_port"), "{response}");
    }

    #[tokio::test]
    async fn terminates_tls_and_routes_by_the_decrypted_host_test() {
        let backend = origin().await;
        let mut view = suspended(Duration::from_secs(5));
        view.state = "active".into();
        view.endpoints = vec![("http".into(), "127.0.0.1".into(), backend)];
        let (control, _) = fake(Some(view), backend, Duration::ZERO);

        let cert = rcgen::generate_simple_self_signed(vec!["*.gw.test".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = (dir.path().join("cert.pem"), dir.path().join("key.pem"));
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
        let acceptor =
            tls_acceptor(cert_path.to_str().unwrap(), key_path.to_str().unwrap()).unwrap();
        assert!(tls_acceptor(key_path.to_str().unwrap(), key_path.to_str().unwrap()).is_err());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let gateway = Arc::new(Gateway::new(control, Some("gw.test".into())));
        tokio::spawn(gateway.serve(listener, Some(acceptor)));

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let name = rustls_pki_types::ServerName::try_from("ws-1.gw.test").unwrap();
        let mut tls = connector.connect(name, tcp).await.unwrap();
        tls.write_all(b"GET /secure HTTP/1.1\r\nHost: ws-1.gw.test\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tls.read_to_end(&mut out).await;
        let out = String::from_utf8_lossy(&out);
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.ends_with("GET /secure"), "{out}");
    }
}
