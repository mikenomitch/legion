//! The worker plane: the `Worker` gRPC service every node serves on 4848,
//! always over mutual TLS (docs/auth-and-acls.md, "The worker plane";
//! docs/architecture-managers-workers.md, "Ports and who dials whom").
//!
//! Three things live here:
//!
//! - **Where the listener binds and what it advertises.** It binds
//!   `LEGION_WORKER_HOST` (default `0.0.0.0`) on `LEGION_WORKER_PORT`
//!   (default 4848) and advertises `https://<host>:<port>` as the node's
//!   `workerAddress`. The host is `--advertise-addr`, else
//!   `LEGION_WORKER_ADVERTISE_ADDR`, else the local address of the route to
//!   the manager (a connected UDP socket's local address: no packet is
//!   sent). Service discovery, the mesh, and dispatch all take the host from
//!   that one address.
//! - **The node identity.** The worker generates its own key, sends a CSR
//!   (`IssueNodeCertificate`), and serves the returned certificate. Key and
//!   certificate persist under `<state-dir>/identity/` with mode `0600`, so a
//!   restarted worker serves before it reaches a manager. The same identity
//!   is the mesh's node certificate.
//! - **Caller classes.** The listener requests a client certificate but does
//!   not require one at the handshake; each RPC declares the classes it
//!   admits ([`WORKER_RPC_CALLERS`]), checked by [`authorize`] before the
//!   handler does anything. `manager` means a client certificate that chains
//!   to the cluster CA (checked in the handshake) and carries a
//!   `legion://manager/<id>` URI SAN (checked per RPC). A node certificate is
//!   not a manager certificate.

use crate::prelude::*;

use crate::manager::mesh_ca::{
    MANAGER_URI_PREFIX, NODE_CERT_REFRESH_AFTER, NODE_CERT_TTL, NODE_URI_PREFIX,
};
use crate::structs::{manager_client::ManagerClient, worker_server::Worker};
use crate::worker::daemon::WorkerCredentials;
use crate::worker::netd::tls::{certs_from_pem, key_from_pem, MeshIdentity};

use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::CertificateDer;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::Channel;
use tonic::Status;

/// Address the worker plane binds (an IP; default `0.0.0.0`).
pub const ENV_WORKER_HOST: &str = "LEGION_WORKER_HOST";
/// Port the worker plane binds and advertises (default 4848).
pub const ENV_WORKER_PORT: &str = "LEGION_WORKER_PORT";
/// Host, or `host:port`, that managers and routers dial (default: detected).
pub const ENV_WORKER_ADVERTISE_ADDR: &str = "LEGION_WORKER_ADVERTISE_ADDR";

/// A TLS handshake that has not finished in this long is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// A handshake that suggests the cluster CA was rotated asks for a fresh
/// node certificate at most this often.
const ROTATION_SIGNAL_INTERVAL: Duration = Duration::from_secs(60);
/// How long the eager issuance at registration may take before the worker
/// moves on (and retries on its heartbeat loop).
const ISSUE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Caller classes
// ---------------------------------------------------------------------------

/// Who may call a worker RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    /// A client certificate from the cluster CA carrying a
    /// `legion://manager/<id>` URI SAN.
    Manager,
    /// A signed ticket in the request body, verified offline by the handler
    /// (the boot ticket for `StartClaim`). No client certificate is needed,
    /// and one that is presented is ignored.
    Ticket,
}

/// Every `Worker` RPC and the caller classes it admits. The handlers consult
/// this table through [`authorize`], and `tests/worker_plane.rs`
/// (`every_worker_rpc_is_classified`) fails the build when a `Worker` RPC in
/// `proto/legion.proto` is missing from it.
///
/// `ExecTask` admits only the manager today. The sandbox-exec plan adds the
/// `ticket` class (direct exec behind `exec.direct = true`); until then a
/// shell into a task always goes through a manager, which applies ACLs.
pub const WORKER_RPC_CALLERS: &[(&str, &[Caller])] = &[
    ("PostWork", &[Caller::Manager]),
    ("RestartWorker", &[Caller::Manager]),
    ("GetTaskLogs", &[Caller::Manager]),
    ("ExecTask", &[Caller::Manager]),
    ("StartClaim", &[Caller::Ticket]),
];

/// The caller classes `rpc` admits; empty for an RPC missing from the table.
pub fn callers_for(rpc: &str) -> &'static [Caller] {
    WORKER_RPC_CALLERS
        .iter()
        .find(|(name, _)| *name == rpc)
        .map(|(_, callers)| *callers)
        .unwrap_or(&[])
}

/// What a connection's client certificate says about the caller. The chain
/// has already been verified against the cluster CA in the handshake; this
/// only reads the role off the leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerClass {
    /// `legion://manager/<id>`.
    Manager(String),
    /// `legion://node/<id>`.
    Node(String),
    /// A certificate with neither role (its first URI SAN, else its CN).
    Other(String),
    /// No client certificate.
    Anonymous,
}

impl std::fmt::Display for PeerClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerClass::Manager(id) => write!(f, "manager {id}"),
            PeerClass::Node(id) => write!(f, "node {id}"),
            PeerClass::Other(name) => write!(f, "certificate {name}"),
            PeerClass::Anonymous => write!(f, "no client certificate"),
        }
    }
}

/// Read the role off the leaf of a (verified) chain. A certificate that names
/// a node is a node even if it also claims a manager SAN: the CA never signs
/// such a thing, so treat it as the weaker role.
pub fn peer_class(chain: Option<&[CertificateDer<'_>]>) -> PeerClass {
    use x509_parser::extensions::GeneralName;
    let Some(leaf) = chain.and_then(|chain| chain.first()) else {
        return PeerClass::Anonymous;
    };
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(leaf.as_ref()) else {
        return PeerClass::Other("unparseable certificate".into());
    };
    let mut manager = None;
    let mut node = None;
    let mut first_uri = None;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::URI(uri) = name {
                if let Some(id) = uri.strip_prefix(NODE_URI_PREFIX) {
                    node.get_or_insert_with(|| id.to_string());
                } else if let Some(id) = uri.strip_prefix(MANAGER_URI_PREFIX) {
                    manager.get_or_insert_with(|| id.to_string());
                }
                first_uri.get_or_insert_with(|| uri.to_string());
            }
        }
    }
    if let Some(id) = node.filter(|id| !id.is_empty()) {
        return PeerClass::Node(id);
    }
    if let Some(id) = manager.filter(|id| !id.is_empty()) {
        return PeerClass::Manager(id);
    }
    let name = first_uri.or_else(|| {
        cert.subject()
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok())
            .map(str::to_string)
    });
    PeerClass::Other(name.unwrap_or_else(|| "unnamed certificate".into()))
}

/// Admit or refuse one call to `rpc` by its caller classes. Call it first in
/// every `Worker` handler, before the request body is read. A refusal is
/// `Unauthenticated` and names what was missing.
#[allow(clippy::result_large_err)]
pub fn authorize<T>(request: &tonic::Request<T>, rpc: &'static str) -> Result<PeerClass, Status> {
    let callers = callers_for(rpc);
    let certs = request.peer_certs();
    let peer = peer_class(certs.as_deref().map(Vec::as_slice));
    if callers.contains(&Caller::Manager) && matches!(peer, PeerClass::Manager(_)) {
        return Ok(peer);
    }
    if callers.contains(&Caller::Ticket) {
        // Two classes on one RPC: a certificate that is not a manager's is
        // refused, never downgraded to the ticket path.
        let foreign_cert = !matches!(peer, PeerClass::Anonymous | PeerClass::Manager(_));
        if !(callers.contains(&Caller::Manager) && foreign_cert) {
            return Ok(peer);
        }
    }
    let message = refusal_message(rpc, callers, &peer);
    warn!(
        rpc,
        remote = ?request.remote_addr(),
        peer = %peer,
        "Refused a worker-plane call"
    );
    Err(Status::unauthenticated(message))
}

fn refusal_message(rpc: &str, callers: &[Caller], peer: &PeerClass) -> String {
    let base = match callers {
        [] => format!("{rpc} has no caller class on this worker; refusing it"),
        callers if callers.contains(&Caller::Ticket) => format!(
            "{rpc} admits a manager certificate or an exec ticket; this request carried neither"
        ),
        _ => format!("{rpc} is a manager-only worker RPC: present a Legion manager certificate"),
    };
    match peer {
        PeerClass::Node(id) => format!(
            "{base} (this connection presented the node certificate of {id}; node certificates \
             cannot call it)"
        ),
        PeerClass::Other(name) => {
            format!("{base} (this connection presented {name}, which is not a manager certificate)")
        }
        PeerClass::Manager(_) | PeerClass::Anonymous => base,
    }
}

// ---------------------------------------------------------------------------
// Bind and advertise
// ---------------------------------------------------------------------------

static ADVERTISE_FLAG: OnceLock<String> = OnceLock::new();

/// Record `--advertise-addr` from `legion join` / `legion start worker`. It
/// wins over `LEGION_WORKER_ADVERTISE_ADDR`. Set once, before the worker
/// starts.
pub fn set_advertise_addr_flag(value: &str) {
    let _ = ADVERTISE_FLAG.set(value.to_string());
}

/// The port the worker plane binds and advertises: `LEGION_WORKER_PORT`, or
/// 4848. Several workers on one host (local multi-node testing) each set
/// their own.
pub fn worker_port() -> u16 {
    std::env::var(ENV_WORKER_PORT)
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|port| *port > 0)
        .unwrap_or(DEF_WORKER_SERVER_PORT as u16)
}

/// The address the worker plane binds: `LEGION_WORKER_HOST`, or `0.0.0.0`.
pub fn bind_ip() -> R<IpAddr> {
    match std::env::var(ENV_WORKER_HOST) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .with_context(|| format!("{ENV_WORKER_HOST}={raw:?} is not an IP address")),
        _ => Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
    }
}

/// Parse an advertise value: `host`, `host:port`, `[v6]`, `[v6]:port`, or a
/// bare IPv6 address. A URL is refused (the worker adds `https://` itself).
pub fn parse_advertise(raw: &str) -> R<(String, Option<u16>)> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("the advertise address is empty");
    }
    if raw.contains("://") || raw.contains('/') {
        bail!(
            "{raw:?} is not a host or host:port (give e.g. 10.0.1.7 or 10.0.1.7:4848; the worker \
             adds https:// itself)"
        );
    }
    let parse_port = |port: &str| -> R<u16> {
        let port: u16 = port
            .parse()
            .with_context(|| format!("{port:?} in {raw:?} is not a port"))?;
        if port == 0 {
            bail!("port 0 in {raw:?} cannot be dialed");
        }
        Ok(port)
    };
    if let Some(rest) = raw.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .with_context(|| format!("{raw:?} has an unclosed ["))?;
        host.parse::<Ipv6Addr>()
            .with_context(|| format!("{host:?} in {raw:?} is not an IPv6 address"))?;
        let port = match after {
            "" => None,
            other => Some(parse_port(other.strip_prefix(':').with_context(|| {
                format!("unexpected {other:?} after ] in {raw:?}")
            })?)?),
        };
        return Ok((host.to_string(), port));
    }
    if let Ok(ip) = raw.parse::<IpAddr>() {
        return Ok((ip.to_string(), None));
    }
    match raw.rsplit_once(':') {
        Some((host, port)) => {
            if host.is_empty() || host.contains(':') {
                bail!("{raw:?} is not a host or host:port (bracket IPv6 addresses: [::1]:4848)");
            }
            Ok((host.to_string(), Some(parse_port(port)?)))
        }
        None => Ok((raw.to_string(), None)),
    }
}

/// `https://host:port`, bracketing an IPv6 host.
pub fn https_address(host: &str, port: u16) -> String {
    if host.parse::<Ipv6Addr>().is_ok() {
        format!("https://[{host}]:{port}")
    } else {
        format!("https://{host}:{port}")
    }
}

/// The local address of the route this worker uses to reach `manager_addr`:
/// a UDP socket is connected toward the manager and its local address read
/// back. Nothing is sent. A worker co-located with its manager gets
/// `127.0.0.1`; on a VPC it gets the private IP managers on the same network
/// dial. `None` when the manager address does not resolve.
pub async fn detect_advertise_ip(manager_addr: &str) -> Option<IpAddr> {
    let with_scheme = if manager_addr.contains("://") {
        manager_addr.to_string()
    } else {
        format!("http://{manager_addr}")
    };
    let uri: http::Uri = with_scheme.parse().ok()?;
    let host = uri.host()?.trim_start_matches('[').trim_end_matches(']');
    let port = uri.port_u16().unwrap_or(DEF_MANAGER_SERVER_PORT as u16);
    let target = tokio::net::lookup_host((host, port)).await.ok()?.next()?;
    let bind: SocketAddr = if target.is_ipv4() {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    } else {
        (Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = std::net::UdpSocket::bind(bind).ok()?;
    socket.connect(target).ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified()).then_some(ip)
}

/// Where the advertised host came from, for the startup log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvertiseSource {
    /// `--advertise-addr`.
    Flag,
    /// `LEGION_WORKER_ADVERTISE_ADDR`.
    Env,
    /// The route toward the manager.
    Detected,
    /// Detection failed; loopback.
    Fallback,
}

impl AdvertiseSource {
    pub fn as_str(self) -> &'static str {
        match self {
            AdvertiseSource::Flag => "--advertise-addr",
            AdvertiseSource::Env => ENV_WORKER_ADVERTISE_ADDR,
            AdvertiseSource::Detected => "route to the manager",
            AdvertiseSource::Fallback => "fallback",
        }
    }
}

/// The explicit advertise value and where it came from, if any.
fn advertise_override() -> Option<(String, AdvertiseSource)> {
    if let Some(flag) = ADVERTISE_FLAG.get() {
        return Some((flag.clone(), AdvertiseSource::Flag));
    }
    match std::env::var(ENV_WORKER_ADVERTISE_ADDR) {
        Ok(value) if !value.trim().is_empty() => Some((value, AdvertiseSource::Env)),
        _ => None,
    }
}

/// The worker plane's bind address and the address it advertises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneAddrs {
    pub bind: SocketAddr,
    pub advertise_host: String,
    pub advertise_port: u16,
    pub source: AdvertiseSource,
}

impl PlaneAddrs {
    /// The node's `workerAddress`: `https://<advertised host>:<port>`.
    pub fn worker_address(&self) -> String {
        https_address(&self.advertise_host, self.advertise_port)
    }

    /// Resolve the bind address from the environment and the advertised
    /// address from the flag, the environment, or the route to
    /// `manager_addr`, in that order.
    pub async fn resolve(manager_addr: &str) -> R<PlaneAddrs> {
        let port = worker_port();
        Self::resolve_with(manager_addr, SocketAddr::new(bind_ip()?, port), port).await
    }

    /// [`Self::resolve`] for a listener whose bind address is already known
    /// (the simulator binds an ephemeral port).
    pub async fn resolve_with(
        manager_addr: &str,
        bind: SocketAddr,
        default_port: u16,
    ) -> R<PlaneAddrs> {
        let (advertise_host, advertise_port, source) = match advertise_override() {
            Some((value, source)) => {
                let (host, port) = parse_advertise(&value)
                    .with_context(|| format!("invalid {}", source.as_str()))?;
                (host, port.unwrap_or(default_port), source)
            }
            None => match detect_advertise_ip(manager_addr).await {
                Some(ip) => (ip.to_string(), default_port, AdvertiseSource::Detected),
                None => {
                    warn!(
                        %manager_addr,
                        "Could not find the route to the manager; advertising 127.0.0.1 for the \
                         worker plane. Set {ENV_WORKER_ADVERTISE_ADDR} (or --advertise-addr) to \
                         the address managers should dial"
                    );
                    (
                        DEF_MANAGER_HOSTNAME.to_string(),
                        default_port,
                        AdvertiseSource::Fallback,
                    )
                }
            },
        };
        Ok(PlaneAddrs {
            bind,
            advertise_host,
            advertise_port,
            source,
        })
    }
}

// ---------------------------------------------------------------------------
// The node identity
// ---------------------------------------------------------------------------

/// A node's worker-plane (and mesh) credential: its certificate, the private
/// key it generated, and the cluster CA. The key is redacted from `Debug`.
pub struct NodeIdentity {
    pub node_id: String,
    pub cert_pem: String,
    key_pem: String,
    pub ca_pem: String,
    /// Certificate expiry, Unix seconds.
    pub expires_at: i64,
}

impl std::fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("node_id", &self.node_id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

impl NodeIdentity {
    /// Validate and assemble an identity: the certificate must name
    /// `node_id`, chain to `ca_pem`, and match the key.
    pub fn new(node_id: &str, cert_pem: String, key_pem: String, ca_pem: String) -> R<Self> {
        let chain = certs_from_pem(cert_pem.as_bytes()).context("node certificate")?;
        let leaf = chain
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("node certificate PEM holds no certificate"))?;
        let ca = certs_from_pem(ca_pem.as_bytes()).context("cluster CA")?;
        if ca.is_empty() {
            bail!("cluster CA PEM holds no certificate");
        }
        match peer_class(Some(&chain)) {
            PeerClass::Node(id) if id == node_id => {}
            other => bail!("the certificate names {other}, not node {node_id}"),
        }
        let expires_at = {
            let (_, parsed) = x509_parser::parse_x509_certificate(leaf.as_ref())
                .map_err(|err| anyhow!("node certificate: {err}"))?;
            parsed.validity().not_after.timestamp()
        };
        let mut roots = RootCertStore::empty();
        for cert in &ca {
            roots.add(cert.clone())?;
        }
        WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|err| anyhow!("cluster CA verifier: {err}"))?
            .verify_client_cert(&leaf, &chain[1..], rustls_pki_types::UnixTime::now())
            .map_err(|err| {
                anyhow!("the node certificate does not chain to the cluster CA: {err}")
            })?;
        let identity = NodeIdentity {
            node_id: node_id.to_string(),
            cert_pem,
            key_pem,
            ca_pem,
            expires_at,
        };
        // Building the server config checks that the key matches the leaf.
        identity.server_config()?;
        Ok(identity)
    }

    /// Whether to ask for a fresh certificate: a third of the lifetime has
    /// passed (so a manager outage of hours never expires the node).
    pub fn needs_refresh(&self) -> bool {
        let refresh_when_remaining = (NODE_CERT_TTL - NODE_CERT_REFRESH_AFTER).as_secs() as i64;
        self.expires_at - now_unix() < refresh_when_remaining
    }

    pub fn expired(&self) -> bool {
        self.expires_at <= now_unix()
    }

    /// The listener's TLS config: serve this certificate, request (but do
    /// not require) a client certificate from the cluster CA, speak h2.
    pub fn server_config(&self) -> R<Arc<ServerConfig>> {
        let chain = certs_from_pem(self.cert_pem.as_bytes())?;
        let key = key_from_pem(self.key_pem.as_bytes())?;
        let mut roots = RootCertStore::empty();
        for cert in certs_from_pem(self.ca_pem.as_bytes())? {
            roots.add(cert)?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()
            .map_err(|err| anyhow!("worker plane client verifier: {err}"))?;
        let mut config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .map_err(|err| anyhow!("node certificate and key do not match: {err}"))?;
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Arc::new(config))
    }

    /// The same credential as the mesh's node certificate.
    pub fn mesh_identity(&self) -> R<MeshIdentity> {
        MeshIdentity::from_pem(
            &self.node_id,
            &self.cert_pem,
            &self.key_pem,
            &self.ca_pem,
            self.expires_at,
        )
    }
}

/// Where a worker persists its identity: `<state-dir>/identity/` holding
/// `node.key`, `node.pem`, and `ca.pem`, each mode `0600`.
#[derive(Debug, Clone)]
pub struct IdentityStore {
    dir: PathBuf,
}

impl IdentityStore {
    pub fn new(dir: PathBuf) -> Self {
        IdentityStore { dir }
    }

    /// The store beside a worker state file (`<state-dir>/identity`).
    pub fn beside(worker_state_path: &Path) -> Self {
        let parent = worker_state_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        IdentityStore::new(parent.join("identity"))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The persisted identity, when one exists, names `node_id`, and has not
    /// expired. Anything else is `None` (and the worker asks for a new one).
    pub fn load(&self, node_id: &str) -> R<Option<NodeIdentity>> {
        let read = |name: &str| -> R<Option<String>> {
            match std::fs::read_to_string(self.dir.join(name)) {
                Ok(raw) => Ok(Some(raw)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err).with_context(|| format!("reading {}", self.dir.display())),
            }
        };
        let (Some(cert), Some(key), Some(ca)) =
            (read("node.pem")?, read("node.key")?, read("ca.pem")?)
        else {
            return Ok(None);
        };
        let identity = NodeIdentity::new(node_id, cert, key, ca)
            .with_context(|| format!("persisted identity in {}", self.dir.display()))?;
        if identity.expired() {
            return Ok(None);
        }
        Ok(Some(identity))
    }

    /// Write the identity, each file owner-only and replaced atomically.
    pub fn save(&self, identity: &NodeIdentity) -> R<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating {}", self.dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700));
        }
        for (name, contents) in [
            ("node.key", identity.key_pem.as_str()),
            ("node.pem", identity.cert_pem.as_str()),
            ("ca.pem", identity.ca_pem.as_str()),
        ] {
            write_private(&self.dir.join(name), contents)?;
        }
        Ok(())
    }
}

/// Write `contents` to `path` with mode `0600`, via a temp file and rename.
fn write_private(path: &Path, contents: &str) -> R<()> {
    use std::io::Write;
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".tmp");
    let tmp = path.with_file_name(tmp_name);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    #[cfg(unix)]
    {
        // `mode` only applies on create; a stale temp file keeps its mode.
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(())
}

/// Ask the manager to sign a fresh key for `node_id`: generate the key
/// locally, send only the CSR, and validate what comes back.
pub async fn issue_identity(
    client: &mut ManagerClient<Channel>,
    credentials: &WorkerCredentials,
    node_id: &str,
) -> R<NodeIdentity> {
    let (csr_pem, key_pem) = crate::manager::mesh_ca::node_csr(node_id)?;
    let request = credentials.attach(tonic::Request::new(
        crate::structs::IssueNodeCertificateRequest {
            node_id: node_id.to_string(),
            csr_pem,
        },
    ));
    let reply = client.issue_node_certificate(request).await?.into_inner();
    NodeIdentity::new(node_id, reply.cert_pem, key_pem, reply.ca_pem)
        .context("the manager returned an unusable node certificate")
}

// ---------------------------------------------------------------------------
// The listener
// ---------------------------------------------------------------------------

struct Installed {
    identity: Arc<NodeIdentity>,
    config: Arc<ServerConfig>,
}

/// The listener's current certificate, swappable while it serves: each new
/// connection uses whatever is installed when it arrives, so a refreshed
/// certificate (or a rotated CA) takes effect without a restart.
pub struct WorkerPlaneTls {
    installed: RwLock<Option<Installed>>,
    ready: tokio::sync::watch::Sender<bool>,
    refresh_wanted: AtomicBool,
    last_signal: Mutex<Option<Instant>>,
}

impl WorkerPlaneTls {
    pub fn new() -> Arc<Self> {
        let (ready, _) = tokio::sync::watch::channel(false);
        Arc::new(WorkerPlaneTls {
            installed: RwLock::new(None),
            ready,
            refresh_wanted: AtomicBool::new(false),
            last_signal: Mutex::new(None),
        })
    }

    /// Serve `identity` from the next connection on.
    pub fn install(&self, identity: NodeIdentity) -> R<Arc<NodeIdentity>> {
        let config = identity.server_config()?;
        let identity = Arc::new(identity);
        *self.installed.write().unwrap_or_else(|p| p.into_inner()) = Some(Installed {
            identity: identity.clone(),
            config,
        });
        self.refresh_wanted.store(false, Ordering::Relaxed);
        self.ready.send_replace(true);
        Ok(identity)
    }

    pub fn identity(&self) -> Option<Arc<NodeIdentity>> {
        self.installed
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|installed| installed.identity.clone())
    }

    fn config(&self) -> Option<Arc<ServerConfig>> {
        self.installed
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|installed| installed.config.clone())
    }

    /// Whether the node should ask for a new certificate now: it has none,
    /// the one it has is aging, or a handshake suggested a rotated CA.
    pub fn needs_refresh(&self) -> bool {
        self.refresh_wanted.load(Ordering::Relaxed)
            || self
                .identity()
                .map(|identity| identity.needs_refresh())
                .unwrap_or(true)
    }

    /// Ask for a fresh certificate on the next heartbeat. Rate-limited, so a
    /// peer that keeps failing handshakes cannot make the node re-issue more
    /// than once a minute.
    pub fn request_refresh(&self) -> bool {
        let mut last = self.last_signal.lock().unwrap_or_else(|p| p.into_inner());
        if last.is_some_and(|at| at.elapsed() < ROTATION_SIGNAL_INTERVAL) {
            return false;
        }
        *last = Some(Instant::now());
        self.refresh_wanted.store(true, Ordering::Relaxed);
        true
    }

    /// Resolves once a certificate is installed.
    pub async fn wait_ready(&self) {
        let mut ready = self.ready.subscribe();
        while !*ready.borrow_and_update() {
            if ready.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Whether a failed handshake looks like the two sides trust different CAs:
/// the client's certificate is from an unknown issuer, or the client refused
/// ours as unknown. That is what a `legion net ca rotate` looks like from a
/// node that has not refreshed yet.
fn suggests_rotated_ca(err: &std::io::Error) -> bool {
    use rustls::{AlertDescription, CertificateError};
    let Some(inner) = err.get_ref() else {
        return false;
    };
    let Some(tls) = inner.downcast_ref::<rustls::Error>() else {
        return false;
    };
    matches!(
        tls,
        rustls::Error::InvalidCertificate(
            CertificateError::UnknownIssuer | CertificateError::BadSignature
        ) | rustls::Error::AlertReceived(
            AlertDescription::UnknownCA | AlertDescription::BadCertificate
        )
    )
}

/// Wait for a certificate, bind, log what was chosen, and serve `service`
/// over mutual TLS until the listener fails.
pub async fn serve<W: Worker>(addrs: PlaneAddrs, tls: Arc<WorkerPlaneTls>, service: W) -> R<()> {
    if tls.identity().is_none() {
        info!(
            bind = %addrs.bind,
            "Worker plane waits for its node certificate before listening"
        );
        tls.wait_ready().await;
    }
    let listener = match TcpListener::bind(addrs.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            error!(
                bind = %addrs.bind,
                error = %err,
                "Worker plane failed to bind; exec, logs, and push dispatch to this node will not \
                 work. Free the port or set {ENV_WORKER_PORT}"
            );
            return Err(err).with_context(|| format!("binding the worker plane on {}", addrs.bind));
        }
    };
    let advertised = addrs.worker_address();
    info!(
        bind = %addrs.bind,
        advertised = %advertised,
        advertise_source = addrs.source.as_str(),
        "Worker plane listening on {} (mTLS); advertised as {}",
        addrs.bind,
        advertised
    );
    serve_on(listener, tls, service).await
}

/// Serve `service` on an already-bound listener. Handshakes run on their own
/// tasks, so a slow or hostile client never holds up the accept loop.
/// Connections that arrive before a certificate is installed are closed.
pub async fn serve_on<W: Worker>(
    listener: TcpListener,
    tls: Arc<WorkerPlaneTls>,
    service: W,
) -> R<()> {
    let (tx, rx) = tokio::sync::mpsc::channel::<
        Result<tokio_rustls::server::TlsStream<TcpStream>, std::io::Error>,
    >(64);
    let accept = tokio::spawn(accept_loop(listener, tls, tx));
    let served = tonic::transport::Server::builder()
        .add_service(crate::structs::worker_server::WorkerServer::new(service))
        .serve_with_incoming(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await;
    accept.abort();
    served.context("worker plane server exited")
}

async fn accept_loop(
    listener: TcpListener,
    tls: Arc<WorkerPlaneTls>,
    tx: tokio::sync::mpsc::Sender<
        Result<tokio_rustls::server::TlsStream<TcpStream>, std::io::Error>,
    >,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                warn!(error = %err, "Worker plane accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Some(config) = tls.config() else {
            debug!(%peer, "Worker plane has no certificate yet; closing the connection");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let tx = tx.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let acceptor = tokio_rustls::TlsAcceptor::from(config);
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(stream)) => {
                    let _ = tx.send(Ok(stream)).await;
                }
                Ok(Err(err)) => {
                    if suggests_rotated_ca(&err) && tls.request_refresh() {
                        info!(
                            %peer,
                            error = %err,
                            "A worker-plane peer trusts a different cluster CA; re-issuing the \
                             node certificate on the next heartbeat"
                        );
                    } else {
                        debug!(%peer, error = %err, "Worker plane TLS handshake failed");
                    }
                }
                Err(_) => debug!(%peer, "Worker plane TLS handshake timed out"),
            }
        });
    }
}

// ---------------------------------------------------------------------------
// The node's plane (the daemon's handle)
// ---------------------------------------------------------------------------

/// A running worker's plane: its listener certificate and where it persists.
pub struct NodePlane {
    pub node_id: String,
    pub tls: Arc<WorkerPlaneTls>,
    store: Option<IdentityStore>,
}

static NODE_PLANE: RwLock<Option<Arc<NodePlane>>> = RwLock::new(None);

impl NodePlane {
    /// Set up this process's node plane, serving the persisted identity (when
    /// one is on disk and still valid) until the manager issues a fresh one.
    pub fn start(node_id: &str, worker_state_path: &Path) -> Arc<NodePlane> {
        let store = IdentityStore::beside(worker_state_path);
        let tls = WorkerPlaneTls::new();
        match store.load(node_id) {
            Ok(Some(identity)) => {
                let expires_at = identity.expires_at;
                match tls.install(identity) {
                    Ok(_) => info!(
                        node_id,
                        expires_at,
                        dir = %store.dir().display(),
                        "Serving the persisted node certificate until the manager issues a fresh one"
                    ),
                    Err(err) => warn!(error = ?err, "Ignoring the persisted node certificate"),
                }
            }
            Ok(None) => {}
            Err(err) => warn!(
                error = %format!("{err:#}"),
                "Ignoring an unusable persisted node certificate; the manager issues a new one"
            ),
        }
        let plane = Arc::new(NodePlane {
            node_id: node_id.to_string(),
            tls,
            store: Some(store),
        });
        *NODE_PLANE.write().unwrap_or_else(|p| p.into_inner()) = Some(plane.clone());
        plane
    }

    /// This process's node plane, once [`NodePlane::start`] ran.
    pub fn current() -> Option<Arc<NodePlane>> {
        NODE_PLANE.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Issue a fresh certificate (CSR), persist it, and serve it.
    pub async fn issue(
        &self,
        client: &mut ManagerClient<Channel>,
        credentials: &WorkerCredentials,
    ) -> R<Arc<NodeIdentity>> {
        let identity = issue_identity(client, credentials, &self.node_id).await?;
        if let Some(store) = &self.store {
            if let Err(err) = store.save(&identity) {
                warn!(
                    error = %format!("{err:#}"),
                    "Failed to persist the node certificate; a restart before the next manager \
                     contact will not serve the worker plane"
                );
            }
        }
        let expires_at = identity.expires_at;
        let installed = self.tls.install(identity)?;
        info!(node_id = %self.node_id, expires_at, "Node certificate issued");
        Ok(installed)
    }

    /// Right after registration: ask for a fresh certificate so the listener
    /// has one before it serves. Bounded, so a manager that cannot sign yet
    /// never blocks the worker's start; the heartbeat loop retries.
    pub async fn issue_after_registration(
        &self,
        client: &mut ManagerClient<Channel>,
        credentials: &WorkerCredentials,
    ) {
        match tokio::time::timeout(ISSUE_TIMEOUT, self.issue(client, credentials)).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => warn!(
                error = %format!("{err:#}"),
                serving_persisted = self.tls.identity().is_some(),
                "Could not issue the node certificate at registration; retrying on each heartbeat"
            ),
            Err(_) => warn!(
                serving_persisted = self.tls.identity().is_some(),
                "Issuing the node certificate timed out; retrying on each heartbeat"
            ),
        }
    }

    /// Re-issue when the certificate is missing, aging, or a handshake
    /// suggested a rotated CA. Fail-static: a failed issuance keeps serving
    /// the certificate the node has.
    pub async fn refresh_if_needed(
        &self,
        client: &mut ManagerClient<Channel>,
        credentials: &WorkerCredentials,
    ) {
        if !self.tls.needs_refresh() {
            return;
        }
        if let Err(err) = self.issue(client, credentials).await {
            match err.downcast_ref::<Status>() {
                Some(status) if status.code() == tonic::Code::Unimplemented => debug!(
                    "Manager predates node certificates; the worker plane cannot serve until it is \
                     upgraded"
                ),
                _ => warn!(
                    error = %format!("{err:#}"),
                    "Failed to issue the node certificate; keeping the current one"
                ),
            }
        }
    }

    /// Hand the current identity to the mesh datapath when it changed.
    pub fn share_with_mesh(&self, context: &crate::worker::forwarder::ForwarderContext) {
        let Some(identity) = self.tls.identity() else {
            return;
        };
        let current = context.mesh_identity().map(|mesh| mesh.expires_at);
        if current == Some(identity.expires_at) {
            return;
        }
        match identity
            .mesh_identity()
            .and_then(|mesh| context.set_mesh_identity(mesh))
        {
            Ok(()) => debug!(
                expires_at = identity.expires_at,
                "Mesh uses the node certificate"
            ),
            Err(err) => warn!(error = ?err, "Rejected the node certificate for the mesh"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests and embedders
// ---------------------------------------------------------------------------

/// Serve `service` on `127.0.0.1:<ephemeral>` with `identity`. For tests and
/// embedders that need a real mTLS worker plane without a worker daemon.
#[doc(hidden)]
pub async fn serve_for_test<W: Worker>(
    service: W,
    identity: NodeIdentity,
) -> R<(SocketAddr, tokio::task::JoinHandle<R<()>>)> {
    let tls = WorkerPlaneTls::new();
    tls.install(identity)?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    Ok((addr, tokio::spawn(serve_on(listener, tls, service))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::mesh_ca::{generate_ca, issue_manager_cert, issue_node_cert};

    fn chain(pem: &str) -> Vec<CertificateDer<'static>> {
        certs_from_pem(pem.as_bytes()).unwrap()
    }

    #[test]
    fn peer_classes_read_the_role_san() -> R<()> {
        let (ca, key) = generate_ca()?;
        let (manager, _, _) = issue_manager_cert(&ca, &key, "mgr-1")?;
        let (node, _, _) = issue_node_cert(&ca, &key, "wkr-1")?;
        assert_eq!(
            peer_class(Some(&chain(&manager))),
            PeerClass::Manager("mgr-1".into())
        );
        assert_eq!(
            peer_class(Some(&chain(&node))),
            PeerClass::Node("wkr-1".into())
        );
        assert_eq!(peer_class(None), PeerClass::Anonymous);
        assert_eq!(peer_class(Some(&[])), PeerClass::Anonymous);
        Ok(())
    }

    #[test]
    fn every_rpc_in_the_table_has_a_class() {
        for (rpc, callers) in WORKER_RPC_CALLERS {
            assert!(!callers.is_empty(), "{rpc} has no caller class");
        }
        assert!(callers_for("NoSuchRpc").is_empty());
        assert_eq!(callers_for("PostWork"), &[Caller::Manager]);
        assert_eq!(callers_for("StartClaim"), &[Caller::Ticket]);
    }

    #[test]
    fn refusals_name_what_was_missing() {
        let anonymous = refusal_message("GetTaskLogs", &[Caller::Manager], &PeerClass::Anonymous);
        assert_eq!(
            anonymous,
            "GetTaskLogs is a manager-only worker RPC: present a Legion manager certificate"
        );
        let node = refusal_message(
            "PostWork",
            &[Caller::Manager],
            &PeerClass::Node("wkr-2".into()),
        );
        assert!(node.contains("node certificate of wkr-2"), "{node}");
        let two = refusal_message(
            "ExecTask",
            &[Caller::Manager, Caller::Ticket],
            &PeerClass::Anonymous,
        );
        assert_eq!(
            two,
            "ExecTask admits a manager certificate or an exec ticket; this request carried neither"
        );
    }

    #[test]
    fn advertise_values_parse() -> R<()> {
        assert_eq!(parse_advertise("10.0.1.7")?, ("10.0.1.7".into(), None));
        assert_eq!(
            parse_advertise("10.0.1.7:9000")?,
            ("10.0.1.7".into(), Some(9000))
        );
        assert_eq!(
            parse_advertise("wkr-1.internal")?,
            ("wkr-1.internal".into(), None)
        );
        assert_eq!(
            parse_advertise("wkr-1.internal:4848")?,
            ("wkr-1.internal".into(), Some(4848))
        );
        assert_eq!(parse_advertise("fd00::7")?, ("fd00::7".into(), None));
        assert_eq!(parse_advertise("[fd00::7]")?, ("fd00::7".into(), None));
        assert_eq!(
            parse_advertise("[fd00::7]:4849")?,
            ("fd00::7".into(), Some(4849))
        );
        for bad in [
            "",
            "https://10.0.1.7:4848",
            "10.0.1.7:0",
            "10.0.1.7:http",
            "[fd00::7",
            "fd00::7]:1",
        ] {
            assert!(parse_advertise(bad).is_err(), "{bad:?} must be refused");
        }
        assert_eq!(https_address("10.0.1.7", 4848), "https://10.0.1.7:4848");
        assert_eq!(https_address("fd00::7", 4848), "https://[fd00::7]:4848");
        Ok(())
    }

    /// A worker co-located with its manager advertises loopback, exactly as
    /// before, and the detection sends nothing.
    #[tokio::test]
    async fn detection_returns_the_route_source_address() {
        assert_eq!(
            detect_advertise_ip("http://127.0.0.1:4747").await,
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );
        assert_eq!(
            detect_advertise_ip("127.0.0.1:4747").await,
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );
        // Hosts without an IPv6 loopback route detect nothing (and fall
        // back); where there is one, it is loopback.
        if let Some(ip) = detect_advertise_ip("http://[::1]:4747").await {
            assert!(ip.is_loopback(), "{ip}");
        }
        assert_eq!(
            detect_advertise_ip("http://no-such-host.invalid:4747").await,
            None
        );
    }

    #[test]
    fn identity_validates_names_chain_and_key() -> R<()> {
        let (ca, key) = generate_ca()?;
        let (other_ca, _) = generate_ca()?;
        let (cert, node_key, _) = issue_node_cert(&ca, &key, "wkr-1")?;
        let (_, wrong_key, _) = issue_node_cert(&ca, &key, "wkr-1")?;
        let identity = NodeIdentity::new("wkr-1", cert.clone(), node_key.clone(), ca.clone())?;
        assert!(!identity.needs_refresh());
        assert!(!identity.expired());
        assert!(!format!("{identity:?}").contains("PRIVATE"));
        assert!(NodeIdentity::new("wkr-2", cert.clone(), node_key.clone(), ca.clone()).is_err());
        assert!(NodeIdentity::new("wkr-1", cert.clone(), node_key.clone(), other_ca).is_err());
        assert!(NodeIdentity::new("wkr-1", cert, wrong_key, ca).is_err());
        Ok(())
    }

    #[test]
    fn identity_persists_owner_only_and_reloads() -> R<()> {
        let dir = tempfile::TempDir::new()?;
        let store = IdentityStore::beside(&dir.path().join("worker_state.json"));
        assert!(store.load("wkr-1")?.is_none());
        let (ca, key) = generate_ca()?;
        let (cert, node_key, expires_at) = issue_node_cert(&ca, &key, "wkr-1")?;
        store.save(&NodeIdentity::new("wkr-1", cert, node_key, ca)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for name in ["node.key", "node.pem", "ca.pem"] {
                let mode = std::fs::metadata(store.dir().join(name))?
                    .permissions()
                    .mode();
                assert_eq!(mode & 0o777, 0o600, "{name} must be owner-only");
            }
        }
        let loaded = store.load("wkr-1")?.expect("persisted identity");
        assert_eq!(loaded.expires_at, expires_at);
        assert!(
            store.load("wkr-2").is_err(),
            "another node's identity is unusable"
        );
        Ok(())
    }

    #[test]
    fn rotation_signals_are_rate_limited() {
        let tls = WorkerPlaneTls::new();
        assert!(tls.needs_refresh(), "no certificate yet");
        assert!(tls.request_refresh());
        assert!(
            !tls.request_refresh(),
            "a second signal within the minute is ignored"
        );
    }
}
