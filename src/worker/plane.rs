//! The worker's side of the authenticated worker plane
//! (worker-plane-auth plan §6.1, §6.4, §6.6).
//!
//! - [`NodeIdentity`]: the node's key pair, generated here and never sent
//!   anywhere, the certificate the manager signs from a CSR, and the cluster
//!   CA bundle. Persisted under the worker's state dir (mode 0600) so a
//!   worker restarted during a manager outage still serves `StartClaim`.
//!   The mesh reuses the same identity.
//! - [`serve`]: the listener. Always mutual TLS with the node certificate,
//!   on `LEGION_WORKER_HOST:LEGION_WORKER_PORT` (default `0.0.0.0:4848`).
//!   Client certificates are requested, not required, because ticket
//!   holders have none; a certificate that is presented must chain to the
//!   cluster CA or the handshake fails.
//! - [`Caller`] and the per-RPC checks: which caller classes each `Worker`
//!   RPC admits. The table `tests/worker_plane_classes.rs` checks against the
//!   proto is the source of truth.
//! - [`advertise_address`]: what the worker tells managers to dial.

use crate::cluster_tls::{
    certified_key, peer_role, server_config, ClusterClientVerifier, PeerRole, SwappableIdentity,
    SwappableRoots,
};
use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tonic::{Request, Status};

/// Env var: the address the worker plane binds. Default `0.0.0.0`.
pub const WORKER_HOST_ENV: &str = "LEGION_WORKER_HOST";
/// Env var: the host (or `host:port`) managers and routers dial. Default:
/// the local address of the route to the manager.
pub const WORKER_ADVERTISE_ENV: &str = "LEGION_WORKER_ADVERTISE_ADDR";
/// Default bind host for the worker plane.
pub const DEF_WORKER_HOST: &str = "0.0.0.0";

/// Who is calling a worker-plane RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// A manager, proven by a client certificate from the cluster CA with a
    /// `legion://manager/<id>` URI SAN.
    Manager(String),
    /// No client certificate: the request must carry a ticket the RPC
    /// verifies itself (a boot ticket on `StartClaim`, an exec ticket on
    /// `ExecTask`).
    Ticket,
}

/// The caller class of a request, from the client certificate the TLS
/// handshake verified. A certificate that is not a manager certificate (a
/// node's, a fleet's) is refused outright, never treated as "no
/// certificate": the two classes can't be mixed (plan §6.1).
pub fn caller_of<T>(request: &Request<T>, rpc: &str) -> Result<Caller, Status> {
    let Some(certs) = request.peer_certs() else {
        return Ok(Caller::Ticket);
    };
    let Some(leaf) = certs.first() else {
        return Ok(Caller::Ticket);
    };
    match peer_role(leaf.as_ref()) {
        PeerRole::Manager(id) => Ok(Caller::Manager(id)),
        other => Err(Status::unauthenticated(format!(
            "{rpc} refuses this client certificate ({}): only a Legion manager certificate \
             or no certificate at all is accepted on the worker plane",
            match other {
                PeerRole::Node(id) => format!("node {id}"),
                PeerRole::Fleet(name) => format!("fleet {name}"),
                _ => "no Legion role".to_string(),
            }
        ))),
    }
}

/// The check for a manager-only RPC (`PostWork`, `GetTaskLogs`,
/// `RestartWorker`, `Probe`). Returns the manager's id.
pub fn require_manager<T>(request: &Request<T>, rpc: &str) -> Result<String, Status> {
    match caller_of(request, rpc)? {
        Caller::Manager(id) => Ok(id),
        Caller::Ticket => Err(Status::unauthenticated(format!(
            "{rpc} is a manager-only worker RPC: present a Legion manager certificate"
        ))),
    }
}

/// The manager's CA does not match the join token's pin. Fatal at join:
/// the worker stops before trusting anything the manager sent.
#[derive(Debug)]
pub struct PinMismatch(pub String);

impl std::fmt::Display for PinMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PinMismatch {}

/// What a node holds after issuance: its certificate chain and the CA
/// bundle, PEM, and when the certificate expires.
#[derive(Debug, Clone)]
struct Issued {
    cert_pem: String,
    ca_pem: String,
    expires_at: i64,
    issued: Instant,
}

/// A node's identity on the worker plane and the mesh.
pub struct NodeIdentity {
    node_id: String,
    /// Simulator fleet name, when this identity speaks for a whole fleet.
    fleet: Option<String>,
    dir: PathBuf,
    key_pem: String,
    /// The CA pin from the join token, checked against the first bundle.
    pin: Option<String>,
    identity: Arc<SwappableIdentity>,
    roots: Mutex<Option<Arc<SwappableRoots>>>,
    issued: Mutex<Option<Issued>>,
}

impl std::fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("node_id", &self.node_id)
            .field("fleet", &self.fleet)
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

/// The join-token pin the persisted bundle was first accepted under.
const ACCEPTED_PIN_FILE: &str = "token-pin";

/// Where a worker keeps its identity: `<state dir>/identity/`.
pub fn identity_dir(state_path: &Path) -> PathBuf {
    state_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default()
        .join("identity")
}

fn write_private(path: &Path, contents: &str) -> R<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let tmp = path.with_extension("tmp");
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("failed to write {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(contents.as_bytes())?;
    file.sync_all().ok();
    std::fs::rename(&tmp, path).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

impl NodeIdentity {
    /// Loads the node's key from `dir` (generating and persisting one on
    /// first start) and any certificate a previous process persisted that
    /// has not expired. `pin` is the join token's CA pin.
    pub fn load(
        dir: &Path,
        node_id: &str,
        fleet: Option<String>,
        pin: Option<String>,
    ) -> R<Arc<Self>> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
        let key_path = dir.join("node.key");
        let key_pem = match std::fs::read_to_string(&key_path) {
            Ok(pem) if rcgen::KeyPair::from_pem(&pem).is_ok() => pem,
            _ => {
                let (key_pem, _) = crate::manager::mesh_ca::generate_csr(node_id)?;
                write_private(&key_path, &key_pem)?;
                key_pem
            }
        };
        let identity = Arc::new(NodeIdentity {
            node_id: node_id.to_string(),
            fleet,
            dir: dir.to_path_buf(),
            key_pem,
            pin,
            identity: SwappableIdentity::new(),
            roots: Mutex::new(None),
            issued: Mutex::new(None),
        });
        if let Err(err) = identity.load_persisted() {
            debug!(error = ?err, "No usable persisted node certificate");
        }
        Ok(identity)
    }

    fn load_persisted(&self) -> R<()> {
        let cert_pem = std::fs::read_to_string(self.dir.join("node.pem"))?;
        let ca_pem = std::fs::read_to_string(self.dir.join("ca.pem"))?;
        let certs = crate::cluster_tls::certs_from_pem(&cert_pem)?;
        let leaf = certs.first().context("persisted certificate is empty")?;
        let (_, parsed) = x509_parser::parse_x509_certificate(leaf.as_ref())
            .map_err(|err| anyhow!("persisted certificate: {err}"))?;
        let expires_at = parsed.validity().not_after.timestamp();
        if expires_at <= time::OffsetDateTime::now_utc().unix_timestamp() {
            bail!("persisted certificate expired");
        }
        if let Some(pin) = &self.pin {
            // A bundle accepted under this same token stays trusted after a
            // CA rotation drops the CA the token pins; a bundle from another
            // token (another cluster, a re-join) must contain its pin.
            let accepted_under =
                std::fs::read_to_string(self.dir.join(ACCEPTED_PIN_FILE)).unwrap_or_default();
            if accepted_under.trim() != pin
                && !crate::cluster_tls::bundle_contains_pin(&ca_pem, pin)?
            {
                bail!("persisted CA bundle does not contain this join token's pin");
            }
        }
        self.install(&cert_pem, &ca_pem, expires_at)?;
        info!(
            node_id = %self.node_id,
            expires_at,
            "Loaded the persisted node certificate"
        );
        Ok(())
    }

    fn install(&self, cert_pem: &str, ca_pem: &str, expires_at: i64) -> R<()> {
        self.identity.set(certified_key(cert_pem, &self.key_pem)?);
        let mut roots = self.roots.lock().unwrap_or_else(|p| p.into_inner());
        match roots.as_ref() {
            Some(existing) => existing.set_pem(ca_pem)?,
            None => *roots = Some(SwappableRoots::from_pem(ca_pem)?),
        }
        *self.issued.lock().unwrap_or_else(|p| p.into_inner()) = Some(Issued {
            cert_pem: cert_pem.to_string(),
            ca_pem: ca_pem.to_string(),
            expires_at,
            issued: Instant::now(),
        });
        Ok(())
    }

    /// True once the node holds a certificate (issued or persisted).
    pub fn has_certificate(&self) -> bool {
        self.issued
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }

    /// The hot-swappable server identity the listener presents.
    pub fn server_identity(&self) -> Arc<SwappableIdentity> {
        self.identity.clone()
    }

    /// The cluster CA bundle, once the node holds one.
    pub fn roots(&self) -> Option<Arc<SwappableRoots>> {
        self.roots.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// The CA bundle PEM, once held.
    pub fn ca_pem(&self) -> Option<String> {
        self.issued
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|issued| issued.ca_pem.clone())
    }

    /// Whether to ask for a new certificate: none held, a third of its
    /// life gone, under an hour left, or the manager's trust bundle differs
    /// from the held one (`HeartbeatReply.cluster_ca_pins`, anchor first): a
    /// different anchor means the CA that signs changed (a rotation), and a
    /// different set means a CA was added to or removed from the bundle
    /// (docs/cluster-ca.md#rotation). Either way the reply to the re-issue
    /// carries the new bundle.
    pub fn needs_refresh(&self, manager_ca_pins: Option<&[String]>) -> bool {
        let issued = self.issued.lock().unwrap_or_else(|p| p.into_inner());
        let Some(issued) = issued.as_ref() else {
            return true;
        };
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        if issued.issued.elapsed() >= crate::manager::mesh_ca::NODE_CERT_REFRESH_AFTER
            || issued.expires_at - now < 60 * 60
        {
            return true;
        }
        match manager_ca_pins.filter(|pins| !pins.is_empty()) {
            Some(pins) => !bundle_matches(&issued.ca_pem, pins),
            None => false,
        }
    }

    /// Asks the manager to sign a CSR for this node's key, checks the CA
    /// bundle in the reply against the join token's pin, installs the
    /// result, and persists it.
    pub async fn issue(
        &self,
        client: &mut ManagerClient<Channel>,
        credentials: &crate::worker::daemon::WorkerCredentials,
        manager_addr: &str,
    ) -> R<()> {
        let csr_pem = crate::manager::mesh_ca::csr_for_key(&self.key_pem, &self.node_id)?;
        let request = credentials.attach(tonic::Request::new(
            crate::structs::IssueNodeCertificateRequest {
                node_id: self.node_id.clone(),
                csr_pem,
                fleet: self.fleet.clone().unwrap_or_default(),
            },
        ));
        let reply = client
            .issue_node_certificate(request)
            .await
            .map_err(|status| {
                anyhow!(
                    "the manager refused to sign this node's certificate: {}",
                    status.message()
                )
            })?
            .into_inner();
        // The first bundle a worker accepts must contain its token's pin;
        // after that, the held bundle is what it trusts: the reply came over
        // a connection verified against it, and after a CA rotation's overlap
        // it no longer holds the CA the token pins.
        if let Some(pin) = self.pin.as_ref().filter(|_| self.roots().is_none()) {
            if !crate::cluster_tls::bundle_contains_pin(&reply.ca_pem, pin)? {
                let presented = crate::cluster_tls::bundle_pins(&reply.ca_pem);
                return Err(anyhow::Error::new(PinMismatch(
                    crate::cluster_tls::pin_mismatch_message(manager_addr, &presented, pin),
                )));
            }
        }
        self.install(&reply.cert_pem, &reply.ca_pem, reply.expires_at)?;
        write_private(&self.dir.join("node.pem"), &reply.cert_pem)?;
        write_private(&self.dir.join("ca.pem"), &reply.ca_pem)?;
        if let Some(pin) = &self.pin {
            write_private(&self.dir.join(ACCEPTED_PIN_FILE), pin)?;
        }
        // From here on the manager must chain to the bundle, which follows
        // CA rotations; the token's pin does not.
        if let Some(roots) = self.roots() {
            trust_manager_bundle(manager_addr, roots);
        }
        info!(
            node_id = %self.node_id,
            expires_at = reply.expires_at,
            "Node certificate issued"
        );
        Ok(())
    }

    /// The same identity as a mesh credential, for the datapath.
    pub fn mesh_identity(&self) -> Option<crate::worker::netd::tls::MeshIdentity> {
        let issued = self
            .issued
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()?;
        crate::worker::netd::tls::MeshIdentity::from_pem(
            &self.node_id,
            &issued.cert_pem,
            &self.key_pem,
            &issued.ca_pem,
            issued.expires_at,
        )
        .ok()
    }
}

/// Serves `router` on the worker plane listener: TLS with the node
/// certificate, client certificates verified against the cluster CA when
/// presented. Runs until the listener fails.
pub async fn serve(
    bind: std::net::SocketAddr,
    identity: Arc<NodeIdentity>,
    router: tonic::transport::server::Router,
) -> R<()> {
    let roots = identity
        .roots()
        .context("the worker plane needs a node certificate before it can listen")?;
    let config = server_config(
        identity.server_identity(),
        Some(ClusterClientVerifier::new(roots)),
        false,
    )?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind the worker plane on {bind}"))?;
    router
        .serve_with_incoming(crate::cluster_tls::tls_incoming(listener, config))
        .await
        .context("worker plane listener exited")
}

/// The address the worker plane binds: `LEGION_WORKER_HOST` (default
/// `0.0.0.0`) and `port`.
pub fn bind_address(port: u64) -> R<std::net::SocketAddr> {
    let host = std::env::var(WORKER_HOST_ENV)
        .ok()
        .map(|host| host.trim().to_string())
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| DEF_WORKER_HOST.to_string());
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let ip: std::net::IpAddr = host
        .parse()
        .with_context(|| format!("{WORKER_HOST_ENV}={host:?} is not an IP address"))?;
    Ok(std::net::SocketAddr::new(ip, port as u16))
}

/// The address managers and routers dial, as an `https://` URL.
///
/// An explicit override (`--advertise-addr`, else
/// `LEGION_WORKER_ADVERTISE_ADDR`) wins; it may be a host or `host:port`.
/// Otherwise the worker picks the local address of its route to the
/// manager (a connected UDP socket's local address; no packet is sent): the
/// private IP on a VPC, `127.0.0.1` when co-located with its manager
/// (calls entry 15).
pub fn advertise_address(manager_addr: &str, port: u64, override_addr: Option<&str>) -> R<String> {
    let explicit = override_addr
        .map(str::to_string)
        .or_else(|| std::env::var(WORKER_ADVERTISE_ENV).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(explicit) = explicit {
        let explicit = explicit
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');
        let with_port = if explicit.starts_with('[') {
            // A bracketed IPv6 literal, with or without a port.
            if explicit.contains("]:") {
                explicit.to_string()
            } else {
                format!("{explicit}:{port}")
            }
        } else if explicit.matches(':').count() > 1 {
            // A bare IPv6 literal.
            format!("[{explicit}]:{port}")
        } else if explicit
            .rsplit_once(':')
            .is_some_and(|(_, p)| p.parse::<u16>().is_ok())
        {
            explicit.to_string()
        } else {
            format!("{explicit}:{port}")
        };
        return Ok(format!("https://{with_port}"));
    }
    let ip = route_source_ip(manager_addr)?;
    Ok(match ip {
        std::net::IpAddr::V4(v4) => format!("https://{v4}:{port}"),
        std::net::IpAddr::V6(v6) => format!("https://[{v6}]:{port}"),
    })
}

/// The local address this host uses to reach `manager_addr`.
pub fn route_source_ip(manager_addr: &str) -> R<std::net::IpAddr> {
    use std::net::ToSocketAddrs;
    let target = crate::cluster_tls::host_port(manager_addr)?;
    let remote = target
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve the manager address {manager_addr}"))?
        .next()
        .with_context(|| format!("the manager address {manager_addr} resolved to nothing"))?;
    let local: std::net::SocketAddr = if remote.is_ipv4() {
        ([0, 0, 0, 0], 0).into()
    } else {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = std::net::UdpSocket::bind(local)?;
    socket
        .connect(remote)
        .with_context(|| format!("no route to the manager at {manager_addr}"))?;
    Ok(socket.local_addr()?.ip())
}

/// How long to wait between identity issuance attempts at startup while
/// the manager refuses or is unreachable.
pub const ISSUE_RETRY: Duration = Duration::from_secs(2);

/// How this process's workers verify the managers they dial, keyed by
/// manager address (worker-plane-auth plan §6.8).
///
/// Trust is cluster-scoped: every address of one cluster maps to the SAME
/// verifier, because the managers share the cluster CA. The join token's
/// manager is recorded first ([`trust_manager`]). Every other manager of
/// that cluster joins it through [`trust_cluster_managers`] as the worker
/// learns it, so moving the token's pin to the bundle
/// ([`trust_manager_bundle`]) moves the whole cluster at once. Keying by
/// address (rather than one process-wide trust) is what lets one process
/// hold several clusters: tests run workers for several side by side.
fn manager_trusts(
) -> &'static Mutex<std::collections::HashMap<String, Arc<crate::cluster_tls::PinnedManagerVerifier>>>
{
    static TRUSTS: std::sync::OnceLock<
        Mutex<std::collections::HashMap<String, Arc<crate::cluster_tls::PinnedManagerVerifier>>>,
    > = std::sync::OnceLock::new();
    TRUSTS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Records the join token's CA pin as what `manager_addr` must present, and
/// returns the verifier every connection to it shares. A later call for the
/// same address keeps the existing verifier (and whatever bundle it has
/// learned since).
pub fn trust_manager(
    manager_addr: &str,
    pin: &str,
) -> R<Arc<crate::cluster_tls::PinnedManagerVerifier>> {
    let key = crate::cluster_tls::host_port(manager_addr)?;
    let mut trusts = manager_trusts().lock().unwrap_or_else(|p| p.into_inner());
    Ok(trusts
        .entry(key)
        .or_insert_with(|| {
            crate::cluster_tls::PinnedManagerVerifier::new(crate::cluster_tls::ManagerTrust::Pin(
                pin.to_string(),
            ))
        })
        .clone())
}

/// Extends the trust recorded for `trusted_addr` (the join token's manager)
/// to every manager address in `addrs`. A worker that fails over to another
/// manager of the same cluster can then reach it from every call site that
/// dials by address: artifact credentials, node identity tokens, the JWKS.
///
/// Only call this with addresses the cluster itself vouched for: the set a
/// manager reported over a connection verified against this same trust
/// (`HeartbeatReply.manager_addresses`), or the set a previous worker process
/// persisted from those replies. Either way the address only says where to
/// dial. The manager there must still present a certificate for
/// `manager.legion` chained to the cluster CA the token pinned, so adding an
/// address never widens what is trusted.
///
/// A no-op when `trusted_addr` has no recorded trust. An address already
/// trusted (for this cluster or another one in the same process) keeps its
/// verifier: one cluster's report never re-points another's address.
pub fn trust_cluster_managers(trusted_addr: &str, addrs: &[String]) {
    let Some(verifier) = manager_verifier(trusted_addr) else {
        return;
    };
    let mut trusts = manager_trusts().lock().unwrap_or_else(|p| p.into_inner());
    for addr in addrs {
        let Ok(key) = crate::cluster_tls::host_port(addr) else {
            continue;
        };
        let existing = trusts.entry(key).or_insert_with(|| verifier.clone());
        if !Arc::ptr_eq(existing, &verifier) {
            debug!(
                manager = %addr,
                "Manager address is already trusted for another cluster; keeping that trust"
            );
        }
    }
}

/// The verifier for `manager_addr`, when this process has joined its
/// cluster: the token's manager, or one [`trust_cluster_managers`] added.
pub fn manager_verifier(
    manager_addr: &str,
) -> Option<Arc<crate::cluster_tls::PinnedManagerVerifier>> {
    let key = crate::cluster_tls::host_port(manager_addr).ok()?;
    manager_trusts()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .cloned()
}

/// A connected gRPC channel to a manager this process has joined: TLS to
/// `manager_addr`'s host and port, naming `manager.legion`, verified against
/// the join token's CA pin (or the bundle once the worker holds one). The
/// address's scheme is ignored. Any manager of a cluster this process joined
/// works, not just the token's ([`trust_cluster_managers`]). There is no
/// plaintext path: a process that has not recorded a pin for this manager's
/// cluster cannot reach it.
pub async fn manager_channel(manager_addr: &str) -> R<tonic::transport::Channel> {
    let verifier = manager_verifier(manager_addr).ok_or_else(|| {
        anyhow!(
            "no cluster CA pin for the manager at {manager_addr}; workers reach managers only \
             over TLS verified against their join token's pin"
        )
    })?;
    let config = crate::cluster_tls::client_config(verifier, None)?;
    crate::cluster_tls::tls_connect(
        manager_addr,
        config,
        crate::cluster_tls::MANAGER_SERVER_NAME,
    )
    .await
}

/// Moves `manager_addr`'s verifier from the join token's pin to the cluster
/// CA bundle this worker now holds, which follows CA rotations. Every manager
/// of the cluster shares the verifier, so they all move together.
pub fn trust_manager_bundle(manager_addr: &str, roots: Arc<SwappableRoots>) {
    if let Some(verifier) = manager_verifier(manager_addr) {
        verifier.set_trust(crate::cluster_tls::ManagerTrust::Bundle(roots));
    }
}

/// Whether a held bundle is the one the manager reports: the same anchor
/// (first certificate) and the same set of CAs.
fn bundle_matches(held_pem: &str, manager_pins: &[String]) -> bool {
    let held = crate::cluster_tls::bundle_pins(held_pem);
    if held.first() != manager_pins.first() {
        return false;
    }
    let held: std::collections::BTreeSet<&String> = held.iter().collect();
    let manager: std::collections::BTreeSet<&String> = manager_pins.iter().collect();
    held == manager
}

#[cfg(test)]
mod plane_test {
    use super::*;

    #[test]
    fn advertise_override_wins_and_takes_host_or_host_port_test() -> R<()> {
        assert_eq!(
            advertise_address("http://127.0.0.1:4747", 4848, Some("10.0.1.7"))?,
            "https://10.0.1.7:4848"
        );
        assert_eq!(
            advertise_address(
                "http://127.0.0.1:4747",
                4848,
                Some("worker-1.internal:9000")
            )?,
            "https://worker-1.internal:9000"
        );
        assert_eq!(
            advertise_address(
                "http://127.0.0.1:4747",
                4848,
                Some("https://10.0.1.7:4848/")
            )?,
            "https://10.0.1.7:4848"
        );
        assert_eq!(
            advertise_address("http://127.0.0.1:4747", 4848, Some("fd00::7"))?,
            "https://[fd00::7]:4848"
        );
        Ok(())
    }

    #[test]
    fn advertise_detection_uses_the_route_to_the_manager_test() -> R<()> {
        // A manager on loopback is reached from loopback.
        assert_eq!(
            route_source_ip("http://127.0.0.1:4747")?,
            std::net::IpAddr::from([127, 0, 0, 1])
        );
        // Any loopback manager is reached from loopback (Linux picks
        // 127.0.0.1 as the source even for 127.0.0.3, so several workers on
        // one host need --advertise-addr to be told apart).
        assert!(route_source_ip("http://127.0.0.3:4747")?.is_loopback());
        assert_eq!(
            advertise_address("http://127.0.0.1:4747", 4848, None)?,
            "https://127.0.0.1:4848"
        );
        Ok(())
    }

    // The trust registry is process-global and tests run in parallel, so
    // each trust test uses addresses no other test touches (TEST-NET ranges).

    /// After a failover the worker talks to a manager its token never named.
    /// The pin recorded for the token's manager covers it once the cluster
    /// reports it, and moving to the bundle moves every manager at once.
    #[test]
    fn a_verifier_recorded_for_one_manager_covers_the_clusters_others_test() -> R<()> {
        let (ca_pem, _) = crate::manager::mesh_ca::generate_ca()?;
        let pin = crate::cluster_tls::ca_pin_pem(&ca_pem)?;
        let seed = "http://192.0.2.11:4747";
        let other = "http://192.0.2.12:4747";
        let recorded = trust_manager(seed, &pin)?;
        assert!(manager_verifier(other).is_none());

        trust_cluster_managers(seed, &[other.to_string(), "192.0.2.13:4747".to_string()]);
        let learned = manager_verifier(other).expect("the cluster's manager is trusted");
        assert!(Arc::ptr_eq(&recorded, &learned), "one verifier per cluster");
        // The scheme is ignored: host and port are the key.
        assert!(manager_verifier("https://192.0.2.13:4747").is_some());

        // The bundle replaces the pin for every manager of the cluster, no
        // matter which address it arrived through.
        trust_manager_bundle(other, SwappableRoots::from_pem(&ca_pem)?);
        assert!(matches!(
            recorded.trust(),
            crate::cluster_tls::ManagerTrust::Bundle(_)
        ));
        Ok(())
    }

    /// A process that joined no cluster trusts no manager, and a cluster
    /// report cannot conjure trust from nothing.
    #[tokio::test]
    async fn an_unrelated_address_is_never_trusted_test() -> R<()> {
        let stranger = "http://198.51.100.21:4747";
        assert!(manager_verifier(stranger).is_none());
        let err = manager_channel(stranger)
            .await
            .expect_err("no pin, no channel");
        assert!(format!("{err:#}").contains("no cluster CA pin"), "{err:#}");

        // Extending from an address with no recorded trust is a no-op.
        trust_cluster_managers("http://198.51.100.22:4747", &[stranger.to_string()]);
        assert!(manager_verifier(stranger).is_none());
        Ok(())
    }

    /// One cluster's report never re-points an address another cluster in
    /// the same process already trusts.
    #[test]
    fn a_cluster_report_keeps_another_clusters_trust_test() -> R<()> {
        let (ca_a, _) = crate::manager::mesh_ca::generate_ca()?;
        let (ca_b, _) = crate::manager::mesh_ca::generate_ca()?;
        let a = trust_manager(
            "http://203.0.113.31:4747",
            &crate::cluster_tls::ca_pin_pem(&ca_a)?,
        )?;
        let b = trust_manager(
            "http://203.0.113.32:4747",
            &crate::cluster_tls::ca_pin_pem(&ca_b)?,
        )?;
        trust_cluster_managers(
            "http://203.0.113.31:4747",
            &["http://203.0.113.32:4747".to_string()],
        );
        let kept = manager_verifier("http://203.0.113.32:4747").expect("still trusted");
        assert!(Arc::ptr_eq(&kept, &b));
        assert!(!Arc::ptr_eq(&kept, &a));
        Ok(())
    }

    #[test]
    fn bind_defaults_to_every_interface_test() -> R<()> {
        // Not set in the test environment.
        if std::env::var(WORKER_HOST_ENV).is_err() {
            assert_eq!(bind_address(4848)?.to_string(), "0.0.0.0:4848");
        }
        Ok(())
    }

    #[test]
    fn the_node_key_persists_and_never_changes_across_loads_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let first = NodeIdentity::load(dir.path(), "node-1", None, None)?;
        let second = NodeIdentity::load(dir.path(), "node-1", None, None)?;
        assert_eq!(first.key_pem, second.key_pem);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("node.key"))?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert!(!first.has_certificate());
        assert!(first.needs_refresh(None));
        Ok(())
    }

    #[test]
    fn a_persisted_certificate_is_reloaded_and_its_bundle_checked_test() -> R<()> {
        let (ca_pem, ca_key) = crate::manager::mesh_ca::generate_ca()?;
        let dir = tempfile::tempdir()?;
        let identity = NodeIdentity::load(dir.path(), "node-1", None, None)?;
        let csr = crate::manager::mesh_ca::csr_for_key(&identity.key_pem, "node-1")?;
        let (cert_pem, expires) = crate::manager::mesh_ca::sign_csr(
            &ca_pem,
            &ca_key,
            &csr,
            &crate::manager::mesh_ca::CsrSubject::Node("node-1".into()),
        )?;
        write_private(&dir.path().join("node.pem"), &cert_pem)?;
        write_private(&dir.path().join("ca.pem"), &ca_pem)?;
        let pin = crate::cluster_tls::ca_pin_pem(&ca_pem)?;

        let reloaded = NodeIdentity::load(dir.path(), "node-1", None, Some(pin))?;
        assert!(reloaded.has_certificate());
        assert!(!reloaded.needs_refresh(None));
        assert!(reloaded.mesh_identity().is_some());
        assert!(expires > 0);

        // A different pin refuses the persisted bundle.
        let (other, _) = crate::manager::mesh_ca::generate_ca()?;
        let other_pin = crate::cluster_tls::ca_pin_pem(&other)?;
        let foreign = NodeIdentity::load(dir.path(), "node-1", None, Some(other_pin.clone()))?;
        assert!(!foreign.has_certificate());

        // A manager naming a CA the bundle lacks asks for a refresh.
        assert!(reloaded.needs_refresh(Some(std::slice::from_ref(&other_pin))));
        Ok(())
    }

    /// The heartbeat names the manager's whole trust bundle, anchor first.
    /// A worker re-issues when the anchor moved (the signing CA changed)
    /// or a CA was added or removed, and not when nothing changed.
    #[test]
    fn a_bundle_change_or_a_new_anchor_asks_for_a_refresh_test() -> R<()> {
        let (ca_a, ca_a_key) = crate::manager::mesh_ca::generate_ca()?;
        let (ca_b, _) = crate::manager::mesh_ca::generate_ca()?;
        let pin_a = crate::cluster_tls::ca_pin_pem(&ca_a)?;
        let pin_b = crate::cluster_tls::ca_pin_pem(&ca_b)?;
        let dir = tempfile::tempdir()?;
        let identity = NodeIdentity::load(dir.path(), "node-1", None, None)?;
        let csr = crate::manager::mesh_ca::csr_for_key(&identity.key_pem, "node-1")?;
        let (cert_pem, expires) = crate::manager::mesh_ca::sign_csr(
            &ca_a,
            &ca_a_key,
            &csr,
            &crate::manager::mesh_ca::CsrSubject::Node("node-1".into()),
        )?;
        identity.install(&cert_pem, &ca_a, expires)?;

        // Unchanged: no refresh.
        assert!(!identity.needs_refresh(Some(std::slice::from_ref(&pin_a))));
        // A CA added to the bundle: refresh, to learn it before it signs.
        assert!(identity.needs_refresh(Some(&[pin_a.clone(), pin_b.clone()])));
        // The anchor moved to a CA the bundle already holds: refresh, so
        // the leaf comes from the new CA before the old one is dropped.
        identity.install(&cert_pem, &format!("{ca_a}{ca_b}"), expires)?;
        assert!(!identity.needs_refresh(Some(&[pin_a.clone(), pin_b.clone()])));
        assert!(identity.needs_refresh(Some(&[pin_b.clone(), pin_a.clone()])));
        // A CA removed from the bundle: refresh, to stop trusting it.
        assert!(identity.needs_refresh(Some(std::slice::from_ref(&pin_a))));
        // No news from the manager: nothing to compare.
        assert!(!identity.needs_refresh(Some(&[])));
        assert!(!identity.needs_refresh(None));
        Ok(())
    }
}
