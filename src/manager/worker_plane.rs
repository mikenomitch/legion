//! The manager's side of the worker plane: the one factory every
//! manager-to-worker call goes through — dispatch (`PostWork`), exec and
//! actions (`ExecTask`), logs (`GetTaskLogs`), restart (`RestartWorker`),
//! and edge boot's speculative dispatch and ratification.
//!
//! [`target`] reads the node's `workerAddress`, dials it with the cluster CA
//! as the only trust root and this manager's own certificate
//! (`legion://manager/<id>`) as its client identity, and verifies that the
//! server certificate names the node it meant to reach
//! (`<node>.node.legion`). A worker that registered a stolen or stale
//! address therefore cannot receive another node's work. There is no
//! plaintext path: an `http://` address (a worker that predates protocol 3)
//! is refused with an error naming its protocol version.
//!
//! Channels are cached per (server name, address) for the life of the
//! manager's CA and certificate, so the TLS handshake amortizes to nothing
//! on the dispatch path. The manager certificate is minted on first use from
//! the replicated CA key and re-minted before it expires; a rotated CA is
//! noticed within [`CA_RECHECK`].

use crate::manager::mesh_ca;
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{worker_client::WorkerClient, Node};

use std::sync::atomic::AtomicUsize;
use std::sync::Weak;
use std::time::{Duration, Instant};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::Status;

/// How often a manager re-reads the cluster CA to notice a rotation.
pub const CA_RECHECK: Duration = Duration::from_secs(10);
/// A worker that does not complete TCP + TLS in this long is reported
/// unreachable instead of hanging the caller.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The node attribute a simulated fleet uses to name the one certificate its
/// shared listener serves for every node in the process
/// (docs/worker-simulation.md). Honored only on nodes that also report
/// `simulated: true`.
pub const PLANE_NAME_ATTR: &str = "workerPlaneName";

/// A node's advertised worker-plane address (`workerAddress`).
pub fn worker_address(node: &Node) -> Option<String> {
    node.config_attrs
        .as_object()?
        .get("workerAddress")?
        .as_str()
        .map(str::to_string)
        .filter(|address| !address.trim().is_empty())
}

/// The node whose certificate serves `node`'s worker plane: the node itself,
/// or for a simulated node, its fleet's shared identity.
pub fn plane_name(node: &Node) -> String {
    let attrs = node.config_attrs.as_object();
    let simulated = attrs
        .and_then(|attrs| attrs.get("simulated"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if simulated {
        if let Some(name) = attrs
            .and_then(|attrs| attrs.get(PLANE_NAME_ATTR))
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
        {
            return name.to_string();
        }
    }
    node.id.clone()
}

/// The worker protocol version the node registered with (0 if unknown).
pub fn node_protocol_version(node: &Node) -> u64 {
    let from = |attrs: &Value| attrs.get("protocolVersion").and_then(Value::as_u64);
    from(&node.dynamic_attrs)
        .or_else(|| from(&node.config_attrs))
        .unwrap_or(0)
}

/// This manager's id in its certificate: the raft node id, or `local` for a
/// single manager.
pub fn manager_id(store: &Store) -> String {
    store
        .raft_status()
        .map(|status| status.node_id)
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| "local".to_string())
}

/// Why a worker could not be dialed at all.
#[derive(Debug)]
pub enum TargetError {
    /// The node registered no `workerAddress`.
    NoAddress { node_id: String },
    /// The node advertises a plaintext address: it predates protocol 3.
    Plaintext {
        node_id: String,
        address: String,
        protocol: u64,
    },
    /// The address is not an `https://` URL.
    BadAddress { node_id: String, address: String },
    /// This manager could not load the CA or mint its certificate.
    Credentials(anyhow::Error),
}

impl std::fmt::Display for TargetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetError::NoAddress { node_id } => write!(
                f,
                "node {node_id} registered no worker address; restart its worker so it \
                 re-registers"
            ),
            TargetError::Plaintext {
                node_id,
                address,
                protocol,
            } => write!(
                f,
                "node {node_id} runs worker protocol {protocol} and serves a plaintext worker \
                 plane at {address}; this manager only dials the mTLS worker plane (protocol \
                 {PROTOCOL_VERSION}). Upgrade the worker (see docs/upgrades.md)"
            ),
            TargetError::BadAddress { node_id, address } => write!(
                f,
                "node {node_id} advertises an unusable worker address {address:?} (expected \
                 https://host:port)"
            ),
            TargetError::Credentials(err) => write!(
                f,
                "this manager cannot dial the worker plane: no usable cluster certificate \
                 ({err:#})"
            ),
        }
    }
}

impl std::error::Error for TargetError {}

impl TargetError {
    pub fn to_status(&self) -> Status {
        match self {
            TargetError::Credentials(_) => Status::unavailable(self.to_string()),
            _ => Status::failed_precondition(self.to_string()),
        }
    }
}

/// A dialable worker: a cached client plus what to say when it fails.
#[derive(Clone)]
pub struct WorkerTarget {
    pub client: WorkerClient<Channel>,
    pub node_id: String,
    pub address: String,
    /// `<server name>@<address>`: one channel per key.
    pub key: String,
}

impl std::fmt::Debug for WorkerTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerTarget")
            .field("node_id", &self.node_id)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl WorkerTarget {
    /// Turn a failed call into something an operator can act on. A failure
    /// to reach the worker at all (refused, timed out, failed TLS) names the
    /// node, the address, the port, and the fix; an answer the worker itself
    /// gave passes through unchanged.
    pub fn explain(&self, status: Status) -> Status {
        explain_failure(&self.node_id, &self.address, status)
    }
}

/// [`WorkerTarget::explain`] without a target.
pub fn explain_failure(node_id: &str, address: &str, status: Status) -> Status {
    use std::error::Error as _;
    let Some(source) = status.source() else {
        return status;
    };
    let mut cause = source.to_string();
    let mut tls: Option<String> = None;
    let mut next: Option<&(dyn std::error::Error + 'static)> = Some(source);
    while let Some(err) = next {
        let text = err.to_string();
        if !text.is_empty() {
            cause = text;
        }
        if let Some(rustls_error) = err.downcast_ref::<rustls::Error>() {
            tls = Some(rustls_error.to_string());
        }
        next = err.source();
    }
    let port = address
        .rsplit_once(':')
        .map(|(_, port)| port.trim_end_matches('/'))
        .unwrap_or("4848");
    let message = match tls {
        Some(tls) => format!(
            "cannot reach node {node_id} at {address}: TLS verification failed ({tls}). The \
             listener there is not node {node_id}'s worker plane, or it trusts a different \
             cluster CA (after `legion net ca rotate` a node re-issues its certificate on its \
             next heartbeat; retry in a few seconds)"
        ),
        None => format!(
            "cannot reach node {node_id} at {address} ({cause}). Managers dial workers on port \
             {port}: allow it from every manager, or set LEGION_WORKER_ADVERTISE_ADDR on the \
             worker if this address is wrong"
        ),
    };
    Status::unavailable(message)
}

// ---------------------------------------------------------------------------
// Per-store state
// ---------------------------------------------------------------------------

struct CachedCa {
    pem: String,
    fingerprint: String,
    checked: Instant,
}

struct ManagerIdentity {
    cert_pem: String,
    key_pem: String,
    expires_at: i64,
    minted: Instant,
}

impl ManagerIdentity {
    fn needs_refresh(&self) -> bool {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        self.minted.elapsed() >= mesh_ca::MANAGER_CERT_REFRESH_AFTER
            || self.expires_at - now < 60 * 60
    }
}

#[derive(Default)]
struct PlaneState {
    ca: Option<CachedCa>,
    identity: Option<ManagerIdentity>,
    /// Channels by `<server name>@<address>`. Cleared whenever the CA or the
    /// manager certificate changes, since both are baked into the channel.
    channels: HashMap<String, WorkerClient<Channel>>,
}

struct Registered {
    store: Weak<AtomicUsize>,
    state: Arc<Mutex<PlaneState>>,
}

/// One state per store family (a store and its clones share `read_index`),
/// held weakly so a dropped store's state goes with it.
static PLANES: Mutex<Vec<Registered>> = Mutex::new(Vec::new());

fn state_for(store: &Store) -> Arc<Mutex<PlaneState>> {
    let mut planes = PLANES.lock().unwrap_or_else(|p| p.into_inner());
    planes.retain(|entry| entry.store.strong_count() > 0);
    let key = Arc::as_ptr(&store.read_index);
    if let Some(entry) = planes.iter().find(|entry| entry.store.as_ptr() == key) {
        return entry.state.clone();
    }
    let state = Arc::new(Mutex::new(PlaneState::default()));
    planes.push(Registered {
        store: Arc::downgrade(&store.read_index),
        state: state.clone(),
    });
    state
}

impl PlaneState {
    /// Re-read the CA when the cached copy is stale; a changed CA drops the
    /// manager certificate and every channel built on the old one.
    fn refresh_ca(&mut self, store: &Store) -> R<()> {
        if self
            .ca
            .as_ref()
            .is_some_and(|ca| ca.checked.elapsed() < CA_RECHECK)
        {
            return Ok(());
        }
        let ca = store.ensure_mesh_ca().context("loading the cluster CA")?;
        let fingerprint = mesh_ca::ca_fingerprint(&ca.cert_pem)?;
        let changed = self
            .ca
            .as_ref()
            .is_none_or(|cached| cached.fingerprint != fingerprint);
        if changed {
            if self.ca.is_some() {
                info!(
                    %fingerprint,
                    "Cluster CA changed; re-minting this manager's worker-plane certificate"
                );
            }
            self.identity = None;
            self.channels.clear();
        }
        self.ca = Some(CachedCa {
            pem: ca.cert_pem,
            fingerprint,
            checked: Instant::now(),
        });
        // The key is only needed to sign; keep it out of the cache.
        if self.identity.is_none() {
            let id = manager_id(store);
            let ca_pem = &self.ca.as_ref().expect("just set").pem;
            let (cert_pem, key_pem, expires_at) =
                mesh_ca::issue_manager_cert(ca_pem, &ca.key_pem, &id)
                    .context("minting this manager's certificate")?;
            info!(manager_id = %id, expires_at, "Minted the manager's worker-plane certificate");
            self.identity = Some(ManagerIdentity {
                cert_pem,
                key_pem,
                expires_at,
                minted: Instant::now(),
            });
            self.channels.clear();
        }
        Ok(())
    }

    fn ensure_identity(&mut self, store: &Store) -> R<()> {
        self.refresh_ca(store)?;
        if self
            .identity
            .as_ref()
            .is_none_or(ManagerIdentity::needs_refresh)
        {
            // Force a CA re-read, which re-mints from the current CA.
            self.identity = None;
            if let Some(ca) = self.ca.as_mut() {
                ca.checked = Instant::now() - CA_RECHECK;
            }
            self.refresh_ca(store)?;
        }
        Ok(())
    }

    fn channel(&mut self, server_name: &str, address: &str) -> R<WorkerClient<Channel>> {
        let key = format!("{server_name}@{address}");
        if let Some(client) = self.channels.get(&key) {
            return Ok(client.clone());
        }
        let ca = self.ca.as_ref().context("cluster CA not loaded")?;
        let identity = self
            .identity
            .as_ref()
            .context("manager certificate not minted")?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&ca.pem))
            .identity(Identity::from_pem(&identity.cert_pem, &identity.key_pem))
            .domain_name(server_name.to_string());
        let endpoint = Endpoint::from_shared(address.to_string())
            .with_context(|| format!("invalid worker address {address}"))?
            .tls_config(tls)
            .context("worker-plane TLS config")?
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_nodelay(true);
        let client = WorkerClient::new(endpoint.connect_lazy());
        self.channels.insert(key, client.clone());
        Ok(client)
    }
}

/// The client for `node`'s worker plane. Synchronous and cheap on the hot
/// path: a cached channel is a map lookup.
pub fn target(store: &Store, node: &Node) -> Result<WorkerTarget, TargetError> {
    let address = worker_address(node).ok_or_else(|| TargetError::NoAddress {
        node_id: node.id.clone(),
    })?;
    if address.starts_with("http://") {
        return Err(TargetError::Plaintext {
            node_id: node.id.clone(),
            address,
            protocol: node_protocol_version(node),
        });
    }
    if !address.starts_with("https://") {
        return Err(TargetError::BadAddress {
            node_id: node.id.clone(),
            address,
        });
    }
    let server_name = mesh_ca::node_dns_name(&plane_name(node));
    let state = state_for(store);
    let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
    state
        .ensure_identity(store)
        .map_err(TargetError::Credentials)?;
    let client = state
        .channel(&server_name, &address)
        .map_err(|_| TargetError::BadAddress {
            node_id: node.id.clone(),
            address: address.clone(),
        })?;
    Ok(WorkerTarget {
        client,
        node_id: node.id.clone(),
        key: format!("{server_name}@{address}"),
        address,
    })
}

/// [`target`] for a node id, loading the node record.
#[allow(clippy::result_large_err)]
pub fn target_for_node(store: &Store, node_id: &str) -> Result<WorkerTarget, Status> {
    let node = store
        .get_node(node_id)
        .map_err(|_| Status::not_found(format!("node {node_id} not found")))?;
    target(store, &node).map_err(|err| err.to_status())
}

// ---------------------------------------------------------------------------
// The CLI's edge-boot dial
// ---------------------------------------------------------------------------

/// A verifier that accepts any *node* certificate from the cluster CA and
/// remembers which node it was. Used once, to learn the name an edge address
/// serves before the real (name-verified) connection.
#[derive(Debug)]
struct LearnNodeName {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    learned: Mutex<Option<String>>,
}

impl rustls::client::danger::ServerCertVerifier for LearnNodeName {
    fn verify_server_cert(
        &self,
        end_entity: &rustls_pki_types::CertificateDer<'_>,
        intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let node = match crate::worker::plane::peer_class(Some(std::slice::from_ref(end_entity))) {
            crate::worker::plane::PeerClass::Node(id) => id,
            _ => {
                return Err(rustls::Error::General(
                    "the edge address did not present a Legion node certificate".into(),
                ))
            }
        };
        let name = rustls_pki_types::ServerName::try_from(mesh_ca::node_dns_name(&node))
            .map_err(|err| rustls::Error::General(err.to_string()))?;
        let verified =
            self.inner
                .verify_server_cert(end_entity, intermediates, &name, ocsp_response, now)?;
        *self.learned.lock().unwrap_or_else(|p| p.into_inner()) = Some(node);
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Dial an edge node's worker plane from outside the cluster (`legion
/// sandbox start --node https://...`). No client certificate: `StartClaim`
/// is authorized by its boot ticket. The server must present a node
/// certificate from `ca_pem`; a first handshake learns which node, and the
/// returned channel verifies that node's name like a manager does.
/// Returns the client and the node id it reached.
pub async fn edge_client(address: &str, ca_pem: &str) -> R<(WorkerClient<Channel>, String)> {
    if address.starts_with("http://") {
        bail!(
            "{address} is plaintext; worker planes serve mTLS since protocol \
             {PROTOCOL_VERSION} — use https://"
        );
    }
    if !address.starts_with("https://") {
        bail!("{address} is not an https:// worker address (e.g. https://10.0.1.7:4848)");
    }
    let uri: http::Uri = address
        .parse()
        .with_context(|| format!("invalid worker address {address}"))?;
    let host = uri
        .host()
        .context("worker address has no host")?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let port = uri.port_u16().unwrap_or(DEF_WORKER_SERVER_PORT as u16);

    let mut roots = rustls::RootCertStore::empty();
    for cert in crate::worker::netd::tls::certs_from_pem(ca_pem.as_bytes())? {
        roots.add(cert)?;
    }
    if roots.is_empty() {
        bail!("the cluster CA PEM holds no certificate");
    }
    let roots = Arc::new(roots);
    let verifier = Arc::new(LearnNodeName {
        inner: rustls::client::WebPkiServerVerifier::builder(roots)
            .build()
            .map_err(|err| anyhow!("cluster CA verifier: {err}"))?,
        learned: Mutex::new(None),
    });
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let tcp = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio::net::TcpStream::connect((host.as_str(), port)),
    )
    .await
    .with_context(|| format!("timed out connecting to {address}"))?
    .with_context(|| format!("cannot connect to {address}"))?;
    let probe = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(
            rustls_pki_types::ServerName::try_from("worker.legion")
                .map_err(|err| anyhow!("{err}"))?,
            tcp,
        )
        .await
        .with_context(|| format!("TLS with the worker plane at {address} failed"))?;
    drop(probe);
    let node = verifier
        .learned
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .context("the worker plane presented no node certificate")?;

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .domain_name(mesh_ca::node_dns_name(&node));
    let channel = Endpoint::from_shared(address.to_string())?
        .tls_config(tls)?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await
        .with_context(|| format!("cannot reach the worker plane of node {node} at {address}"))?;
    Ok((WorkerClient::new(channel), node))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, attrs: Value) -> Node {
        Node {
            id: id.into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: attrs,
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({ "protocolVersion": 2 }),
            health_checks: json!({}),
        }
    }

    #[test]
    fn plane_name_is_the_node_unless_simulated() {
        assert_eq!(plane_name(&node("wkr-1", json!({}))), "wkr-1");
        // A real worker cannot borrow another node's identity.
        assert_eq!(
            plane_name(&node("wkr-1", json!({ PLANE_NAME_ATTR: "wkr-2" }))),
            "wkr-1"
        );
        assert_eq!(
            plane_name(&node(
                "sim-web-3",
                json!({ "simulated": true, PLANE_NAME_ATTR: "sim-web-0" })
            )),
            "sim-web-0"
        );
    }

    #[test]
    fn plaintext_and_missing_addresses_are_refused_with_the_reason() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::start_storage(&crate::structs::ManagerConfigFull {
            store_path: dir.path().join("store").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let old = node(
            "wkr-old",
            json!({ "workerAddress": "http://10.0.0.5:4848" }),
        );
        let err = target(&store, &old).unwrap_err();
        let message = err.to_string();
        assert!(matches!(err, TargetError::Plaintext { protocol: 2, .. }));
        assert!(message.contains("protocol 2"), "{message}");
        assert!(message.contains("http://10.0.0.5:4848"), "{message}");
        assert_eq!(err.to_status().code(), tonic::Code::FailedPrecondition);

        let missing = node("wkr-x", json!({}));
        assert!(matches!(
            target(&store, &missing).unwrap_err(),
            TargetError::NoAddress { .. }
        ));
        let odd = node("wkr-y", json!({ "workerAddress": "10.0.0.5:4848" }));
        assert!(matches!(
            target(&store, &odd).unwrap_err(),
            TargetError::BadAddress { .. }
        ));
    }

    #[test]
    fn https_targets_share_channels_per_name_and_address() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::start_storage(&crate::structs::ManagerConfigFull {
            store_path: dir.path().join("store").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .unwrap();
        let a = node("wkr-a", json!({ "workerAddress": "https://10.0.0.5:4848" }));
        let b = node("wkr-b", json!({ "workerAddress": "https://10.0.0.5:4848" }));
        let first = target(&store, &a).unwrap();
        let again = target(&store, &a).unwrap();
        let other = target(&store, &b).unwrap();
        assert_eq!(first.key, again.key);
        assert_ne!(
            first.key, other.key,
            "two nodes behind one address never share a channel: each verifies its own name"
        );
        assert_eq!(first.key, "wkr-a.node.legion@https://10.0.0.5:4848");
    }

    #[test]
    fn worker_answers_pass_through_and_transport_failures_get_a_hint() {
        let answer = explain_failure(
            "wkr-1",
            "https://10.0.1.7:4848",
            Status::not_found("allocation 9 not on this node"),
        );
        assert_eq!(answer.code(), tonic::Code::NotFound);
        assert_eq!(answer.message(), "allocation 9 not on this node");
    }
}
