//! TLS for Legion's own planes, all anchored in the cluster CA.
//!
//! One CA per cluster (`net_mesh_ca`, see `crate::manager::mesh_ca`) signs
//! every certificate Legion's processes present to each other. A
//! certificate's *role* is carried in its subject alternative names and is
//! checked on every connection, so "signed by our CA" is never enough on its
//! own (worker-plane-auth plan §6.1, §6.2):
//!
//! | Role | DNS SAN | URI SAN | Used for |
//! | --- | --- | --- | --- |
//! | node | `<node>.node.legion` | `legion://node/<id>` | the worker plane listener, the mesh |
//! | fleet | `<fleet>.fleet.legion` | `legion://fleet/<name>` | one simulator process serving many nodes |
//! | manager (client) | | `legion://manager/<id>` | managers dialing the worker plane |
//! | manager (server) | `manager.legion` | `legion://manager/<id>` | workers dialing a manager (port 4747) |
//!
//! This module holds the pieces every side shares: CA pins, PEM parsing,
//! hot-swappable identities and trust roots (certificates rotate every few
//! hours while connections and listeners stay up), the verifiers, and the
//! TLS connector and acceptor that plug rustls into tonic.

use crate::prelude::*;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use std::sync::RwLock;

/// The DNS name every manager's cluster server certificate carries, and the
/// name a worker verifies when it dials a manager. A node certificate never
/// carries it, so a node can't pose as a manager.
pub const MANAGER_SERVER_NAME: &str = "manager.legion";
/// URI SAN prefix naming a manager (`legion://manager/<id>`).
pub const MANAGER_URI_PREFIX: &str = "legion://manager/";
/// URI SAN prefix naming a node (`legion://node/<id>`).
pub const NODE_URI_PREFIX: &str = "legion://node/";
/// URI SAN prefix naming a simulated fleet (`legion://fleet/<name>`).
pub const FLEET_URI_PREFIX: &str = "legion://fleet/";
/// Prefix of a CA pin (`sha256:<hex of the CA certificate's DER>`).
pub const CA_PIN_PREFIX: &str = "sha256:";

/// The pin of a CA certificate: `sha256:` plus the hex SHA-256 of its DER.
/// This is what a join token carries (`caHash`), the same shape kubeadm's
/// `--discovery-token-ca-cert-hash` uses.
pub fn ca_pin(der: &[u8]) -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(der);
    let mut pin = String::with_capacity(CA_PIN_PREFIX.len() + digest.len() * 2);
    pin.push_str(CA_PIN_PREFIX);
    for byte in digest {
        let _ = write!(pin, "{byte:02x}");
    }
    pin
}

/// The pin of the first certificate in a PEM bundle.
pub fn ca_pin_pem(pem: &str) -> R<String> {
    let certs = certs_from_pem(pem)?;
    let first = certs
        .first()
        .ok_or_else(|| anyhow!("CA PEM holds no certificate"))?;
    Ok(ca_pin(first.as_ref()))
}

/// Every certificate in a PEM bundle, in order.
pub fn certs_from_pem(pem: &str) -> R<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("invalid certificate PEM")
}

/// The first private key in a PEM document.
pub fn key_from_pem(pem: &str) -> R<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut pem.as_bytes())
        .context("invalid private key PEM")?
        .ok_or_else(|| anyhow!("PEM holds no private key"))
}

/// True when any certificate in `bundle_pem` has the pin `pin`.
pub fn bundle_contains_pin(bundle_pem: &str, pin: &str) -> R<bool> {
    Ok(certs_from_pem(bundle_pem)?
        .iter()
        .any(|cert| ca_pin(cert.as_ref()) == pin))
}

/// The pins of every certificate in a bundle, for messages.
pub fn bundle_pins(bundle_pem: &str) -> Vec<String> {
    certs_from_pem(bundle_pem)
        .map(|certs| certs.iter().map(|cert| ca_pin(cert.as_ref())).collect())
        .unwrap_or_default()
}

/// A short form of a pin for messages (`sha256:9c1e4f...`).
pub fn short_pin(pin: &str) -> String {
    match pin.strip_prefix(CA_PIN_PREFIX) {
        Some(hex) if hex.len() > 12 => format!("{CA_PIN_PREFIX}{}...", &hex[..12]),
        _ => pin.to_string(),
    }
}

/// A root store holding every certificate in a PEM bundle.
pub fn roots_from_pem(bundle_pem: &str) -> R<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in certs_from_pem(bundle_pem)? {
        roots
            .add(cert)
            .map_err(|err| anyhow!("invalid CA certificate in bundle: {err}"))?;
    }
    if roots.is_empty() {
        bail!("CA bundle holds no certificate");
    }
    Ok(roots)
}

/// Builds a signing identity from a certificate chain PEM (leaf first) and a
/// private key PEM.
pub fn certified_key(chain_pem: &str, key_pem: &str) -> R<Arc<CertifiedKey>> {
    let chain = certs_from_pem(chain_pem)?;
    if chain.is_empty() {
        bail!("certificate PEM holds no certificate");
    }
    let key = key_from_pem(key_pem)?;
    let signing_key = rustls::crypto::ring::sign::any_supported_type(&key)
        .map_err(|err| anyhow!("unsupported private key: {err}"))?;
    Ok(Arc::new(CertifiedKey::new(chain, signing_key)))
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The role a peer certificate's URI SANs give it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRole {
    /// `legion://manager/<id>`.
    Manager(String),
    /// `legion://node/<id>`.
    Node(String),
    /// `legion://fleet/<name>`.
    Fleet(String),
    /// A certificate that names no Legion role.
    Unknown,
}

/// The URI SANs of a DER certificate.
pub fn uri_sans(der: &[u8]) -> R<Vec<String>> {
    use x509_parser::extensions::GeneralName;
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|err| anyhow!("invalid peer certificate: {err}"))?;
    let mut uris = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::URI(uri) = name {
                uris.push(uri.to_string());
            }
        }
    }
    Ok(uris)
}

/// The role of a peer certificate, read from its URI SANs. The chain has
/// already been verified against the cluster CA by the TLS handshake; this
/// only reads which role the CA gave it.
pub fn peer_role(der: &[u8]) -> PeerRole {
    let Ok(uris) = uri_sans(der) else {
        return PeerRole::Unknown;
    };
    for uri in uris {
        if let Some(id) = uri.strip_prefix(MANAGER_URI_PREFIX) {
            return PeerRole::Manager(id.to_string());
        }
        if let Some(id) = uri.strip_prefix(NODE_URI_PREFIX) {
            return PeerRole::Node(id.to_string());
        }
        if let Some(name) = uri.strip_prefix(FLEET_URI_PREFIX) {
            return PeerRole::Fleet(name.to_string());
        }
    }
    PeerRole::Unknown
}

/// A certificate and key that can be replaced while every listener and
/// connector built on it keeps running. Serves as both the server and the
/// client identity resolver.
#[derive(Default)]
pub struct SwappableIdentity {
    current: RwLock<Option<Arc<CertifiedKey>>>,
}

impl std::fmt::Debug for SwappableIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwappableIdentity")
            .field("loaded", &self.current().is_some())
            .finish()
    }
}

impl SwappableIdentity {
    pub fn new() -> Arc<Self> {
        Arc::new(SwappableIdentity::default())
    }

    pub fn set(&self, key: Arc<CertifiedKey>) {
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = Some(key);
    }

    pub fn current(&self) -> Option<Arc<CertifiedKey>> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

impl ResolvesServerCert for SwappableIdentity {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.current()
    }
}

impl rustls::client::ResolvesClientCert for SwappableIdentity {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        self.current()
    }

    fn has_certs(&self) -> bool {
        self.current().is_some()
    }
}

/// A set of trust roots (the cluster CA bundle) that can be replaced while
/// connections built on it stay up. During a CA rotation the bundle holds
/// both CAs.
pub struct SwappableRoots {
    current: RwLock<(Arc<RootCertStore>, String)>,
}

impl std::fmt::Debug for SwappableRoots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwappableRoots")
            .field("pins", &bundle_pins(&self.pem()))
            .finish()
    }
}

impl SwappableRoots {
    pub fn from_pem(bundle_pem: &str) -> R<Arc<Self>> {
        Ok(Arc::new(SwappableRoots {
            current: RwLock::new((
                Arc::new(roots_from_pem(bundle_pem)?),
                bundle_pem.to_string(),
            )),
        }))
    }

    /// Replaces the bundle. Returns false (and keeps the current one) when
    /// the PEM holds no usable certificate.
    pub fn set_pem(&self, bundle_pem: &str) -> R<()> {
        let roots = Arc::new(roots_from_pem(bundle_pem)?);
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = (roots, bundle_pem.to_string());
        Ok(())
    }

    pub fn roots(&self) -> Arc<RootCertStore> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .clone()
    }

    pub fn pem(&self) -> String {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .1
            .clone()
    }
}

/// Verifies a server certificate against the current cluster CA bundle and
/// a fixed DNS name, whatever name the connector asked for. Dialing
/// `https://10.0.1.7:4848` with this verifier and the name
/// `wkr-1.node.legion` accepts only `wkr-1`'s certificate, which is how a
/// manager knows a stolen or stale address can't receive another node's
/// work (plan §6.3).
#[derive(Debug)]
pub struct NamedServerVerifier {
    roots: Arc<SwappableRoots>,
    name: ServerName<'static>,
}

impl NamedServerVerifier {
    pub fn new(roots: Arc<SwappableRoots>, name: &str) -> R<Arc<Self>> {
        Ok(Arc::new(NamedServerVerifier {
            roots,
            name: ServerName::try_from(name.to_string())
                .map_err(|err| anyhow!("invalid server name {name:?}: {err}"))?,
        }))
    }

    fn inner(&self) -> Result<Arc<rustls::client::WebPkiServerVerifier>, rustls::Error> {
        rustls::client::WebPkiServerVerifier::builder_with_provider(self.roots.roots(), provider())
            .build()
            .map_err(|err| rustls::Error::General(format!("cluster CA verifier: {err}")))
    }
}

impl ServerCertVerifier for NamedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner()?
            .verify_server_cert(end_entity, intermediates, &self.name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verifies a server certificate as *some* node (or simulator fleet) of the
/// cluster: it must chain to the cluster CA bundle, and the name it is
/// checked against is its own `*.node.legion` or `*.fleet.legion` DNS SAN.
/// For clients that know a worker's address but not its node name
/// (`legion sandbox start --node <addr>` without `--node-name`): a manager
/// certificate, or anything signed elsewhere, still fails.
#[derive(Debug)]
pub struct AnyNodeVerifier {
    roots: Arc<SwappableRoots>,
}

impl AnyNodeVerifier {
    pub fn new(roots: Arc<SwappableRoots>) -> Arc<Self> {
        Arc::new(AnyNodeVerifier { roots })
    }
}

/// The DNS SANs of a DER certificate.
pub fn dns_sans(der: &[u8]) -> R<Vec<String>> {
    use x509_parser::extensions::GeneralName;
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|err| anyhow!("invalid certificate: {err}"))?;
    let mut names = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::DNSName(dns) = name {
                names.push(dns.to_string());
            }
        }
    }
    Ok(names)
}

impl ServerCertVerifier for AnyNodeVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let names = dns_sans(end_entity.as_ref())
            .map_err(|err| rustls::Error::General(format!("{err:#}")))?;
        let name = names
            .into_iter()
            .find(|name| name.ends_with(".node.legion") || name.ends_with(".fleet.legion"))
            .ok_or_else(|| {
                rustls::Error::General(
                    "the peer's certificate is not a Legion node certificate".into(),
                )
            })?;
        let name =
            ServerName::try_from(name).map_err(|err| rustls::Error::General(err.to_string()))?;
        rustls::client::WebPkiServerVerifier::builder_with_provider(self.roots.roots(), provider())
            .build()
            .map_err(|err| rustls::Error::General(format!("cluster CA verifier: {err}")))?
            .verify_server_cert(end_entity, intermediates, &name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verifies a manager's server certificate on a worker's first contact,
/// when all the worker holds is the CA pin from its join token.
///
/// The manager sends the CA certificates with its chain. The verifier takes
/// the one whose SHA-256 matches the pin as the only trust anchor, and checks
/// the leaf against it for the name `manager.legion`. A certificate that
/// merely *contains* the real CA (which is public) but was signed by another
/// key fails, because the anchor is the pinned key, not the bundle. Once the
/// worker holds the cluster's CA bundle (from its node certificate reply),
/// [`ManagerTrust::Bundle`] replaces the pin.
#[derive(Debug)]
pub struct PinnedManagerVerifier {
    trust: RwLock<ManagerTrust>,
}

/// What a worker trusts when it dials a manager.
#[derive(Debug, Clone)]
pub enum ManagerTrust {
    /// The join token's CA pin, before the worker has fetched a bundle.
    Pin(String),
    /// The cluster CA bundle the worker fetched over a verified channel.
    Bundle(Arc<SwappableRoots>),
}

/// The error a worker prints when the manager's CA does not match its pin.
pub fn pin_mismatch_message(addr: &str, presented: &[String], pin: &str) -> String {
    let presented = if presented.is_empty() {
        "no CA certificate".to_string()
    } else {
        format!(
            "cluster CA {}",
            presented
                .iter()
                .map(|pin| short_pin(pin))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "the manager at {addr} presented {presented}, but this join token pins {}\n\
         Hint: the address may not be your manager, or the CA was rotated after this token \
         was minted. Mint a new token with `legion join-token create`.",
        short_pin(pin)
    )
}

impl PinnedManagerVerifier {
    pub fn new(trust: ManagerTrust) -> Arc<Self> {
        Arc::new(PinnedManagerVerifier {
            trust: RwLock::new(trust),
        })
    }

    pub fn set_trust(&self, trust: ManagerTrust) {
        *self.trust.write().unwrap_or_else(|p| p.into_inner()) = trust;
    }

    pub fn trust(&self) -> ManagerTrust {
        self.trust.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn roots_for_chain(
        &self,
        intermediates: &[CertificateDer<'_>],
    ) -> Result<Arc<RootCertStore>, rustls::Error> {
        match self.trust() {
            ManagerTrust::Bundle(roots) => Ok(roots.roots()),
            ManagerTrust::Pin(pin) => {
                let anchor = intermediates
                    .iter()
                    .find(|cert| ca_pin(cert.as_ref()) == pin)
                    .ok_or_else(|| {
                        let presented: Vec<String> = intermediates
                            .iter()
                            .map(|cert| ca_pin(cert.as_ref()))
                            .collect();
                        rustls::Error::General(format!(
                            "CA pin mismatch: presented {presented:?}, pinned {pin}"
                        ))
                    })?;
                let mut roots = RootCertStore::empty();
                roots
                    .add(anchor.clone().into_owned())
                    .map_err(|err| rustls::Error::General(format!("pinned CA: {err}")))?;
                Ok(Arc::new(roots))
            }
        }
    }
}

impl ServerCertVerifier for PinnedManagerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let roots = self.roots_for_chain(intermediates)?;
        let name = ServerName::try_from(MANAGER_SERVER_NAME)
            .map_err(|err| rustls::Error::General(err.to_string()))?;
        rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider())
            .build()
            .map_err(|err| rustls::Error::General(format!("pinned CA verifier: {err}")))?
            .verify_server_cert(end_entity, intermediates, &name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Server-side client certificate verification against the current cluster
/// CA bundle. Client certificates are requested but optional: `StartClaim`
/// callers and direct-exec clients authenticate with tickets, not
/// certificates (plan §6.1). A certificate that *is* presented must chain to
/// the cluster CA, or the handshake fails.
#[derive(Debug)]
pub struct ClusterClientVerifier {
    roots: Arc<SwappableRoots>,
}

impl ClusterClientVerifier {
    pub fn new(roots: Arc<SwappableRoots>) -> Arc<Self> {
        Arc::new(ClusterClientVerifier { roots })
    }

    fn inner(&self) -> Result<Arc<dyn ClientCertVerifier>, rustls::Error> {
        WebPkiClientVerifier::builder_with_provider(self.roots.roots(), provider())
            .allow_unauthenticated()
            .build()
            .map_err(|err| rustls::Error::General(format!("cluster CA client verifier: {err}")))
    }
}

impl ClientCertVerifier for ClusterClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // An empty hint list tells a client to send any certificate it has.
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.inner()?
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A client config that verifies the server with `verifier` and, when
/// `identity` is set, presents it as a client certificate. Speaks HTTP/2
/// only (gRPC).
pub fn client_config(
    verifier: Arc<dyn ServerCertVerifier>,
    identity: Option<Arc<SwappableIdentity>>,
) -> R<Arc<ClientConfig>> {
    let builder = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let mut config = match identity {
        Some(identity) => builder.with_client_cert_resolver(identity),
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// A server config presenting `identity`, verifying optional client
/// certificates with `client_verifier` (or none), and speaking HTTP/2 (and,
/// when `http1` is set, HTTP/1.1 for grpc-web and the OIDC endpoints).
pub fn server_config(
    identity: Arc<dyn ResolvesServerCert>,
    client_verifier: Option<Arc<dyn ClientCertVerifier>>,
    http1: bool,
) -> R<Arc<ServerConfig>> {
    let builder =
        ServerConfig::builder_with_provider(provider()).with_safe_default_protocol_versions()?;
    let mut config = match client_verifier {
        Some(verifier) => builder.with_client_cert_verifier(verifier),
        None => builder.with_no_client_auth(),
    }
    .with_cert_resolver(identity);
    config.alpn_protocols = if http1 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"h2".to_vec()]
    };
    Ok(Arc::new(config))
}

/// The `host:port` an address names, with any scheme and path dropped
/// (`https://10.0.1.7:4848` -> `10.0.1.7:4848`).
pub fn host_port(address: &str) -> R<String> {
    let uri: http::Uri = address
        .parse()
        .with_context(|| format!("invalid address {address:?}"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow!("address {address:?} has no host"))?;
    let port = authority
        .port_u16()
        .ok_or_else(|| anyhow!("address {address:?} has no port"))?;
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    Ok(if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

/// A lazily connecting gRPC channel that dials `address` over TLS with
/// `config`, sending `sni` as the server name. The scheme of `address` is
/// ignored: the channel always speaks TLS.
///
/// Connect timeout is short (5s) so an unreachable worker fails a call
/// quickly instead of hanging it; the caller turns that into a
/// `WORKER_UNREACHABLE` error with a hint.
pub fn tls_channel(
    address: &str,
    config: Arc<ClientConfig>,
    sni: &str,
) -> R<tonic::transport::Channel> {
    let target = host_port(address)?;
    let sni = ServerName::try_from(sni.to_string())
        .map_err(|err| anyhow!("invalid TLS server name {sni:?}: {err}"))?;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{target}"))?
        .connect_timeout(std::time::Duration::from_secs(5))
        .tcp_nodelay(true);
    Ok(endpoint.connect_with_connector_lazy(tls_connector(target, sni, config)))
}

/// Accepts TCP connections and completes TLS handshakes off the accept loop,
/// yielding finished TLS streams for `tonic::transport::Server::
/// serve_with_incoming`. A client that never finishes its handshake costs a
/// task, not the listener; failed handshakes are logged at debug and
/// dropped.
pub fn tls_incoming(
    listener: tokio::net::TcpListener,
    config: Arc<ServerConfig>,
) -> tokio_stream::wrappers::ReceiverStream<
    std::io::Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>,
> {
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    tokio::spawn(async move {
        loop {
            let (tcp, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    warn!(error = ?err, "TLS listener accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };
            let _ = tcp.set_nodelay(true);
            if tx.is_closed() {
                break;
            }
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let handshake =
                    tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept(tcp))
                        .await;
                match handshake {
                    Ok(Ok(stream)) => {
                        let _ = tx.send(Ok(stream)).await;
                    }
                    Ok(Err(err)) => {
                        debug!(%peer, error = %err, "TLS handshake failed");
                    }
                    Err(_) => debug!(%peer, "TLS handshake timed out"),
                }
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// What a manager presents on its API port (worker-plane-auth plan §6.8):
/// the cluster server certificate (`manager.legion`) to a client that asks
/// for that name, and the operator's `server.tls` certificate, when one is
/// configured, to everyone else. Without an operator certificate every TLS
/// client gets the cluster certificate. Workers always send
/// `manager.legion`, so they never need the operator's CA, and the CLI and
/// browsers never see a certificate they cannot verify.
#[derive(Debug)]
pub struct ManagerServerCerts {
    pub cluster: Arc<SwappableIdentity>,
    pub operator: Option<Arc<CertifiedKey>>,
}

impl ResolvesServerCert for ManagerServerCerts {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let wants_cluster = client_hello
            .server_name()
            .is_some_and(|name| name.eq_ignore_ascii_case(MANAGER_SERVER_NAME));
        match (&self.operator, wants_cluster) {
            (Some(operator), false) => Some(operator.clone()),
            _ => self.cluster.current(),
        }
    }
}

/// A connection on the manager's API port: TLS, or plaintext for the CLI and
/// UI when the operator has not turned `server.tls` on.
pub enum MaybeTlsStream {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
}

impl tonic::transport::server::Connected for MaybeTlsStream {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            MaybeTlsStream::Plain(tcp) => tcp.connect_info(),
            MaybeTlsStream::Tls(tls) => tls.get_ref().0.connect_info(),
        }
    }
}

impl tokio::io::AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(tcp) => std::pin::Pin::new(tcp).poll_read(cx, buf),
            MaybeTlsStream::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(tcp) => std::pin::Pin::new(tcp).poll_write(cx, buf),
            MaybeTlsStream::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(tcp) => std::pin::Pin::new(tcp).poll_flush(cx),
            MaybeTlsStream::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(tcp) => std::pin::Pin::new(tcp).poll_shutdown(cx),
            MaybeTlsStream::Tls(tls) => std::pin::Pin::new(tls.as_mut()).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(tcp) => std::pin::Pin::new(tcp).poll_write_vectored(cx, bufs),
            MaybeTlsStream::Tls(tls) => {
                std::pin::Pin::new(tls.as_mut()).poll_write_vectored(cx, bufs)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            MaybeTlsStream::Plain(tcp) => tcp.is_write_vectored(),
            MaybeTlsStream::Tls(tls) => tls.is_write_vectored(),
        }
    }
}

/// The first byte of every TLS connection (a handshake record). HTTP/2's
/// preface starts with `P` and an HTTP/1 request with a method name, so one
/// peeked byte tells the two apart.
pub const TLS_HANDSHAKE_BYTE: u8 = 0x16;

/// Accepts connections on the manager's API port and sorts them by their
/// first byte: a TLS handshake is completed with `config`, anything else is
/// passed through as plaintext, unless `tls_only` (the operator turned
/// `server.tls` on), in which case it is dropped. Handshakes run off the
/// accept loop, as in [`tls_incoming`].
pub fn sniffing_incoming(
    listener: tokio::net::TcpListener,
    config: Arc<ServerConfig>,
    tls_only: bool,
) -> tokio_stream::wrappers::ReceiverStream<std::io::Result<MaybeTlsStream>> {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    tokio::spawn(async move {
        loop {
            let (tcp, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    warn!(error = ?err, "Manager listener accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };
            let _ = tcp.set_nodelay(true);
            if tx.is_closed() {
                break;
            }
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let classified = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let mut first = [0u8; 1];
                    let peeked = tcp.peek(&mut first).await?;
                    if peeked == 1 && first[0] == TLS_HANDSHAKE_BYTE {
                        let tls = acceptor.accept(tcp).await?;
                        Ok::<_, std::io::Error>(Some(MaybeTlsStream::Tls(Box::new(tls))))
                    } else if tls_only || peeked == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(MaybeTlsStream::Plain(tcp)))
                    }
                })
                .await;
                match classified {
                    Ok(Ok(Some(stream))) => {
                        let _ = tx.send(Ok(stream)).await;
                    }
                    Ok(Ok(None)) => {
                        debug!(%peer, "Dropped a plaintext connection on a TLS-only listener")
                    }
                    Ok(Err(err)) => debug!(%peer, error = %err, "Manager TLS handshake failed"),
                    Err(_) => debug!(%peer, "Manager connection sent nothing in time"),
                }
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// The connector behind [`tls_channel`] and [`tls_connect`]: TCP to
/// `target`, then TLS naming `sni`.
fn tls_connector(
    target: String,
    sni: ServerName<'static>,
    config: Arc<ClientConfig>,
) -> impl tower::Service<
    http::Uri,
    Response = hyper_util::rt::TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>,
    Error = std::io::Error,
    Future = impl std::future::Future<
        Output = std::io::Result<
            hyper_util::rt::TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>,
        >,
    > + Send
                 + 'static,
> + Clone
       + Send
       + 'static {
    let connector = tokio_rustls::TlsConnector::from(config);
    tower::service_fn(move |_uri: http::Uri| {
        let target = target.clone();
        let sni = sni.clone();
        let connector = connector.clone();
        async move {
            let tcp = tokio::net::TcpStream::connect(&target).await?;
            let _ = tcp.set_nodelay(true);
            let tls = connector.connect(sni, tcp).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tls))
        }
    })
}

/// [`tls_channel`], connected before it returns: the first TCP connection
/// and TLS handshake (including the server certificate check) happen here,
/// so a caller that retries a dead or impostor manager sees the failure.
pub async fn tls_connect(
    address: &str,
    config: Arc<ClientConfig>,
    sni: &str,
) -> R<tonic::transport::Channel> {
    let target = host_port(address)?;
    let sni = ServerName::try_from(sni.to_string())
        .map_err(|err| anyhow!("invalid TLS server name {sni:?}: {err}"))?;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{target}"))?
        .connect_timeout(std::time::Duration::from_secs(5))
        .tcp_nodelay(true);
    endpoint
        .connect_with_connector(tls_connector(target.clone(), sni, config))
        .await
        .with_context(|| format!("TLS connection to {address}"))
}

#[cfg(test)]
mod cluster_tls_test {
    use super::*;

    #[test]
    fn pins_are_sha256_of_the_der_test() -> R<()> {
        let (ca_pem, _) = crate::manager::mesh_ca::generate_ca()?;
        let pin = ca_pin_pem(&ca_pem)?;
        assert!(pin.starts_with("sha256:"));
        assert_eq!(pin.len(), "sha256:".len() + 64);
        assert!(bundle_contains_pin(&ca_pem, &pin)?);
        let (other, _) = crate::manager::mesh_ca::generate_ca()?;
        assert!(!bundle_contains_pin(&other, &pin)?);
        assert!(bundle_contains_pin(&format!("{other}{ca_pem}"), &pin)?);
        assert!(short_pin(&pin).ends_with("..."));
        Ok(())
    }

    #[test]
    fn host_port_drops_scheme_and_path_test() -> R<()> {
        assert_eq!(host_port("https://10.0.1.7:4848")?, "10.0.1.7:4848");
        assert_eq!(host_port("http://127.0.0.1:4848/")?, "127.0.0.1:4848");
        assert_eq!(host_port("https://[::1]:4848")?, "[::1]:4848");
        assert!(host_port("https://10.0.1.7").is_err());
        Ok(())
    }

    #[test]
    fn pin_mismatch_names_both_pins_and_the_fix_test() {
        let message = pin_mismatch_message(
            "https://10.0.0.5:4747",
            &["sha256:51aa07aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()],
            "sha256:9c1e4fbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        assert!(message.contains("https://10.0.0.5:4747"), "{message}");
        assert!(message.contains("sha256:51aa07"), "{message}");
        assert!(message.contains("sha256:9c1e4f"), "{message}");
        assert!(message.contains("legion join-token create"), "{message}");
    }

    /// A manager's cluster server certificate from a fresh CA, and the CA's
    /// pin.
    fn manager_server_key() -> R<(Arc<CertifiedKey>, String)> {
        let (ca_pem, ca_key) = crate::manager::mesh_ca::generate_ca()?;
        let (cert, key, _) = crate::manager::mesh_ca::issue_manager_cert(
            &ca_pem,
            &ca_key,
            "m1",
            crate::manager::mesh_ca::ManagerCertKind::Server,
        )?;
        Ok((
            certified_key(&format!("{cert}{ca_pem}"), &key)?,
            ca_pin_pem(&ca_pem)?,
        ))
    }

    async fn sniffing(
        operator: Option<Arc<CertifiedKey>>,
    ) -> R<(
        std::net::SocketAddr,
        tokio_stream::wrappers::ReceiverStream<std::io::Result<MaybeTlsStream>>,
        String,
    )> {
        let (cluster_key, pin) = manager_server_key()?;
        let cluster = SwappableIdentity::new();
        cluster.set(cluster_key);
        let tls_only = operator.is_some();
        let config = server_config(
            Arc::new(ManagerServerCerts { cluster, operator }),
            None,
            true,
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        Ok((addr, sniffing_incoming(listener, config, tls_only), pin))
    }

    async fn tls_client(
        addr: std::net::SocketAddr,
        pin: &str,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let verifier = PinnedManagerVerifier::new(ManagerTrust::Pin(pin.to_string()));
        let config = client_config(verifier, None).expect("client config");
        let tcp = tokio::net::TcpStream::connect(addr).await?;
        tokio_rustls::TlsConnector::from(config)
            .connect(
                ServerName::try_from(MANAGER_SERVER_NAME.to_string()).expect("name"),
                tcp,
            )
            .await
    }

    #[tokio::test]
    async fn the_manager_port_serves_tls_to_workers_and_plaintext_to_the_cli_test() -> R<()> {
        use tokio::io::AsyncWriteExt;
        use tokio_stream::StreamExt;
        let (addr, mut incoming, pin) = sniffing(None).await?;

        // A worker's TLS handshake, verified against the CA pin.
        let client = tokio::spawn(async move { tls_client(addr, &pin).await });
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), incoming.next())
            .await?
            .context("a connection")??;
        assert!(matches!(accepted, MaybeTlsStream::Tls(_)));
        client
            .await?
            .context("the worker verified the cluster certificate")?;

        // The CLI's plaintext HTTP/2 preface on the same port.
        let mut plain = tokio::net::TcpStream::connect(addr).await?;
        plain.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await?;
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), incoming.next())
            .await?
            .context("a connection")??;
        assert!(matches!(accepted, MaybeTlsStream::Plain(_)));
        Ok(())
    }

    #[tokio::test]
    async fn a_worker_refuses_a_manager_from_another_cluster_test() -> R<()> {
        use tokio_stream::StreamExt;
        let (addr, mut incoming, _pin) = sniffing(None).await?;
        let (_, other_pin) = manager_server_key()?;
        let client = tokio::spawn(async move { tls_client(addr, &other_pin).await });
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), incoming.next()).await;
        let err = client.await?.expect_err("the pin does not match");
        assert!(err.to_string().contains("CA pin mismatch"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn with_operator_tls_workers_still_get_the_cluster_certificate_test() -> R<()> {
        use tokio::io::AsyncWriteExt;
        use tokio_stream::StreamExt;
        // The operator's certificate comes from somewhere else entirely.
        let (operator, _) = manager_server_key()?;
        let (addr, mut incoming, pin) = sniffing(Some(operator)).await?;
        let client = tokio::spawn(async move { tls_client(addr, &pin).await });
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), incoming.next())
            .await?
            .context("a connection")??;
        assert!(matches!(accepted, MaybeTlsStream::Tls(_)));
        client
            .await?
            .context("`manager.legion` selects the cluster certificate")?;

        // Plaintext is dropped when the operator turned TLS on.
        let mut plain = tokio::net::TcpStream::connect(addr).await?;
        plain.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(500), incoming.next())
                .await
                .is_err(),
            "no plaintext connection is yielded"
        );
        Ok(())
    }
}
