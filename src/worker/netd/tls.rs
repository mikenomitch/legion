//! TLS on the datapath (docs/network-handlers.md, "mTLS").
//!
//! Legion terminates and originates; a handler never sees TLS. Two sources
//! of material:
//!
//! - **bring-your-own** — `tls = { mode = "server" | "mutual", certPath,
//!   keyPath, clientCa }` on an ingress chain: files the operator placed on
//!   the node (or staged as artifacts). With `mutual`, the client's
//!   certificate is verified against `clientCa` and its identity — the first
//!   URI SAN, else the first DNS SAN, else the CN — feeds `identity` rules
//!   and `requireIdentity`.
//! - **the mesh** — `certFrom = "legion"` (and `clientCa = "legion"`): the
//!   node's own certificate from `IssueNodeCertificate` — the same one its
//!   worker plane serves (`crate::worker::plane`), issued at registration
//!   from a key the node generated, persisted under `<state-dir>/identity/`,
//!   and refreshed on the heartbeat loop. Node-to-node hops are mTLS on node
//!   certificates; the *workload* on each end is named by a PROXY v2 TLV
//!   inside the session, trusted because the peer proved it is a node.

use crate::prelude::*;
use crate::structs::netpolicy::{glob_matches, TlsConfig, TLS_MUTUAL};

use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::path::Path;
use std::time::Instant;

/// A node's mesh credential, as issued by the manager.
pub struct MeshIdentity {
    pub node_id: String,
    pub cert_chain: Vec<CertificateDer<'static>>,
    key_pem: String,
    pub ca: Vec<CertificateDer<'static>>,
    pub ca_pem: String,
    pub expires_at: i64,
    pub issued: Instant,
}

impl std::fmt::Debug for MeshIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshIdentity")
            .field("node_id", &self.node_id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl MeshIdentity {
    pub fn from_pem(
        node_id: &str,
        cert_pem: &str,
        key_pem: &str,
        ca_pem: &str,
        expires_at: i64,
    ) -> R<Self> {
        let cert_chain = certs_from_pem(cert_pem.as_bytes()).context("mesh certificate")?;
        if cert_chain.is_empty() {
            bail!("mesh certificate PEM holds no certificate");
        }
        // Parse once to fail early; the key is re-read per config build
        // because `PrivateKeyDer` is not `Clone` across all variants cheaply.
        let _ = key_from_pem(key_pem.as_bytes()).context("mesh key")?;
        let ca = certs_from_pem(ca_pem.as_bytes()).context("mesh CA")?;
        if ca.is_empty() {
            bail!("mesh CA PEM holds no certificate");
        }
        Ok(MeshIdentity {
            node_id: node_id.to_string(),
            cert_chain,
            key_pem: key_pem.to_string(),
            ca,
            ca_pem: ca_pem.to_string(),
            expires_at,
            issued: Instant::now(),
        })
    }

    /// Whether the worker should ask for a new certificate.
    pub fn needs_refresh(&self) -> bool {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        self.issued.elapsed() >= crate::manager::mesh_ca::NODE_CERT_REFRESH_AFTER
            || self.expires_at - now < 60 * 60
    }

    fn key(&self) -> R<PrivateKeyDer<'static>> {
        key_from_pem(self.key_pem.as_bytes())
    }

    fn roots(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        for cert in &self.ca {
            let _ = roots.add(cert.clone());
        }
        roots
    }

    /// Dial another node: trust the mesh CA, present this node's certificate.
    pub fn client_config(&self) -> R<Arc<ClientConfig>> {
        Ok(Arc::new(
            ClientConfig::builder()
                .with_root_certificates(self.roots())
                .with_client_auth_cert(self.cert_chain.clone(), self.key()?)?,
        ))
    }

    /// Terminate on an advertised port with this node's certificate;
    /// `mutual` demands a certificate from the mesh CA.
    pub fn server_config(&self, mutual: bool) -> R<Arc<ServerConfig>> {
        let builder = ServerConfig::builder();
        let config = if mutual {
            let verifier = WebPkiClientVerifier::builder(Arc::new(self.roots()))
                .build()
                .map_err(|err| anyhow::anyhow!("mesh client verifier: {err}"))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(self.cert_chain.clone(), self.key()?)?
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(self.cert_chain.clone(), self.key()?)?
        };
        Ok(Arc::new(config))
    }
}

/// A certificate held in KV (`certFrom = { kv = "<path>" }`), as one PEM
/// bundle: the chain, leaf first, then the private key. Kept in memory only;
/// the key is redacted from `Debug`.
pub struct KvCertificate {
    pub kv_path: String,
    pub version: i64,
    pub cert_chain: Vec<CertificateDer<'static>>,
    key_pem: String,
}

impl std::fmt::Debug for KvCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvCertificate")
            .field("kv_path", &self.kv_path)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl KvCertificate {
    pub fn from_bundle(kv_path: &str, version: i64, bundle_pem: &str) -> R<Self> {
        validate_bundle(bundle_pem)?;
        Ok(KvCertificate {
            kv_path: kv_path.to_string(),
            version,
            cert_chain: certs_from_pem(bundle_pem.as_bytes())?,
            key_pem: bundle_pem.to_string(),
        })
    }

    /// Terminate with this certificate. `mutual` verifies clients against
    /// `clientCa`: the mesh CA for `"legion"`, otherwise a bundle on disk.
    pub fn server_config(
        &self,
        tls: &TlsConfig,
        mesh: Option<&MeshIdentity>,
    ) -> R<Arc<ServerConfig>> {
        let key = key_from_pem(self.key_pem.as_bytes())?;
        let builder = ServerConfig::builder();
        let config = if tls.mode == TLS_MUTUAL {
            let roots = match tls.client_ca.as_deref() {
                Some("legion") => mesh
                    .ok_or_else(|| anyhow::anyhow!("mesh certificate not issued yet"))?
                    .roots(),
                Some(path) => {
                    let mut roots = RootCertStore::empty();
                    for cert in certs_from_pem(&read(path)?)? {
                        roots.add(cert)?;
                    }
                    roots
                }
                None => bail!("tls.clientCa is required for mutual"),
            };
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|err| anyhow::anyhow!("client verifier: {err}"))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(self.cert_chain.clone(), key)?
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(self.cert_chain.clone(), key)?
        };
        Ok(Arc::new(config))
    }
}

/// Check a `certFrom = { kv }` bundle: at least one certificate, a private
/// key, and a key that belongs to the leaf. The manager runs this before
/// sending a bundle, so a bad `kv put` names the key in the allocation's
/// status instead of failing every handshake.
pub fn validate_bundle(bundle_pem: &str) -> R<()> {
    let chain = certs_from_pem(bundle_pem.as_bytes())?;
    if chain.is_empty() {
        bail!("holds no certificate (expected the chain, then the private key, as PEM)");
    }
    let key = key_from_pem(bundle_pem.as_bytes())
        .map_err(|_| anyhow::anyhow!("holds no private key (append it after the chain)"))?;
    // Building a config checks that the key matches the leaf.
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|err| anyhow::anyhow!("certificate and key do not match: {err}"))?;
    Ok(())
}

pub fn certs_from_pem(pem: &[u8]) -> R<Vec<CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(pem);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| anyhow::anyhow!("invalid certificate PEM: {err}"))
}

pub fn key_from_pem(pem: &[u8]) -> R<PrivateKeyDer<'static>> {
    let mut reader = std::io::BufReader::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| anyhow::anyhow!("invalid key PEM: {err}"))?
        .ok_or_else(|| anyhow::anyhow!("key PEM holds no private key"))
}

fn read(path: &str) -> R<Vec<u8>> {
    std::fs::read(Path::new(path)).with_context(|| format!("reading {path}"))
}

/// Bring-your-own material for an ingress chain.
pub fn byo_server_config(tls: &TlsConfig) -> R<Arc<ServerConfig>> {
    let cert_path = tls
        .cert_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("tls.certPath is required"))?;
    let key_path = tls
        .key_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("tls.keyPath is required"))?;
    let chain = certs_from_pem(&read(cert_path)?)?;
    if chain.is_empty() {
        bail!("{cert_path} holds no certificate");
    }
    let key = key_from_pem(&read(key_path)?)?;
    let builder = ServerConfig::builder();
    let config = if tls.mode == TLS_MUTUAL {
        let ca_path = tls
            .client_ca
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("tls.clientCa is required for mutual"))?;
        let mut roots = RootCertStore::empty();
        for cert in certs_from_pem(&read(ca_path)?)? {
            roots.add(cert)?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|err| anyhow::anyhow!("client verifier from {ca_path}: {err}"))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)?
    } else {
        builder.with_no_client_auth().with_single_cert(chain, key)?
    };
    Ok(Arc::new(config))
}

/// Egress `originate` against a pinned CA bundle (`tls.caPath`).
pub fn pinned_client_config(ca_path: &str) -> R<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    for cert in certs_from_pem(&read(ca_path)?)? {
        roots.add(cert)?;
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

/// Who a verified client certificate says it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerIdentity {
    /// A Legion node (`legion://node/<id>` SAN): trust its PROXY v2 TLVs.
    Node(String),
    /// Anything else: the first URI SAN, else DNS SAN, else CN.
    Other(String),
}

impl PeerIdentity {
    pub fn as_str(&self) -> &str {
        match self {
            PeerIdentity::Node(id) | PeerIdentity::Other(id) => id.as_str(),
        }
    }
}

/// Read the identity off the leaf of a verified chain.
pub fn peer_identity(chain: &[CertificateDer<'_>]) -> Option<PeerIdentity> {
    use x509_parser::extensions::GeneralName;
    let leaf = chain.first()?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref()).ok()?;
    let mut uri = None;
    let mut dns = None;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            match name {
                GeneralName::URI(value) => {
                    if let Some(node) = value.strip_prefix("legion://node/") {
                        return Some(PeerIdentity::Node(node.to_string()));
                    }
                    if uri.is_none() {
                        uri = Some(value.to_string());
                    }
                }
                GeneralName::DNSName(value) if dns.is_none() => {
                    dns = Some(value.to_string());
                }
                _ => {}
            }
        }
    }
    if let Some(uri) = uri {
        return Some(PeerIdentity::Other(uri));
    }
    if let Some(dns) = dns {
        return Some(PeerIdentity::Other(dns));
    }
    let common_name = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string);
    common_name.map(PeerIdentity::Other)
}

/// `tls.requireIdentity`: every glob is an alternative; empty means any
/// verified peer.
pub fn identity_allowed(globs: &[String], identity: Option<&str>) -> bool {
    if globs.is_empty() {
        return true;
    }
    match identity {
        Some(identity) => globs.iter().any(|glob| glob_matches(glob, identity)),
        None => false,
    }
}

pub fn server_name(host: &str) -> R<ServerName<'static>> {
    ServerName::try_from(host.to_string())
        .map_err(|err| anyhow::anyhow!("invalid TLS server name {host:?}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::mesh_ca::{generate_ca, issue_node_cert};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn identity_globs() {
        assert!(identity_allowed(&[], None));
        assert!(!identity_allowed(&["auto:web:*".into()], None));
        assert!(identity_allowed(
            &["auto:web:*".into()],
            Some("auto:web:default:server")
        ));
        assert!(!identity_allowed(
            &["auto:web:*".into()],
            Some("auto:db:default:pg")
        ));
    }

    /// Two mesh identities from one CA: a node-to-node handshake verifies
    /// both ways and the server reads the client's node id off its SAN.
    #[tokio::test]
    async fn mesh_identities_handshake_and_name_the_peer() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        let (a_cert, a_key, a_exp) = issue_node_cert(&ca_pem, &ca_key, "node-a")?;
        let (b_cert, b_key, b_exp) = issue_node_cert(&ca_pem, &ca_key, "node-b")?;
        let a = MeshIdentity::from_pem("node-a", &a_cert, &a_key, &ca_pem, a_exp)?;
        let b = MeshIdentity::from_pem("node-b", &b_cert, &b_key, &ca_pem, b_exp)?;
        assert!(!a.needs_refresh());

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let acceptor = tokio_rustls::TlsAcceptor::from(b.server_config(true)?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(stream).await.unwrap();
            let peer = peer_identity(tls.get_ref().1.peer_certificates().unwrap());
            let mut buf = [0u8; 5];
            tls.read_exact(&mut buf).await.unwrap();
            tls.write_all(b"pong!").await.unwrap();
            (peer, buf)
        });
        let connector = tokio_rustls::TlsConnector::from(a.client_config()?);
        let stream = TcpStream::connect(addr).await?;
        let mut tls = connector
            .connect(
                server_name(&crate::manager::mesh_ca::node_dns_name("node-b"))?,
                stream,
            )
            .await?;
        tls.write_all(b"ping!").await?;
        let mut reply = [0u8; 5];
        tls.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"pong!");
        let (peer, got) = server.await?;
        assert_eq!(&got, b"ping!");
        assert_eq!(peer, Some(PeerIdentity::Node("node-a".into())));

        // A client with no certificate is refused by a mutual listener.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let acceptor = tokio_rustls::TlsAcceptor::from(b.server_config(true)?);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(stream).await;
        });
        let mut roots = RootCertStore::empty();
        for cert in certs_from_pem(ca_pem.as_bytes())? {
            roots.add(cert)?;
        }
        let anonymous = tokio_rustls::TlsConnector::from(Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));
        let stream = TcpStream::connect(addr).await?;
        let result = async {
            let mut tls = anonymous
                .connect(
                    server_name(&crate::manager::mesh_ca::node_dns_name("node-b"))?,
                    stream,
                )
                .await?;
            tls.write_all(b"ping!").await?;
            let mut reply = [0u8; 5];
            tls.read_exact(&mut reply).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        assert!(
            result.is_err(),
            "an anonymous client must not get through mutual TLS"
        );
        Ok(())
    }
}
