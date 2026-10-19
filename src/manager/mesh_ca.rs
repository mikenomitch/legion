//! The cluster CA: the certificates behind the worker plane, the
//! worker-to-manager channel, and the mesh (`service.mesh = true`)
//! (docs/auth-and-acls.md, "The worker plane"; docs/network-handlers.md,
//! "mTLS").
//!
//! With the default `builtin` provider: one CA per cluster, generated on
//! first use and replicated with the control lane (`net_mesh_ca`). Every
//! manager holds the CA key through the store, so every manager can sign;
//! nothing is distributed. It signs:
//!
//! - **node** certificates, from a CSR the worker sends: the worker keeps its
//!   private key, and the manager chooses the SANs, ignoring whatever the
//!   CSR asks for. One per worker, valid 24 hours, refreshed after 8;
//! - **fleet** certificates for the simulator, which serves a whole
//!   simulated fleet from one listener;
//! - **manager** certificates, minted by each manager for itself from a
//!   key and CSR it generates: a client certificate it presents to workers
//!   and a server certificate it presents to workers on port 4747.
//!
//! The role is in the SANs (`crate::cluster_tls`), checked on every
//! connection. A workload's identity rides *inside* the node-to-node mesh
//! session as a PROXY protocol v2 TLV, never as a certificate of its own
//! (service-mesh calls entry 2).
//!
//! Rotation (`legion net ca rotate`) cross-signs the new CA with the old one
//! and switches signing at once; both CAs stay in the trust bundle for one
//! node-certificate lifetime (calls entry 20).
//!
//! This module is the *built-in* signer. Which CA actually signs is chosen
//! by the manager's `clusterCa` provider (`crate::manager::cluster_ca`):
//! `builtin` uses the replicated row above, `file` signs with these same
//! functions under an operator-provided CA, and `exec` hands the request to
//! an external signer. Every provider is given the same [`LeafSpec`]: the
//! names come from the authenticated caller, never from the CSR.

use crate::cluster_tls::{
    FLEET_URI_PREFIX, MANAGER_SERVER_NAME, MANAGER_URI_PREFIX, NODE_URI_PREFIX,
};
use crate::prelude::*;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// How long a node, fleet, or manager certificate is valid.
pub const NODE_CERT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Holders refresh once this much of the lifetime has elapsed, so a
/// manager outage of several hours never expires a node out of the mesh or
/// the worker plane.
pub const NODE_CERT_REFRESH_AFTER: Duration = Duration::from_secs(8 * 60 * 60);
/// How long the previous CA stays trusted after a rotation: one
/// certificate lifetime, so every certificate the old CA signed has expired
/// by the time it leaves the bundle.
pub const CA_ROTATION_OVERLAP: Duration = NODE_CERT_TTL;
/// Backdate `notBefore` by this much so a holder whose clock lags the
/// manager's accepts its own certificate immediately.
const NOT_BEFORE_SKEW: time::Duration = time::Duration::minutes(5);
/// The CA lives for a decade; rotation is an operator action.
const CA_TTL_DAYS: i64 = 365 * 10;

/// The DNS name a node certificate is verified against.
pub fn node_dns_name(node_id: &str) -> String {
    format!("{}.node.legion", dns_label(node_id))
}

/// The DNS name a simulated fleet's shared certificate is verified against.
pub fn fleet_dns_name(fleet: &str) -> String {
    format!("{}.fleet.legion", dns_label(fleet))
}

/// One DNS label for a node, fleet, or manager name, distinct for distinct
/// names. A name that already is a plain label (lowercase `[a-z0-9-]`, at
/// most 63 characters, no leading, trailing, or doubled `-`) is its own
/// label. Any other name gets a readable head and `--` plus 16 hex digits of
/// its SHA-256, a form no plain label takes. Folding case and characters
/// alone (`NODE-1`, `node.1`, and `node_1` all became `node-1`) let a node
/// registered under a look-alike name hold a certificate that verified as
/// another node.
fn dns_label(raw: &str) -> String {
    let plain = !raw.is_empty()
        && raw.len() <= 63
        && !raw.starts_with('-')
        && !raw.ends_with('-')
        && !raw.contains("--")
        && raw
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-');
    if plain {
        return raw.to_string();
    }
    let folded: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let mut head = String::new();
    for ch in folded.trim_matches('-').chars() {
        if head.len() == 45 {
            break;
        }
        // Collapse runs, so the only `--` is the one before the digest.
        if !(ch == '-' && head.ends_with('-')) {
            head.push(ch);
        }
    }
    let head = head.trim_end_matches('-');
    let digest: String = Sha256::digest(raw.as_bytes())[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if head.is_empty() {
        digest
    } else {
        format!("{head}--{digest}")
    }
}

/// The URI SAN that names a node.
pub fn node_uri(node_id: &str) -> String {
    format!("{NODE_URI_PREFIX}{node_id}")
}

/// The URI SAN that names a manager.
pub fn manager_uri(manager_id: &str) -> String {
    format!("{MANAGER_URI_PREFIX}{manager_id}")
}

/// The URI SAN that names a simulated fleet.
pub fn fleet_uri(fleet: &str) -> String {
    format!("{FLEET_URI_PREFIX}{fleet}")
}

/// A fresh CA: (certificate PEM, private key PEM). Each CA gets a distinct
/// common name, so during a rotation the two CAs in the bundle (and the
/// cross-signed certificate between them) are told apart by name, not only
/// by key.
pub fn generate_ca() -> R<(String, String)> {
    let key = KeyPair::generate()?;
    let suffix: String = (0..4)
        .map(|_| format!("{:02x}", rand::random::<u8>()))
        .collect();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name = ca_name(&suffix);
    params.not_before = time::OffsetDateTime::now_utc() - NOT_BEFORE_SKEW;
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(CA_TTL_DAYS);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let cert = params.self_signed(&key)?;
    Ok((cert.pem(), key.serialize_pem()))
}

fn ca_name(suffix: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, format!("legion-cluster-ca-{suffix}"));
    dn
}

/// The new CA's certificate signed by the previous CA, so a holder that
/// still trusts only the previous CA verifies chains the new CA signs
/// (`leaf, cross`), while a holder that trusts the new CA ignores it.
pub fn cross_sign(
    new_ca_cert_pem: &str,
    new_ca_key_pem: &str,
    old_ca_cert_pem: &str,
    old_ca_key_pem: &str,
) -> R<String> {
    let new_key = KeyPair::from_pem(new_ca_key_pem).context("new CA key")?;
    let params = CertificateParams::from_ca_cert_pem(new_ca_cert_pem).context("new CA")?;
    let old = CaSigner::from_pem(old_ca_cert_pem, old_ca_key_pem)?;
    Ok(params.signed_by(&new_key, &old.issuer, &old.key)?.pem())
}

/// What a CSR is signed as. The manager picks this from the caller's
/// authenticated identity, never from the CSR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsrSubject {
    /// A worker node: `<node>.node.legion`, `legion://node/<id>`.
    Node(String),
    /// A simulated fleet: `<fleet>.fleet.legion`, `legion://fleet/<name>`.
    Fleet(String),
}

/// Which manager certificate to mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerCertKind {
    /// Presented when dialing the worker plane: `legion://manager/<id>`,
    /// client auth only.
    Client,
    /// Presented to workers on port 4747: `manager.legion` and
    /// `legion://manager/<id>`, server auth only.
    Server,
}

/// The four kinds of leaf Legion issues. An `exec` provider receives it as
/// the request's `role`, so a helper can map roles to separate CA roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafRole {
    Node,
    Fleet,
    ManagerServer,
    ManagerClient,
}

impl LeafRole {
    /// The wire name: `node`, `fleet`, `manager_server`, `manager_client`.
    pub fn as_str(self) -> &'static str {
        match self {
            LeafRole::Node => "node",
            LeafRole::Fleet => "fleet",
            LeafRole::ManagerServer => "manager_server",
            LeafRole::ManagerClient => "manager_client",
        }
    }
}

/// Everything a leaf certificate is issued as: Legion's decision, made from
/// the caller's authenticated identity. Every provider signs exactly this,
/// and what a non-builtin provider returns is checked against it
/// (`crate::manager::cluster_ca::verify`). The CSR contributes only its
/// public key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafSpec {
    pub role: LeafRole,
    /// Hostname-shaped, so an external CA's role can restrict it to
    /// `*.legion` (cluster-ca-provider calls entry 9). Never used for
    /// verification: roles come from URI SANs.
    pub common_name: String,
    /// Exactly these DNS SANs, no more.
    pub dns_names: Vec<String>,
    /// Exactly these URI SANs, no more.
    pub uris: Vec<String>,
    pub server_auth: bool,
    pub client_auth: bool,
    /// The lifetime asked for.
    pub ttl: Duration,
}

impl LeafSpec {
    /// A node or fleet certificate: server and client auth, 24 hours.
    pub fn for_subject(subject: &CsrSubject) -> LeafSpec {
        match subject {
            CsrSubject::Node(node_id) => LeafSpec {
                role: LeafRole::Node,
                common_name: node_dns_name(node_id),
                dns_names: vec![node_dns_name(node_id)],
                uris: vec![node_uri(node_id)],
                server_auth: true,
                client_auth: true,
                ttl: NODE_CERT_TTL,
            },
            CsrSubject::Fleet(fleet) => LeafSpec {
                role: LeafRole::Fleet,
                common_name: fleet_dns_name(fleet),
                dns_names: vec![fleet_dns_name(fleet)],
                uris: vec![fleet_uri(fleet)],
                server_auth: true,
                client_auth: true,
                ttl: NODE_CERT_TTL,
            },
        }
    }

    /// A manager's own certificate: `legion://manager/<id>`, plus
    /// `manager.legion` for the server certificate.
    pub fn for_manager(manager_id: &str, kind: ManagerCertKind) -> LeafSpec {
        match kind {
            ManagerCertKind::Server => LeafSpec {
                role: LeafRole::ManagerServer,
                common_name: MANAGER_SERVER_NAME.to_string(),
                dns_names: vec![MANAGER_SERVER_NAME.to_string()],
                uris: vec![manager_uri(manager_id)],
                server_auth: true,
                client_auth: false,
                ttl: NODE_CERT_TTL,
            },
            ManagerCertKind::Client => LeafSpec {
                role: LeafRole::ManagerClient,
                common_name: format!("{}.manager.legion", dns_label(manager_id)),
                dns_names: Vec::new(),
                uris: vec![manager_uri(manager_id)],
                server_auth: false,
                client_auth: true,
                ttl: NODE_CERT_TTL,
            },
        }
    }

    /// The extended key usages asked for, by their wire names
    /// (`server_auth`, `client_auth`).
    pub fn usages(&self) -> Vec<&'static str> {
        let mut usages = Vec::new();
        if self.server_auth {
            usages.push("server_auth");
        }
        if self.client_auth {
            usages.push("client_auth");
        }
        usages
    }

    fn params(&self) -> R<(CertificateParams, i64)> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, self.common_name.clone());
        let mut params = CertificateParams::default();
        params.distinguished_name = dn;
        let mut sans = Vec::new();
        for dns in &self.dns_names {
            sans.push(SanType::DnsName(Ia5String::try_from(dns.clone())?));
        }
        for uri in &self.uris {
            sans.push(SanType::URI(Ia5String::try_from(uri.clone())?));
        }
        params.subject_alt_names = sans;
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - NOT_BEFORE_SKEW;
        let not_after = now + time::Duration::try_from(self.ttl)?;
        params.not_after = not_after;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let mut usages = Vec::new();
        if self.server_auth {
            usages.push(ExtendedKeyUsagePurpose::ServerAuth);
        }
        if self.client_auth {
            usages.push(ExtendedKeyUsagePurpose::ClientAuth);
        }
        params.extended_key_usages = usages;
        Ok((params, not_after.unix_timestamp()))
    }
}

/// A CA certificate and its key, parsed once, that signs leaves locally:
/// the built-in CA, or an operator's CA under the `file` provider.
pub struct CaSigner {
    issuer: rcgen::Certificate,
    key: KeyPair,
}

impl CaSigner {
    /// Parses a CA certificate (the first certificate in `ca_cert_pem`) and
    /// its private key. The key must be PKCS#8 (`BEGIN PRIVATE KEY`):
    /// ECDSA P-256/P-384, Ed25519, or RSA.
    pub fn from_pem(ca_cert_pem: &str, ca_key_pem: &str) -> R<CaSigner> {
        let key = parse_ca_key(ca_key_pem)?;
        let first = crate::cluster_tls::certs_from_pem(ca_cert_pem)
            .context("cluster CA certificate")?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("the cluster CA certificate PEM holds no certificate"))?;
        // The CA as an rcgen issuer: its parameters re-signed with its own
        // key. The subject and key are what leaves chain to, so this signs
        // exactly as the stored certificate would.
        let issuer = CertificateParams::from_ca_cert_der(&first)
            .context("cluster CA certificate")?
            .self_signed(&key)?;
        Ok(CaSigner { issuer, key })
    }

    /// Signs `csr_pem` as `spec`: (leaf certificate PEM, expiry as Unix
    /// seconds). Only the CSR's public key is used.
    pub fn sign(&self, csr_pem: &str, spec: &LeafSpec) -> R<(String, i64)> {
        let csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .map_err(|err| anyhow!("invalid certificate signing request: {err}"))?;
        let (params, expires) = spec.params()?;
        let cert = params.signed_by(&csr.public_key, &self.issuer, &self.key)?;
        Ok((cert.pem(), expires))
    }
}

/// The CA key as an rcgen key pair. rcgen (on `ring`) reads PKCS#8 only, so
/// a SEC1 or PKCS#1 key gets the conversion command instead of a parse
/// error.
fn parse_ca_key(ca_key_pem: &str) -> R<KeyPair> {
    use rustls_pki_types::PrivateKeyDer;
    let key = crate::cluster_tls::key_from_pem(ca_key_pem).context("cluster CA key")?;
    match key {
        PrivateKeyDer::Pkcs8(der) => KeyPair::try_from(der.secret_pkcs8_der())
            .map_err(|err| anyhow!("cluster CA key: unsupported PKCS#8 key ({err})")),
        PrivateKeyDer::Sec1(_) | PrivateKeyDer::Pkcs1(_) => bail!(
            "cluster CA key is not PKCS#8 (it is a `BEGIN EC PRIVATE KEY` or `BEGIN RSA PRIVATE \
             KEY` file); convert it with `openssl pkcs8 -topk8 -nocrypt -in ca.key -out \
             ca-pkcs8.key`"
        ),
        _ => bail!("cluster CA key: unsupported private key format"),
    }
}

/// Signs `csr_pem` as `spec` with the given CA: (certificate PEM, expiry as
/// Unix seconds).
pub fn sign_leaf(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    csr_pem: &str,
    spec: &LeafSpec,
) -> R<(String, i64)> {
    CaSigner::from_pem(ca_cert_pem, ca_key_pem)?.sign(csr_pem, spec)
}

/// Signs a CSR with the CA: (certificate PEM, expiry as Unix seconds).
///
/// Only the CSR's public key is used. The SANs, extensions, and subject it
/// requests are ignored, so a worker can't ask for `manager.legion` or
/// another node's name (plan §6.4).
pub fn sign_csr(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    csr_pem: &str,
    subject: &CsrSubject,
) -> R<(String, i64)> {
    sign_leaf(
        ca_cert_pem,
        ca_key_pem,
        csr_pem,
        &LeafSpec::for_subject(subject),
    )
}

/// A certificate a manager mints for itself: (certificate PEM, private key
/// PEM, expiry as Unix seconds). The key is generated in the manager process
/// and never leaves it; the CA signs a CSR for it, the same way every
/// provider does (`crate::manager::cluster_ca`).
pub fn issue_manager_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    manager_id: &str,
    kind: ManagerCertKind,
) -> R<(String, String, i64)> {
    let spec = LeafSpec::for_manager(manager_id, kind);
    let (key_pem, csr_pem) = generate_csr(&spec.common_name)?;
    let (cert_pem, expires) = sign_leaf(ca_cert_pem, ca_key_pem, &csr_pem, &spec)?;
    Ok((cert_pem, key_pem, expires))
}

/// A fresh key pair and a CSR for it: (key PEM, CSR PEM). The CSR's subject
/// is informational; the manager ignores it.
pub fn generate_csr(common_name: &str) -> R<(String, String)> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name.to_string());
    params.distinguished_name = dn;
    let csr = params.serialize_request(&key)?;
    Ok((key.serialize_pem(), csr.pem()?))
}

/// A CSR for an existing key.
pub fn csr_for_key(key_pem: &str, common_name: &str) -> R<String> {
    let key = KeyPair::from_pem(key_pem).context("node key")?;
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name.to_string());
    params.distinguished_name = dn;
    Ok(params.serialize_request(&key)?.pem()?)
}

/// A node certificate with a key generated here: (certificate PEM, key PEM,
/// expiry). Tests only. Real nodes send a CSR, and the manager never
/// generates or returns a node's private key.
#[cfg(test)]
pub fn issue_node_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    node_id: &str,
) -> R<(String, String, i64)> {
    let (key_pem, csr_pem) = generate_csr(node_id)?;
    let (cert_pem, expires) = sign_csr(
        ca_cert_pem,
        ca_key_pem,
        &csr_pem,
        &CsrSubject::Node(node_id.to_string()),
    )?;
    Ok((cert_pem, key_pem, expires))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_tls::{peer_role, uri_sans, PeerRole};

    fn verify_client(ca_pem: &str, chain_pem: &str) -> R<()> {
        let roots = crate::cluster_tls::roots_from_pem(ca_pem)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
            .build()
            .expect("verifier");
        let chain = crate::cluster_tls::certs_from_pem(chain_pem)?;
        verifier
            .verify_client_cert(&chain[0], &chain[1..], rustls_pki_types::UnixTime::now())
            .map_err(|err| anyhow!("{err}"))?;
        Ok(())
    }

    #[test]
    fn node_names_are_dns_safe() {
        let folded = node_dns_name("Node_1.local");
        assert!(folded.starts_with("node-1-local--"), "{folded}");
        assert!(folded.ends_with(".node.legion"), "{folded}");
        assert_eq!(node_dns_name("node-1"), "node-1.node.legion");
        assert_eq!(node_uri("n1"), "legion://node/n1");
        assert_eq!(fleet_dns_name("sim"), "sim.fleet.legion");
        assert_eq!(manager_uri("mgr-a"), "legion://manager/mgr-a");
    }

    /// Names that fold to the same characters keep distinct certificate
    /// names, and no name can take the label another name was given.
    #[test]
    fn look_alike_node_names_get_distinct_dns_names_test() {
        let names = ["node-1", "NODE-1", "node.1", "node_1", "node--1", "-node-1"];
        let labels: std::collections::HashSet<String> =
            names.iter().map(|name| node_dns_name(name)).collect();
        assert_eq!(labels.len(), names.len(), "{labels:?}");
        // A plain name that spells out another name's hashed label is itself
        // hashed (it contains `--`), so it cannot claim that label.
        let hashed = dns_label("node.1");
        assert_ne!(dns_label(&hashed), hashed);
        let long = "a".repeat(80);
        assert!(dns_label(&long).len() <= 63);
        assert!(!dns_label("...").is_empty());
    }

    #[test]
    fn issued_node_cert_chains_to_the_ca() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        let (cert_pem, key_pem, expires) = issue_node_cert(&ca_pem, &ca_key, "node-1")?;
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("PRIVATE KEY"));
        assert!(expires > time::OffsetDateTime::now_utc().unix_timestamp());
        verify_client(&ca_pem, &cert_pem)
    }

    #[test]
    fn csr_signing_ignores_requested_names_test() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        // A CSR asking for the manager's name and another node's URI.
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![
            SanType::DnsName(Ia5String::try_from(MANAGER_SERVER_NAME.to_string())?),
            SanType::URI(Ia5String::try_from(manager_uri("evil"))?),
            SanType::URI(Ia5String::try_from(node_uri("node-2"))?),
        ];
        let csr_pem = params.serialize_request(&key)?.pem()?;
        let (cert_pem, _) = sign_csr(
            &ca_pem,
            &ca_key,
            &csr_pem,
            &CsrSubject::Node("node-1".into()),
        )?;
        let der = crate::cluster_tls::certs_from_pem(&cert_pem)?;
        assert_eq!(uri_sans(der[0].as_ref())?, vec![node_uri("node-1")]);
        assert_eq!(peer_role(der[0].as_ref()), PeerRole::Node("node-1".into()));
        let (_, parsed) = x509_parser::parse_x509_certificate(der[0].as_ref())?;
        let dns: Vec<String> = parsed
            .subject_alternative_name()?
            .expect("sans")
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                x509_parser::extensions::GeneralName::DNSName(dns) => Some(dns.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(dns, vec![node_dns_name("node-1")]);
        Ok(())
    }

    #[test]
    fn a_request_without_a_valid_csr_is_refused_test() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        assert!(sign_csr(&ca_pem, &ca_key, "", &CsrSubject::Node("n".into())).is_err());
        assert!(sign_csr(
            &ca_pem,
            &ca_key,
            "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n",
            &CsrSubject::Node("n".into())
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn manager_certs_carry_their_role_test() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        let (client, _, _) =
            issue_manager_cert(&ca_pem, &ca_key, "mgr-a", ManagerCertKind::Client)?;
        let der = crate::cluster_tls::certs_from_pem(&client)?;
        assert_eq!(
            peer_role(der[0].as_ref()),
            PeerRole::Manager("mgr-a".into())
        );
        verify_client(&ca_pem, &client)?;

        let (server, key, _) =
            issue_manager_cert(&ca_pem, &ca_key, "mgr-a", ManagerCertKind::Server)?;
        let roots = crate::cluster_tls::roots_from_pem(&ca_pem)?;
        let verifier =
            rustls::client::WebPkiServerVerifier::builder(std::sync::Arc::new(roots)).build()?;
        let chain = crate::cluster_tls::certs_from_pem(&server)?;
        use rustls::client::danger::ServerCertVerifier;
        verifier
            .verify_server_cert(
                &chain[0],
                &[],
                &rustls_pki_types::ServerName::try_from(MANAGER_SERVER_NAME)?,
                &[],
                rustls_pki_types::UnixTime::now(),
            )
            .map_err(|err| anyhow!("{err}"))?;
        assert!(key.contains("PRIVATE KEY"));

        // A node certificate never verifies as the manager's server name.
        let (node, _, _) = issue_node_cert(&ca_pem, &ca_key, "node-1")?;
        let node_chain = crate::cluster_tls::certs_from_pem(&node)?;
        assert!(verifier
            .verify_server_cert(
                &node_chain[0],
                &[],
                &rustls_pki_types::ServerName::try_from(MANAGER_SERVER_NAME)?,
                &[],
                rustls_pki_types::UnixTime::now(),
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn cross_signed_chains_verify_under_either_ca_test() -> R<()> {
        let (old_pem, old_key) = generate_ca()?;
        let (new_pem, new_key) = generate_ca()?;
        let cross = cross_sign(&new_pem, &new_key, &old_pem, &old_key)?;
        let (leaf, _, _) = issue_node_cert(&new_pem, &new_key, "node-1")?;
        // A holder that trusts only the old CA verifies `leaf, cross`.
        verify_client(&old_pem, &format!("{leaf}{cross}"))?;
        // A holder that trusts only the new CA verifies the same chain.
        verify_client(&new_pem, &format!("{leaf}{cross}"))?;
        // Without the cross certificate, the old CA does not verify it.
        assert!(verify_client(&old_pem, &leaf).is_err());
        Ok(())
    }
}
