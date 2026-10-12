//! The cluster CA: the certificates behind the mTLS worker plane
//! (docs/auth-and-acls.md, "The worker plane") and the service mesh's
//! `certFrom = "legion"` (docs/network-handlers.md, "mTLS").
//!
//! One CA per cluster, generated on first use and replicated with the
//! control lane (`net_mesh_ca`). It signs two kinds of certificate, and the
//! role lives in the URI SAN, checked on every call:
//!
//! - **Node certificates** — one per worker node, short-lived, refreshed by
//!   the worker on its heartbeat loop. The worker generates its own key pair
//!   and sends a CSR; the manager signs the public key with SANs it chooses
//!   (`<node>.node.legion`, what a dialing manager or node verifies, and
//!   `legion://node/<node id>`, what a receiving side reads back to name the
//!   peer) and ignores whatever the CSR asked for. A node's private key never
//!   crosses the network. The node certificate serves the worker plane
//!   (`Worker` gRPC on 4848) and the mesh's node-to-node hops.
//! - **Manager certificates** — minted by each manager for itself
//!   (`legion://manager/<manager id>`, `ClientAuth` only). A manager presents
//!   one when it dials a worker; the worker admits manager-only RPCs only for
//!   a certificate carrying that SAN, so "signed by the cluster CA" is never
//!   enough to drive a node.
//!
//! A workload's identity rides *inside* the node-to-node TLS session as a
//! PROXY protocol v2 TLV the receiving node attributes, never as a
//! certificate of its own (service-mesh calls entry 2).

use crate::prelude::*;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use std::time::Duration;

/// How long a node certificate is valid.
pub const NODE_CERT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Workers refresh once this much of the lifetime has elapsed, so a
/// manager outage of several hours never expires a node out of the mesh or
/// the worker plane.
pub const NODE_CERT_REFRESH_AFTER: Duration = Duration::from_secs(8 * 60 * 60);
/// How long a manager certificate is valid.
pub const MANAGER_CERT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Managers re-mint their own certificate once this much of its lifetime has
/// elapsed.
pub const MANAGER_CERT_REFRESH_AFTER: Duration = Duration::from_secs(8 * 60 * 60);
/// Backdate `notBefore` by this much so a peer whose clock lags the signer's
/// accepts a fresh certificate immediately.
const NOT_BEFORE_SKEW: time::Duration = time::Duration::minutes(5);
/// The CA lives for a decade; rotation is an operator action
/// (`legion net ca rotate`).
const CA_TTL_DAYS: i64 = 365 * 10;

/// The URI SAN prefix that marks a node certificate.
pub const NODE_URI_PREFIX: &str = "legion://node/";
/// The URI SAN prefix that marks a manager certificate. Only a certificate
/// carrying it may call a manager-only worker RPC.
pub const MANAGER_URI_PREFIX: &str = "legion://manager/";

/// The DNS name a node certificate is verified against.
pub fn node_dns_name(node_id: &str) -> String {
    let safe: String = node_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    format!("{}.node.legion", safe.trim_matches('-'))
}

/// The URI SAN that names a node.
pub fn node_uri(node_id: &str) -> String {
    format!("{NODE_URI_PREFIX}{node_id}")
}

/// The URI SAN that names a manager.
pub fn manager_uri(manager_id: &str) -> String {
    format!("{MANAGER_URI_PREFIX}{manager_id}")
}

/// A fresh CA: (certificate PEM, private key PEM).
pub fn generate_ca() -> R<(String, String)> {
    let key = KeyPair::generate()?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "legion-mesh-ca");
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name = dn;
    params.not_before = time::OffsetDateTime::now_utc() - NOT_BEFORE_SKEW;
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(CA_TTL_DAYS);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let cert = params.self_signed(&key)?;
    Ok((cert.pem(), key.serialize_pem()))
}

/// A short, stable fingerprint of a CA certificate: `sha256:<hex>` over the
/// DER of the first certificate in `ca_pem`. Managers key their worker-plane
/// caches on it, so a rotated CA is noticed without comparing PEM text.
pub fn ca_fingerprint(ca_pem: &str) -> R<String> {
    use sha2::{Digest, Sha256};
    let der = crate::worker::netd::tls::certs_from_pem(ca_pem.as_bytes())?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("CA PEM holds no certificate"))?;
    let digest = Sha256::digest(der.as_ref());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!("sha256:{hex}"))
}

/// The CA as an rcgen issuer: (certificate, key).
fn issuer(ca_cert_pem: &str, ca_key_pem: &str) -> R<(rcgen::Certificate, KeyPair)> {
    let ca_key = KeyPair::from_pem(ca_key_pem).context("cluster CA key")?;
    let ca_cert = CertificateParams::from_ca_cert_pem(ca_cert_pem)
        .context("cluster CA certificate")?
        .self_signed(&ca_key)?;
    Ok((ca_cert, ca_key))
}

/// A worker's key pair and the CSR for it: (CSR PEM, key PEM). Called on the
/// worker; the key never leaves it. The CSR's subject and SANs are a
/// courtesy only — the manager chooses the certificate's names itself.
pub fn node_csr(node_id: &str) -> R<(String, String)> {
    let key = KeyPair::generate()?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, node_id.to_string());
    let mut params = CertificateParams::default();
    params.distinguished_name = dn;
    let csr = params.serialize_request(&key)?;
    Ok((csr.pem()?, key.serialize_pem()))
}

/// Check that `csr_pem` is a PEM certificate request whose self-signature
/// verifies (proof the requester holds the key). The error is fit to show
/// the caller.
pub fn check_csr(csr_pem: &str) -> R<()> {
    CertificateSigningRequestParams::from_pem(csr_pem)
        .map(|_| ())
        .map_err(|err| anyhow!("invalid CSR (expected a PEM `CERTIFICATE REQUEST`): {err}"))
}

/// Sign a node's CSR: (certificate PEM, expiry as Unix seconds).
///
/// Only the CSR's public key is used (and its self-signature checked as
/// proof of possession). The subject, SANs, key usages, and validity are
/// chosen here: `CN=<node>`, `<node>.node.legion`, `legion://node/<node>`,
/// `ServerAuth` + `ClientAuth`, 24 hours. Whatever names the CSR requests
/// are ignored, so a node can never obtain a certificate that names another
/// node or a manager.
pub fn sign_node_csr(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    node_id: &str,
    csr_pem: &str,
) -> R<(String, i64)> {
    if node_id.trim().is_empty() {
        bail!("node id is empty");
    }
    let csr = CertificateSigningRequestParams::from_pem(csr_pem)
        .map_err(|err| anyhow!("invalid CSR (expected a PEM `CERTIFICATE REQUEST`): {err}"))?;
    let (ca_cert, ca_key) = issuer(ca_cert_pem, ca_key_pem)?;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, node_id.to_string());
    let mut params = CertificateParams::default();
    params.distinguished_name = dn;
    params.subject_alt_names = vec![
        SanType::DnsName(Ia5String::try_from(node_dns_name(node_id))?),
        SanType::URI(Ia5String::try_from(node_uri(node_id))?),
    ];
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - NOT_BEFORE_SKEW;
    let not_after = now + time::Duration::try_from(NODE_CERT_TTL)?;
    params.not_after = not_after;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params.signed_by(&csr.public_key, &ca_cert, &ca_key)?;
    Ok((cert.pem(), not_after.unix_timestamp()))
}

/// A manager's own client certificate: (certificate PEM, key PEM, expiry as
/// Unix seconds). Signed by the manager itself from the replicated CA key,
/// so nothing is distributed and the key never leaves this process.
/// `ClientAuth` only: a manager certificate cannot serve the worker plane or
/// a mesh port.
pub fn issue_manager_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    manager_id: &str,
) -> R<(String, String, i64)> {
    if manager_id.trim().is_empty() {
        bail!("manager id is empty");
    }
    let (ca_cert, ca_key) = issuer(ca_cert_pem, ca_key_pem)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, format!("legion-manager-{manager_id}"));
    let mut params = CertificateParams::default();
    params.distinguished_name = dn;
    params.subject_alt_names = vec![SanType::URI(Ia5String::try_from(manager_uri(manager_id))?)];
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - NOT_BEFORE_SKEW;
    let not_after = now + time::Duration::try_from(MANAGER_CERT_TTL)?;
    params.not_after = not_after;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, &ca_cert, &ca_key)?;
    Ok((cert.pem(), key.serialize_pem(), not_after.unix_timestamp()))
}

/// Test helper: a node certificate and its key, as a worker would hold them
/// after a CSR round trip: (certificate PEM, key PEM, expiry).
#[cfg(test)]
pub fn issue_node_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    node_id: &str,
) -> R<(String, String, i64)> {
    let (csr_pem, key_pem) = node_csr(node_id)?;
    let (cert_pem, expires_at) = sign_node_csr(ca_cert_pem, ca_key_pem, node_id, &csr_pem)?;
    Ok((cert_pem, key_pem, expires_at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::extensions::GeneralName;

    #[test]
    fn node_names_are_dns_safe() {
        assert_eq!(node_dns_name("Node_1.local"), "node-1-local.node.legion");
        assert_eq!(node_uri("n1"), "legion://node/n1");
        assert_eq!(manager_uri("m1"), "legion://manager/m1");
    }

    fn sans(cert_pem: &str) -> Vec<String> {
        let der = crate::worker::netd::tls::certs_from_pem(cert_pem.as_bytes())
            .unwrap()
            .remove(0);
        let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref()).unwrap();
        let mut out = Vec::new();
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            for name in &san.value.general_names {
                match name {
                    GeneralName::URI(uri) => out.push(uri.to_string()),
                    GeneralName::DNSName(dns) => out.push(dns.to_string()),
                    _ => {}
                }
            }
        }
        out
    }

    #[test]
    fn issued_node_cert_chains_to_the_ca() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        let (cert_pem, key_pem, expires) = issue_node_cert(&ca_pem, &ca_key, "node-1")?;
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("PRIVATE KEY"));
        assert!(expires > time::OffsetDateTime::now_utc().unix_timestamp());

        // Chain-validate with rustls: the CA is the only trust anchor.
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_pem.as_bytes()) {
            roots.add(cert?)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
            .build()
            .expect("verifier");
        let leaf = rustls_pemfile::certs(&mut cert_pem.as_bytes())
            .next()
            .expect("leaf")?;
        verifier
            .verify_client_cert(&leaf, &[], rustls_pki_types::UnixTime::now())
            .expect("node cert verifies against the cluster CA");
        Ok(())
    }

    /// A CSR that asks for a manager's SAN, or another node's name, still
    /// gets exactly the names the manager chose for the authenticated node.
    #[test]
    fn csr_signing_ignores_requested_names() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![
            SanType::URI(Ia5String::try_from("legion://manager/evil")?),
            SanType::DnsName(Ia5String::try_from("node-2.node.legion")?),
        ];
        let csr = params.serialize_request(&key)?.pem()?;
        let (cert_pem, _) = sign_node_csr(&ca_pem, &ca_key, "node-1", &csr)?;
        let names = sans(&cert_pem);
        assert_eq!(
            names,
            vec![
                "node-1.node.legion".to_string(),
                "legion://node/node-1".to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn signing_refuses_garbage_and_a_missing_csr() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        assert!(sign_node_csr(&ca_pem, &ca_key, "node-1", "").is_err());
        assert!(sign_node_csr(&ca_pem, &ca_key, "node-1", "not a csr").is_err());
        let (csr, _) = node_csr("node-1")?;
        assert!(sign_node_csr(&ca_pem, &ca_key, "", &csr).is_err());
        Ok(())
    }

    #[test]
    fn manager_cert_names_the_manager_and_is_client_only() -> R<()> {
        let (ca_pem, ca_key) = generate_ca()?;
        let (cert_pem, key_pem, expires) = issue_manager_cert(&ca_pem, &ca_key, "mgr-a")?;
        assert!(key_pem.contains("PRIVATE KEY"));
        assert!(expires > time::OffsetDateTime::now_utc().unix_timestamp());
        assert_eq!(sans(&cert_pem), vec!["legion://manager/mgr-a".to_string()]);
        let der = crate::worker::netd::tls::certs_from_pem(cert_pem.as_bytes())?.remove(0);
        let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref())?;
        let eku = cert.extended_key_usage()?.expect("EKU present").value;
        assert!(eku.client_auth);
        assert!(!eku.server_auth, "a manager certificate cannot serve");
        Ok(())
    }

    #[test]
    fn fingerprints_differ_per_ca_and_are_stable() -> R<()> {
        let (a, _) = generate_ca()?;
        let (b, _) = generate_ca()?;
        assert_eq!(ca_fingerprint(&a)?, ca_fingerprint(&a)?);
        assert_ne!(ca_fingerprint(&a)?, ca_fingerprint(&b)?);
        assert!(ca_fingerprint(&a)?.starts_with("sha256:"));
        Ok(())
    }
}
