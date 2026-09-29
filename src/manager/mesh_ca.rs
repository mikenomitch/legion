//! The mesh CA: the cluster-issued certificates behind `certFrom = "legion"`
//! (docs/network-handlers.md, "mTLS").
//!
//! One CA per cluster, generated on first use and replicated with the
//! control lane (`net_mesh_ca`). It signs **node** certificates — one per
//! worker node, short-lived, refreshed by the worker on its heartbeat loop —
//! and nothing else: a workload's identity rides *inside* the node-to-node
//! TLS session as a PROXY protocol v2 TLV the receiving node attributes,
//! never as a certificate of its own (service-mesh calls entry 2).
//!
//! A node certificate carries the node in two SANs: `<node>.node.legion`
//! (what a dialing node verifies) and `legion://node/<node id>` (what the
//! receiving side reads back to name the peer node).

use crate::prelude::*;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
    Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use std::time::Duration;

/// How long a node certificate is valid.
pub const NODE_CERT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Workers refresh once this much of the lifetime has elapsed, so a
/// manager outage of several hours never expires a node out of the mesh.
pub const NODE_CERT_REFRESH_AFTER: Duration = Duration::from_secs(8 * 60 * 60);
/// Backdate `notBefore` by this much so a worker whose clock lags the
/// manager's accepts its own certificate immediately.
const NOT_BEFORE_SKEW: time::Duration = time::Duration::minutes(5);
/// The CA lives for a decade; rotation is an operator action (delete the
/// row, every node re-issues on its next refresh).
const CA_TTL_DAYS: i64 = 365 * 10;

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
    format!("legion://node/{node_id}")
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

/// A node certificate signed by the CA: (certificate PEM, key PEM, expiry
/// as Unix seconds). The manager generates the key pair and hands both
/// halves over the authenticated worker channel; a CSR flow is more
/// ceremony than a 24-hour credential warrants.
pub fn issue_node_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    node_id: &str,
) -> R<(String, String, i64)> {
    let ca_key = KeyPair::from_pem(ca_key_pem).context("mesh CA key")?;
    let ca_cert = CertificateParams::from_ca_cert_pem(ca_cert_pem)
        .context("mesh CA certificate")?
        .self_signed(&ca_key)?;

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
    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, &ca_cert, &ca_key)?;
    Ok((cert.pem(), key.serialize_pem(), not_after.unix_timestamp()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_names_are_dns_safe() {
        assert_eq!(node_dns_name("Node_1.local"), "node-1-local.node.legion");
        assert_eq!(node_uri("n1"), "legion://node/n1");
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
            .expect("node cert verifies against the mesh CA");
        Ok(())
    }
}
