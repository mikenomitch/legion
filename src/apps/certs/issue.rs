//! Making and reading certificates: keys, private CAs, self-signed and
//! CA-signed leaves, CSRs for ACME, and the KV bundle format the node
//! datapath serves (`ports[].tls.cert = { kv }`): the chain, leaf first,
//! then the PKCS#8 private key.

use super::spec::{hex_string, Desired, KeyAlgorithm, Usage};
use crate::prelude::*;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::IpAddr;
use std::time::Duration;

/// A generated CA lasts this long.
pub const CA_LIFETIME: Duration = Duration::from_secs(10 * 365 * 86400);
/// Every certificate starts this far in the past, so a peer whose clock
/// runs a little behind still accepts it.
pub const BACKDATE: Duration = Duration::from_secs(3600);

/// What the app reads off a certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertInfo {
    /// Unix seconds.
    pub not_before: i64,
    pub not_after: i64,
    /// Lower-case hex.
    pub serial: String,
    pub dns_names: Vec<String>,
    pub ip_addresses: Vec<IpAddr>,
    /// `None` for a key the app does not issue (RSA, Ed25519).
    pub key_algorithm: Option<KeyAlgorithm>,
    pub subject: String,
    pub issuer: String,
    pub is_ca: bool,
    /// SHA-256 of the DER, lower-case hex.
    pub fingerprint: String,
}

impl CertInfo {
    /// Read a DER certificate.
    pub fn from_der(der: &[u8]) -> R<CertInfo> {
        use x509_parser::extensions::GeneralName;
        let (_, cert) = x509_parser::parse_x509_certificate(der)
            .map_err(|err| anyhow!("the certificate does not parse: {err}"))?;
        let mut dns_names = Vec::new();
        let mut ip_addresses = Vec::new();
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            for name in &san.value.general_names {
                match name {
                    GeneralName::DNSName(dns) => dns_names.push(dns.to_ascii_lowercase()),
                    GeneralName::IPAddress(bytes) => match bytes.len() {
                        4 => ip_addresses.push(IpAddr::from(
                            <[u8; 4]>::try_from(*bytes).expect("four bytes"),
                        )),
                        16 => ip_addresses.push(IpAddr::from(
                            <[u8; 16]>::try_from(*bytes).expect("sixteen bytes"),
                        )),
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
        // id-ecPublicKey, told apart by the point's length (uncompressed).
        let spki = cert.public_key();
        let key_algorithm = if spki.algorithm.algorithm.to_id_string() == "1.2.840.10045.2.1" {
            match spki.subject_public_key.data.len() {
                65 => Some(KeyAlgorithm::EcdsaP256),
                97 => Some(KeyAlgorithm::EcdsaP384),
                _ => None,
            }
        } else {
            None
        };
        use sha2::Digest;
        Ok(CertInfo {
            not_before: cert.validity().not_before.timestamp(),
            not_after: cert.validity().not_after.timestamp(),
            serial: hex_string(cert.raw_serial()),
            dns_names,
            ip_addresses,
            key_algorithm,
            subject: cert.subject().to_string(),
            issuer: cert.issuer().to_string(),
            is_ca: cert.is_ca(),
            fingerprint: hex_string(&sha2::Sha256::digest(der)),
        })
    }

    /// Whether it carries exactly the names `desired` asks for.
    pub fn names_match(&self, desired: &Desired) -> bool {
        let mut have = self.dns_names.clone();
        have.sort();
        have.dedup();
        let mut want = desired.dns_names.clone();
        want.sort();
        let mut have_ips = self.ip_addresses.clone();
        have_ips.sort();
        have_ips.dedup();
        let mut want_ips = desired.ip_addresses.clone();
        want_ips.sort();
        have == want && have_ips == want_ips
    }

    /// When to renew: `renew_before` ahead of expiry, or two thirds of the
    /// way through the certificate's life (cert-manager's default).
    pub fn renewal_time(&self, renew_before: Option<Duration>) -> i64 {
        let lifetime = (self.not_after - self.not_before).max(0);
        match renew_before {
            Some(before) => {
                let before = (before.as_secs() as i64).min(lifetime);
                self.not_after - before
            }
            None => self.not_before + lifetime * 2 / 3,
        }
    }
}

/// A certificate bundle as stored in KV, parsed and checked the way the
/// datapath checks it before serving it.
#[derive(Debug, Clone)]
pub struct Bundle {
    /// The chain, leaf first, as PEM.
    pub chain_pem: String,
    /// The private key, PKCS#8 PEM.
    pub key_pem: String,
    /// The leaf.
    pub leaf: CertInfo,
}

impl Bundle {
    /// Parse a stored bundle. Errors say what is wrong with it.
    pub fn parse(pem: &str) -> R<Bundle> {
        crate::worker::netd::tls::validate_bundle(pem)?;
        let chain = crate::worker::netd::tls::certs_from_pem(pem.as_bytes())?;
        let leaf = CertInfo::from_der(chain[0].as_ref())?;
        let chain_pem = chain.iter().map(|der| der_to_pem(der.as_ref())).collect();
        let key_pem = private_key_pem(pem).context("the bundle holds no PKCS#8 private key")?;
        Ok(Bundle {
            chain_pem,
            key_pem,
            leaf,
        })
    }

    /// The value written to KV: the chain, then the key.
    pub fn to_pem(&self) -> String {
        format!("{}{}", self.chain_pem, self.key_pem)
    }

    /// The certificates after the leaf, as PEM: the issuing CA chain. For a
    /// self-signed certificate, the leaf itself.
    pub fn ca_pem(&self) -> String {
        let chain =
            crate::worker::netd::tls::certs_from_pem(self.chain_pem.as_bytes()).unwrap_or_default();
        if chain.len() <= 1 {
            return self.chain_pem.clone();
        }
        chain[1..]
            .iter()
            .map(|der| der_to_pem(der.as_ref()))
            .collect()
    }
}

/// One DER certificate as PEM.
pub fn der_to_pem(der: &[u8]) -> String {
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in body.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(line).expect("base64 is ascii"));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

/// The first `PRIVATE KEY` block of `pem`, as written.
fn private_key_pem(pem: &str) -> Option<String> {
    let begin = pem.find("-----BEGIN PRIVATE KEY-----")?;
    let end_marker = "-----END PRIVATE KEY-----";
    let end = pem[begin..].find(end_marker)? + begin + end_marker.len();
    Some(format!("{}\n", &pem[begin..end]))
}

/// A fresh private key.
pub fn generate_key(algorithm: KeyAlgorithm) -> R<KeyPair> {
    KeyPair::generate_for(algorithm.signature_algorithm())
        .map_err(|err| anyhow!("generating an {} key: {err}", algorithm.as_str()))
}

fn random_serial() -> SerialNumber {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    // Positive, and never a leading zero byte.
    bytes[0] = (bytes[0] & 0x7f) | 0x40;
    SerialNumber::from_slice(&bytes)
}

fn timestamp(unix: i64) -> R<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp(unix).map_err(|err| anyhow!("time {unix}: {err}"))
}

/// The parameters of a leaf for `desired`, valid from `now`.
fn leaf_params(desired: &Desired, now: i64) -> R<CertificateParams> {
    let mut params = CertificateParams::default();
    let mut sans = Vec::new();
    for name in &desired.dns_names {
        sans.push(SanType::DnsName(
            name.clone()
                .try_into()
                .map_err(|err| anyhow!("{name}: {err}"))?,
        ));
    }
    for ip in &desired.ip_addresses {
        sans.push(SanType::IpAddress(*ip));
    }
    params.subject_alt_names = sans;
    let mut dn = DistinguishedName::new();
    let cn = desired
        .common_name
        .clone()
        .or_else(|| desired.names().into_iter().next())
        .unwrap_or_default();
    dn.push(DnType::CommonName, cn);
    params.distinguished_name = dn;
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = desired
        .usages
        .iter()
        .map(|usage| match usage {
            Usage::Server => ExtendedKeyUsagePurpose::ServerAuth,
            Usage::Client => ExtendedKeyUsagePurpose::ClientAuth,
        })
        .collect();
    params.not_before = timestamp(now - BACKDATE.as_secs() as i64)?;
    params.not_after = timestamp(now + desired.duration.as_secs() as i64)?;
    params.serial_number = Some(random_serial());
    params.use_authority_key_identifier_extension = true;
    Ok(params)
}

/// A self-signed certificate for `desired`.
pub fn self_signed(desired: &Desired, now: i64) -> R<Bundle> {
    let key = generate_key(desired.key_algorithm)?;
    let mut params = leaf_params(desired, now)?;
    params.use_authority_key_identifier_extension = false;
    let cert = params
        .self_signed(&key)
        .map_err(|err| anyhow!("self-signing: {err}"))?;
    Bundle::parse(&format!("{}{}", cert.pem(), key.serialize_pem()))
}

/// A private CA, loaded from its KV bundle.
#[derive(Debug, Clone)]
pub struct CaMaterial {
    /// The CA's own certificate.
    pub cert_pem: String,
    /// The CA's certificate, then whatever intermediates followed it in KV
    /// (toward a root): what goes after a leaf it signs.
    pub chain_pem: String,
    key_pem: String,
    pub info: CertInfo,
}

impl CaMaterial {
    /// Load a CA from its bundle (certificate, intermediates, PKCS#8 key),
    /// and prove it can sign: a probe leaf is signed and verified against
    /// the CA's certificate, as a client would.
    pub fn load(pem: &str) -> R<CaMaterial> {
        let chain = crate::worker::netd::tls::certs_from_pem(pem.as_bytes())?;
        let Some(first) = chain.first() else {
            bail!("holds no certificate (expected the CA certificate, then its private key)");
        };
        let info = CertInfo::from_der(first.as_ref())?;
        if !info.is_ca {
            bail!("its first certificate ({}) is not a CA", info.subject);
        }
        let key_pem = private_key_pem(pem).ok_or_else(|| {
            anyhow!(
                "holds no PKCS#8 private key (\"BEGIN PRIVATE KEY\"; convert one with `openssl \
                 pkcs8 -topk8 -nocrypt`)"
            )
        })?;
        let key = KeyPair::from_pem(&key_pem).map_err(|err| anyhow!("the private key: {err}"))?;
        let (_, parsed) = x509_parser::parse_x509_certificate(first.as_ref())
            .map_err(|err| anyhow!("the CA certificate: {err}"))?;
        if parsed.public_key().raw != key.public_key_der().as_slice() {
            bail!("the private key does not belong to the CA certificate");
        }
        let material = CaMaterial {
            cert_pem: der_to_pem(first.as_ref()),
            chain_pem: chain.iter().map(|der| der_to_pem(der.as_ref())).collect(),
            key_pem,
            info,
        };
        material.self_test()?;
        Ok(material)
    }

    /// Generate a CA named after `issuer`: ECDSA P-256, ten years, able to
    /// sign leaves only. Returns its KV bundle and the loaded material.
    pub fn generate(issuer: &str, now: i64) -> R<(String, CaMaterial)> {
        let key = generate_key(KeyAlgorithm::EcdsaP256)?;
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::OrganizationName, "Legion");
        dn.push(DnType::CommonName, format!("{issuer} CA"));
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        params.not_before = timestamp(now - BACKDATE.as_secs() as i64)?;
        params.not_after = timestamp(now + CA_LIFETIME.as_secs() as i64)?;
        params.serial_number = Some(random_serial());
        let cert = params
            .self_signed(&key)
            .map_err(|err| anyhow!("generating the CA: {err}"))?;
        let bundle = format!("{}{}", cert.pem(), key.serialize_pem());
        let material = CaMaterial::load(&bundle)?;
        Ok((bundle, material))
    }

    /// Sign a leaf for `desired`. Its lifetime never outlasts the CA.
    pub fn sign(&self, desired: &Desired, now: i64) -> R<Bundle> {
        let leaf_key = generate_key(desired.key_algorithm)?;
        let mut params = leaf_params(desired, now)?;
        let ca_not_after = timestamp(self.info.not_after)?;
        if params.not_after > ca_not_after {
            params.not_after = ca_not_after;
        }
        let (issuer, issuer_key) = self.issuer()?;
        let cert = params
            .signed_by(&leaf_key, &issuer, &issuer_key)
            .map_err(|err| anyhow!("signing: {err}"))?;
        Bundle::parse(&format!(
            "{}{}{}",
            cert.pem(),
            self.chain_pem,
            leaf_key.serialize_pem()
        ))
    }

    fn issuer(&self) -> R<(rcgen::Certificate, KeyPair)> {
        let key = KeyPair::from_pem(&self.key_pem).map_err(|err| anyhow!("CA key: {err}"))?;
        let issuer = CertificateParams::from_ca_cert_pem(&self.cert_pem)
            .map_err(|err| anyhow!("CA certificate: {err}"))?
            .self_signed(&key)
            .map_err(|err| anyhow!("CA certificate: {err}"))?;
        Ok((issuer, key))
    }

    /// Sign a probe and verify it against the CA certificate: catches a CA
    /// whose name constraints, key usage, or subject encoding would make
    /// every certificate it signs fail at clients.
    fn self_test(&self) -> R<()> {
        let now = UnixTime::now().as_secs() as i64;
        let probe = super::spec::CertificateSpec {
            issuer: "probe".into(),
            kv: "probe".into(),
            dns_names: vec!["probe.certs.legion.invalid".into()],
            ip_addresses: vec![],
            common_name: None,
            duration: "1h".into(),
            renew_before: None,
            key_algorithm: KeyAlgorithm::EcdsaP256,
            usages: vec![Usage::Server],
            parts: None,
        }
        .desired()
        .map_err(|err| anyhow!("{err}"))?;
        let mut params = leaf_params(&probe, now)?;
        params.not_before = timestamp(now - 60)?;
        params.not_after = timestamp((now + 3600).min(self.info.not_after))?;
        let leaf_key = generate_key(KeyAlgorithm::EcdsaP256)?;
        let (issuer, issuer_key) = self.issuer()?;
        let cert = params
            .signed_by(&leaf_key, &issuer, &issuer_key)
            .map_err(|err| anyhow!("signing a probe: {err}"))?;
        verify_chain(
            &format!("{}{}", cert.pem(), self.chain_pem),
            &self.cert_pem,
            "probe.certs.legion.invalid",
            Usage::Server,
        )
        .context("a certificate this CA signs does not verify against it")
    }
}

/// Verify `chain_pem` (leaf first) for `name` and `usage` against the CA
/// certificates in `roots_pem`, as a client (server usage) or a server
/// (client usage) would, now.
pub fn verify_chain(chain_pem: &str, roots_pem: &str, name: &str, usage: Usage) -> R<()> {
    let chain = crate::worker::netd::tls::certs_from_pem(chain_pem.as_bytes())?;
    let Some((leaf, intermediates)) = chain.split_first() else {
        bail!("no certificate to verify");
    };
    let roots = crate::worker::netd::tls::roots_from_pem(roots_pem.as_bytes())?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // A CA certificate inside the chain is a trust anchor here, not an
    // intermediate.
    let intermediates: Vec<CertificateDer<'static>> = intermediates.to_vec();
    match usage {
        Usage::Server => {
            let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()?;
            let server_name =
                ServerName::try_from(name.to_string()).map_err(|err| anyhow!("{name}: {err}"))?;
            rustls::client::danger::ServerCertVerifier::verify_server_cert(
                verifier.as_ref(),
                leaf,
                &intermediates,
                &server_name,
                &[],
                UnixTime::now(),
            )
            .map_err(|err| anyhow!("{err}"))?;
        }
        Usage::Client => {
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()?;
            rustls::server::danger::ClientCertVerifier::verify_client_cert(
                verifier.as_ref(),
                leaf,
                &intermediates,
                UnixTime::now(),
            )
            .map_err(|err| anyhow!("{err}"))?;
        }
    }
    Ok(())
}

/// A key and a CSR for `desired`, for an ACME CA: the CA decides the
/// lifetime and usages, so the CSR names only the key and the names.
pub fn csr(desired: &Desired) -> R<(KeyPair, Vec<u8>)> {
    let key = generate_key(desired.key_algorithm)?;
    let mut params = CertificateParams::default();
    let mut sans = Vec::new();
    for name in &desired.dns_names {
        sans.push(SanType::DnsName(
            name.clone()
                .try_into()
                .map_err(|err| anyhow!("{name}: {err}"))?,
        ));
    }
    for ip in &desired.ip_addresses {
        sans.push(SanType::IpAddress(*ip));
    }
    params.subject_alt_names = sans;
    let mut dn = DistinguishedName::new();
    if let Some(cn) = desired
        .common_name
        .clone()
        .or_else(|| desired.dns_names.first().cloned())
    {
        dn.push(DnType::CommonName, cn);
    }
    params.distinguished_name = dn;
    let request = params
        .serialize_request(&key)
        .map_err(|err| anyhow!("building the CSR: {err}"))?;
    Ok((key, request.der().to_vec()))
}

/// The bundle an ACME CA's chain makes with the key behind its CSR.
pub fn acme_bundle(chain_pem: &str, key: &KeyPair) -> R<Bundle> {
    Bundle::parse(&format!(
        "{}{}",
        normalize_pem(chain_pem),
        key.serialize_pem()
    ))
}

/// PEM with `\r\n` turned into `\n` and a trailing newline.
fn normalize_pem(pem: &str) -> String {
    let mut text = pem.replace("\r\n", "\n");
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::certs::spec::CertificateSpec;

    fn desired(spec: Value) -> Desired {
        serde_json::from_value::<CertificateSpec>(spec)
            .unwrap()
            .desired()
            .unwrap()
    }

    fn now() -> i64 {
        UnixTime::now().as_secs() as i64
    }

    #[test]
    fn a_generated_ca_signs_leaves_clients_and_servers_accept_test() -> R<()> {
        let (bundle, ca) = CaMaterial::generate("internal", now())?;
        assert!(ca.info.is_ca);
        assert!(
            ca.info.subject.contains("internal CA"),
            "{}",
            ca.info.subject
        );
        // The KV bundle round-trips.
        let reloaded = CaMaterial::load(&bundle)?;
        assert_eq!(reloaded.info.fingerprint, ca.info.fingerprint);

        let want = desired(json!({
            "issuer": "internal", "kv": "certs/db",
            "dnsNames": ["db.internal", "*.db.internal"], "ipAddresses": ["10.0.0.7"],
        }));
        let leaf = ca.sign(&want, now())?;
        // What the node datapath would accept from KV.
        crate::worker::netd::tls::validate_bundle(&leaf.to_pem())?;
        assert!(leaf.leaf.names_match(&want));
        assert!(!leaf.leaf.is_ca);
        assert_eq!(leaf.leaf.key_algorithm, Some(KeyAlgorithm::EcdsaP256));
        assert_eq!(leaf.ca_pem(), ca.cert_pem);
        verify_chain(&leaf.chain_pem, &ca.cert_pem, "db.internal", Usage::Server)?;
        verify_chain(
            &leaf.chain_pem,
            &ca.cert_pem,
            "x.db.internal",
            Usage::Server,
        )?;
        verify_chain(&leaf.chain_pem, &ca.cert_pem, "10.0.0.7", Usage::Server)?;
        verify_chain(&leaf.chain_pem, &ca.cert_pem, "db.internal", Usage::Client)?;
        assert!(verify_chain(
            &leaf.chain_pem,
            &ca.cert_pem,
            "other.internal",
            Usage::Server
        )
        .is_err());
        // Ninety days, from an hour ago.
        assert_eq!(
            leaf.leaf.not_after - leaf.leaf.not_before,
            90 * 86400 + BACKDATE.as_secs() as i64
        );
        // Another CA does not vouch for it.
        let (_, other) = CaMaterial::generate("other", now())?;
        assert!(verify_chain(
            &leaf.chain_pem,
            &other.cert_pem,
            "db.internal",
            Usage::Server
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn usages_and_key_algorithms_are_honored_test() -> R<()> {
        let (_, ca) = CaMaterial::generate("internal", now())?;
        let server_only = ca.sign(
            &desired(json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.test"],
                             "usages": ["server"], "keyAlgorithm": "ecdsa-p384" })),
            now(),
        )?;
        assert_eq!(
            server_only.leaf.key_algorithm,
            Some(KeyAlgorithm::EcdsaP384)
        );
        verify_chain(
            &server_only.chain_pem,
            &ca.cert_pem,
            "a.test",
            Usage::Server,
        )?;
        assert!(verify_chain(
            &server_only.chain_pem,
            &ca.cert_pem,
            "a.test",
            Usage::Client
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn a_leaf_never_outlives_its_ca_test() -> R<()> {
        let (_, ca) = CaMaterial::generate("internal", now())?;
        let long = ca.sign(
            &desired(
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.test"], "duration": "87600h" }),
            ),
            now() + 9 * 365 * 86400,
        )?;
        assert_eq!(long.leaf.not_after, ca.info.not_after);
        Ok(())
    }

    #[test]
    fn a_ca_bundle_must_be_a_ca_with_its_own_key_test() -> R<()> {
        let (bundle, _) = CaMaterial::generate("a", now())?;
        let (other_bundle, _) = CaMaterial::generate("b", now())?;
        let leaf = self_signed(
            &desired(json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.test"] })),
            now(),
        )?;
        let err = CaMaterial::load(&leaf.to_pem()).unwrap_err().to_string();
        assert!(err.contains("not a CA"), "{err}");
        // A's certificate with B's key.
        let cert_a = &bundle[..bundle.find("-----BEGIN PRIVATE KEY").unwrap()];
        let key_b = &other_bundle[other_bundle.find("-----BEGIN PRIVATE KEY").unwrap()..];
        let err = CaMaterial::load(&format!("{cert_a}{key_b}"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not belong"), "{err}");
        let err = CaMaterial::load(cert_a).unwrap_err().to_string();
        assert!(err.contains("no PKCS#8 private key"), "{err}");
        Ok(())
    }

    /// A CA brought from outside (an intermediate under the operator's own
    /// root) signs leaves that chain to the root.
    #[test]
    fn an_intermediate_from_outside_chains_to_its_root_test() -> R<()> {
        let root_key = generate_key(KeyAlgorithm::EcdsaP256)?;
        let mut root_params = CertificateParams::default();
        root_params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, "Acme Root");
            dn
        };
        root_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        root_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let root = root_params.self_signed(&root_key)?;
        let inter_key = generate_key(KeyAlgorithm::EcdsaP256)?;
        let mut inter_params = CertificateParams::default();
        inter_params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::OrganizationName, "Acme");
            dn.push(DnType::CommonName, "Acme Legion Intermediate");
            dn
        };
        inter_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        inter_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        inter_params.use_authority_key_identifier_extension = true;
        let inter = inter_params.signed_by(&inter_key, &root, &root_key)?;

        let ca = CaMaterial::load(&format!(
            "{}{}{}",
            inter.pem(),
            root.pem(),
            inter_key.serialize_pem()
        ))?;
        let leaf = ca.sign(
            &desired(json!({ "issuer": "i", "kv": "k", "dnsNames": ["api.acme.test"] })),
            now(),
        )?;
        verify_chain(&leaf.chain_pem, &root.pem(), "api.acme.test", Usage::Server)?;
        Ok(())
    }

    #[test]
    fn self_signed_certificates_are_their_own_ca_test() -> R<()> {
        let want = desired(json!({ "issuer": "self", "kv": "k", "dnsNames": ["dev.test"] }));
        let bundle = self_signed(&want, now())?;
        assert_eq!(bundle.ca_pem(), bundle.chain_pem);
        assert_eq!(bundle.leaf.subject, bundle.leaf.issuer);
        Ok(())
    }

    #[test]
    fn bundles_round_trip_and_name_their_problems_test() -> R<()> {
        let want = desired(json!({ "issuer": "self", "kv": "k", "dnsNames": ["dev.test"] }));
        let bundle = self_signed(&want, now())?;
        let parsed = Bundle::parse(&bundle.to_pem())?;
        assert_eq!(parsed.leaf, bundle.leaf);
        assert!(Bundle::parse(&bundle.chain_pem)
            .unwrap_err()
            .to_string()
            .contains("private key"));
        assert!(Bundle::parse("hello").is_err());
        // Another certificate's key.
        let other = self_signed(&want, now())?;
        assert!(Bundle::parse(&format!("{}{}", bundle.chain_pem, other.key_pem)).is_err());
        Ok(())
    }

    #[test]
    fn renewal_is_two_thirds_through_unless_renew_before_says_test() {
        let info = CertInfo {
            not_before: 0,
            not_after: 90 * 86400,
            serial: String::new(),
            dns_names: vec![],
            ip_addresses: vec![],
            key_algorithm: None,
            subject: String::new(),
            issuer: String::new(),
            is_ca: false,
            fingerprint: String::new(),
        };
        assert_eq!(info.renewal_time(None), 60 * 86400);
        assert_eq!(
            info.renewal_time(Some(Duration::from_secs(10 * 86400))),
            80 * 86400
        );
        // Never before it was issued.
        assert_eq!(info.renewal_time(Some(Duration::from_secs(900 * 86400))), 0);
    }

    #[test]
    fn a_csr_names_what_the_spec_asks_for_test() -> R<()> {
        let want = desired(json!({ "issuer": "le", "kv": "k",
                                   "dnsNames": ["a.example.com", "*.example.com"] }));
        let (_, der) = csr(&want)?;
        let params = rcgen::CertificateSigningRequestParams::from_der(&der.into())?;
        assert_eq!(params.params.subject_alt_names.len(), 2);
        Ok(())
    }
}
