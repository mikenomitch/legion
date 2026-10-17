//! Checking what a cluster CA provider hands Legion (docs/cluster-ca.md,
//! "What Legion checks").
//!
//! Two jobs:
//!
//! - [`assemble_view`] turns a provider's signing chain and trust bundle
//!   into a [`CaView`]: it finds the **anchor** (the bundle certificate the
//!   signing CA chains to, by signature, not only by name), orders the
//!   bundle anchor-first, and works out which certificates a leaf must carry
//!   after itself.
//! - [`verify_issued`] checks a returned leaf against the [`LeafSpec`]
//!   Legion asked for before anything is handed out. Each mismatch is its
//!   own [`Refusal`], so the error names exactly what was wrong.

use super::{CaView, Issued, MAX_LIFETIME_SLACK, MAX_NOT_BEFORE_SKEW, MIN_LEAF_LIFETIME};
use crate::cluster_tls::{ca_pin, short_pin};
use crate::manager::mesh_ca::LeafSpec;
use crate::storage::netpolicy_store::CaRotationPhase;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::collections::BTreeSet;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;

/// Why Legion will not hand out a certificate a provider returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The reply held no parseable certificate.
    NoCertificate(String),
    /// The leaf is a CA, or may sign certificates.
    IsCa(String),
    /// The leaf's public key is not the CSR's.
    WrongKey,
    /// The DNS SANs are not exactly the requested set.
    DnsSans { got: Vec<String>, want: Vec<String> },
    /// The URI SANs are not exactly the requested set.
    UriSans { got: Vec<String>, want: Vec<String> },
    /// The leaf carries a SAN type Legion never asks for.
    OtherSans(Vec<String>),
    /// The extended key usages are missing, incomplete, or too broad.
    Usage(String),
    /// `notBefore` is too far in the future.
    NotYetValid { not_before: String },
    /// Valid for longer than requested (plus slack).
    TooLong { lifetime: String, requested: String },
    /// Valid for less than [`MIN_LEAF_LIFETIME`].
    TooShort { lifetime: String },
    /// The chain does not verify against the trust bundle.
    Chain(String),
}

impl Refusal {
    /// A chain failure is the one refusal a fresh view of the CA could
    /// cure (an issuer rotated in the external CA).
    pub fn is_chain_failure(&self) -> bool {
        matches!(self, Refusal::Chain(_))
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::NoCertificate(detail) => {
                write!(f, "the reply holds no certificate ({detail})")
            }
            Refusal::IsCa(detail) => write!(
                f,
                "the certificate is a CA certificate or may sign certificates ({detail}); a \
                 holder could mint any identity with it"
            ),
            Refusal::WrongKey => f.write_str(
                "the certificate's public key is not the one in the certificate signing request",
            ),
            Refusal::DnsSans { got, want } => write!(
                f,
                "its DNS SANs are [{}], but Legion asked for exactly [{}]",
                got.join(", "),
                want.join(", ")
            ),
            Refusal::UriSans { got, want } => write!(
                f,
                "its URI SANs are [{}], but Legion asked for exactly [{}]",
                got.join(", "),
                want.join(", ")
            ),
            Refusal::OtherSans(names) => write!(
                f,
                "it carries subject alternative names Legion never asks for: [{}]",
                names.join(", ")
            ),
            Refusal::Usage(detail) => write!(f, "{detail}"),
            Refusal::NotYetValid { not_before } => write!(
                f,
                "it is not valid until {not_before}, more than {} from now (check the CA's \
                 clock)",
                humantime::format_duration(MAX_NOT_BEFORE_SKEW)
            ),
            Refusal::TooLong {
                lifetime,
                requested,
            } => write!(
                f,
                "it is valid for {lifetime}, but Legion asked for {requested} (at most {} \
                 longer is accepted)",
                humantime::format_duration(MAX_LIFETIME_SLACK)
            ),
            Refusal::TooShort { lifetime } => write!(
                f,
                "it is valid for only {lifetime}; Legion needs at least {} (it asks for 24h). \
                 Raise the CA's maximum TTL for Legion's role",
                humantime::format_duration(MIN_LEAF_LIFETIME)
            ),
            Refusal::Chain(detail) => write!(
                f,
                "it does not chain to the cluster's trust bundle ({detail})"
            ),
        }
    }
}

/// PEM for one DER certificate.
pub fn der_to_pem(der: &[u8]) -> String {
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in body.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(line).unwrap_or_default());
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

fn parse(der: &[u8]) -> Result<X509Certificate<'_>, String> {
    x509_parser::parse_x509_certificate(der)
        .map(|(_, cert)| cert)
        .map_err(|err| format!("unparseable certificate: {err}"))
}

fn subject_name(cert: &X509Certificate<'_>) -> String {
    cert.subject().to_string()
}

fn rfc3339(unix: i64) -> String {
    OffsetDateTime::from_unix_timestamp(unix)
        .ok()
        .and_then(|at| at.format(&Rfc3339).ok())
        .unwrap_or_else(|| unix.to_string())
}

fn humanize(seconds: i64) -> String {
    humantime::format_duration(std::time::Duration::from_secs(seconds.max(0) as u64)).to_string()
}

/// Whether `cert` may act as a CA: `basicConstraints CA:TRUE`, and a key
/// usage (when present) that allows signing certificates.
fn check_is_ca(cert: &X509Certificate<'_>) -> Result<(), String> {
    let ca = cert
        .basic_constraints()
        .map_err(|err| format!("invalid basicConstraints: {err}"))?
        .map(|ext| ext.value.ca)
        .unwrap_or(false);
    if !ca {
        return Err(format!(
            "{} is not a CA certificate (no basicConstraints CA:TRUE)",
            subject_name(cert)
        ));
    }
    if let Some(usage) = cert
        .key_usage()
        .map_err(|err| format!("invalid keyUsage: {err}"))?
    {
        if !usage.value.key_cert_sign() {
            return Err(format!(
                "{}'s key usage does not allow signing certificates (keyCertSign)",
                subject_name(cert)
            ));
        }
    }
    Ok(())
}

/// Whether `issuer` signed `cert`: names match and the signature verifies
/// under the issuer's key.
fn issued_by(cert: &X509Certificate<'_>, issuer: &X509Certificate<'_>) -> bool {
    cert.issuer().as_raw() == issuer.subject().as_raw()
        && cert.verify_signature(Some(issuer.public_key())).is_ok()
}

/// A provider's signing chain (the signing CA first, then intermediates
/// toward the bundle) and trust bundle as a [`CaView`]. Errors name what is
/// wrong, for the provider to prefix.
pub fn assemble_view(
    provider: &'static str,
    location: String,
    chain_pem: &str,
    bundle_pem: &str,
) -> Result<CaView, String> {
    let chain = crate::cluster_tls::certs_from_pem(chain_pem)
        .map_err(|err| format!("the signing CA chain is not PEM certificates: {err:#}"))?;
    let bundle = crate::cluster_tls::certs_from_pem(bundle_pem)
        .map_err(|err| format!("the trust bundle is not PEM certificates: {err:#}"))?;
    if chain.is_empty() {
        return Err("the signing CA chain holds no certificate".into());
    }
    if bundle.is_empty() {
        return Err("the trust bundle holds no certificate".into());
    }
    let signing = parse(chain[0].as_ref())?;
    check_is_ca(&signing)?;
    let now = OffsetDateTime::now_utc().unix_timestamp();
    if signing.validity().not_after.timestamp() <= now {
        return Err(format!(
            "the signing CA {} expired at {}",
            subject_name(&signing),
            rfc3339(signing.validity().not_after.timestamp())
        ));
    }

    // The bundle, de-duplicated, in the order given.
    let mut bundle_ders: Vec<CertificateDer<'static>> = Vec::new();
    let mut seen = BTreeSet::new();
    for cert in bundle {
        if seen.insert(ca_pin(cert.as_ref())) {
            parse(cert.as_ref())?;
            bundle_ders.push(cert);
        }
    }
    let bundle_pins: Vec<String> = bundle_ders.iter().map(|c| ca_pin(c.as_ref())).collect();

    // Walk the chain until a certificate is in the bundle; otherwise find
    // the bundle certificate that signed the chain's last certificate.
    let mut anchor_index = None;
    let mut suffix: Vec<CertificateDer<'static>> = Vec::new();
    for cert in &chain {
        let pin = ca_pin(cert.as_ref());
        if let Some(index) = bundle_pins.iter().position(|candidate| *candidate == pin) {
            anchor_index = Some(index);
            break;
        }
        suffix.push(cert.clone());
    }
    if anchor_index.is_none() {
        let last_der = chain.last().expect("chain is not empty");
        let last = parse(last_der.as_ref())?;
        for (index, candidate) in bundle_ders.iter().enumerate() {
            let candidate = parse(candidate.as_ref())?;
            if issued_by(&last, &candidate) {
                anchor_index = Some(index);
                break;
            }
        }
    }
    let Some(anchor_index) = anchor_index else {
        return Err(format!(
            "the signing CA {} does not chain to any certificate in the trust bundle (bundle: {})",
            subject_name(&signing),
            bundle_pins
                .iter()
                .map(|pin| short_pin(pin))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    };

    let mut ordered = vec![bundle_ders[anchor_index].clone()];
    ordered.extend(
        bundle_ders
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != anchor_index)
            .map(|(_, cert)| cert.clone()),
    );
    let ordered_pins: Vec<String> = ordered.iter().map(|c| ca_pin(c.as_ref())).collect();
    Ok(CaView {
        provider,
        location,
        signing_cert_pem: der_to_pem(chain[0].as_ref()),
        bundle_pem: ordered.iter().map(|c| der_to_pem(c.as_ref())).collect(),
        chain_pem: suffix.iter().map(|c| der_to_pem(c.as_ref())).collect(),
        pin: ordered_pins[0].clone(),
        bundle_pins: ordered_pins,
        created_at: rfc3339(signing.validity().not_before.timestamp()),
        phase: CaRotationPhase::Stable,
        previous_cert_pem: None,
    })
}

/// Parse a CSR and prove its signature (possession of the key): the one
/// thing a caller's request contributes.
pub fn check_csr(csr_pem: &str) -> Result<(), String> {
    if csr_pem.trim().is_empty() {
        return Err("invalid certificate signing request: empty".into());
    }
    rcgen::CertificateSigningRequestParams::from_pem(csr_pem)
        .map(|_| ())
        .map_err(|err| format!("invalid certificate signing request: {err}"))
}

fn csr_spki(csr_pem: &str) -> Result<Vec<u8>, String> {
    use x509_parser::prelude::FromDer;
    let der = rustls_pemfile::csr(&mut csr_pem.as_bytes())
        .map_err(|err| format!("invalid certificate signing request: {err}"))?
        .ok_or_else(|| "invalid certificate signing request: no CSR in PEM".to_string())?;
    let (_, csr) =
        x509_parser::certification_request::X509CertificationRequest::from_der(der.as_ref())
            .map_err(|err| format!("invalid certificate signing request: {err}"))?;
    Ok(csr.certification_request_info.subject_pki.raw.to_vec())
}

/// The leaf and the chain to append: the provider's own intermediates when
/// it returned any (up to the first certificate in the bundle, so a root a
/// CA appends is not carried by every leaf), else the view's chain.
fn split_returned(
    returned_pem: &str,
    view: &CaView,
) -> Result<(CertificateDer<'static>, Vec<CertificateDer<'static>>), Refusal> {
    let mut certs = crate::cluster_tls::certs_from_pem(returned_pem)
        .map_err(|err| Refusal::NoCertificate(format!("{err:#}")))?;
    if certs.is_empty() {
        return Err(Refusal::NoCertificate(
            "no BEGIN CERTIFICATE block".to_string(),
        ));
    }
    let leaf = certs.remove(0);
    let returned_chain: Vec<CertificateDer<'static>> = certs
        .into_iter()
        .take_while(|cert| !view.bundle_pins.contains(&ca_pin(cert.as_ref())))
        .collect();
    let chain = if returned_chain.is_empty() {
        crate::cluster_tls::certs_from_pem(&view.chain_pem).unwrap_or_default()
    } else {
        returned_chain
    };
    Ok((leaf, chain))
}

/// What the built-in signer returned, without the checks: it built the
/// certificate from the spec itself.
pub fn assemble_trusted(returned_pem: &str, view: &CaView) -> Result<Issued, String> {
    let (leaf, chain) = split_returned(returned_pem, view).map_err(|err| err.to_string())?;
    let cert = parse(leaf.as_ref())?;
    Ok(Issued {
        leaf_pem: der_to_pem(leaf.as_ref()),
        chain_pem: chain.iter().map(|c| der_to_pem(c.as_ref())).collect(),
        bundle_pem: view.bundle_pem.clone(),
        expires_at: cert.validity().not_after.timestamp(),
        not_before: cert.validity().not_before.timestamp(),
    })
}

fn sorted_lower(names: &[String]) -> Vec<String> {
    let mut names: Vec<String> = names.iter().map(|n| n.to_ascii_lowercase()).collect();
    names.sort();
    names
}

/// Check a certificate a provider returned for `spec` before handing it
/// out. See the module docs for the list; each failure is one [`Refusal`].
pub fn verify_issued(
    spec: &LeafSpec,
    csr_pem: &str,
    returned_pem: &str,
    view: &CaView,
    now: OffsetDateTime,
) -> Result<Issued, Refusal> {
    let (leaf_der, chain) = split_returned(returned_pem, view)?;
    let leaf = parse(leaf_der.as_ref()).map_err(Refusal::NoCertificate)?;

    // Not a CA, and cannot sign certificates.
    let is_ca = leaf
        .basic_constraints()
        .map_err(|err| Refusal::IsCa(format!("invalid basicConstraints: {err}")))?
        .is_some_and(|ext| ext.value.ca);
    if is_ca {
        return Err(Refusal::IsCa("basicConstraints CA:TRUE".into()));
    }
    if let Some(usage) = leaf
        .key_usage()
        .map_err(|err| Refusal::IsCa(format!("invalid keyUsage: {err}")))?
    {
        if usage.value.key_cert_sign() || usage.value.crl_sign() {
            return Err(Refusal::IsCa("keyUsage keyCertSign or cRLSign".into()));
        }
        if !usage.value.digital_signature() {
            return Err(Refusal::Usage(
                "its key usage lacks digitalSignature, which TLS needs".into(),
            ));
        }
    }

    // The CSR's key.
    let want_key = csr_spki(csr_pem).map_err(Refusal::NoCertificate)?;
    if leaf.tbs_certificate.subject_pki.raw != want_key.as_slice() {
        return Err(Refusal::WrongKey);
    }

    // Exactly the requested names.
    let mut dns = Vec::new();
    let mut uris = Vec::new();
    let mut other = Vec::new();
    if let Some(san) = leaf
        .subject_alternative_name()
        .map_err(|err| Refusal::OtherSans(vec![format!("invalid SAN extension: {err}")]))?
    {
        for name in &san.value.general_names {
            match name {
                GeneralName::DNSName(value) => dns.push(value.to_string()),
                GeneralName::URI(value) => uris.push(value.to_string()),
                other_name => other.push(format!("{other_name:?}")),
            }
        }
    }
    if !other.is_empty() {
        return Err(Refusal::OtherSans(other));
    }
    if sorted_lower(&dns) != sorted_lower(&spec.dns_names) {
        return Err(Refusal::DnsSans {
            got: dns,
            want: spec.dns_names.clone(),
        });
    }
    let mut got_uris = uris.clone();
    got_uris.sort();
    let mut want_uris = spec.uris.clone();
    want_uris.sort();
    if got_uris != want_uris {
        return Err(Refusal::UriSans {
            got: uris,
            want: spec.uris.clone(),
        });
    }

    // Extended key usage: present, holding every requested usage and
    // nothing beyond serverAuth/clientAuth.
    let eku = leaf
        .extended_key_usage()
        .map_err(|err| Refusal::Usage(format!("invalid extendedKeyUsage: {err}")))?
        .map(|ext| ext.value);
    let Some(eku) = eku else {
        return Err(Refusal::Usage(
            "it has no extended key usage, so it would be valid for any purpose; Legion needs \
             exactly serverAuth and/or clientAuth"
                .into(),
        ));
    };
    if eku.any
        || eku.code_signing
        || eku.email_protection
        || eku.time_stamping
        || eku.ocsp_signing
        || !eku.other.is_empty()
    {
        return Err(Refusal::Usage(
            "its extended key usage allows more than serverAuth and clientAuth".into(),
        ));
    }
    if spec.server_auth && !eku.server_auth {
        return Err(Refusal::Usage(
            "its extended key usage lacks serverAuth, which Legion asked for".into(),
        ));
    }
    if spec.client_auth && !eku.client_auth {
        return Err(Refusal::Usage(
            "its extended key usage lacks clientAuth, which Legion asked for".into(),
        ));
    }

    // A sane validity window.
    let not_before = leaf.validity().not_before.timestamp();
    let not_after = leaf.validity().not_after.timestamp();
    let now_unix = now.unix_timestamp();
    if not_before > now_unix + MAX_NOT_BEFORE_SKEW.as_secs() as i64 {
        return Err(Refusal::NotYetValid {
            not_before: rfc3339(not_before),
        });
    }
    let start = not_before.max(now_unix);
    let lifetime = not_after - start;
    let requested = spec.ttl.as_secs() as i64;
    if not_after > now_unix + requested + MAX_LIFETIME_SLACK.as_secs() as i64 {
        return Err(Refusal::TooLong {
            lifetime: humanize(not_after - now_unix),
            requested: humanize(requested),
        });
    }
    if lifetime < MIN_LEAF_LIFETIME.as_secs() as i64 {
        return Err(Refusal::TooShort {
            lifetime: humanize(lifetime),
        });
    }

    // The chain verifies against the trust bundle, for every usage asked
    // for. webpki also enforces the CA's name constraints and path length.
    let roots = crate::cluster_tls::roots_from_pem(&view.bundle_pem)
        .map_err(|err| Refusal::Chain(format!("{err:#}")))?;
    let roots = std::sync::Arc::new(roots);
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let at = UnixTime::since_unix_epoch(std::time::Duration::from_secs((start + 1).max(0) as u64));
    if spec.server_auth {
        use rustls::client::danger::ServerCertVerifier;
        let name = spec
            .dns_names
            .first()
            .ok_or_else(|| Refusal::Chain("a server certificate needs a DNS name".into()))?;
        let server_name = ServerName::try_from(name.as_str())
            .map_err(|err| Refusal::Chain(format!("{name}: {err}")))?
            .to_owned();
        rustls::client::WebPkiServerVerifier::builder_with_provider(
            roots.clone(),
            provider.clone(),
        )
        .build()
        .map_err(|err| Refusal::Chain(err.to_string()))?
        .verify_server_cert(&leaf_der, &chain, &server_name, &[], at)
        .map_err(|err| Refusal::Chain(err.to_string()))?;
    }
    if spec.client_auth {
        rustls::server::WebPkiClientVerifier::builder_with_provider(roots, provider)
            .build()
            .map_err(|err| Refusal::Chain(err.to_string()))?
            .verify_client_cert(&leaf_der, &chain, at)
            .map_err(|err| Refusal::Chain(err.to_string()))?;
    }

    Ok(Issued {
        leaf_pem: der_to_pem(leaf_der.as_ref()),
        chain_pem: chain.iter().map(|c| der_to_pem(c.as_ref())).collect(),
        bundle_pem: view.bundle_pem.clone(),
        expires_at: not_after,
        not_before,
    })
}
