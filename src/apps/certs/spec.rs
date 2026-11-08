//! The specs of the cert manager's two kinds, `certs/Issuer` and
//! `certs/Certificate` (docs/cert-manager.md), as the app parses them.
//!
//! The manager checks each object against the JSON Schema in the bundled
//! manifest (`src/apps/bundled/certs.ncl`) and its CEL rules before the app
//! ever sees it, so these types only have to agree with that schema (a unit
//! test parses every manifest example). What a schema cannot say (a DNS
//! name's shape, a duration, `renewBefore` shorter than `duration`) is
//! checked here, and the app reports a failure as `Accepted=false`.

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::IpAddr;
use std::time::Duration;

/// Let's Encrypt's production directory, `acme.server = "letsencrypt"`.
pub const LETSENCRYPT: &str = "https://acme-v02.api.letsencrypt.org/directory";
/// Let's Encrypt's staging directory, `acme.server = "letsencrypt-staging"`.
pub const LETSENCRYPT_STAGING: &str = "https://acme-staging-v02.api.letsencrypt.org/directory";

/// How long a private certificate lasts unless the spec says otherwise.
pub const DEFAULT_DURATION: &str = "2160h";
/// The shortest certificate the app issues: anything shorter would renew
/// on every resync.
pub const MIN_DURATION: Duration = Duration::from_secs(3600);
/// How long DNS-01 waits after publishing a record before asking the CA to
/// check it, unless the Issuer says otherwise.
pub const DEFAULT_PROPAGATION_DELAY: Duration = Duration::from_secs(30);
/// The longest `propagationDelay` an Issuer may set.
pub const MAX_PROPAGATION_DELAY: Duration = Duration::from_secs(600);

/// A KV key holding a secret: `{ kv = "secrets/cloudflare-dns" }`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvRef {
    pub kv: String,
}

/// `certs/Issuer`'s spec: exactly one of `selfSigned`, `ca`, and `acme`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IssuerSpec {
    /// Namespaces (globs) whose Certificates may use this Issuer. Empty
    /// (the schema's default is `["*"]`) means every namespace.
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub self_signed: Option<SelfSignedSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<CaSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acme: Option<AcmeSpec>,
}

/// `selfSigned = {}`: each certificate signs itself.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SelfSignedSpec {}

/// `ca = { kv, trust? }`: a CA whose certificate and key live in KV.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaSpec {
    /// The CA's bundle: its certificate (and any intermediates toward a
    /// root), then its PKCS#8 private key. Generated when the key does not
    /// exist.
    pub kv: String,
    /// Where the CA's certificate alone is published, for clients to trust.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<String>,
}

/// `acme = { ... }`: an RFC 8555 CA such as Let's Encrypt.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcmeSpec {
    /// `letsencrypt`, `letsencrypt-staging`, or a directory URL.
    #[serde(default = "default_server")]
    pub server: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eab: Option<EabSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns01: Option<Dns01Spec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http01: Option<Http01Spec>,
}

fn default_server() -> String {
    "letsencrypt".to_string()
}

/// External account binding (RFC 8555 §7.3.4): the key id and HMAC key a
/// CA such as ZeroSSL hands out.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EabSpec {
    pub kid: String,
    /// The HMAC key, base64url as the CA gives it, held in KV.
    pub hmac_key: KvRef,
}

/// DNS-01: one provider, and how long to wait for the record to spread.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Dns01Spec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub propagation_delay: Option<String>,
}

/// Cloudflare's API, with a token that may edit DNS in the zone.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudflareSpec {
    pub api_token: KvRef,
}

/// Any DNS API behind lego's `httpreq` shape: `POST <url>/present` and
/// `<url>/cleanup` with `{ "fqdn", "value" }`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebhookSpec {
    pub url: String,
    /// Sent as `Authorization: Bearer <token>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<KvRef>,
}

/// HTTP-01: answered by `legion cert http01` responders (or any server
/// that answers with the Issuer's thumbprint).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct Http01Spec {}

/// Which kind of Issuer a spec is.
#[derive(Debug, Clone, PartialEq)]
pub enum IssuerType {
    SelfSigned,
    Ca(CaSpec),
    Acme(AcmeSpec),
}

impl IssuerType {
    /// `selfSigned`, `ca`, or `acme`, as the spec spells it.
    pub fn name(&self) -> &'static str {
        match self {
            IssuerType::SelfSigned => "selfSigned",
            IssuerType::Ca(_) => "ca",
            IssuerType::Acme(_) => "acme",
        }
    }
}

impl IssuerSpec {
    /// The Issuer's type, with the checks a schema cannot make.
    pub fn issuer_type(&self) -> Result<IssuerType, String> {
        let set = [
            self.self_signed.is_some(),
            self.ca.is_some(),
            self.acme.is_some(),
        ];
        if set.iter().filter(|set| **set).count() != 1 {
            return Err("an Issuer is exactly one of selfSigned, ca, and acme".into());
        }
        if self.self_signed.is_some() {
            return Ok(IssuerType::SelfSigned);
        }
        if let Some(ca) = &self.ca {
            check_kv_path("ca.kv", &ca.kv)?;
            if let Some(trust) = &ca.trust {
                check_kv_path("ca.trust", trust)?;
                if trust == &ca.kv {
                    return Err(
                        "ca.trust must differ from ca.kv: trust is published without the key"
                            .into(),
                    );
                }
            }
            return Ok(IssuerType::Ca(ca.clone()));
        }
        let acme = self.acme.clone().unwrap_or_else(|| AcmeSpec {
            server: default_server(),
            email: None,
            eab: None,
            dns01: None,
            http01: None,
        });
        directory_url(&acme.server)?;
        if acme.dns01.is_none() && acme.http01.is_none() {
            return Err(
                "acme needs a way to prove control of a name: dns01, http01, or both".into(),
            );
        }
        if let Some(dns01) = &acme.dns01 {
            let providers = [dns01.cloudflare.is_some(), dns01.webhook.is_some()];
            if providers.iter().filter(|set| **set).count() != 1 {
                return Err("acme.dns01 names exactly one provider: cloudflare or webhook".into());
            }
            if let Some(delay) = &dns01.propagation_delay {
                let delay = parse_duration(delay)
                    .map_err(|err| format!("acme.dns01.propagationDelay: {err}"))?;
                if delay > MAX_PROPAGATION_DELAY {
                    return Err("acme.dns01.propagationDelay is at most 10m".into());
                }
            }
            if let Some(webhook) = &dns01.webhook {
                let url = url::Url::parse(&webhook.url)
                    .map_err(|err| format!("acme.dns01.webhook.url: {err}"))?;
                if !matches!(url.scheme(), "http" | "https") {
                    return Err("acme.dns01.webhook.url must be http or https".into());
                }
            }
        }
        if let Some(email) = &acme.email {
            if !email.contains('@') || email.chars().any(char::is_whitespace) {
                return Err(format!("acme.email {email:?} is not an email address"));
            }
        }
        Ok(IssuerType::Acme(acme))
    }

    /// Whether a Certificate in `namespace` may use this Issuer.
    pub fn allows_namespace(&self, namespace: &str) -> bool {
        self.namespaces.is_empty()
            || self
                .namespaces
                .iter()
                .any(|pattern| crate::structs::acl::namespace_glob_match(pattern, namespace))
    }
}

/// The directory URL an `acme.server` names.
pub fn directory_url(server: &str) -> Result<String, String> {
    match server.trim() {
        "" | "letsencrypt" => Ok(LETSENCRYPT.to_string()),
        "letsencrypt-staging" => Ok(LETSENCRYPT_STAGING.to_string()),
        other => {
            let url = url::Url::parse(other).map_err(|_| {
                format!(
                    "acme.server {other:?} is not letsencrypt, letsencrypt-staging, or a \
                     directory URL"
                )
            })?;
            if !matches!(url.scheme(), "https" | "http") {
                return Err(format!("acme.server {other:?} must be an https URL"));
            }
            Ok(other.to_string())
        }
    }
}

/// The private key a certificate gets. ECDSA only: `ring` cannot generate
/// RSA keys.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum KeyAlgorithm {
    #[default]
    #[serde(rename = "ecdsa-p256")]
    EcdsaP256,
    #[serde(rename = "ecdsa-p384")]
    EcdsaP384,
}

impl KeyAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyAlgorithm::EcdsaP256 => "ecdsa-p256",
            KeyAlgorithm::EcdsaP384 => "ecdsa-p384",
        }
    }

    pub fn signature_algorithm(&self) -> &'static rcgen::SignatureAlgorithm {
        match self {
            KeyAlgorithm::EcdsaP256 => &rcgen::PKCS_ECDSA_P256_SHA256,
            KeyAlgorithm::EcdsaP384 => &rcgen::PKCS_ECDSA_P384_SHA384,
        }
    }
}

/// What a certificate may be used for (extended key usage).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Usage {
    Server,
    Client,
}

fn default_duration() -> String {
    DEFAULT_DURATION.to_string()
}

fn default_usages() -> Vec<Usage> {
    vec![Usage::Server, Usage::Client]
}

/// Extra keys the certificate is also written to, one part each.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct Parts {
    /// The certificate chain, leaf first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert: Option<String>,
    /// The private key, PKCS#8 PEM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The issuing CA's certificate (for a self-signed certificate, itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<String>,
}

/// `certs/Certificate`'s spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CertificateSpec {
    /// The `certs/Issuer` that issues it.
    pub issuer: String,
    /// Where the bundle goes: the chain, leaf first, then the key.
    pub kv: String,
    #[serde(default)]
    pub dns_names: Vec<String>,
    #[serde(default)]
    pub ip_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub common_name: Option<String>,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renew_before: Option<String>,
    #[serde(default)]
    pub key_algorithm: KeyAlgorithm,
    #[serde(default = "default_usages")]
    pub usages: Vec<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<Parts>,
}

/// A Certificate spec, checked and normalized: names lower-cased and
/// deduplicated, durations parsed.
#[derive(Debug, Clone, PartialEq)]
pub struct Desired {
    pub issuer: String,
    pub kv: String,
    pub dns_names: Vec<String>,
    pub ip_addresses: Vec<IpAddr>,
    pub common_name: Option<String>,
    pub duration: Duration,
    pub renew_before: Option<Duration>,
    pub key_algorithm: KeyAlgorithm,
    pub usages: Vec<Usage>,
    pub parts: Parts,
}

impl CertificateSpec {
    /// Check what the schema cannot, and normalize.
    pub fn desired(&self) -> Result<Desired, String> {
        if self.issuer.trim().is_empty() {
            return Err("issuer names the certs/Issuer to use".into());
        }
        check_kv_path("kv", &self.kv)?;
        let mut dns_names = Vec::new();
        for name in &self.dns_names {
            let name = name.trim().trim_end_matches('.').to_ascii_lowercase();
            check_dns_name(&name)?;
            if !dns_names.contains(&name) {
                dns_names.push(name);
            }
        }
        let mut ip_addresses = Vec::new();
        for ip in &self.ip_addresses {
            let parsed: IpAddr = ip
                .trim()
                .parse()
                .map_err(|_| format!("ipAddresses: {ip:?} is not an IP address"))?;
            if !ip_addresses.contains(&parsed) {
                ip_addresses.push(parsed);
            }
        }
        if dns_names.is_empty() && ip_addresses.is_empty() {
            return Err("a certificate names at least one of dnsNames and ipAddresses".into());
        }
        if let Some(cn) = &self.common_name {
            if cn.trim().is_empty() || cn.len() > 64 {
                return Err("commonName is 1 to 64 characters".into());
            }
        }
        let duration = parse_duration(&self.duration).map_err(|err| format!("duration: {err}"))?;
        if duration < MIN_DURATION {
            return Err("duration is at least 1h".into());
        }
        let renew_before = match &self.renew_before {
            Some(text) => {
                let before = parse_duration(text).map_err(|err| format!("renewBefore: {err}"))?;
                if before >= duration {
                    return Err(format!(
                        "renewBefore ({text}) must be shorter than duration ({})",
                        self.duration
                    ));
                }
                if before.is_zero() {
                    return Err("renewBefore must be more than 0".into());
                }
                Some(before)
            }
            None => None,
        };
        let mut usages = self.usages.clone();
        usages.sort();
        usages.dedup();
        if usages.is_empty() {
            return Err("usages names server, client, or both".into());
        }
        let parts = self.parts.clone().unwrap_or_default();
        for (field, path) in [
            ("parts.cert", &parts.cert),
            ("parts.key", &parts.key),
            ("parts.ca", &parts.ca),
        ] {
            if let Some(path) = path {
                check_kv_path(field, path)?;
                if path == &self.kv {
                    return Err(format!("{field} must differ from kv"));
                }
            }
        }
        Ok(Desired {
            issuer: self.issuer.trim().to_string(),
            kv: self.kv.clone(),
            dns_names,
            ip_addresses,
            common_name: self.common_name.clone(),
            duration,
            renew_before,
            key_algorithm: self.key_algorithm,
            usages,
            parts,
        })
    }
}

impl Desired {
    /// Whether any name is a wildcard (`*.example.com`).
    pub fn has_wildcard(&self) -> bool {
        self.dns_names.iter().any(|name| name.starts_with("*."))
    }

    /// Every name the certificate carries, DNS names first.
    pub fn names(&self) -> Vec<String> {
        let mut names = self.dns_names.clone();
        names.extend(self.ip_addresses.iter().map(IpAddr::to_string));
        names
    }

    /// A digest of everything that decides what a newly issued certificate
    /// holds. A change means the certificate in KV no longer matches the
    /// spec and is re-issued.
    pub fn issuance_hash(&self) -> String {
        let mut dns = self.dns_names.clone();
        dns.sort();
        let mut ips: Vec<String> = self.ip_addresses.iter().map(IpAddr::to_string).collect();
        ips.sort();
        let material = json!({
            "issuer": self.issuer,
            "dnsNames": dns,
            "ipAddresses": ips,
            "commonName": self.common_name,
            "duration": self.duration.as_secs(),
            "keyAlgorithm": self.key_algorithm.as_str(),
            "usages": self.usages,
        });
        let digest = Sha256::digest(material.to_string().as_bytes());
        hex_string(&digest[..12])
    }
}

/// Lower-case hex of `bytes`.
pub fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `90d`, `2160h`, `30m`, `1h 30m`: humantime's durations.
pub fn parse_duration(text: &str) -> R<Duration> {
    humantime::parse_duration(text.trim())
        .map_err(|err| anyhow!("{text:?} is not a duration (e.g. 2160h, 90d, 30m): {err}"))
}

fn check_kv_path(field: &str, path: &str) -> Result<(), String> {
    if path.trim().is_empty() || path != path.trim() {
        return Err(format!("{field} must be a KV key path"));
    }
    if path.starts_with('/') || path.ends_with('/') || path.contains("//") {
        return Err(format!(
            "{field} {path:?} must not start or end with '/' or hold an empty segment"
        ));
    }
    Ok(())
}

/// A DNS name a certificate may carry: labels of letters, digits, `-` and
/// `_`, at most 63 each and 253 in all, with an optional leading `*.`.
pub fn check_dns_name(name: &str) -> Result<(), String> {
    let bare = name.strip_prefix("*.").unwrap_or(name);
    let bad = || format!("dnsNames: {name:?} is not a DNS name");
    if bare.is_empty() || name.len() > 253 || bare.contains('*') {
        return Err(if bare.contains('*') {
            format!("dnsNames: {name:?}: a wildcard is only the whole first label (*.example.com)")
        } else {
            bad()
        });
    }
    for label in bare.split('.') {
        let shaped = !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
        if !shaped {
            return Err(bad());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn certificate(spec: Value) -> Result<Desired, String> {
        serde_json::from_value::<CertificateSpec>(spec)
            .map_err(|err| err.to_string())?
            .desired()
    }

    #[test]
    fn a_minimal_certificate_gets_the_defaults_test() {
        let desired = certificate(json!({
            "issuer": "internal", "kv": "certs/web", "dnsNames": ["Web.Example.com.", "web.example.com"]
        }))
        .unwrap();
        assert_eq!(desired.dns_names, vec!["web.example.com"]);
        assert_eq!(desired.duration, Duration::from_secs(90 * 86400));
        assert_eq!(desired.renew_before, None);
        assert_eq!(desired.key_algorithm, KeyAlgorithm::EcdsaP256);
        assert_eq!(desired.usages, vec![Usage::Server, Usage::Client]);
        assert!(!desired.has_wildcard());
    }

    #[test]
    fn certificate_checks_name_what_is_wrong_test() {
        let cases = [
            (json!({ "issuer": "i", "kv": "k" }), "at least one"),
            (
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.*.com"] }),
                "wildcard",
            ),
            (
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["-a.com"] }),
                "not a DNS name",
            ),
            (
                json!({ "issuer": "i", "kv": "k", "ipAddresses": ["10.0.0"] }),
                "not an IP",
            ),
            (
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com"], "duration": "30m" }),
                "at least 1h",
            ),
            (
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com"], "duration": "24h", "renewBefore": "48h" }),
                "shorter than duration",
            ),
            (
                json!({ "issuer": "i", "kv": "/k", "dnsNames": ["a.com"] }),
                "must not start",
            ),
            (
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com"], "parts": { "key": "k" } }),
                "must differ",
            ),
            (
                json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com"], "usages": [] }),
                "usages",
            ),
        ];
        for (spec, expected) in cases {
            let err = certificate(spec.clone()).expect_err(&spec.to_string());
            assert!(err.contains(expected), "{spec}: {err}");
        }
        assert!(
            certificate(json!({ "issuer": "i", "kv": "k", "dnsNames": ["*.a.com"] }))
                .unwrap()
                .has_wildcard()
        );
    }

    #[test]
    fn the_issuance_hash_moves_only_with_what_a_certificate_holds_test() {
        let base = certificate(json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com", "b.com"] }))
            .unwrap();
        let reordered = certificate(
            json!({ "issuer": "i", "kv": "k", "dnsNames": ["b.com", "a.com"], "renewBefore": "240h" }),
        )
        .unwrap();
        // Order and renewBefore do not change the certificate.
        assert_eq!(base.issuance_hash(), reordered.issuance_hash());
        let moved =
            certificate(json!({ "issuer": "i", "kv": "other", "dnsNames": ["a.com", "b.com"] }))
                .unwrap();
        assert_eq!(
            base.issuance_hash(),
            moved.issuance_hash(),
            "kv is where, not what"
        );
        for changed in [
            json!({ "issuer": "j", "kv": "k", "dnsNames": ["a.com", "b.com"] }),
            json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com"] }),
            json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com", "b.com"], "keyAlgorithm": "ecdsa-p384" }),
            json!({ "issuer": "i", "kv": "k", "dnsNames": ["a.com", "b.com"], "usages": ["server"] }),
        ] {
            assert_ne!(
                base.issuance_hash(),
                certificate(changed.clone()).unwrap().issuance_hash(),
                "{changed}"
            );
        }
    }

    #[test]
    fn issuers_are_exactly_one_type_test() {
        let parse = |spec: Value| -> Result<IssuerType, String> {
            serde_json::from_value::<IssuerSpec>(spec)
                .map_err(|err| err.to_string())?
                .issuer_type()
        };
        assert_eq!(
            parse(json!({ "selfSigned": {} })).unwrap(),
            IssuerType::SelfSigned
        );
        assert!(parse(json!({})).unwrap_err().contains("exactly one"));
        assert!(parse(json!({ "selfSigned": {}, "ca": { "kv": "a" } }))
            .unwrap_err()
            .contains("exactly one"));
        assert!(parse(json!({ "ca": { "kv": "a", "trust": "a" } }))
            .unwrap_err()
            .contains("differ"));
        assert!(parse(json!({ "acme": {} }))
            .unwrap_err()
            .contains("dns01, http01"));
        assert!(parse(json!({ "acme": { "http01": {}, "server": "ftp://x" } })).is_err());
        assert!(parse(json!({ "acme": { "dns01": {} } }))
            .unwrap_err()
            .contains("exactly one provider"));
        assert!(parse(
            json!({ "acme": { "dns01": { "webhook": { "url": "http://x" } }, "email": "nope" } })
        )
        .unwrap_err()
        .contains("email"));
        let IssuerType::Acme(acme) = parse(json!({ "acme": { "http01": {} } })).unwrap() else {
            panic!("acme");
        };
        assert_eq!(directory_url(&acme.server).unwrap(), LETSENCRYPT);
        assert_eq!(
            directory_url("letsencrypt-staging").unwrap(),
            LETSENCRYPT_STAGING
        );
    }

    #[test]
    fn an_issuer_names_the_namespaces_that_may_use_it_test() {
        let open = IssuerSpec::default();
        assert!(open.allows_namespace("team-a"));
        let narrow = IssuerSpec {
            namespaces: vec!["team-*".into()],
            ..Default::default()
        };
        assert!(narrow.allows_namespace("team-a"));
        assert!(!narrow.allows_namespace("default"));
    }
}
