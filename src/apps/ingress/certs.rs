//! Certificates: the bundles the ingress serves on port 443, chosen by SNI
//! (docs/ingress.md, "HTTPS").
//!
//! Two sources, both PEM bundles (the chain, leaf first, then the key) in
//! the app's reserved KV prefix:
//!
//! - **operator bundles**, any key under `legion/apps/ingress/certs/`,
//!   served for the names in their leaf's SANs, wildcards included. They
//!   win over ordered ones, and the issuer orders nothing they cover;
//! - **issued bundles**, `legion/apps/ingress/issued/<certificate>`, which
//!   the cert manager writes for the `certs/Certificate` the ingress keeps
//!   for each routed host ([`super::certificates`]).
//!
//! Every replica follows both with [`CertStore::sync`] and resolves SNI from
//! memory; the handshake never waits on Legion.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use crate::prelude::*;

/// Every key the app keeps lives under this prefix.
pub const KV_PREFIX: &str = "legion/apps/ingress/";
/// Operator bundles.
pub const OPERATOR_PREFIX: &str = "legion/apps/ingress/certs/";
/// Bundles the cert manager issued for the ingress's Certificates.
pub const ISSUED_PREFIX: &str = super::certificates::ISSUED_PREFIX;

/// Where a bundle came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CertSource {
    /// Under [`OPERATOR_PREFIX`].
    Operator,
    /// Under [`ISSUED_PREFIX`].
    Issued,
}

impl CertSource {
    pub fn label(self) -> &'static str {
        match self {
            CertSource::Operator => "operator",
            CertSource::Issued => "cert manager",
        }
    }

    /// The source of a KV key, or `None` for a key that holds no bundle.
    pub fn of_key(key: &str) -> Option<CertSource> {
        if key.starts_with(OPERATOR_PREFIX) && key.len() > OPERATOR_PREFIX.len() {
            Some(CertSource::Operator)
        } else if key.starts_with(ISSUED_PREFIX) && key.len() > ISSUED_PREFIX.len() {
            Some(CertSource::Issued)
        } else {
            None
        }
    }
}

/// One parsed bundle.
#[derive(Clone)]
pub struct LoadedCert {
    pub key: String,
    pub version: i64,
    pub source: CertSource,
    /// The leaf's DNS names, lower-case (`*.example.com` included).
    pub names: Vec<String>,
    /// Unix seconds.
    pub not_before: i64,
    pub not_after: i64,
    pub issuer: String,
    pub certified: Arc<CertifiedKey>,
}

impl std::fmt::Debug for LoadedCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedCert")
            .field("key", &self.key)
            .field("version", &self.version)
            .field("source", &self.source)
            .field("names", &self.names)
            .field("not_after", &self.not_after)
            .finish()
    }
}

impl LoadedCert {
    /// Whether a third or less of its lifetime is left at `now` (unix
    /// seconds): when it is due for renewal.
    pub fn renewal_due(&self, now: i64) -> bool {
        let lifetime = (self.not_after - self.not_before).max(0);
        self.not_after - now <= lifetime / 3
    }

    pub fn expired(&self, now: i64) -> bool {
        self.not_after <= now
    }
}

/// Parse a PEM bundle into what rustls serves, reading the leaf's names and
/// validity.
pub fn parse_bundle(key: &str, version: i64, source: CertSource, pem: &str) -> R<LoadedCert> {
    use x509_parser::extensions::GeneralName;
    let chain = crate::worker::netd::tls::certs_from_pem(pem.as_bytes())?;
    let Some(leaf) = chain.first() else {
        bail!("{key} holds no certificate");
    };
    let private = crate::worker::netd::tls::key_from_pem(pem.as_bytes())
        .with_context(|| format!("{key} holds no private key"))?;
    let signer = rustls::crypto::ring::sign::any_supported_type(&private)
        .map_err(|err| anyhow!("{key}: unsupported private key: {err}"))?;
    let (_, parsed) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|err| anyhow!("{key}: invalid certificate: {err}"))?;
    let mut names = Vec::new();
    if let Ok(Some(san)) = parsed.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::DNSName(dns) = name {
                let dns = dns.trim_end_matches('.').to_ascii_lowercase();
                if !names.contains(&dns) {
                    names.push(dns);
                }
            }
        }
    }
    if names.is_empty() {
        bail!("{key}: the certificate names no DNS host in its subject alternative names");
    }
    let validity = parsed.validity();
    let certified = CertifiedKey::new(chain.clone(), signer);
    // A key that does not belong to the leaf would fail every handshake.
    certified
        .keys_match()
        .map_err(|err| anyhow!("{key}: the private key does not match the certificate: {err}"))?;
    Ok(LoadedCert {
        key: key.to_string(),
        version,
        source,
        names,
        not_before: validity.not_before.timestamp(),
        not_after: validity.not_after.timestamp(),
        issuer: parsed.issuer().to_string(),
        certified: Arc::new(certified),
    })
}

/// What [`CertStore::sync`] needs from KV.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvVersion {
    pub key: String,
    pub version: i64,
}

#[derive(Default)]
struct Index {
    /// Every loaded bundle, by KV key.
    loaded: BTreeMap<String, LoadedCert>,
    /// Bundles that failed to parse, by KV key: version and error.
    broken: BTreeMap<String, (i64, String)>,
    /// Name to the bundle served for it (exact names and `*.parent`).
    by_name: HashMap<String, Arc<CertifiedKey>>,
}

impl Index {
    /// Rebuild `by_name`: for each name, an operator bundle over an issued
    /// one, then the one that expires last, skipping expired bundles while
    /// a live one exists.
    fn rebuild(&mut self, now: i64) {
        let mut best: HashMap<String, &LoadedCert> = HashMap::new();
        for cert in self.loaded.values() {
            for name in &cert.names {
                let better = match best.get(name) {
                    None => true,
                    Some(current) => {
                        let rank = |cert: &LoadedCert| {
                            (
                                !cert.expired(now),
                                cert.source == CertSource::Operator,
                                cert.not_after,
                            )
                        };
                        rank(cert) > rank(current)
                    }
                };
                if better {
                    best.insert(name.clone(), cert);
                }
            }
        }
        self.by_name = best
            .into_iter()
            .map(|(name, cert)| (name, cert.certified.clone()))
            .collect();
    }
}

/// The certificates every replica serves.
#[derive(Default)]
pub struct CertStore {
    index: RwLock<Index>,
}

impl std::fmt::Debug for CertStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertStore").finish_non_exhaustive()
    }
}

impl CertStore {
    /// The bundle served for `host`: its exact name, else the `*.` parent.
    pub fn lookup(&self, host: &str) -> Option<Arc<CertifiedKey>> {
        let index = self.index.read().ok()?;
        if let Some(certified) = index.by_name.get(host) {
            return Some(certified.clone());
        }
        let (_, parent) = host.split_once('.')?;
        index.by_name.get(&format!("*.{parent}")).cloned()
    }

    /// Whether HTTPS is served for `host` (what the redirect checks).
    pub fn serves(&self, host: &str) -> bool {
        self.lookup(host).is_some()
    }

    /// Whether a live operator bundle covers `host`: the ingress asks the
    /// cert manager for nothing for it.
    pub fn operator_covers(&self, host: &str, now: i64) -> bool {
        let Ok(index) = self.index.read() else {
            return false;
        };
        let wildcard = host
            .split_once('.')
            .map(|(_, parent)| format!("*.{parent}"));
        index.loaded.values().any(|cert| {
            cert.source == CertSource::Operator
                && !cert.expired(now)
                && cert.names.iter().any(|name| {
                    name == host || wildcard.as_deref().is_some_and(|wildcard| name == wildcard)
                })
        })
    }

    /// Every loaded bundle, and every broken one with its error.
    pub fn snapshot(&self) -> (Vec<LoadedCert>, Vec<(String, String)>) {
        match self.index.read() {
            Ok(index) => (
                index.loaded.values().cloned().collect(),
                index
                    .broken
                    .iter()
                    .map(|(key, (_, error))| (key.clone(), error.clone()))
                    .collect(),
            ),
            Err(_) => (Vec::new(), Vec::new()),
        }
    }

    /// Bring the store in line with `listing` (every bundle key under the
    /// app's prefix, with its version): fetch what is new or changed with
    /// `fetch`, drop what is gone. Returns how many bundles changed.
    pub async fn sync<F, Fut>(&self, listing: &[KvVersion], now: i64, fetch: F) -> usize
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = R<Option<(String, i64)>>>,
    {
        let wanted: BTreeMap<&str, (i64, CertSource)> = listing
            .iter()
            .filter_map(|entry| {
                CertSource::of_key(&entry.key)
                    .map(|source| (entry.key.as_str(), (entry.version, source)))
            })
            .collect();
        let stale: Vec<(String, CertSource)> = {
            let Ok(index) = self.index.read() else {
                return 0;
            };
            wanted
                .iter()
                .filter(|(key, (version, _))| {
                    let loaded = index.loaded.get(**key).map(|cert| cert.version);
                    let broken = index.broken.get(**key).map(|(version, _)| *version);
                    loaded.or(broken) != Some(*version)
                })
                .map(|(key, (_, source))| (key.to_string(), *source))
                .collect()
        };
        let mut fetched = Vec::new();
        for (key, source) in stale {
            match fetch(key.clone()).await {
                Ok(Some((pem, version))) => fetched.push((
                    key.clone(),
                    version,
                    parse_bundle(&key, version, source, &pem),
                )),
                Ok(None) => {}
                Err(err) => debug!(key, "Reading a certificate bundle: {err:#}"),
            }
        }
        let Ok(mut index) = self.index.write() else {
            return 0;
        };
        let mut changed = 0;
        let gone: Vec<String> = index
            .loaded
            .keys()
            .chain(index.broken.keys())
            .filter(|key| !wanted.contains_key(key.as_str()))
            .cloned()
            .collect();
        for key in gone {
            index.loaded.remove(&key);
            index.broken.remove(&key);
            changed += 1;
        }
        for (key, version, parsed) in fetched {
            changed += 1;
            match parsed {
                Ok(cert) => {
                    index.broken.remove(&key);
                    index.loaded.insert(key, cert);
                }
                Err(err) => {
                    warn!(key, "Not serving a certificate bundle: {err:#}");
                    index.loaded.remove(&key);
                    index.broken.insert(key, (version, format!("{err:#}")));
                }
            }
        }
        if changed > 0 {
            index.rebuild(now);
        }
        changed
    }
}

/// The SNI resolver rustls asks on every handshake.
#[derive(Debug)]
pub struct SniResolver(pub Arc<CertStore>);

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let name = hello.server_name()?.to_ascii_lowercase();
        self.0.lookup(name.trim_end_matches('.'))
    }
}

/// The TLS config port 443 serves: certificates by SNI, HTTP/2 and
/// HTTP/1.1 by ALPN.
pub fn server_config(store: Arc<CertStore>) -> Arc<rustls::ServerConfig> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports the default protocol versions")
    .with_no_client_auth()
    .with_cert_resolver(Arc::new(SniResolver(store)));
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(config)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A self-signed bundle for `names`, valid for `days` from now.
    pub fn bundle(names: &[&str], days: i64) -> String {
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params = rcgen::CertificateParams::new(
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
        .expect("params");
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::days(1);
        params.not_after = now + time::Duration::days(days);
        let cert = params.self_signed(&key).expect("cert");
        format!("{}{}", cert.pem(), key.serialize_pem())
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    #[test]
    fn bundles_parse_with_their_names_and_validity() {
        let pem = bundle(&["Shop.example.com", "*.example.com"], 90);
        let cert = parse_bundle("legion/apps/ingress/certs/x", 3, CertSource::Operator, &pem)
            .expect("parses");
        assert_eq!(cert.names, vec!["shop.example.com", "*.example.com"]);
        assert!(!cert.renewal_due(now()));
        assert!(cert.not_after > now());
        assert!(parse_bundle("k", 1, CertSource::Operator, "not pem").is_err());
        // A key from another certificate is refused.
        let other = bundle(&["a.example.com"], 1);
        let mixed = format!(
            "{}{}",
            &pem[..pem.find("-----BEGIN PRIVATE KEY").unwrap()],
            &other[other.find("-----BEGIN PRIVATE KEY").unwrap()..]
        );
        let err = parse_bundle("k", 1, CertSource::Operator, &mixed).unwrap_err();
        assert!(format!("{err:#}").contains("does not match"), "{err:#}");
    }

    #[test]
    fn renewal_is_due_in_the_last_third() {
        let pem = bundle(&["a.example.com"], 20);
        let cert = parse_bundle("k", 1, CertSource::Issued, &pem).unwrap();
        // 21 days of life (one before now), 20 left: not due.
        assert!(!cert.renewal_due(now()));
        assert!(cert.renewal_due(now() + 14 * 86_400));
    }

    #[tokio::test]
    async fn sync_follows_kv_and_operator_bundles_win() {
        let store = CertStore::default();
        let operator = bundle(&["*.example.com"], 30);
        let ordered = bundle(&["shop.example.com"], 90);
        let kv: std::collections::HashMap<String, (String, i64)> = [
            (format!("{OPERATOR_PREFIX}wild"), (operator, 1)),
            (format!("{ISSUED_PREFIX}shop-example-com-0"), (ordered, 4)),
            (format!("{OPERATOR_PREFIX}broken"), ("junk".to_string(), 2)),
        ]
        .into_iter()
        .collect();
        let listing: Vec<KvVersion> = kv
            .iter()
            .map(|(key, (_, version))| KvVersion {
                key: key.clone(),
                version: *version,
            })
            .chain(std::iter::once(KvVersion {
                key: format!("{KV_PREFIX}acme/account"),
                version: 1,
            }))
            .collect();
        let fetch = |key: String| {
            let value = kv.get(&key).cloned();
            async move { Ok(value) }
        };
        assert_eq!(store.sync(&listing, now(), fetch).await, 3);
        assert!(store.serves("shop.example.com"));
        assert!(store.serves("other.example.com"), "by wildcard");
        assert!(!store.serves("a.b.example.com"));
        assert!(!store.serves("example.org"));
        assert!(store.operator_covers("other.example.com", now()));
        assert!(store.operator_covers("shop.example.com", now()));
        let (loaded, broken) = store.snapshot();
        assert_eq!(loaded.len(), 2);
        assert_eq!(broken.len(), 1);

        // Nothing changed: nothing fetched.
        let none = |_key: String| async move { Ok::<_, anyhow::Error>(None) };
        assert_eq!(store.sync(&listing, now(), none).await, 0);

        // The operator bundle goes away; the ordered one serves on.
        let fewer: Vec<KvVersion> = listing
            .iter()
            .filter(|entry| !entry.key.ends_with("wild"))
            .cloned()
            .collect();
        assert_eq!(store.sync(&fewer, now(), none).await, 1);
        assert!(store.serves("shop.example.com"));
        assert!(!store.serves("other.example.com"));
    }
}
