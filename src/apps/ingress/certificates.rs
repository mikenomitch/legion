//! Certificates for routed hosts, from the cert manager (docs/ingress.md,
//! "HTTPS"; ingress plan calls entry 1).
//!
//! The ingress orders nothing itself. For every routed host its Issuer can
//! prove, it keeps one `certs/Certificate` in `legion-system`, writing the
//! certificate under its own KV prefix (`legion/apps/ingress/issued/`),
//! and the bundled cert manager issues and renews it there. The ingress
//! then serves it like any bundle ([`super::certs`]), and answers the
//! Issuer's HTTP-01 challenges on port 80 with `<token>.<thumbprint>`: the
//! thumbprint is public, so every replica answers every challenge with no
//! shared state.
//!
//! Every replica follows the Issuer (for the thumbprint and the views) and
//! the Certificates (for the views). Only the holder of [`LOCK_KEY`]
//! writes, so two replicas that see a route change a moment apart do not
//! undo each other.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::control::{Control, ObjectView};
use super::routes::{publicly_issuable, RouteTable};
use super::State;
use crate::prelude::*;

/// The lock whose holder writes the ingress's Certificates and Issuer.
pub const LOCK_KEY: &str = "legion/apps/ingress/certificates/lock";
/// Its lease, renewed every pass.
pub const LOCK_TTL: Duration = Duration::from_secs(90);
/// Where the ingress's Certificates live.
pub const NAMESPACE: &str = "legion-system";
/// Where the cert manager writes each one: `<prefix><certificate name>`.
pub const ISSUED_PREFIX: &str = "legion/apps/ingress/issued/";
/// The Issuer the ingress keeps itself, from its `acme` config, unless the
/// config names another.
pub const DEFAULT_ISSUER: &str = "ingress";
/// The label on every Certificate the ingress keeps, and the host one names.
pub const MANAGED_LABEL: &str = "ingress/managed";
pub const HOST_LABEL: &str = "ingress/host";
/// How often everything is re-read when nothing changes.
pub const PASS_EVERY: Duration = Duration::from_secs(30);
/// How often the Issuer is re-read while its thumbprint is not known yet:
/// HTTP-01 challenges cannot be answered until it is.
pub const THUMBPRINT_EVERY: Duration = Duration::from_secs(5);

/// The app that owns the certificate kinds: `certs`, the bundled cert
/// manager (tests install it under another name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kinds {
    pub app: String,
}

impl Default for Kinds {
    fn default() -> Self {
        Kinds {
            app: "certs".to_string(),
        }
    }
}

impl Kinds {
    pub fn issuer(&self) -> String {
        format!("{}/Issuer", self.app)
    }
    pub fn certificate(&self) -> String {
        format!("{}/Certificate", self.app)
    }
}

/// What kind of Issuer issues the ingress's certificates, which decides
/// which hosts it can prove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssuerKind {
    /// An ACME CA; with `dns01` it can prove wildcards, and without it only
    /// public exact names over HTTP-01.
    Acme { dns01: bool, http01: bool },
    /// A private CA or self-signed: any name.
    Private,
}

/// The Issuer as the ingress last read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuerView {
    pub name: String,
    /// `None` when it does not exist.
    pub kind: Option<IssuerKind>,
    /// The Ready condition: whether it is ready, and why not.
    pub ready: bool,
    pub reason: String,
    pub message: String,
    /// The ACME account's thumbprint, which HTTP-01 answers carry.
    pub thumbprint: Option<String>,
    /// Whether its `namespaces` admit `legion-system`.
    pub admits_ingress: bool,
}

impl IssuerView {
    pub fn missing(name: &str) -> IssuerView {
        IssuerView {
            name: name.to_string(),
            kind: None,
            ready: false,
            reason: "IssuerNotFound".into(),
            message: format!("there is no certs/Issuer named {name}"),
            thumbprint: None,
            admits_ingress: false,
        }
    }

    pub fn from_object(name: &str, object: &ObjectView) -> IssuerView {
        let spec = &object.spec;
        let kind = if let Some(acme) = spec.get("acme") {
            Some(IssuerKind::Acme {
                dns01: acme.get("dns01").is_some_and(|dns01| !dns01.is_null()),
                http01: acme.get("http01").is_some_and(|http01| !http01.is_null()),
            })
        } else if spec.get("ca").is_some() || spec.get("selfSigned").is_some() {
            Some(IssuerKind::Private)
        } else {
            None
        };
        let namespaces: Vec<String> = spec
            .get("namespaces")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_else(|| vec!["*".to_string()]);
        let (ready, reason, message) = match &object.ready {
            Some((status, reason, message)) => (status == "true", reason.clone(), message.clone()),
            None => (
                false,
                "Pending".into(),
                "the cert manager has not reported on it yet".into(),
            ),
        };
        IssuerView {
            name: name.to_string(),
            kind,
            ready,
            reason,
            message,
            thumbprint: object.fields["thumbprint"]
                .as_str()
                .filter(|thumbprint| !thumbprint.is_empty())
                .map(str::to_string),
            admits_ingress: namespaces
                .iter()
                .any(|pattern| crate::structs::acl::namespace_glob_match(pattern, NAMESPACE)),
        }
    }

    /// Whether this Issuer can prove `host`, and if not, why.
    pub fn proves(&self, host: &str) -> Result<(), String> {
        match &self.kind {
            None => Err(format!("the certs/Issuer {} does not exist", self.name)),
            Some(IssuerKind::Private) => Ok(()),
            Some(IssuerKind::Acme { dns01, http01 }) => {
                if host.starts_with("*.") {
                    return if *dns01 {
                        Ok(())
                    } else {
                        Err(format!(
                            "a wildcard needs DNS-01: the certs/Issuer {} has no dns01",
                            self.name
                        ))
                    };
                }
                if !publicly_issuable(host) {
                    return Err(format!(
                        "no public CA issues for {host}: serve it over HTTP, or use a ca Issuer"
                    ));
                }
                if *dns01 || *http01 {
                    Ok(())
                } else {
                    Err(format!(
                        "the certs/Issuer {} has neither dns01 nor http01",
                        self.name
                    ))
                }
            }
        }
    }
}

/// Whether `issuer` proves names over HTTP-01 only and has no account
/// thumbprint yet: no challenge could be answered.
pub fn issuer_awaits_account(issuer: &IssuerView) -> bool {
    matches!(
        issuer.kind,
        Some(IssuerKind::Acme {
            dns01: false,
            http01: true
        })
    ) && issuer.thumbprint.is_none()
}

/// The ingress's Certificate for `host`: one DNS label, unique per host.
pub fn certificate_name(host: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(host.as_bytes());
    let short: String = digest[..5]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let readable = host.replace("*.", "wildcard.");
    let mut slug: String = readable
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    // One label: at most 63, with room for "-" and the digest.
    slug.truncate(63 - 1 - short.len());
    let slug = slug.trim_matches('-');
    format!("{slug}-{short}")
}

/// The KV key the cert manager writes `host`'s certificate to.
pub fn issued_key(host: &str) -> String {
    format!("{ISSUED_PREFIX}{}", certificate_name(host))
}

/// The Certificate document for `host`.
pub fn certificate_document(kinds: &Kinds, issuer: &str, host: &str) -> Value {
    json!({
        "kind": kinds.certificate(),
        "namespace": NAMESPACE,
        "name": certificate_name(host),
        "labels": { MANAGED_LABEL: "true", HOST_LABEL: host },
        "spec": {
            "issuer": issuer,
            "dnsNames": [host],
            "kv": issued_key(host),
        },
    })
}

/// The Issuer document the ingress keeps when it uses its own.
pub fn default_issuer_document(kinds: &Kinds, server: &str, email: Option<&str>) -> Value {
    let mut acme = json!({ "server": server, "http01": {} });
    if let Some(email) = email {
        acme["email"] = json!(email);
    }
    json!({
        "kind": kinds.issuer(),
        "name": DEFAULT_ISSUER,
        "labels": { MANAGED_LABEL: "true" },
        "spec": { "namespaces": [NAMESPACE], "acme": acme },
    })
}

/// Every routed host and whether the Issuer can prove it: the Certificates
/// the ingress wants, by name, and the hosts it skips, with why.
pub fn wanted(
    table: &RouteTable,
    issuer: &IssuerView,
) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
    let mut wanted = BTreeMap::new();
    let mut skipped = BTreeMap::new();
    for host in table.served_hosts() {
        match issuer.proves(&host) {
            Ok(()) => {
                wanted.insert(certificate_name(&host), host);
            }
            Err(why) => {
                skipped.insert(host, why);
            }
        }
    }
    (wanted, skipped)
}

/// What the views know about one host's certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertState {
    /// The cert manager has it: its Ready condition.
    Requested {
        ready: bool,
        reason: String,
        message: String,
    },
    /// The Certificate exists and the cert manager has not reported yet.
    Waiting,
    /// Not asked for yet: the Issuer proves names over HTTP-01 and has not
    /// registered its ACME account, so the ingress could not answer the
    /// challenge.
    AwaitingIssuer,
    /// No Certificate: the ingress does not ask for one, and why.
    Skipped(String),
}

/// The certificates loop of one replica.
pub struct Certificates {
    control: Arc<dyn Control>,
    state: Arc<State>,
    lock_id: String,
    owner: String,
    holding: bool,
    /// The default Issuer as last written, so it is written only on change.
    written_issuer: Option<Value>,
}

impl Certificates {
    pub fn new(control: Arc<dyn Control>, state: Arc<State>, owner: String) -> Certificates {
        Certificates {
            control,
            state,
            lock_id: format!("{:032x}", rand::random::<u128>()),
            owner,
            holding: false,
            written_issuer: None,
        }
    }

    pub fn holding(&self) -> bool {
        self.holding
    }

    /// Give the lock back, so another replica takes over at once.
    pub async fn release(&mut self) {
        if self.holding {
            self.control.unlock(LOCK_KEY, &self.lock_id).await;
            self.holding = false;
        }
    }

    async fn renew(&mut self) -> bool {
        match self
            .control
            .lock(LOCK_KEY, &self.lock_id, &self.owner, LOCK_TTL)
            .await
        {
            Ok(held) => {
                if held && !self.holding {
                    info!(owner = %self.owner, "This replica keeps the ingress's certificates");
                }
                self.holding = held;
            }
            Err(err) => {
                debug!("Renewing the certificates lock: {err:#}");
                self.holding = false;
            }
        }
        self.holding
    }

    /// Re-read the Issuer: its thumbprint answers HTTP-01 challenges.
    pub async fn read_issuer(&self) -> IssuerView {
        let config = &self.state.config;
        let issuer = match self
            .control
            .get_object(&config.kinds.issuer(), &config.issuer, "")
            .await
        {
            Ok(Some(object)) => IssuerView::from_object(&config.issuer, &object),
            Ok(None) => IssuerView::missing(&config.issuer),
            Err(err) => {
                debug!("Reading the certs/Issuer: {err:#}");
                // Keep what was known: a manager blip must not stop
                // answering challenges.
                return self
                    .state
                    .issuer()
                    .unwrap_or_else(|| IssuerView::missing(&config.issuer));
            }
        };
        self.state.set_issuer(issuer.clone());
        issuer
    }

    /// One pass: the default Issuer (holder only), the Issuer as it reads
    /// now, the Certificates as they read now, and (holder only) the
    /// Certificates brought in line with the routes.
    pub async fn pass(&mut self) {
        let config = self.state.config.clone();
        if !config.certificates {
            self.state.set_cert_states(BTreeMap::new());
            return;
        }
        let holding = self.renew().await;
        if holding && config.issuer == DEFAULT_ISSUER {
            let document = default_issuer_document(
                &config.kinds,
                &config.acme_server,
                config.acme_email.as_deref(),
            );
            if self.written_issuer.as_ref() != Some(&document) {
                match self.control.apply_object(&document).await {
                    Ok(()) => self.written_issuer = Some(document),
                    Err(err) => warn!("Keeping the ingress's certs/Issuer: {err:#}"),
                }
            }
        }
        let issuer = self.read_issuer().await;
        let table = self.state.table();
        let (mut wanted, mut skipped) = wanted(&table, &issuer);
        drop(table);
        // A host an operator's bundle covers is served from it: nothing to
        // ask the cert manager for.
        let now = chrono::Utc::now().timestamp();
        wanted.retain(|_, host| {
            let covered = self.state.certs.operator_covers(host, now);
            if covered {
                skipped.insert(
                    host.clone(),
                    "served from an operator bundle under legion/apps/ingress/certs/".into(),
                );
            }
            !covered
        });
        let labels: BTreeMap<String, String> =
            [(MANAGED_LABEL.to_string(), "true".to_string())].into();
        let existing = match self
            .control
            .list_objects(&config.kinds.certificate(), NAMESPACE, &labels)
            .await
        {
            Ok(existing) => existing,
            Err(err) => {
                debug!("Listing the ingress's Certificates: {err:#}");
                return;
            }
        };
        let by_name: BTreeMap<&str, &ObjectView> = existing
            .iter()
            .map(|object| (object.name.as_str(), object))
            .collect();
        // An HTTP-01 challenge is answered with the Issuer's account
        // thumbprint. Until the cert manager has registered the account, a
        // new order would fail its challenge and wait out the cert
        // manager's backoff, so a new Certificate waits for it.
        let awaiting = issuer_awaits_account(&issuer);
        if holding {
            for (name, host) in &wanted {
                let document = certificate_document(&config.kinds, &config.issuer, host);
                let current = by_name.get(name.as_str());
                if awaiting && current.is_none() {
                    continue;
                }
                let same = current.is_some_and(|object| {
                    object.spec == document["spec"]
                        && object.labels.get(HOST_LABEL).map(String::as_str) == Some(host.as_str())
                });
                if !same {
                    match self.control.apply_object(&document).await {
                        Ok(()) => {
                            info!(host, certificate = %name, "Asked the cert manager for a certificate")
                        }
                        Err(err) => {
                            warn!(host, "Asking the cert manager for a certificate: {err:#}")
                        }
                    }
                }
            }
            for object in &existing {
                if wanted.contains_key(&object.name) {
                    continue;
                }
                match self
                    .control
                    .delete_object(&config.kinds.certificate(), &object.name, NAMESPACE)
                    .await
                {
                    Ok(()) => {
                        info!(certificate = %object.name, "Dropped a certificate no route serves");
                        // The cert manager leaves the bundle; nothing
                        // routes to it any more.
                        let _ = self
                            .control
                            .kv_delete(&format!("{ISSUED_PREFIX}{}", object.name))
                            .await;
                    }
                    Err(err) => debug!(certificate = %object.name, "Deleting: {err:#}"),
                }
            }
        }
        let mut states = BTreeMap::new();
        for (name, host) in &wanted {
            let state = match by_name.get(name.as_str()) {
                Some(object) => match &object.ready {
                    Some((status, reason, message)) => CertState::Requested {
                        ready: status == "true",
                        reason: reason.clone(),
                        message: message.clone(),
                    },
                    None => CertState::Waiting,
                },
                None if awaiting => CertState::AwaitingIssuer,
                None => CertState::Waiting,
            };
            states.insert(host.clone(), state);
        }
        for (host, why) in skipped {
            states.insert(host, CertState::Skipped(why));
        }
        self.state.set_cert_states(states);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::ingress::routes::{RouteSpec, Target};

    fn issuer(spec: Value, thumbprint: Option<&str>) -> IssuerView {
        IssuerView::from_object(
            "ingress",
            &ObjectView {
                name: "ingress".into(),
                spec,
                fields: json!({ "thumbprint": thumbprint }),
                ready: Some(("true".into(), "Registered".into(), String::new())),
                reported: true,
                ..Default::default()
            },
        )
    }

    #[test]
    fn names_are_one_label_unique_per_host() {
        let name = certificate_name("shop.example.com");
        assert!(name.starts_with("shop-example-com-"), "{name}");
        crate::structs::app_object::validate_object_name(&name).expect("a valid object name");
        assert_ne!(certificate_name("a-b.com"), certificate_name("a.b-com"));
        let wildcard = certificate_name("*.example.com");
        assert!(wildcard.starts_with("wildcard-example-com-"), "{wildcard}");
        let long = certificate_name(&format!("{}.example.com", "a".repeat(60)));
        assert!(long.len() <= 63);
        crate::structs::app_object::validate_object_name(&long).expect("still valid");
        assert_eq!(
            issued_key("shop.example.com"),
            format!("{ISSUED_PREFIX}{name}")
        );
    }

    #[test]
    fn what_an_issuer_can_prove() {
        let http = issuer(
            json!({ "namespaces": ["legion-system"], "acme": { "http01": {} } }),
            Some("T"),
        );
        assert_eq!(
            http.kind,
            Some(IssuerKind::Acme {
                dns01: false,
                http01: true
            })
        );
        assert!(http.admits_ingress);
        assert_eq!(http.thumbprint.as_deref(), Some("T"));
        assert!(http.proves("shop.example.com").is_ok());
        assert!(http.proves("*.example.com").unwrap_err().contains("DNS-01"));
        assert!(http
            .proves("admin.internal")
            .unwrap_err()
            .contains("no public CA"));

        let dns = issuer(json!({ "acme": { "dns01": { "cloudflare": {} } } }), None);
        assert!(dns.proves("*.example.com").is_ok());
        assert!(!dns.admits_ingress, "`*` does not reach legion-system");

        let private = issuer(
            json!({ "namespaces": ["legion-*"], "ca": { "kv": "x" } }),
            None,
        );
        assert!(private.proves("admin.internal").is_ok());
        assert!(private.proves("*.internal").is_ok());
        assert!(private.admits_ingress);

        assert!(IssuerView::missing("x").proves("a.example.com").is_err());
    }

    #[test]
    fn only_an_http01_issuer_without_an_account_holds_orders_back() {
        let http = json!({ "acme": { "http01": {} } });
        assert!(issuer_awaits_account(&issuer(http.clone(), None)));
        assert!(!issuer_awaits_account(&issuer(http, Some("T"))));
        assert!(
            !issuer_awaits_account(&issuer(
                json!({ "acme": { "http01": {}, "dns01": { "cloudflare": {} } } }),
                None
            )),
            "DNS-01 needs no thumbprint from the ingress"
        );
        assert!(!issuer_awaits_account(&issuer(
            json!({ "selfSigned": {} }),
            None
        )));
        assert!(!issuer_awaits_account(&IssuerView::missing("x")));
    }

    #[test]
    fn documents_name_the_issuer_the_host_and_the_key() {
        let kinds = Kinds::default();
        let certificate = certificate_document(&kinds, "ingress", "shop.example.com");
        assert_eq!(certificate["kind"], "certs/Certificate");
        assert_eq!(certificate["namespace"], NAMESPACE);
        assert_eq!(certificate["spec"]["dnsNames"], json!(["shop.example.com"]));
        assert_eq!(certificate["spec"]["kv"], issued_key("shop.example.com"));
        assert_eq!(certificate["labels"][HOST_LABEL], "shop.example.com");
        let issuer =
            default_issuer_document(&kinds, "letsencrypt-staging", Some("ops@example.com"));
        assert_eq!(issuer["kind"], "certs/Issuer");
        assert_eq!(issuer["name"], DEFAULT_ISSUER);
        assert_eq!(issuer["spec"]["namespaces"], json!([NAMESPACE]));
        assert_eq!(issuer["spec"]["acme"]["server"], "letsencrypt-staging");
        assert_eq!(issuer["spec"]["acme"]["http01"], json!({}));
        assert!(
            default_issuer_document(&kinds, "letsencrypt", None)["spec"]["acme"]
                .get("email")
                .is_none()
        );
    }

    #[test]
    fn every_host_is_wanted_or_skipped_with_a_reason() {
        let target = Target {
            job_id: "shop".into(),
            namespace: "default".into(),
            task: "web".into(),
            service: "web".into(),
        };
        let specs = vec![RouteSpec::from_value(
            &json!({ "host": ["shop.example.com", "*.example.com", "shop.internal"] }),
            target,
        )
        .unwrap()];
        let mut backends = std::collections::HashMap::new();
        let table = RouteTable::compile(&specs, &mut backends);
        let http = issuer(
            json!({ "namespaces": ["legion-system"], "acme": { "http01": {} } }),
            None,
        );
        let (wanted, skipped) = wanted(&table, &http);
        assert_eq!(
            wanted.values().cloned().collect::<Vec<_>>(),
            vec!["shop.example.com".to_string()]
        );
        assert_eq!(skipped.len(), 2);
        assert!(skipped["*.example.com"].contains("DNS-01"));
    }
}
