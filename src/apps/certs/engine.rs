//! The cert manager's engine: what reconciling an Issuer and a Certificate
//! does (docs/cert-manager.md, "How it works").
//!
//! - **Issuers** are prepared once per spec version and cached: a `ca`
//!   Issuer's CA is loaded from KV (or generated, and published to its
//!   `trust` key); an `acme` Issuer's account key is loaded from the app's
//!   own KV (or generated) and registered with the CA. Certificates prepare
//!   their Issuer through the same cache, so they never wait on the
//!   Issuer's own reconcile.
//! - **Certificates** are compared with what KV holds, and re-issued when
//!   it is missing, expired, past its renewal time, issued for another spec
//!   or Issuer, or when a renewal was requested (the `certs/renew` label).
//!   A failed issuance backs off (5 minutes, doubling to 6 hours) and keeps
//!   the certificate KV holds.
//! - **Every KV read and write for an object acts for that object**
//!   (`legion-for-object`): a Certificate's key is written within what the
//!   Certificate's writer may write, and a CA key or DNS token is read
//!   within what the Issuer's writer may read. Only ACME account keys live
//!   under the app's own `legion/apps/certs/`.

use super::acme::{AccountKey, AcmeClient, Proof};
use super::dns01::{Cloudflare, DnsProvider, Webhook, CLOUDFLARE_API};
use super::issue::{acme_bundle, csr, self_signed, Bundle, CaMaterial};
use super::spec::{
    directory_url, hex_string, parse_duration, CertificateSpec, Desired, Dns01Spec, IssuerSpec,
    IssuerType, DEFAULT_PROPAGATION_DELAY,
};
use crate::apps::objects::{acting_for, ObjectCache, ObjectOutcome};
use crate::cmd::helpers::ManagerConnection;
use crate::prelude::*;
use crate::structs::{AppObject, GetKvRequest, KvValueType, PutKvRequest};
use sha2::{Digest, Sha256};
use std::sync::RwLock;
use std::time::Duration;

/// The app's name.
pub const APP: &str = "certs";
/// The Issuer kind, as declared.
pub const ISSUER_KIND: &str = "Issuer";
/// The Certificate kind, as declared.
pub const CERTIFICATE_KIND: &str = "Certificate";
/// The label `legion cert renew` sets: a new value re-issues once.
pub const RENEW_LABEL: &str = "certs/renew";
/// Where app `app` keeps an Issuer's ACME account key: under its own
/// reserved KV prefix, by the Issuer's uid.
pub fn account_key_path(app: &str, issuer_uid: u64) -> String {
    format!("legion/apps/{app}/acme/{issuer_uid}.key")
}

/// The first wait after a failed issuance; it doubles per failure.
pub const BACKOFF_MIN: Duration = Duration::from_secs(300);
/// The longest wait between attempts.
pub const BACKOFF_MAX: Duration = Duration::from_secs(6 * 3600);
/// How often a Certificate whose Issuer is missing or not ready is looked
/// at again.
const ISSUER_WAIT: Duration = Duration::from_secs(15);
/// How often an Issuer that failed to set up is tried again.
const ISSUER_RETRY: Duration = Duration::from_secs(60);

/// The wait after `failures` failed attempts in a row.
pub fn backoff(failures: u32) -> Duration {
    BACKOFF_MIN
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(16)))
        .min(BACKOFF_MAX)
}

/// RFC 3339, UTC, seconds.
pub fn rfc3339(unix: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(unix, 0)
        .map(|time| time.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

fn parse_rfc3339(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.timestamp())
}

/// One KV value and its version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvValue {
    pub value: String,
    pub version: i64,
}

/// KV as the engine uses it: for an object (`Some`), or as the app.
#[async_trait::async_trait]
pub trait CertsPlane: Send + Sync + 'static {
    async fn get_kv(&self, acting: Option<&AppObject>, key: &str) -> R<Option<KvValue>>;
    /// Write `value`; with `expected_version`, only over that version (`0`:
    /// only if the key does not exist).
    async fn put_kv(
        &self,
        acting: Option<&AppObject>,
        key: &str,
        value: &str,
        expected_version: Option<i64>,
    ) -> R<()>;
}

/// The engine's KV through the app's connection to Legion.
pub struct LegionPlane {
    connection: ManagerConnection,
}

impl LegionPlane {
    pub fn new(connection: ManagerConnection) -> LegionPlane {
        LegionPlane { connection }
    }

    fn client(&self, acting: Option<&AppObject>) -> R<crate::cmd::helpers::AuthedManagerClient> {
        match acting {
            Some(object) => self.connection.client_for_object(&acting_for(object)),
            None => Ok(self.connection.client()),
        }
    }
}

#[async_trait::async_trait]
impl CertsPlane for LegionPlane {
    async fn get_kv(&self, acting: Option<&AppObject>, key: &str) -> R<Option<KvValue>> {
        let reply = self
            .client(acting)?
            .get_kv(GetKvRequest {
                key: key.to_string(),
                metadata_only: false,
            })
            .await
            .map_err(|status| anyhow!("reading kv {key}: {}", status.message()))?
            .into_inner();
        Ok(reply.entry.map(|entry| KvValue {
            value: entry.value,
            version: entry.version,
        }))
    }

    async fn put_kv(
        &self,
        acting: Option<&AppObject>,
        key: &str,
        value: &str,
        expected_version: Option<i64>,
    ) -> R<()> {
        self.client(acting)?
            .put_kv(PutKvRequest {
                key: key.to_string(),
                value: value.to_string(),
                value_type: KvValueType::String as i32,
                expected_version,
                lock_id: None,
            })
            .await
            .map_err(|status| anyhow!("writing kv {key}: {}", status.message()))?;
        Ok(())
    }
}

/// How the engine reaches the outside world, and its clock.
#[derive(Clone)]
pub struct EngineOptions {
    /// For ACME CAs and DNS APIs.
    pub http: reqwest::Client,
    /// Now, in Unix seconds.
    pub clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Cloudflare's API base.
    pub cloudflare_api: String,
    /// Overrides every Issuer's DNS-01 propagation delay (tests).
    pub propagation_delay: Option<Duration>,
    /// How long one ACME issuance may take.
    pub acme_timeout: Duration,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            http: reqwest::Client::builder()
                .user_agent(concat!("legion-certs/", env!("CARGO_PKG_VERSION")))
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            clock: Arc::new(|| chrono::Utc::now().timestamp()),
            cloudflare_api: CLOUDFLARE_API.to_string(),
            propagation_delay: None,
            acme_timeout: Duration::from_secs(600),
        }
    }
}

/// An Issuer, ready to issue.
enum Prepared {
    SelfSigned,
    Ca {
        material: CaMaterial,
        kv_version: i64,
    },
    Acme {
        client: Arc<AcmeClient>,
        dns01: Option<Dns01Spec>,
        http01: bool,
    },
}

/// A prepared Issuer, for the spec version it was prepared from.
struct PreparedEntry {
    spec_version: u64,
    prepared: Arc<Prepared>,
    /// What a certificate was issued by, for noticing an Issuer change:
    /// the CA's fingerprint, or the ACME directory.
    identity: String,
    /// The Issuer's status fields.
    fields: Value,
    /// The Issuer's `Ready` reason and message.
    reason: &'static str,
    message: String,
}

/// What the engine remembers about one Certificate between reconciles, and
/// seeds from its status after a restart.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    /// The issuance hash of the spec the current certificate was issued for.
    pub issued_hash: Option<String>,
    /// The Issuer identity that issued it.
    pub issuer_identity: Option<String>,
    /// The serial of the certificate the app last wrote.
    pub serial: Option<String>,
    /// The `certs/renew` label value last honored.
    pub renew_request: Option<String>,
    /// How many certificates the app has issued for this object.
    pub revision: u64,
    /// Failed attempts in a row, and the last one.
    pub failures: u32,
    pub last_failure: Option<String>,
    pub last_failure_at: Option<i64>,
    pub next_attempt_at: Option<i64>,
    /// What the failing attempts were for: a different attempt (the spec,
    /// Issuer, or renew request changed) is not held back by their backoff.
    pub failed_for: Option<String>,
}

impl Record {
    /// What a status written by this engine says.
    pub fn from_status(fields: &Value) -> Record {
        let text = |key: &str| {
            fields[key]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        Record {
            issued_hash: text("specHash"),
            issuer_identity: text("issuerIdentity"),
            serial: text("serial"),
            renew_request: text("renewRequest"),
            revision: fields["revision"].as_u64().unwrap_or(0),
            failures: fields["failedAttempts"].as_u64().unwrap_or(0) as u32,
            last_failure: text("lastFailure"),
            last_failure_at: text("lastFailureTime").and_then(|t| parse_rfc3339(&t)),
            next_attempt_at: text("nextAttemptTime").and_then(|t| parse_rfc3339(&t)),
            failed_for: None,
        }
    }
}

/// Whether to issue, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The certificate in KV is good until `renew_at`.
    Keep { renew_at: i64 },
    /// Issue now, for this reason.
    Issue(String),
}

/// Decide whether the certificate in KV (`current`, parsed; or why it did
/// not parse) still serves `desired`.
pub fn decide(
    now: i64,
    desired: &Desired,
    issuer_identity: &str,
    current: Result<Option<&Bundle>, &str>,
    record: &Record,
    renew_request: Option<&str>,
) -> Decision {
    let bundle = match current {
        Err(problem) => {
            return Decision::Issue(format!(
                "kv {} holds no usable certificate ({problem})",
                desired.kv
            ))
        }
        Ok(None) => return Decision::Issue(format!("kv {} holds no certificate yet", desired.kv)),
        Ok(Some(bundle)) => bundle,
    };
    let leaf = &bundle.leaf;
    if leaf.not_after <= now {
        return Decision::Issue(format!(
            "the certificate expired at {}",
            rfc3339(leaf.not_after)
        ));
    }
    if !leaf.names_match(desired) {
        let mut names = leaf.dns_names.clone();
        names.extend(leaf.ip_addresses.iter().map(|ip| ip.to_string()));
        return Decision::Issue(format!(
            "the certificate names {}, the spec asks for {}",
            names.join(", "),
            desired.names().join(", ")
        ));
    }
    if leaf.key_algorithm != Some(desired.key_algorithm) {
        return Decision::Issue(format!(
            "the spec asks for an {} key",
            desired.key_algorithm.as_str()
        ));
    }
    if record.serial.as_deref() != Some(leaf.serial.as_str()) {
        return Decision::Issue(format!(
            "kv {} holds a certificate this Certificate did not issue",
            desired.kv
        ));
    }
    if record.issued_hash.as_deref() != Some(desired.issuance_hash().as_str()) {
        return Decision::Issue("the spec changed".into());
    }
    if record.issuer_identity.as_deref() != Some(issuer_identity) {
        return Decision::Issue(format!("issuer {} changed", desired.issuer));
    }
    if let Some(request) = renew_request {
        if record.renew_request.as_deref() != Some(request) {
            return Decision::Issue(format!("renewal requested ({RENEW_LABEL}={request})"));
        }
    }
    let renew_at = leaf.renewal_time(desired.renew_before);
    if now >= renew_at {
        return Decision::Issue(format!("due for renewal since {}", rfc3339(renew_at)));
    }
    Decision::Keep { renew_at }
}

/// One object's latest outcome, for the app's views.
#[derive(Debug, Clone)]
pub struct BoardRow {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub ready: Option<bool>,
    pub accepted: bool,
    pub reason: String,
    pub message: String,
    pub fields: Value,
}

/// The cert manager.
pub struct Engine {
    plane: Arc<dyn CertsPlane>,
    options: EngineOptions,
    /// Every object of the app's kinds, kept by a watch.
    objects: Arc<RwLock<ObjectCache>>,
    /// Prepared Issuers by uid. Held only to look up or insert.
    prepared: Mutex<HashMap<u64, Arc<PreparedEntry>>>,
    /// One gate per Issuer, held while it is prepared: a slow or unreachable
    /// ACME server holds back its own Certificates, never another Issuer's.
    preparing: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    records: Mutex<HashMap<u64, Record>>,
    board: RwLock<HashMap<u64, BoardRow>>,
}

impl Engine {
    pub fn new(plane: Arc<dyn CertsPlane>, options: EngineOptions) -> Arc<Engine> {
        Arc::new(Engine {
            plane,
            options,
            objects: Arc::new(RwLock::new(ObjectCache::default())),
            prepared: Mutex::new(HashMap::new()),
            preparing: Mutex::new(HashMap::new()),
            records: Mutex::new(HashMap::new()),
            board: RwLock::new(HashMap::new()),
        })
    }

    /// The cache the object watch keeps.
    pub fn objects(&self) -> Arc<RwLock<ObjectCache>> {
        self.objects.clone()
    }

    fn now(&self) -> i64 {
        (self.options.clock)()
    }

    /// The live objects of `kind`.
    pub fn live(&self, kind: &str) -> Vec<AppObject> {
        self.objects
            .read()
            .map(|cache| {
                cache
                    .objects()
                    .filter(|object| object.kind == kind)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The latest outcome of object `uid`.
    pub fn board_row(&self, uid: u64) -> Option<BoardRow> {
        self.board.read().ok()?.get(&uid).cloned()
    }

    fn issuer_named(&self, name: &str) -> Option<AppObject> {
        self.live(ISSUER_KIND)
            .into_iter()
            .find(|object| object.name == name && !object.deleting)
    }

    fn post(&self, object: &AppObject, outcome: &ObjectOutcome) {
        let conditions = outcome.conditions();
        let find = |kind: &str| conditions.iter().find(|c| c.r#type == kind);
        let ready = find(crate::structs::app_object::CONDITION_READY);
        let accepted = find(crate::structs::app_object::CONDITION_ACCEPTED);
        let main = ready.or(accepted);
        let row = BoardRow {
            kind: object.kind.clone(),
            namespace: object.namespace.clone(),
            name: object.name.clone(),
            ready: ready.map(|c| c.status == "true"),
            accepted: accepted.is_none_or(|c| c.status == "true"),
            reason: main.map(|c| c.reason.clone()).unwrap_or_default(),
            message: main.map(|c| c.message.clone()).unwrap_or_default(),
            fields: outcome.fields().clone(),
        };
        if let Ok(mut board) = self.board.write() {
            board.insert(object.uid, row);
        }
    }

    /// Prepare `issuer` (from cache when its spec has not changed; a `ca`
    /// Issuer re-reads its KV when `refresh`).
    async fn prepare(&self, issuer: &AppObject, refresh: bool) -> R<Arc<PreparedEntry>> {
        let gate = self
            .preparing
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(issuer.uid)
            .or_default()
            .clone();
        let _held = gate.lock().await;
        let cached = self
            .prepared
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&issuer.uid)
            .cloned();
        if let Some(entry) = cached {
            let fresh = entry.spec_version == issuer.spec_version;
            let ca_unchanged = match (&*entry.prepared, refresh) {
                (Prepared::Ca { kv_version, .. }, true) if fresh => {
                    let spec: IssuerSpec = serde_json::from_str(&issuer.effective_json)?;
                    let kv = spec.ca.as_ref().map(|ca| ca.kv.clone()).unwrap_or_default();
                    let current = self.plane.get_kv(Some(issuer), &kv).await?;
                    current.map(|v| v.version) == Some(*kv_version)
                }
                _ => true,
            };
            if fresh && ca_unchanged {
                return Ok(entry);
            }
        }
        let entry = Arc::new(self.prepare_fresh(issuer).await?);
        self.prepared
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(issuer.uid, entry.clone());
        Ok(entry)
    }

    async fn prepare_fresh(&self, issuer: &AppObject) -> R<PreparedEntry> {
        let spec: IssuerSpec = serde_json::from_str(&issuer.effective_json)
            .context("the Issuer's spec does not parse")?;
        let kind = spec.issuer_type().map_err(|err| anyhow!("{err}"))?;
        match kind {
            IssuerType::SelfSigned => Ok(PreparedEntry {
                spec_version: issuer.spec_version,
                prepared: Arc::new(Prepared::SelfSigned),
                identity: "selfSigned".into(),
                fields: json!({ "type": "selfSigned" }),
                reason: "SelfSigned",
                message: "each certificate signs itself".into(),
            }),
            IssuerType::Ca(ca) => {
                let mut generated = false;
                let (material, kv_version) = match self.plane.get_kv(Some(issuer), &ca.kv).await? {
                    Some(stored) => (
                        CaMaterial::load(&stored.value)
                            .with_context(|| format!("kv {} holds no usable CA", ca.kv))?,
                        stored.version,
                    ),
                    None => {
                        let (bundle, _) = CaMaterial::generate(&issuer.name, self.now())?;
                        match self
                            .plane
                            .put_kv(Some(issuer), &ca.kv, &bundle, Some(0))
                            .await
                        {
                            Ok(()) => {
                                generated = true;
                                info!(issuer = %issuer.name, kv = %ca.kv, "Generated a CA");
                            }
                            // Someone else wrote it first: use theirs.
                            Err(err) => debug!(error = %format!("{err:#}"), "CA write raced"),
                        }
                        let stored = self
                            .plane
                            .get_kv(Some(issuer), &ca.kv)
                            .await?
                            .ok_or_else(|| anyhow!("kv {} vanished after it was written", ca.kv))?;
                        let loaded = CaMaterial::load(&stored.value)
                            .with_context(|| format!("kv {} holds no usable CA", ca.kv))?;
                        (loaded, stored.version)
                    }
                };
                if let Some(trust) = &ca.trust {
                    let current = self.plane.get_kv(Some(issuer), trust).await?;
                    if current.as_ref().map(|v| v.value.as_str())
                        != Some(material.cert_pem.as_str())
                    {
                        self.plane
                            .put_kv(Some(issuer), trust, &material.cert_pem, None)
                            .await?;
                    }
                }
                let now = self.now();
                let (reason, message) = if material.info.not_after <= now {
                    bail!(
                        "the CA in kv {} expired at {}; put a new one there",
                        ca.kv,
                        rfc3339(material.info.not_after)
                    );
                } else if generated {
                    ("Generated", format!("generated a CA in kv {}", ca.kv))
                } else {
                    ("Loaded", format!("signing with the CA in kv {}", ca.kv))
                };
                Ok(PreparedEntry {
                    spec_version: issuer.spec_version,
                    identity: format!("ca:{}", &material.info.fingerprint[..16]),
                    fields: json!({
                        "type": "ca",
                        "caSubject": material.info.subject,
                        "caNotAfter": rfc3339(material.info.not_after),
                        "caFingerprint": material.info.fingerprint,
                        "generated": generated,
                    }),
                    prepared: Arc::new(Prepared::Ca {
                        material,
                        kv_version,
                    }),
                    reason,
                    message,
                })
            }
            IssuerType::Acme(acme) => {
                let directory = directory_url(&acme.server).map_err(|err| anyhow!("{err}"))?;
                let key_path = account_key_path(&issuer.app, issuer.uid);
                let key = match self.plane.get_kv(None, &key_path).await? {
                    Some(stored) => AccountKey::from_pem(&stored.value)?,
                    None => {
                        let key = AccountKey::generate()?;
                        if let Err(err) = self
                            .plane
                            .put_kv(None, &key_path, key.to_pem(), Some(0))
                            .await
                        {
                            debug!(error = %format!("{err:#}"), "account key write raced");
                        }
                        let stored =
                            self.plane.get_kv(None, &key_path).await?.ok_or_else(|| {
                                anyhow!("kv {key_path} vanished after it was written")
                            })?;
                        AccountKey::from_pem(&stored.value)?
                    }
                };
                let client =
                    AcmeClient::connect(self.options.http.clone(), &directory, Arc::new(key))
                        .await?;
                let eab = match &acme.eab {
                    Some(eab) => {
                        let stored = self
                            .plane
                            .get_kv(Some(issuer), &eab.hmac_key.kv)
                            .await?
                            .ok_or_else(|| anyhow!("kv {} does not exist", eab.hmac_key.kv))?;
                        Some((
                            eab.kid.clone(),
                            super::acme::decode_hmac_key(&stored.value)?,
                        ))
                    }
                    None => None,
                };
                let account = client
                    .register(
                        acme.email.as_deref(),
                        eab.as_ref()
                            .map(|(kid, key)| (kid.as_str(), key.as_slice())),
                    )
                    .await?;
                info!(issuer = %issuer.name, %directory, %account, "ACME account ready");
                let thumbprint = client.thumbprint().to_string();
                let mut proofs = Vec::new();
                if let Some(dns01) = &acme.dns01 {
                    proofs.push(if dns01.cloudflare.is_some() {
                        "dns01 (cloudflare)"
                    } else {
                        "dns01 (webhook)"
                    });
                }
                if acme.http01.is_some() {
                    proofs.push("http01");
                }
                Ok(PreparedEntry {
                    spec_version: issuer.spec_version,
                    identity: format!("acme:{directory}"),
                    fields: json!({
                        "type": "acme",
                        "server": directory,
                        "accountUri": account,
                        "thumbprint": thumbprint,
                        "email": acme.email.clone().unwrap_or_default(),
                    }),
                    message: format!(
                        "registered at {directory}; proves names with {}",
                        proofs.join(" and ")
                    ),
                    prepared: Arc::new(Prepared::Acme {
                        client: Arc::new(client),
                        dns01: acme.dns01.clone(),
                        http01: acme.http01.is_some(),
                    }),
                    reason: "Registered",
                })
            }
        }
    }

    /// Reconcile an Issuer: prepare it and report.
    pub async fn reconcile_issuer(&self, object: &AppObject) -> ObjectOutcome {
        let outcome = self.issuer_outcome(object).await;
        self.post(object, &outcome);
        outcome
    }

    async fn issuer_outcome(&self, object: &AppObject) -> ObjectOutcome {
        let spec: IssuerSpec = match serde_json::from_str(&object.effective_json) {
            Ok(spec) => spec,
            Err(err) => return ObjectOutcome::rejected("InvalidSpec", err.to_string()),
        };
        if let Err(err) = spec.issuer_type() {
            return ObjectOutcome::rejected("InvalidSpec", err);
        }
        match self.prepare(object, true).await {
            Ok(entry) => ObjectOutcome::ready()
                .because(entry.reason, &entry.message)
                .with_fields(entry.fields.clone()),
            Err(err) => {
                warn!(issuer = %object.name, error = %format!("{err:#}"), "Issuer setup failed");
                ObjectOutcome::not_ready("SetupFailed", format!("{err:#}"))
                    .with_fields(
                        json!({ "type": spec.issuer_type().map(|t| t.name()).unwrap_or("") }),
                    )
                    .requeue_after(ISSUER_RETRY)
            }
        }
    }

    /// Forget what is remembered about objects that are gone (the watch's
    /// cache no longer holds them).
    pub async fn prune(&self) {
        let live: std::collections::HashSet<u64> = match self.objects.read() {
            Ok(cache) if cache.synced() => cache.objects().map(|object| object.uid).collect(),
            _ => return,
        };
        if let Ok(mut prepared) = self.prepared.lock() {
            prepared.retain(|uid, _| live.contains(uid));
        }
        if let Ok(mut preparing) = self.preparing.lock() {
            preparing.retain(|uid, _| live.contains(uid));
        }
        if let Ok(mut records) = self.records.lock() {
            records.retain(|uid, _| live.contains(uid));
        }
        if let Ok(mut board) = self.board.write() {
            board.retain(|uid, _| live.contains(uid));
        }
    }

    /// Reconcile a Certificate: issue it when it needs it, and report.
    pub async fn reconcile_certificate(&self, object: &AppObject) -> ObjectOutcome {
        let outcome = self.certificate_outcome(object).await;
        self.post(object, &outcome);
        outcome
    }

    fn record(&self, object: &AppObject) -> Record {
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        records
            .entry(object.uid)
            .or_insert_with(|| {
                let fields = object
                    .status
                    .as_ref()
                    .and_then(|status| serde_json::from_str(&status.fields_json).ok())
                    .unwrap_or(Value::Null);
                Record::from_status(&fields)
            })
            .clone()
    }

    fn keep_record(&self, uid: u64, record: Record) {
        if let Ok(mut records) = self.records.lock() {
            records.insert(uid, record);
        }
    }

    async fn certificate_outcome(&self, object: &AppObject) -> ObjectOutcome {
        let now = self.now();
        let spec: CertificateSpec = match serde_json::from_str(&object.effective_json) {
            Ok(spec) => spec,
            Err(err) => return ObjectOutcome::rejected("InvalidSpec", err.to_string()),
        };
        let desired = match spec.desired() {
            Ok(desired) => desired,
            Err(err) => return ObjectOutcome::rejected("InvalidSpec", err),
        };
        let Some(issuer) = self.issuer_named(&desired.issuer) else {
            return ObjectOutcome::not_ready(
                "IssuerNotFound",
                format!("no certs/Issuer named {}", desired.issuer),
            )
            .requeue_after(ISSUER_WAIT);
        };
        let issuer_spec: IssuerSpec = match serde_json::from_str(&issuer.effective_json) {
            Ok(spec) => spec,
            Err(err) => {
                return ObjectOutcome::not_ready(
                    "IssuerNotReady",
                    format!("issuer {}: {err}", desired.issuer),
                )
                .requeue_after(ISSUER_WAIT)
            }
        };
        if !issuer_spec.allows_namespace(&object.namespace) {
            return ObjectOutcome::rejected(
                "IssuerNotAllowed",
                format!(
                    "issuer {} serves namespaces {}, not {}",
                    desired.issuer,
                    issuer_spec.namespaces.join(", "),
                    object.namespace
                ),
            );
        }
        if let Ok(IssuerType::Acme(acme)) = issuer_spec.issuer_type() {
            if desired.has_wildcard() && acme.dns01.is_none() {
                return ObjectOutcome::rejected(
                    "WildcardNeedsDns01",
                    format!(
                        "a wildcard name can only be proven with DNS-01, and issuer {} has no dns01",
                        desired.issuer
                    ),
                );
            }
            if !desired.ip_addresses.is_empty() && acme.http01.is_none() {
                return ObjectOutcome::rejected(
                    "IpNeedsHttp01",
                    format!(
                        "an IP address can only be proven with HTTP-01, and issuer {} has no http01",
                        desired.issuer
                    ),
                );
            }
        }
        let entry = match self.prepare(&issuer, false).await {
            Ok(entry) => entry,
            Err(err) => {
                return ObjectOutcome::not_ready(
                    "IssuerNotReady",
                    format!("issuer {}: {err:#}", desired.issuer),
                )
                .requeue_after(ISSUER_WAIT)
            }
        };
        let stored = match self.plane.get_kv(Some(object), &desired.kv).await {
            Ok(stored) => stored,
            Err(err) => {
                return ObjectOutcome::not_ready("KvUnreadable", format!("{err:#}"))
                    .requeue_after(ISSUER_RETRY)
            }
        };
        let parsed = stored.as_ref().map(|stored| Bundle::parse(&stored.value));
        let current: Result<Option<&Bundle>, String> = match &parsed {
            None => Ok(None),
            Some(Ok(bundle)) => Ok(Some(bundle)),
            Some(Err(err)) => Err(format!("{err:#}")),
        };
        let mut record = self.record(object);
        let renew_request = object.labels.get(RENEW_LABEL).map(String::as_str);
        let decision = decide(
            now,
            &desired,
            &entry.identity,
            current
                .as_ref()
                .map(|bundle| *bundle)
                .map_err(String::as_str),
            &record,
            renew_request,
        );
        let usable = current
            .as_ref()
            .ok()
            .and_then(|bundle| *bundle)
            .filter(|bundle| bundle.leaf.not_after > now);
        let reason = match decision {
            Decision::Keep { renew_at } => {
                let bundle = usable.expect("kept certificates are usable");
                if let Err(err) = self.sync_parts(object, &desired, bundle).await {
                    return self.parts_failed(&desired, bundle, &record, renew_at, err);
                }
                let outcome = self.ready_outcome(&desired, bundle, &record, renew_at);
                // Look again when it is due (sooner than the resync).
                let due = Duration::from_secs((renew_at - now).max(1) as u64);
                return outcome.requeue_after(due);
            }
            Decision::Issue(reason) => reason,
        };
        let attempt = attempt_key(&desired, &entry.identity, renew_request);
        if record.failures > 0 && record.failed_for.is_none() {
            // Seeded from status after a restart: assume it is this attempt.
            record.failed_for = Some(attempt.clone());
        }
        if record.failures > 0 && record.failed_for.as_deref() == Some(attempt.as_str()) {
            if let Some(next) = record.next_attempt_at.filter(|next| *next > now) {
                self.keep_record(object.uid, record.clone());
                return self
                    .failing_outcome(&desired, usable, &record)
                    .requeue_after(Duration::from_secs((next - now) as u64));
            }
        } else if record.failed_for.as_deref() != Some(attempt.as_str()) {
            record.failures = 0;
            record.next_attempt_at = None;
        }

        info!(certificate = %object.id, issuer = %desired.issuer, %reason, "Issuing a certificate");
        let issued = self.issue(&desired, &entry.prepared, &issuer, now).await;
        let written = match issued {
            Ok(bundle) => self
                .write(
                    object,
                    &desired,
                    &bundle,
                    stored.as_ref().map(|v| v.version),
                )
                .await
                .map(|()| bundle),
            Err(err) => Err(err),
        };
        match written {
            Ok(bundle) => {
                record.issued_hash = Some(desired.issuance_hash());
                record.issuer_identity = Some(entry.identity.clone());
                record.serial = Some(bundle.leaf.serial.clone());
                record.renew_request = renew_request.map(str::to_string);
                record.revision += 1;
                record.failures = 0;
                record.last_failure = None;
                record.last_failure_at = None;
                record.next_attempt_at = None;
                record.failed_for = None;
                self.keep_record(object.uid, record.clone());
                info!(
                    certificate = %object.id,
                    serial = %bundle.leaf.serial,
                    not_after = %rfc3339(bundle.leaf.not_after),
                    "Issued a certificate"
                );
                let renew_at = bundle.leaf.renewal_time(desired.renew_before);
                if let Err(err) = self.sync_parts(object, &desired, &bundle).await {
                    return self.parts_failed(&desired, &bundle, &record, renew_at, err);
                }
                let due = Duration::from_secs((renew_at - now).max(1) as u64);
                self.ready_outcome(&desired, &bundle, &record, renew_at)
                    .requeue_after(due)
            }
            Err(err) => {
                let message = format!("{err:#}");
                warn!(certificate = %object.id, error = %message, "Issuing a certificate failed");
                record.failures += 1;
                record.last_failure = Some(message);
                record.last_failure_at = Some(now);
                let wait = backoff(record.failures);
                record.next_attempt_at = Some(now + wait.as_secs() as i64);
                record.failed_for = Some(attempt);
                self.keep_record(object.uid, record.clone());
                self.failing_outcome(&desired, usable, &record)
                    .requeue_after(wait)
            }
        }
    }

    async fn issue(
        &self,
        desired: &Desired,
        prepared: &Prepared,
        issuer: &AppObject,
        now: i64,
    ) -> R<Bundle> {
        let bundle = match prepared {
            Prepared::SelfSigned => self_signed(desired, now)?,
            Prepared::Ca { material, .. } => material.sign(desired, now)?,
            Prepared::Acme {
                client,
                dns01,
                http01,
            } => {
                let dns01 = match dns01 {
                    Some(spec) => Some((
                        self.dns_provider(spec, issuer).await?,
                        match self.options.propagation_delay {
                            Some(delay) => delay,
                            None => match &spec.propagation_delay {
                                Some(text) => parse_duration(text)?,
                                None => DEFAULT_PROPAGATION_DELAY,
                            },
                        },
                    )),
                    None => None,
                };
                let proof = Proof {
                    dns01,
                    http01: *http01,
                };
                let (key, csr_der) = csr(desired)?;
                let chain = client
                    .issue(
                        &desired.names(),
                        &csr_der,
                        &proof,
                        self.options.acme_timeout,
                    )
                    .await?;
                let bundle = acme_bundle(&chain, &key)?;
                if !bundle.leaf.names_match(desired) {
                    bail!("the CA issued a certificate for other names than the order's");
                }
                bundle
            }
        };
        Ok(bundle)
    }

    /// The DNS-01 provider an Issuer names, with its credentials read now,
    /// for the Issuer, so a rotated token is picked up on the next issuance.
    async fn dns_provider(&self, spec: &Dns01Spec, issuer: &AppObject) -> R<Arc<dyn DnsProvider>> {
        let read = |path: String| async move {
            self.plane
                .get_kv(Some(issuer), &path)
                .await?
                .map(|stored| stored.value)
                .ok_or_else(|| anyhow!("kv {path} does not exist"))
        };
        if let Some(cloudflare) = &spec.cloudflare {
            let token = read(cloudflare.api_token.kv.clone()).await?;
            return Ok(Arc::new(Cloudflare::new(
                self.options.http.clone(),
                &self.options.cloudflare_api,
                &token,
            )));
        }
        if let Some(webhook) = &spec.webhook {
            let token = match &webhook.token {
                Some(token) => Some(read(token.kv.clone()).await?),
                None => None,
            };
            return Ok(Arc::new(Webhook::new(
                self.options.http.clone(),
                &webhook.url,
                token,
            )));
        }
        bail!("acme.dns01 names no provider")
    }

    /// Write a new certificate's bundle in one put, over the version read,
    /// so a concurrent writer is not overwritten.
    async fn write(
        &self,
        object: &AppObject,
        desired: &Desired,
        bundle: &Bundle,
        read_version: Option<i64>,
    ) -> R<()> {
        crate::worker::netd::tls::validate_bundle(&bundle.to_pem())
            .context("the issued bundle would not be served")?;
        self.plane
            .put_kv(
                Some(object),
                &desired.kv,
                &bundle.to_pem(),
                Some(read_version.unwrap_or(0)),
            )
            .await
    }

    /// Make the `parts` keys hold `bundle`'s parts, key first: written when
    /// they differ, so a part someone overwrote, or one a failed write
    /// left behind, is put right on the next pass.
    async fn sync_parts(&self, object: &AppObject, desired: &Desired, bundle: &Bundle) -> R<()> {
        let parts = &desired.parts;
        let wanted = [
            (parts.key.as_ref(), bundle.key_pem.clone()),
            (parts.cert.as_ref(), bundle.chain_pem.clone()),
            (parts.ca.as_ref(), bundle.ca_pem()),
        ];
        for (key, value) in wanted {
            let Some(key) = key else {
                continue;
            };
            let current = self.plane.get_kv(Some(object), key).await?;
            if current.as_ref().map(|v| v.value.as_str()) != Some(value.as_str()) {
                self.plane.put_kv(Some(object), key, &value, None).await?;
            }
        }
        Ok(())
    }

    /// The certificate is issued, but a part could not be written: not
    /// ready (whoever reads the parts has stale ones), and tried again
    /// soon. No backoff: nothing outside Legion is asked.
    fn parts_failed(
        &self,
        desired: &Desired,
        bundle: &Bundle,
        record: &Record,
        renew_at: i64,
        err: anyhow::Error,
    ) -> ObjectOutcome {
        ObjectOutcome::not_ready(
            "PartsNotWritten",
            format!(
                "the certificate in kv {} is current, but its parts are not: {err:#}",
                desired.kv
            ),
        )
        .with_fields(self.status_fields(desired, Some(bundle), record, Some(renew_at)))
        .requeue_after(ISSUER_RETRY)
    }

    fn status_fields(
        &self,
        desired: &Desired,
        bundle: Option<&Bundle>,
        record: &Record,
        renew_at: Option<i64>,
    ) -> Value {
        let mut fields = json!({
            "issuer": desired.issuer,
            "revision": record.revision,
            "failedAttempts": record.failures,
        });
        let mut set = |key: &str, value: Option<String>| {
            if let Some(value) = value {
                fields[key] = json!(value);
            }
        };
        set("specHash", record.issued_hash.clone());
        set("issuerIdentity", record.issuer_identity.clone());
        set("serial", record.serial.clone());
        set("renewRequest", record.renew_request.clone());
        set("lastFailure", record.last_failure.clone());
        set("lastFailureTime", record.last_failure_at.map(rfc3339));
        set("nextAttemptTime", record.next_attempt_at.map(rfc3339));
        set("renewalTime", renew_at.map(rfc3339));
        if let Some(bundle) = bundle {
            fields["notBefore"] = json!(rfc3339(bundle.leaf.not_before));
            fields["notAfter"] = json!(rfc3339(bundle.leaf.not_after));
            let mut names = bundle.leaf.dns_names.clone();
            names.extend(bundle.leaf.ip_addresses.iter().map(|ip| ip.to_string()));
            fields["names"] = json!(names);
            fields["kv"] = json!(desired.kv);
        }
        fields
    }

    fn ready_outcome(
        &self,
        desired: &Desired,
        bundle: &Bundle,
        record: &Record,
        renew_at: i64,
    ) -> ObjectOutcome {
        ObjectOutcome::ready()
            .because(
                "Issued",
                format!(
                    "valid until {}; renews at {}",
                    rfc3339(bundle.leaf.not_after),
                    rfc3339(renew_at)
                ),
            )
            .with_fields(self.status_fields(desired, Some(bundle), record, Some(renew_at)))
    }

    /// A failed attempt: still `Ready` while the certificate in KV is valid
    /// (it is still served), with the failure as the reason.
    fn failing_outcome(
        &self,
        desired: &Desired,
        usable: Option<&Bundle>,
        record: &Record,
    ) -> ObjectOutcome {
        let failure = record.last_failure.clone().unwrap_or_default();
        let next = record.next_attempt_at.map(rfc3339).unwrap_or_default();
        match usable {
            Some(bundle) => ObjectOutcome::ready()
                .because(
                    "RenewalFailing",
                    format!(
                        "{} failed attempt(s), the last: {failure}; serving the current \
                         certificate until {}; next attempt at {next}",
                        record.failures,
                        rfc3339(bundle.leaf.not_after)
                    ),
                )
                .with_fields(self.status_fields(
                    desired,
                    Some(bundle),
                    record,
                    Some(bundle.leaf.renewal_time(desired.renew_before)),
                )),
            None => ObjectOutcome::not_ready(
                "IssueFailed",
                format!(
                    "{} failed attempt(s), the last: {failure}; next attempt at {next}",
                    record.failures
                ),
            )
            .with_fields(self.status_fields(desired, None, record, None)),
        }
    }
}

/// What one issuance attempt is for: a change makes a new attempt that the
/// previous attempt's backoff does not hold back.
fn attempt_key(desired: &Desired, identity: &str, renew_request: Option<&str>) -> String {
    let digest = Sha256::digest(
        format!(
            "{}|{identity}|{}",
            desired.issuance_hash(),
            renew_request.unwrap_or("")
        )
        .as_bytes(),
    );
    hex_string(&digest[..8])
}

/// KV in memory, for tests: every key readable and writable unless denied.
#[derive(Default)]
pub struct MemoryPlane {
    pub values: Mutex<HashMap<String, KvValue>>,
    /// Keys whose reads and writes are refused (`PermissionDenied`).
    pub denied: Mutex<std::collections::HashSet<String>>,
    /// Keys that read but refuse writes.
    pub read_only: Mutex<std::collections::HashSet<String>>,
}

#[async_trait::async_trait]
impl CertsPlane for MemoryPlane {
    async fn get_kv(&self, _acting: Option<&AppObject>, key: &str) -> R<Option<KvValue>> {
        if self.denied.lock().unwrap().contains(key) {
            bail!("reading kv {key}: permission denied");
        }
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    async fn put_kv(
        &self,
        _acting: Option<&AppObject>,
        key: &str,
        value: &str,
        expected_version: Option<i64>,
    ) -> R<()> {
        if self.denied.lock().unwrap().contains(key) || self.read_only.lock().unwrap().contains(key)
        {
            bail!("writing kv {key}: permission denied");
        }
        let mut values = self.values.lock().unwrap();
        let current = values.get(key).map(|v| v.version).unwrap_or(0);
        if let Some(expected) = expected_version {
            if expected != current {
                bail!("writing kv {key}: version {current}, not {expected}");
            }
        }
        values.insert(
            key.to_string(),
            KvValue {
                value: value.to_string(),
                version: current + 1,
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::certs::issue::verify_chain;
    use crate::apps::certs::spec::Usage;
    use crate::apps::certs::test_support::{FakeAcme, FakeAcmeOptions};
    use crate::structs::ObjectStatus;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct Rig {
        engine: Arc<Engine>,
        plane: Arc<MemoryPlane>,
        clock: Arc<AtomicI64>,
    }

    fn rig() -> Rig {
        let plane = Arc::new(MemoryPlane::default());
        let clock = Arc::new(AtomicI64::new(chrono::Utc::now().timestamp()));
        let tick = clock.clone();
        let engine = Engine::new(
            plane.clone(),
            EngineOptions {
                clock: Arc::new(move || tick.load(Ordering::SeqCst)),
                propagation_delay: Some(Duration::ZERO),
                acme_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        );
        Rig {
            engine,
            plane,
            clock,
        }
    }

    fn object(kind: &str, uid: u64, namespace: &str, name: &str, spec: Value) -> AppObject {
        AppObject {
            app: APP.into(),
            kind: kind.into(),
            qualified_kind: format!("{APP}/{kind}"),
            namespace: namespace.into(),
            name: name.into(),
            id: if namespace.is_empty() {
                name.to_string()
            } else {
                format!("{namespace}/{name}")
            },
            uid,
            generation: 1,
            spec_version: 1,
            spec_json: spec.to_string(),
            effective_json: spec.to_string(),
            state: "valid".into(),
            ..Default::default()
        }
    }

    impl Rig {
        fn put_object(&self, object: AppObject) {
            self.engine
                .objects
                .write()
                .unwrap()
                .apply(crate::structs::ObjectEvent {
                    upserts: vec![object],
                    ..Default::default()
                });
        }

        fn sync(&self, objects: Vec<AppObject>) {
            self.engine
                .objects
                .write()
                .unwrap()
                .apply(crate::structs::ObjectEvent {
                    snapshot: true,
                    snapshot_done: true,
                    upserts: objects,
                    ..Default::default()
                });
        }

        fn kv(&self, key: &str) -> Option<String> {
            self.plane
                .values
                .lock()
                .unwrap()
                .get(key)
                .map(|v| v.value.clone())
        }

        fn advance(&self, by: Duration) {
            self.clock.fetch_add(by.as_secs() as i64, Ordering::SeqCst);
        }
    }

    fn reason(outcome: &ObjectOutcome) -> (String, String, String) {
        let conditions = outcome.conditions();
        let main = conditions
            .iter()
            .find(|c| c.r#type == "Ready")
            .or_else(|| conditions.iter().find(|c| c.r#type == "Accepted"))
            .unwrap();
        (
            main.status.clone(),
            main.reason.clone(),
            main.message.clone(),
        )
    }

    #[test]
    fn backoff_doubles_from_five_minutes_to_six_hours_test() {
        assert_eq!(backoff(1), Duration::from_secs(300));
        assert_eq!(backoff(2), Duration::from_secs(600));
        assert_eq!(backoff(4), Duration::from_secs(2400));
        assert_eq!(backoff(20), BACKOFF_MAX);
        // Under Let's Encrypt's five failed validations per hostname per
        // hour: at most four attempts in any first hour.
        let mut at = 0u64;
        let mut attempts = 0;
        for failures in 1.. {
            if at > 3600 {
                break;
            }
            attempts += 1;
            at += backoff(failures).as_secs();
        }
        assert!(attempts <= 4, "{attempts}");
    }

    #[tokio::test]
    async fn a_ca_issuer_generates_its_ca_and_publishes_the_trust_test() -> R<()> {
        let rig = rig();
        let issuer = object(
            ISSUER_KIND,
            1,
            "",
            "internal",
            json!({ "ca": { "kv": "certs/ca/internal", "trust": "certs/ca/internal.pem" } }),
        );
        rig.sync(vec![issuer.clone()]);
        let outcome = rig.engine.reconcile_issuer(&issuer).await;
        assert_eq!(reason(&outcome).1, "Generated");
        let bundle = rig.kv("certs/ca/internal").expect("generated");
        let trust = rig.kv("certs/ca/internal.pem").expect("published");
        assert!(bundle.contains("PRIVATE KEY"));
        assert!(!trust.contains("PRIVATE KEY"), "the trust key holds no key");
        assert_eq!(outcome.fields()["generated"], true);
        // Again: loaded, not generated again.
        let mut again = issuer.clone();
        again.spec_version = 2;
        let outcome = rig.engine.reconcile_issuer(&again).await;
        assert_eq!(reason(&outcome).1, "Loaded");
        assert_eq!(rig.kv("certs/ca/internal").unwrap(), bundle);
        Ok(())
    }

    #[tokio::test]
    async fn a_certificate_is_issued_kept_renewed_and_reissued_on_request_test() -> R<()> {
        let rig = rig();
        let issuer = object(
            ISSUER_KIND,
            1,
            "",
            "internal",
            json!({ "ca": { "kv": "certs/ca", "trust": "certs/ca.pem" } }),
        );
        let mut cert = object(
            CERTIFICATE_KIND,
            2,
            "default",
            "db",
            json!({ "issuer": "internal", "kv": "certs/db", "dnsNames": ["db.internal"],
                    "duration": "720h",
                    "parts": { "cert": "certs/db.crt", "key": "certs/db.key", "ca": "certs/db-ca.crt" } }),
        );
        rig.sync(vec![issuer.clone(), cert.clone()]);

        let outcome = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(reason(&outcome).0, "true", "{outcome:?}");
        assert_eq!(reason(&outcome).1, "Issued");
        let first = Bundle::parse(&rig.kv("certs/db").unwrap())?;
        verify_chain(
            &first.chain_pem,
            &rig.kv("certs/ca.pem").unwrap(),
            "db.internal",
            Usage::Server,
        )?;
        assert_eq!(rig.kv("certs/db.key").unwrap(), first.key_pem);
        assert_eq!(rig.kv("certs/db.crt").unwrap(), first.chain_pem);
        assert_eq!(
            rig.kv("certs/db-ca.crt").unwrap(),
            rig.kv("certs/ca.pem").unwrap()
        );
        assert_eq!(outcome.fields()["revision"], 1);
        assert_eq!(outcome.fields()["names"], json!(["db.internal"]));

        // Nothing changed: kept, and the outcome is identical (no status
        // write).
        let kept = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(kept.conditions(), outcome.conditions());
        assert_eq!(kept.fields(), outcome.fields());
        assert_eq!(
            Bundle::parse(&rig.kv("certs/db").unwrap())?.leaf.serial,
            first.leaf.serial
        );

        // Two thirds through its 30 days: renewed.
        rig.advance(Duration::from_secs(21 * 86400));
        let renewed = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(reason(&renewed).1, "Issued");
        let second = Bundle::parse(&rig.kv("certs/db").unwrap())?;
        assert_ne!(second.leaf.serial, first.leaf.serial);
        assert_ne!(second.key_pem, first.key_pem, "a new key every time");
        assert_eq!(renewed.fields()["revision"], 2);

        // A renew request re-issues once.
        cert.labels
            .insert(RENEW_LABEL.into(), "2026-11-07T10:00:00Z".into());
        cert.spec_version += 1;
        rig.engine.reconcile_certificate(&cert).await;
        let third = Bundle::parse(&rig.kv("certs/db").unwrap())?;
        assert_ne!(third.leaf.serial, second.leaf.serial);
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(
            Bundle::parse(&rig.kv("certs/db").unwrap())?.leaf.serial,
            third.leaf.serial
        );
        assert_eq!(outcome.fields()["renewRequest"], "2026-11-07T10:00:00Z");

        // A name added: re-issued for the new names.
        cert.effective_json = json!({ "issuer": "internal", "kv": "certs/db",
                                      "dnsNames": ["db.internal", "db2.internal"] })
        .to_string();
        cert.spec_version += 1;
        rig.engine.reconcile_certificate(&cert).await;
        let fourth = Bundle::parse(&rig.kv("certs/db").unwrap())?;
        assert_eq!(fourth.leaf.dns_names.len(), 2);

        // Someone else's certificate in the key is replaced.
        let foreign = crate::apps::certs::issue::self_signed(
            &CertificateSpec {
                issuer: "x".into(),
                kv: "x".into(),
                dns_names: vec!["db.internal".into(), "db2.internal".into()],
                ip_addresses: vec![],
                common_name: None,
                duration: "720h".into(),
                renew_before: None,
                key_algorithm: Default::default(),
                usages: vec![Usage::Server],
                parts: None,
            }
            .desired()
            .unwrap(),
            rig.engine.now(),
        )?;
        rig.plane
            .put_kv(None, "certs/db", &foreign.to_pem(), None)
            .await?;
        rig.engine.reconcile_certificate(&cert).await;
        let fifth = Bundle::parse(&rig.kv("certs/db").unwrap())?;
        assert_ne!(fifth.leaf.serial, foreign.leaf.serial);
        Ok(())
    }

    #[tokio::test]
    async fn issuers_must_exist_and_allow_the_namespace_test() -> R<()> {
        let rig = rig();
        let cert = object(
            CERTIFICATE_KIND,
            2,
            "team-b",
            "web",
            json!({ "issuer": "internal", "kv": "certs/web", "dnsNames": ["web.test"] }),
        );
        rig.sync(vec![cert.clone()]);
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(
            reason(&outcome),
            (
                "false".into(),
                "IssuerNotFound".into(),
                "no certs/Issuer named internal".into()
            )
        );
        let issuer = object(
            ISSUER_KIND,
            1,
            "",
            "internal",
            json!({ "namespaces": ["team-a"], "selfSigned": {} }),
        );
        rig.put_object(issuer);
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        let (status, why, message) = reason(&outcome);
        assert_eq!(
            (status.as_str(), why.as_str()),
            ("false", "IssuerNotAllowed")
        );
        assert!(message.contains("team-a"), "{message}");
        assert!(rig.kv("certs/web").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_failing_issuance_backs_off_and_keeps_the_old_certificate_test() -> R<()> {
        let rig = rig();
        let issuer = object(ISSUER_KIND, 1, "", "self", json!({ "selfSigned": {} }));
        let cert = object(
            CERTIFICATE_KIND,
            2,
            "default",
            "web",
            json!({ "issuer": "self", "kv": "certs/web", "dnsNames": ["web.test"], "duration": "720h" }),
        );
        rig.sync(vec![issuer, cert.clone()]);
        rig.engine.reconcile_certificate(&cert).await;
        let good = rig.kv("certs/web").unwrap();

        // The key becomes unwritable; renewal comes due.
        rig.plane
            .read_only
            .lock()
            .unwrap()
            .insert("certs/web".into());
        rig.advance(Duration::from_secs(21 * 86400));
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        let (status, why, message) = reason(&outcome);
        assert_eq!((status.as_str(), why.as_str()), ("true", "RenewalFailing"));
        assert!(message.contains("permission denied"), "{message}");
        assert!(
            message.contains("serving the current certificate"),
            "{message}"
        );
        assert_eq!(outcome.fields()["failedAttempts"], 1);
        assert_eq!(
            rig.kv("certs/web").unwrap(),
            good,
            "the old certificate stays"
        );

        // Within the backoff nothing is attempted, and the status holds.
        rig.advance(Duration::from_secs(60));
        let held = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(held.fields(), outcome.fields());
        assert_eq!(held.conditions(), outcome.conditions());

        // After it, another attempt (the wait doubles); then fixed, issued.
        rig.advance(Duration::from_secs(300));
        let second = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(second.fields()["failedAttempts"], 2);
        rig.plane.read_only.lock().unwrap().clear();
        // A renew request is a new attempt: it does not wait out the backoff.
        let mut asked = cert.clone();
        asked.labels.insert(RENEW_LABEL.into(), "now".into());
        let fixed = rig.engine.reconcile_certificate(&asked).await;
        assert_eq!(reason(&fixed).1, "Issued");
        assert_eq!(fixed.fields()["failedAttempts"], 0);
        assert!(fixed.fields().get("lastFailure").is_none());
        assert_ne!(rig.kv("certs/web").unwrap(), good);
        Ok(())
    }

    /// Past expiry with nothing usable, a failure is not ready.
    #[tokio::test]
    async fn without_a_usable_certificate_a_failure_is_not_ready_test() -> R<()> {
        let rig = rig();
        let issuer = object(ISSUER_KIND, 1, "", "self", json!({ "selfSigned": {} }));
        let cert = object(
            CERTIFICATE_KIND,
            2,
            "default",
            "web",
            json!({ "issuer": "self", "kv": "certs/web", "dnsNames": ["web.test"] }),
        );
        rig.sync(vec![issuer, cert.clone()]);
        rig.plane
            .read_only
            .lock()
            .unwrap()
            .insert("certs/web".into());
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        let (status, why, _) = reason(&outcome);
        assert_eq!((status.as_str(), why.as_str()), ("false", "IssueFailed"));
        Ok(())
    }

    /// A part that cannot be written leaves the certificate issued, reads
    /// not ready, and is retried without the issuance backoff.
    #[tokio::test]
    async fn parts_are_kept_in_step_with_the_bundle_test() -> R<()> {
        let rig = rig();
        let issuer = object(ISSUER_KIND, 1, "", "self", json!({ "selfSigned": {} }));
        let cert = object(
            CERTIFICATE_KIND,
            2,
            "default",
            "web",
            json!({ "issuer": "self", "kv": "certs/web", "dnsNames": ["web.test"],
                    "parts": { "key": "certs/web.key", "cert": "certs/web.crt" } }),
        );
        rig.sync(vec![issuer, cert.clone()]);
        rig.plane
            .read_only
            .lock()
            .unwrap()
            .insert("certs/web.key".into());
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        let (status, why, _) = reason(&outcome);
        assert_eq!(
            (status.as_str(), why.as_str()),
            ("false", "PartsNotWritten")
        );
        assert_eq!(outcome.fields()["failedAttempts"], 0);
        let bundle = Bundle::parse(&rig.kv("certs/web").unwrap())?;
        rig.plane.read_only.lock().unwrap().clear();
        let fixed = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(reason(&fixed).1, "Issued");
        assert_eq!(rig.kv("certs/web.key").unwrap(), bundle.key_pem);
        assert_eq!(rig.kv("certs/web.crt").unwrap(), bundle.chain_pem);
        // Someone overwrites a part: put right on the next pass.
        rig.plane
            .put_kv(None, "certs/web.crt", "junk", None)
            .await?;
        rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(rig.kv("certs/web.crt").unwrap(), bundle.chain_pem);
        Ok(())
    }

    #[tokio::test]
    async fn a_restart_remembers_the_certificate_and_the_backoff_test() -> R<()> {
        let rig = rig();
        let issuer = object(ISSUER_KIND, 1, "", "self", json!({ "selfSigned": {} }));
        let mut cert = object(
            CERTIFICATE_KIND,
            2,
            "default",
            "web",
            json!({ "issuer": "self", "kv": "certs/web", "dnsNames": ["web.test"] }),
        );
        rig.sync(vec![issuer.clone(), cert.clone()]);
        let outcome = rig.engine.reconcile_certificate(&cert).await;
        let serial = Bundle::parse(&rig.kv("certs/web").unwrap())?.leaf.serial;

        // A new engine, with the same KV and the status the old one wrote.
        let restarted = Engine::new(rig.plane.clone(), rig.engine.options.clone());
        cert.status = Some(ObjectStatus {
            observed_generation: 1,
            fields_json: outcome.fields().to_string(),
            ..Default::default()
        });
        restarted
            .objects
            .write()
            .unwrap()
            .apply(crate::structs::ObjectEvent {
                snapshot: true,
                snapshot_done: true,
                upserts: vec![issuer, cert.clone()],
                ..Default::default()
            });
        let again = restarted.reconcile_certificate(&cert).await;
        assert_eq!(reason(&again).1, "Issued");
        assert_eq!(
            Bundle::parse(&rig.kv("certs/web").unwrap())?.leaf.serial,
            serial,
            "not re-issued after a restart"
        );
        Ok(())
    }

    /// An ACME Issuer and a Certificate end to end through the engine:
    /// account key in the app's own KV, DNS-01 through the webhook, the
    /// thumbprint in the Issuer's status.
    #[tokio::test]
    async fn acme_issuers_register_and_issue_test() -> R<()> {
        let ca = FakeAcme::start(FakeAcmeOptions::default()).await?;
        let hook = ca.dns_webhook().await?;
        let rig = rig();
        let issuer = object(
            ISSUER_KIND,
            7,
            "",
            "le",
            json!({ "acme": { "server": ca.directory_url(), "email": "ops@example.com",
                              "dns01": { "webhook": { "url": hook } } } }),
        );
        let cert = object(
            CERTIFICATE_KIND,
            8,
            "default",
            "shop",
            json!({ "issuer": "le", "kv": "certs/shop", "dnsNames": ["shop.example.com", "*.shop.example.com"] }),
        );
        rig.sync(vec![issuer.clone(), cert.clone()]);
        let outcome = rig.engine.reconcile_issuer(&issuer).await;
        assert_eq!(reason(&outcome).1, "Registered", "{outcome:?}");
        let thumbprint = outcome.fields()["thumbprint"].as_str().unwrap().to_string();
        let key = rig.kv("legion/apps/certs/acme/7.key").expect("account key");
        assert_eq!(AccountKey::from_pem(&key)?.thumbprint(), thumbprint);

        let outcome = rig.engine.reconcile_certificate(&cert).await;
        assert_eq!(reason(&outcome).1, "Issued", "{outcome:?}");
        let bundle = Bundle::parse(&rig.kv("certs/shop").unwrap())?;
        verify_chain(
            &bundle.chain_pem,
            &ca.root_pem(),
            "x.shop.example.com",
            Usage::Server,
        )?;
        assert_eq!(ca.orders(), 1);
        assert_eq!(ca.accounts(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn acme_rejects_what_its_challenges_cannot_prove_test() -> R<()> {
        let rig = rig();
        let issuer = object(
            ISSUER_KIND,
            1,
            "",
            "le",
            json!({ "acme": { "server": "https://ca.invalid/directory", "http01": {} } }),
        );
        let wildcard = object(
            CERTIFICATE_KIND,
            2,
            "default",
            "w",
            json!({ "issuer": "le", "kv": "certs/w", "dnsNames": ["*.example.com"] }),
        );
        rig.sync(vec![issuer, wildcard.clone()]);
        let (status, why, _) = reason(&rig.engine.reconcile_certificate(&wildcard).await);
        assert_eq!(
            (status.as_str(), why.as_str()),
            ("false", "WildcardNeedsDns01")
        );
        Ok(())
    }

    #[test]
    fn decide_names_why_it_issues_test() {
        let desired = serde_json::from_value::<CertificateSpec>(json!({
            "issuer": "i", "kv": "certs/x", "dnsNames": ["a.test"],
        }))
        .unwrap()
        .desired()
        .unwrap();
        let record = Record::default();
        assert_eq!(
            decide(0, &desired, "id", Ok(None), &record, None),
            Decision::Issue("kv certs/x holds no certificate yet".into())
        );
        assert!(matches!(
            decide(0, &desired, "id", Err("not PEM"), &record, None),
            Decision::Issue(reason) if reason.contains("not PEM")
        ));
    }
}
