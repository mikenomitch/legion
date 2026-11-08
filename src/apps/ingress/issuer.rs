//! The issuer: keeps an ACME certificate for every routed public host
//! (docs/ingress.md, "Automatic certificates"; ingress plan §6.5).
//!
//! Every replica runs [`Issuer::pass`] on a timer and whenever routes
//! change, but only the holder of [`LOCK_KEY`] acts, so the cluster orders
//! each certificate once however many replicas run. A pass orders what is
//! missing or due for renewal, one host at a time, skipping hosts an
//! operator bundle covers and hosts still in their failure backoff. The
//! account, the bundles, and the HTTP-01 answers live in the app's KV, so a
//! new holder picks up where the last one stopped and every replica can
//! answer the CA.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use super::acme::{AcmeClient, ChallengeBoard};
use super::certs::{parse_bundle, CertSource, ACME_PREFIX, KV_PREFIX};
use super::control::Control;
use super::State;
use crate::prelude::*;

/// The lock whose holder orders certificates.
pub const LOCK_KEY: &str = "legion/apps/ingress/acme/lock";
/// Its lease: long enough to cover one order, renewed before each.
pub const LOCK_TTL: Duration = Duration::from_secs(180);
/// HTTP-01 answers, by token.
pub const CHALLENGE_PREFIX: &str = "legion/apps/ingress/acme/challenges/";
/// What the issuer last did, for every replica's views.
pub const STATUS_KEY: &str = "legion/apps/ingress/acme/status";
/// The first wait after a failed order; it doubles per failure.
pub const BACKOFF_FIRST: Duration = Duration::from_secs(5 * 60);
/// The longest wait between attempts for one host.
pub const BACKOFF_MAX: Duration = Duration::from_secs(6 * 60 * 60);

/// The account kept for one directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredAccount {
    pub directory: String,
    /// The P-256 key, PKCS#8, base64.
    pub key: String,
    pub url: String,
}

/// Where the account for `directory` is kept: one per directory, so moving
/// to staging and back keeps both.
pub fn account_key(directory: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, directory.as_bytes());
    let short: String = digest.as_ref()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{KV_PREFIX}acme/accounts/{short}")
}

/// One host's issuing history, as the views show it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostStatus {
    /// Consecutive failures.
    #[serde(default)]
    pub failures: u32,
    /// The last failure's reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Unix seconds before which the host is not tried again.
    #[serde(default)]
    pub next_attempt: i64,
    /// Unix seconds of the last certificate ordered.
    #[serde(default)]
    pub issued_at: i64,
}

/// What the issuer writes to [`STATUS_KEY`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IssuerStatus {
    /// The replica holding the lock.
    #[serde(default)]
    pub holder: String,
    #[serde(default)]
    pub directory: String,
    /// Unix seconds of the last pass.
    #[serde(default)]
    pub checked_at: i64,
    /// An error that stopped the whole pass (the CA unreachable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub hosts: BTreeMap<String, HostStatus>,
}

/// The wait after `failures` consecutive failures.
pub fn backoff(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    BACKOFF_FIRST
        .saturating_mul(1 << doublings)
        .min(BACKOFF_MAX)
}

/// HTTP-01 answers on the shared board: in KV for every replica, and in
/// this replica's memory so its own answers need no read.
pub struct KvBoard {
    pub control: Arc<dyn Control>,
    pub state: Arc<State>,
}

#[async_trait::async_trait]
impl ChallengeBoard for KvBoard {
    async fn put(&self, token: &str, key_authorization: &str) -> R<()> {
        self.state.publish_challenge(token, key_authorization);
        self.control
            .kv_put(&format!("{CHALLENGE_PREFIX}{token}"), key_authorization)
            .await
    }

    async fn remove(&self, token: &str) {
        self.state.withdraw_challenge(token);
        if let Err(err) = self
            .control
            .kv_delete(&format!("{CHALLENGE_PREFIX}{token}"))
            .await
        {
            debug!(token, "Removing an HTTP-01 answer: {err:#}");
        }
    }
}

/// The issuer of one replica.
pub struct Issuer {
    control: Arc<dyn Control>,
    state: Arc<State>,
    /// This replica's lock id and owner label.
    lock_id: String,
    owner: String,
    holding: bool,
    client: Option<AcmeClient>,
    status: IssuerStatus,
    /// What was last written to [`STATUS_KEY`].
    written: Option<IssuerStatus>,
}

impl Issuer {
    pub fn new(control: Arc<dyn Control>, state: Arc<State>, owner: String) -> Issuer {
        Issuer {
            control,
            state,
            lock_id: format!("{:032x}", rand::random::<u128>()),
            owner,
            holding: false,
            client: None,
            status: IssuerStatus::default(),
            written: None,
        }
    }

    /// Whether this replica held the lock at its last pass.
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
                    info!(owner = %self.owner, "This replica orders the ingress's certificates");
                }
                self.holding = held;
            }
            Err(err) => {
                debug!("Renewing the issuer lock: {err:#}");
                self.holding = false;
            }
        }
        self.holding
    }

    /// One pass: take the lock, then order what is missing or due.
    pub async fn pass(&mut self) {
        let config = self.state.config.acme.clone();
        if !config.enabled || !self.renew().await {
            return;
        }
        let now = chrono::Utc::now().timestamp();
        let hosts = self.state.table().issuable_hosts();
        self.status.holder = self.owner.clone();
        self.status.directory = config.directory.clone();
        self.status.checked_at = now;
        self.status
            .hosts
            .retain(|host, _| hosts.binary_search(host).is_ok());
        let due: Vec<String> = hosts
            .into_iter()
            .filter(|host| !self.state.certs.operator_covers(host, now))
            .filter(|host| {
                self.state
                    .certs
                    .ordered(host)
                    .is_none_or(|cert| cert.renewal_due(now))
            })
            .filter(|host| {
                self.status
                    .hosts
                    .get(host)
                    .is_none_or(|status| status.next_attempt <= now)
            })
            .collect();
        if !due.is_empty() {
            match self.client().await {
                Ok(()) => {
                    self.status.error = None;
                    for host in due {
                        if self.state.stopping() || !self.renew().await {
                            break;
                        }
                        self.order(&host).await;
                    }
                }
                Err(err) => {
                    warn!("The ingress cannot reach its ACME CA: {err:#}");
                    self.status.error = Some(format!("{err:#}"));
                }
            }
        }
        self.write_status().await;
    }

    async fn order(&mut self, host: &str) {
        let Some(client) = self.client.as_ref() else {
            return;
        };
        let board = KvBoard {
            control: self.control.clone(),
            state: self.state.clone(),
        };
        let now = chrono::Utc::now().timestamp();
        info!(host, "Ordering a certificate");
        let outcome = async {
            let issued = client.issue(host, &board).await?;
            let bundle = issued.bundle();
            let key = format!("{ACME_PREFIX}{host}");
            let cert = parse_bundle(&key, 0, CertSource::Acme, &bundle)
                .context("the CA's certificate does not load")?;
            self.control
                .kv_put(&key, &bundle)
                .await
                .context("storing the certificate")?;
            Ok::<_, anyhow::Error>(cert)
        }
        .await;
        let status = self.status.hosts.entry(host.to_string()).or_default();
        match outcome {
            Ok(cert) => {
                info!(host, not_after = cert.not_after, "Certificate ordered");
                // Served here now; the other replicas load it on their next
                // sync (version 0 makes this one reload too).
                self.state.certs.install(cert, now);
                *status = HostStatus {
                    issued_at: now,
                    ..HostStatus::default()
                };
            }
            Err(err) => {
                status.failures += 1;
                let wait = backoff(status.failures);
                status.error = Some(format!("{err:#}"));
                status.next_attempt = now + wait.as_secs() as i64;
                warn!(
                    host,
                    failures = status.failures,
                    retry_in_s = wait.as_secs(),
                    "Ordering a certificate failed: {err:#}"
                );
            }
        }
    }

    /// The ACME client, connected and registered, kept across passes.
    async fn client(&mut self) -> R<()> {
        let config = &self.state.config.acme;
        if self.client.is_some() {
            return Ok(());
        }
        let http = super::acme::http_client(config.trust_roots.as_deref())?;
        let key_name = account_key(&config.directory);
        let stored = match self.control.kv_get(&key_name).await? {
            Some((value, _)) => serde_json::from_str::<StoredAccount>(&value)
                .ok()
                .filter(|account| account.directory == config.directory),
            None => None,
        };
        let client = match stored {
            Some(account) => {
                let key = base64::engine::general_purpose::STANDARD
                    .decode(account.key.as_bytes())
                    .context("the stored ACME account key is not base64")?;
                let mut client = AcmeClient::connect(http, &config.directory, &key).await?;
                client.set_account(account.url);
                client
            }
            None => {
                let key = AcmeClient::generate_key()?;
                let mut client = AcmeClient::connect(http, &config.directory, &key).await?;
                let url = client
                    .register(config.email.as_deref())
                    .await
                    .context("registering the ACME account")?;
                let account = StoredAccount {
                    directory: config.directory.clone(),
                    key: base64::engine::general_purpose::STANDARD.encode(&key),
                    url: url.clone(),
                };
                self.control
                    .kv_put(&key_name, &serde_json::to_string(&account)?)
                    .await
                    .context("storing the ACME account")?;
                info!(account = %url, directory = %config.directory, "Registered the ingress's ACME account");
                client
            }
        };
        self.client = Some(client);
        Ok(())
    }

    async fn write_status(&mut self) {
        if self.written.as_ref() == Some(&self.status) {
            return;
        }
        let Ok(text) = serde_json::to_string(&self.status) else {
            return;
        };
        match self.control.kv_put(STATUS_KEY, &text).await {
            Ok(()) => {
                self.state.set_issuer_status(self.status.clone());
                self.written = Some(self.status.clone());
            }
            Err(err) => debug!("Writing the issuer status: {err:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_five_minutes_to_six_hours() {
        assert_eq!(backoff(1), Duration::from_secs(300));
        assert_eq!(backoff(2), Duration::from_secs(600));
        assert_eq!(backoff(3), Duration::from_secs(1200));
        assert_eq!(backoff(10), BACKOFF_MAX);
        assert_eq!(backoff(1000), BACKOFF_MAX);
    }

    #[test]
    fn each_directory_keeps_its_own_account() {
        let production = account_key(super::super::acme::LETS_ENCRYPT);
        let staging = account_key(super::super::acme::LETS_ENCRYPT_STAGING);
        assert_ne!(production, staging);
        assert!(production.starts_with("legion/apps/ingress/acme/accounts/"));
        assert_eq!(production, account_key(super::super::acme::LETS_ENCRYPT));
    }
}
