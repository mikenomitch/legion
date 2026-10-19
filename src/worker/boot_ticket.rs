//! Offline boot-ticket verification on the worker (edge boot plan §6.4).
//!
//! A boot ticket is a short-lived JWT signed by the cluster's
//! workload-identity signer. This module is the *consumer* half: a JWKS cache
//! plus the seven checks a node runs before it will start anything on a
//! ticket's authority. Every one of them is local — no manager is contacted on
//! the fast path, which is the entire point of the capability.
//!
//! ```text
//! 1. signature against a cached JWKS (refreshed on a `kid` miss and on TTL;
//!    a fetch failure never invalidates cached keys)
//! 2. iss, aud == legion:node, exp/nbf with bounded clock skew
//! 3. legion.io/pool against this node's own pool
//! 4. legion.io/gate intersected with the task's speculativeStart.mode —
//!    the LOWER of the two always wins
//! 5. jti against a local single-use set (bounded, expires with the ticket)
//! 6. the local claim table — one live claim per instance id per node
//! 7. the local resource ledger — the node must genuinely have room
//! ```
//!
//! Checks 1-5 live here. Checks 6 and 7 belong to
//! [`crate::worker::claims`], which owns the state they read.
//!
//! **Replay across nodes is bounded by ratification, not by verification.**
//! Two nodes presented the same ticket both boot; the second is refused with
//! `InstanceClaimed` and kills its work. Globally-single-use is enforced at
//! the manager. Nodes are honest, but they are not trusted to be the only
//! holder.

use crate::prelude::*;
use crate::structs::edge_boot::{BootTicketClaims, BOOT_TICKET_AUDIENCE};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Map;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a fetched JWKS is trusted before a proactive refresh. Mirrors
/// `edgeBoot.jwksRefresh`.
pub const DEFAULT_JWKS_REFRESH: Duration = Duration::from_secs(300);

/// Tolerance for clock disagreement between the minting manager and the
/// redeeming node. Mirrors `edgeBoot.clockSkew`.
pub const DEFAULT_CLOCK_SKEW: Duration = Duration::from_secs(30);

/// Where the verifier gets public keys. Behind a trait so the verification
/// logic is testable without an HTTP server — the thing worth testing is the
/// decision, not the transport.
pub trait JwksSource: Send + Sync {
    /// Fetch the current JWKS document (`{"keys": [...]}`).
    fn fetch(&self) -> R<Value>;
}

/// Fetches the JWKS from a manager over TLS that verifies the cluster CA
/// (worker-plane-auth plan §6.1, §6.8): the connection names
/// `manager.legion`, and the certificate must chain to the pinned CA, or to
/// the bundle once the worker holds one. A ticket verifier is only as strong
/// as its keys, so a JWKS an on-path attacker could swap would let it forge
/// boot and exec tickets.
///
/// Every manager serves the same JWKS (the signing keys are replicated), so
/// the source tries the managers the node knows in failover order and takes
/// the first answer: a node keeps refreshing keys while the manager it
/// joined through is down. The managers share the cluster CA, so the one
/// pinned verifier covers all of them.
pub struct PinnedJwksSource {
    seed: String,
    /// The managers to try, in order. `None` asks only `seed`.
    managers: Option<Box<dyn Fn() -> Vec<String> + Send + Sync>>,
    verifier: Arc<crate::cluster_tls::PinnedManagerVerifier>,
    timeout: Duration,
}

impl PinnedJwksSource {
    pub fn new(
        manager_addr: &str,
        verifier: Arc<crate::cluster_tls::PinnedManagerVerifier>,
        timeout: Duration,
    ) -> R<Self> {
        crate::cluster_tls::host_port(manager_addr)?;
        Ok(PinnedJwksSource {
            seed: manager_addr.to_string(),
            managers: None,
            verifier,
            timeout,
        })
    }

    /// Try the managers `managers` returns (active first) before the seed.
    /// `timeout` then bounds each attempt rather than the whole fetch.
    pub fn with_managers(
        mut self,
        managers: impl Fn() -> Vec<String> + Send + Sync + 'static,
    ) -> Self {
        self.managers = Some(Box::new(managers));
        self
    }

    /// The managers to ask, in order, without repeats; the seed always last
    /// at the latest.
    fn candidates(&self) -> Vec<String> {
        let mut candidates: Vec<String> = Vec::new();
        let listed = self
            .managers
            .as_ref()
            .map(|list| list())
            .unwrap_or_default();
        for addr in listed.into_iter().chain([self.seed.clone()]) {
            if !addr.trim().is_empty() && !candidates.contains(&addr) {
                candidates.push(addr);
            }
        }
        candidates
    }

    async fn fetch_async(&self) -> R<Value> {
        let mut last_err = None;
        for addr in self.candidates() {
            let attempt = tokio::time::timeout(self.timeout, self.fetch_from(&addr)).await;
            match attempt {
                Ok(Ok(document)) => return Ok(document),
                Ok(Err(err)) => {
                    debug!(manager = %addr, error = ?err, "JWKS fetch failed; trying the next manager");
                    last_err = Some(err);
                }
                Err(_) => {
                    debug!(manager = %addr, "JWKS fetch timed out; trying the next manager");
                    last_err = Some(anyhow!("JWKS fetch from {addr} timed out"));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no manager to fetch the JWKS from")))
    }

    async fn fetch_from(&self, manager_addr: &str) -> R<Value> {
        let host_port = crate::cluster_tls::host_port(manager_addr)?;
        use http_body_util::BodyExt;
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(self.verifier.clone())
        .with_no_client_auth();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let server_name = rustls_pki_types::ServerName::try_from(
            crate::cluster_tls::MANAGER_SERVER_NAME.to_string(),
        )?;
        let tcp = tokio::net::TcpStream::connect(&host_port)
            .await
            .with_context(|| format!("connect to {host_port} for the JWKS"))?;
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(server_name, tcp)
            .await
            .with_context(|| format!("TLS to {host_port} for the JWKS"))?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls)).await?;
        tokio::spawn(connection);
        let request = http::Request::get(crate::manager::workload_identity::DEFAULT_JWKS_PATH)
            .header(http::header::HOST, crate::cluster_tls::MANAGER_SERVER_NAME)
            .body(http_body_util::Empty::<bytes::Bytes>::new())?;
        let response = sender.send_request(request).await?;
        if !response.status().is_success() {
            bail!("JWKS fetch from {host_port} returned {}", response.status());
        }
        let body = response.into_body().collect().await?.to_bytes();
        serde_json::from_slice(&body).context("invalid JWKS document")
    }
}

impl JwksSource for PinnedJwksSource {
    /// Fetches on a thread of its own: verifiers run inside RPC handlers,
    /// on the server's runtime, where blocking on another runtime panics.
    /// A fetch happens once per refresh interval or unknown `kid`, so the
    /// thread is cheap next to what it guards.
    fn fetch(&self) -> R<Value> {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .context("build JWKS fetch runtime")?;
                    runtime.block_on(self.fetch_async())
                })
                .join()
                .map_err(|_| anyhow!("JWKS fetch thread panicked"))?
        })
    }
}

/// The `iss` a node expects on every ticket: the cluster's workload-identity
/// issuer as the managers report it (`WorkerPreRegisterReply` and every
/// `HeartbeatReply.workload_identity_issuer`).
///
/// Learned rather than assumed: the address in a node's join token names one
/// manager, while tickets come from whichever manager minted them and carry
/// the one cluster-wide issuer (`workload_identity::default_issuer`, or the
/// operator's `workloadIdentity.issuer`). Assuming `iss` was the join address
/// refused every ticket once the cluster's issuer was anything else.
#[derive(Debug, Default)]
pub struct ExpectedIssuer(std::sync::RwLock<String>);

impl ExpectedIssuer {
    pub fn new(issuer: impl Into<String>) -> Self {
        ExpectedIssuer(std::sync::RwLock::new(issuer.into()))
    }

    pub fn get(&self) -> String {
        self.0.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Adopt the issuer a manager reported. Empty (workload identity off on
    /// that manager) keeps the current value. True when it changed.
    pub fn learn(&self, reported: &str) -> bool {
        let reported = reported.trim().trim_end_matches('/');
        if reported.is_empty() {
            return false;
        }
        let mut current = self.0.write().unwrap_or_else(|p| p.into_inner());
        if *current == reported {
            return false;
        }
        *current = reported.to_string();
        true
    }
}

/// This worker process's expected issuer, fed by pre-registration and every
/// heartbeat and followed by its ticket verifier.
pub fn cluster_issuer() -> &'static Arc<ExpectedIssuer> {
    static CLUSTER_ISSUER: std::sync::OnceLock<Arc<ExpectedIssuer>> = std::sync::OnceLock::new();
    CLUSTER_ISSUER.get_or_init(|| Arc::new(ExpectedIssuer::default()))
}

/// One cached public key.
struct CachedKey {
    decoding: DecodingKey,
}

/// A JWKS cache plus the offline checks a ticket must pass.
pub struct BootTicketVerifier {
    source: Box<dyn JwksSource>,
    issuer: Arc<ExpectedIssuer>,
    /// This node's pool, from its own registration. A ticket is checked
    /// against this, never against anything the caller supplies.
    pool: String,
    refresh_after: Duration,
    clock_skew: Duration,
    keys: Mutex<KeyCache>,
    /// Spent `jti`s, with the expiry that lets them be forgotten. Bounded by
    /// ticket lifetime rather than by count: a ticket that has expired can
    /// never be replayed, so its entry has no further job.
    spent: Mutex<HashMap<String, i64>>,
    /// Session tickets already verified, by a SHA-256 of their bytes.
    verified_sessions: Mutex<HashMap<[u8; 32], crate::structs::edge_boot::SessionTicketClaims>>,
}

/// Distinct verified session tickets a node remembers at once.
const VERIFIED_SESSION_TICKETS_MAX: usize = 4_096;

#[derive(Default)]
struct KeyCache {
    keys: HashMap<String, CachedKey>,
    fetched_at: Option<Instant>,
}

impl BootTicketVerifier {
    pub fn new(
        source: Box<dyn JwksSource>,
        issuer: impl Into<String>,
        pool: impl Into<String>,
        refresh_after: Duration,
        clock_skew: Duration,
    ) -> Self {
        BootTicketVerifier {
            source,
            issuer: Arc::new(ExpectedIssuer::new(issuer)),
            pool: pool.into(),
            refresh_after,
            clock_skew,
            keys: Mutex::new(KeyCache::default()),
            spent: Mutex::new(HashMap::new()),
            verified_sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Check `iss` against `issuer`, which the caller keeps current, instead
    /// of the fixed value given to [`Self::new`].
    pub fn following(mut self, issuer: Arc<ExpectedIssuer>) -> Self {
        self.issuer = issuer;
        self
    }

    /// Verify a ticket and return its Legion claims.
    ///
    /// `instance_id` is the instance the caller asked to start; it is checked
    /// against the ticket's narrowing here so a caller cannot present a valid
    /// ticket for one sandbox and start another.
    ///
    /// `now_unix` is passed in rather than read, so the skew boundaries are
    /// testable without sleeping.
    pub fn verify(&self, token: &str, instance_id: &str, now_unix: i64) -> R<BootTicketClaims> {
        let header = decode_header(token).context("invalid boot ticket header")?;
        let kid = header.kid.context("boot ticket is missing a kid header")?;

        let raw = self.decode_with_kid(token, &kid, BOOT_TICKET_AUDIENCE)?;
        let not_before = raw.get("nbf").and_then(Value::as_i64);
        let claims = BootTicketClaims::from_claims(&raw)?;

        // The ticket must be live *at this node's clock*, with bounded
        // tolerance both ways.
        let skew = self.clock_skew.as_secs() as i64;
        if now_unix - skew > claims.expires_at_unix {
            bail!(
                "boot ticket expired at {} (node clock {now_unix}, skew {skew}s)",
                claims.expires_at_unix
            );
        }
        if let Some(not_before) = not_before {
            if now_unix + skew < not_before {
                bail!(
                    "boot ticket is not valid until {not_before} \
                     (node clock {now_unix}, skew {skew}s)"
                );
            }
        }

        claims.authorizes(instance_id, &self.pool)?;
        self.check_unspent(&claims)?;
        Ok(claims)
    }

    /// Verify a session ticket (session durability plan §6.4): signature,
    /// issuer, audience, lifetime with this node's skew, scope, and this
    /// node's pool. Never spent: a session ticket is good for any number of
    /// sessions until it expires.
    ///
    /// A verified ticket is cached by its exact bytes until it expires, so a
    /// node taking thousands of starts a second on one ticket verifies its
    /// signature once.
    pub fn verify_session(
        &self,
        token: &str,
        now_unix: i64,
    ) -> R<crate::structs::edge_boot::SessionTicketClaims> {
        let skew = self.clock_skew.as_secs() as i64;
        let cache_key = {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(token.as_bytes());
            let mut key = [0u8; 32];
            key.copy_from_slice(&digest);
            key
        };
        if let Some(claims) = self
            .verified_sessions
            .lock()
            .expect("verified session tickets")
            .get(&cache_key)
            .cloned()
        {
            if now_unix - skew > claims.expires_at_unix {
                bail!(
                    "session ticket expired at {} (node clock {now_unix}, skew {skew}s)",
                    claims.expires_at_unix
                );
            }
            return Ok(claims);
        }
        let raw = self.decode(token, BOOT_TICKET_AUDIENCE)?;
        let claims = crate::structs::edge_boot::SessionTicketClaims::from_claims(&raw)?;
        if now_unix - skew > claims.expires_at_unix {
            bail!(
                "session ticket expired at {} (node clock {now_unix}, skew {skew}s)",
                claims.expires_at_unix
            );
        }
        if let Some(not_before) = raw.get("nbf").and_then(Value::as_i64) {
            if now_unix + skew < not_before {
                bail!("session ticket is not valid until {not_before} (node clock {now_unix})");
            }
        }
        if claims.pool != self.pool {
            bail!(
                "session ticket is scoped to pool {} but this node is in pool {}",
                claims.pool,
                self.pool
            );
        }
        let mut cache = self
            .verified_sessions
            .lock()
            .expect("verified session tickets");
        // Bounded by expiry, and by count against a flood of distinct
        // tickets: past the cap the expired go first, then everything.
        if cache.len() >= VERIFIED_SESSION_TICKETS_MAX {
            cache.retain(|_, cached| cached.expires_at_unix + skew >= now_unix);
            if cache.len() >= VERIFIED_SESSION_TICKETS_MAX {
                cache.clear();
            }
        }
        cache.insert(cache_key, claims.clone());
        Ok(claims)
    }

    /// Decode against the cached key for `kid`, refreshing the JWKS once on a
    /// miss (a rotation the node has not seen yet) and once on TTL.
    ///
    /// A fetch failure never invalidates cached keys: a node that cannot
    /// reach a manager must still be able to redeem tickets signed by keys it
    /// already holds, which is the availability property the whole feature
    /// exists for.
    fn decode_with_kid(&self, token: &str, kid: &str, audience: &str) -> R<Map<String, Value>> {
        if self.refresh_needed(kid) {
            match self.source.fetch() {
                Ok(document) => self.install(&document)?,
                Err(err) => {
                    warn!(error = ?err, "JWKS refresh failed; keeping cached keys");
                }
            }
        }

        let issuer = self.issuer.get();
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[&issuer]);
        validation.set_audience(&[audience]);
        // Expiry is checked against the clock the CALLER passed in, not the
        // process clock, so the skew boundaries are exactly the ones
        // documented and are testable without sleeping. The library's own
        // exp/nbf validation is therefore switched off here and reimplemented
        // in `verify` — leaving both on would apply two different clocks to
        // the same token.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.leeway = self.clock_skew.as_secs();

        let guard = self.keys.lock().expect("jwks cache");
        let key = guard
            .keys
            .get(kid)
            .with_context(|| format!("no cached signing key for kid {kid}"))?;
        let data = decode::<Map<String, Value>>(token, &key.decoding, &validation)
            .with_context(|| format!("ticket failed verification (expected issuer {issuer:?})"))?;
        Ok(data.claims)
    }

    /// Verifies a ticket's signature, issuer, and `audience` against the
    /// cached JWKS and returns its raw claims, with expiry left to the
    /// caller (it checks against its own clock with this verifier's skew).
    /// The exec ticket verifier shares this cache: one JWKS, one refresh
    /// schedule, per worker.
    pub fn decode(&self, token: &str, audience: &str) -> R<Map<String, Value>> {
        let header = decode_header(token).context("invalid ticket header")?;
        let kid = header.kid.context("ticket is missing a kid header")?;
        self.decode_with_kid(token, &kid, audience)
    }

    /// The clock skew tickets are checked with.
    pub fn clock_skew(&self) -> Duration {
        self.clock_skew
    }

    fn refresh_needed(&self, kid: &str) -> bool {
        let guard = self.keys.lock().expect("jwks cache");
        match guard.fetched_at {
            None => true,
            Some(fetched_at) => {
                !guard.keys.contains_key(kid) || fetched_at.elapsed() >= self.refresh_after
            }
        }
    }

    /// Replace the cached keys from a JWKS document.
    pub fn install(&self, document: &Value) -> R<()> {
        let entries = document
            .get("keys")
            .and_then(Value::as_array)
            .context("JWKS document has no `keys` array")?;
        let mut keys = HashMap::new();
        for entry in entries {
            let (Some(kid), Some(x), Some(y)) = (
                entry.get("kid").and_then(Value::as_str),
                entry.get("x").and_then(Value::as_str),
                entry.get("y").and_then(Value::as_str),
            ) else {
                continue;
            };
            match DecodingKey::from_ec_components(x, y) {
                Ok(decoding) => {
                    keys.insert(kid.to_string(), CachedKey { decoding });
                }
                Err(err) => warn!(kid, error = ?err, "Skipping unusable JWKS entry"),
            }
        }
        if keys.is_empty() {
            bail!("JWKS document contained no usable ES256 keys");
        }
        let mut guard = self.keys.lock().expect("jwks cache");
        guard.keys = keys;
        guard.fetched_at = Some(Instant::now());
        Ok(())
    }

    /// Node-local single-use enforcement. Global single use is the manager's
    /// job (it spends the `jti` through the idempotency table at
    /// ratification); this stops the cheapest replay — the same ticket
    /// presented twice to the same node — without a round trip.
    fn check_unspent(&self, claims: &BootTicketClaims) -> R<()> {
        if claims.uses != 1 {
            return Ok(());
        }
        let guard = self.spent.lock().expect("spent tickets");
        if guard.contains_key(&claims.jti) {
            bail!("boot ticket {} has already been redeemed", claims.jti);
        }
        Ok(())
    }

    /// Mark a single-use ticket spent, and forget any entry whose ticket has
    /// expired (an expired ticket can never be replayed, so its entry has no
    /// remaining job — this is what keeps the set bounded without a count
    /// cap).
    pub fn spend(&self, claims: &BootTicketClaims, now_unix: i64) {
        let mut guard = self.spent.lock().expect("spent tickets");
        guard.retain(|_, expires_at| *expires_at > now_unix);
        // An already-expired ticket is unusable on its own, so recording it
        // would only grow the set with entries that protect nothing.
        if claims.uses == 1 && claims.expires_at_unix > now_unix {
            guard.insert(claims.jti.clone(), claims.expires_at_unix);
        }
    }

    /// Release a `jti` reserved by a start that never happened (admission
    /// failed after verification), so an operator's retry is not wasted.
    pub fn unspend(&self, jti: &str) {
        self.spent.lock().expect("spent tickets").remove(jti);
    }

    pub fn pool(&self) -> &str {
        &self.pool
    }
}

#[cfg(test)]
mod boot_ticket_test {
    use super::*;
    use crate::manager::workload_identity::Issuer;
    use crate::structs::edge_boot::Gate;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const ISSUER: &str = "https://manager.example.com:4747";

    struct StaticJwks {
        document: Value,
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl JwksSource for StaticJwks {
        fn fetch(&self) -> R<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                bail!("network is down");
            }
            Ok(self.document.clone())
        }
    }

    fn issuer_with_key() -> (Issuer, Value) {
        let issuer = Issuer::new(
            ISSUER,
            "/.well-known/jwks.json",
            Duration::from_secs(3600),
            None,
            std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000),
        )
        .expect("issuer");
        let jwks = issuer.jwks();
        (issuer, jwks)
    }

    fn ticket(issuer: &Issuer, overrides: Value) -> String {
        let mut body = json!({
            "legion.io/scope": "boot",
            "legion.io/job": "agent-sandboxes",
            "legion.io/job_config": "jc-7f3a91",
            "legion.io/task": "workspace",
            "legion.io/instance": "user-42",
            "legion.io/pool": "edge-eu",
            "legion.io/gate": "boot",
            "legion.io/uses": 1,
        });
        let map = body.as_object_mut().expect("object");
        for (key, value) in overrides.as_object().expect("object") {
            map.insert(key.clone(), value.clone());
        }
        issuer
            .issue_boot_ticket(
                "tkt_test",
                "boot:agent-sandboxes/workspace",
                map.clone(),
                1_000_000,
                1_000_300,
            )
            .expect("mint")
            .token
    }

    fn verifier(jwks: Value, pool: &str) -> (BootTicketVerifier, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = StaticJwks {
            document: jwks,
            calls: calls.clone(),
            fail: false,
        };
        (
            BootTicketVerifier::new(
                Box::new(source),
                ISSUER,
                pool,
                DEFAULT_JWKS_REFRESH,
                DEFAULT_CLOCK_SKEW,
            ),
            calls,
        )
    }

    #[test]
    fn a_valid_ticket_verifies_entirely_offline_after_one_fetch() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, calls) = verifier(jwks, "edge-eu");

        let claims = verifier.verify(&token, "user-42", 1_000_100)?;
        assert_eq!(claims.gate, Gate::Boot);
        assert_eq!(claims.job_config_id, "jc-7f3a91");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second redemption of a *different* ticket costs no fetch: the key
        // is cached, which is what makes the edge path control-plane-free.
        let other = ticket(&issuer, json!({ "legion.io/instance": "user-43" }));
        verifier.verify(&other, "user-43", 1_000_100)?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn a_session_ticket_verifies_once_and_is_never_spent_test() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let session = json!({
            "legion.io/scope": "session",
            "legion.io/instance": Value::Null,
            "legion.io/uses": 0,
        });
        let token = ticket(&issuer, session);
        let (edge_eu, calls) = verifier(jwks.clone(), "edge-eu");
        let verifier_for = verifier;
        let verifier = edge_eu;
        for _ in 0..3 {
            let claims = verifier.verify_session(&token, 1_000_100)?;
            assert_eq!(
                (claims.job.as_str(), claims.task.as_str()),
                ("agent-sandboxes", "workspace")
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Expiry holds for a cached ticket too.
        let expired = verifier
            .verify_session(&token, 1_000_300 + 31)
            .expect_err("expired");
        assert!(expired.to_string().contains("expired"), "{expired}");
        // A boot ticket is not a session ticket, and the pool must match.
        let boot = ticket(&issuer, json!({}));
        assert!(verifier.verify_session(&boot, 1_000_100).is_err());
        let (other_pool, _) = verifier_for(jwks, "edge-us");
        assert!(other_pool.verify_session(&token, 1_000_100).is_err());
        Ok(())
    }

    #[test]
    fn a_ticket_for_another_pool_is_refused() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, _) = verifier(jwks, "edge-us");
        let err = verifier
            .verify(&token, "user-42", 1_000_100)
            .expect_err("wrong pool");
        assert!(err.to_string().contains("pool"), "{err}");
        Ok(())
    }

    #[test]
    fn a_ticket_cannot_start_a_different_instance() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, _) = verifier(jwks, "edge-eu");
        assert!(verifier.verify(&token, "user-99", 1_000_100).is_err());
        Ok(())
    }

    #[test]
    fn expiry_is_enforced_with_bounded_skew() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, _) = verifier(jwks, "edge-eu");

        // Inside the skew window past expiry: still accepted.
        verifier.verify(&token, "user-42", 1_000_300 + 29)?;
        // Beyond it: refused.
        assert!(verifier.verify(&token, "user-42", 1_000_300 + 31).is_err());
        Ok(())
    }

    #[test]
    fn a_tampered_ticket_fails_the_signature_check() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, _) = verifier(jwks, "edge-eu");

        let mut parts = token.split('.').collect::<Vec<_>>();
        let forged_payload = base64_url(
            &serde_json::to_vec(&json!({
                "iss": ISSUER, "aud": [BOOT_TICKET_AUDIENCE],
                "exp": 1_000_300, "iat": 1_000_000, "nbf": 1_000_000, "jti": "tkt_forged",
                "sub": "boot:agent-sandboxes/workspace",
                "legion.io/scope": "boot", "legion.io/job": "agent-sandboxes",
                "legion.io/job_config": "jc-7f3a91", "legion.io/task": "workspace",
                "legion.io/instance": "user-42", "legion.io/pool": "edge-eu",
                "legion.io/gate": "serve", "legion.io/uses": 1
            }))
            .expect("json"),
        );
        parts[1] = &forged_payload;
        let forged = parts.join(".");
        assert!(verifier.verify(&forged, "user-42", 1_000_100).is_err());
        Ok(())
    }

    #[test]
    fn an_unknown_kid_triggers_one_refresh_then_fails_closed() -> R<()> {
        let (issuer, _) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        // The verifier's source serves a *different* cluster's keys.
        let (other, other_jwks) = issuer_with_key();
        let _ = other;
        let (verifier, calls) = verifier(other_jwks, "edge-eu");

        assert!(verifier.verify(&token, "user-42", 1_000_100).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one refresh, not a loop");
        Ok(())
    }

    #[test]
    fn a_failed_fetch_keeps_cached_keys_usable() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier = BootTicketVerifier::new(
            Box::new(StaticJwks {
                document: json!({}),
                calls: calls.clone(),
                fail: true,
            }),
            ISSUER,
            "edge-eu",
            Duration::from_secs(0), // every verify wants a refresh
            DEFAULT_CLOCK_SKEW,
        );
        // Seed the cache the way registration does, then let every refresh
        // fail. A node cut off from its managers must still redeem tickets.
        verifier.install(&jwks)?;
        verifier.verify(&token, "user-42", 1_000_100)?;
        assert!(calls.load(Ordering::SeqCst) >= 1, "refresh was attempted");
        Ok(())
    }

    #[test]
    fn a_single_use_ticket_cannot_be_replayed_at_the_same_node() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, _) = verifier(jwks, "edge-eu");

        let claims = verifier.verify(&token, "user-42", 1_000_100)?;
        verifier.spend(&claims, 1_000_100);
        assert!(verifier.verify(&token, "user-42", 1_000_100).is_err());

        // A start that never happened releases the reservation.
        verifier.unspend(&claims.jti);
        verifier.verify(&token, "user-42", 1_000_100)?;
        Ok(())
    }

    #[test]
    fn a_multi_use_ticket_is_not_consumed_locally() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({ "legion.io/uses": 5 }));
        let (verifier, _) = verifier(jwks, "edge-eu");
        let claims = verifier.verify(&token, "user-42", 1_000_100)?;
        verifier.spend(&claims, 1_000_100);
        verifier.verify(&token, "user-42", 1_000_100)?;
        Ok(())
    }

    #[test]
    fn spent_entries_are_forgotten_once_their_ticket_expires() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let (verifier, _) = verifier(jwks, "edge-eu");
        let claims = verifier.verify(&token, "user-42", 1_000_100)?;
        verifier.spend(&claims, 1_000_100);
        // Long after expiry, the bookkeeping drops the entry — the ticket is
        // unusable anyway, so the set stays bounded by ticket lifetime.
        verifier.spend(&claims, 1_000_400);
        assert!(verifier.spent.lock().expect("spent").is_empty());
        Ok(())
    }

    /// Serves `body` at the JWKS path over TLS with a manager's cluster
    /// certificate, on its own runtime. Returns the address and the CA pin.
    fn serve_jwks(body: &'static str) -> R<(String, String)> {
        let (ca_pem, ca_key) = crate::manager::mesh_ca::generate_ca()?;
        let (cert, key, _) = crate::manager::mesh_ca::issue_manager_cert(
            &ca_pem,
            &ca_key,
            "m1",
            crate::manager::mesh_ca::ManagerCertKind::Server,
        )?;
        let identity = crate::cluster_tls::SwappableIdentity::new();
        identity.set(crate::cluster_tls::certified_key(
            &format!("{cert}{ca_pem}"),
            &key,
        )?);
        let config = crate::cluster_tls::server_config(identity, None, true)?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = format!("http://{}", listener.local_addr()?);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).expect("listener");
                let acceptor = tokio_rustls::TlsAcceptor::from(config);
                while let Ok((tcp, _)) = listener.accept().await {
                    let Ok(tls) = acceptor.accept(tcp).await else {
                        continue;
                    };
                    let service = hyper::service::service_fn(
                        |request: http::Request<hyper::body::Incoming>| async move {
                            let found = request.uri().path()
                                == crate::manager::workload_identity::DEFAULT_JWKS_PATH;
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(if found { 200 } else { 404 })
                                    .body(http_body_util::Full::new(bytes::Bytes::from(body)))
                                    .expect("response"),
                            )
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                        .await;
                }
            });
        });
        Ok((addr, crate::cluster_tls::ca_pin_pem(&ca_pem)?))
    }

    #[test]
    fn the_jwks_is_fetched_over_tls_pinned_to_the_cluster_ca_test() -> R<()> {
        let (addr, pin) = serve_jwks(r#"{"keys":[]}"#)?;
        let pinned = PinnedJwksSource::new(
            &addr,
            crate::cluster_tls::PinnedManagerVerifier::new(crate::cluster_tls::ManagerTrust::Pin(
                pin,
            )),
            Duration::from_secs(5),
        )?;
        assert_eq!(pinned.fetch()?, json!({ "keys": [] }));

        // A JWKS from a manager whose CA is not the pinned one is refused,
        // so an on-path attacker cannot swap the keys.
        let (_, other) = serve_jwks(r#"{"keys":[]}"#)?;
        let impostor = PinnedJwksSource::new(
            &addr,
            crate::cluster_tls::PinnedManagerVerifier::new(crate::cluster_tls::ManagerTrust::Pin(
                other,
            )),
            Duration::from_secs(5),
        )?;
        let err = impostor.fetch().expect_err("pin mismatch");
        assert!(format!("{err:#}").contains("CA pin mismatch"), "{err:#}");
        Ok(())
    }

    /// Regression (AWS spot check): the node assumed `iss` was the manager
    /// address in its join token, so a ticket carrying the cluster issuer
    /// failed with `InvalidIssuer`. A verifier following the issuer the
    /// managers report accepts it, and picks up a change without a restart.
    #[test]
    fn a_verifier_follows_the_issuer_the_managers_report_test() -> R<()> {
        let (issuer, jwks) = issuer_with_key();
        let token = ticket(&issuer, json!({}));
        let expected = Arc::new(ExpectedIssuer::new("http://172.31.93.106:4747"));
        let (verifier, _) = verifier(jwks, "edge-eu");
        let verifier = verifier.following(expected.clone());

        let err = verifier
            .verify(&token, "user-42", 1_000_010)
            .expect_err("wrong issuer");
        assert!(format!("{err:#}").contains("InvalidIssuer"), "{err:#}");
        assert!(format!("{err:#}").contains("172.31.93.106"), "{err:#}");

        assert!(expected.learn(&format!("{ISSUER}/")));
        assert!(!expected.learn(""), "an empty report keeps the issuer");
        assert_eq!(expected.get(), ISSUER);
        verifier.verify(&token, "user-42", 1_000_010)?;
        Ok(())
    }

    /// Regression (AWS spot check): a session ticket minted on manager B is
    /// redeemed on a node that joined through manager A. Both managers run
    /// the raft default issuer from different advertised addresses and share
    /// the signing keys through the store, as replicated managers do.
    #[test]
    fn a_ticket_minted_on_one_manager_verifies_on_a_node_joined_through_another_test() -> R<()> {
        use crate::manager::workload_identity::{default_issuer, WorkloadIdentityState};
        let store_dir = tempfile::Builder::new()
            .prefix("legion-boot-ticket-cluster-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        let store =
            crate::storage::store::Store::start_storage(&crate::structs::ManagerConfigFull {
                store_path: store_dir.to_str().context("store path")?.to_string(),
                ..Default::default()
            })?;
        store.reset_all_tables()?;
        let manager = |ip: &str| -> R<WorkloadIdentityState> {
            let mut config = crate::structs::ManagerConfigFull::default();
            config.persistence.r#type = crate::structs::PersistenceType::Raft;
            config.server.host = "0.0.0.0".into();
            config.server.advertise_addr = Some(format!("http://{ip}:4747"));
            config.workload_identity.enabled = true;
            WorkloadIdentityState::initialize(
                &store,
                &config.workload_identity,
                &default_issuer(&config),
                std::time::SystemTime::now(),
            )
        };
        let manager_a = manager("172.31.93.106")?;
        let manager_b = manager("172.31.88.20")?;

        // The node joined through A: it follows the issuer A reported and
        // holds A's JWKS.
        let expected = Arc::new(ExpectedIssuer::default());
        assert!(expected.learn(&manager_a.issuer_url()?));
        let source = StaticJwks {
            document: manager_a.jwks()?,
            calls: Arc::new(AtomicUsize::new(0)),
            fail: false,
        };
        let verifier = BootTicketVerifier::new(
            Box::new(source),
            String::new(),
            "default",
            DEFAULT_JWKS_REFRESH,
            DEFAULT_CLOCK_SKEW,
        )
        .following(expected);

        let minted = crate::manager::edge_boot::mint_session_ticket(
            &manager_b,
            &crate::manager::edge_boot::SessionTicketRequest {
                job: "sessions".into(),
                task: "shell".into(),
                pool: "default".into(),
                ttl: Duration::from_secs(300),
                accessor_id: String::new(),
            },
            "jc-1",
            "tkt_from_b",
            std::time::SystemTime::now(),
        )?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        verifier.verify_session(&minted.token, now)?;
        Ok(())
    }

    /// The JWKS comes from whichever known manager answers: the one the node
    /// joined through being down does not stop key refreshes.
    #[test]
    fn the_jwks_fetch_fails_over_to_another_known_manager_test() -> R<()> {
        let (addr, pin) = serve_jwks(r#"{"keys":[]}"#)?;
        // A port nothing listens on: the join-token manager, now down.
        let down = {
            let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
            format!("http://{}", socket.local_addr()?)
        };
        let live = addr.clone();
        let source = PinnedJwksSource::new(
            &down,
            crate::cluster_tls::PinnedManagerVerifier::new(crate::cluster_tls::ManagerTrust::Pin(
                pin,
            )),
            Duration::from_secs(5),
        )?
        .with_managers(move || vec![live.clone()]);
        assert_eq!(source.candidates(), vec![addr, down]);
        assert_eq!(source.fetch()?, json!({ "keys": [] }));
        Ok(())
    }

    fn base64_url(bytes: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }
}
