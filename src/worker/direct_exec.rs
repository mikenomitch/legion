//! Direct exec on the worker (sandbox-exec plan §6.3, §6.4;
//! docs/exec-and-actions.md, "Direct exec").
//!
//! - [`ExecTicketVerifier`]: offline verification of an exec ticket against
//!   the cluster JWKS the edge-boot verifier already caches, plus the checks
//!   only this node can make: the ticket names this node, it was issued
//!   after this process started, and its `jti` has not been redeemed here.
//! - The direct session registry: open sessions (reported on every heartbeat
//!   so a manager can re-check their minting token, and ended when it says
//!   so) and finished sessions (reported once, for the manager's
//!   `exec_audit` log).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::prelude::*;
use crate::structs::exec_ticket::{ExecTicketClaims, EXEC_TICKET_AUDIENCE};
use crate::structs::{ExecAuditRecord, ExecSessionRef, RevokedExecSession};
use crate::worker::boot_ticket::BootTicketVerifier;

/// The most finished-session records a worker holds for its next heartbeat.
/// Past this the oldest go, with a warning: audit must never grow without
/// bound on a node cut off from every manager.
pub const MAX_PENDING_AUDIT: usize = 1024;

/// Verifies exec tickets on this node.
pub struct ExecTicketVerifier {
    jwks: Arc<BootTicketVerifier>,
    node_id: String,
    /// When this worker process started, unix seconds. The spent set lives
    /// in memory, so a ticket issued before a restart could otherwise be
    /// redeemed a second time; such tickets are refused instead, and the
    /// client mints again.
    started_at: i64,
    /// Redeemed `jti`s, with the expiry that lets them be forgotten.
    spent: Mutex<HashMap<String, i64>>,
    /// Delegated minting keys, from the heartbeat, and their version.
    delegated: Mutex<DelegatedKeys>,
}

/// The delegated keys a worker holds (docs/exec-and-actions.md, "Delegated
/// minting keys").
struct DelegatedKeys {
    version: String,
    keys: HashMap<String, DelegatedKey>,
}

struct DelegatedKey {
    decoding: jsonwebtoken::DecodingKey,
    jobs: String,
    max_ttl: i64,
    max_session: u64,
}

impl ExecTicketVerifier {
    pub fn new(jwks: Arc<BootTicketVerifier>, node_id: impl Into<String>, started_at: i64) -> Self {
        ExecTicketVerifier {
            jwks,
            node_id: node_id.into(),
            started_at,
            spent: Mutex::new(HashMap::new()),
            delegated: Mutex::new(DelegatedKeys {
                version: "none".into(),
                keys: HashMap::new(),
            }),
        }
    }

    /// The version of the delegated keys held, for
    /// `HeartbeatRequest.exec_keys_version` (`"none"` before the first set).
    pub fn keys_version(&self) -> String {
        self.delegated
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .version
            .clone()
    }

    /// Replaces the delegated keys with a heartbeat's set. A key whose
    /// public key does not parse is skipped with a warning; the manager
    /// validated it at create, so that is a bug, not an attack.
    pub fn install_keys(&self, version: &str, keys: &[crate::structs::ExecKey]) {
        let mut parsed = HashMap::new();
        for key in keys {
            match jsonwebtoken::DecodingKey::from_ec_pem(key.public_key_pem.as_bytes()) {
                Ok(decoding) => {
                    parsed.insert(
                        key.id.clone(),
                        DelegatedKey {
                            decoding,
                            jobs: key.jobs.clone(),
                            max_ttl: key.max_ttl_seconds as i64,
                            max_session: key.max_session_seconds,
                        },
                    );
                }
                Err(err) => warn!(key_id = %key.id, error = %err, "Skipping an unusable exec key"),
            }
        }
        let mut delegated = self.delegated.lock().unwrap_or_else(|p| p.into_inner());
        if delegated.version != version {
            info!(
                "Exec keys updated: {} key(s), version {version}",
                parsed.len()
            );
        }
        delegated.version = version.to_string();
        delegated.keys = parsed;
    }

    /// Verifies `token` at `now_unix` and returns its claims, without
    /// redeeming it: the caller checks the target and the command, then
    /// calls [`ExecTicketVerifier::redeem`]. A ticket signed by a delegated
    /// key (`kid` starting `xk_`) is checked against that key's job glob
    /// and ceilings; the caller refuses it for a host-level runtime.
    pub fn verify(&self, token: &str, now_unix: i64) -> R<ExecTicketClaims> {
        let kid = jsonwebtoken::decode_header(token)
            .context("invalid ticket header")?
            .kid
            .context("ticket is missing a kid header")?;
        let (raw, claims) = if kid.starts_with(crate::structs::exec_ticket::EXEC_KEY_PREFIX) {
            self.decode_delegated(token, &kid)?
        } else {
            let raw = self.jwks.decode(token, EXEC_TICKET_AUDIENCE)?;
            let claims = ExecTicketClaims::from_claims(&raw)?;
            if claims.sub.starts_with("key:") {
                bail!("a ticket signed by the cluster cannot claim a delegated key's subject");
            }
            (raw, claims)
        };
        let not_before = raw.get("nbf").and_then(Value::as_i64);
        let skew = self.jwks.clock_skew().as_secs() as i64;
        if now_unix - skew > claims.exp {
            bail!(
                "the ticket expired at {} (node clock {now_unix}, skew {skew}s); mint a new one",
                claims.exp
            );
        }
        // A delegated key bounds `exp - iat`; without this an `iat` set in
        // the future stretched a ticket's life past the key's `max_ttl`.
        if claims.iat > now_unix + skew {
            bail!(
                "the ticket was issued at {}, after this node's clock ({now_unix}, skew {skew}s); \
                 mint a new one",
                claims.iat
            );
        }
        if let Some(not_before) = not_before {
            if now_unix + skew < not_before {
                bail!(
                    "the ticket is not valid until {not_before} (node clock {now_unix}, skew \
                     {skew}s)"
                );
            }
        }
        if claims.node != self.node_id {
            bail!(
                "the ticket is for node {}, not this node ({})",
                claims.node,
                self.node_id
            );
        }
        if claims.iat < self.started_at - skew {
            bail!(
                "the ticket was issued before this worker restarted, and a restart forgets which \
                 tickets were redeemed; mint a new one"
            );
        }
        if claims.uses != 1 {
            bail!(
                "the ticket allows {} uses; exec tickets allow one",
                claims.uses
            );
        }
        if self
            .spent
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&claims.jti)
        {
            bail!("the ticket {} has already been redeemed", claims.jti);
        }
        Ok(claims)
    }

    /// Decodes a ticket signed by the delegated key `kid`, checking it
    /// against that key's job glob and ceilings.
    fn decode_delegated(
        &self,
        token: &str,
        kid: &str,
    ) -> R<(serde_json::Map<String, Value>, ExecTicketClaims)> {
        let delegated = self.delegated.lock().unwrap_or_else(|p| p.into_inner());
        let key = delegated.keys.get(kid).with_context(|| {
            format!(
                "the ticket is signed by exec key {kid}, which this node does not hold (it was \
                 deleted, or this node has not heard of it yet)"
            )
        })?;
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::ES256);
        validation.set_audience(&[EXEC_TICKET_AUDIENCE]);
        // Expiry is checked against the caller's clock with the node's
        // skew, as for cluster-signed tickets.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        let raw = jsonwebtoken::decode::<serde_json::Map<String, Value>>(
            token,
            &key.decoding,
            &validation,
        )
        .context("ticket failed verification")?
        .claims;
        let claims = ExecTicketClaims::from_claims(&raw)?;
        if claims.sub != format!("key:{kid}") {
            bail!("a ticket signed by exec key {kid} must carry sub = key:{kid}");
        }
        if !crate::structs::namespace::job_pattern_matches(&key.jobs, &claims.job) {
            bail!(
                "exec key {kid} mints tickets for jobs matching {}, not {}",
                key.jobs,
                claims.job
            );
        }
        if claims.exp - claims.iat > key.max_ttl {
            bail!(
                "the ticket's redeem window ({}s) is above exec key {kid}'s ceiling ({}s)",
                claims.exp - claims.iat,
                key.max_ttl
            );
        }
        if claims.max_session > key.max_session {
            bail!(
                "the ticket's max_session ({}s) is above exec key {kid}'s ceiling ({}s)",
                claims.max_session,
                key.max_session
            );
        }
        Ok((raw, claims))
    }

    /// Marks a verified ticket spent. Fails if a concurrent redemption won.
    /// Forgets entries whose ticket has expired, which is what keeps the set
    /// bounded.
    pub fn redeem(&self, claims: &ExecTicketClaims, now_unix: i64) -> R<()> {
        let mut spent = self.spent.lock().unwrap_or_else(|p| p.into_inner());
        spent.retain(|_, expires_at| *expires_at > now_unix);
        if spent.contains_key(&claims.jti) {
            bail!("the ticket {} has already been redeemed", claims.jti);
        }
        spent.insert(claims.jti.clone(), claims.exp.max(now_unix + 1));
        Ok(())
    }

    /// Returns a redeemed ticket whose session never opened, so the caller
    /// can retry it inside its redeem window.
    pub fn release(&self, jti: &str) {
        self.spent
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(jti);
    }
}

/// How an open direct session is told to end early.
#[derive(Debug)]
pub struct SessionEnd {
    /// When `max_session` runs out.
    pub deadline: Instant,
    /// Set when a manager revokes the session.
    revoked: Mutex<Option<String>>,
}

impl SessionEnd {
    pub fn new(max_session: Duration) -> Arc<Self> {
        Arc::new(SessionEnd {
            deadline: Instant::now() + max_session,
            revoked: Mutex::new(None),
        })
    }

    /// The revocation reason, once a manager has revoked the session.
    pub fn revoked(&self) -> Option<String> {
        self.revoked
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn revoke(&self, reason: &str) {
        let mut revoked = self.revoked.lock().unwrap_or_else(|p| p.into_inner());
        if revoked.is_none() {
            *revoked = Some(reason.to_string());
        }
    }
}

struct OpenDirect {
    reference: ExecSessionRef,
    end: Arc<SessionEnd>,
}

#[derive(Default)]
struct Registry {
    open: HashMap<String, OpenDirect>,
    finished: VecDeque<ExecAuditRecord>,
}

static REGISTRY: std::sync::LazyLock<Mutex<Registry>> =
    std::sync::LazyLock::new(|| Mutex::new(Registry::default()));

fn registry() -> std::sync::MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap_or_else(|p| p.into_inner())
}

/// One open direct session. Dropping it without [`DirectSession::finish`]
/// records the session as ended without an exit code.
pub struct DirectSession {
    record: Option<ExecAuditRecord>,
    pub end: Arc<SessionEnd>,
}

impl DirectSession {
    /// Registers a session opened with `claims` running `command`.
    pub fn open(claims: &ExecTicketClaims, command: &[String], now_unix: i64) -> Self {
        let end = SessionEnd::new(Duration::from_secs(claims.max_session.max(1)));
        let reference = ExecSessionRef {
            jti: claims.jti.clone(),
            subject: claims.sub.clone(),
            job: claims.job.clone(),
            action: claims.action.clone().unwrap_or_default(),
            allocation_id: claims.allocation_id,
        };
        registry().open.insert(
            claims.jti.clone(),
            OpenDirect {
                reference,
                end: end.clone(),
            },
        );
        let action = claims.action.clone().unwrap_or_default();
        DirectSession {
            record: Some(ExecAuditRecord {
                jti: claims.jti.clone(),
                subject: claims.sub.clone(),
                allocation_id: claims.allocation_id,
                job: claims.job.clone(),
                task: claims.task.clone(),
                // A rendered action argv can carry sensitive parameters.
                command: if action.is_empty() {
                    command.to_vec()
                } else {
                    Vec::new()
                },
                action,
                tty: claims.tty,
                exit_code: -1,
                started_at: now_unix,
                ended_at: 0,
                ended_by: String::new(),
            }),
            end,
        }
    }

    /// Records how the session ended.
    pub fn finish(mut self, exit_code: i32, ended_by: &str) {
        self.close(exit_code, ended_by);
    }

    fn close(&mut self, exit_code: i32, ended_by: &str) {
        let Some(mut record) = self.record.take() else {
            return;
        };
        record.exit_code = exit_code;
        record.ended_by = ended_by.to_string();
        record.ended_at = time::OffsetDateTime::now_utc().unix_timestamp();
        let mut registry = registry();
        registry.open.remove(&record.jti);
        push_bounded(&mut registry.finished, record);
    }
}

impl Drop for DirectSession {
    fn drop(&mut self) {
        self.close(-1, "the session ended without an exit status");
    }
}

fn push_bounded(queue: &mut VecDeque<ExecAuditRecord>, record: ExecAuditRecord) {
    if queue.len() >= MAX_PENDING_AUDIT {
        if let Some(dropped) = queue.pop_front() {
            warn!(
                jti = %dropped.jti,
                "Dropping a direct exec audit record: {MAX_PENDING_AUDIT} are waiting for a \
                 heartbeat"
            );
        }
    }
    queue.push_back(record);
}

/// The open direct sessions, for `HeartbeatRequest.exec_sessions`.
pub fn open_sessions() -> Vec<ExecSessionRef> {
    let mut sessions: Vec<_> = registry()
        .open
        .values()
        .map(|open| open.reference.clone())
        .collect();
    sessions.sort_by(|a, b| a.jti.cmp(&b.jti));
    sessions
}

/// Takes the finished-session records for a heartbeat.
pub fn take_audit() -> Vec<ExecAuditRecord> {
    registry().finished.drain(..).collect()
}

/// Puts records back after a failed heartbeat, ahead of any finished since.
pub fn restore_audit(records: Vec<ExecAuditRecord>) {
    let mut registry = registry();
    let newer: Vec<_> = registry.finished.drain(..).collect();
    for record in records.into_iter().chain(newer) {
        push_bounded(&mut registry.finished, record);
    }
}

/// The process's exec ticket verifier, for the heartbeat loop.
static VERIFIER: std::sync::OnceLock<Arc<ExecTicketVerifier>> = std::sync::OnceLock::new();

/// Hands the heartbeat loop this worker's verifier.
pub fn register_verifier(verifier: Arc<ExecTicketVerifier>) {
    let _ = VERIFIER.set(verifier);
}

/// `HeartbeatRequest.exec_keys_version`: empty on a worker that admits no
/// tickets, so the manager skips the lookup.
pub fn keys_version() -> String {
    VERIFIER
        .get()
        .map(|verifier| verifier.keys_version())
        .unwrap_or_default()
}

/// Installs a heartbeat's delegated keys, when it carried a new set.
pub fn apply_exec_keys(version: &str, keys: &[crate::structs::ExecKey]) {
    let Some(verifier) = VERIFIER.get() else {
        return;
    };
    if !version.is_empty() && version != verifier.keys_version() {
        verifier.install_keys(version, keys);
    }
}

/// Ends the sessions a manager revoked (`HeartbeatReply.revoked_exec_sessions`).
pub fn apply_revocations(revoked: &[RevokedExecSession]) {
    if revoked.is_empty() {
        return;
    }
    let registry = registry();
    for revocation in revoked {
        if let Some(open) = registry.open.get(&revocation.jti) {
            info!(jti = %revocation.jti, "Ending a revoked direct exec session");
            open.end.revoke(&revocation.reason);
        }
    }
}

#[cfg(test)]
mod direct_exec_test {
    use super::*;
    use crate::manager::workload_identity::Issuer;
    use crate::worker::boot_ticket::JwksSource;

    struct Static(Value);
    impl JwksSource for Static {
        fn fetch(&self) -> R<Value> {
            Ok(self.0.clone())
        }
    }

    const ISSUER: &str = "https://mgr.test:4747";

    fn issuer() -> (Issuer, Value) {
        let issuer = Issuer::new(
            ISSUER,
            "/.well-known/jwks.json",
            Duration::from_secs(3600),
            None,
            std::time::SystemTime::now() - Duration::from_secs(600),
        )
        .expect("issuer");
        let jwks = issuer.jwks();
        (issuer, jwks)
    }

    fn claims(now: i64) -> ExecTicketClaims {
        ExecTicketClaims {
            sub: "accessor:abc".into(),
            jti: crate::structs::exec_ticket::new_exec_ticket_id(),
            iat: now,
            exp: now + 60,
            node: "w-17".into(),
            job: "agents".into(),
            task: "workspace".into(),
            allocation_id: 7,
            instance: None,
            argv: Some(vec!["/bin/sh".into()]),
            action: None,
            tty: false,
            max_session: 3600,
            uses: 1,
        }
    }

    fn mint(issuer: &Issuer, claims: &ExecTicketClaims, audience: &str) -> String {
        issuer
            .issue_ticket(
                audience,
                &claims.jti,
                &claims.sub,
                claims.to_claims(),
                claims.iat as u64,
                claims.exp as u64,
            )
            .expect("ticket")
            .token
    }

    fn verifier(jwks: Value, started_at: i64) -> ExecTicketVerifier {
        let cache = Arc::new(BootTicketVerifier::new(
            Box::new(Static(jwks)),
            ISSUER,
            "default",
            Duration::from_secs(300),
            Duration::from_secs(30),
        ));
        ExecTicketVerifier::new(cache, "w-17", started_at)
    }

    fn now() -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp()
    }

    #[test]
    fn a_ticket_verifies_once_on_its_node_test() -> R<()> {
        let (issuer, jwks) = issuer();
        let now = now();
        let verifier = verifier(jwks, now - 100);
        let claims = claims(now);
        let token = mint(&issuer, &claims, EXEC_TICKET_AUDIENCE);
        let verified = verifier.verify(&token, now)?;
        assert_eq!(verified, claims);
        verifier.redeem(&verified, now)?;
        let replay = verifier.verify(&token, now).unwrap_err();
        assert!(format!("{replay:#}").contains("already been redeemed"));
        // A session that never opened gives its ticket back.
        verifier.release(&claims.jti);
        verifier.verify(&token, now)?;
        Ok(())
    }

    #[test]
    fn tickets_for_another_node_audience_or_time_are_refused_test() -> R<()> {
        let (issuer, jwks) = issuer();
        let now = now();
        let verifier = verifier(jwks, now - 100);

        let elsewhere = ExecTicketClaims {
            node: "w-18".into(),
            ..claims(now)
        };
        let err = verifier
            .verify(&mint(&issuer, &elsewhere, EXEC_TICKET_AUDIENCE), now)
            .unwrap_err();
        assert!(format!("{err:#}").contains("for node w-18"), "{err:#}");

        // A boot ticket can't stand in for an exec ticket.
        let boot = mint(
            &issuer,
            &claims(now),
            crate::structs::edge_boot::BOOT_TICKET_AUDIENCE,
        );
        assert!(verifier.verify(&boot, now).is_err());

        let expired = ExecTicketClaims {
            iat: now - 200,
            exp: now - 100,
            ..claims(now)
        };
        let err = verifier
            .verify(&mint(&issuer, &expired, EXEC_TICKET_AUDIENCE), now)
            .unwrap_err();
        assert!(format!("{err:#}").contains("expired"), "{err:#}");
        Ok(())
    }

    #[test]
    fn a_restarted_worker_refuses_tickets_issued_before_it_started_test() -> R<()> {
        let (issuer, jwks) = issuer();
        let now = now();
        let verifier = verifier(jwks, now);
        let before = ExecTicketClaims {
            iat: now - 45,
            exp: now + 15,
            ..claims(now)
        };
        let err = verifier
            .verify(&mint(&issuer, &before, EXEC_TICKET_AUDIENCE), now)
            .unwrap_err();
        assert!(format!("{err:#}").contains("before this worker restarted"));
        // Inside the clock skew is fine: the mint and the start raced.
        let racing = ExecTicketClaims {
            iat: now - 5,
            ..claims(now)
        };
        verifier.verify(&mint(&issuer, &racing, EXEC_TICKET_AUDIENCE), now)?;
        Ok(())
    }

    fn delegated(jobs: &str) -> (rcgen::KeyPair, crate::structs::ExecKey) {
        let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let key = crate::structs::ExecKey {
            id: "xk_platform".into(),
            name: "platform".into(),
            jobs: jobs.into(),
            public_key_pem: pair.public_key_pem(),
            max_ttl_seconds: 300,
            max_session_seconds: 1800,
            created_by: "accessor:abc".into(),
            created_at: String::new(),
        };
        (pair, key)
    }

    fn sign(pair: &rcgen::KeyPair, job: &str, ttl: u64, max_session: u64, now: i64) -> String {
        crate::cmd::exec_ticket::sign_delegated(
            &crate::cmd::exec_ticket::SignRequest {
                key_pem: pair.serialize_pem(),
                key_id: "xk_platform".into(),
                node: "w-17".into(),
                allocation_id: 7,
                job: job.into(),
                task: "workspace".into(),
                instance: None,
                argv: Some(vec!["/bin/sh".into()]),
                tty: false,
                ttl: Duration::from_secs(ttl),
                max_session: Duration::from_secs(max_session),
            },
            now,
        )
        .expect("sign")
        .0
    }

    #[test]
    fn a_delegated_key_mints_only_inside_its_glob_and_ceilings_test() -> R<()> {
        let (_, jwks) = issuer();
        let now = now();
        let verifier = verifier(jwks, now - 100);
        let (pair, key) = delegated("agents-*");

        // Unknown until the heartbeat delivers it.
        let token = sign(&pair, "agents-a", 60, 600, now);
        let err = verifier.verify(&token, now).unwrap_err();
        assert!(format!("{err:#}").contains("does not hold"), "{err:#}");
        assert_eq!(verifier.keys_version(), "none");

        verifier.install_keys("k1-abc", std::slice::from_ref(&key));
        let claims = verifier.verify(&token, now)?;
        assert_eq!(claims.sub, "key:xk_platform");

        let err = verifier
            .verify(&sign(&pair, "billing", 60, 600, now), now)
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("jobs matching agents-*"),
            "{err:#}"
        );
        let err = verifier
            .verify(&sign(&pair, "agents-a", 900, 600, now), now)
            .unwrap_err();
        assert!(format!("{err:#}").contains("redeem window"), "{err:#}");
        let err = verifier
            .verify(&sign(&pair, "agents-a", 60, 3600, now), now)
            .unwrap_err();
        assert!(format!("{err:#}").contains("max_session"), "{err:#}");
        // Issued a year from now, inside the key's window from that time: it
        // would otherwise be valid for a year plus the window.
        let err = verifier
            .verify(&sign(&pair, "agents-a", 60, 600, now + 365 * 86_400), now)
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("after this node's clock"),
            "{err:#}"
        );

        // Another key's signature under this kid is a forgery.
        let (other, _) = delegated("agents-*");
        assert!(verifier
            .verify(&sign(&other, "agents-a", 60, 600, now), now)
            .is_err());

        // Deleted: the next heartbeat's set no longer holds it.
        verifier.install_keys("k0-empty", &[]);
        assert!(verifier.verify(&token, now).is_err());
        Ok(())
    }

    #[test]
    fn sessions_report_while_open_and_audit_once_finished_test() {
        let now = now();
        let mut claims = claims(now);
        claims.jti = "ext_registry_test".into();
        let session = DirectSession::open(&claims, &["/bin/sh".into()], now);
        assert!(open_sessions().iter().any(|s| s.jti == "ext_registry_test"));

        apply_revocations(&[RevokedExecSession {
            jti: "ext_registry_test".into(),
            reason: "the token that minted this session was deleted".into(),
        }]);
        assert_eq!(
            session.end.revoked().as_deref(),
            Some("the token that minted this session was deleted")
        );

        session.finish(3, "revoked");
        assert!(!open_sessions().iter().any(|s| s.jti == "ext_registry_test"));
        let audit = take_audit();
        let record = audit
            .iter()
            .find(|r| r.jti == "ext_registry_test")
            .expect("audited");
        assert_eq!(record.exit_code, 3);
        assert_eq!(record.command, vec!["/bin/sh".to_string()]);
        restore_audit(audit);
        assert!(take_audit().iter().any(|r| r.jti == "ext_registry_test"));
    }

    #[test]
    fn an_action_session_never_reports_its_rendered_argv_test() {
        let now = now();
        let mut claims = claims(now);
        claims.jti = "ext_action_test".into();
        claims.action = Some("rotate".into());
        drop(DirectSession::open(
            &claims,
            &["rotate".into(), "--secret=hunter2".into()],
            now,
        ));
        let audit = take_audit();
        let record = audit.iter().find(|r| r.jti == "ext_action_test").unwrap();
        assert!(record.command.is_empty());
        assert_eq!(record.action, "rotate");
        assert_eq!(record.exit_code, -1);
    }
}
