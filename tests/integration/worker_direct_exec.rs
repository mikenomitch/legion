//! The worker's exec-ticket verifier against tickets minted by the manager's
//! issuer and by a delegated key (docs/exec-and-actions.md).

use legion::manager::workload_identity::Issuer;
use legion::prelude::*;
use legion::structs::exec_ticket::{ExecTicketClaims, EXEC_TICKET_AUDIENCE};
use legion::structs::RevokedExecSession;
use legion::worker::boot_ticket::BootTicketVerifier;
use legion::worker::boot_ticket::JwksSource;
use legion::worker::direct_exec::*;
use std::sync::Arc;
use std::time::Duration;

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
        jti: legion::structs::exec_ticket::new_exec_ticket_id(),
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
        legion::structs::edge_boot::BOOT_TICKET_AUDIENCE,
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

fn delegated(jobs: &str) -> (rcgen::KeyPair, legion::structs::ExecKey) {
    let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let key = legion::structs::ExecKey {
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
    legion::cmd::exec_ticket::sign_delegated(
        &legion::cmd::exec_ticket::SignRequest {
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
