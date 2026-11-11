//! The worker's boot-ticket verifier against tickets the manager's issuer
//! mints: the contract between the two halves of an edge start (docs/edge-
//! boot.md). Moved here from the verifier's unit tests when the manager became
//! a crate above the worker.

use legion::manager::workload_identity::Issuer;
use legion::prelude::*;
use legion::structs::edge_boot::Gate;
use legion::structs::edge_boot::BOOT_TICKET_AUDIENCE;
use legion::worker::boot_ticket::*;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    assert_eq!(verifier.spent_count(), 0);
    Ok(())
}

/// Serves `body` at the JWKS path over TLS with a manager's cluster
/// certificate, on its own runtime. Returns the address and the CA pin.
fn serve_jwks(body: &'static str) -> R<(String, String)> {
    let (ca_pem, ca_key) = legion::mesh_ca::generate_ca()?;
    let (cert, key, _) = legion::mesh_ca::issue_manager_cert(
        &ca_pem,
        &ca_key,
        "m1",
        legion::mesh_ca::ManagerCertKind::Server,
    )?;
    let identity = legion::cluster_tls::SwappableIdentity::new();
    identity.set(legion::cluster_tls::certified_key(
        &format!("{cert}{ca_pem}"),
        &key,
    )?);
    let config = legion::cluster_tls::server_config(identity, None, true)?;
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
                            == legion::structs::workload_identity::DEFAULT_JWKS_PATH;
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
    Ok((addr, legion::cluster_tls::ca_pin_pem(&ca_pem)?))
}

#[test]
fn the_jwks_is_fetched_over_tls_pinned_to_the_cluster_ca_test() -> R<()> {
    let (addr, pin) = serve_jwks(r#"{"keys":[]}"#)?;
    let pinned = PinnedJwksSource::new(
        &addr,
        legion::cluster_tls::PinnedManagerVerifier::new(legion::cluster_tls::ManagerTrust::Pin(
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
        legion::cluster_tls::PinnedManagerVerifier::new(legion::cluster_tls::ManagerTrust::Pin(
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
    use legion::manager::workload_identity::{default_issuer, WorkloadIdentityState};
    let store_dir = tempfile::Builder::new()
        .prefix("legion-boot-ticket-cluster-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    let store =
        legion::storage::store::Store::start_storage(&legion::structs::ManagerConfigFull {
            store_path: store_dir.to_str().context("store path")?.to_string(),
            ..Default::default()
        })?;
    store.reset_all_tables()?;
    let manager = |ip: &str| -> R<WorkloadIdentityState> {
        let mut config = legion::structs::ManagerConfigFull::default();
        config.persistence.r#type = legion::structs::PersistenceType::Raft;
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

    let minted = legion::manager::edge_boot::mint_session_ticket(
        &manager_b,
        &legion::manager::edge_boot::SessionTicketRequest {
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
        legion::cluster_tls::PinnedManagerVerifier::new(legion::cluster_tls::ManagerTrust::Pin(
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
