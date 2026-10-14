use super::test_support::*;
use super::verify::{assemble_view, verify_issued, Refusal};
use super::*;
use crate::cluster_tls::{ca_pin_pem, peer_role, PeerRole, MANAGER_SERVER_NAME};
use crate::manager::mesh_ca::{generate_csr, CsrSubject, LeafSpec, ManagerCertKind};
use crate::structs::config::ClusterCaConfigFull;
use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, Ia5String, KeyPair, KeyUsagePurpose, SanType,
};
use std::path::Path;

/// Not a test: the `exec` helper's entry point. A no-op unless the helper
/// script (`test_support::exec_helper`) re-runs this binary with
/// [`HELPER_DIR_ENV`] set.
#[test]
fn exec_helper_main() {
    let (Ok(dir), Ok(out)) = (std::env::var(HELPER_DIR_ENV), std::env::var(HELPER_OUT_ENV)) else {
        return;
    };
    run_helper(Path::new(&dir), Path::new(&out));
}

fn verify_as_client(bundle_pem: &str, chain_pem: &str) -> R<()> {
    let roots = crate::cluster_tls::roots_from_pem(bundle_pem)?;
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
    let chain = crate::cluster_tls::certs_from_pem(chain_pem)?;
    verifier
        .verify_client_cert(&chain[0], &chain[1..], rustls_pki_types::UnixTime::now())
        .map_err(|err| anyhow!("{err}"))?;
    Ok(())
}

fn verify_as_server(bundle_pem: &str, chain_pem: &str, name: &str) -> R<()> {
    use rustls::client::danger::ServerCertVerifier;
    let roots = crate::cluster_tls::roots_from_pem(bundle_pem)?;
    let verifier = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots)).build()?;
    let chain = crate::cluster_tls::certs_from_pem(chain_pem)?;
    verifier
        .verify_server_cert(
            &chain[0],
            &chain[1..],
            &rustls_pki_types::ServerName::try_from(name.to_string())?,
            &[],
            rustls_pki_types::UnixTime::now(),
        )
        .map_err(|err| anyhow!("{err}"))?;
    Ok(())
}

/// What a worker holding only its join token's pin does with a manager's
/// server chain (`cluster_tls::PinnedManagerVerifier`).
fn verify_with_pin(pin: &str, server_chain_pem: &str) -> R<()> {
    use rustls::client::danger::ServerCertVerifier;
    let verifier = crate::cluster_tls::PinnedManagerVerifier::new(
        crate::cluster_tls::ManagerTrust::Pin(pin.to_string()),
    );
    let chain = crate::cluster_tls::certs_from_pem(server_chain_pem)?;
    verifier
        .verify_server_cert(
            &chain[0],
            &chain[1..],
            &rustls_pki_types::ServerName::try_from(MANAGER_SERVER_NAME)?,
            &[],
            rustls_pki_types::UnixTime::now(),
        )
        .map_err(|err| anyhow!("{err}"))?;
    Ok(())
}

fn role_of(chain_pem: &str) -> R<PeerRole> {
    let certs = crate::cluster_tls::certs_from_pem(chain_pem)?;
    Ok(peer_role(certs[0].as_ref()))
}

fn bump_mtime(path: &Path, seconds: u64) -> R<()> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(seconds))?;
    Ok(())
}

fn pki_other() -> R<TestPki> {
    pki("other")
}

// ── builtin ──────────────────────────────────────────────────────────────

#[test]
fn builtin_provider_signs_and_rotates_like_the_built_in_ca_test() -> R<()> {
    let store = store_with(ClusterCaConfigFull::default())?;
    let ca = store.cluster_ca().clone();
    assert_eq!(ca.kind(), "builtin");
    assert!(ca.current(&store)?.is_none(), "no CA before first use");

    let view = ca.ensure(&store)?;
    let row = store.mesh_ca()?.context("the built-in CA row")?;
    assert_eq!(view.pin, row.pin()?);
    assert_eq!(view.bundle_pins, vec![view.pin.clone()]);
    assert!(view.chain_pem.is_empty());

    let (_, csr) = generate_csr("node-1")?;
    let issued = ca.sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))?;
    verify_as_client(&view.bundle_pem, &issued.chain())?;
    assert_eq!(role_of(&issued.leaf_pem)?, PeerRole::Node("node-1".into()));
    let lifetime = issued.lifetime().as_secs();
    assert!(
        (24 * 3600..=24 * 3600 + 600).contains(&lifetime),
        "{lifetime}"
    );

    let (server, key) = ca.issue_manager_cert(&store, "mgr-a", ManagerCertKind::Server)?;
    assert!(key.contains("PRIVATE KEY"));
    verify_as_server(&view.bundle_pem, &server.chain(), MANAGER_SERVER_NAME)?;
    verify_with_pin(&view.pin, &server.server_chain())?;
    assert_eq!(
        role_of(&server.leaf_pem)?,
        PeerRole::Manager("mgr-a".into())
    );

    // Rotation: the new CA signs, the chain carries the cross certificate,
    // and the bundle holds both, the new one first.
    let old_pin = view.pin.clone();
    let rotated = ca.rotate(&store)?;
    assert!(matches!(rotated.phase, CaRotationPhase::Overlap { .. }));
    assert_ne!(rotated.pin, old_pin);
    assert_eq!(
        rotated.bundle_pins,
        vec![rotated.pin.clone(), old_pin.clone()]
    );
    assert!(!rotated.chain_pem.is_empty());
    let (_, csr) = generate_csr("node-1")?;
    let issued = ca.sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))?;
    // A holder that trusts only the old CA still verifies the new chain.
    verify_as_client(&view.bundle_pem, &issued.chain())?;
    verify_as_client(&rotated.bundle_pem, &issued.chain())?;
    Ok(())
}

// ── file ─────────────────────────────────────────────────────────────────

#[test]
fn file_provider_signs_under_an_operator_intermediate_test() -> R<()> {
    let pki = pki("acme")?;
    let dir = tempfile::tempdir()?;
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let store = store_with(file_config(&cert, &key, None))?;
    let ca = store.cluster_ca().clone();
    ca.start()?;

    let view = ca.current(&store)?.context("a file CA always has a view")?;
    assert_eq!(view.provider, "file");
    // The bundle defaults to the signing CA itself (calls entry 4).
    assert_eq!(view.pin, ca_pin_pem(&pki.inter_pem)?);
    assert_eq!(view.bundle_pins, vec![view.pin.clone()]);
    assert!(view.chain_pem.is_empty());

    let (_, csr) = generate_csr("node-1")?;
    let issued = ca.sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))?;
    verify_as_client(&pki.inter_pem, &issued.chain())?;
    // Trusting the operator's root is not what holders do by default: the
    // chain is the bare leaf, scoped to Legion's own intermediate.
    assert!(verify_as_client(&pki.root_pem, &issued.chain()).is_err());

    let (server, _) = ca.issue_manager_cert(&store, "mgr-a", ManagerCertKind::Server)?;
    verify_with_pin(&view.pin, &server.server_chain())?;
    let (client, _) = ca.issue_manager_cert(&store, "mgr-a", ManagerCertKind::Client)?;
    verify_as_client(&view.bundle_pem, &client.chain())?;

    // The key never entered the store.
    assert!(store.mesh_ca()?.is_none());
    assert!(ca.status(&store).problem.is_none());
    Ok(())
}

#[test]
fn file_provider_with_a_root_bundle_chains_through_the_intermediate_test() -> R<()> {
    let pki = pki("acme")?;
    let dir = tempfile::tempdir()?;
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let bundle = write(dir.path(), "trust.pem", &pki.root_pem)?;
    let store = store_with(file_config(&cert, &key, Some(&bundle)))?;
    let ca = store.cluster_ca().clone();

    let view = ca.ensure(&store)?;
    assert_eq!(
        view.pin,
        ca_pin_pem(&pki.root_pem)?,
        "the anchor is the root"
    );
    assert_eq!(
        crate::cluster_tls::bundle_pins(&view.chain_pem),
        vec![ca_pin_pem(&pki.inter_pem)?],
        "leaves carry the intermediate"
    );

    let (_, csr) = generate_csr("node-1")?;
    let issued = ca.sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))?;
    verify_as_client(&pki.root_pem, &issued.chain())?;

    // A worker holding a join token that pins the root verifies the
    // manager from the first byte, through the intermediate.
    let (server, _) = ca.issue_manager_cert(&store, "mgr-a", ManagerCertKind::Server)?;
    verify_with_pin(&view.pin, &server.server_chain())?;

    // The manager's worker-plane material comes from the same CA.
    let materials = crate::manager::worker_plane::WorkerPlane::global().materials(&store)?;
    let key = materials
        .server_identity
        .current()
        .context("a server identity")?;
    let presented: String = key
        .cert
        .iter()
        .map(|cert| verify::der_to_pem(cert.as_ref()))
        .collect();
    verify_with_pin(&view.pin, &presented)?;
    Ok(())
}

#[test]
fn file_provider_reloads_changed_files_and_keeps_the_last_good_ca_test() -> R<()> {
    let pki = pki("acme")?;
    let dir = tempfile::tempdir()?;
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let bundle = write(dir.path(), "trust.pem", &pki.root_pem)?;
    let store = store_with(file_config(&cert, &key, Some(&bundle)))?;
    let ca = store.cluster_ca().clone();
    let first = ca.ensure(&store)?;

    // A new intermediate under the same root: picked up on the next read,
    // and the anchor (the root) does not move.
    let (next_pem, next_key) = second_intermediate(&pki, "acme legion intermediate 2")?;
    std::fs::write(&cert, &next_pem)?;
    std::fs::write(&key, &next_key)?;
    bump_mtime(&cert, 5)?;
    bump_mtime(&key, 5)?;
    let second = ca.ensure(&store)?;
    assert_ne!(first.fingerprint(), second.fingerprint());
    assert_eq!(first.pin, second.pin);
    assert_eq!(
        crate::cluster_tls::bundle_pins(&second.chain_pem),
        vec![ca_pin_pem(&next_pem)?]
    );

    // Broken files keep the last good CA signing, and say so.
    std::fs::write(&cert, "not a certificate")?;
    bump_mtime(&cert, 10)?;
    let third = ca.ensure(&store)?;
    assert_eq!(third.fingerprint(), second.fingerprint());
    let problem = ca.status(&store).problem.context("a problem")?;
    assert!(problem.contains("cannot be used"), "{problem}");
    let (_, csr) = generate_csr("node-1")?;
    ca.sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))?;
    Ok(())
}

#[test]
fn file_provider_refuses_unusable_material_at_start_test() -> R<()> {
    let pki = pki("acme")?;
    let other = pki_other()?;
    let dir = tempfile::tempdir()?;
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let start = |cert: &Path, key: &Path, bundle: Option<&Path>| -> String {
        ClusterCa::from_config(&file_config(cert, key, bundle))
            .and_then(|ca| ca.start())
            .map(|()| String::new())
            .unwrap_or_else(|err| format!("{err:#}"))
    };

    let missing = dir.path().join("missing.key");
    let err = start(&cert, &missing, None);
    assert!(err.contains("clusterCa.keyPath"), "{err}");

    // A key that is not the certificate's: the self-test's probe does not
    // verify.
    let wrong_key = write(dir.path(), "wrong.key", &pki.root_key)?;
    let err = start(&cert, &wrong_key, None);
    assert!(
        err.contains("cannot issue a certificate Legion accepts"),
        "{err}"
    );

    // A leaf, not a CA.
    let (leaf, leaf_key, _) =
        crate::manager::mesh_ca::issue_node_cert(&pki.inter_pem, &pki.inter_key, "node-1")?;
    let leaf_path = write(dir.path(), "leaf.pem", &leaf)?;
    let leaf_key_path = write(dir.path(), "leaf.key", &leaf_key)?;
    let err = start(&leaf_path, &leaf_key_path, None);
    assert!(err.contains("not a CA certificate"), "{err}");

    // A SEC1 key gets the conversion command.
    let sec1 = write(
        dir.path(),
        "sec1.key",
        "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIA==\n-----END EC PRIVATE KEY-----\n",
    )?;
    let err = start(&cert, &sec1, None);
    assert!(err.contains("openssl pkcs8 -topk8"), "{err}");

    // A bundle the CA does not chain to.
    let foreign = write(dir.path(), "foreign.pem", &other.root_pem)?;
    let err = start(&cert, &key, Some(&foreign));
    assert!(err.contains("does not chain to any certificate"), "{err}");

    // And the good material starts.
    assert_eq!(start(&cert, &key, None), "");
    Ok(())
}

#[test]
fn file_and_exec_cas_refuse_to_rotate_with_the_procedure_test() -> R<()> {
    let pki = pki("acme")?;
    let dir = tempfile::tempdir()?;
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let store = store_with(file_config(&cert, &key, None))?;
    let err = store.cluster_ca().rotate(&store).expect_err("file refuses");
    assert_eq!(err.to_status().code(), tonic::Code::FailedPrecondition);
    let message = err.to_string();
    assert!(message.contains("`file` provider"), "{message}");
    assert!(message.contains("bundlePath"), "{message}");

    let exec = exec::ExecCa::new(
        vec!["true".into()],
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(60),
    );
    let err = exec.rotate(&store).expect_err("exec refuses");
    assert_eq!(err.to_status().code(), tonic::Code::FailedPrecondition);
    assert!(err.to_string().contains("new issuer"), "{err}");
    Ok(())
}

// ── exec ─────────────────────────────────────────────────────────────────

#[cfg(unix)]
#[test]
fn exec_provider_signs_through_the_helper_and_refuses_bad_answers_test() -> R<()> {
    let pki = pki("vault")?;
    let dir = tempfile::tempdir()?;
    let command = exec_helper(
        dir.path(),
        &pki.inter_pem,
        &pki.inter_key,
        &pki.root_pem,
        "good",
    )?;
    let store = store_with(exec_config(command))?;
    let ca = store.cluster_ca().clone();
    assert!(!ca.signs_locally());

    let view = ca.ensure(&store)?;
    assert_eq!(view.provider, "exec");
    assert_eq!(view.pin, ca_pin_pem(&pki.root_pem)?);

    let (_, csr) = generate_csr("node-1")?;
    let issued = ca.sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))?;
    verify_as_client(&pki.root_pem, &issued.chain())?;
    assert_eq!(role_of(&issued.leaf_pem)?, PeerRole::Node("node-1".into()));
    let (server, _) = ca.issue_manager_cert(&store, "mgr-a", ManagerCertKind::Server)?;
    verify_with_pin(&view.pin, &server.server_chain())?;

    let refused = |mode: &str| -> R<CaError> {
        set_mode(dir.path(), mode)?;
        let (_, csr) = generate_csr("node-1")?;
        Ok(ca
            .sign_csr(&store, &csr, &CsrSubject::Node("node-1".into()))
            .expect_err("refused"))
    };
    let err = refused("wrong-uri")?;
    assert_eq!(err.to_status().code(), tonic::Code::FailedPrecondition);
    let message = err.to_string();
    assert!(message.contains("exec ("), "names the provider: {message}");
    assert!(message.contains("URI SANs"), "{message}");
    assert!(message.contains("spiffe://corp/x"), "{message}");

    let message = refused("long")?.to_string();
    assert!(message.contains("but Legion asked for 1day"), "{message}");

    let message = refused("garbage")?.to_string();
    assert!(message.contains("holds no certificate"), "{message}");

    let err = refused("fail")?;
    assert_eq!(err.to_status().code(), tonic::Code::Unavailable);
    assert!(err.to_string().contains("told to fail"), "{err}");

    // A describe that starts failing keeps the last good bundle, and says
    // so.
    ca.inner
        .provider
        .reload(&store)
        .expect_err("describe fails now");
    let status = ca.status(&store);
    assert_eq!(
        status.view.as_ref().map(|v| v.pin.clone()),
        Some(view.pin.clone())
    );
    let problem = status.problem.context("a problem")?;
    assert!(problem.contains("keeps the CA it read last"), "{problem}");
    Ok(())
}

#[cfg(unix)]
#[test]
fn exec_provider_errors_name_the_failure_test() -> R<()> {
    let slow = exec::describe(
        &["/bin/sh".into(), "-c".into(), "sleep 5".into()],
        std::time::Duration::from_millis(200),
    )
    .expect_err("times out");
    assert!(matches!(slow, CaError::Unavailable(_)));
    assert!(slow.to_string().contains("did not answer"), "{slow}");

    let bad = exec::describe(
        &["/bin/sh".into(), "-c".into(), "echo '{}'".into()],
        std::time::Duration::from_secs(5),
    )
    .expect_err("missing fields");
    assert!(matches!(bad, CaError::FailedPrecondition(_)));
    assert!(bad.to_string().contains("bundle_pem"), "{bad}");

    // A signer that is down: the first call waits for it, later calls are
    // refused at once instead of piling up behind it.
    let store = store_with(ClusterCaConfigFull::default())?;
    let down = exec::ExecCa::new(
        vec!["/bin/sh".into(), "-c".into(), "sleep 1; exit 7".into()],
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(60),
    );
    let first = down.ensure(&store).expect_err("down");
    assert!(first.to_string().contains("exited"), "{first}");
    let started = std::time::Instant::now();
    let second = down.ensure(&store).expect_err("still down");
    assert!(matches!(second, CaError::Unavailable(_)));
    assert!(
        started.elapsed() < std::time::Duration::from_millis(500),
        "the second call waited {:?}",
        started.elapsed()
    );
    assert!(down.problem().is_some());
    Ok(())
}

// ── verification ─────────────────────────────────────────────────────────

/// A leaf for `csr_pem` signed by `pki`'s intermediate, starting from what
/// `spec` asks for and then changed by `edit`.
fn craft(
    pki: &TestPki,
    csr_pem: &str,
    spec: &LeafSpec,
    edit: impl FnOnce(&mut CertificateParams),
) -> R<String> {
    let csr = CertificateSigningRequestParams::from_pem(csr_pem)?;
    let issuer_key = KeyPair::from_pem(&pki.inter_key)?;
    let issuer = CertificateParams::from_ca_cert_pem(&pki.inter_pem)?.self_signed(&issuer_key)?;
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, spec.common_name.clone());
    params.distinguished_name = dn;
    let mut sans = Vec::new();
    for dns in &spec.dns_names {
        sans.push(SanType::DnsName(Ia5String::try_from(dns.clone())?));
    }
    for uri in &spec.uris {
        sans.push(SanType::URI(Ia5String::try_from(uri.clone())?));
    }
    params.subject_alt_names = sans;
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::minutes(1);
    params.not_after = now + time::Duration::hours(24);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    edit(&mut params);
    Ok(params
        .signed_by(&csr.public_key, &issuer, &issuer_key)?
        .pem())
}

#[test]
fn verification_refuses_each_kind_of_bad_certificate_test() -> R<()> {
    let pki = pki("acme")?;
    let view = assemble_view("exec", "test".into(), &pki.inter_pem, &pki.root_pem)
        .map_err(|err| anyhow!(err))?;
    let spec = LeafSpec::for_subject(&CsrSubject::Node("node-1".into()));
    let (_, csr) = generate_csr("node-1")?;
    let now = time::OffsetDateTime::now_utc();
    let check = |pem: &str, spec: &LeafSpec| verify_issued(spec, &csr, pem, &view, now);

    // The control: what a good CA returns.
    let good = craft(&pki, &csr, &spec, |_| {})?;
    check(&good, &spec).map_err(|r| anyhow!("{r}"))?;

    assert!(matches!(check("", &spec), Err(Refusal::NoCertificate(_))));

    let ca = craft(&pki, &csr, &spec, |p| {
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained)
    })?;
    assert!(matches!(check(&ca, &spec), Err(Refusal::IsCa(_))));

    let signs = craft(&pki, &csr, &spec, |p| {
        p.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ]
    })?;
    assert!(matches!(check(&signs, &spec), Err(Refusal::IsCa(_))));

    let (_, other_csr) = generate_csr("node-1")?;
    let other_key = craft(&pki, &other_csr, &spec, |_| {})?;
    assert_eq!(check(&other_key, &spec), Err(Refusal::WrongKey));

    let other_node = LeafSpec::for_subject(&CsrSubject::Node("node-2".into()));
    let renamed = craft(&pki, &csr, &other_node, |_| {})?;
    assert!(matches!(
        check(&renamed, &spec),
        Err(Refusal::DnsSans { .. })
    ));

    let extra_uri = craft(&pki, &csr, &spec, |p| {
        p.subject_alt_names.push(SanType::URI(
            Ia5String::try_from("legion://manager/evil".to_string()).expect("ia5"),
        ))
    })?;
    let refusal = check(&extra_uri, &spec).expect_err("extra URI");
    assert!(matches!(refusal, Refusal::UriSans { .. }));
    assert!(
        refusal.to_string().contains("legion://manager/evil"),
        "{refusal}"
    );

    let ip = craft(&pki, &csr, &spec, |p| {
        p.subject_alt_names
            .push(SanType::IpAddress("10.0.0.1".parse().expect("ip")))
    })?;
    assert!(matches!(check(&ip, &spec), Err(Refusal::OtherSans(_))));

    let no_eku = craft(&pki, &csr, &spec, |p| p.extended_key_usages.clear())?;
    let refusal = check(&no_eku, &spec).expect_err("no EKU");
    assert!(
        refusal.to_string().contains("no extended key usage"),
        "{refusal}"
    );

    let code_signing = craft(&pki, &csr, &spec, |p| {
        p.extended_key_usages
            .push(ExtendedKeyUsagePurpose::CodeSigning)
    })?;
    let refusal = check(&code_signing, &spec).expect_err("extra EKU");
    assert!(refusal.to_string().contains("allows more"), "{refusal}");

    let client_only = craft(&pki, &csr, &spec, |p| {
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth]
    })?;
    let refusal = check(&client_only, &spec).expect_err("lacks serverAuth");
    assert!(
        refusal.to_string().contains("lacks serverAuth"),
        "{refusal}"
    );

    // A manager client certificate may carry serverAuth as well (Vault's
    // default role flags; calls entry 7), and still has no server name.
    let client_spec = LeafSpec::for_manager("mgr-a", ManagerCertKind::Client);
    let both = craft(&pki, &csr, &client_spec, |_| {})?;
    check(&both, &client_spec).map_err(|r| anyhow!("{r}"))?;

    let future = craft(&pki, &csr, &spec, |p| {
        p.not_before = now + time::Duration::hours(1);
        p.not_after = now + time::Duration::hours(24);
    })?;
    assert!(matches!(
        check(&future, &spec),
        Err(Refusal::NotYetValid { .. })
    ));

    let long = craft(&pki, &csr, &spec, |p| {
        p.not_after = now + time::Duration::days(3)
    })?;
    assert!(matches!(check(&long, &spec), Err(Refusal::TooLong { .. })));

    let short = craft(&pki, &csr, &spec, |p| {
        p.not_after = now + time::Duration::hours(1)
    })?;
    assert!(matches!(
        check(&short, &spec),
        Err(Refusal::TooShort { .. })
    ));

    // Signed by a CA outside the bundle.
    let stranger = pki_other()?;
    let foreign = craft(&stranger, &csr, &spec, |_| {})?;
    let refusal = check(&foreign, &spec).expect_err("foreign chain");
    assert!(refusal.is_chain_failure(), "{refusal}");
    Ok(())
}

#[test]
fn assembling_a_view_finds_the_anchor_by_signature_test() -> R<()> {
    let pki = pki("acme")?;
    // A second root with the very same name (a renewed root) but another
    // key: only the one that really signed the intermediate is the anchor.
    let renewed = {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::from_ca_cert_pem(&pki.root_pem)?;
        params.key_identifier_method = rcgen::KeyIdMethod::Sha256;
        params.self_signed(&key)?.pem()
    };
    let bundle = format!("{renewed}{}", pki.root_pem);
    let view = assemble_view("file", String::new(), &pki.inter_pem, &bundle)
        .map_err(|err| anyhow!(err))?;
    assert_eq!(view.pin, ca_pin_pem(&pki.root_pem)?);
    assert_eq!(view.bundle_pins.len(), 2);
    assert_eq!(view.bundle_pins[0], view.pin, "anchor first");

    // A chain that includes its root stops at the bundle.
    let view = assemble_view(
        "file",
        String::new(),
        &format!("{}{}", pki.inter_pem, pki.root_pem),
        &pki.root_pem,
    )
    .map_err(|err| anyhow!(err))?;
    assert_eq!(
        crate::cluster_tls::bundle_pins(&view.chain_pem),
        vec![ca_pin_pem(&pki.inter_pem)?]
    );
    Ok(())
}

// ── agreement ────────────────────────────────────────────────────────────

fn record(provider: &str, anchor: &str, bundle: &[&str]) -> ClusterCaRecord {
    ClusterCaRecord {
        provider: provider.into(),
        anchor_pin: anchor.into(),
        bundle_pins: bundle.iter().map(|p| p.to_string()).collect(),
        recorded_by: "mgr-1".into(),
        recorded_at: "2026-10-14T00:00:00Z".into(),
    }
}

#[test]
fn managers_agree_when_their_cas_trust_each_other_test() {
    let (a, b) = ("sha256:aaaa", "sha256:bbbb");
    assert!(check_agreement(&record("builtin", a, &[a]), &record("builtin", b, &[b])).is_ok());
    let err = check_agreement(&record("builtin", a, &[a]), &record("file", b, &[b])).unwrap_err();
    assert!(
        err.contains("built-in cluster CA") && err.contains("mgr-1"),
        "{err}"
    );
    let err = check_agreement(&record("exec", a, &[a]), &record("builtin", b, &[b])).unwrap_err();
    assert!(err.contains("uses the built-in CA"), "{err}");

    // Same CA.
    assert!(check_agreement(&record("file", a, &[a]), &record("file", a, &[a])).is_ok());
    // Mid-rotation: both CAs in both bundles, either anchor.
    assert!(check_agreement(&record("file", a, &[a, b]), &record("file", b, &[b, a])).is_ok());
    // A file manager and an exec manager on the same CA.
    assert!(check_agreement(&record("file", a, &[a]), &record("exec", a, &[a])).is_ok());
    // This manager switched before the new CA was added everywhere.
    let err = check_agreement(&record("file", b, &[b]), &record("file", a, &[a])).unwrap_err();
    assert!(err.contains("do not trust each other"), "{err}");
    assert!(err.contains("lacks this manager's CA"), "{err}");
    // The cluster switched; this manager never got the new CA.
    let err = check_agreement(&record("file", a, &[a]), &record("file", b, &[a, b])).unwrap_err();
    assert!(err.contains("lacks the cluster's CA"), "{err}");
}

#[test]
fn a_manager_that_disagrees_with_the_leaders_record_refuses_test() -> R<()> {
    let pki = pki("acme")?;
    let dir = tempfile::tempdir()?;
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let store = store_with(file_config(&cert, &key, None))?;
    let ca = store.cluster_ca().clone();
    let view = ca.ensure(&store)?;
    // Leading, this manager recorded its own view.
    let recorded = store.cluster_ca_record()?.context("recorded")?;
    assert_eq!(recorded.anchor_pin, view.pin);
    assert_eq!(recorded.provider, "file");

    // Another manager led and recorded a CA this one does not trust.
    let stranger = pki_other()?;
    let theirs = ca_pin_pem(&stranger.inter_pem)?;
    store.record_cluster_ca(&record("file", &theirs, &[&theirs]))?;
    let err = ca.agree_as(&store, &view, false).unwrap_err();
    assert!(err.contains("recorded by manager mgr-1"), "{err}");
    assert!(err.contains("do not trust each other"), "{err}");

    // A leader's own view replaces the record instead.
    ca.agree_as(&store, &view, true)
        .map_err(|err| anyhow!(err))?;
    assert_eq!(
        store.cluster_ca_record()?.context("recorded")?.anchor_pin,
        view.pin
    );

    // A built-in manager in that cluster refuses too.
    let builtin = ClusterCa::from_config(&ClusterCaConfigFull::default())?;
    let builtin_view = builtin::view_of(&store.ensure_mesh_ca()?)?;
    let err = builtin.agree_as(&store, &builtin_view, false).unwrap_err();
    assert!(err.contains("built-in cluster CA"), "{err}");
    Ok(())
}

// ── conformance harness ──────────────────────────────────────────────────

#[cfg(unix)]
#[test]
fn conformance_harness_passes_a_good_signer_and_catches_csr_names_test() -> R<()> {
    use conformance::{test_provider, Candidate};
    let pki = pki("vault")?;
    let dir = tempfile::tempdir()?;
    let command = exec_helper(
        dir.path(),
        &pki.inter_pem,
        &pki.inter_key,
        &pki.root_pem,
        "good",
    )?;
    let candidate = Candidate::Exec {
        command,
        timeout: std::time::Duration::from_secs(30),
    };
    let checks = test_provider(&candidate);
    let names: Vec<&str> = checks.iter().map(|c| c.name).collect();
    assert_eq!(
        names,
        vec![
            "describe",
            "sign node",
            "sign manager server",
            "sign manager client",
            "requested names win"
        ]
    );
    assert!(checks.iter().all(|c| c.passed), "{checks:#?}");
    assert!(candidate.config_line().contains("provider = \"exec\""));

    // A signer that copies the CSR's names fails exactly one check.
    set_mode(dir.path(), "copy-csr")?;
    let checks = test_provider(&candidate);
    let failed: Vec<&conformance::ProviderCheck> = checks.iter().filter(|c| !c.passed).collect();
    assert_eq!(failed.len(), 1, "{checks:#?}");
    assert_eq!(failed[0].name, "requested names win");
    assert!(
        failed[0].detail.contains("manager.legion"),
        "{}",
        failed[0].detail
    );

    // A signer that cannot run stops at describe.
    set_mode(dir.path(), "fail")?;
    let checks = test_provider(&candidate);
    assert_eq!(checks.len(), 1);
    assert!(!checks[0].passed);

    // File material is tested the same way.
    let cert = write(dir.path(), "cluster-ca.pem", &pki.inter_pem)?;
    let key = write(dir.path(), "cluster-ca.key", &pki.inter_key)?;
    let checks = test_provider(&Candidate::File {
        cert_path: cert,
        key_path: key,
        bundle_path: None,
    });
    assert_eq!(checks[0].name, "load");
    assert!(checks.iter().all(|c| c.passed), "{checks:#?}");
    Ok(())
}

// ── config ───────────────────────────────────────────────────────────────

#[test]
fn the_cluster_ca_block_is_validated_by_field_test() {
    let base = ClusterCaConfigFull::default();
    assert!(base.validate().is_ok());
    let err = |config: ClusterCaConfigFull| format!("{:#}", config.validate().unwrap_err());

    let message = err(ClusterCaConfigFull {
        provider: "file".into(),
        ..base.clone()
    });
    assert!(
        message.contains("certPath") && message.contains("keyPath"),
        "{message}"
    );
    let message = err(ClusterCaConfigFull {
        provider: "exec".into(),
        ..base.clone()
    });
    assert!(message.contains("command"), "{message}");
    let message = err(ClusterCaConfigFull {
        provider: "vault".into(),
        ..base.clone()
    });
    assert!(message.contains("not one of"), "{message}");
    let message = err(ClusterCaConfigFull {
        cert_path: "/etc/legion/ca.pem".into(),
        ..base.clone()
    });
    assert!(message.contains("does not use it"), "{message}");
    let message = err(ClusterCaConfigFull {
        provider: "exec".into(),
        command: vec!["x".into()],
        timeout: "soon".into(),
        ..base.clone()
    });
    assert!(message.contains("clusterCa.timeout"), "{message}");

    // A misspelled field fails the parse rather than being ignored.
    let parsed: Result<crate::structs::config::ClusterCaConfig, _> =
        serde_json::from_value(json!({ "provider": "file", "certFile": "/x" }));
    assert!(parsed.is_err());
    let parsed: crate::structs::config::ClusterCaConfig = serde_json::from_value(json!({
        "provider": "File", "certPath": "/a.pem", "keyPath": "/a.key"
    }))
    .expect("parses");
    let full = parsed.with_defaults();
    assert_eq!(full.provider, "file");
    assert!(full.validate().is_ok());
}
