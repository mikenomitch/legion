//! Caller classes on the worker plane (worker-plane-auth plan §6.1).
//!
//! Every `Worker` RPC declares which callers it admits: `manager` (a client
//! certificate from the cluster CA carrying `legion://manager/<id>`) and/or
//! `ticket` (no client certificate; the request carries a ticket the RPC
//! verifies). This suite is the gate, the way `tests/acl_endpoints.rs` gates
//! the `Manager` service:
//!
//! 1. `every_worker_rpc_is_classified` parses `proto/legion.proto` and fails
//!    when a `Worker` RPC has no entry in [`worker_table`], so a new worker
//!    RPC can't ship without deciding who may call it.
//! 2. `classes_match_the_handlers` serves the real `WorkerDaemonService`
//!    over the real mTLS listener and calls every RPC as every caller:
//!    each listed class gets past authentication, and each unlisted class is
//!    refused with `Unauthenticated`. A node certificate is refused
//!    everywhere it is presented, never treated as "no certificate".
//! 3. Handshake-level checks: a certificate from a foreign CA, or an expired
//!    one, never completes a handshake, and the manager's dial refuses a
//!    worker whose certificate names another node.
//!
//! When you add a Worker RPC: add it to `worker_table` and a request
//! builder in `call`, and start the handler with the class check
//! (`crate::worker::plane::require_manager` or `caller_of`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use legion::cluster_tls::{
    certified_key, client_config, server_config, tls_channel, ClusterClientVerifier,
    NamedServerVerifier, SwappableIdentity, SwappableRoots,
};
use legion::manager::mesh_ca::{
    generate_ca, generate_csr, issue_manager_cert, node_dns_name, sign_csr, CsrSubject,
    ManagerCertKind,
};
use legion::prelude::R;
use legion::structs::worker_client::WorkerClient;
use legion::structs::worker_server::WorkerServer;
use legion::structs::{
    exec_client_frame, ExecClientFrame, ExecStart, GetTaskLogsRequest, PostWorkRequest,
    RestartWorkerRequest, StartClaimRequest,
};
use legion::worker::server::WorkerDaemonService;
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::WorkerTaskManager;
use tonic::transport::Channel;
use tonic::Code;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Caller {
    Manager,
    Ticket,
}

/// The source of truth for who may call each Worker RPC.
fn worker_table() -> Vec<(&'static str, &'static [Caller])> {
    vec![
        ("PostWork", &[Caller::Manager]),
        ("RestartWorker", &[Caller::Manager]),
        ("GetTaskLogs", &[Caller::Manager]),
        // A manager certificate, or (direct exec, `exec.direct`) an exec
        // ticket in the first frame.
        ("ExecTask", &[Caller::Manager, Caller::Ticket]),
        ("StartClaim", &[Caller::Ticket]),
        ("Probe", &[Caller::Manager]),
    ]
}

fn proto_worker_rpcs() -> BTreeSet<String> {
    let proto = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/proto/legion.proto"))
        .expect("read proto");
    let start = proto.find("service Worker {").expect("Worker service");
    let body = &proto[start..];
    let end = body.find("\n}").expect("end of Worker service");
    let rpc = regex::Regex::new(r"(?m)^\s*rpc\s+(\w+)").expect("regex");
    rpc.captures_iter(&body[..end])
        .map(|capture| capture[1].to_string())
        .collect()
}

#[test]
fn every_worker_rpc_is_classified() {
    let proto = proto_worker_rpcs();
    let table: BTreeSet<String> = worker_table()
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();
    let missing: Vec<_> = proto.difference(&table).collect();
    let stale: Vec<_> = table.difference(&proto).collect();
    assert!(
        missing.is_empty(),
        "Worker RPCs with no caller class (add them to worker_table in \
         tests/worker_plane_classes.rs and a class check in the handler): {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "worker_table entries with no matching proto RPC: {stale:?}"
    );
}

/// A cluster: a CA, a worker for `node-1`, and credentials to call it with.
struct Cluster {
    ca_pem: String,
    ca_key: String,
    roots: Arc<SwappableRoots>,
    address: String,
}

fn node_identity(ca_pem: &str, ca_key: &str, node: &str) -> R<Arc<SwappableIdentity>> {
    let (key, csr) = generate_csr(node)?;
    let (cert, _) = sign_csr(ca_pem, ca_key, &csr, &CsrSubject::Node(node.into()))?;
    let identity = SwappableIdentity::new();
    identity.set(certified_key(&cert, &key)?);
    Ok(identity)
}

fn manager_identity(ca_pem: &str, ca_key: &str) -> R<Arc<SwappableIdentity>> {
    let (cert, key, _) = issue_manager_cert(ca_pem, ca_key, "mgr-a", ManagerCertKind::Client)?;
    let identity = SwappableIdentity::new();
    identity.set(certified_key(&cert, &key)?);
    Ok(identity)
}

async fn serve_worker(dir: &tempfile::TempDir) -> R<Cluster> {
    let (ca_pem, ca_key) = generate_ca()?;
    let roots = SwappableRoots::from_pem(&ca_pem)?;
    let config = server_config(
        node_identity(&ca_pem, &ca_key, "node-1")?,
        Some(ClusterClientVerifier::new(roots.clone())),
        false,
    )?;
    let service = WorkerDaemonService {
        node_id: "node-1".into(),
        task_manager: Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(dir.path().join("worker_state.json")),
        ))),
        identity: None,
        edge_boot: None,
        direct_exec: None,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(WorkerServer::new(service))
            .serve_with_incoming(legion::cluster_tls::tls_incoming(listener, config))
            .await;
    });
    Ok(Cluster {
        ca_pem,
        ca_key,
        roots,
        address,
    })
}

/// A client dialing node-1, verifying its name, presenting `identity`.
fn client(cluster: &Cluster, identity: Option<Arc<SwappableIdentity>>) -> R<WorkerClient<Channel>> {
    let name = node_dns_name("node-1");
    let verifier = NamedServerVerifier::new(cluster.roots.clone(), &name)?;
    let config = client_config(verifier, identity)?;
    Ok(WorkerClient::new(tls_channel(
        &cluster.address,
        config,
        &name,
    )?))
}

/// Calls `rpc` with a request that gets past authentication but fails
/// harmlessly in the handler (no allocation, a wrong node id, an unknown
/// allocation), and returns the status code (Ok for success).
async fn call(client: &mut WorkerClient<Channel>, rpc: &str) -> Code {
    let result: Result<(), tonic::Status> = match rpc {
        "PostWork" => client
            .post_work(PostWorkRequest { allocation: None })
            .await
            .map(|_| ()),
        // A node id that is not this worker's: refused after the class
        // check, so a test can't restart the test process.
        "RestartWorker" => client
            .restart_worker(RestartWorkerRequest {
                node_id: "some-other-node".into(),
            })
            .await
            .map(|_| ()),
        "GetTaskLogs" => client
            .get_task_logs(GetTaskLogsRequest {
                allocation_id: 404,
                ..Default::default()
            })
            .await
            .map(|_| ()),
        "ExecTask" => {
            let frame = ExecClientFrame {
                frame: Some(exec_client_frame::Frame::Start(ExecStart {
                    allocation_id: 404,
                    command: vec!["true".into()],
                    // Ignored on the manager leg; on the ticket leg it gets
                    // the request past the class check to ticket handling.
                    exec_ticket: "not-a-ticket".into(),
                    ..Default::default()
                })),
            };
            client
                .exec_task(tokio_stream::iter(vec![frame]))
                .await
                .map(|_| ())
        }
        "StartClaim" => client
            .start_claim(StartClaimRequest {
                instance_id: "i-1".into(),
                ..Default::default()
            })
            .await
            .map(|_| ()),
        "Probe" => client
            .probe(legion::structs::ProbeRequest {})
            .await
            .map(|_| ()),
        other => panic!("no request builder for Worker RPC {other}"),
    };
    match result {
        Ok(()) => Code::Ok,
        Err(status) => status.code(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn classes_match_the_handlers() -> R<()> {
    let dir = tempfile::tempdir()?;
    let cluster = serve_worker(&dir).await?;
    let mut as_manager = client(
        &cluster,
        Some(manager_identity(&cluster.ca_pem, &cluster.ca_key)?),
    )?;
    let mut as_ticket = client(&cluster, None)?;
    let mut as_node = client(
        &cluster,
        Some(node_identity(&cluster.ca_pem, &cluster.ca_key, "node-2")?),
    )?;

    for (rpc, classes) in worker_table() {
        let manager = call(&mut as_manager, rpc).await;
        let ticket = call(&mut as_ticket, rpc).await;
        let node = call(&mut as_node, rpc).await;
        if classes.contains(&Caller::Manager) {
            assert_ne!(manager, Code::Unauthenticated, "{rpc} must admit a manager");
        } else if rpc != "StartClaim" {
            assert_eq!(
                manager,
                Code::Unauthenticated,
                "{rpc} must refuse a manager"
            );
        }
        if classes.contains(&Caller::Ticket) {
            // Admitted to ticket verification: the class check passed, so any
            // refusal is about the (missing) ticket or the node's config, not
            // the caller's class. `StartClaim` without `edgeBoot.acceptClaims`
            // and `ExecTask` without `exec.direct` say so by name.
            assert_ne!(
                ticket,
                Code::Ok,
                "{rpc} must not succeed without a valid ticket"
            );
        } else {
            assert_eq!(
                ticket,
                Code::Unauthenticated,
                "{rpc} must refuse a caller with no manager certificate"
            );
        }
        // A node certificate is not a manager certificate, and is never
        // downgraded to the ticket path. StartClaim ignores certificates.
        if rpc == "StartClaim" {
            assert_ne!(node, Code::Ok, "{rpc} without a ticket must not succeed");
        } else {
            assert_eq!(
                node,
                Code::Unauthenticated,
                "{rpc} must refuse a node certificate"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn manager_only_refusal_names_the_rule() -> R<()> {
    let dir = tempfile::tempdir()?;
    let cluster = serve_worker(&dir).await?;
    let mut anonymous = client(&cluster, None)?;
    let status = anonymous
        .get_task_logs(GetTaskLogsRequest::default())
        .await
        .expect_err("refused");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert!(
        status
            .message()
            .contains("GetTaskLogs is a manager-only worker RPC"),
        "{}",
        status.message()
    );
    let frame = ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Start(ExecStart {
            allocation_id: 1,
            command: vec!["sh".into()],
            ..Default::default()
        })),
    };
    let status = anonymous
        .exec_task(tokio_stream::iter(vec![frame]))
        .await
        .expect_err("refused");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert!(
        status
            .message()
            .contains("ExecTask admits a manager certificate or an exec ticket"),
        "{}",
        status.message()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_on_a_worker_without_direct_exec_names_the_setting() -> R<()> {
    let dir = tempfile::tempdir()?;
    let cluster = serve_worker(&dir).await?;
    let mut anonymous = client(&cluster, None)?;
    let frame = ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Start(ExecStart {
            command: vec!["sh".into()],
            exec_ticket: "eyJ.not.verified".into(),
            ..Default::default()
        })),
    };
    let status = anonymous
        .exec_task(tokio_stream::iter(vec![frame]))
        .await
        .expect_err("refused");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("exec = { direct = true }"),
        "{}",
        status.message()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_and_expired_certificates_never_complete_a_handshake() -> R<()> {
    let dir = tempfile::tempdir()?;
    let cluster = serve_worker(&dir).await?;

    // A manager certificate from another cluster's CA.
    let (foreign_pem, foreign_key) = generate_ca()?;
    let mut foreign = client(
        &cluster,
        Some(manager_identity(&foreign_pem, &foreign_key)?),
    )?;
    let status = foreign
        .post_work(PostWorkRequest { allocation: None })
        .await
        .expect_err("a foreign CA must fail the handshake");
    assert_ne!(status.code(), Code::Ok);
    assert_ne!(status.code(), Code::InvalidArgument, "reached the handler");

    // An expired manager certificate from the right CA.
    let key = rcgen::KeyPair::generate()?;
    let mut params = rcgen::CertificateParams::default();
    params.subject_alt_names = vec![rcgen::SanType::URI(rcgen::Ia5String::try_from(
        "legion://manager/old".to_string(),
    )?)];
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    params.not_before = time::OffsetDateTime::now_utc() - time::Duration::days(3);
    params.not_after = time::OffsetDateTime::now_utc() - time::Duration::days(1);
    let ca_key = rcgen::KeyPair::from_pem(&cluster.ca_key)?;
    let ca = rcgen::CertificateParams::from_ca_cert_pem(&cluster.ca_pem)?.self_signed(&ca_key)?;
    let expired_cert = params.signed_by(&key, &ca, &ca_key)?;
    let expired = SwappableIdentity::new();
    expired.set(certified_key(&expired_cert.pem(), &key.serialize_pem())?);
    let mut expired_client = client(&cluster, Some(expired))?;
    let status = expired_client
        .post_work(PostWorkRequest { allocation: None })
        .await
        .expect_err("an expired certificate must fail the handshake");
    assert_ne!(status.code(), Code::InvalidArgument, "reached the handler");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_manager_refuses_a_worker_whose_certificate_names_another_node() -> R<()> {
    let dir = tempfile::tempdir()?;
    let cluster = serve_worker(&dir).await?;
    // Dial node-1's address expecting node-2: the worker presents node-1's
    // certificate, so the handshake fails before any payload is sent.
    let name = node_dns_name("node-2");
    let verifier = NamedServerVerifier::new(cluster.roots.clone(), &name)?;
    let config = client_config(
        verifier,
        Some(manager_identity(&cluster.ca_pem, &cluster.ca_key)?),
    )?;
    let mut wrong = WorkerClient::new(tls_channel(&cluster.address, config, &name)?);
    let status = wrong
        .post_work(PostWorkRequest { allocation: None })
        .await
        .expect_err("the wrong node must be refused");
    assert_ne!(status.code(), Code::InvalidArgument, "reached the handler");
    Ok(())
}
