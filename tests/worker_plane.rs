//! The worker plane (internal_docs/plans/worker-plane-auth-plan.md §6.1–§6.4):
//! every `Worker` RPC has a caller class, the mTLS listener admits exactly
//! the classes each RPC lists, and the manager's one client factory verifies
//! the node it dials.
//!
//! 1. `every_worker_rpc_is_classified` parses `proto/legion.proto` and fails
//!    when a `Worker` RPC has no entry in `WORKER_RPC_CALLERS` (the table the
//!    handlers themselves consult), the way `tests/acl_endpoints.rs` guards
//!    the Manager service.
//! 2. A request per caller class against the real `WorkerDaemonService`
//!    checks the table against the handlers: a manager certificate is
//!    admitted, a node certificate and no certificate are refused for every
//!    manager-only RPC, and `StartClaim` reaches its ticket check either way.
//! 3. The factory (`legion::manager::worker_plane::target`) dials with a
//!    manager certificate it minted itself, refuses a server certificate for
//!    the wrong node, and explains an unreachable worker by node, address,
//!    and port.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use legion::manager::{mesh_ca, worker_plane};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, worker_client::WorkerClient, ExecClientFrame, GetTaskLogsRequest,
    NodeMsg, PostWorkRequest, RestartWorkerRequest, StartClaimRequest, StopSessionsRequest,
};
use legion::worker::plane::{serve_on, Caller, NodeIdentity, WorkerPlaneTls, WORKER_RPC_CALLERS};
use legion::worker::server::WorkerDaemonService;
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::WorkerTaskManager;
use serde_json::json;
use tempfile::TempDir;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::Code;

type R<T> = anyhow::Result<T>;

#[test]
fn every_worker_rpc_is_classified() -> R<()> {
    let proto_path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/legion.proto");
    let proto = std::fs::read_to_string(proto_path)?;
    let start = proto
        .find("service Worker {")
        .context("service Worker not found in proto")?;
    let body = &proto[start..];
    let body = &body[..body
        .find("\n}")
        .context("end of service Worker not found")?];
    let rpc = regex::Regex::new(r"rpc\s+(\w+)\s*\(")?;
    let proto_rpcs: BTreeSet<String> = rpc
        .captures_iter(body)
        .map(|capture| capture[1].to_string())
        .collect();
    assert!(!proto_rpcs.is_empty(), "parsed no Worker RPCs");
    let table: BTreeSet<String> = WORKER_RPC_CALLERS
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();
    let missing: Vec<_> = proto_rpcs.difference(&table).collect();
    let stale: Vec<_> = table.difference(&proto_rpcs).collect();
    assert!(
        missing.is_empty(),
        "Worker RPCs with no caller class (add them to WORKER_RPC_CALLERS in \
         src/worker/plane.rs and call `authorize` first in the handler): {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "WORKER_RPC_CALLERS entries with no matching proto RPC: {stale:?}"
    );
    for (name, callers) in WORKER_RPC_CALLERS {
        assert!(!callers.is_empty(), "{name} admits no caller class");
    }
    Ok(())
}

struct Ca {
    cert: String,
    key: String,
}

fn ca() -> R<Ca> {
    let (cert, key) = mesh_ca::generate_ca()?;
    Ok(Ca { cert, key })
}

fn node_identity(ca: &Ca, node_id: &str) -> R<NodeIdentity> {
    let (csr, key) = mesh_ca::node_csr(node_id)?;
    let (cert, _) = mesh_ca::sign_node_csr(&ca.cert, &ca.key, node_id, &csr)?;
    NodeIdentity::new(node_id, cert, key, ca.cert.clone())
}

fn node_cert(ca: &Ca, node_id: &str) -> R<(String, String)> {
    let (csr, key) = mesh_ca::node_csr(node_id)?;
    let (cert, _) = mesh_ca::sign_node_csr(&ca.cert, &ca.key, node_id, &csr)?;
    Ok((cert, key))
}

/// A real worker service over the real mTLS listener. The TLS handle is
/// returned so a test can watch the rotation signal.
async fn serve_worker(
    dir: &TempDir,
    identity: NodeIdentity,
) -> R<(
    SocketAddr,
    Arc<WorkerPlaneTls>,
    tokio::task::JoinHandle<R<()>>,
)> {
    let task_manager = WorkerTaskManager::with_state_store(WorkerStateStore::new(
        dir.path().join("worker_state.json"),
    ));
    let service = WorkerDaemonService {
        task_manager: Arc::new(Mutex::new(task_manager)),
        identity: None,
        edge_boot: None,
    };
    let tls = WorkerPlaneTls::new();
    tls.install(identity)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve_on(listener, tls.clone(), service));
    Ok((addr, tls, handle))
}

/// A client that trusts `ca`, expects `server_node`, and presents
/// `identity` (cert, key) when given.
fn client(
    addr: SocketAddr,
    ca: &Ca,
    server_node: &str,
    identity: Option<&(String, String)>,
) -> R<WorkerClient<Channel>> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&ca.cert))
        .domain_name(mesh_ca::node_dns_name(server_node));
    if let Some((cert, key)) = identity {
        tls = tls.identity(Identity::from_pem(cert, key));
    }
    let channel = Endpoint::from_shared(format!("https://{addr}"))?
        .tls_config(tls)?
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy();
    Ok(WorkerClient::new(channel))
}

/// Call `rpc` with a request that is harmless when admitted. RestartWorker
/// is only ever sent by callers expected to be refused: admitted, it exits
/// the process.
async fn call(client: &mut WorkerClient<Channel>, rpc: &str) -> Result<(), tonic::Status> {
    match rpc {
        "PostWork" => client
            .post_work(PostWorkRequest { allocation: None })
            .await
            .map(|_| ()),
        "GetTaskLogs" => client
            .get_task_logs(GetTaskLogsRequest {
                allocation_id: 424_242,
                ..Default::default()
            })
            .await
            .map(|_| ()),
        "ExecTask" => client
            .exec_task(tokio_stream::iter(Vec::<ExecClientFrame>::new()))
            .await
            .map(|_| ()),
        "RestartWorker" => client
            .restart_worker(RestartWorkerRequest {
                node_id: "wkr-1".into(),
            })
            .await
            .map(|_| ()),
        "StartClaim" => client
            .start_claim(StartClaimRequest {
                ticket: "not-a-ticket".into(),
                instance_id: "i-1".into(),
                ..Default::default()
            })
            .await
            .map(|_| ()),
        "StopSessions" => client
            .stop_sessions(StopSessionsRequest {
                session_ids: vec!["workspace-not-running".into()],
                reason: "stop".into(),
            })
            .await
            .map(|_| ()),
        other => panic!("no test request for {other}; add one"),
    }
}

/// The table, checked against the handlers: each listed class is admitted
/// and each unlisted class refused, with the message the plan names.
#[tokio::test(flavor = "multi_thread")]
async fn caller_classes_match_the_handlers() -> R<()> {
    let dir = TempDir::new()?;
    let ca = ca()?;
    let (addr, _tls, server) = serve_worker(&dir, node_identity(&ca, "wkr-1")?).await?;
    let (manager_cert, manager_key, _) = mesh_ca::issue_manager_cert(&ca.cert, &ca.key, "mgr-1")?;
    let manager = (manager_cert, manager_key);
    let other_node = node_cert(&ca, "wkr-2")?;

    for (rpc, callers) in WORKER_RPC_CALLERS {
        let manager_only = !callers.contains(&Caller::Ticket);

        for (who, identity) in [
            ("no certificate", None),
            ("a node certificate", Some(&other_node)),
        ] {
            let result = call(&mut client(addr, &ca, "wkr-1", identity)?, rpc).await;
            if manager_only {
                let status = result.expect_err(&format!("{rpc} must refuse {who}"));
                assert_eq!(
                    status.code(),
                    Code::Unauthenticated,
                    "{rpc} with {who}: {}",
                    status.message()
                );
                assert!(
                    status.message().starts_with(&format!(
                        "{rpc} is a manager-only worker RPC: present a Legion manager certificate"
                    )),
                    "{rpc} with {who}: {}",
                    status.message()
                );
            } else if let Err(status) = result {
                assert_ne!(
                    status.code(),
                    Code::Unauthenticated,
                    "{rpc} admits ticket holders with {who}: {}",
                    status.message()
                );
            }
        }

        if *rpc == "RestartWorker" {
            continue; // admitted, it would exit this test process
        }
        if let Err(status) = call(&mut client(addr, &ca, "wkr-1", Some(&manager))?, rpc).await {
            assert_ne!(
                status.code(),
                Code::Unauthenticated,
                "{rpc} must admit a manager certificate: {}",
                status.message()
            );
        }
    }

    // The ticket path is reached with no certificate at all: this node did
    // not opt into edge boot, which is a FailedPrecondition, not an auth
    // refusal.
    let status = call(&mut client(addr, &ca, "wkr-1", None)?, "StartClaim")
        .await
        .expect_err("edge boot is off on this node");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "{}",
        status.message()
    );

    server.abort();
    Ok(())
}

/// A client certificate from another CA never reaches a handler, and the
/// refused handshake asks the node to re-issue its own certificate (what a
/// `legion net ca rotate` looks like from a node that has not refreshed).
#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_ca_is_refused_at_the_handshake_and_signals_a_refresh() -> R<()> {
    let dir = TempDir::new()?;
    let ca = ca()?;
    let (addr, tls, server) = serve_worker(&dir, node_identity(&ca, "wkr-1")?).await?;
    assert!(!tls.needs_refresh());

    let foreign = self::ca()?;
    let (cert, key, _) = mesh_ca::issue_manager_cert(&foreign.cert, &foreign.key, "mgr-x")?;
    let status = call(
        &mut client(addr, &ca, "wkr-1", Some(&(cert, key)))?,
        "GetTaskLogs",
    )
    .await
    .expect_err("a manager certificate from another CA must not be admitted");
    assert_ne!(status.code(), Code::Ok);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !tls.needs_refresh() {
        assert!(
            std::time::Instant::now() < deadline,
            "an unknown-issuer handshake must request a node certificate refresh"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.abort();
    Ok(())
}

fn store(dir: &TempDir) -> R<Store> {
    Store::start_storage(&ManagerConfigFull {
        store_path: dir
            .path()
            .join("store")
            .to_str()
            .context("utf-8 path")?
            .to_string(),
        ..Default::default()
    })
}

fn register(store: &Store, node_id: &str, address: &str) -> R<legion::structs::Node> {
    store.insert_node(NodeMsg {
        id: node_id.into(),
        status: "active".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "workerAddress": address }).to_string(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: json!({ "protocolVersion": 3 }).to_string(),
        health_checks: "{}".into(),
    })?;
    store.get_node(node_id)
}

/// The manager side end to end: the factory mints a manager certificate from
/// the store's CA and is admitted; a listener serving another node's
/// certificate at a registered address is refused; a closed port gets the
/// actionable message; a plaintext address names the protocol.
#[tokio::test(flavor = "multi_thread")]
async fn the_factory_verifies_the_node_it_dials() -> R<()> {
    let dir = TempDir::new()?;
    let store = store(&dir)?;
    let stored = store.ensure_mesh_ca()?;
    let ca = Ca {
        cert: stored.cert_pem.clone(),
        key: stored.key_pem.clone(),
    };
    let worker_dir = TempDir::new()?;
    let (addr, _tls, server) = serve_worker(&worker_dir, node_identity(&ca, "wkr-1")?).await?;

    // The right node: admitted (the allocation is unknown, which is the
    // worker's own answer, not an auth refusal).
    let node = register(&store, "wkr-1", &format!("https://{addr}"))?;
    let target = worker_plane::target(&store, &node).map_err(|err| anyhow::anyhow!("{err}"))?;
    let mut client = target.client.clone();
    if let Err(status) = client
        .get_task_logs(GetTaskLogsRequest {
            allocation_id: 7,
            ..Default::default()
        })
        .await
    {
        assert_ne!(status.code(), Code::Unauthenticated, "{}", status.message());
        assert_ne!(status.code(), Code::Unavailable, "{}", status.message());
    }

    // Another node registered at wkr-1's address: the certificate there
    // names wkr-1, so the dial is refused before any payload is sent.
    let impostor = register(&store, "wkr-2", &format!("https://{addr}"))?;
    let target = worker_plane::target(&store, &impostor).map_err(|err| anyhow::anyhow!("{err}"))?;
    let status = target
        .client
        .clone()
        .post_work(PostWorkRequest { allocation: None })
        .await
        .expect_err("a server certificate for wkr-1 is not wkr-2");
    let explained = target.explain(status);
    eprintln!("wrong node: {}", explained.message());
    assert_eq!(explained.code(), Code::Unavailable);
    assert!(
        explained.message().contains("node wkr-2") && explained.message().contains("TLS"),
        "{}",
        explained.message()
    );

    // Nothing listening: the message names the node, address, and port.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let gone = register(&store, "wkr-3", &format!("https://{closed}"))?;
    let target = worker_plane::target(&store, &gone).map_err(|err| anyhow::anyhow!("{err}"))?;
    let status = target
        .client
        .clone()
        .get_task_logs(GetTaskLogsRequest::default())
        .await
        .expect_err("nothing listens there");
    let explained = target.explain(status);
    assert_eq!(explained.code(), Code::Unavailable);
    let message = explained.message();
    eprintln!("unreachable worker: {message}");
    assert!(
        message.contains(&format!("cannot reach node wkr-3 at https://{closed}"))
            && message.contains(&format!("port {}", closed.port()))
            && message.contains("LEGION_WORKER_ADVERTISE_ADDR"),
        "{message}"
    );

    // A protocol-2 worker's plaintext address is never dialed.
    let old = store.insert_node(NodeMsg {
        id: "wkr-old".into(),
        status: "active".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "workerAddress": "http://127.0.0.1:4848" }).to_string(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: json!({ "protocolVersion": 2 }).to_string(),
        health_checks: "{}".into(),
    });
    old?;
    let err = worker_plane::target(&store, &store.get_node("wkr-old")?)
        .expect_err("plaintext is refused");
    assert!(err.to_string().contains("protocol 2"), "{err}");

    server.abort();
    Ok(())
}
