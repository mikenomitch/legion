//! Child sessions end to end (docs/child-sessions.md): a real manager on a
//! raft store and a real worker over the mTLS worker plane.
//!
//! What is asserted here is the plan's contract: a child starts under an
//! owner with `job:write` and the owner's declaration, on its owner's node,
//! with no raft write; it is listed under its owner; it ends when its owner
//! ends (on the node for a session owner, on the managers' notice for an
//! allocation, an instance, and a session that moved away); promotion
//! carries ownership; and a workload starts, lists, and stops its own
//! children over the API socket's `Workload` service with no credential.
//!
//! The node-side admission and cascade rules are unit-tested in
//! `legion-worker` (`task_manager::tests`, "Child sessions"), and the
//! submit checks in `legion-common` (`structs::children`).

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use legion::cluster_tls::{
    certified_key, server_config, tls_incoming, ClusterClientVerifier, SwappableIdentity,
    SwappableRoots,
};
use legion::manager::eval::EvalExt;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::Issuer;
use legion::mesh_ca::{generate_csr, sign_csr, CsrSubject};
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::Store;
use legion::structs::children::{owner_ref, OWNER_ALLOCATION, OWNER_INSTANCE, OWNER_SESSION};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::JobConfig;
use legion::structs::manager_server::Manager;
use legion::structs::worker_server::WorkerServer;
use legion::structs::workload_server::Workload;
use legion::structs::{
    CreateInstanceReply, CreateInstanceRequest, GetInstanceRequest, HeartbeatRequest, InstanceInfo,
    ListChildrenRequest, Node, NodeMsg, OwnerNotice, OwnerRef, PromoteInstanceRequest,
    RatifyClaimReply, RatifyClaimRequest, RunSessionRequest, SessionReport,
    StartChildSessionRequest, StopChildSessionRequest, StopInstanceRequest, Submission,
};
use legion::worker::api_socket::{AllocationWorkload, WorkloadChannel};
use legion::worker::boot_ticket::{
    BootTicketVerifier, JwksSource, DEFAULT_CLOCK_SKEW, DEFAULT_JWKS_REFRESH,
};
use legion::worker::edge;
use legion::worker::server::{ClaimRatifier, EdgeBootRuntime, WorkerDaemonService};
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::WorkerTaskManager;
use tonic::{Code, Request, Status};

const JOB: &str = "agents";
/// The owner session task: its sessions may own two `machine` sessions.
const AGENT: &str = "agent";
/// The child session task.
const TASK: &str = "machine";
/// A service task whose allocation may own `machine` sessions.
const CI: &str = "ci";
/// A keyed task whose instances own session and keyed children.
const KEEPER: &str = "keeper";
/// A keyed task whose instances can be keyed children.
const WORKSPACE: &str = "workspace";
const NODE: &str = "w-17";
const ISSUER: &str = "https://manager.example.com:4747";

struct Jwks(Value);

impl JwksSource for Jwks {
    fn fetch(&self) -> R<Value> {
        Ok(self.0.clone())
    }
}

struct NoRatifier;

#[tonic::async_trait]
impl ClaimRatifier for NoRatifier {
    async fn ratify(&self, _request: RatifyClaimRequest) -> R<RatifyClaimReply> {
        Ok(RatifyClaimReply::default())
    }
}

/// One manager on a raft store and one real worker, with an `exec` session
/// job whose envelope the scheduler placed on the worker.
struct Cluster {
    svc: ManagerDaemonService,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    launch_pool: Arc<legion::worker::launch_pool::LaunchPool>,
    /// Holds the stores and the worker's state for the test's lifetime.
    _dir: tempfile::TempDir,
    /// The epoch of the next session report this test sends for the node.
    epoch: Mutex<u64>,
}

impl Cluster {
    /// One manager, one worker, and the `agents` job placed on it.
    async fn new() -> R<Self> {
        let dir = tempfile::tempdir()?;
        let mut config = ManagerConfigFull {
            store_path: dir.path().join("store").to_str().context("utf-8")?.into(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-children".into();
        config.persistence.raft.data_dir =
            dir.path().join("raft").to_str().context("utf-8")?.into();
        let store = tokio::task::block_in_place(|| Store::start_storage(&config))?;
        let svc = ManagerDaemonService::new(store);

        // The worker: the real task manager and the real service.
        let mut manager = WorkerTaskManager::with_state_store(WorkerStateStore::new(
            dir.path().join("worker").join("worker_state.json"),
        ));
        manager.load_builtin_plugins();
        let task_manager = Arc::new(Mutex::new(manager));
        let issuer = Issuer::new(
            ISSUER,
            "/.well-known/jwks.json",
            Duration::from_secs(3600),
            None,
            SystemTime::now(),
        )?;
        let launch_pool = Arc::new(legion::worker::launch_pool::LaunchPool::new(
            8,
            Duration::from_secs(1),
        ));
        let edge_boot = Arc::new(EdgeBootRuntime {
            verifier: Arc::new(BootTicketVerifier::new(
                Box::new(Jwks(issuer.jwks())),
                ISSUER,
                "default",
                DEFAULT_JWKS_REFRESH,
                DEFAULT_CLOCK_SKEW,
            )),
            spec_cache: edge::SpecCache::default(),
            node_id: NODE.into(),
            ratifier: Arc::new(NoRatifier),
        });
        let service = || WorkerDaemonService {
            node_id: NODE.into(),
            task_manager: task_manager.clone(),
            identity: None,
            edge_boot: Some(edge_boot.clone()),
            direct_exec: None,
            launch_pool: launch_pool.clone(),
            node_identity: None,
            netd: None,
        };

        // Served on the worker plane under the cluster CA, as a node.
        let ca = svc.store.ensure_mesh_ca()?;
        let roots = SwappableRoots::from_pem(&ca.cert_pem)?;
        let (key, csr) = generate_csr(NODE)?;
        let (cert, _) = sign_csr(
            &ca.cert_pem,
            &ca.key_pem,
            &csr,
            &CsrSubject::Node(NODE.to_string()),
        )?;
        let identity = SwappableIdentity::new();
        identity.set(certified_key(&cert, &key)?);
        let tls = server_config(identity, Some(ClusterClientVerifier::new(roots)), false)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let served = WorkerServer::new(service());
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(served)
                .serve_with_incoming(tls_incoming(listener, tls))
                .await;
        });
        svc.store.insert_node(NodeMsg::from(Node {
            id: NODE.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "node_id": NODE,
                "maxAllocations": 50,
                "resources": { "memory": 65536, "cpu": 40000 },
                "workerAddress": format!("https://{address}"),
            }),
            fingerprint_attrs: json!({
                "memory_total": "68719476736",
                "cpus": { "cpu-1": { "frequency": 2000.0 } },
            }),
            dynamic_attrs: json!({ "capabilities": ["sessions"], "protocolVersion": 3 }),
            health_checks: json!({}),
        }))?;

        let cluster = Cluster {
            svc,
            task_manager,
            launch_pool,
            _dir: dir,
            epoch: Mutex::new(0),
        };
        cluster.submit()?;
        cluster.wait_for_envelope().await?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn submit(&self) -> R<()> {
        let session = |name: &str| {
            json!({
                "name": name,
                "runtime": "exec",
                "config": { "command": ["sleep", "600"] },
                "resources": { "cpu": 100, "memory": 64 },
                "stateful": {
                    "mode": "session",
                    "envelope": { "resources": { "cpu": 400, "memory": 256 } },
                },
            })
        };
        let mut agent = session(AGENT);
        agent["children"] = json!({ "tasks": [TASK], "max": 2 });
        let mut machine = session(TASK);
        machine["run"] = json!({ "command": ["sh", "-c", "echo ran"], "wall": "30s" });
        let config: JobConfig = serde_json::from_value(json!({
            "name": JOB,
            "tasks": [
                agent,
                machine,
                {
                    "name": CI,
                    "runtime": "exec",
                    "config": { "command": ["sleep", "600"] },
                    "resources": { "cpu": 100, "memory": 64 },
                    "children": { "tasks": [TASK, WORKSPACE], "max": 8 },
                },
                {
                    "name": KEEPER,
                    "runtime": "exec",
                    "config": { "command": ["sleep", "600"] },
                    "resources": { "cpu": 100, "memory": 64 },
                    "stateful": { "mode": "keyed" },
                    "children": { "tasks": [TASK, WORKSPACE], "max": 2, "suspendWithOwner": true },
                },
                {
                    "name": WORKSPACE,
                    "runtime": "exec",
                    "config": { "command": ["sleep", "600"] },
                    "resources": { "cpu": 100, "memory": 64 },
                    "stateful": { "mode": "keyed" },
                },
            ],
        }))?;
        tokio::task::block_in_place(|| -> R<()> {
            self.store().handle_job_submit(
                &config,
                &[Submission {
                    path: format!("{JOB}.json"),
                    content: "{}".to_string(),
                    is_primary: true,
                }],
            )?;
            let config_id = self.store().get_job_config(JOB)?.id();
            self.store()
                .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
            while let Some(eval) = self.store().claim_eval()? {
                eval.handle(self.store())?;
                self.store().complete_eval(&eval.id, true)?;
            }
            Ok(())
        })
    }

    /// The scheduler placed both envelopes and the `ci` allocation and
    /// pushed them; wait until the worker runs them.
    async fn wait_for_envelope(&self) -> R<()> {
        for _ in 0..200 {
            let ready = {
                let guard = self.task_manager.lock().expect("task manager");
                guard.session_spec(JOB, TASK).is_some()
                    && guard.session_spec(JOB, AGENT).is_some()
                    && self
                        .ci_allocation()
                        .is_some_and(|id| guard.allocation_task(id).is_some())
            };
            if ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("the worker never received the work of job {JOB}")
    }

    /// The `ci` service task's allocation.
    fn ci_allocation(&self) -> Option<i64> {
        let allocations = self.store().allocs_for_job(JOB).ok()?;
        allocations.into_iter().find_map(|allocation| {
            let tasks = self
                .store()
                .shared_task_configs_for_job_config(&allocation.job_config_id)
                .ok()?;
            tasks
                .iter()
                .any(|(id, task)| *id == allocation.task_config_id && task.name == CI)
                .then_some(allocation.id)
        })
    }

    async fn create(
        &self,
        task: &str,
        owner: Option<OwnerRef>,
    ) -> Result<CreateInstanceReply, Status> {
        self.svc
            .create_instance(Request::new(CreateInstanceRequest {
                job_id: JOB.into(),
                task: Some(task.into()),
                owner,
                ..Default::default()
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    /// Create a keyed instance (a child when `owner` is set).
    async fn create_keyed(
        &self,
        task: &str,
        id: &str,
        owner: Option<OwnerRef>,
    ) -> Result<CreateInstanceReply, Status> {
        self.svc
            .create_instance(Request::new(CreateInstanceRequest {
                job_id: JOB.into(),
                task: Some(task.into()),
                instance_id: Some(id.into()),
                owner,
                ..Default::default()
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    /// Run the scheduler until its queue is empty.
    fn run_evals(&self) -> R<()> {
        tokio::task::block_in_place(|| -> R<()> {
            while let Some(eval) = self.store().claim_eval()? {
                eval.handle(self.store())?;
                self.store().complete_eval(&eval.id, true)?;
            }
            Ok(())
        })
    }

    fn instance(&self, id: &str) -> R<legion::structs::Instance> {
        self.store().get_instance(id)?.context("the instance row")
    }

    /// Start a session and wait until the worker runs it.
    async fn start(&self, task: &str, owner: Option<OwnerRef>) -> R<CreateInstanceReply> {
        let reply = self.create(task, owner).await?;
        self.wait_running(&reply.instance_id).await?;
        Ok(reply)
    }

    async fn wait_running(&self, session_id: &str) -> R<()> {
        for _ in 0..200 {
            let state = self
                .task_manager
                .lock()
                .expect("task manager")
                .session_state(session_id);
            if state.as_deref() == Some("running") {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!("session {session_id} never ran")
    }

    async fn get(&self, id: &str) -> Result<InstanceInfo, Status> {
        self.svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: id.into(),
            }))
            .await?
            .into_inner()
            .instance
            .ok_or_else(|| Status::internal("no instance"))
    }

    /// Whether the worker still holds the session.
    fn holds(&self, session_id: &str) -> bool {
        self.task_manager
            .lock()
            .expect("task manager")
            .session_status(session_id)
            .is_some()
    }

    async fn wait_gone(&self, session_id: &str) -> R<()> {
        for _ in 0..100 {
            if !self.holds(session_id) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::bail!("session {session_id} is still on the node after a second")
    }

    /// One heartbeat with the node's full session report, as the worker's
    /// loop sends it. Returns the owner notices the reply carried.
    async fn heartbeat(&self) -> R<Vec<OwnerNotice>> {
        let upserts = self
            .task_manager
            .lock()
            .expect("task manager")
            .session_statuses();
        let epoch = {
            let mut epoch = self.epoch.lock().expect("epoch");
            *epoch += 1;
            *epoch
        };
        let reply = self
            .svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: NODE.into(),
                session_report: Some(SessionReport {
                    epoch,
                    base_epoch: 0,
                    full: true,
                    upserts,
                    removed: Vec::new(),
                }),
                ..Default::default()
            }))
            .await?
            .into_inner();
        Ok(reply.owner_notices)
    }

    /// Apply notices the way the worker's heartbeat loop does.
    fn apply(&self, notices: &[OwnerNotice]) {
        legion::worker::task_manager::children::apply_owner_notices(&self.task_manager, notices);
    }

    /// The `Workload` service as the `ci` allocation's API socket serves it.
    fn socket(&self) -> R<AllocationWorkload> {
        let launch_pool = Arc::new(std::sync::OnceLock::new());
        let _ = launch_pool.set(self.launch_pool.clone());
        Ok(AllocationWorkload::new(
            self.ci_allocation().context("the ci allocation")?,
            WorkloadChannel {
                manager: None,
                task_manager: self.task_manager.clone(),
                launch_pool,
                node_id: NODE.into(),
            },
        ))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // The envelope and anything a failed assertion left behind.
        if let Ok(mut manager) = self.task_manager.lock() {
            for claim in manager.claims_snapshot() {
                manager.kill_claim(&claim.claim_id, "test over");
            }
        }
    }
}

fn session(id: &str) -> OwnerRef {
    owner_ref(OWNER_SESSION, id)
}

/// Child-sessions plan §6.3, §6.4, §6.6, Phase 1: a child starts under an
/// owner on the owner's node with no raft write, is listed under it, is
/// refused past `max` and without a declaration, and ends when its owner is
/// stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_starts_under_its_owner_and_ends_with_it_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let agent = cluster.start(AGENT, None).await?;
    assert_eq!(agent.owner, None);
    let raft_before = cluster.store().raft_applied_entries()?;

    // The kind is worked out from the id.
    let first = cluster
        .start(TASK, Some(owner_ref("", &agent.instance_id)))
        .await?;
    assert_eq!(first.owner, Some(session(&agent.instance_id)));
    assert_eq!(first.node_id, NODE, "a child goes to its owner's node");
    assert_eq!(first.mode, "session");
    let second = cluster
        .start(TASK, Some(session(&agent.instance_id)))
        .await?;
    assert_eq!(
        cluster.store().raft_applied_entries()?,
        raft_before,
        "a session child writes nothing to raft"
    );

    // Listed under the owner, and each child names it.
    let owner = cluster.get(&agent.instance_id).await?;
    let mut listed: Vec<&str> = owner
        .children
        .iter()
        .map(|child| child.session_id.as_str())
        .collect();
    listed.sort_unstable();
    let mut expected = vec![first.instance_id.as_str(), second.instance_id.as_str()];
    expected.sort_unstable();
    assert_eq!(listed, expected);
    assert!(owner.children.iter().all(|child| child.task_name == TASK));
    assert_eq!(
        cluster.get(&first.instance_id).await?.owner,
        Some(session(&agent.instance_id))
    );
    // The node's report carries the reference.
    let reported = cluster
        .task_manager
        .lock()
        .expect("task manager")
        .session_status(&first.instance_id)
        .and_then(|status| status.owner);
    assert_eq!(reported, Some(session(&agent.instance_id)));

    // Over `max`.
    let over = cluster
        .create(TASK, Some(session(&agent.instance_id)))
        .await
        .expect_err("the owner already has max children");
    assert_eq!(over.code(), Code::ResourceExhausted, "{over:?}");
    assert!(over.message().contains("children.max = 2"), "{over:?}");

    // The refusals of §6.3.
    let undeclared = cluster
        .create(AGENT, Some(session(&agent.instance_id)))
        .await
        .expect_err("agent does not declare agent");
    assert_eq!(
        undeclared.code(),
        Code::FailedPrecondition,
        "{undeclared:?}"
    );
    assert!(
        undeclared.message().contains("does not declare"),
        "{undeclared:?}"
    );
    let nested = cluster
        .create(TASK, Some(session(&first.instance_id)))
        .await
        .expect_err("a child cannot own");
    assert_eq!(nested.code(), Code::FailedPrecondition, "{nested:?}");
    assert!(nested.message().contains("depth 1 only"), "{nested:?}");
    let missing = cluster
        .create(TASK, Some(session("agent-q4n7c2rd.nosuchsession")))
        .await
        .expect_err("no such owner");
    assert_eq!(missing.code(), Code::NotFound, "{missing:?}");
    let named = cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: JOB.into(),
            task: Some(TASK.into()),
            instance_id: Some("ws-1".into()),
            owner: Some(session(&agent.instance_id)),
            ..Default::default()
        }))
        .await
        .expect_err("a child is an unnamed session");
    assert_eq!(named.code(), Code::FailedPrecondition, "{named:?}");

    // Stopping the owner ends its children, within a second.
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: agent.instance_id.clone(),
            idempotency_key: None,
        }))
        .await?;
    cluster.wait_gone(&first.instance_id).await?;
    cluster.wait_gone(&second.instance_id).await?;
    assert!(!cluster.holds(&agent.instance_id));
    // The node says why a child is gone.
    let gone = cluster
        .get(&first.instance_id)
        .await
        .expect_err("the child ended");
    assert_eq!(gone.code(), Code::NotFound, "{gone:?}");
    Ok(())
}

/// Phase 2: an allocation owns session children, and the managers decide
/// when it ended. Its terminal state reaches the node as an owner notice on
/// the heartbeat reply, repeated until the node's report no longer lists a
/// child, so a lost notice is corrected by the next beat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_allocations_end_reaches_its_children_through_the_managers_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let ci = cluster.ci_allocation().context("ci allocation")?;
    let owner = owner_ref(OWNER_ALLOCATION, &ci.to_string());
    let child = cluster
        .start(TASK, Some(owner_ref("", &ci.to_string())))
        .await?;
    assert_eq!(child.owner, Some(owner.clone()));

    // A running owner: nothing to say, and the node never judges it.
    assert!(cluster.heartbeat().await?.is_empty());
    cluster
        .task_manager
        .lock()
        .expect("task manager")
        .reconcile_children();
    assert!(cluster.holds(&child.instance_id));

    // The batch run completes.
    tokio::task::block_in_place(|| cluster.store().complete_allocation(ci, true))?;
    let notices = cluster.heartbeat().await?;
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].owner, Some(owner.clone()));
    assert_eq!(notices[0].event, "stopped");
    // The notice is lost: the next beat repeats it.
    let again = cluster.heartbeat().await?;
    assert_eq!(again, notices);
    cluster.apply(&again);
    cluster.wait_gone(&child.instance_id).await?;
    // Nothing left to say once the report no longer lists the child.
    assert!(cluster.heartbeat().await?.is_empty());
    Ok(())
}

/// Phase 2: a workload starts, lists, and stops its own children over its
/// API socket with no credential, and reaches nobody else's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workload_owns_children_through_its_api_socket_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let socket = cluster.socket()?;
    let ci = cluster.ci_allocation().context("ci allocation")?;
    let me = owner_ref(OWNER_ALLOCATION, &ci.to_string());

    let started = socket
        .start_child_session(Request::new(StartChildSessionRequest {
            task_name: TASK.into(),
            idempotency_key: "build-1".into(),
            wait: None,
        }))
        .await?
        .into_inner();
    assert_eq!(started.owner, Some(me.clone()), "the owner is the caller");
    assert_eq!(started.state, "running");
    assert_eq!(started.node_id, NODE);
    // A retry with the same key finds the same child.
    let retried = socket
        .start_child_session(Request::new(StartChildSessionRequest {
            task_name: TASK.into(),
            idempotency_key: "build-1".into(),
            wait: None,
        }))
        .await?
        .into_inner();
    assert_eq!(retried.session_id, started.session_id);
    assert!(retried.deduplicated);

    // The jobspec decides: `ci` does not declare `agent`.
    let undeclared = socket
        .start_child_session(Request::new(StartChildSessionRequest {
            task_name: AGENT.into(),
            ..Default::default()
        }))
        .await
        .expect_err("ci does not declare agent");
    assert_eq!(
        undeclared.code(),
        Code::FailedPrecondition,
        "{undeclared:?}"
    );

    // A declared child task may also be run once (docs/session-runs.md).
    let ran = socket
        .run_session(Request::new(RunSessionRequest {
            task_name: TASK.into(),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(ran.exit, Some(0), "{ran:?}");
    assert_eq!(ran.stdout, b"ran\n");

    // Somebody else's session is not reachable, and is not revealed.
    let other = cluster.start(AGENT, None).await?;
    let foreign = socket
        .stop_child_session(Request::new(StopChildSessionRequest {
            session_id: other.instance_id.clone(),
        }))
        .await
        .expect_err("not this workload's child");
    assert_eq!(foreign.code(), Code::NotFound, "{foreign:?}");
    assert!(cluster.holds(&other.instance_id));

    let listed = socket
        .list_children(Request::new(ListChildrenRequest {}))
        .await?
        .into_inner();
    assert_eq!(listed.owner, Some(me));
    assert_eq!(listed.max, 8);
    assert_eq!(listed.children.len(), 1, "{listed:?}");
    assert_eq!(listed.children[0].session_id, started.session_id);
    assert_eq!(listed.children[0].state, "running");

    let stopped = socket
        .stop_child_session(Request::new(StopChildSessionRequest {
            session_id: started.session_id.clone(),
        }))
        .await?
        .into_inner();
    assert!(stopped.stopped);
    assert!(!cluster.holds(&started.session_id));
    // The owner learns why it is gone.
    let listed = socket
        .list_children(Request::new(ListChildrenRequest {}))
        .await?
        .into_inner();
    assert_eq!(listed.children.len(), 1, "{listed:?}");
    assert_eq!(listed.children[0].state, "gone");
    assert_eq!(listed.children[0].reason, "stopped by its owner");
    Ok(())
}

/// Plan §6.5, Phase 2: promotion carries ownership. A promoted owner's
/// child keeps running under the instance it became and ends when that
/// instance is stopped; a promoted child is detached and outlives its owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_carries_ownership_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let agent = cluster.start(AGENT, None).await?;
    let follows = cluster
        .start(TASK, Some(session(&agent.instance_id)))
        .await?;
    let detached = cluster
        .start(TASK, Some(session(&agent.instance_id)))
        .await?;

    // The child is promoted: nobody's child any more.
    cluster
        .svc
        .promote_instance(Request::new(PromoteInstanceRequest {
            instance_id: detached.instance_id.clone(),
            new_id: String::new(),
            idempotency_key: None,
        }))
        .await?;
    // The owner is promoted under a name.
    cluster
        .svc
        .promote_instance(Request::new(PromoteInstanceRequest {
            instance_id: agent.instance_id.clone(),
            new_id: "user-42".into(),
            idempotency_key: None,
        }))
        .await?;
    let instance = owner_ref(OWNER_INSTANCE, "user-42");
    let mut rekeyed = false;
    for _ in 0..200 {
        rekeyed = {
            let guard = cluster.task_manager.lock().expect("task manager");
            let link = guard
                .child_link(&follows.instance_id)
                .map(|link| link.owner.clone());
            link == Some(instance.clone()) && guard.child_link(&detached.instance_id).is_none()
        };
        if rekeyed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(rekeyed, "the node re-keyed the child to the promoted owner");
    // The managers list it under the instance from the next report on.
    assert!(cluster.heartbeat().await?.is_empty(), "a live instance");
    let owner = cluster.get("user-42").await?;
    assert_eq!(owner.children.len(), 1, "{owner:?}");
    assert_eq!(owner.children[0].session_id, follows.instance_id);

    // From now on the managers decide: stopping the instance ends the child.
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: "user-42".into(),
            idempotency_key: None,
        }))
        .await?;
    let notices = cluster.heartbeat().await?;
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].owner, Some(instance));
    assert_eq!(notices[0].event, "stopped");
    cluster.apply(&notices);
    cluster.wait_gone(&follows.instance_id).await?;
    // The promoted child was detached, so it is still an instance.
    let kept = cluster.get(&detached.instance_id).await?;
    assert_eq!(kept.owner, None);
    assert_eq!(kept.mode, "keyed");
    Ok(())
}

/// Phase 2: a session owner that is not on its child's node (it moved, or
/// the child was placed elsewhere) is the managers' to ask about. An owner
/// its node says is gone ends the child at the next beat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_session_owners_end_reaches_its_children_test() -> R<()> {
    // The grace that covers a session briefly in no index (it just moved)
    // is 20 s; this test's owner is gone for good.
    legion::manager::children::NOT_FOUND_GRACE_MS.store(0, std::sync::atomic::Ordering::Relaxed);
    let cluster = Cluster::new().await?;
    let agent = cluster.start(AGENT, None).await?;
    let child = cluster
        .start(TASK, Some(session(&agent.instance_id)))
        .await?;
    // The owner's copy here is superseded: it lives on elsewhere, as far as
    // this node knows, so the child stays.
    assert!(cluster
        .task_manager
        .lock()
        .expect("task manager")
        .end_superseded(&agent.instance_id, "test"));
    {
        let mut guard = cluster.task_manager.lock().expect("task manager");
        guard.reconcile_children();
        let link = guard
            .child_link(&child.instance_id)
            .cloned()
            .context("link")?;
        assert!(link.remote);
    }
    assert!(cluster.holds(&child.instance_id));

    // The managers ask where the owner is. Its node answers that it holds
    // no such session, and no manager indexes it: it ended.
    let mut notices = Vec::new();
    for _ in 0..100 {
        notices = cluster.heartbeat().await?;
        if !notices.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].owner, Some(session(&agent.instance_id)));
    assert_eq!(notices[0].event, "stopped");
    cluster.apply(&notices);
    cluster.wait_gone(&child.instance_id).await?;
    Ok(())
}

/// Phase 3: a keyed instance owns keyed children. The child's row carries
/// the reference, `max` bounds session and keyed children together, a
/// session cannot own one, the child's identity names its owner, and the
/// owner's stop and destroy end it in the owner's own entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_owner_owns_keyed_children_test() -> R<()> {
    let cluster = Cluster::new().await?;
    cluster.create_keyed(KEEPER, "user-1", None).await?;
    cluster.run_evals()?;
    let keeper = owner_ref(OWNER_INSTANCE, "user-1");

    let child = cluster
        .create_keyed(WORKSPACE, "ws-1", Some(owner_ref("", "user-1")))
        .await?;
    assert_eq!(child.instance_id, "ws-1");
    assert_eq!(child.owner, Some(keeper.clone()), "the kind was worked out");
    assert!(child.durable);
    assert_eq!(
        cluster.instance("ws-1")?.owner.as_deref(),
        Some("instance:user-1"),
        "the reference lives in the child's row"
    );

    // Placed beside its owner, and its identity names the tree it is in.
    cluster.run_evals()?;
    let row = cluster.instance("ws-1")?;
    let allocation = cluster
        .store()
        .get_allocation_by_id(row.current_alloc_id.context("placed")?)?
        .context("the child's allocation")?;
    assert_eq!(allocation.node_id, NODE);
    let task = cluster
        .store()
        .get_job_config(JOB)?
        .tasks
        .iter()
        .find(|task| task.name == WORKSPACE)
        .cloned()
        .context("workspace task")?;
    let claims = legion::manager::workload_identity::claims_for_allocation(
        cluster.store(),
        &allocation,
        &task,
        "global",
    )?;
    assert_eq!(claims.owner.as_deref(), Some("instance:user-1"));

    // Listed under the owner, by kind.
    let owner = cluster.get("user-1").await?;
    assert_eq!(owner.children.len(), 1, "{owner:?}");
    assert_eq!(owner.children[0].session_id, "ws-1");
    assert_eq!(owner.children[0].kind, "instance");
    assert_eq!(cluster.get("ws-1").await?.owner, Some(keeper.clone()));

    // A child cannot own (depth 1), and a session cannot own a keyed child.
    let nested = cluster
        .create_keyed(
            WORKSPACE,
            "ws-nested",
            Some(owner_ref(OWNER_INSTANCE, "ws-1")),
        )
        .await
        .expect_err("a child owns nothing");
    assert_eq!(nested.code(), Code::FailedPrecondition, "{nested}");
    let agent = cluster.start(AGENT, None).await?;
    let durable_of_session = cluster
        .create_keyed(
            WORKSPACE,
            "ws-of-session",
            Some(session(&agent.instance_id)),
        )
        .await
        .expect_err("a session cannot own a durable child");
    assert_eq!(
        durable_of_session.code(),
        Code::FailedPrecondition,
        "{durable_of_session}"
    );

    // `max = 2` counts the keyed child and a session child together.
    let machine = cluster.start(TASK, Some(keeper.clone())).await?;
    cluster.heartbeat().await?;
    let over = cluster
        .create_keyed(WORKSPACE, "ws-2", Some(keeper.clone()))
        .await
        .expect_err("at max");
    assert_eq!(over.code(), Code::ResourceExhausted, "{over}");
    let over = cluster
        .create(TASK, Some(keeper.clone()))
        .await
        .expect_err("at max");
    assert_eq!(over.code(), Code::ResourceExhausted, "{over}");

    // Stopping the owner stops the keyed child in the same entry; the
    // session child hears from the managers at the next beat.
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: "user-1".into(),
            idempotency_key: None,
        }))
        .await?;
    assert_eq!(
        cluster.instance("ws-1")?.state,
        legion::structs::InstanceState::Stopped
    );
    let notices = cluster.heartbeat().await?;
    cluster.apply(&notices);
    cluster.wait_gone(&machine.instance_id).await?;

    // Destroying it destroys the child.
    cluster
        .svc
        .destroy_instance(Request::new(legion::structs::DestroyInstanceRequest {
            instance_id: "user-1".into(),
            ..Default::default()
        }))
        .await?;
    assert_eq!(
        cluster.instance("ws-1")?.state,
        legion::structs::InstanceState::Terminated
    );
    Ok(())
}

/// Phase 3, from inside a task: the worker vouches for one of its
/// allocations and a manager creates the keyed child
/// (`Manager.CreateChildInstance`). When that allocation ends, the leader's
/// sweep stops the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_creates_a_keyed_child_for_its_allocation_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let ci = cluster.ci_allocation().context("ci allocation")?;
    let request = |node: &str, task: &str, key: &str| {
        Request::new(legion::structs::CreateChildInstanceRequest {
            node_id: node.into(),
            allocation_id: ci,
            task_name: task.into(),
            instance_id: "ws-ci".into(),
            idempotency_key: key.into(),
        })
    };
    let created = cluster
        .svc
        .create_child_instance(request(NODE, WORKSPACE, "k1"))
        .await?
        .into_inner();
    let me = owner_ref(OWNER_ALLOCATION, &ci.to_string());
    assert_eq!(created.instance_id, "ws-ci");
    assert_eq!(created.owner, Some(me.clone()));
    let retried = cluster
        .svc
        .create_child_instance(request(NODE, WORKSPACE, "k1"))
        .await?
        .into_inner();
    assert!(retried.deduplicated);
    assert_eq!(retried.instance_id, "ws-ci");

    // Another node may not speak for this allocation, and a session task
    // is not a keyed child.
    let elsewhere = cluster
        .svc
        .create_child_instance(request("w-99", WORKSPACE, "k2"))
        .await
        .expect_err("not that node's allocation");
    assert_eq!(elsewhere.code(), Code::NotFound, "{elsewhere}");
    let not_keyed = cluster
        .svc
        .create_child_instance(request(NODE, TASK, "k3"))
        .await
        .expect_err("a session task");
    assert_eq!(not_keyed.code(), Code::FailedPrecondition, "{not_keyed}");

    // The socket refuses what the jobspec does not grant before it asks a
    // manager, and says so when it has no manager to ask.
    let socket = cluster.socket()?;
    let undeclared = socket
        .create_child_instance(Request::new(
            legion::structs::WorkloadCreateChildInstanceRequest {
                task_name: AGENT.into(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("not declared");
    assert_eq!(undeclared.code(), Code::FailedPrecondition, "{undeclared}");
    let no_link = socket
        .create_child_instance(Request::new(
            legion::structs::WorkloadCreateChildInstanceRequest {
                task_name: WORKSPACE.into(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("no manager link in this fixture");
    assert_eq!(no_link.code(), Code::Unavailable, "{no_link}");

    // The allocation ends: nothing cascades in its own entry, so the
    // leader's sweep stops the child.
    cluster.run_evals()?;
    assert_eq!(
        legion::manager::children::sweep_keyed_children(cluster.store())?,
        0
    );
    tokio::task::block_in_place(|| cluster.store().remove_allocation_by_id(ci))?;
    assert_eq!(
        legion::manager::children::sweep_keyed_children(cluster.store())?,
        1
    );
    assert_eq!(
        cluster.instance("ws-ci")?.state,
        legion::structs::InstanceState::Stopped
    );
    Ok(())
}

/// Phase 3: promoting a session child whose owner the managers judge keeps
/// the reference, written into the new row by the promotion itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promoted_child_of_an_allocation_stays_its_child_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let ci = cluster.ci_allocation().context("ci allocation")?;
    let me = owner_ref(OWNER_ALLOCATION, &ci.to_string());
    let child = cluster.start(TASK, Some(me.clone())).await?;
    cluster.heartbeat().await?;
    cluster
        .svc
        .promote_instance(Request::new(PromoteInstanceRequest {
            instance_id: child.instance_id.clone(),
            new_id: "kept".into(),
            idempotency_key: None,
        }))
        .await?;
    let row = cluster.instance("kept")?;
    assert_eq!(row.owner, Some(format!("allocation:{ci}")));
    let info = cluster.get("kept").await?;
    assert_eq!(info.owner, Some(me));
    assert_eq!(info.mode, "keyed");

    // It ends with its owner still: here through the sweep, since an
    // allocation's end is not an instance stop.
    tokio::task::block_in_place(|| cluster.store().remove_allocation_by_id(ci))?;
    assert_eq!(
        legion::manager::children::sweep_keyed_children(cluster.store())?,
        1
    );
    assert_eq!(
        cluster.instance("kept")?.state,
        legion::structs::InstanceState::Stopped
    );
    Ok(())
}

/// `children.suspendWithOwner`, and Phase 4's lookup: a suspended owner's
/// node is told which children follow it, and a handoff reaches only the
/// caller's own running children and only ports they declare.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspend_with_owner_and_handoff_reach_only_the_owners_children_test() -> R<()> {
    let cluster = Cluster::new().await?;
    cluster.create_keyed(KEEPER, "user-1", None).await?;
    cluster.run_evals()?;
    let keeper = owner_ref(OWNER_INSTANCE, "user-1");
    let follows = cluster.start(TASK, Some(keeper.clone())).await?;
    let ci = cluster.ci_allocation().context("ci allocation")?;
    let stays = cluster
        .start(TASK, Some(owner_ref(OWNER_ALLOCATION, &ci.to_string())))
        .await?;

    {
        let mut guard = cluster.task_manager.lock().expect("task manager");
        // `keeper` sets suspendWithOwner; `ci` does not.
        assert_eq!(
            guard.children_to_suspend_with(&keeper),
            vec![follows.instance_id.clone()]
        );
        assert!(guard
            .children_to_suspend_with(&owner_ref(OWNER_ALLOCATION, &ci.to_string()))
            .is_empty());
        // A `suspended` notice names the same children, and ends nothing.
        let to_suspend = guard.apply_owner_notice(&OwnerNotice {
            owner: Some(keeper.clone()),
            event: "suspended".into(),
            promoted_to: None,
            detail: String::new(),
        });
        assert_eq!(to_suspend, vec![follows.instance_id.clone()]);

        // Handoff: `ci` reaches its own child, not `keeper`'s, and only a
        // port the child declares.
        let own = guard
            .child_port_target(ci, &stays.instance_id, "http")
            .expect_err("the child declares no port");
        assert_eq!(own.code(), Code::FailedPrecondition, "{own}");
        assert!(own.message().contains("declares no port"), "{own}");
        let other = guard
            .child_port_target(ci, &follows.instance_id, "http")
            .expect_err("another owner's child");
        assert_eq!(other.code(), Code::NotFound, "{other}");
        let missing = guard
            .child_port_target(ci, "machine-nope.01", "http")
            .expect_err("no such child");
        assert_eq!(missing.code(), Code::NotFound, "{missing}");
    }
    assert!(cluster.holds(&follows.instance_id));
    assert!(cluster.holds(&stays.instance_id));
    Ok(())
}
