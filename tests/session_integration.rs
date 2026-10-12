//! Session mode on the manager (sandbox-scaling plan, Phase 6;
//! docs/session-mode.md): an unnamed session starts with no raft entry, is
//! listed from the nodes' reports, converges to one copy when duplicated, is
//! promoted to a keyed instance in one entry, and ends on `stop`/`destroy`.
//!
//! The manager runs a single-voter raft store, so "no raft entry" is a real
//! applied-index comparison. Each node is a fake worker gRPC server that
//! admits sessions as the real one does (no ledger hold, refusing when told
//! its envelope is full) and reports them on a heartbeat the test sends. The
//! worker side of admission, adoption, and the orphan sweep is covered by
//! the unit tests in `src/worker/task_manager.rs`. Fake nodes serve the real
//! worker-plane listener with a node certificate from the store's mesh CA, so
//! the manager reaches them the way it reaches real workers (mTLS).

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use legion::cluster_tls::{
    certified_key, server_config, tls_incoming, ClusterClientVerifier, SwappableIdentity,
    SwappableRoots,
};
use legion::manager::mesh_ca::{generate_csr, sign_csr, CsrSubject};
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::JobConfig;
use legion::structs::manager_server::Manager;
use legion::structs::worker_server::{Worker, WorkerServer};
use legion::structs::{
    CreateInstanceRequest, DestroyInstanceRequest, ExecClientFrame, ExecServerFrame,
    GetInstanceRequest, GetSessionReply, GetSessionRequest, GetTaskLogsReply, GetTaskLogsRequest,
    HeartbeatReply, HeartbeatRequest, InstanceInfo, ListInstancesReply, ListInstancesRequest, Node,
    NodeMsg, PostWorkReply, PostWorkRequest, ProbeReply, ProbeRequest, PromoteInstanceReply,
    PromoteInstanceRequest, RestartWorkerReply, RestartWorkerRequest, ResumeSessionReply,
    ResumeSessionRequest, SessionReport, SessionSleepFailure, SessionStatus, StartClaimReply,
    StartClaimRequest, StartInstanceRequest, StartSessionReply, StartSessionRequest,
    StopInstanceRequest, StopSessionReply, StopSessionRequest, StopSessionsReply,
    StopSessionsRequest, Submission, SuspendInstanceRequest, SuspendSessionsReply,
    SuspendSessionsRequest, SuspendedSession, TaskAllocation,
};
use tonic::{Code, Request, Response, Status, Streaming};

const TASK: &str = "workspace";

/// What one fake node holds.
#[derive(Default)]
struct NodeState {
    sessions: BTreeMap<String, SessionStatus>,
    /// Refuse session starts as an envelope with no room would.
    full: bool,
    /// Every payload pushed to the node, sessions and allocations alike.
    posts: Vec<TaskAllocation>,
    /// Every session id a StopSessions call named.
    stops: Vec<String>,
    /// Sessions this node evicted, with the `evicted:` answer's details.
    evicted: BTreeMap<String, String>,
    epoch: u64,
}

#[derive(Clone)]
struct FakeNode {
    state: Arc<Mutex<NodeState>>,
}

impl FakeNode {
    fn lock(&self) -> std::sync::MutexGuard<'_, NodeState> {
        self.state.lock().expect("node state")
    }
}

#[tonic::async_trait]
impl Worker for FakeNode {
    type StartClaimStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<StartClaimReply, Status>> + Send + 'static>,
    >;

    async fn start_claim(
        &self,
        _request: Request<StartClaimRequest>,
    ) -> Result<Response<Self::StartClaimStream>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        let allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("no allocation"))?;
        let mut state = self.lock();
        let session = allocation
            .provisional
            .as_ref()
            .and_then(|provisional| provisional.session.map(|session| (provisional, session)));
        if let Some((provisional, session)) = session {
            if state.full {
                return Err(Status::resource_exhausted("the envelope is full"));
            }
            state.sessions.insert(
                provisional.instance_id.clone(),
                SessionStatus {
                    session_id: provisional.instance_id.clone(),
                    job_id: allocation.job_id.clone(),
                    job_config_id: allocation.job_config_id.clone(),
                    task_name: allocation.task_name.clone(),
                    envelope_allocation_id: 1,
                    state: "running".into(),
                    lease_issued_at_unix_ms: session.lease_issued_at_unix_ms,
                    resources: allocation.task_resources.clone(),
                    ..Default::default()
                },
            );
        }
        if !allocation.promoted_from_session.is_empty() {
            // Adoption: the session is an allocation from here on, and the
            // node stops reporting it.
            state.sessions.remove(&allocation.promoted_from_session);
        }
        state.posts.push(allocation);
        Ok(Response::new(PostWorkReply {
            message: "Task accepted".into(),
        }))
    }

    async fn stop_sessions(
        &self,
        request: Request<StopSessionsRequest>,
    ) -> Result<Response<StopSessionsReply>, Status> {
        let mut state = self.lock();
        let mut stopped = Vec::new();
        for id in request.into_inner().session_ids {
            state.stops.push(id.clone());
            if state.sessions.remove(&id).is_some() {
                stopped.push(id);
            }
        }
        Ok(Response::new(StopSessionsReply { stopped }))
    }

    /// Suspend as a node does it, minus the checkpoint: the session stays
    /// listed as `suspended`.
    async fn suspend_sessions(
        &self,
        request: Request<SuspendSessionsRequest>,
    ) -> Result<Response<SuspendSessionsReply>, Status> {
        let mut state = self.lock();
        let mut reply = SuspendSessionsReply::default();
        for id in request.into_inner().session_ids {
            match state.sessions.get_mut(&id) {
                Some(status) => {
                    let already = status.state == "suspended";
                    status.state = "suspended".into();
                    reply.suspended.push(SuspendedSession {
                        session_id: id,
                        snapshot_id: "snap-fake".into(),
                        kind: "memory".into(),
                        bytes: 4096,
                        already_suspended: already,
                    });
                }
                None => reply.failed.push(SessionSleepFailure {
                    session_id: id.clone(),
                    code: "NotFound".into(),
                    message: format!("not_found: no session {id}"),
                }),
            }
        }
        Ok(Response::new(reply))
    }

    async fn resume_session(
        &self,
        request: Request<ResumeSessionRequest>,
    ) -> Result<Response<ResumeSessionReply>, Status> {
        let id = request.into_inner().session_id;
        let mut state = self.lock();
        if let Some(when) = state.evicted.get(&id) {
            return Err(Status::not_found(format!(
                "evicted: session {id} was evicted from n1 {when}; a local-only session cannot be restored"
            )));
        }
        let full = state.full;
        let Some(status) = state.sessions.get_mut(&id) else {
            return Err(Status::not_found(format!("not_found: no session {id}")));
        };
        if status.state != "suspended" {
            return Ok(Response::new(ResumeSessionReply {
                session: Some(status.clone()),
                already_running: true,
                ..Default::default()
            }));
        }
        if full {
            return Err(Status::resource_exhausted("envelope full"));
        }
        status.state = "running".into();
        Ok(Response::new(ResumeSessionReply {
            session: Some(status.clone()),
            resumed_from: "local".into(),
            ..Default::default()
        }))
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionReply>, Status> {
        let id = request.into_inner().session_id;
        let state = self.lock();
        if let Some(when) = state.evicted.get(&id) {
            return Err(Status::not_found(format!(
                "evicted: session {id} was evicted from n1 {when}; a local-only session cannot be restored"
            )));
        }
        match state.sessions.get(&id) {
            Some(status) => Ok(Response::new(GetSessionReply {
                session: Some(status.clone()),
                node_id: String::new(),
            })),
            None => Err(Status::not_found(format!("not_found: no session {id}"))),
        }
    }

    async fn start_session(
        &self,
        _request: Request<StartSessionRequest>,
    ) -> Result<Response<StartSessionReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    async fn stop_session(
        &self,
        _request: Request<StopSessionRequest>,
    ) -> Result<Response<StopSessionReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    async fn probe(&self, _request: Request<ProbeRequest>) -> Result<Response<ProbeReply>, Status> {
        Ok(Response::new(ProbeReply::default()))
    }

    async fn restart_worker(
        &self,
        _request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    async fn get_task_logs(
        &self,
        _request: Request<GetTaskLogsRequest>,
    ) -> Result<Response<GetTaskLogsReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    type ExecTaskStream = tokio_stream::wrappers::ReceiverStream<Result<ExecServerFrame, Status>>;

    async fn exec_task(
        &self,
        _request: Request<Streaming<ExecClientFrame>>,
    ) -> Result<Response<Self::ExecTaskStream>, Status> {
        Err(Status::unimplemented("fake node"))
    }
}

struct Cluster {
    svc: ManagerDaemonService,
    job: String,
    /// `stateful.envelope.resources`, and one session's `resources`.
    sizes: (serde_json::Value, serde_json::Value),
    /// The session task's `suspend` block, when it has one.
    suspend: Option<serde_json::Value>,
    nodes: BTreeMap<String, FakeNode>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    /// A raft store, `nodes` fake workers with the `sessions` capability, and
    /// a session job whose envelope holds four sessions per node, placed.
    async fn new(job: &str, nodes: &[&str]) -> R<Self> {
        Self::with_sizes(
            job,
            nodes,
            json!({ "cpu": 2000, "memory": 2048 }),
            json!({ "cpu": 500, "memory": 512 }),
        )
        .await
    }

    async fn with_sizes(
        job: &str,
        nodes: &[&str],
        envelope: serde_json::Value,
        session: serde_json::Value,
    ) -> R<Self> {
        Self::build(job, nodes, envelope, session, None).await
    }

    /// [`Cluster::new`] with a `suspend` block on the session task.
    async fn with_suspend(job: &str, nodes: &[&str], suspend: serde_json::Value) -> R<Self> {
        Self::build(
            job,
            nodes,
            json!({ "cpu": 2000, "memory": 2048 }),
            json!({ "cpu": 500, "memory": 512 }),
            Some(suspend),
        )
        .await
    }

    async fn build(
        job: &str,
        nodes: &[&str],
        envelope: serde_json::Value,
        session: serde_json::Value,
        suspend: Option<serde_json::Value>,
    ) -> R<Self> {
        let dir = tempfile::tempdir()?;
        let mut config = ManagerConfigFull {
            store_path: dir.path().join("store").to_str().context("utf-8")?.into(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-sessions".into();
        config.persistence.raft.data_dir =
            dir.path().join("raft").to_str().context("utf-8")?.into();
        let store = tokio::task::block_in_place(|| Store::start_storage(&config))?;
        let mut cluster = Cluster {
            svc: ManagerDaemonService::new(store),
            job: job.to_string(),
            sizes: (envelope, session),
            suspend,
            nodes: BTreeMap::new(),
            _dir: dir,
        };
        let ca = cluster.store().ensure_mesh_ca()?;
        let roots = SwappableRoots::from_pem(&ca.cert_pem)?;
        for node_id in nodes {
            let node = FakeNode {
                state: Arc::new(Mutex::new(NodeState::default())),
            };
            let (key, csr) = generate_csr(node_id)?;
            let (cert, _) = sign_csr(
                &ca.cert_pem,
                &ca.key_pem,
                &csr,
                &CsrSubject::Node(node_id.to_string()),
            )?;
            let identity = SwappableIdentity::new();
            identity.set(certified_key(&cert, &key)?);
            let config = server_config(
                identity,
                Some(ClusterClientVerifier::new(roots.clone())),
                false,
            )?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let service = WorkerServer::new(node.clone());
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming(tls_incoming(listener, config))
                    .await;
            });
            cluster.store().insert_node(NodeMsg::from(Node {
                id: node_id.to_string(),
                pool: "default".into(),
                status: "ready".into(),
                availability: "available".into(),
                last_heartbeat: "2023-01-01T00:00:00Z".into(),
                heartbeat_source: String::new(),
                config_attrs: json!({
                    "node_id": node_id,
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
            cluster.nodes.insert(node_id.to_string(), node);
        }
        cluster.submit()?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn node(&self, id: &str) -> &FakeNode {
        &self.nodes[id]
    }

    fn submit(&self) -> R<()> {
        // Parsed as a jobspec is, which turns the session task into a keyed
        // task plus its generated envelope task.
        let mut task = json!({
            "name": TASK,
            "runtime": "noop",
            "resources": self.sizes.1,
            "stateful": {
                "mode": "session",
                "envelope": { "resources": self.sizes.0 },
            },
        });
        if let Some(suspend) = &self.suspend {
            task["suspend"] = suspend.clone();
        }
        let config: JobConfig = serde_json::from_value(json!({
            "name": self.job,
            "task": task,
        }))?;
        tokio::task::block_in_place(|| -> R<()> {
            self.store().handle_job_submit(
                &config,
                &[Submission {
                    path: format!("{}.json", self.job),
                    content: "{}".to_string(),
                    is_primary: true,
                }],
            )?;
            let config_id = self.store().get_job_config(&self.job)?.id();
            self.store().enqueue_eval(
                "job_reschedule",
                Some(&self.job),
                Some(&config_id),
                None,
                None,
            )?;
            while let Some(eval) = self.store().claim_eval()? {
                eval.handle(self.store())?;
                self.store().complete_eval(&eval.id, true)?;
            }
            Ok(())
        })
    }

    /// The applied index summed over every raft lane: what "no raft entry"
    /// is measured against.
    fn raft_entries(&self) -> u64 {
        self.store()
            .raft_status()
            .map(|status| status.lanes.iter().map(|lane| lane.applied_index).sum())
            .unwrap_or_default()
    }

    async fn create(
        &self,
        key: Option<&str>,
    ) -> Result<legion::structs::CreateInstanceReply, Status> {
        self.svc
            .create_instance(Request::new(CreateInstanceRequest {
                job_id: self.job.clone(),
                idempotency_key: key.map(str::to_string),
                ..Default::default()
            }))
            .await
            .map(Response::into_inner)
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

    async fn list(&self) -> Result<ListInstancesReply, Status> {
        self.svc
            .list_instances(Request::new(ListInstancesRequest {
                job_id: self.job.clone(),
                ..Default::default()
            }))
            .await
            .map(Response::into_inner)
    }

    async fn promote(&self, id: &str, new_id: &str) -> Result<PromoteInstanceReply, Status> {
        self.svc
            .promote_instance(Request::new(PromoteInstanceRequest {
                instance_id: id.into(),
                new_id: new_id.into(),
                idempotency_key: None,
            }))
            .await
            .map(Response::into_inner)
    }

    /// The node's heartbeat, carrying a full report of its sessions.
    async fn report(&self, node_id: &str) -> R<HeartbeatReply> {
        let report = {
            let mut state = self.node(node_id).lock();
            state.epoch += 1;
            SessionReport {
                epoch: state.epoch,
                base_epoch: 0,
                full: true,
                upserts: state.sessions.values().cloned().collect(),
                removed: Vec::new(),
            }
        };
        Ok(self
            .svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: node_id.into(),
                session_report: Some(report),
                ..Default::default()
            }))
            .await?
            .into_inner())
    }

    fn running(&self) -> BTreeMap<String, usize> {
        self.nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.lock().sessions.len()))
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unnamed_sessions_start_with_no_raft_entry_until_the_envelopes_fill_test() -> R<()> {
    let cluster = Cluster::new("agents-fill", &["n1", "n2"]).await?;
    let before = cluster.raft_entries();
    assert!(before > 0, "the raft store applied the job submit");

    let mut ids = Vec::new();
    for _ in 0..8 {
        let reply = cluster.create(None).await?;
        assert_eq!(reply.mode, "session");
        assert!(!reply.durable);
        assert_eq!(reply.state, "starting");
        assert!(cluster.nodes.contains_key(&reply.node_id), "{reply:?}");
        assert!(legion::structs::is_minted_for_task(
            &reply.instance_id,
            TASK
        ));
        ids.push(reply.instance_id);
    }
    assert_eq!(
        cluster.raft_entries(),
        before,
        "a session start writes no raft entry"
    );
    // Most room first: four on each node's envelope (2000 MHz / 500 MHz).
    assert_eq!(
        cluster.running(),
        BTreeMap::from([("n1".to_string(), 4), ("n2".to_string(), 4)])
    );
    assert!(cluster.store().list_instances(&cluster.job)?.is_empty());

    let full = cluster.create(None).await.expect_err("no room left");
    assert_eq!(full.code(), Code::ResourceExhausted, "{full:?}");
    assert_eq!(cluster.raft_entries(), before);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusing_node_sends_the_session_to_the_next_test() -> R<()> {
    let cluster = Cluster::new("agents-refuse", &["n1", "n2"]).await?;
    cluster.node("n1").lock().full = true;
    for _ in 0..2 {
        assert_eq!(cluster.create(None).await?.node_id, "n2");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listing_is_complete_once_every_envelope_node_reported_test() -> R<()> {
    let cluster = Cluster::new("agents-list", &["n1", "n2"]).await?;
    let a = cluster.create(None).await?;
    let b = cluster.create(None).await?;

    // Before any report: the router's own starts stand in, and the listing
    // says it cannot vouch for nodes it has not heard from.
    let listed = cluster.list().await?;
    assert!(!listed.complete);
    assert_eq!(listed.instances.len(), 2);

    cluster.report("n1").await?;
    assert!(!cluster.list().await?.complete, "n2 has not reported");
    cluster.report("n2").await?;
    let listed = cluster.list().await?;
    assert!(listed.complete);
    assert!(!listed.as_of.is_empty());
    let mut listed_ids: Vec<&str> = listed
        .instances
        .iter()
        .map(|info| info.instance_id.as_str())
        .collect();
    listed_ids.sort();
    let mut expected = vec![a.instance_id.as_str(), b.instance_id.as_str()];
    expected.sort();
    assert_eq!(listed_ids, expected);
    let group = cluster
        .store()
        .get_job_config(&cluster.job)?
        .tasks
        .iter()
        .find(|task| task.name == TASK)
        .map(|task| task.group.clone())
        .context("session task")?;
    for info in &listed.instances {
        assert_eq!(info.mode, "session");
        assert!(!info.durable);
        assert_eq!(info.state, "active");
        assert_eq!(info.task, TASK);
        assert_eq!(info.group, group);
        assert!(info.node_id.is_some());
    }

    let info = cluster.get(&a.instance_id).await?;
    assert_eq!(info.node_id.as_deref(), Some(a.node_id.as_str()));
    let missing = cluster
        .get("workspace-aaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect_err("no such session");
    assert_eq!(missing.code(), Code::NotFound);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_session_listing_pages_and_counts_test() -> R<()> {
    let cluster = Cluster::new("agents-page", &["n1", "n2"]).await?;
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(cluster.create(None).await?.instance_id);
    }
    ids.sort();
    cluster.report("n1").await?;
    cluster.report("n2").await?;
    let page = |limit: u32, token: String| {
        cluster
            .svc
            .list_instances(Request::new(ListInstancesRequest {
                job_id: cluster.job.clone(),
                session_limit: Some(limit),
                session_page_token: token,
                ..Default::default()
            }))
    };

    // Counts only: no sessions, the totals by node.
    let counted = page(0, String::new()).await?.into_inner();
    assert!(counted.instances.is_empty());
    assert_eq!(counted.session_count, 5);
    assert_eq!(counted.sessions_by_node.values().sum::<u64>(), 5);
    assert!(counted.complete);

    let mut listed = Vec::new();
    let mut token = String::new();
    loop {
        let reply = page(2, token).await?.into_inner();
        assert!(reply.instances.len() <= 2);
        listed.extend(reply.instances.into_iter().map(|info| info.instance_id));
        token = reply.next_session_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert_eq!(listed, ids, "every session once, in id order");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idempotent_create_finds_the_running_session_test() -> R<()> {
    let cluster = Cluster::new("agents-idem", &["n1", "n2"]).await?;
    let first = cluster.create(Some("req-1")).await?;
    assert!(!first.deduplicated);
    let again = cluster.create(Some("req-1")).await?;
    assert_eq!(again.instance_id, first.instance_id);
    assert_eq!(again.node_id, first.node_id);
    assert!(again.deduplicated);
    let other = cluster.create(Some("req-2")).await?;
    assert_ne!(other.instance_id, first.instance_id);
    let total: usize = cluster.running().values().sum();
    assert_eq!(total, 2, "the retry started nothing");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicated_session_converges_to_the_earliest_lease_test() -> R<()> {
    let cluster = Cluster::new("agents-dup", &["n1", "n2"]).await?;
    let id = "workspace-bbbbbbbbbbbbbbbbbbbbbbbbbb";
    for (node, iat) in [("n1", 1_000), ("n2", 2_000)] {
        cluster.node(node).lock().sessions.insert(
            id.into(),
            SessionStatus {
                session_id: id.into(),
                job_id: cluster.job.clone(),
                job_config_id: cluster.store().get_job_config(&cluster.job)?.id(),
                task_name: TASK.into(),
                envelope_allocation_id: 1,
                state: "running".into(),
                lease_issued_at_unix_ms: iat,
                resources: json!({ "cpu": 500, "memory": 512 }).to_string(),
                ..Default::default()
            },
        );
    }
    assert!(cluster.report("n1").await?.stop_sessions.is_empty());
    let reply = cluster.report("n2").await?;
    assert_eq!(reply.stop_sessions, vec![id.to_string()]);
    assert_eq!(cluster.get(id).await?.node_id.as_deref(), Some("n1"));
    // The node stops listing it once it stops it, and the request goes away.
    cluster.node("n2").lock().sessions.remove(id);
    assert!(cluster.report("n2").await?.stop_sessions.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_duplicate_sweep_stops_the_older_generation_test() -> R<()> {
    let cluster = Cluster::new("agents-sweep", &["n1", "n2"]).await?;
    // A derived (untagged) id on both nodes; n2's copy has the newer
    // generation, so n1's earlier-lease copy is the one that stops.
    let id = "workspace-cccccccccccccccccccccccccc";
    for (node, iat, generation) in [("n1", 1_000, 1), ("n2", 2_000, 2)] {
        cluster.node(node).lock().sessions.insert(
            id.into(),
            SessionStatus {
                session_id: id.into(),
                job_id: cluster.job.clone(),
                job_config_id: cluster.store().get_job_config(&cluster.job)?.id(),
                task_name: TASK.into(),
                envelope_allocation_id: 1,
                state: "running".into(),
                lease_issued_at_unix_ms: iat,
                generation,
                resources: json!({ "cpu": 500, "memory": 512 }).to_string(),
                ..Default::default()
            },
        );
    }
    cluster.report("n1").await?;
    cluster.report("n2").await?;
    assert_eq!(
        legion::manager::sessions::sweep_duplicates(cluster.store()).await,
        1
    );
    for _ in 0..50 {
        if !cluster.node("n1").lock().stops.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(cluster.node("n1").lock().stops, vec![id.to_string()]);
    assert!(cluster.node("n2").lock().stops.is_empty());
    // Swept ids are taken once.
    assert_eq!(
        legion::manager::sessions::sweep_duplicates(cluster.store()).await,
        0
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tagged_session_id_is_read_from_its_node_test() -> R<()> {
    let cluster = Cluster::new("agents-tag", &["n1", "n2"]).await?;
    let started = cluster.create(None).await?;
    assert_eq!(
        legion::structs::session_node_tag(&started.instance_id),
        Some(legion::structs::node_tag(&started.node_id).as_str()),
        "the router mints the id for the node that took it"
    );

    // A session the index has never heard of (started at the edge, no
    // report yet) is found by asking the node its id names.
    let edge_id = legion::structs::mint_session_id(TASK, "n2");
    cluster.node("n2").lock().sessions.insert(
        edge_id.clone(),
        SessionStatus {
            session_id: edge_id.clone(),
            job_id: cluster.job.clone(),
            job_config_id: cluster.store().get_job_config(&cluster.job)?.id(),
            task_name: TASK.into(),
            envelope_allocation_id: 1,
            state: "running".into(),
            lease_issued_at_unix_ms: 1_000,
            resources: json!({ "cpu": 500, "memory": 512 }).to_string(),
            origin: "edge".into(),
            ..Default::default()
        },
    );
    assert_eq!(cluster.get(&edge_id).await?.node_id.as_deref(), Some("n2"));

    // The named node's "never had it" is final.
    let gone = legion::structs::mint_session_id(TASK, "n1");
    let missing = cluster
        .get(&gone)
        .await
        .expect_err("n1 holds no such session");
    assert_eq!(missing.code(), Code::NotFound);
    Ok(())
}

#[test]
fn a_higher_generation_wins_a_duplicate_before_the_earlier_lease_test() {
    let copy = |node: &str, iat: i64, generation: u64| legion::structs::IndexedSession {
        node_id: node.into(),
        status: Some(SessionStatus {
            session_id: "s".into(),
            lease_issued_at_unix_ms: iat,
            generation,
            ..Default::default()
        }),
        recent: false,
    };
    let (kept, later) = legion::manager::sessions::winner(vec![
        copy("n1", 1_000, 1),
        copy("n2", 2_000, 2),
        copy("n0", 2_000, 2),
    ])
    .expect("a winner");
    assert_eq!(kept.node_id, "n0");
    let mut stopped: Vec<_> = later.into_iter().map(|copy| copy.node_id).collect();
    stopped.sort();
    assert_eq!(stopped, vec!["n1", "n2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_writes_one_entry_and_the_node_adopts_the_session_test() -> R<()> {
    let cluster = Cluster::new("agents-promote", &["n1"]).await?;
    let session = cluster.create(None).await?;
    let other = cluster.create(None).await?;
    cluster.report("n1").await?;

    let before = cluster.raft_entries();
    let promoted = cluster.promote(&session.instance_id, "user-42").await?;
    assert_eq!(promoted.instance_id, "user-42");
    assert_eq!(promoted.promoted_from, session.instance_id);
    assert_eq!(promoted.node_id, "n1");
    assert!(promoted.durable);
    assert!(!promoted.deduplicated);
    assert_eq!(cluster.raft_entries(), before + 1, "promotion is one entry");

    let info = cluster.get("user-42").await?;
    assert_eq!(info.mode, "keyed");
    assert!(info.durable);
    assert_eq!(
        info.promoted_from.as_deref(),
        Some(session.instance_id.as_str())
    );
    assert_eq!(info.current_alloc_id, Some(promoted.allocation_id));
    let allocation = cluster
        .store()
        .get_allocation_by_id(promoted.allocation_id)?
        .context("allocation row")?;
    assert_eq!(allocation.node_id, "n1");
    assert_eq!(allocation.instance_id.as_deref(), Some("user-42"));

    // The push names the session, so the node adopts it instead of
    // starting a second copy.
    let mut adopted = false;
    for _ in 0..100 {
        adopted = cluster.node("n1").lock().posts.iter().any(|post| {
            post.allocation_id == promoted.allocation_id
                && post.promoted_from_session == session.instance_id
        });
        if adopted {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        adopted,
        "the promoted allocation was pushed with promoted_from_session"
    );

    let events = cluster
        .store()
        .list_scheduler_events(&SchedulerEventFilter {
            kind: Some("instance.promoted".into()),
            limit: Some(10),
            ..Default::default()
        })?;
    assert_eq!(events.len(), 1);

    // A retry without a key reports the same promotion.
    let again = cluster.promote(&session.instance_id, "user-42").await?;
    assert!(again.deduplicated);
    assert_eq!(again.allocation_id, promoted.allocation_id);
    // The name is taken for any other session.
    let taken = cluster
        .promote(&other.instance_id, "user-42")
        .await
        .expect_err("name taken");
    assert_eq!(taken.code(), Code::AlreadyExists, "{taken:?}");
    // Promotion without a name keeps the session id.
    let kept = cluster.promote(&other.instance_id, "").await?;
    assert_eq!(kept.instance_id, other.instance_id);
    let missing = cluster
        .promote("workspace-cccccccccccccccccccccccccc", "")
        .await
        .expect_err("no such session");
    assert_eq!(missing.code(), Code::NotFound);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_and_destroy_end_a_session_and_other_verbs_ask_for_promotion_test() -> R<()> {
    let cluster = Cluster::new("agents-stop", &["n1"]).await?;
    let a = cluster.create(None).await?;
    let b = cluster.create(None).await?;
    cluster.report("n1").await?;
    let before = cluster.raft_entries();

    let suspend = cluster
        .svc
        .suspend_instance(Request::new(SuspendInstanceRequest {
            instance_id: a.instance_id.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("a session cannot be suspended");
    assert_eq!(suspend.code(), Code::FailedPrecondition);
    assert!(suspend.message().contains("promote"), "{suspend:?}");

    let stopped = cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: a.instance_id.clone(),
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    assert!(
        stopped.message.starts_with("Stopped session"),
        "{stopped:?}"
    );
    assert_eq!(cluster.node("n1").lock().stops, vec![a.instance_id.clone()]);
    assert_eq!(
        cluster.get(&a.instance_id).await.expect_err("ended").code(),
        Code::NotFound
    );
    // A retry on the same manager is a success, not NotFound.
    let again = cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: a.instance_id.clone(),
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    assert!(again.message.contains("already ended"), "{again:?}");

    let destroyed = cluster
        .svc
        .destroy_instance(Request::new(DestroyInstanceRequest {
            instance_id: b.instance_id.clone(),
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    assert!(
        destroyed.message.starts_with("Destroyed session"),
        "{destroyed:?}"
    );
    assert!(cluster.node("n1").lock().sessions.is_empty());
    assert_eq!(
        cluster.raft_entries(),
        before,
        "ending a session writes nothing"
    );
    Ok(())
}

/// A session whose task has a suspend block sleeps on its node: suspend
/// and start route by the id's tag to `Worker.SuspendSessions` and
/// `Worker.ResumeSession`, a full envelope keeps it suspended, an evicted
/// session answers NOT_FOUND with the node's account, and none of it
/// writes a raft entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_with_a_suspend_block_sleeps_on_its_node_test() -> R<()> {
    let cluster = Cluster::with_suspend(
        "agents-sleep",
        &["n1"],
        json!({ "durability": "local", "idle": { "after": "5m" } }),
    )
    .await?;
    let a = cluster.create(None).await?;
    cluster.report("n1").await?;
    let before = cluster.raft_entries();
    let suspend = |id: String| {
        cluster
            .svc
            .suspend_instance(Request::new(SuspendInstanceRequest {
                instance_id: id,
                ..Default::default()
            }))
    };
    let start = |id: String| {
        cluster
            .svc
            .start_instance(Request::new(StartInstanceRequest {
                instance_id: id,
                ..Default::default()
            }))
    };

    let slept = suspend(a.instance_id.clone()).await?.into_inner();
    assert_eq!(slept.durability, "local");
    assert_eq!(slept.node_id, "n1");
    assert_eq!(slept.snapshot_id, "snap-fake");
    assert_eq!(cluster.get(&a.instance_id).await?.state, "suspended");
    let again = suspend(a.instance_id.clone()).await?.into_inner();
    assert!(again.message.contains("already suspended"), "{again:?}");

    // A full envelope refuses the resume as retryable; the session stays.
    cluster.node("n1").lock().full = true;
    let full = start(a.instance_id.clone())
        .await
        .expect_err("the envelope is full");
    assert_eq!(full.code(), Code::ResourceExhausted);
    assert!(full.message().contains("stays suspended"), "{full:?}");
    cluster.node("n1").lock().full = false;

    let resumed = start(a.instance_id.clone()).await?.into_inner();
    assert!(resumed.message.contains("resumed on n1"), "{resumed:?}");
    assert_eq!(resumed.resuming_from, "local");
    assert_eq!(cluster.get(&a.instance_id).await?.state, "active");
    let running = start(a.instance_id.clone()).await?.into_inner();
    assert!(running.message.contains("already running"), "{running:?}");

    // An evicted session: its node's account, with the hint.
    cluster.node("n1").lock().sessions.remove(&a.instance_id);
    cluster.node("n1").lock().evicted.insert(
        a.instance_id.clone(),
        "at 2026-10-12T00:00:00Z (expired)".into(),
    );
    for err in [
        start(a.instance_id.clone()).await.expect_err("evicted"),
        cluster.get(&a.instance_id).await.expect_err("evicted"),
        suspend(a.instance_id.clone()).await.expect_err("evicted"),
    ] {
        assert_eq!(err.code(), Code::NotFound, "{err:?}");
        assert!(err.message().starts_with("session "), "{err:?}");
        assert!(err.message().contains("cannot be restored"), "{err:?}");
    }
    assert_eq!(
        cluster.raft_entries(),
        before,
        "suspend and resume of a session write nothing"
    );
    Ok(())
}

/// A session whose node does not answer: UNAVAILABLE naming the node, not
/// NOT_FOUND.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_on_a_silent_node_answers_unavailable_test() -> R<()> {
    let cluster = Cluster::with_suspend(
        "agents-silent",
        &["n1"],
        json!({ "durability": "local", "idle": { "after": "5m" } }),
    )
    .await?;
    // An id tagged for n1 that no index has: point n1 at a dead address.
    let id = legion::structs::mint_session_id(TASK, "n1");
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let dead = listener.local_addr()?;
    drop(listener);
    let mut node = cluster.store().get_node("n1")?;
    node.config_attrs["workerAddress"] = json!(format!("https://{dead}"));
    cluster.store().insert_node(NodeMsg::from(node))?;
    let err = cluster
        .svc
        .start_instance(Request::new(StartInstanceRequest {
            instance_id: id.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("n1 does not answer");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(
        err.message()
            .contains(&format!("node n1 holds session {id}")),
        "{err:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_create_on_a_session_task_is_a_keyed_instance_test() -> R<()> {
    let cluster = Cluster::new("agents-named", &["n1"]).await?;
    let reply = cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: cluster.job.clone(),
            instance_id: Some("user-7".into()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(reply.mode, "keyed");
    assert!(reply.durable);
    assert!(reply.node_id.is_empty());
    assert!(cluster.running().values().all(|count| *count == 0));
    Ok(())
}

/// Session start latency through the router, against fake nodes on
/// loopback gRPC: the control plane's share of a start (pick a node, send
/// the start, record it), with no raft entry. Run with
/// `cargo test --release --test session_integration -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; prints numbers"]
async fn session_start_latency_measure() -> R<()> {
    use futures::StreamExt;
    const NODES: usize = 16;
    const SERIAL: usize = 2_000;
    const CONCURRENT: usize = 3_000;
    const IN_FLIGHT: usize = 64;
    let names: Vec<String> = (0..NODES).map(|index| format!("n{index:02}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    // 380 sessions per envelope, 6,080 in all: room for both phases.
    let cluster = Cluster::with_sizes(
        "agents-measure",
        &names,
        json!({ "cpu": 1900, "memory": 60000 }),
        json!({ "cpu": 5, "memory": 128 }),
    )
    .await?;
    let quantile = |latencies: &mut Vec<std::time::Duration>, q: f64| {
        latencies.sort();
        latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)]
    };
    let before = cluster.raft_entries();

    let mut latencies = Vec::with_capacity(SERIAL);
    let started = std::time::Instant::now();
    for _ in 0..SERIAL {
        let at = std::time::Instant::now();
        cluster.create(None).await?;
        latencies.push(at.elapsed());
    }
    let elapsed = started.elapsed();
    println!(
        "serial: {SERIAL} starts over {NODES} nodes, {:.0}/s; p50 {:?}, p99 {:?}",
        SERIAL as f64 / elapsed.as_secs_f64(),
        quantile(&mut latencies, 0.50),
        quantile(&mut latencies, 0.99),
    );

    let started = std::time::Instant::now();
    let results: Vec<(std::time::Duration, bool)> = futures::stream::iter(0..CONCURRENT)
        .map(|_| async {
            let at = std::time::Instant::now();
            let ok = cluster.create(None).await.is_ok();
            (at.elapsed(), ok)
        })
        .buffer_unordered(IN_FLIGHT)
        .collect()
        .await;
    let elapsed = started.elapsed();
    let failed = results.iter().filter(|(_, ok)| !ok).count();
    let mut latencies: Vec<_> = results.into_iter().map(|(latency, _)| latency).collect();
    println!(
        "concurrent: {CONCURRENT} starts, {IN_FLIGHT} in flight, {:.0}/s; p50 {:?}, p99 {:?}; \
         {failed} refused",
        CONCURRENT as f64 / elapsed.as_secs_f64(),
        quantile(&mut latencies, 0.50),
        quantile(&mut latencies, 0.99),
    );
    println!("raft entries +{}", cluster.raft_entries() - before);
    assert_eq!(cluster.raft_entries(), before);
    Ok(())
}
