//! The manager's half of the `isolate` runtime (docs/task-runtime-isolate.md):
//! runs routed to a node with their isolate fields, a session isolate's id
//! pinning its node, the isolate RPCs (`GetIsolateStats` from heartbeats,
//! `GetIsolate` forwarded with its job ACL), session isolates that never
//! enter the session index, promotion refused, and a disabled `isolate`
//! runtime that keeps the envelope and lets an `auto` run reach the node.
//!
//! Each node is a fake worker gRPC server on the real mTLS worker plane (as
//! in `session_integration.rs`) that answers the way the isolate supervisor
//! does: it decides the tier, fills the reply's isolate fields, and keeps
//! session isolates out of its session report. The node side itself is
//! tested in `tests/integration/isolate.rs` and the worker crate.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use legion::cluster_tls::{
    certified_key, server_config, tls_incoming, ClusterClientVerifier, SwappableIdentity,
    SwappableRoots,
};
use legion::manager::acl::Authenticator;
use legion::manager::eval::EvalExt;
use legion::manager::server::ManagerDaemonService;
use legion::mesh_ca::{generate_csr, sign_csr, CsrSubject};
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{AclConfig, ManagerConfigFull};
use legion::structs::job::JobConfig;
use legion::structs::manager_server::Manager;
use legion::structs::worker_server::{Worker, WorkerServer};
use legion::structs::{
    CreateInstanceRequest, ExecClientFrame, ExecServerFrame, GetInstanceRequest, GetIsolateReply,
    GetIsolateRequest, GetIsolateStatsReply, GetIsolateStatsRequest, GetSessionReply,
    GetSessionRequest, GetTaskLogsReply, GetTaskLogsRequest, HeartbeatRequest, IsolateHostStats,
    IsolateNodeStats, IsolateTaskStats, ListInstancesRequest, Node, NodeMsg, PostWorkReply,
    PostWorkRequest, ProbeReply, ProbeRequest, PromoteInstanceRequest, RestartWorkerReply,
    RestartWorkerRequest, ResumeSessionReply, ResumeSessionRequest, RunSessionRequest,
    RunSessionResponse, SessionStatus, SetRuntimeEnabledRequest, StartClaimReply,
    StartClaimRequest, StartSessionReply, StartSessionRequest, StopSessionReply,
    StopSessionRequest, StopSessionsReply, StopSessionsRequest, Submission, SuspendSessionsReply,
    SuspendSessionsRequest,
};
use tonic::{Code, Request, Response, Status, Streaming};

const JOB: &str = "code-runner";
const TASK: &str = "run";
const VM_TASK: &str = "machine";

/// What one fake node holds.
#[derive(Default)]
struct NodeState {
    node_id: String,
    /// Session isolates it holds, as `Worker.GetSession` synthesizes them.
    sessions: BTreeMap<String, SessionStatus>,
    /// Every run request it received.
    runs: Vec<RunSessionRequest>,
    /// What `Worker.GetIsolateStats` answers.
    stats: Vec<IsolateTaskStats>,
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

    /// A session start on an isolate task opens a session isolate: held
    /// here, never in a session report.
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        let allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("no allocation"))?;
        if let Some(provisional) = allocation.provisional.as_ref() {
            if provisional.session.is_some() {
                self.lock().sessions.insert(
                    provisional.instance_id.clone(),
                    SessionStatus {
                        session_id: provisional.instance_id.clone(),
                        job_id: allocation.job_id.clone(),
                        // The supervisor's synthesized status names no job
                        // config.
                        job_config_id: String::new(),
                        task_name: allocation.task_name.clone(),
                        state: "running".into(),
                        ..Default::default()
                    },
                );
            }
        }
        Ok(Response::new(PostWorkReply {
            message: "Task accepted".into(),
        }))
    }

    async fn stop_sessions(
        &self,
        request: Request<StopSessionsRequest>,
    ) -> Result<Response<StopSessionsReply>, Status> {
        let mut state = self.lock();
        let stopped = request
            .into_inner()
            .session_ids
            .into_iter()
            .filter(|id| state.sessions.remove(id).is_some())
            .collect();
        Ok(Response::new(StopSessionsReply { stopped }))
    }

    async fn suspend_sessions(
        &self,
        _request: Request<SuspendSessionsRequest>,
    ) -> Result<Response<SuspendSessionsReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    async fn resume_session(
        &self,
        _request: Request<ResumeSessionRequest>,
    ) -> Result<Response<ResumeSessionReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionReply>, Status> {
        let id = request.into_inner().session_id;
        match self.lock().sessions.get(&id) {
            Some(status) => Ok(Response::new(GetSessionReply {
                session: Some(status.clone()),
                node_id: String::new(),
                children: Vec::new(),
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

    async fn resize_session(
        &self,
        _request: Request<legion::structs::ResizeSessionRequest>,
    ) -> Result<Response<legion::structs::ResizeSessionReply>, Status> {
        Err(Status::unimplemented("fake node"))
    }

    /// Answers as the isolate supervisor does: decides the tier and fills
    /// the reply's isolate fields.
    async fn run_session(
        &self,
        request: Request<RunSessionRequest>,
    ) -> Result<Response<RunSessionResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.lock();
        let session = !request.session_id.is_empty();
        if session && !state.sessions.contains_key(&request.session_id) {
            return Err(Status::not_found(format!(
                "not_found: no session isolate {}",
                request.session_id
            )));
        }
        let tier = match request.tier.as_str() {
            "" | "auto" => "isolate",
            other => other,
        };
        let reply = RunSessionResponse {
            node_id: state.node_id.clone(),
            session_id: request.session_id.clone(),
            task_name: request.task_name.clone(),
            exit: Some(0),
            stdout: b"42\n".to_vec(),
            tier: tier.into(),
            tier_reason: if request.tier.is_empty() || request.tier == "auto" {
                "auto: no imports outside the image".into()
            } else {
                "requested".into()
            },
            cpu_micros: 3_100,
            wall_micros: 4_000,
            cpu_ms: 3,
            wall_ms: 4,
            isolate_start: if session { "warm" } else { "fresh" }.into(),
            ..Default::default()
        };
        state.runs.push(request);
        Ok(Response::new(reply))
    }

    async fn get_isolate_stats(
        &self,
        _request: Request<GetIsolateStatsRequest>,
    ) -> Result<Response<GetIsolateStatsReply>, Status> {
        let state = self.lock();
        Ok(Response::new(GetIsolateStatsReply {
            nodes: vec![IsolateNodeStats {
                node_id: state.node_id.clone(),
                tasks: state.stats.clone(),
                hosts: Vec::new(),
                age_ms: 0,
            }],
        }))
    }

    async fn get_isolate(
        &self,
        request: Request<GetIsolateRequest>,
    ) -> Result<Response<GetIsolateReply>, Status> {
        let id = request.into_inner().session_id;
        let state = self.lock();
        let status = state
            .sessions
            .get(&id)
            .ok_or_else(|| Status::not_found(format!("no session isolate {id}")))?;
        Ok(Response::new(GetIsolateReply {
            session_id: id.clone(),
            node_id: state.node_id.clone(),
            job_id: status.job_id.clone(),
            task_name: status.task_name.clone(),
            state: "hot".into(),
            invocations: 7,
            ..Default::default()
        }))
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

    async fn forward_app_request(
        &self,
        _request: Request<legion::structs::ForwardAppRequestRequest>,
    ) -> Result<Response<legion::structs::ForwardAppRequestReply>, Status> {
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

/// How a cluster is set up.
#[derive(Default)]
struct Setup {
    /// `config.lifetime` of the isolate task.
    lifetime: &'static str,
    /// Runtimes to disable before the job is submitted.
    disabled: Vec<&'static str>,
    /// Enforce ACLs.
    acl: bool,
}

struct Cluster {
    svc: ManagerDaemonService,
    nodes: BTreeMap<String, FakeNode>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn new(nodes: &[&str], setup: Setup) -> R<Self> {
        let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
        let config = ManagerConfigFull {
            store_path: dir.path().join("store").to_str().context("utf-8")?.into(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        };
        let store = tokio::task::block_in_place(|| Store::start_storage(&config))?;
        let svc = if setup.acl {
            let auth = Arc::new(
                Authenticator::from_config(
                    &AclConfig {
                        enabled: Some(true),
                        ..Default::default()
                    }
                    .with_defaults(),
                )
                .map_err(|status| anyhow::anyhow!(status.to_string()))?,
            );
            ManagerDaemonService::with_acl(store, auth)
        } else {
            ManagerDaemonService::new(store)
        };
        let mut cluster = Cluster {
            svc,
            nodes: BTreeMap::new(),
            _dir: dir,
        };
        let ca = cluster.store().ensure_mesh_ca()?;
        let roots = SwappableRoots::from_pem(&ca.cert_pem)?;
        for node_id in nodes {
            let node = FakeNode {
                state: Arc::new(Mutex::new(NodeState {
                    node_id: node_id.to_string(),
                    ..Default::default()
                })),
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
            let tls = server_config(
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
                    .serve_with_incoming(tls_incoming(listener, tls))
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
                dynamic_attrs: json!({
                    "capabilities": ["sessions", "isolates"],
                    "protocolVersion": 3,
                }),
                health_checks: json!({}),
            }))?;
            cluster.nodes.insert(node_id.to_string(), node);
        }
        for runtime in &setup.disabled {
            cluster
                .svc
                .set_runtime_enabled(Request::new(SetRuntimeEnabledRequest {
                    runtime: runtime.to_string(),
                    enabled: false,
                    reason: "CVE-2026-0001".into(),
                    ..Default::default()
                }))
                .await?;
        }
        cluster.submit(if setup.lifetime.is_empty() {
            "request"
        } else {
            setup.lifetime
        })?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn node(&self, id: &str) -> &FakeNode {
        &self.nodes[id]
    }

    /// The job: an isolate task whose VM task is a sibling session task
    /// with a `run` block, each with an envelope per node.
    fn submit(&self, lifetime: &str) -> R<()> {
        let spec = json!({
            "name": JOB,
            "tasks": [
                {
                    "name": TASK,
                    "runtime": "isolate",
                    "config": {
                        "image": "python",
                        "lifetime": lifetime,
                        "limits": { "cpu": "100ms", "wall": "30s" },
                        "vm": { "task": VM_TASK },
                    },
                    "resources": { "cpu": 10, "memory": 2, "memoryMax": 128 },
                    "stateful": {
                        "mode": "session",
                        "envelope": { "resources": { "cpu": 2000, "memory": 2048 } },
                    },
                    "children": { "tasks": [VM_TASK], "max": 1 },
                },
                {
                    "name": VM_TASK,
                    "runtime": "noop",
                    "resources": { "cpu": 500, "memory": 512 },
                    "stateful": {
                        "mode": "session",
                        "envelope": { "resources": { "cpu": 2000, "memory": 2048 } },
                    },
                    "run": { "command": ["python3", "/legion/run/main.py"], "wall": "5m" },
                },
            ],
        });
        let config: JobConfig = serde_json::from_value(spec.clone())?;
        tokio::task::block_in_place(|| -> R<()> {
            self.store().handle_job_submit(
                &config,
                &[Submission {
                    path: format!("{JOB}.json"),
                    content: spec.to_string(),
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

    /// The nodes holding an envelope for `task`.
    fn envelope_nodes(&self, task: &str) -> Vec<String> {
        legion::manager::sessions::envelope_nodes(self.store(), JOB, task).unwrap_or_default()
    }

    async fn run(&self, request: RunSessionRequest) -> Result<RunSessionResponse, Status> {
        self.svc
            .run_session(Request::new(RunSessionRequest {
                job_id: JOB.into(),
                ..request
            }))
            .await
            .map(Response::into_inner)
    }

    fn runs(&self) -> Vec<(String, RunSessionRequest)> {
        self.nodes
            .iter()
            .flat_map(|(id, node)| {
                node.lock()
                    .runs
                    .iter()
                    .map(|run| (id.clone(), run.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// A token holding `job:read` (and `job:write`) on `jobs` only.
    fn token_for(&self, name: &str, jobs: &[&str]) -> R<String> {
        let rules = json!([{
            "resources": ["job"],
            "namespaces": ["*"],
            "actions": ["read", "write"],
            "names": jobs,
        }])
        .to_string();
        self.store().upsert_acl_policy(name, "", &rules)?;
        let (_, secret) = self
            .svc
            .auth
            .mint_token(self.store(), name, "client", &[name.into()], None)
            .map_err(|status| anyhow::anyhow!(status.to_string()))?;
        Ok(secret)
    }
}

fn with_token<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

fn file(path: &str, content: &str) -> legion::structs::RunFile {
    legion::structs::RunFile {
        path: path.into(),
        content: content.as_bytes().to_vec(),
        executable: false,
    }
}

fn task_stats(task: &str, resident: u32) -> IsolateTaskStats {
    IsolateTaskStats {
        job_id: JOB.into(),
        task_name: task.into(),
        cordon: "default".into(),
        resident,
        active: 2,
        invocations_per_second: 840.0,
        ..Default::default()
    }
}

/// A run with no task named picks the isolate task (not its VM task, which
/// also takes runs), reaches a node with `tier` and `tenant` as sent, and
/// returns the node's isolate fields untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolate_run_reaches_the_node_with_its_fields_test() -> R<()> {
    let cluster = Cluster::new(&["w-1"], Setup::default()).await?;
    assert_eq!(cluster.envelope_nodes(TASK), ["w-1"]);
    let reply = cluster
        .run(RunSessionRequest {
            files: vec![file("main.py", "print(42)\n")],
            tier: "isolate".into(),
            tenant: "acme".into(),
            ..Default::default()
        })
        .await?;
    assert_eq!(reply.node_id, "w-1");
    assert_eq!(reply.task_name, TASK);
    assert_eq!(reply.tier, "isolate");
    assert_eq!(reply.tier_reason, "requested");
    assert_eq!(reply.cpu_micros, 3_100);
    assert_eq!(reply.isolate_start, "fresh");
    let runs = cluster.runs();
    assert_eq!(runs.len(), 1);
    let (node, sent) = &runs[0];
    assert_eq!(node, "w-1");
    assert_eq!(sent.task_name, TASK);
    assert_eq!(sent.tier, "isolate");
    assert_eq!(sent.tenant, "acme");

    // A burst of concurrent runs is not throttled by the router as if each
    // were a full session: every one reaches the node.
    let burst: Vec<_> = (0..64)
        .map(|_| {
            cluster.run(RunSessionRequest {
                files: vec![file("main.py", "print(1)\n")],
                ..Default::default()
            })
        })
        .collect();
    for reply in futures::future::join_all(burst).await {
        assert_eq!(reply?.tier, "isolate");
    }
    assert_eq!(cluster.runs().len(), 65);
    Ok(())
}

/// `command` has nothing to replace on an isolate task, and the isolate
/// fields have no meaning on another runtime: both InvalidArgument before
/// anything is routed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolate_fields_are_refused_where_they_do_not_apply_test() -> R<()> {
    let cluster = Cluster::new(&["w-1"], Setup::default()).await?;
    let err = cluster
        .run(RunSessionRequest {
            task_name: TASK.into(),
            command: vec!["python3".into(), "x.py".into()],
            ..Default::default()
        })
        .await
        .expect_err("command on an isolate task");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert!(err.message().contains("no command to replace"), "{err:?}");

    for request in [
        RunSessionRequest {
            tier: "vm".into(),
            ..Default::default()
        },
        RunSessionRequest {
            tenant: "acme".into(),
            ..Default::default()
        },
        RunSessionRequest {
            session_id: "machine-x.y".into(),
            ..Default::default()
        },
    ] {
        let err = cluster
            .run(RunSessionRequest {
                task_name: VM_TASK.into(),
                ..request
            })
            .await
            .expect_err("isolate field on a noop task");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert!(err
            .message()
            .contains("applies only to tasks of the isolate runtime"));
    }

    let err = cluster
        .run(RunSessionRequest {
            task_name: TASK.into(),
            tier: "gpu".into(),
            ..Default::default()
        })
        .await
        .expect_err("unknown tier");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // `session_id` on a task whose isolates live for one request.
    let err = cluster
        .run(RunSessionRequest {
            task_name: TASK.into(),
            session_id: legion::structs::mint_session_id(TASK, "w-1"),
            ..Default::default()
        })
        .await
        .expect_err("no session isolates");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(cluster.runs().is_empty(), "nothing reached a node");
    Ok(())
}

/// A session isolate is started through CreateInstance and answered by the
/// node: the reply is the confirmation, nothing waits on the session index
/// (which never lists it), and its id routes later calls to its node: a
/// run, GetInstance, GetIsolate. Promotion is refused before any write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_isolate_is_reached_by_the_tag_in_its_id_test() -> R<()> {
    let cluster = Cluster::new(
        &["w-1", "w-2"],
        Setup {
            lifetime: "session",
            ..Default::default()
        },
    )
    .await?;
    let started = cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: JOB.into(),
            task: Some(TASK.into()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(started.mode, "session");
    assert_eq!(
        started.state, "running",
        "the node's answer is the confirmation"
    );
    let id = started.instance_id.clone();
    let node = started.node_id.clone();
    assert_eq!(
        legion::structs::session_node_tag(&id),
        Some(legion::structs::node_tag(&node).as_str())
    );
    assert!(cluster.node(&node).lock().sessions.contains_key(&id));

    // Not in the session index, so not in a listing.
    let listed = cluster
        .svc
        .list_instances(Request::new(ListInstancesRequest {
            job_id: JOB.into(),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(
        listed.instances.iter().all(|info| info.instance_id != id),
        "{listed:?}"
    );

    // GetInstance asks the node the tag names.
    let info = cluster
        .svc
        .get_instance(Request::new(GetInstanceRequest {
            instance_id: id.clone(),
        }))
        .await?
        .into_inner()
        .instance
        .context("instance")?;
    assert_eq!(info.node_id.as_deref(), Some(node.as_str()));
    assert_eq!(info.mode, "session");

    // A run in it goes to its node only.
    let reply = cluster
        .run(RunSessionRequest {
            task_name: TASK.into(),
            session_id: id.clone(),
            files: vec![file("main.py", "print(42)\n")],
            ..Default::default()
        })
        .await?;
    assert_eq!(reply.node_id, node);
    assert_eq!(reply.isolate_start, "warm");
    assert_eq!(cluster.runs().len(), 1);
    assert_eq!(cluster.runs()[0].0, node);

    // An id whose tag names no node, or that the node does not hold.
    let other = if node == "w-1" { "w-2" } else { "w-1" };
    for (missing, why) in [
        (
            legion::structs::mint_session_id(TASK, "w-gone"),
            "no node matches",
        ),
        (
            legion::structs::mint_session_id(TASK, other),
            "no session isolate",
        ),
    ] {
        let err = cluster
            .run(RunSessionRequest {
                task_name: TASK.into(),
                session_id: missing,
                ..Default::default()
            })
            .await
            .expect_err("unknown session isolate");
        assert_eq!(err.code(), Code::NotFound, "{err:?}");
        assert!(err.message().contains(why), "{err:?}");
    }

    // GetIsolate forwards to the node.
    let isolate = cluster
        .svc
        .get_isolate(Request::new(GetIsolateRequest {
            session_id: id.clone(),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(isolate.node_id, node);
    assert_eq!(isolate.job_id, JOB);
    assert_eq!(isolate.state, "hot");
    let err = cluster
        .svc
        .get_isolate(Request::new(GetIsolateRequest {
            session_id: "run-abcdefghijklmnopqrstuvwxyz".into(),
            ..Default::default()
        }))
        .await
        .expect_err("no tag, no node");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    let err = cluster
        .svc
        .get_isolate(Request::new(GetIsolateRequest {
            session_id: legion::structs::mint_session_id(TASK, other),
            ..Default::default()
        }))
        .await
        .expect_err("the node holds no such isolate");
    assert_eq!(err.code(), Code::NotFound, "{err:?}");

    // Promotion: FAILED_PRECONDITION with the calls-entry-10 text, and no
    // instance row.
    let err = cluster
        .svc
        .promote_instance(Request::new(PromoteInstanceRequest {
            instance_id: id.clone(),
            new_id: String::new(),
            idempotency_key: None,
        }))
        .await
        .expect_err("isolates are never promoted");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(
        err.message().starts_with(
            "isolate sessions cannot be promoted (a keyed instance is an allocation each); \
             give the session a stable id with an idempotency key and use \
             suspend.durability = \"store\" to keep it across nodes"
        ),
        "{err:?}"
    );
    assert!(cluster.store().list_instances(JOB)?.is_empty());

    // A named create would make a keyed instance: refused.
    let err = cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: JOB.into(),
            task: Some(TASK.into()),
            instance_id: Some("named-isolate".into()),
            ..Default::default()
        }))
        .await
        .expect_err("no keyed isolates");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // Stop reaches the node by the tag too.
    cluster
        .svc
        .stop_instance(Request::new(legion::structs::StopInstanceRequest {
            instance_id: id.clone(),
            ..Default::default()
        }))
        .await?;
    assert!(!cluster.node(&node).lock().sessions.contains_key(&id));
    Ok(())
}

/// A request-lifetime isolate task has nothing to start: FailedPrecondition
/// pointing at `legion session run`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_isolate_task_cannot_be_started_as_a_session_test() -> R<()> {
    let cluster = Cluster::new(&["w-1"], Setup::default()).await?;
    let err = cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: JOB.into(),
            task: Some(TASK.into()),
            ..Default::default()
        }))
        .await
        .expect_err("request isolates are not sessions");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("legion session run"), "{err:?}");
    assert!(cluster.node("w-1").lock().sessions.is_empty());
    Ok(())
}

/// Stats come from the heartbeats this manager holds, and a node with an
/// envelope it holds none from is asked directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolate_stats_come_from_heartbeats_test() -> R<()> {
    let cluster = Cluster::new(&["w-1", "w-2"], Setup::default()).await?;
    let mut nodes = cluster.envelope_nodes(TASK);
    nodes.sort();
    assert_eq!(nodes, ["w-1", "w-2"]);
    cluster
        .svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: "w-1".into(),
            isolate_tasks: vec![task_stats(TASK, 48), {
                IsolateTaskStats {
                    job_id: "another-job".into(),
                    ..task_stats(TASK, 9)
                }
            }],
            isolate_hosts: vec![
                IsolateHostStats {
                    cordon: "default".into(),
                    rss_bytes: 64 << 20,
                    ..Default::default()
                },
                IsolateHostStats {
                    cordon: "shared".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }))
        .await?;
    // w-2 heartbeats elsewhere (here: not at all) and answers directly.
    cluster.node("w-2").lock().stats = vec![task_stats(TASK, 5)];

    let reply = cluster
        .svc
        .get_isolate_stats(Request::new(GetIsolateStatsRequest {
            job_id: JOB.into(),
            task_name: String::new(),
        }))
        .await?
        .into_inner();
    assert_eq!(reply.nodes.len(), 2, "{reply:?}");
    let w1 = &reply.nodes[0];
    assert_eq!(w1.node_id, "w-1");
    assert_eq!(w1.tasks.len(), 1, "only this job's task");
    assert_eq!(w1.tasks[0].resident, 48);
    assert_eq!(w1.hosts.len(), 1, "only the cordon serving the task");
    assert_eq!(w1.hosts[0].rss_bytes, 64 << 20);
    assert!(w1.age_ms < 60_000);
    let w2 = &reply.nodes[1];
    assert_eq!(w2.node_id, "w-2");
    assert_eq!(w2.tasks[0].resident, 5);
    assert_eq!(w2.age_ms, 0, "asked directly");

    let refuse = |job: &str, task: &str| {
        cluster
            .svc
            .get_isolate_stats(Request::new(GetIsolateStatsRequest {
                job_id: job.into(),
                task_name: task.into(),
            }))
    };
    assert_eq!(
        refuse("", "").await.expect_err("no job").code(),
        Code::InvalidArgument
    );
    assert_eq!(
        refuse("no-such-job", "").await.expect_err("no job").code(),
        Code::NotFound
    );
    let err = refuse(JOB, VM_TASK).await.expect_err("not an isolate task");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");

    // A beat with no isolate report forgets the node's numbers; w-1 is
    // then asked directly, and reports nothing for the task.
    cluster
        .svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: "w-1".into(),
            ..Default::default()
        }))
        .await?;
    let reply = refuse(JOB, TASK).await?.into_inner();
    let nodes: Vec<&str> = reply.nodes.iter().map(|n| n.node_id.as_str()).collect();
    assert_eq!(nodes, ["w-2"]);
    Ok(())
}

/// GetIsolate checks coarsely before it asks the node and against the
/// isolate's own job after: a caller who cannot read that job gets
/// PermissionDenied, never the data. GetIsolateStats checks before it
/// reads the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_isolate_enforces_the_isolates_job_acl_test() -> R<()> {
    let cluster = Cluster::new(
        &["w-1"],
        Setup {
            lifetime: "session",
            acl: true,
            ..Default::default()
        },
    )
    .await?;
    let id = legion::structs::mint_session_id(TASK, "w-1");
    cluster.node("w-1").lock().sessions.insert(
        id.clone(),
        SessionStatus {
            session_id: id.clone(),
            job_id: JOB.into(),
            task_name: TASK.into(),
            state: "running".into(),
            ..Default::default()
        },
    );
    let reader = cluster.token_for("isolate-reader", &[JOB])?;
    let outsider = cluster.token_for("isolate-outsider", &["some-other-job"])?;
    let get = |token: &str| {
        cluster.svc.get_isolate(with_token(
            GetIsolateRequest {
                session_id: id.clone(),
                ..Default::default()
            },
            token,
        ))
    };
    assert_eq!(get(&reader).await?.into_inner().job_id, JOB);
    let err = get(&outsider).await.expect_err("not its job");
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");

    let stats = |token: &str, job: &str| {
        cluster.svc.get_isolate_stats(with_token(
            GetIsolateStatsRequest {
                job_id: job.into(),
                task_name: String::new(),
            },
            token,
        ))
    };
    assert!(stats(&reader, JOB).await.is_ok());
    let err = stats(&outsider, JOB).await.expect_err("not its job");
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    // Check before fetch: a job that does not exist reads the same.
    let err = stats(&outsider, "no-such-job").await.expect_err("denied");
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    Ok(())
}

/// `legion operator runtime disable isolate`: the isolate task's envelope
/// is an `envelope` allocation and still places, the VM task is
/// unaffected, an `auto` run reaches the node (which sends it to the VM),
/// and a run pinned to the isolate tier is refused up front.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disabled_isolate_runtime_keeps_the_envelope_and_auto_runs_test() -> R<()> {
    let cluster = Cluster::new(
        &["w-1"],
        Setup {
            disabled: vec!["isolate"],
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(cluster.envelope_nodes(TASK), ["w-1"], "the envelope placed");
    assert_eq!(cluster.envelope_nodes(VM_TASK), ["w-1"]);

    let reply = cluster
        .run(RunSessionRequest {
            task_name: TASK.into(),
            files: vec![file("main.py", "print(42)\n")],
            ..Default::default()
        })
        .await?;
    assert_eq!(reply.node_id, "w-1");
    assert_eq!(cluster.runs().len(), 1, "the node decides");

    let err = cluster
        .run(RunSessionRequest {
            task_name: TASK.into(),
            tier: "isolate".into(),
            ..Default::default()
        })
        .await
        .expect_err("the isolate tier is disabled");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(err.message().contains("runtime_disabled"), "{err:?}");
    assert_eq!(cluster.runs().len(), 1);
    Ok(())
}
