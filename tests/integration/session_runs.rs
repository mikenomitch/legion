//! Session runs end to end (docs/session-runs.md): `Manager.RunSession`
//! routed to a real worker over the real mTLS worker plane, and
//! `Worker.RunSession` with a session ticket, on an `exec` session task.
//!
//! What is asserted here is the plan's contract: one call returns stdout,
//! stderr, the exit status, `result.json`, and the named files; a stopped
//! run is a successful call with `stopped`; no path leaves a session
//! running; nothing is written to raft; and an idempotent retry on the same
//! node is answered without running again.
//!
//! The permission matrix for `command` is in
//! `acl_endpoints::a_run_with_a_callers_command_needs_exec_write_test`, and
//! the caller classes of `Worker.RunSession` in `worker_plane_classes`.

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
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::JobConfig;
use legion::structs::manager_server::Manager;
use legion::structs::worker_server::{Worker, WorkerServer};
use legion::structs::{
    Node, NodeMsg, RatifyClaimReply, RatifyClaimRequest, RunFile, RunSessionRequest,
    RunSessionResponse, Submission,
};
use legion::worker::boot_ticket::{
    BootTicketVerifier, JwksSource, DEFAULT_CLOCK_SKEW, DEFAULT_JWKS_REFRESH,
};
use legion::worker::edge;
use legion::worker::server::{ClaimRatifier, EdgeBootRuntime, WorkerDaemonService};
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::WorkerTaskManager;
use tonic::{Code, Request, Status};

const JOB: &str = "code-runner";
const TASK: &str = "machine";
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
    /// The same worker the manager dials, for in-process edge calls.
    edge: WorkerDaemonService,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    issuer: Issuer,
    dir: tempfile::TempDir,
}

impl Cluster {
    /// A cluster whose session task carries this `run` block.
    async fn new(run: Value) -> R<Self> {
        let dir = tempfile::tempdir()?;
        let mut config = ManagerConfigFull {
            store_path: dir.path().join("store").to_str().context("utf-8")?.into(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-runs".into();
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
            edge: service(),
            task_manager,
            issuer,
            dir,
        };
        cluster.submit(run)?;
        cluster.wait_for_envelope().await?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn submit(&self, run: Value) -> R<()> {
        let config: JobConfig = serde_json::from_value(json!({
            "name": JOB,
            "task": {
                "name": TASK,
                "runtime": "exec",
                "config": { "command": ["sleep", "600"] },
                "resources": { "cpu": 100, "memory": 64 },
                "stateful": {
                    "mode": "session",
                    "envelope": { "resources": { "cpu": 400, "memory": 256 } },
                },
                "run": run,
            },
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

    /// The scheduler placed the envelope and pushed it; wait until the
    /// worker runs it and so knows the session task's spec.
    async fn wait_for_envelope(&self) -> R<()> {
        for _ in 0..200 {
            let known = self
                .task_manager
                .lock()
                .expect("task manager")
                .session_spec(JOB, TASK)
                .is_some();
            if known {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("the worker never received the envelope for {JOB}/{TASK}")
    }

    async fn run(&self, request: RunSessionRequest) -> Result<RunSessionResponse, Status> {
        self.svc
            .run_session(Request::new(RunSessionRequest {
                job_id: JOB.into(),
                ..request
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    /// Sessions the worker holds right now, running or not.
    fn sessions(&self) -> usize {
        self.task_manager
            .lock()
            .expect("task manager")
            .session_statuses()
            .len()
    }

    fn session_ticket(&self) -> R<String> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let claims = json!({
            "legion.io/scope": "session",
            "legion.io/job": JOB,
            "legion.io/job_config": self.store().get_job_config(JOB)?.id(),
            "legion.io/task": TASK,
            "legion.io/instance": Value::Null,
            "legion.io/pool": "default",
            "legion.io/gate": "serve",
            "legion.io/uses": 0,
        });
        Ok(self
            .issuer
            .issue_boot_ticket(
                "tkt_runs",
                "session:code-runner/machine",
                claims.as_object().cloned().unwrap_or_default(),
                now,
                now + 300,
            )?
            .token)
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

fn script(body: &str) -> RunFile {
    RunFile {
        path: "main.sh".into(),
        content: body.as_bytes().to_vec(),
        executable: false,
    }
}

fn own_command() -> Value {
    json!({ "command": ["sh", "/legion/run/main.sh"], "wall": "30s" })
}

/// One call: files and input in, stdout, stderr, exit, `result.json`, and
/// named files out, the session gone, and raft untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_returns_its_output_and_result_and_leaves_nothing_test() -> R<()> {
    let cluster = Cluster::new(own_command()).await?;
    let raft_before = cluster.store().raft_applied_entries()?;
    let reply = cluster
        .run(RunSessionRequest {
            files: vec![
                script(
                    "echo hello\n\
                     echo oops >&2\n\
                     test \"$(pwd)\" = \"$LEGION_RUN_DIR\" || exit 90\n\
                     test \"$LEGION_INSTANCE_MODE\" = session || exit 91\n\
                     n=$(tr -cd 0-9 < \"$LEGION_RUN_INPUT\")\n\
                     printf '{\"rows\": %s, \"greeting\": \"%s\"}' \"$n\" \"$(cat data/name.txt)\" \
                     > \"$LEGION_RUN_DIR/result.json\"\n\
                     mkdir -p out && echo csv > out/a.csv\n\
                     exit 3\n",
                ),
                RunFile {
                    path: "/legion/run/data/name.txt".into(),
                    content: b"ada".to_vec(),
                    executable: false,
                },
            ],
            input: Some(r#"{"n": 1204}"#.into()),
            get: vec!["out/a.csv".into(), "missing.txt".into()],
            ..Default::default()
        })
        .await?;
    assert_eq!(reply.exit, Some(3), "{reply:?}");
    assert_eq!(reply.stdout, b"hello\n");
    assert_eq!(reply.stderr, b"oops\n");
    assert!(reply.stopped.is_none());
    assert!(!reply.truncated && !reply.deduplicated);
    let result: Value = serde_json::from_str(&reply.result)?;
    assert_eq!(result, json!({ "rows": 1204, "greeting": "ada" }));
    assert_eq!(reply.files.len(), 2);
    assert_eq!(reply.files[0].path, "out/a.csv");
    assert_eq!(reply.files[0].content, b"csv\n");
    assert!(reply.files[1].missing);
    assert_eq!(reply.node_id, NODE);
    assert_eq!(reply.task_name, TASK);
    assert!(
        reply.session_id.starts_with("machine-"),
        "{}",
        reply.session_id
    );
    assert!(reply.run_id.starts_with("run-"));

    assert_eq!(cluster.sessions(), 0, "the run's session ended with it");
    assert_eq!(
        cluster.store().raft_applied_entries()?,
        raft_before,
        "a run writes nothing to raft"
    );
    Ok(())
}

/// A command that outlives `run.wall` is stopped; the call still succeeds,
/// with `stopped` and no exit status, and output cut at `run.output` says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_stopped_by_its_wall_limit_is_data_test() -> R<()> {
    let cluster = Cluster::new(json!({
        "command": ["sh", "/legion/run/main.sh"],
        "wall": "1s",
        "output": "16B",
    }))
    .await?;
    let reply = cluster
        .run(RunSessionRequest {
            files: vec![script("echo 0123456789abcdefOVERFLOW\nexec sleep 30\n")],
            ..Default::default()
        })
        .await?;
    let stop = reply.stopped.as_ref().expect("stopped");
    assert_eq!(stop.reason, "wall");
    assert_eq!(stop.limit, "1s");
    assert!(stop.hint.contains("run.wall"), "{stop:?}");
    assert_eq!(reply.exit, None);
    assert_eq!(reply.stdout, b"0123456789abcdef");
    assert!(reply.truncated);
    assert!(
        reply.wall_ms >= 1000 && reply.wall_ms < 20_000,
        "{}",
        reply.wall_ms
    );
    assert_eq!(cluster.sessions(), 0, "a stopped run's session ended too");
    Ok(())
}

/// A run that cannot execute its command is an error, and its session is
/// ended all the same: nothing is leaked between start and collect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_after_the_start_still_ends_the_session_test() -> R<()> {
    let cluster = Cluster::new(json!({
        "command": ["/nonexistent/legion-run-binary"],
        "wall": "10s",
    }))
    .await?;
    let status = cluster
        .run(RunSessionRequest::default())
        .await
        .expect_err("the command does not exist");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(status.message().contains("could not execute"), "{status:?}");
    assert_eq!(cluster.sessions(), 0, "a failed run's session ended");

    // Bad requests are refused before anything starts.
    for (request, needle) in [
        (
            RunSessionRequest {
                files: vec![RunFile {
                    path: "../escape.sh".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            "plain relative path",
        ),
        (
            RunSessionRequest {
                get: vec!["/etc/passwd".into()],
                ..Default::default()
            },
            "outside the run directory",
        ),
        (
            RunSessionRequest {
                input: Some("{not json".into()),
                ..Default::default()
            },
            "input is not JSON",
        ),
        (
            RunSessionRequest {
                command: vec!["id".into()],
                ..Default::default()
            },
            "does not allow --command",
        ),
    ] {
        let status = cluster.run(request).await.expect_err(needle);
        assert!(status.message().contains(needle), "{needle}: {status:?}");
        assert!(matches!(
            status.code(),
            Code::InvalidArgument | Code::FailedPrecondition
        ));
    }
    assert_eq!(cluster.sessions(), 0);
    Ok(())
}

/// The same key on the same node: the command runs once, a concurrent
/// retry waits for it, and a later one is answered from the kept reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idempotent_retry_is_answered_without_running_again_test() -> R<()> {
    let cluster = Cluster::new(own_command()).await?;
    let marker = cluster.dir.path().join("ran");
    let request = || RunSessionRequest {
        files: vec![script(&format!(
            "echo x >> {}\nsleep 1\necho done\n",
            marker.display()
        ))],
        idempotency_key: Some("attempt-1".into()),
        ..Default::default()
    };
    let (first, second) = tokio::join!(cluster.run(request()), cluster.run(request()));
    let (first, second) = (first?, second?);
    assert_eq!(first.stdout, b"done\n");
    assert_eq!(second.stdout, b"done\n");
    assert_eq!(first.session_id, second.session_id);
    assert!(
        first.deduplicated ^ second.deduplicated,
        "one ran and one waited: {} {}",
        first.deduplicated,
        second.deduplicated
    );
    let third = cluster.run(request()).await?;
    assert!(third.deduplicated);
    assert_eq!(third.run_id, first.run_id);
    assert_eq!(
        std::fs::read_to_string(&marker)?.lines().count(),
        1,
        "the command ran once for three calls"
    );

    // Another key, or the same key on a different request, runs again.
    let other = cluster
        .run(RunSessionRequest {
            idempotency_key: Some("attempt-2".into()),
            ..request()
        })
        .await?;
    assert!(!other.deduplicated);
    assert_eq!(std::fs::read_to_string(&marker)?.lines().count(), 2);
    assert_eq!(cluster.sessions(), 0);
    Ok(())
}

/// A dry run names the node and its room and starts nothing; a full
/// envelope answers RESOURCE_EXHAUSTED, which is retryable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_runs_nothing_and_a_full_envelope_is_retryable_test() -> R<()> {
    let cluster = Cluster::new(own_command()).await?;
    let marker = cluster.dir.path().join("ran");
    let reply = cluster
        .run(RunSessionRequest {
            files: vec![script(&format!("echo x >> {}\n", marker.display()))],
            dry_run: true,
            ..Default::default()
        })
        .await?;
    let dry = reply.dry_run.expect("a dry run answers dry_run");
    assert_eq!(dry.node_id, NODE);
    assert!(dry.has_room, "{}", dry.reason);
    assert_eq!(dry.command, ["sh", "/legion/run/main.sh"]);
    assert_eq!(dry.wall, "30s");
    assert!(reply.session_id.is_empty());
    assert!(!marker.exists(), "a dry run executes nothing");
    assert_eq!(cluster.sessions(), 0);

    // Four sessions fill the envelope (400 cpu / 100 each): four runs hold
    // it while a fifth is refused, retryably, and then fits.
    let hold = || {
        cluster.run(RunSessionRequest {
            files: vec![script("sleep 3\n")],
            ..Default::default()
        })
    };
    let holders = futures::future::join_all([hold(), hold(), hold(), hold()]);
    let probe = async {
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if cluster.sessions() == 4 {
                return Some(
                    cluster
                        .run(RunSessionRequest {
                            files: vec![script("true\n")],
                            ..Default::default()
                        })
                        .await,
                );
            }
        }
        None
    };
    let (held, refused) = tokio::join!(holders, probe);
    let status = refused
        .context("the four runs never held the envelope together")?
        .expect_err("the envelope is full");
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    let envelope = legion::cmd::output::CliError::from_status("Failed to run", &status);
    assert!(envelope.retryable);
    for held in held {
        assert_eq!(held?.exit, Some(0));
    }
    let after = cluster
        .run(RunSessionRequest {
            files: vec![script("echo fits\n")],
            ..Default::default()
        })
        .await?;
    assert_eq!(after.stdout, b"fits\n");
    assert_eq!(cluster.sessions(), 0);
    Ok(())
}

/// The edge form: a session ticket runs the task's own command on the node
/// with no manager on the path, and may not replace the command.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_ticket_runs_at_the_edge_test() -> R<()> {
    let cluster = Cluster::new(json!({
        "command": ["sh", "/legion/run/main.sh"],
        "wall": "30s",
        "allowCommand": true,
    }))
    .await?;
    let ticket = cluster.session_ticket()?;
    let reply = cluster
        .edge
        .run_session(Request::new(RunSessionRequest {
            ticket: ticket.clone(),
            files: vec![script("echo edge\n")],
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(reply.stdout, b"edge\n");
    assert_eq!(reply.exit, Some(0));
    assert_eq!(reply.node_id, NODE);

    // Replacing the command needs exec:write, which only a manager checks:
    // refused for a ticket even on a task that allows it.
    let status = cluster
        .edge
        .run_session(Request::new(RunSessionRequest {
            ticket,
            command: vec!["id".into()],
            ..Default::default()
        }))
        .await
        .expect_err("a ticket cannot replace the command");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(status.message().contains("exec:write"), "{status:?}");

    // No ticket, no run.
    let status = cluster
        .edge
        .run_session(Request::new(RunSessionRequest::default()))
        .await
        .expect_err("no ticket");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(cluster.sessions(), 0);
    Ok(())
}

/// A run session whose driver died with the worker (the session log brings
/// it back after a restart) is ended by the maintenance sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_session_without_its_driver_is_swept_test() -> R<()> {
    use legion::worker::task_manager::session_runs::{end_orphaned_runs, RUN_ORIGIN};
    let cluster = Cluster::new(own_command()).await?;
    // A session started as a run would start one, with nothing driving it.
    let spec = cluster
        .task_manager
        .lock()
        .expect("task manager")
        .session_spec(JOB, TASK)
        .context("spec")?;
    let session_id = legion::structs::mint_session_id(TASK, NODE);
    let (payload, provisional) = edge::session_payload(
        &spec,
        JOB,
        &session_id,
        NODE,
        RUN_ORIGIN,
        "",
        legion::util::now_unix_ms(),
    )?;
    let manager = cluster.task_manager.clone();
    tokio::task::spawn_blocking(move || {
        edge::admit_and_launch(&manager, &payload, &provisional, None, None)
    })
    .await??;
    assert!(
        edge::wait_until_running(&cluster.task_manager, &session_id, Duration::from_secs(10))
            .await?
    );
    assert_eq!(cluster.sessions(), 1);
    let manager = cluster.task_manager.clone();
    let ended = tokio::task::spawn_blocking(move || end_orphaned_runs(&manager)).await?;
    assert_eq!(ended, 1);
    assert_eq!(cluster.sessions(), 0);
    Ok(())
}
