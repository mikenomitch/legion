//! Resize end to end (docs/resize.md): `Manager.ResizeAllocation` and
//! `Manager.ResizeInstance` against a real worker over the real mTLS worker
//! plane, on an `exec` service allocation, a keyed instance, and an `exec`
//! session.
//!
//! What is asserted here is the plan's contract: an allocation's resize is
//! an override on its row that the node applies with no restart and no new
//! job version; a grow past the node's free room is refused with the room;
//! a session's resize routes to its node, is admitted against its envelope,
//! and writes nothing to raft; retries with a key are deduplicated; and a
//! dry run changes nothing.
//!
//! The cgroup writes themselves are asserted in
//! `worker::resources::tests` against a directory of files (this suite's
//! worker runs without enforcement, where a resize is an accounting change),
//! the sandbox balloon in `task_plugins::sandbox::tests`, and the `scale`
//! permission in `acl_endpoints`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion::cluster_tls::{
    certified_key, server_config, tls_incoming, ClusterClientVerifier, SwappableIdentity,
    SwappableRoots,
};
use legion::manager::eval::EvalExt;
use legion::manager::server::ManagerDaemonService;
use legion::mesh_ca::{generate_csr, sign_csr, CsrSubject};
use legion::prelude::{json, Context, Value, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::JobConfig;
use legion::structs::manager_server::Manager;
use legion::structs::resize::ResourceSize;
use legion::structs::worker_server::WorkerServer;
use legion::structs::{
    CreateInstanceRequest, GetInstanceRequest, Node, NodeMsg, ResizeAllocationRequest,
    ResizeInstanceRequest, ResizeReply, ResizeResources, Submission,
};
use legion::worker::server::WorkerDaemonService;
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::WorkerTaskManager;
use tonic::{Code, Request, Status};

const JOB: &str = "shop";
const NODE: &str = "w-03";

/// One manager on a raft store and one real worker with 8 GiB and 8000 MHz,
/// running an `exec` service (`web`), a keyed task (`cart`), and the
/// envelope of an `exec` session task (`machine`).
struct Cluster {
    svc: ManagerDaemonService,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    /// The `machine` task's `stateful.envelope` block.
    envelope: Value,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn new() -> R<Self> {
        Self::with_capabilities(json!(["sessions", "resize"])).await
    }

    async fn with_capabilities(capabilities: Value) -> R<Self> {
        Self::with(
            capabilities,
            json!({ "resources": { "cpu": 400, "memory": 256 } }),
        )
        .await
    }

    async fn with(capabilities: Value, envelope: Value) -> R<Self> {
        let dir = tempfile::tempdir()?;
        let mut config = ManagerConfigFull {
            store_path: dir.path().join("store").to_str().context("utf-8")?.into(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-resize".into();
        config.persistence.raft.data_dir =
            dir.path().join("raft").to_str().context("utf-8")?.into();
        let store = tokio::task::block_in_place(|| Store::start_storage(&config))?;
        let svc = ManagerDaemonService::new(store);

        let mut manager = WorkerTaskManager::with_state_store(WorkerStateStore::new(
            dir.path().join("worker").join("worker_state.json"),
        ));
        manager.load_builtin_plugins();
        let task_manager = Arc::new(Mutex::new(manager));
        let launch_pool = Arc::new(legion::worker::launch_pool::LaunchPool::new(
            8,
            Duration::from_secs(1),
        ));
        let service = WorkerDaemonService {
            node_id: NODE.into(),
            task_manager: task_manager.clone(),
            identity: None,
            edge_boot: None,
            direct_exec: None,
            launch_pool,
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
        let served = WorkerServer::new(service);
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
                "resources": { "memory": 8192, "cpu": 8000 },
                "workerAddress": format!("https://{address}"),
            }),
            fingerprint_attrs: json!({
                "memory_total": "8589934592",
                "cpus": {
                    "cpu-1": { "frequency": 2000.0 },
                    "cpu-2": { "frequency": 2000.0 },
                    "cpu-3": { "frequency": 2000.0 },
                    "cpu-4": { "frequency": 2000.0 },
                },
            }),
            dynamic_attrs: json!({ "capabilities": capabilities, "protocolVersion": 3 }),
            health_checks: json!({}),
        }))?;

        let cluster = Cluster {
            svc,
            task_manager,
            envelope,
            _dir: dir,
        };
        cluster.submit(json!({ "cpu": 1000, "memory": 1024 }))?;
        cluster.wait_for_envelope().await?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    /// Submit the job with `web` at these resources, and run the scheduler.
    fn submit(&self, web: Value) -> R<()> {
        let sleep = json!({ "command": ["sleep", "600"] });
        let config: JobConfig = serde_json::from_value(json!({
            "name": JOB,
            "tasks": [
                {
                    "name": "web",
                    "runtime": "exec",
                    "config": sleep,
                    "resources": web,
                },
                {
                    "name": "cart",
                    "runtime": "exec",
                    "config": sleep,
                    "resources": { "cpu": 500, "memory": 512 },
                    "stateful": { "mode": "keyed" },
                },
                {
                    "name": "machine",
                    "runtime": "exec",
                    "config": sleep,
                    "resources": { "cpu": 100, "memory": 64 },
                    "stateful": { "mode": "session", "envelope": self.envelope },
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
            self.run_evals()
        })
    }

    fn run_evals(&self) -> R<()> {
        while let Some(eval) = self.store().claim_eval()? {
            eval.handle(self.store())?;
            self.store().complete_eval(&eval.id, true)?;
        }
        Ok(())
    }

    /// The worker runs the session envelope and the `web` allocation.
    async fn wait_for_envelope(&self) -> R<()> {
        let web = self.allocation_of("web")?;
        for _ in 0..200 {
            let ready = {
                let manager = self.task_manager.lock().expect("task manager");
                manager.session_spec(JOB, "machine").is_some() && manager.has_allocation(web)
            };
            if ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("the worker never ran the envelope and the web allocation")
    }

    /// The id of the one live allocation of `task`.
    fn allocation_of(&self, task: &str) -> R<i64> {
        let config = self.store().get_job_config(JOB)?;
        let tasks = self
            .store()
            .shared_task_configs_for_job_config(&config.id())?;
        let task_id = tasks
            .iter()
            .find(|(_, config)| config.name == task)
            .map(|(id, _)| *id)
            .context("task config")?;
        self.store()
            .allocs_for_node(NODE)?
            .into_iter()
            .find(|allocation| allocation.task_config_id == task_id)
            .map(|allocation| allocation.id)
            .with_context(|| format!("no allocation of {task}"))
    }

    async fn resize_allocation(
        &self,
        allocation_id: i64,
        to: ResizeResources,
        rest: ResizeAllocationRequest,
    ) -> Result<ResizeReply, Status> {
        self.svc
            .resize_allocation(Request::new(ResizeAllocationRequest {
                allocation_id,
                to: Some(to),
                ..rest
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn resize_instance(
        &self,
        instance_id: &str,
        to: ResizeResources,
        rest: ResizeInstanceRequest,
    ) -> Result<ResizeReply, Status> {
        self.svc
            .resize_instance(Request::new(ResizeInstanceRequest {
                instance_id: instance_id.into(),
                to: Some(to),
                ..rest
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    /// What the worker runs `allocation_id` at.
    fn running_size(&self, allocation_id: i64) -> ResourceSize {
        let manager = self.task_manager.lock().expect("task manager");
        ResourceSize::of(
            &manager
                .task_config(allocation_id)
                .expect("the worker runs the allocation")
                .resources,
        )
    }

    /// What the row reserves for `allocation_id`.
    fn reserved_size(&self, allocation_id: i64) -> R<ResourceSize> {
        Ok(ResourceSize::of(
            &self
                .store()
                .allocation_size(allocation_id)?
                .context("row")?
                .effective,
        ))
    }

    fn pid(&self, allocation_id: i64) -> Option<u32> {
        self.task_manager
            .lock()
            .expect("task manager")
            .allocation_pid(allocation_id)
    }

    async fn start_session(&self) -> R<String> {
        Ok(self
            .svc
            .create_instance(Request::new(CreateInstanceRequest {
                job_id: JOB.into(),
                task: Some("machine".into()),
                ..Default::default()
            }))
            .await?
            .into_inner()
            .instance_id)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if let Ok(mut manager) = self.task_manager.lock() {
            for claim in manager.claims_snapshot() {
                manager.kill_claim(&claim.claim_id, "test over");
            }
            for task in manager.take_tasks_for_shutdown() {
                task.run(std::time::Instant::now() + Duration::from_secs(2));
            }
        }
    }
}

fn amounts(memory: Option<f64>, memory_max: Option<f64>, cpu: Option<f64>) -> ResizeResources {
    ResizeResources {
        memory,
        memory_max,
        cpu,
    }
}

fn size(raw: &str) -> ResourceSize {
    ResourceSize::of(&serde_json::from_str::<Value>(raw).expect("a JSON size"))
}

/// A running `exec` allocation is resized in place: the row reserves the
/// new size and records the override, the worker runs at it with the same
/// process, the job keeps its version, an event says what changed, and a
/// resize back to the spec removes the override.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_allocation_is_resized_in_place_with_no_restart_and_no_new_version_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let web = cluster.allocation_of("web")?;
    let config_before = cluster.store().get_job_config(JOB)?.id();
    let pid_before = cluster.pid(web);

    let reply = cluster
        .resize_allocation(
            web,
            amounts(Some(2048.0), Some(4096.0), Some(2000.0)),
            ResizeAllocationRequest {
                reason: "batch week".into(),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(reply.outcome, "applied", "{}", reply.message);
    assert_eq!(reply.node_id, NODE);
    assert_eq!(reply.allocation_id, web);
    assert_eq!(reply.job_id, JOB);
    assert!(!reply.deduplicated);
    assert_eq!(size(&reply.from), size(r#"{"cpu":1000,"memory":1024}"#));
    let wanted = size(r#"{"cpu":2000,"memory":2048,"memoryMax":4096}"#);
    assert_eq!(size(&reply.to), wanted);
    assert_eq!(size(&reply.spec), size(r#"{"cpu":1000,"memory":1024}"#));

    // The row, the worker, and the process.
    assert_eq!(cluster.reserved_size(web)?, wanted);
    assert_eq!(cluster.running_size(web), wanted);
    assert_eq!(cluster.pid(web), pid_before, "resized, not restarted");
    assert_eq!(cluster.store().get_job_config(JOB)?.id(), config_before);
    assert_eq!(cluster.allocation_of("web")?, web, "not replaced");

    // The override: what differs from the spec, who, and why.
    let view = cluster.store().allocation_size(web)?.context("row")?;
    let record = view.resources_override.context("override recorded")?;
    assert_eq!(record["memory"], json!(2048.0));
    assert_eq!(record["memoryMax"], json!(4096.0));
    assert_eq!(record["cpu"], json!(2000.0));
    assert_eq!(record["spec"]["cpu"], json!(1000.0));
    assert_eq!(record["reason"], json!("batch week"));

    // The payload a worker would launch it from afresh carries the override.
    let desired = legion::manager::server::desired_allocations_for_test(cluster.store(), NODE)?;
    let payload = desired
        .iter()
        .find(|payload| payload.allocation_id == web)
        .context("desired payload")?;
    assert_eq!(size(&payload.task_resources), wanted);

    let events = cluster
        .store()
        .list_scheduler_events(&SchedulerEventFilter {
            kind: Some("allocation.resized".into()),
            ..Default::default()
        })?;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].allocation_id, Some(web));
    assert!(
        events[0].message.contains("batch week"),
        "{}",
        events[0].message
    );
    let meta: Value = serde_json::from_str(&events[0].metadata_json)?;
    assert_eq!(meta["to"]["cpu"], json!(2000));
    assert_eq!(meta["from"]["memory"], json!(1024));

    // Back to the spec: a shrink, applied on the node and then released.
    let back = cluster
        .resize_allocation(
            web,
            amounts(Some(1024.0), Some(1024.0), Some(1000.0)),
            ResizeAllocationRequest::default(),
        )
        .await?;
    assert_eq!(back.outcome, "applied", "{}", back.message);
    assert_eq!(cluster.running_size(web).cpu, Some(1000.0));
    assert_eq!(cluster.reserved_size(web)?.memory, Some(1024.0));
    Ok(())
}

/// A grow past the node's free room is refused with the room the node has,
/// changes nothing, and a dry run answers the same without a write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grow_past_the_nodes_free_room_is_refused_with_the_room_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let web = cluster.allocation_of("web")?;
    // The node has 8192 MiB; `web` holds 1024 and the envelope 256.
    let free = 8192.0 - 1024.0 - 256.0;

    for dry_run in [true, false] {
        let reply = cluster
            .resize_allocation(
                web,
                amounts(Some(65536.0), None, None),
                ResizeAllocationRequest {
                    dry_run,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(reply.outcome, "refused", "{}", reply.message);
        assert!(reply.retryable);
        assert_eq!(reply.dry_run, dry_run);
        let room: Value = serde_json::from_str(&reply.free)?;
        assert_eq!(room["memory"], json!(free as i64), "{room}");
        assert!(
            reply.message.contains(&format!("node {NODE} has"))
                && reply.message.contains("the grow needs 63GiB more"),
            "{}",
            reply.message
        );
        assert!(reply.hint.contains("resize to at most"), "{}", reply.hint);
        assert_eq!(cluster.reserved_size(web)?.memory, Some(1024.0));
        assert_eq!(cluster.running_size(web).memory, Some(1024.0));
    }

    // Exactly the room fits, and a dry run of it commits nothing.
    let fits = amounts(Some(1024.0 + free), None, None);
    let dry = cluster
        .resize_allocation(
            web,
            fits,
            ResizeAllocationRequest {
                dry_run: true,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(dry.outcome, "applied");
    assert_eq!(cluster.reserved_size(web)?.memory, Some(1024.0));
    let real = cluster
        .resize_allocation(web, fits, ResizeAllocationRequest::default())
        .await?;
    assert_eq!(real.outcome, "applied", "{}", real.message);
    assert_eq!(cluster.reserved_size(web)?.memory, Some(1024.0 + free));
    Ok(())
}

/// A retry with the same idempotency key is answered from the first reply.
/// Bad requests are refused before anything changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_are_deduplicated_and_bad_requests_are_refused_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let web = cluster.allocation_of("web")?;
    let keyed = || ResizeAllocationRequest {
        idempotency_key: Some("k-1".into()),
        ..Default::default()
    };
    let first = cluster
        .resize_allocation(web, amounts(None, None, Some(1500.0)), keyed())
        .await?;
    assert_eq!(first.outcome, "applied");
    assert!(!first.deduplicated);
    let again = cluster
        .resize_allocation(web, amounts(None, None, Some(1500.0)), keyed())
        .await?;
    assert!(again.deduplicated);
    assert_eq!(again.from, first.from, "the first reply, replayed");
    assert_eq!(
        cluster
            .store()
            .list_scheduler_events(&SchedulerEventFilter {
                kind: Some("allocation.resized".into()),
                ..Default::default()
            })?
            .len(),
        1
    );

    let nothing = cluster
        .resize_allocation(web, amounts(None, None, None), Default::default())
        .await
        .expect_err("nothing to resize");
    assert_eq!(nothing.code(), Code::InvalidArgument);
    let under = cluster
        .resize_allocation(
            web,
            amounts(Some(2048.0), Some(1024.0), None),
            Default::default(),
        )
        .await
        .expect_err("a ceiling under the reservation");
    assert_eq!(under.code(), Code::InvalidArgument);
    assert!(under.message().contains("memoryMax"), "{under:?}");
    let missing = cluster
        .resize_allocation(987_654, amounts(Some(64.0), None, None), Default::default())
        .await
        .expect_err("no such allocation");
    assert_eq!(missing.code(), Code::NotFound);
    assert_eq!(cluster.running_size(web).cpu, Some(1500.0));
    Ok(())
}

/// A keyed instance resizes through its current allocation, reports the
/// override on `GetInstance`, and emits `instance.resized`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_instance_resizes_through_its_allocation_test() -> R<()> {
    let cluster = Cluster::new().await?;
    cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: JOB.into(),
            task: Some("cart".into()),
            instance_id: Some("user-42".into()),
            ..Default::default()
        }))
        .await?;
    tokio::task::block_in_place(|| cluster.run_evals())?;
    let allocation = cluster.allocation_of("cart")?;
    for _ in 0..200 {
        if cluster
            .task_manager
            .lock()
            .expect("task manager")
            .has_allocation(allocation)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let reply = cluster
        .resize_instance(
            "user-42",
            amounts(Some(1024.0), Some(2048.0), None),
            ResizeInstanceRequest {
                reason: "bigger cart".into(),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(reply.outcome, "applied", "{}", reply.message);
    assert_eq!(reply.instance_id, "user-42");
    assert_eq!(reply.allocation_id, allocation);
    assert_eq!(cluster.running_size(allocation).memory_max, Some(2048.0));

    let info = cluster
        .svc
        .get_instance(Request::new(GetInstanceRequest {
            instance_id: "user-42".into(),
        }))
        .await?
        .into_inner()
        .instance
        .context("instance")?;
    assert_eq!(
        size(&info.resources),
        size(r#"{"cpu":500,"memory":1024,"memoryMax":2048}"#)
    );
    let record: Value = serde_json::from_str(&info.resources_override)?;
    assert_eq!(record["reason"], json!("bigger cart"));
    assert_eq!(record["spec"]["memory"], json!(512.0));

    let events = cluster
        .store()
        .list_scheduler_events(&SchedulerEventFilter {
            kind: Some("instance.resized".into()),
            ..Default::default()
        })?;
    assert_eq!(events.len(), 1, "{events:?}");
    Ok(())
}

/// A session's resize routes to its node by the id's tag, is admitted
/// against its envelope, and writes nothing to raft. A grow the envelope
/// cannot hold is refused with the envelope's room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_resizes_inside_its_envelope_with_no_raft_write_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let session = cluster.start_session().await?;
    let raft_before = cluster.store().raft_applied_entries()?;

    // The envelope is 256 MiB / 400 MHz and the session holds 64 / 100.
    let grown = cluster
        .resize_instance(
            &session,
            amounts(Some(128.0), None, Some(200.0)),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(grown.outcome, "applied", "{}", grown.message);
    assert_eq!(grown.node_id, NODE);
    assert_eq!(grown.allocation_id, 0, "a session has no allocation");
    assert_eq!(size(&grown.from), size(r#"{"cpu":100,"memory":64}"#));
    assert_eq!(size(&grown.to), size(r#"{"cpu":200,"memory":128}"#));
    assert_eq!(size(&grown.spec), size(r#"{"cpu":100,"memory":64}"#));

    // The node's own record of the session, and its envelope accounting.
    let status = cluster
        .task_manager
        .lock()
        .expect("task manager")
        .session_status(&session)
        .context("the session runs")?;
    assert_eq!(size(&status.resources), size(r#"{"cpu":200,"memory":128}"#));
    let info = cluster
        .svc
        .get_instance(Request::new(GetInstanceRequest {
            instance_id: session.clone(),
        }))
        .await?
        .into_inner()
        .instance
        .context("session")?;
    assert_eq!(size(&info.resources), size(r#"{"cpu":200,"memory":128}"#));

    // Past the envelope: refused with what the envelope has left.
    let refused = cluster
        .resize_instance(
            &session,
            amounts(Some(512.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(refused.outcome, "refused", "{}", refused.message);
    assert!(refused.retryable);
    let room: Value = serde_json::from_str(&refused.free)?;
    assert_eq!(room["memory"], json!(128), "{room}");
    assert!(
        refused.message.contains("envelope") && refused.message.contains("384 more"),
        "{}",
        refused.message
    );

    // A second session sees the room the first one took.
    let other = cluster.start_session().await?;
    let squeezed = cluster
        .resize_instance(
            &other,
            amounts(Some(129.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(squeezed.outcome, "refused", "{}", squeezed.message);
    // A shrink gives it back.
    let shrunk = cluster
        .resize_instance(
            &session,
            amounts(Some(64.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(shrunk.outcome, "applied");
    let fits = cluster
        .resize_instance(
            &other,
            amounts(Some(128.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(fits.outcome, "applied", "{}", fits.message);

    assert_eq!(
        cluster.store().raft_applied_entries()?,
        raft_before,
        "a session resize writes nothing to raft"
    );

    let unknown = cluster
        .resize_instance(
            "machine-doesnotexist",
            amounts(Some(64.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await
        .expect_err("no such instance");
    assert_eq!(unknown.code(), Code::NotFound);
    Ok(())
}

/// A resubmit that changes nothing keeps a running allocation and its
/// override; a worker that never advertised the capability is never sent a
/// resize.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_without_the_capability_is_never_sent_a_resize_test() -> R<()> {
    let cluster = Cluster::with_capabilities(json!(["sessions"])).await?;
    let web = cluster.allocation_of("web")?;
    let reply = cluster
        .resize_allocation(
            web,
            amounts(None, None, Some(2000.0)),
            ResizeAllocationRequest::default(),
        )
        .await?;
    assert_eq!(reply.outcome, "not_supported", "{}", reply.message);
    assert!(
        reply.message.contains(NODE) && reply.message.contains("cannot resize"),
        "{}",
        reply.message
    );
    assert_eq!(cluster.reserved_size(web)?.cpu, Some(1000.0));
    assert_eq!(cluster.running_size(web).cpu, Some(1000.0));

    let session = cluster.start_session().await?;
    let reply = cluster
        .resize_instance(
            &session,
            amounts(Some(128.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(reply.outcome, "not_supported", "{}", reply.message);
    Ok(())
}

/// A committed resize whose direct call never reached the node is applied
/// from the desired set: the worker converges a running allocation to the
/// size its payload carries, in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_allocation_converges_to_its_committed_size_test() -> R<()> {
    let cluster = Cluster::new().await?;
    let web = cluster.allocation_of("web")?;
    let pid_before = cluster.pid(web);
    // Commit on the row alone, as if the node had been unreachable.
    let committed = tokio::task::block_in_place(|| {
        cluster
            .store()
            .resize_allocation(&legion::storage::store::AllocationResizeRequest {
                allocation_id: web,
                from: size(r#"{"cpu":1000,"memory":1024}"#),
                to: size(r#"{"cpu":3000,"memory":1024}"#),
                reason: "missed".into(),
                by: "test".into(),
                dry_run: false,
            })
    })?;
    assert_eq!(committed.outcome, "applied", "{}", committed.message);
    assert_eq!(cluster.running_size(web).cpu, Some(1000.0));

    // The node's next heartbeats reconcile against the desired set, which
    // carries the committed size once the read model has it.
    for _ in 0..100 {
        let desired = legion::manager::server::desired_allocations_for_test(cluster.store(), NODE)?;
        let pending = cluster
            .task_manager
            .lock()
            .expect("task manager")
            .reconcile_allocations(&desired);
        assert!(
            pending.iter().all(|payload| payload.allocation_id != web),
            "a resized allocation is never relaunched"
        );
        if cluster.running_size(web).cpu == Some(3000.0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(cluster.running_size(web).cpu, Some(3000.0));
    assert_eq!(cluster.pid(web), pid_before);
    Ok(())
}

/// A session grow its elastic envelope has no room for yet is `deferred`:
/// the node asks its manager for the room on its next heartbeat, as it does
/// for a session start, and the retry is applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grow_an_elastic_envelope_can_hold_is_deferred_until_it_grows_test() -> R<()> {
    let cluster = Cluster::with(
        json!(["sessions", "resize"]),
        json!({
            "resources": { "cpu": 400, "memory": 256 },
            "max": { "cpu": 1600, "memory": 1024 },
        }),
    )
    .await?;
    let session = cluster.start_session().await?;
    let to = amounts(Some(512.0), None, None);

    let deferred = cluster
        .resize_instance(&session, to, ResizeInstanceRequest::default())
        .await?;
    assert_eq!(deferred.outcome, "deferred", "{}", deferred.message);
    assert!(deferred.retryable);
    assert!(deferred.message.contains("elastic"), "{}", deferred.message);

    // Past the envelope's ceiling there is nothing to wait for.
    let refused = cluster
        .resize_instance(
            &session,
            amounts(Some(2048.0), None, None),
            ResizeInstanceRequest::default(),
        )
        .await?;
    assert_eq!(refused.outcome, "refused", "{}", refused.message);

    // The node's next heartbeat: it asks for the room the grow waits for,
    // the manager commits it, and the node learns its envelope's new size.
    let now = std::time::Instant::now();
    let requests = cluster
        .task_manager
        .lock()
        .expect("task manager")
        .envelope_resize_requests(now);
    assert_eq!(requests.len(), 1, "{requests:?}");
    let asked: Value = serde_json::from_str(&requests[0].to)?;
    assert!(
        asked["memory"].as_f64().unwrap_or(0.0) >= 512.0,
        "the envelope is asked to hold the grow: {asked}"
    );
    let results = tokio::task::block_in_place(|| {
        legion::manager::sessions::apply_envelope_resizes(cluster.store(), NODE, &requests)
    });
    assert_eq!(results[0].outcome, "applied", "{}", results[0].message);
    cluster
        .task_manager
        .lock()
        .expect("task manager")
        .apply_envelope_resize_results(&results, now);

    let applied = cluster
        .resize_instance(&session, to, ResizeInstanceRequest::default())
        .await?;
    assert_eq!(applied.outcome, "applied", "{}", applied.message);
    Ok(())
}
