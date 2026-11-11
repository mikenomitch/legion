//! A node sheds edge starts while the control plane is slow to ratify, and
//! recovers (docs/edge-boot.md, 'Overload'): the worker's claim window against
//! the manager's issuer and the CLI's error envelope.
//!
//! `Worker.StartClaim` under a slow control plane, end to end on one node:
//! a real ticket verifier, the real claim window and task manager, the
//! `exec` runtime, and a ratifier whose latency the test controls
//! (docs/edge-boot.md, "Overload").

use legion::manager::workload_identity::Issuer;
use legion::prelude::*;
use legion::structs::edge_boot::SpeculativeStartConfig;
use legion::structs::{worker_server::Worker, StartClaimRequest};
use legion::structs::{RatifyClaimReply, RatifyClaimRequest, TaskConfig};
use legion::worker::boot_ticket::BootTicketVerifier;
use legion::worker::boot_ticket::{JwksSource, DEFAULT_CLOCK_SKEW, DEFAULT_JWKS_REFRESH};
use legion::worker::claim_window::ClaimWindow;
use legion::worker::edge;
use legion::worker::server::*;
use legion::worker::task_manager::WorkerTaskManager;
use serde_json::json;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tonic::{Request, Response, Status};

const ISSUER: &str = "https://manager.example.com:4747";
const JOB_CONFIG: &str = "jc-overload";

struct Jwks(Value);

impl JwksSource for Jwks {
    fn fetch(&self) -> R<Value> {
        Ok(self.0.clone())
    }
}

/// Confirms every claim after `delay_ms`, with a committed payload built
/// the way the manager builds one.
struct SlowRatifier {
    task: TaskConfig,
    delay_ms: AtomicU64,
    next_allocation_id: AtomicI64,
}

#[tonic::async_trait]
impl ClaimRatifier for SlowRatifier {
    async fn ratify(&self, request: RatifyClaimRequest) -> R<RatifyClaimReply> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms.load(Ordering::SeqCst))).await;
        let allocation_id = self.next_allocation_id.fetch_add(1, Ordering::SeqCst);
        let payload = legion::structs::task_payload::build_task_allocation_with(
            &self.task,
            &legion::structs::Allocation {
                id: allocation_id,
                status: "pending".into(),
                resources: self.task.resources.clone(),
                node_id: request.node_id.clone(),
                job_config_id: request.job_config_id.clone(),
                task_config_id: 1,
                job_id: "agent-sandboxes".into(),
                instance_id: Some(request.instance_id.clone()),
                network: None,
            },
            None,
            &[],
            Some(legion::structs::ProvisionalAllocation {
                claim_id: request.claim_id.clone(),
                gate: request.gate,
                lease_deadline_unix: 0,
                instance_id: request.instance_id.clone(),
                origin: "edge".into(),
                on_refused: String::new(),
                ticket_jti: String::new(),
                session: None,
            }),
        )?;
        Ok(RatifyClaimReply {
            confirmed: true,
            allocation_id,
            reason: 0,
            detail: String::new(),
            allocation: Some(payload),
        })
    }
}

struct Node {
    service: WorkerDaemonService,
    issuer: Issuer,
    ratifier: Arc<SlowRatifier>,
    window: Arc<ClaimWindow>,
    _dir: tempfile::TempDir,
}

fn task() -> TaskConfig {
    TaskConfig {
        speculative_start: Some(SpeculativeStartConfig {
            mode: Some("serve".into()),
            ratify_timeout: Some("2s".into()),
            on_refused: None,
            unknown_fields: Default::default(),
        }),
        config: json!({ "command": ["sleep", "30"] }),
        resources: json!({ "memory": 64 }),
        ..TaskConfig::new("workspace", "exec")
    }
}

fn node(ceiling: usize) -> R<Node> {
    let dir = tempfile::tempdir()?;
    let issuer = Issuer::new(
        ISSUER,
        "/.well-known/jwks.json",
        Duration::from_secs(3600),
        None,
        SystemTime::now(),
    )?;
    let verifier = BootTicketVerifier::new(
        Box::new(Jwks(issuer.jwks())),
        ISSUER,
        "edge-eu",
        DEFAULT_JWKS_REFRESH,
        DEFAULT_CLOCK_SKEW,
    );
    let mut manager = WorkerTaskManager::with_state_store(
        legion::worker::state::WorkerStateStore::new(dir.path().join("worker_state.json")),
    );
    manager.load_builtin_plugins();
    // A short queue timeout keeps the test fast, and a threshold of half
    // the 2s lease keeps its timings easy to read.
    let window = Arc::new(ClaimWindow::new(
        ceiling,
        0.5,
        std::time::Duration::from_millis(100),
    ));
    manager.configure_claim_window(window.clone());
    let ratifier = Arc::new(SlowRatifier {
        task: task(),
        delay_ms: AtomicU64::new(0),
        next_allocation_id: AtomicI64::new(9001),
    });
    let spec_cache: edge::SpecCache = Arc::new(Mutex::new(
        [(JOB_CONFIG.to_string(), vec![task()])]
            .into_iter()
            .collect(),
    ));
    let service = WorkerDaemonService {
        node_id: "edge-1".into(),
        task_manager: Arc::new(Mutex::new(manager)),
        identity: None,
        edge_boot: Some(Arc::new(EdgeBootRuntime {
            verifier: Arc::new(verifier),
            spec_cache,
            node_id: "edge-1".into(),
            ratifier: ratifier.clone(),
        })),
        direct_exec: None,
        launch_pool: Arc::new(legion::worker::launch_pool::LaunchPool::new(
            8,
            Duration::from_secs(1),
        )),
        node_identity: None,
        netd: None,
    };
    Ok(Node {
        service,
        issuer,
        ratifier,
        window,
        _dir: dir,
    })
}

impl Node {
    /// A single-use ticket for one instance.
    fn ticket(&self, instance: &str) -> R<String> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let claims = json!({
            "legion.io/scope": "boot",
            "legion.io/job": "agent-sandboxes",
            "legion.io/job_config": JOB_CONFIG,
            "legion.io/task": "workspace",
            "legion.io/instance": instance,
            "legion.io/pool": "edge-eu",
            "legion.io/gate": "serve",
            "legion.io/uses": 1,
        });
        Ok(self
            .issuer
            .issue_boot_ticket(
                &format!("tkt_{instance}"),
                "boot:agent-sandboxes/workspace",
                claims.as_object().cloned().unwrap_or_default(),
                now,
                now + 300,
            )?
            .token)
    }

    async fn start(
        &self,
        instance: &str,
        ticket: &str,
    ) -> Result<<WorkerDaemonService as Worker>::StartClaimStream, Status> {
        self.service
            .start_claim(Request::new(StartClaimRequest {
                ticket: ticket.to_string(),
                instance_id: instance.to_string(),
                claim_id: format!("clm_{instance}"),
                gate: 0,
                dry_run: false,
            }))
            .await
            .map(Response::into_inner)
    }

    fn holds_claim(&self, claim_id: &str) -> bool {
        self.service
            .task_manager
            .lock()
            .expect("task manager")
            .claims_snapshot()
            .iter()
            .any(|claim| claim.claim_id == claim_id)
    }

    fn kill_everything(&self) {
        let mut guard = self.service.task_manager.lock().expect("task manager");
        for claim in guard.claims_snapshot() {
            guard.kill_claim(&claim.claim_id, "test over");
        }
    }
}

/// The stage the stream ends on: `ratified` or `refused`.
async fn last_stage(mut stream: <WorkerDaemonService as Worker>::StartClaimStream) -> String {
    use tokio_stream::StreamExt;
    let mut last = String::new();
    while let Some(frame) = stream.next().await {
        last = frame.expect("frame").stage;
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_control_plane_sheds_new_starts_before_boot_and_recovers_test() -> R<()> {
    let node = node(2)?;
    // Ratification now takes 1.5s of a 2s lease: late, but inside it.
    node.ratifier.delay_ms.store(1_500, Ordering::SeqCst);
    let slow = node.start("user-a", &node.ticket("user-a")?).await?;
    let slow = tokio::spawn(last_stage(slow));

    // Past the 1s threshold the in-flight claim counts as late, the
    // window halves to the one slot it holds, and a new start is refused
    // before it is admitted, booted, or spends its ticket.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let ticket_b = node.ticket("user-b")?;
    let refused = match node.start("user-b", &ticket_b).await {
        Ok(_) => panic!("a start past the threshold must be shed"),
        Err(status) => status,
    };
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        refused
            .metadata()
            .get(legion::structs::worker_protocol::ERROR_CODE_METADATA)
            .and_then(|value| value.to_str().ok()),
        Some(legion::structs::worker_protocol::OVERLOADED)
    );
    assert!(
        refused.message().contains("overloaded"),
        "{}",
        refused.message()
    );
    let envelope = legion::cmd::output::CliError::from_status("Failed to start sandbox", &refused);
    assert_eq!(envelope.code, "OVERLOADED");
    assert!(envelope.retryable, "an overload refusal is retryable");
    assert!(!node.holds_claim("clm_user-b"), "nothing was admitted");
    assert_eq!(node.window.snapshot().window, 1);

    // The in-flight claim was not killed early: it is still held, and it
    // ratifies when its verdict lands.
    assert!(node.holds_claim("clm_user-a"));
    assert_eq!(slow.await?, "ratified");

    // Verdicts are fast again. The refused ticket was never spent, so
    // the router's retry of the same start is admitted, and timely
    // verdicts grow the window back.
    node.ratifier.delay_ms.store(0, Ordering::SeqCst);
    let retried = node.start("user-b", &ticket_b).await?;
    assert_eq!(last_stage(retried).await, "ratified");
    assert_eq!(node.window.snapshot().window, 2, "window recovered");
    let next = node.start("user-c", &node.ticket("user-c")?).await?;
    assert_eq!(last_stage(next).await, "ratified");
    assert_eq!(
        node.window.snapshot().held,
        0,
        "ratified claims free their slots"
    );

    node.kill_everything();
    Ok(())
}
