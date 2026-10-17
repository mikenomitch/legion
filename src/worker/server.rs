use crate::prelude::*;
use crate::structs::edge_boot::{Gate, RefusalReason};
use crate::structs::{
    worker_server::Worker, ExecClientFrame, GetTaskLogsReply, GetTaskLogsRequest, PostWorkReply,
    PostWorkRequest, RestartWorkerReply, RestartWorkerRequest, StartClaimReply, StartClaimRequest,
    TaskAllocation, TaskLogStream,
};
use crate::worker::boot_ticket::BootTicketVerifier;
use crate::worker::claim_window::Feedback;
use crate::worker::edge;
use crate::worker::exec::{handle_exec_task, Admission, ExecTaskStream};
use crate::worker::identity::WorkerIdentityManager;
use crate::worker::task_manager::{launch_task_allocation, WorkerTaskManager};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// Everything the worker needs to serve `StartClaim`. `None` on a node that
/// did not opt in (`edgeBoot.acceptClaims: false`, the default), in which case
/// the endpoint refuses with FailedPrecondition.
pub struct EdgeBootRuntime {
    /// Offline ticket verification.
    pub verifier: Arc<BootTicketVerifier>,
    /// Task configs this node may start work for, keyed by job config id.
    /// Populated from every allocation the node is dispatched (see
    /// [`edge::remember_specs`]); nothing is fetched on the fast path.
    pub spec_cache: edge::SpecCache,
    /// This node's id, for the ratification request.
    pub node_id: String,
    /// Ratifies a claim against the manager. Behind a trait object so the
    /// stream logic is testable without a live manager.
    pub ratifier: Arc<dyn ClaimRatifier>,
}

/// The manager call a node makes to ratify a claim.
#[tonic::async_trait]
pub trait ClaimRatifier: Send + Sync {
    async fn ratify(
        &self,
        request: crate::structs::RatifyClaimRequest,
    ) -> R<crate::structs::RatifyClaimReply>;
}

/// The status a refused provisional start answers with. A session refused
/// for room is RESOURCE_EXHAUSTED, which the manager's session router reads as
/// "try the next node"; everything else is FAILED_PRECONDITION, as before.
fn admission_status(err: &anyhow::Error) -> Status {
    use crate::worker::task_manager::session_sleep::SessionSleepError;
    use crate::worker::task_manager::SessionRefusal;
    if let Some(sleep) = err.downcast_ref::<SessionSleepError>() {
        if let Some(status) = store_status(sleep) {
            return status;
        }
    }
    match err.downcast_ref::<SessionRefusal>() {
        Some(SessionRefusal::EnvelopeFull { .. }) => Status::resource_exhausted(format!("{err:#}")),
        _ => Status::failed_precondition(format!("{err:#}")),
    }
}

/// The status an edge start refused by the claim window answers with:
/// `RESOURCE_EXHAUSTED` (retryable) tagged `OVERLOADED`, with the reason and
/// a `Hint:` line, so the CLI and MCP envelopes carry `code: OVERLOADED` and
/// `retryable: true`.
fn overloaded_status(refused: &crate::worker::claim_window::Overloaded) -> Status {
    let mut status = Status::resource_exhausted(refused.to_string());
    status.metadata_mut().insert(
        crate::manager::worker_plane::ERROR_CODE_METADATA,
        tonic::metadata::MetadataValue::from_static(crate::manager::worker_plane::OVERLOADED),
    );
    status
}

/// The status of a `store` session refusal (session durability plan §6.7):
/// a lost claim or a superseded copy is ABORTED, a session another node
/// created first is ALREADY_EXISTS, an unreachable content store is
/// UNAVAILABLE, and a move that found the session running elsewhere is
/// FAILED_PRECONDITION `running_elsewhere`. Each message ends in `[winner
/// node=<id> generation=<g>]` when it names a winner.
fn store_status(
    sleep: &crate::worker::task_manager::session_sleep::SessionSleepError,
) -> Option<Status> {
    use crate::worker::task_manager::session_sleep::SessionSleepError;
    Some(match sleep {
        SessionSleepError::Superseded(why) => Status::aborted(why.clone()),
        SessionSleepError::AlreadyExists(why) => Status::already_exists(why.clone()),
        SessionSleepError::StoreUnavailable(why) => Status::unavailable(why.clone()),
        SessionSleepError::RunningElsewhere { .. } => {
            Status::failed_precondition(sleep.to_string())
        }
        _ => return None,
    })
}

/// The status a task that did not start answers `PostWork` with.
///
/// The message carries the whole error chain (`{err:#}`), not just the
/// outermost context: "failed to place task web within this node's resource
/// limits" says nothing an operator can act on, while the cgroup file and
/// errno underneath it do. The code says whose move it is:
/// RESOURCE_EXHAUSTED when the node has no room right now (the manager can
/// place elsewhere), the code of a gRPC call that failed underneath (a
/// manager round trip), and FAILED_PRECONDITION otherwise, because a start
/// failure is this node or this task config being unable to run the task,
/// not a bug in the worker. INTERNAL stays for panics and poisoned locks.
fn task_start_status(context: &str, err: &anyhow::Error) -> Status {
    let message = format!("{context}: {err:#}");
    let code = if err
        .chain()
        .any(|cause| cause.is::<crate::worker::resources::PlacementExhausted>())
    {
        tonic::Code::ResourceExhausted
    } else if let Some(status) = err.chain().find_map(|cause| cause.downcast_ref::<Status>()) {
        status.code()
    } else {
        tonic::Code::FailedPrecondition
    };
    Status::new(code, message)
}

/// How long a move waits for the node a session's manifest names before it
/// calls that node unreachable (session durability plan §6.7, "Moving is
/// cautious for running sessions").
pub const MOVE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// What a probe of the node a manifest names found.
#[derive(Debug)]
enum PeerProbe {
    /// The session runs there.
    Running,
    /// The node answered and does not run it (suspended or gone).
    NotRunning,
    /// No answer within [`MOVE_PROBE_TIMEOUT`].
    Unreachable(String),
}

pub struct WorkerDaemonService {
    /// This worker's node id, checked against requests that name a node.
    pub node_id: String,
    pub task_manager: Arc<Mutex<WorkerTaskManager>>,
    /// Workload-identity delivery/refresh; `None` when this worker runs
    /// without a manager identity client (tests).
    pub identity: Option<Arc<WorkerIdentityManager>>,
    /// Edge boot support; `None` unless `edgeBoot.acceptClaims` is true.
    pub edge_boot: Option<Arc<EdgeBootRuntime>>,
    /// Direct exec: the exec ticket verifier, `None` unless the worker
    /// config sets `exec = { direct = true }`.
    pub direct_exec: Option<Arc<crate::worker::direct_exec::ExecTicketVerifier>>,
    /// Bounds the provisional starts (sessions and claims) this node runs
    /// through its runtime at once (`edgeBoot.launchConcurrency`).
    pub launch_pool: Arc<crate::worker::launch_pool::LaunchPool>,
    /// This node's identity, whose cluster CA bundle verifies another
    /// worker an edge resume probes (`StartSession.resume_id`). `None` in
    /// tests: such a node cannot probe, so it moves only with `force_move`.
    pub node_identity: Option<Arc<crate::worker::plane::NodeIdentity>>,
    /// The node's egress proxies, so a gated sandbox delivered by push or
    /// the work stream has its proxy socket bound before its launch rather
    /// than on the next heartbeat (`daemon::bind_launch_datapath`). A
    /// restored guest resumes only once that socket exists. `None` in tests
    /// that run no datapath.
    pub egress: Option<Arc<crate::worker::netd::proxy::EgressProxies>>,
}

impl WorkerDaemonService {
    /// Take a launch slot, or refuse the start as retryable on another node.
    async fn launch_slot(&self) -> Result<crate::worker::launch_pool::LaunchSlot, Status> {
        self.launch_pool
            .acquire()
            .await
            .map_err(|full| Status::resource_exhausted(full.to_string()))
    }

    /// Start one allocation the manager handed this node, by whichever route
    /// it arrived: a `PostWork` push (after the plane's manager check) or a
    /// `StreamWork` delta (`daemon::launch_streamed_work`). The stream was
    /// authenticated when the worker opened it to its manager, so its deltas
    /// come here directly. Routing them through `post_work` as an in-process
    /// call fails the manager-certificate check, because a request built in
    /// process carries no peer certificate at all.
    #[instrument(skip_all, fields(allocation_id = allocation.allocation_id))]
    pub async fn accept_work(
        &self,
        mut allocation: TaskAllocation,
    ) -> Result<PostWorkReply, Status> {
        // Every payload this node is handed teaches it a job config it may
        // later be asked to start from a boot ticket (docs/edge-boot.md). A
        // session start teaches nothing: its task's envelope already carried
        // the spec, and a boot ticket never names a session task, so parsing
        // the payload again per start was pure cost.
        let session_start = allocation
            .provisional
            .as_ref()
            .is_some_and(|provisional| provisional.session.is_some());
        if let Some(edge_boot) = self.edge_boot.as_ref().filter(|_| !session_start) {
            edge::remember_specs(&edge_boot.spec_cache, std::slice::from_ref(&allocation));
        }

        // Provisional dispatches and their ratifications ride the same RPC as
        // an ordinary dispatch, distinguished by the `provisional` block and
        // by whether the payload yet has an allocation id.
        if let Some(provisional) = allocation.provisional.clone() {
            if allocation.allocation_id == 0 {
                let slot = self.launch_slot().await?;
                // An idempotent `store` start creates its manifest first, so
                // two nodes that derived the same id cannot both run it.
                let task_manager = self.task_manager.clone();
                let payload = allocation.clone();
                tokio::task::spawn_blocking(move || {
                    crate::worker::task_manager::session_store::claim_new_store_session(
                        &task_manager,
                        &payload,
                    )
                })
                .await
                .map_err(|err| Status::internal(format!("session create panicked: {err}")))?
                .map_err(|err| admission_status(&err))?;
                // Not counted in the claim window: the scheduler placed this
                // with the node's capacity in view, and a session is never
                // ratified at all.
                let admitted = edge::admit_and_launch(
                    &self.task_manager,
                    &allocation,
                    &provisional,
                    Some(slot),
                    None,
                )
                .map_err(|err| admission_status(&err))?;
                return Ok(PostWorkReply {
                    message: format!("Provisional start {} admitted", admitted.claim_id),
                });
            }
            // A ratification: the committed payload, carrying the claim id it
            // settles.
            edge::apply_verdict(
                &self.task_manager,
                &provisional.claim_id,
                Some(&allocation),
                RefusalReason::Unspecified,
                "",
                Feedback::Verdict,
            );
            return Ok(PostWorkReply {
                message: format!("Claim {} ratified", provisional.claim_id),
            });
        }

        // Fetch + deliver identity tokens before the task starts (env tokens
        // must be in the env map at exec time). A task must not start without
        // an identity it declared.
        let delivery_base = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .file_delivery_base(
                allocation.allocation_id,
                &allocation.task_name,
                &allocation.task_runtime,
            )
            .map(
                |(in_task_root, host_task_dir)| crate::worker::identity::FileDeliveryBase {
                    in_task_root,
                    host_task_dir,
                },
            );
        let prepared = match self.identity.as_ref() {
            Some(identity) => match identity
                .prepare_task_allocation(&mut allocation, delivery_base)
                .await
            {
                Ok(prepared) => prepared,
                Err(err) => {
                    let status = task_start_status("Failed to deliver workload identities", &err);
                    self.task_manager
                        .lock()
                        .map_err(|_| Status::internal("Task manager lock poisoned"))?
                        .record_failure(allocation.allocation_id, status.message());
                    return Err(status);
                }
            },
            None => Vec::new(),
        };

        let allocation_id = allocation.allocation_id;

        // A gated sandbox's egress proxy, bound before the launch: a restore
        // resumes its guest only once the socket exists, and no heartbeat
        // has bound this delivery's datapath yet.
        if let Some(egress) = self.egress.as_ref() {
            let alloc_root = self
                .task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?
                .alloc_root()
                .to_path_buf();
            crate::worker::daemon::bind_launch_datapath(egress, &alloc_root, &allocation).await;
        }

        // Synchronous failures (bad config, unknown runtime) surface here;
        // artifact-bearing tasks stage on a background thread and report
        // staging failures through the heartbeat failure path.
        let task_manager = self.task_manager.clone();
        let launched =
            tokio::task::spawn_blocking(move || launch_task_allocation(&task_manager, &allocation))
                .await
                .map_err(|err| Status::internal(format!("launch task panicked: {err}")))?;
        if let Err(err) = launched {
            return Err(task_start_status("Failed to start task", &err));
        }

        // Refresh loops are keyed by allocation id; identity_manager.retain
        // (run each heartbeat) prunes them if the allocation goes away.
        if let Some(identity) = self.identity.as_ref() {
            identity.spawn_refreshers(allocation_id, prepared, self.task_manager.clone());
        }

        Ok(PostWorkReply {
            message: "Task accepted".into(),
        })
    }
}

#[tonic::async_trait]
impl Worker for WorkerDaemonService {
    #[instrument(err, skip_all)]
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        // Caller class: manager only.
        let manager = crate::worker::plane::require_manager(&request, "PostWork")?;
        debug!(manager = %manager, "PostWork from manager");
        let allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("Missing allocation"))?;
        self.accept_work(allocation).await.map(Response::new)
    }

    #[instrument(err, skip_all)]
    async fn get_task_logs(
        &self,
        request: Request<GetTaskLogsRequest>,
    ) -> Result<Response<GetTaskLogsReply>, Status> {
        // Caller class: manager only.
        crate::worker::plane::require_manager(&request, "GetTaskLogs")?;
        let req = request.into_inner();
        let streams = {
            let manager = self
                .task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?;
            manager
                .read_allocation_logs(
                    req.allocation_id,
                    &req.stream,
                    req.offset,
                    req.max_bytes,
                    req.tail,
                )
                .map_err(|err| Status::internal(format!("Failed to read task logs: {err}")))?
        };

        let streams = streams
            .into_iter()
            .map(|stream| TaskLogStream {
                stream: stream.stream,
                content: stream.content,
                size_bytes: stream.size_bytes,
                truncated: stream.truncated,
                start_offset: stream.start_offset,
                next_offset: stream.next_offset,
            })
            .collect();

        Ok(Response::new(GetTaskLogsReply { streams }))
    }

    type ExecTaskStream = ExecTaskStream;

    #[instrument(skip_all)]
    async fn exec_task(
        &self,
        request: Request<Streaming<ExecClientFrame>>,
    ) -> Result<Response<Self::ExecTaskStream>, Status> {
        // Caller classes: a manager, or (where the worker opts in with
        // `exec.direct`) an exec ticket in the first frame. A certificate
        // that is not a manager's is refused, never downgraded to the ticket
        // path.
        let admission = match crate::worker::plane::caller_of(&request, "ExecTask")? {
            crate::worker::plane::Caller::Manager(_) => Admission::Manager,
            crate::worker::plane::Caller::Ticket => Admission::Ticket(self.direct_exec.clone()),
        };
        let stream =
            handle_exec_task(self.task_manager.clone(), request.into_inner(), admission).await?;
        Ok(Response::new(stream))
    }

    type StartClaimStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<StartClaimReply, Status>> + Send + 'static>>;

    /// The edge entry point: start work on this node from a boot ticket,
    /// without any call to a manager on the critical path
    /// (docs/edge-boot.md).
    #[instrument(skip_all, fields(instance_id, claim_id))]
    async fn start_claim(
        &self,
        request: Request<StartClaimRequest>,
    ) -> Result<Response<Self::StartClaimStream>, Status> {
        // Caller class: ticket. The boot ticket below is the only credential;
        // a client certificate, if any, is ignored.
        let Some(edge_boot) = self.edge_boot.clone() else {
            return Err(Status::failed_precondition(
                "this node does not accept edge starts \
                 (set edgeBoot.acceptClaims: true in its worker config)",
            ));
        };
        let req = request.into_inner();
        tracing::Span::current().record("instance_id", req.instance_id.as_str());

        if req.instance_id.trim().is_empty() {
            return Err(Status::invalid_argument("instance_id is required"));
        }

        // Every check below is local. A node cut off from every manager can
        // still get all the way to `booted`; only the ratification needs the
        // control plane, and it runs in parallel with the boot.
        // Off the async runtime: a verification can refresh the JWKS, which
        // blocks.
        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        let claims = {
            let verifier = edge_boot.verifier.clone();
            let ticket = req.ticket.clone();
            let instance_id = req.instance_id.clone();
            tokio::task::spawn_blocking(move || verifier.verify(&ticket, &instance_id, now_unix))
                .await
                .map_err(|e| Status::internal(format!("ticket verification panicked: {e}")))?
                .map_err(|err| {
                    Status::permission_denied(format!("boot ticket rejected: {err:#}"))
                })?
        };

        let task = {
            let cache = edge_boot.spec_cache.lock().expect("spec cache");
            cache
                .get(&claims.job_config_id)
                .and_then(|tasks| tasks.iter().find(|task| task.name == claims.task))
                .cloned()
        };
        let Some(task) = task else {
            edge_boot.verifier.unspend(&claims.jti);
            return Err(Status::failed_precondition(format!(
                "this node has no cached spec for job config {}; it caches a config \
                 once it has been dispatched an allocation of it, and does not fetch \
                 specs on the start path",
                claims.job_config_id
            )));
        };

        // The lowest of the three ceilings wins: what the caller asked for,
        // what the ticket allows, and what the task itself declares. Neither a
        // caller nor a ticket can raise the task's own limit.
        let task_gate = crate::manager::dispatcher::resolved_speculative_start(&task)
            .map_err(|err| Status::failed_precondition(format!("{err:#}")))?;
        let requested = match Gate::from_wire(req.gate) {
            Gate::Off => claims.gate,
            explicit => explicit,
        };
        let gate = requested.min(claims.gate).min(task_gate.mode);
        if gate == Gate::Off {
            edge_boot.verifier.unspend(&claims.jti);
            return Err(Status::failed_precondition(format!(
                "task {} does not permit speculative starts (speculativeStart.mode is `off`, \
                 or was downgraded by an invariant)",
                task.name
            )));
        }

        let claim_id = if req.claim_id.trim().is_empty() {
            crate::structs::edge_boot::new_claim_id()
        } else {
            req.claim_id.clone()
        };
        tracing::Span::current().record("claim_id", claim_id.as_str());

        let window = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .claim_window();

        if req.dry_run {
            // Verify and report, hold nothing, boot nothing. The self-correct
            // path an agent or a router wants before it mutates a cluster.
            edge_boot.verifier.unspend(&claims.jti);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let room = window.snapshot();
            let admission = if room.held < room.window {
                "node would admit this start".to_string()
            } else {
                format!(
                    "node's claim window is full ({}/{} unratified edge claims), so a start \
                     would wait up to edgeBoot.queueTimeout for room and otherwise be refused \
                     OVERLOADED",
                    room.held, room.window
                )
            };
            let reply = StartClaimReply {
                claim_id,
                stage: "admitted".into(),
                allocation_id: 0,
                reason: 0,
                detail: format!("dry run: ticket valid, gate would be `{gate}`, {admission}"),
                gate: gate.to_wire(),
            };
            let _ = tx.send(Ok(reply)).await;
            return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
        }

        // The claim's own identity for the workload's env: the cached task
        // config carries whichever instance it was last dispatched for.
        let identity = crate::structs::Instance {
            instance_id: req.instance_id.clone(),
            job_id: claims.job.clone(),
            group: task.group.clone(),
            task_name: task.name.clone(),
            mode: crate::structs::InstanceMode::Keyed,
            state: crate::structs::InstanceState::Pending,
            slot_index: None,
            current_alloc_id: None,
            storage_handle: None,
            created_at: String::new(),
            updated_at: String::new(),
            restore_from: None,
            identity_epoch: 0,
            named: true,
            promoted_from: None,
        };
        let payload = crate::manager::dispatcher::build_task_allocation_with(
            &task,
            &crate::structs::Allocation {
                id: 0,
                status: "provisional".into(),
                resources: task.resources.clone(),
                node_id: edge_boot.node_id.clone(),
                job_config_id: claims.job_config_id.clone(),
                task_config_id: 0,
                job_id: claims.job.clone(),
                instance_id: Some(req.instance_id.clone()),
                network: None,
            },
            Some(&identity),
            &[],
            Some(crate::structs::ProvisionalAllocation {
                claim_id: claim_id.clone(),
                gate: gate.to_wire(),
                lease_deadline_unix: now_unix + task_gate.ratify_timeout.as_secs() as i64,
                instance_id: req.instance_id.clone(),
                origin: "edge".into(),
                on_refused: task_gate.on_refused.as_str().to_string(),
                ticket_jti: claims.jti.clone(),
                session: None,
            }),
        )
        .map_err(|err| Status::internal(format!("failed to build payload: {err:#}")))?;

        let provisional = payload
            .provisional
            .clone()
            .expect("provisional block was just set");
        // Room in the claim window first, before a launch slot: a start
        // that ratification cannot keep up with is refused here, before it
        // holds anything, boots, or spends its ticket. Waiting for it must
        // not hold a launch slot that sessions also need.
        let reservation = window.acquire().await.map_err(|refused| {
            edge_boot.verifier.unspend(&claims.jti);
            debug!(
                held = refused.held,
                window = refused.window,
                ceiling = refused.ceiling,
                "Edge start refused: claim window full"
            );
            overloaded_status(&refused)
        })?;
        let slot = self.launch_slot().await.inspect_err(|_| {
            edge_boot.verifier.unspend(&claims.jti);
        })?;
        let admitted = edge::admit_and_launch(
            &self.task_manager,
            &payload,
            &provisional,
            Some(slot),
            Some((reservation, task_gate.ratify_timeout)),
        )
        .map_err(|err| {
            // The start never happened, so the ticket was not really
            // redeemed; let an operator's retry work.
            edge_boot.verifier.unspend(&claims.jti);
            Status::resource_exhausted(format!("{err:#}"))
        })?;
        edge_boot.verifier.spend(&claims, now_unix);

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let task_manager = self.task_manager.clone();
        let job_config_id = claims.job_config_id.clone();
        let task_name = claims.task.clone();
        let instance_id = req.instance_id.clone();
        let ticket = req.ticket.clone();
        tokio::spawn(async move {
            drive_claim(
                edge_boot,
                task_manager,
                tx,
                admitted.claim_id,
                gate,
                job_config_id,
                task_name,
                instance_id,
                ticket,
                task_gate.ratify_timeout,
            )
            .await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    #[instrument(err, skip_all)]
    async fn restart_worker(
        &self,
        request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        // Caller class: manager only.
        crate::worker::plane::require_manager(&request, "RestartWorker")?;
        let node_id = request.into_inner().node_id;
        // A manager holding a stale address must not restart whatever node
        // now answers there.
        if !node_id.is_empty() && node_id != self.node_id {
            return Err(Status::failed_precondition(format!(
                "this worker is node {:?}, not {node_id:?}",
                self.node_id
            )));
        }
        info!(node_id = %node_id, "Received worker restart request");

        let reply = RestartWorkerReply {
            message: "Worker restarting".into(),
        };

        tokio::spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        });

        Ok(Response::new(reply))
    }

    #[instrument(err, skip_all)]
    async fn probe(
        &self,
        request: Request<crate::structs::ProbeRequest>,
    ) -> Result<Response<crate::structs::ProbeReply>, Status> {
        // Caller class: manager only.
        crate::worker::plane::require_manager(&request, "Probe")?;
        Ok(Response::new(crate::structs::ProbeReply {
            node_id: self.node_id.clone(),
            protocol_version: u64::from(crate::constants::PROTOCOL_VERSION),
        }))
    }

    /// End sessions on this node (docs/session-mode.md). Manager-only, like
    /// `PostWork`, which starts them.
    #[instrument(err, skip_all, fields(sessions))]
    async fn stop_sessions(
        &self,
        request: Request<crate::structs::StopSessionsRequest>,
    ) -> Result<Response<crate::structs::StopSessionsReply>, Status> {
        // Caller class: manager only.
        crate::worker::plane::require_manager(&request, "StopSessions")?;
        let request = request.into_inner();
        tracing::Span::current().record("sessions", request.session_ids.len());
        let reason = if request.reason.is_empty() {
            "stop".to_string()
        } else {
            request.reason
        };
        let stopped = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .stop_sessions(&request.session_ids, &format!("requested ({reason})"));
        Ok(Response::new(crate::structs::StopSessionsReply { stopped }))
    }

    /// Suspend sessions on this node (session durability plan §6.6).
    /// Manager-only, like `StopSessions`.
    #[instrument(err, skip_all, fields(sessions))]
    async fn suspend_sessions(
        &self,
        request: Request<crate::structs::SuspendSessionsRequest>,
    ) -> Result<Response<crate::structs::SuspendSessionsReply>, Status> {
        // Caller class: manager only.
        crate::worker::plane::require_manager(&request, "SuspendSessions")?;
        let request = request.into_inner();
        tracing::Span::current().record("sessions", request.session_ids.len());
        let reason = if request.reason.is_empty() {
            "manual".to_string()
        } else {
            request.reason
        };
        let mut reply = crate::structs::SuspendSessionsReply::default();
        for session_id in request.session_ids {
            let task_manager = self.task_manager.clone();
            let id = session_id.clone();
            let trigger = reason.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                crate::worker::task_manager::session_sleep::suspend_session(
                    &task_manager,
                    &id,
                    &trigger,
                )
            })
            .await
            .map_err(|err| Status::internal(format!("session suspend panicked: {err}")))?;
            match outcome {
                Ok(outcome) => {
                    use crate::worker::task_manager::session_sleep::SuspendOutcome;
                    let (checkpoint, already) = match outcome {
                        SuspendOutcome::Suspended(checkpoint) => (checkpoint, false),
                        SuspendOutcome::AlreadySuspended(checkpoint) => (checkpoint, true),
                    };
                    let (durability, published_url) = {
                        let guard = self
                            .task_manager
                            .lock()
                            .map_err(|_| Status::internal("Task manager lock poisoned"))?;
                        let store = guard.idle_row(&session_id).is_some_and(|row| row.store);
                        (
                            if store { "store" } else { "local" }.to_string(),
                            guard.published_url(&session_id),
                        )
                    };
                    reply.suspended.push(crate::structs::SuspendedSession {
                        session_id,
                        snapshot_id: checkpoint.snapshot_id,
                        kind: checkpoint.kind,
                        bytes: checkpoint.total_bytes,
                        already_suspended: already,
                        durability,
                        published_url,
                    });
                }
                Err(err) => {
                    let status = self.session_sleep_status(&session_id, &err);
                    reply.failed.push(crate::structs::SessionSleepFailure {
                        session_id,
                        code: format!("{:?}", status.code()),
                        message: status.message().to_string(),
                    });
                }
            }
        }
        Ok(Response::new(reply))
    }

    /// Resume a suspended session on this node (session durability plan
    /// §6.6). Manager-only.
    #[instrument(err, skip_all, fields(session_id))]
    async fn resume_session(
        &self,
        request: Request<crate::structs::ResumeSessionRequest>,
    ) -> Result<Response<crate::structs::ResumeSessionReply>, Status> {
        // Caller class: manager only.
        crate::worker::plane::require_manager(&request, "ResumeSession")?;
        let request = request.into_inner();
        tracing::Span::current().record("session_id", request.session_id.as_str());
        let slot = self.launch_slot().await?;
        let task_manager = self.task_manager.clone();
        let session_id = request.session_id.clone();
        let cold = request.cold;
        if request.job_id.is_empty() {
            // A session this node holds, by its id alone.
            let outcome = tokio::task::spawn_blocking(move || {
                let _slot = slot;
                crate::worker::task_manager::session_sleep::resume_session(
                    &task_manager,
                    &session_id,
                    cold,
                )
            })
            .await
            .map_err(|err| Status::internal(format!("session resume panicked: {err}")))?
            .map_err(|err| self.session_sleep_status(&request.session_id, &err))?;
            use crate::worker::task_manager::session_sleep::ResumeOutcome;
            let (resumed_from, already_running) = match outcome {
                ResumeOutcome::Resumed { from } => (from.to_string(), false),
                ResumeOutcome::AlreadyRunning => (String::new(), true),
            };
            let session = self
                .task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?
                .session_status(&request.session_id);
            return Ok(Response::new(crate::structs::ResumeSessionReply {
                generation: session.as_ref().map(|s| s.generation).unwrap_or_default(),
                session,
                node_id: self.node_id.clone(),
                resumed_from,
                already_running,
                moved_from: String::new(),
            }));
        }
        // With its job and task: a `store` session resumes here from its
        // manifest even when another node ran it (session durability plan
        // §6.6, "Resume"). The manager probed the old node, so a session
        // the manifest says runs elsewhere moves only with `force_move`.
        let move_request = crate::worker::task_manager::session_store::MoveRequest {
            session_id: session_id.clone(),
            job_id: request.job_id.clone(),
            task_name: request.task_name.clone(),
            force_move: request.force_move,
            cold,
            origin: "manager".into(),
            ticket_jti: String::new(),
        };
        let outcome = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            crate::worker::task_manager::session_store::resume_moved(
                &task_manager,
                &move_request,
                None,
            )
        })
        .await
        .map_err(|err| Status::internal(format!("session resume panicked: {err}")))?
        .map_err(|err| self.session_sleep_status(&request.session_id, &err))?;
        let session = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .session_status(&request.session_id);
        Ok(Response::new(crate::structs::ResumeSessionReply {
            session,
            node_id: self.node_id.clone(),
            resumed_from: outcome.from.to_string(),
            already_running: outcome.from.is_empty(),
            generation: outcome.generation,
            moved_from: outcome.moved_from,
        }))
    }

    /// One session on this node, for tag routing (session durability plan
    /// §6.3). The node's answer is authoritative for itself.
    #[instrument(err, skip_all, fields(session_id))]
    async fn get_session(
        &self,
        request: Request<crate::structs::GetSessionRequest>,
    ) -> Result<Response<crate::structs::GetSessionReply>, Status> {
        // Caller classes: manager, ticket (a session ticket).
        let caller = crate::worker::plane::caller_of(&request, "GetSession")?;
        let request = request.into_inner();
        tracing::Span::current().record("session_id", request.session_id.as_str());
        let ticket = match caller {
            crate::worker::plane::Caller::Manager(_) => None,
            crate::worker::plane::Caller::Ticket => {
                Some(self.verify_session_ticket(&request.ticket, "GetSession")?)
            }
        };
        let session = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .session_status(&request.session_id);
        let Some(session) = session else {
            return Err(self.session_not_here(&request.session_id));
        };
        if let Some(ticket) = ticket {
            ticket.allows(&session.job_id, &session.task_name)?;
        }
        Ok(Response::new(crate::structs::GetSessionReply {
            session: Some(session),
            node_id: self.node_id.clone(),
        }))
    }

    /// Start a session from a session ticket (session durability plan
    /// §6.4): no manager on the path, nothing written to raft.
    #[instrument(err, skip_all, fields(job, task, session_id))]
    async fn start_session(
        &self,
        request: Request<crate::structs::StartSessionRequest>,
    ) -> Result<Response<crate::structs::StartSessionReply>, Status> {
        // Caller class: ticket only.
        let caller = crate::worker::plane::caller_of(&request, "StartSession")?;
        if let crate::worker::plane::Caller::Manager(_) = caller {
            return Err(Status::unauthenticated(
                "StartSession is the edge entry point and admits a session ticket only; \
                 managers start sessions with PostWork",
            ));
        }
        let request = request.into_inner();
        let ticket = self.verify_session_ticket(&request.ticket, "StartSession")?;
        let claims = ticket.0.clone();
        let job = if request.job_id.is_empty() {
            claims.job.clone()
        } else {
            request.job_id.clone()
        };
        let task = if request.task_name.is_empty() {
            claims.task.clone()
        } else {
            request.task_name.clone()
        };
        ticket.allows(&job, &task)?;
        let span = tracing::Span::current();
        span.record("job", job.as_str());
        span.record("task", task.as_str());

        if !request.resume_id.trim().is_empty() {
            return self
                .resume_from_edge(&request, claims, job, task)
                .await
                .map(Response::new);
        }

        let slot = self.launch_slot().await?;
        let task_manager = self.task_manager.clone();
        let node_id = self.node_id.clone();
        let key = request.idempotency_key.clone();
        let started = tokio::task::spawn_blocking(move || {
            edge::start_edge_session(&task_manager, &node_id, &claims, &key, slot)
        })
        .await
        .map_err(|err| Status::internal(format!("session start panicked: {err}")))?
        .map_err(|err| admission_status(&err))?;
        span.record("session_id", started.session_id.as_str());

        let state = if started.deduplicated || !request.wait.unwrap_or(true) {
            self.task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?
                .session_status(&started.session_id)
                .map(|status| status.state)
                .unwrap_or_else(|| "starting".to_string())
        } else {
            match edge::wait_until_running(&self.task_manager, &started.session_id, EDGE_START_WAIT)
                .await
            {
                Ok(true) => "running".to_string(),
                Ok(false) => "starting".to_string(),
                Err(err) => return Err(task_start_status("Failed to start session", &err)),
            }
        };
        let generation = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .session_status(&started.session_id)
            .map(|status| status.generation)
            .unwrap_or(1);
        Ok(Response::new(crate::structs::StartSessionReply {
            session_id: started.session_id,
            node_id: self.node_id.clone(),
            state,
            deduplicated: started.deduplicated,
            resumed_from: String::new(),
            generation,
            moved_from: String::new(),
        }))
    }

    /// End one session from a session ticket.
    #[instrument(err, skip_all, fields(session_id))]
    async fn stop_session(
        &self,
        request: Request<crate::structs::StopSessionRequest>,
    ) -> Result<Response<crate::structs::StopSessionReply>, Status> {
        // Caller class: ticket only.
        let caller = crate::worker::plane::caller_of(&request, "StopSession")?;
        if let crate::worker::plane::Caller::Manager(_) = caller {
            return Err(Status::unauthenticated(
                "StopSession admits a session ticket only; managers end sessions with \
                 StopSessions",
            ));
        }
        let request = request.into_inner();
        tracing::Span::current().record("session_id", request.session_id.as_str());
        let ticket = self.verify_session_ticket(&request.ticket, "StopSession")?;
        let mut manager = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?;
        let Some(session) = manager.session_status(&request.session_id) else {
            // An evicted session runs nothing and holds nothing: stopping
            // it forgets the id, once the ticket is checked against it.
            if let Some((job, task)) = manager.idle_session_owner(&request.session_id) {
                ticket.allows(&job, &task)?;
                manager.stop_sessions(
                    std::slice::from_ref(&request.session_id),
                    "requested (edge stop)",
                );
                return Ok(Response::new(crate::structs::StopSessionReply {
                    stopped: false,
                    node_id: self.node_id.clone(),
                }));
            }
            drop(manager);
            let status = self.session_not_here(&request.session_id);
            // Already gone from this node: idempotent, unless the caller has
            // the wrong node.
            if status.message().starts_with("not_found") {
                return Ok(Response::new(crate::structs::StopSessionReply {
                    stopped: false,
                    node_id: self.node_id.clone(),
                }));
            }
            return Err(status);
        };
        ticket.allows(&session.job_id, &session.task_name)?;
        let stopped = manager.stop_sessions(
            std::slice::from_ref(&request.session_id),
            "requested (edge stop)",
        );
        Ok(Response::new(crate::structs::StopSessionReply {
            stopped: !stopped.is_empty(),
            node_id: self.node_id.clone(),
        }))
    }
}

/// How long `StartSession` waits for a session to be running before it
/// answers `starting`.
const EDGE_START_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// A verified session ticket.
pub(crate) struct VerifiedSessionTicket(crate::structs::edge_boot::SessionTicketClaims);

impl VerifiedSessionTicket {
    /// PermissionDenied unless the ticket covers this job and task.
    fn allows(&self, job: &str, task: &str) -> Result<(), Status> {
        self.0
            .covers(job, task)
            .map_err(|err| Status::permission_denied(format!("{err:#}")))
    }
}

impl WorkerDaemonService {
    /// Verify a session ticket presented on the worker plane.
    pub(crate) fn verify_session_ticket(
        &self,
        token: &str,
        rpc: &str,
    ) -> Result<VerifiedSessionTicket, Status> {
        if token.trim().is_empty() {
            return Err(Status::unauthenticated(format!(
                "{rpc} admits a manager certificate or a session ticket; this request carried \
                 neither"
            )));
        }
        let Some(edge_boot) = self.edge_boot.as_ref() else {
            return Err(Status::failed_precondition(format!(
                "{rpc}: this node does not accept edge starts (set edgeBoot.acceptClaims: true \
                 in its worker config)"
            )));
        };
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        edge_boot
            .verifier
            .verify_session(token, now)
            .map(VerifiedSessionTicket)
            .map_err(|err| Status::permission_denied(format!("session ticket refused: {err:#}")))
    }
}

impl WorkerDaemonService {
    /// The status a failed suspend or resume of a session answers with
    /// (docs/session-mode.md, "Idle sessions").
    pub(crate) fn session_sleep_status(&self, session_id: &str, err: &anyhow::Error) -> Status {
        use crate::worker::task_manager::session_sleep::SessionSleepError;
        use crate::worker::task_manager::SessionRefusal;
        if let Some(sleep) = err.downcast_ref::<SessionSleepError>() {
            return match sleep {
                SessionSleepError::NotHere | SessionSleepError::Evicted(_) => {
                    self.session_not_here(session_id)
                }
                SessionSleepError::NotSuspendable(why) => Status::failed_precondition(why.clone()),
                SessionSleepError::Busy => Status::aborted(sleep.to_string()),
                other => store_status(other).unwrap_or_else(|| Status::internal(other.to_string())),
            };
        }
        match err.downcast_ref::<SessionRefusal>() {
            Some(SessionRefusal::EnvelopeFull { .. }) => Status::resource_exhausted(format!(
                "{err:#}; session {session_id} stays suspended with its checkpoint, retry once \
                 sessions end"
            )),
            Some(SessionRefusal::NoEnvelope { .. }) => {
                Status::failed_precondition(format!("{err:#}"))
            }
            None => Status::internal(format!("{err:#}")),
        }
    }

    /// `StartSession` with `resume_id` (session durability plan §6.7, "Moving
    /// is cautious for running sessions"): resume a `store` session on this
    /// node from its manifest. A session the manifest says runs on another
    /// node is probed there first (`GetSession`, 2 s, with the caller's
    /// ticket): running there, the move claims the next generation and asks
    /// that node to stop its copy (`StopSession`) before it launches;
    /// unreachable, the move refuses UNAVAILABLE unless `force_move` says
    /// the caller knows the node is down. A suspended session moves at once.
    async fn resume_from_edge(
        &self,
        request: &crate::structs::StartSessionRequest,
        claims: crate::structs::edge_boot::SessionTicketClaims,
        job: String,
        task: String,
    ) -> Result<crate::structs::StartSessionReply, Status> {
        let session_id = request.resume_id.trim().to_string();
        tracing::Span::current().record("session_id", session_id.as_str());
        if !crate::structs::is_minted_for_task(&session_id, &task) {
            return Err(Status::invalid_argument(format!(
                "resume_id {session_id:?} is not a session id of task {task}"
            )));
        }
        let move_request = crate::worker::task_manager::session_store::MoveRequest {
            session_id: session_id.clone(),
            job_id: job.clone(),
            task_name: task.clone(),
            force_move: request.force_move,
            cold: false,
            origin: "edge".into(),
            ticket_jti: claims.jti.clone(),
        };
        let mut outcome = self.run_move(move_request.clone(), None).await;
        let elsewhere = outcome.as_ref().err().and_then(running_elsewhere);
        if let Some(SessionSleepErrorRef::RunningElsewhere {
            node,
            address,
            generation,
        }) = elsewhere
        {
            let probe = self
                .probe_peer(&node, &address, &session_id, &request.ticket)
                .await;
            debug!(session_id = %session_id, old_node = %node, generation, probe = ?probe, "Probed the node a moving session's manifest names");
            match probe {
                PeerProbe::Unreachable(why) => {
                    return Err(Status::unavailable(format!(
                        "running_elsewhere: session {session_id} runs on node {node} (generation \
                         {generation}), which did not answer within {}s: {why}. Retry with \
                         force_move only once that node is known to be down \
                         (`legion session nodes`) [winner node={node} generation={generation}]",
                        MOVE_PROBE_TIMEOUT.as_secs()
                    )));
                }
                PeerProbe::NotRunning => {
                    let mut forced = move_request.clone();
                    forced.force_move = true;
                    outcome = self.run_move(forced, None).await;
                }
                PeerProbe::Running => {
                    let mut forced = move_request.clone();
                    forced.force_move = true;
                    let stopper = self.peer_stopper(&node, &address, &session_id, &request.ticket);
                    outcome = self.run_move(forced, Some(stopper)).await;
                }
            }
        }
        let moved = outcome.map_err(|err| self.session_sleep_status(&session_id, &err))?;
        let state = self
            .task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .session_status(&session_id)
            .map(|status| status.state)
            .unwrap_or_else(|| "starting".to_string());
        Ok(crate::structs::StartSessionReply {
            session_id,
            node_id: self.node_id.clone(),
            state,
            deduplicated: moved.from.is_empty(),
            resumed_from: moved.from.to_string(),
            generation: moved.generation,
            moved_from: moved.moved_from,
        })
    }

    /// Run one move attempt on a blocking thread with a launch slot.
    async fn run_move(
        &self,
        request: crate::worker::task_manager::session_store::MoveRequest,
        after_claim: Option<crate::worker::task_manager::session_store::AfterClaim>,
    ) -> R<crate::worker::task_manager::session_store::MoveOutcome> {
        let slot = self
            .launch_pool
            .acquire()
            .await
            .map_err(|full| anyhow!(full.to_string()))?;
        let task_manager = self.task_manager.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            crate::worker::task_manager::session_store::resume_moved(
                &task_manager,
                &request,
                after_claim,
            )
        })
        .await
        .map_err(|err| anyhow!("session resume panicked: {err}"))?
    }

    /// A client for another worker, verified against the cluster CA as
    /// `<node>.node.legion`, presenting no client certificate (the ticket
    /// is the credential).
    async fn peer_client(
        node_identity: Option<Arc<crate::worker::plane::NodeIdentity>>,
        node: &str,
        address: &str,
    ) -> R<crate::structs::worker_client::WorkerClient<tonic::transport::Channel>> {
        let roots = node_identity
            .and_then(|identity| identity.roots())
            .context("this node holds no cluster CA bundle yet")?;
        let name = crate::manager::mesh_ca::node_dns_name(node);
        let verifier = crate::cluster_tls::NamedServerVerifier::new(roots, &name)?;
        let config = crate::cluster_tls::client_config(verifier, None)?;
        let channel = crate::cluster_tls::tls_connect(address, config, &name).await?;
        Ok(crate::structs::worker_client::WorkerClient::new(channel))
    }

    /// Ask the node a manifest names whether it runs the session.
    async fn probe_peer(
        &self,
        node: &str,
        address: &str,
        session_id: &str,
        ticket: &str,
    ) -> PeerProbe {
        if address.is_empty() {
            return PeerProbe::Unreachable("the manifest names no address for it".into());
        }
        let identity = self.node_identity.clone();
        let probe = async {
            let mut client = Self::peer_client(identity, node, address).await?;
            Ok::<_, anyhow::Error>(
                client
                    .get_session(crate::structs::GetSessionRequest {
                        session_id: session_id.to_string(),
                        ticket: ticket.to_string(),
                    })
                    .await,
            )
        };
        match tokio::time::timeout(MOVE_PROBE_TIMEOUT, probe).await {
            Err(_) => PeerProbe::Unreachable("timed out".into()),
            Ok(Err(err)) => PeerProbe::Unreachable(format!("{err:#}")),
            Ok(Ok(Ok(reply))) => match reply.into_inner().session {
                Some(session) if session.state != "suspended" => PeerProbe::Running,
                _ => PeerProbe::NotRunning,
            },
            Ok(Ok(Err(status))) if status.code() == tonic::Code::NotFound => PeerProbe::NotRunning,
            Ok(Ok(Err(status))) => PeerProbe::Unreachable(status.message().to_string()),
        }
    }

    /// The hook a move runs once its claim is won: ask the old node to stop
    /// its copy (best effort; its next manifest write or the fence marker
    /// stops it otherwise).
    fn peer_stopper(
        &self,
        node: &str,
        address: &str,
        session_id: &str,
        ticket: &str,
    ) -> crate::worker::task_manager::session_store::AfterClaim {
        let identity = self.node_identity.clone();
        let (node, address, session_id, ticket) = (
            node.to_string(),
            address.to_string(),
            session_id.to_string(),
            ticket.to_string(),
        );
        let handle = tokio::runtime::Handle::current();
        Box::new(move |_| {
            let stop = async {
                let mut client = Self::peer_client(identity, &node, &address).await?;
                client
                    .stop_session(crate::structs::StopSessionRequest {
                        ticket,
                        session_id: session_id.clone(),
                    })
                    .await
                    .map_err(|status| anyhow!(status.message().to_string()))?;
                Ok::<_, anyhow::Error>(())
            };
            let result = handle.block_on(async {
                tokio::time::timeout(MOVE_PROBE_TIMEOUT, stop)
                    .await
                    .unwrap_or_else(|_| Err(anyhow!("timed out")))
            });
            if let Err(err) = result {
                warn!(session_id = %session_id, old_node = %node, error = %format!("{err:#}"), "Could not stop a moved session's old copy; its fence marker or next manifest write stops it");
            }
        })
    }

    /// NotFound for a session this node does not hold: `wrong_node` when its
    /// id carries another node's tag, `evicted` when the node remembers
    /// evicting it, `not_found` otherwise (session durability plan §6.7, row
    /// 4).
    pub(crate) fn session_not_here(&self, session_id: &str) -> Status {
        if let Some(evicted) = self
            .task_manager
            .lock()
            .ok()
            .and_then(|manager| manager.evicted_session(session_id))
        {
            return Status::not_found(format!(
                "evicted: session {session_id} was evicted from {} {}; a local-only session \
                 cannot be restored\nHint: start a new session, or set suspend.durability = \
                 \"store\" so idle sessions are kept in the content store",
                self.node_id, evicted
            ));
        }
        let own = crate::structs::node_tag(&self.node_id);
        match crate::structs::session_node_tag(session_id) {
            Some(tag) if tag != own => Status::not_found(format!(
                "wrong_node: session {session_id} carries node tag {tag}, and this node \
                 ({}) is {own}. Route requests for a session by its id's tag, not by \
                 re-hashing a key: `legion session nodes` maps tags to node addresses",
                self.node_id
            )),
            _ => Status::not_found(format!(
                "not_found: node {} holds no session {session_id}",
                self.node_id
            )),
        }
    }
}

/// Drive one edge-initiated claim: stream its stages to the caller, ratify it
/// against the manager, and stream the verdict.
///
/// Ratification runs *concurrently* with the boot — that is the whole saving.
/// The caller sees `booted` as soon as the workload is up (a router may begin
/// routing there, knowing a `refused` may still arrive) and `ratified` when
/// the network opens.
#[allow(clippy::too_many_arguments)]
async fn drive_claim(
    edge_boot: Arc<EdgeBootRuntime>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    tx: tokio::sync::mpsc::Sender<Result<StartClaimReply, Status>>,
    claim_id: String,
    gate: Gate,
    job_config_id: String,
    task_name: String,
    instance_id: String,
    ticket: String,
    ratify_timeout: std::time::Duration,
) {
    // Frames are built as `Ok` because they go straight into an
    // `mpsc::Sender<Result<_, Status>>`; `tonic::Status` is a large error type
    // and only ever the unused variant here.
    #[allow(clippy::result_large_err)]
    let frame = |stage: &str,
                 detail: String,
                 allocation_id: i64,
                 reason: RefusalReason|
     -> Result<StartClaimReply, Status> {
        Ok(StartClaimReply {
            claim_id: claim_id.clone(),
            stage: stage.to_string(),
            allocation_id,
            reason: reason.to_wire(),
            detail,
            gate: gate.to_wire(),
        })
    };

    let _ = tx
        .send(frame(
            "admitted",
            "claim admitted locally".into(),
            0,
            RefusalReason::Unspecified,
        ))
        .await;

    // Ratify while the node boots. A verdict that lands before the boot
    // finishes is applied all the same: `ratify_claim` opens the gate the
    // start is currently behind, whichever one that is.
    let request = crate::structs::RatifyClaimRequest {
        node_id: edge_boot.node_id.clone(),
        claim_id: claim_id.clone(),
        job_config_id,
        task_name,
        instance_id,
        gate: gate.to_wire(),
        ticket,
    };
    let verdict = tokio::time::timeout(ratify_timeout, edge_boot.ratifier.ratify(request)).await;

    // Report whatever stage the boot actually reached before the verdict
    // decides the claim's fate — a router reading this stream needs to know
    // whether a workload exists at all.
    let stage = task_manager
        .lock()
        .expect("task manager")
        .claims_snapshot()
        .into_iter()
        .find(|claim| claim.claim_id == claim_id)
        .map(|claim| claim.stage);
    if let Some(stage) = stage {
        let _ = tx
            .send(frame(
                stage.as_str(),
                String::new(),
                0,
                RefusalReason::Unspecified,
            ))
            .await;
    }

    match verdict {
        Ok(Ok(reply)) if reply.confirmed => {
            let allocation = reply.allocation.clone();
            match allocation {
                Some(allocation) => {
                    let allocation_id = allocation.allocation_id;
                    edge::apply_verdict(
                        &task_manager,
                        &claim_id,
                        Some(&allocation),
                        RefusalReason::Unspecified,
                        "",
                        Feedback::Verdict,
                    );
                    let _ = tx
                        .send(frame(
                            "ratified",
                            String::new(),
                            allocation_id,
                            RefusalReason::Unspecified,
                        ))
                        .await;
                }
                None => {
                    // Confirmed but without the committed payload: the node
                    // cannot finish the start correctly (no ports, no bound
                    // instance, no mounts), so it kills rather than running
                    // something the cluster would describe differently.
                    let detail = "manager confirmed the claim without returning the committed \
                                  allocation"
                        .to_string();
                    edge::apply_verdict(
                        &task_manager,
                        &claim_id,
                        None,
                        RefusalReason::Internal,
                        &detail,
                        Feedback::Verdict,
                    );
                    let _ = tx
                        .send(frame("refused", detail, 0, RefusalReason::Internal))
                        .await;
                }
            }
        }
        Ok(Ok(reply)) => {
            // A refusal is still an answer: how fast it came is what the
            // window learns from, whatever the reason.
            let reason = RefusalReason::from_wire(reply.reason);
            edge::apply_verdict(
                &task_manager,
                &claim_id,
                None,
                reason,
                &reply.detail,
                Feedback::Verdict,
            );
            let _ = tx
                .send(frame("refused", reply.detail.clone(), 0, reason))
                .await;
        }
        Ok(Err(err)) => {
            // The manager was reachable but the call failed. The lease sweep
            // would kill this anyway; doing it now frees the capacity sooner
            // and gives the caller a reason instead of a silence.
            let detail = format!("ratification call failed: {err:#}");
            edge::apply_verdict(
                &task_manager,
                &claim_id,
                None,
                RefusalReason::Internal,
                &detail,
                Feedback::Failed,
            );
            let _ = tx
                .send(frame("refused", detail, 0, RefusalReason::Internal))
                .await;
        }
        Err(_) => {
            let detail = format!(
                "no verdict within the {} lease",
                humantime::format_duration(ratify_timeout)
            );
            edge::apply_verdict(
                &task_manager,
                &claim_id,
                None,
                RefusalReason::Expired,
                &detail,
                Feedback::Failed,
            );
            let _ = tx
                .send(frame("refused", detail, 0, RefusalReason::Expired))
                .await;
        }
    }
}

/// A borrowed view of a `running_elsewhere` refusal.
enum SessionSleepErrorRef {
    RunningElsewhere {
        node: String,
        address: String,
        generation: u64,
    },
}

/// The `running_elsewhere` refusal inside a move's error, if that is what
/// it was.
fn running_elsewhere(err: &anyhow::Error) -> Option<SessionSleepErrorRef> {
    use crate::worker::task_manager::session_sleep::SessionSleepError;
    match err.downcast_ref::<SessionSleepError>()? {
        SessionSleepError::RunningElsewhere {
            node,
            address,
            generation,
        } => Some(SessionSleepErrorRef::RunningElsewhere {
            node: node.clone(),
            address: address.clone(),
            generation: *generation,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A start failure carries its whole chain, so the cause under the
    /// outermost context (here, the cgroup file and errno) reaches the
    /// manager log and the allocation's failure record.
    #[test]
    fn a_start_failure_carries_its_cause_and_is_not_internal_test() {
        let err = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound))
            .context("write /sys/fs/cgroup/legion/share/legion-923/memory.max = \"67108864\"")
            .context("failed to place task web within this node's resource limits");
        let status = task_start_status("Failed to start task", &err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status.message().contains("legion-923/memory.max"),
            "{}",
            status.message()
        );
        assert!(status
            .message()
            .starts_with("Failed to start task: failed to place task web"));
    }

    #[test]
    fn a_node_out_of_cores_answers_resource_exhausted_test() {
        let err = anyhow::Error::new(crate::worker::resources::PlacementExhausted(
            "cannot reserve 2 exclusive core(s)".into(),
        ))
        .context("failed to place task web within this node's resource limits");
        let status = task_start_status("Failed to start task", &err);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(
            status.message().contains("cannot reserve 2"),
            "{}",
            status.message()
        );
    }

    #[test]
    fn a_failed_grpc_call_underneath_keeps_its_code_test() {
        let err = anyhow::Error::new(Status::unavailable("manager unreachable"))
            .context("fetch identity token");
        let status = task_start_status("Failed to deliver workload identities", &err);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(
            status.message().contains("manager unreachable"),
            "{}",
            status.message()
        );
    }
}

/// `Worker.StartClaim` under a slow control plane, end to end on one node:
/// a real ticket verifier, the real claim window and task manager, the
/// `exec` runtime, and a ratifier whose latency the test controls
/// (docs/edge-boot.md, "Overload").
#[cfg(test)]
mod edge_overload_test {
    use super::*;
    use crate::manager::workload_identity::Issuer;
    use crate::structs::edge_boot::SpeculativeStartConfig;
    use crate::structs::{RatifyClaimReply, RatifyClaimRequest, TaskConfig};
    use crate::worker::boot_ticket::{JwksSource, DEFAULT_CLOCK_SKEW, DEFAULT_JWKS_REFRESH};
    use crate::worker::claim_window::ClaimWindow;
    use serde_json::json;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::time::{Duration, SystemTime};

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
            let payload = crate::manager::dispatcher::build_task_allocation_with(
                &self.task,
                &crate::structs::Allocation {
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
                Some(crate::structs::ProvisionalAllocation {
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
            crate::worker::state::WorkerStateStore::new(dir.path().join("worker_state.json")),
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
            launch_pool: Arc::new(crate::worker::launch_pool::LaunchPool::new(
                8,
                Duration::from_secs(1),
            )),
            node_identity: None,
            egress: None,
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
                .get(crate::manager::worker_plane::ERROR_CODE_METADATA)
                .and_then(|value| value.to_str().ok()),
            Some(crate::manager::worker_plane::OVERLOADED)
        );
        assert!(
            refused.message().contains("overloaded"),
            "{}",
            refused.message()
        );
        let envelope =
            crate::cmd::output::CliError::from_status("Failed to start sandbox", &refused);
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
}
