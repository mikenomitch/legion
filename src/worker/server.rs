use crate::prelude::*;
use crate::structs::edge_boot::{Gate, RefusalReason};
use crate::structs::{
    worker_server::Worker, ExecClientFrame, GetTaskLogsReply, GetTaskLogsRequest, PostWorkReply,
    PostWorkRequest, RestartWorkerReply, RestartWorkerRequest, StartClaimReply, StartClaimRequest,
    TaskLogStream,
};
use crate::worker::boot_ticket::BootTicketVerifier;
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
    use crate::worker::task_manager::SessionRefusal;
    match err.downcast_ref::<SessionRefusal>() {
        Some(SessionRefusal::EnvelopeFull { .. }) => Status::resource_exhausted(format!("{err:#}")),
        _ => Status::failed_precondition(format!("{err:#}")),
    }
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
}

impl WorkerDaemonService {
    /// Take a launch slot, or refuse the start as retryable on another node.
    async fn launch_slot(&self) -> Result<crate::worker::launch_pool::LaunchSlot, Status> {
        self.launch_pool
            .acquire()
            .await
            .map_err(|full| Status::resource_exhausted(full.to_string()))
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
        let mut allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("Missing allocation"))?;

        // Every payload this node is handed teaches it a job config it may
        // later be asked to start from a boot ticket (docs/edge-boot.md).
        if let Some(edge_boot) = self.edge_boot.as_ref() {
            edge::remember_specs(&edge_boot.spec_cache, std::slice::from_ref(&allocation));
        }

        // Provisional dispatches and their ratifications ride the same RPC as
        // an ordinary dispatch, distinguished by the `provisional` block and
        // by whether the payload yet has an allocation id.
        if let Some(provisional) = allocation.provisional.clone() {
            if allocation.allocation_id == 0 {
                let slot = self.launch_slot().await?;
                let admitted = edge::admit_and_launch(
                    &self.task_manager,
                    &allocation,
                    &provisional,
                    Some(slot),
                )
                .map_err(|err| admission_status(&err))?;
                return Ok(Response::new(PostWorkReply {
                    message: format!("Provisional start {} admitted", admitted.claim_id),
                }));
            }
            // A ratification: the committed payload, carrying the claim id it
            // settles.
            edge::apply_verdict(
                &self.task_manager,
                &provisional.claim_id,
                Some(&allocation),
                RefusalReason::Unspecified,
                "",
            );
            return Ok(Response::new(PostWorkReply {
                message: format!("Claim {} ratified", provisional.claim_id),
            }));
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
                    let reason = format!("Failed to deliver workload identities: {err}");
                    self.task_manager
                        .lock()
                        .map_err(|_| Status::internal("Task manager lock poisoned"))?
                        .record_failure(allocation.allocation_id, reason.clone());
                    return Err(Status::internal(reason));
                }
            },
            None => Vec::new(),
        };

        let allocation_id = allocation.allocation_id;

        // Synchronous failures (bad config, unknown runtime) surface here;
        // artifact-bearing tasks stage on a background thread and report
        // staging failures through the heartbeat failure path.
        let task_manager = self.task_manager.clone();
        let launched =
            tokio::task::spawn_blocking(move || launch_task_allocation(&task_manager, &allocation))
                .await
                .map_err(|err| Status::internal(format!("launch task panicked: {err}")))?;
        if let Err(err) = launched {
            return Err(Status::internal(format!("Failed to start task: {err}")));
        }

        // Refresh loops are keyed by allocation id; identity_manager.retain
        // (run each heartbeat) prunes them if the allocation goes away.
        if let Some(identity) = self.identity.as_ref() {
            identity.spawn_refreshers(allocation_id, prepared, self.task_manager.clone());
        }

        let reply = PostWorkReply {
            message: "Task accepted".into(),
        };

        Ok(Response::new(reply))
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

        if req.dry_run {
            // Verify and report, hold nothing, boot nothing. The self-correct
            // path an agent or a router wants before it mutates a cluster.
            edge_boot.verifier.unspend(&claims.jti);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let reply = StartClaimReply {
                claim_id,
                stage: "admitted".into(),
                allocation_id: 0,
                reason: 0,
                detail: format!(
                    "dry run: ticket valid, gate would be `{gate}`, node would admit this start"
                ),
                gate: gate.to_wire(),
            };
            let _ = tx.send(Ok(reply)).await;
            return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
        }

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
            None,
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
        let slot = self.launch_slot().await.inspect_err(|_| {
            edge_boot.verifier.unspend(&claims.jti);
        })?;
        let admitted =
            edge::admit_and_launch(&self.task_manager, &payload, &provisional, Some(slot))
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
                Err(err) => return Err(Status::internal(format!("{err:#}"))),
            }
        };
        Ok(Response::new(crate::structs::StartSessionReply {
            session_id: started.session_id,
            node_id: self.node_id.clone(),
            state,
            deduplicated: started.deduplicated,
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
                 cannot be restored. Start a new session, or set suspend.durability = \"store\" \
                 to keep idle sessions in the content store",
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
                    );
                    let _ = tx
                        .send(frame("refused", detail, 0, RefusalReason::Internal))
                        .await;
                }
            }
        }
        Ok(Ok(reply)) => {
            let reason = RefusalReason::from_wire(reply.reason);
            edge::apply_verdict(&task_manager, &claim_id, None, reason, &reply.detail);
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
            );
            let _ = tx
                .send(frame("refused", detail, 0, RefusalReason::Expired))
                .await;
        }
    }
}
