use crate::prelude::*;
use crate::structs::edge_boot::{Gate, RefusalReason};
use crate::structs::{
    worker_server::Worker, ExecClientFrame, GetTaskLogsReply, GetTaskLogsRequest, PostWorkReply,
    PostWorkRequest, RestartWorkerReply, RestartWorkerRequest, StartClaimReply, StartClaimRequest,
    TaskLogStream,
};
use crate::worker::boot_ticket::BootTicketVerifier;
use crate::worker::edge;
use crate::worker::exec::{handle_exec_task, ExecTaskStream};
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

pub struct WorkerDaemonService {
    /// This worker's node id, checked against requests that name a node.
    pub node_id: String,
    pub task_manager: Arc<Mutex<WorkerTaskManager>>,
    /// Workload-identity delivery/refresh; `None` when this worker runs
    /// without a manager identity client (tests).
    pub identity: Option<Arc<WorkerIdentityManager>>,
    /// Edge boot support; `None` unless `edgeBoot.acceptClaims` is true.
    pub edge_boot: Option<Arc<EdgeBootRuntime>>,
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
                let admitted =
                    edge::admit_and_launch(&self.task_manager, &allocation, &provisional)
                        .map_err(|err| Status::failed_precondition(format!("{err:#}")))?;
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
        match crate::worker::plane::caller_of(&request, "ExecTask")? {
            crate::worker::plane::Caller::Manager(_) => {}
            crate::worker::plane::Caller::Ticket => {
                return Err(Status::unauthenticated(
                    "ExecTask admits a manager certificate or an exec ticket; this request \
                     carried neither",
                ));
            }
        }
        let stream = handle_exec_task(self.task_manager.clone(), request.into_inner()).await?;
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
        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        let claims = edge_boot
            .verifier
            .verify(&req.ticket, &req.instance_id, now_unix)
            .map_err(|err| Status::permission_denied(format!("boot ticket rejected: {err:#}")))?;

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
            }),
        )
        .map_err(|err| Status::internal(format!("failed to build payload: {err:#}")))?;

        let provisional = payload
            .provisional
            .clone()
            .expect("provisional block was just set");
        let admitted =
            edge::admit_and_launch(&self.task_manager, &payload, &provisional).map_err(|err| {
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
