use std::collections::HashMap;
use std::time::Duration;

use crate::prelude::*;

use crate::manager::dispatcher::build_task_allocation;
use crate::manager::scheduler::{PlanConfigOutcome, PlanMode, Scheduler};
use crate::storage::errors::StorageError;
use crate::storage::store::{JobSubmitOutcome, Store};
use crate::structs::manager_server::Manager;
use crate::structs::{
    worker_client::WorkerClient, AcquireKvLockReply, AcquireKvLockRequest, DeleteKvReply,
    DeleteKvRequest, DestroyJobReply, DestroyJobRequest, FailedAllocation, GetJobInfoReply,
    GetJobInfoRequest, GetJobSpecReply, GetJobSpecRequest, GetKvLockReply, GetKvLockRequest,
    GetKvReply, GetKvRequest, GetLedgerReply, GetLedgerRequest, GetManagerConfigsReply,
    GetManagerConfigsRequest, GetNodeReply, GetNodeRequest, GetNodesReply, GetNodesRequest,
    HealthCheckReply, HealthCheckRequest, HeartbeatReply, HeartbeatRequest, JobConfig, KvValueType,
    LedgerNode, ListJobsReply, ListJobsRequest, ListKvReply, ListKvRequest, Node, NodeMsg,
    PatchKvReply, PatchKvRequest, PlanJobReply, PlanJobRequest, PlannedAllocation, PutKvReply,
    PutKvRequest, ReleaseKvLockReply, ReleaseKvLockRequest, RenewKvLockReply, RenewKvLockRequest,
    RestartWorkerReply, RestartWorkerRequest, RunJobReply, RunJobRequest, StopJobReply,
    StopJobRequest, TaskConfig, TaskOnUnhealthyConfig, UpdateNodeDynamicReply,
    UpdateNodeDynamicRequest, WorkerConfig, WorkerPreRegisterReply, WorkerPreRegisterRequest,
    WorkerRegisterReply, WorkerRegisterRequest,
};
use crate::task_plugins::validate_job_config;

use time::{Duration as TimeDuration, OffsetDateTime};
use tonic::{Request, Response, Status};

fn map_store_error(action: &str, err: anyhow::Error) -> Status {
    error!("{action}: {:?}", err);

    let Some(storage_error) = StorageError::from_anyhow(&err) else {
        return Status::internal(action);
    };

    let message = format!("{action}: {storage_error}");
    match storage_error {
        StorageError::NotFound { .. } => Status::not_found(message),
        StorageError::Conflict { .. } => Status::already_exists(message),
        StorageError::InvalidInput { .. } => Status::invalid_argument(message),
        StorageError::ForeignKey { .. } => Status::failed_precondition(message),
        StorageError::DatabaseBusy | StorageError::DatabaseLocked => Status::unavailable(message),
        StorageError::DatabaseReadOnly => Status::failed_precondition(message),
        StorageError::DatabaseCorrupt
        | StorageError::DatabaseIo { .. }
        | StorageError::Sqlite { .. } => Status::internal(message),
    }
}

#[derive(Debug)]
pub struct ManagerAgentService {
    pub store: Store,
    allocation_failures: Arc<Mutex<AllocationFailureTracker>>,
}

impl ManagerAgentService {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            allocation_failures: Arc::new(Mutex::new(AllocationFailureTracker::default())),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TaskKey {
    job_config_id: String,
    task_config_id: i64,
}

#[derive(Debug, Clone)]
struct AllocationFailureContext {
    job_id: String,
    job_config_id: String,
    task_config_id: i64,
    on_unhealthy: TaskOnUnhealthyConfig,
}

#[derive(Debug, Default)]
struct AllocationFailureTracker {
    allocations: HashMap<i64, AllocationFailureContext>,
    move_history: HashMap<TaskKey, Vec<OffsetDateTime>>,
}

#[tonic::async_trait]
impl Manager for ManagerAgentService {
    #[instrument(skip_all)]
    async fn run_job(
        &self,
        request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {
        let inner = request.into_inner();
        let js = inner.parsed_jobspec;
        let subs = inner.submissions;
        let priority = inner.priority.unwrap_or(self.store.default_eval_priority);

        let config: JobConfig = serde_json::from_str(&js).map_err(|e| {
            error!("Error parsing jobspec: {:?}", e);
            Status::invalid_argument(format!("Error parsing jobspec: {e}"))
        })?;

        let force_reschedule = inner.force_reschedule;
        validate_job_config(&config).map_err(|e| {
            error!("Error validating task configs: {:?}", e);
            Status::invalid_argument("Error validating task configs")
        })?;
        let submit_outcome = self
            .store
            .handle_job_submit(&config, &subs)
            .map_err(|e| map_store_error("Error running job", e))?;

        let job_config_id = submit_outcome.job_config_id().to_string();
        let (eval_kind, message) = match submit_outcome {
            JobSubmitOutcome::Unchanged { .. } => {
                if force_reschedule {
                    (
                        Some("job_update"),
                        "Job config unchanged; forced reschedule queued",
                    )
                } else {
                    (None, "Job config unchanged; no update needed")
                }
            }
            JobSubmitOutcome::Created { .. } | JobSubmitOutcome::Updated { .. } => {
                (Some("job_submit"), "Job submit success")
            }
        };

        if let Some(kind) = eval_kind {
            self.store
                .enqueue_eval(
                    kind,
                    Some(&config.name),
                    Some(&job_config_id),
                    None,
                    Some(priority),
                )
                .map_err(|e| map_store_error("Error running job", e))?;
        }

        let reply = RunJobReply {
            message: message.into(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_job_info(
        &self,
        request: Request<GetJobInfoRequest>,
    ) -> Result<Response<GetJobInfoReply>, Status> {
        let name = request.into_inner().name;

        let job = self
            .store
            .get_job(&name)
            .map_err(|e| map_store_error("Error getting job", e))?;

        let reply = GetJobInfoReply { job: Some(job) };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn plan_job(
        &self,
        request: Request<PlanJobRequest>,
    ) -> Result<Response<PlanJobReply>, Status> {
        let jobspec = request.into_inner().parsed_jobspec;
        if jobspec.trim().is_empty() {
            return Err(Status::invalid_argument("Plan request missing jobspec"));
        }

        let config: JobConfig = serde_json::from_str(&jobspec).map_err(|e| {
            error!("Error parsing jobspec for plan: {:?}", e);
            Status::invalid_argument("Error parsing jobspec")
        })?;
        let config = self
            .store
            .normalize_job_config(&config)
            .map_err(|e| map_store_error("Error normalizing jobspec", e))?;
        validate_job_config(&config).map_err(|e| {
            error!("Error validating task configs: {:?}", e);
            Status::invalid_argument("Error validating task configs")
        })?;

        let scheduler = Scheduler::new(&self.store).map_err(|e| {
            error!("Error building scheduler: {:?}", e);
            Status::internal("Error preparing scheduler")
        })?;

        let existing_config = self
            .store
            .get_job_config_optional(&config.name)
            .map_err(|e| map_store_error("Error getting job config", e))?;

        let (plan, task_lookup, message, matches_existing, same_content) = match existing_config {
            Some(existing) if existing.is_same_as(&config) => {
                let plan = scheduler
                    .plan_only(&existing.id(), PlanMode::Fresh)
                    .map_err(|e| {
                        error!("Error planning job {}: {:?}", existing.name, e);
                        Status::internal("Error planning job")
                    })?;
                let task_configs = self
                    .store
                    .get_task_configs_for_job_config(&existing.id())
                    .map_err(|e| map_store_error("Error getting task configs", e))?;
                let task_lookup = task_configs
                    .into_iter()
                    .map(|(id, task)| (id, task.name))
                    .collect::<HashMap<_, _>>();

                (
                    plan,
                    task_lookup,
                    "Job config unchanged; showing force-reschedule placements".to_string(),
                    true,
                    true,
                )
            }
            Some(_existing) => {
                let PlanConfigOutcome {
                    allocations,
                    task_lookup,
                } = scheduler
                    .plan_for_config(&config, PlanMode::Fresh, Some(&config.name))
                    .map_err(|e| {
                        error!("Error planning updated job {}: {:?}", config.name, e);
                        Status::internal("Error planning job")
                    })?;
                (
                    allocations,
                    task_lookup,
                    "Job config changed; showing updated placements".to_string(),
                    true,
                    false,
                )
            }
            None => {
                let PlanConfigOutcome {
                    allocations,
                    task_lookup,
                } = scheduler
                    .plan_for_config(&config, PlanMode::Fresh, None)
                    .map_err(|e| {
                        error!("Error planning new job {}: {:?}", config.name, e);
                        Status::internal("Error planning job")
                    })?;
                (
                    allocations,
                    task_lookup,
                    "Job config new; showing placements".to_string(),
                    false,
                    false,
                )
            }
        };

        let allocations = plan
            .into_iter()
            .map(|placement| PlannedAllocation {
                node_id: placement.node_id,
                task_config_id: placement.task_config_id,
                task_name: task_lookup
                    .get(&placement.task_config_id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();

        let reply = PlanJobReply {
            allocations,
            message,
            matches_existing,
            same_content,
        };

        Ok(Response::new(reply))
    }

    async fn get_job_spec(
        &self,
        request: Request<GetJobSpecRequest>,
    ) -> Result<Response<GetJobSpecReply>, Status> {
        let name = request.into_inner().name;

        let config = match self
            .store
            .get_job_config_optional(&name)
            .map_err(|e| map_store_error("Error getting job config", e))?
        {
            Some(config) => config,
            None => {
                return Err(Status::not_found("Job config not found"));
            }
        };

        let submissions = self
            .store
            .get_submissions_for_config(&config.id())
            .map_err(|e| map_store_error("Error getting submissions", e))?;

        let reply = GetJobSpecReply { submissions };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn list_jobs(
        &self,
        _request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsReply>, Status> {
        let jobs = self
            .store
            .get_jobs()
            .map_err(|e| map_store_error("Error getting jobs", e))?;

        let reply = ListJobsReply { jobs };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn stop_job(
        &self,
        request: Request<StopJobRequest>,
    ) -> Result<Response<StopJobReply>, Status> {
        let inner = request.into_inner();
        let name = inner.name;
        let priority = inner.priority.unwrap_or(self.store.default_eval_priority);

        let job_config_id = self
            .store
            .get_job_config(&name)
            .map_err(|e| map_store_error("Error stopping job", e))?
            .id();

        let _ = self
            .store
            .stop_job(&name)
            .map_err(|e| map_store_error("Error stopping job", e))?;

        self.store
            .enqueue_eval(
                "job_stop",
                Some(&name),
                Some(&job_config_id),
                None,
                Some(priority),
            )
            .map_err(|e| map_store_error("Error stopping job", e))?;

        let message = format!("Stopped {}", name);
        let reply = StopJobReply { message };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn destroy_job(
        &self,
        request: Request<DestroyJobRequest>,
    ) -> Result<Response<DestroyJobReply>, Status> {
        let name = request.into_inner().name;

        let _ = self
            .store
            .destroy_job(&name)
            .map_err(|e| map_store_error("Error deleting job", e))?;

        // TODO: Conditionalize
        let message = format!("Destroyed {}", name);
        let reply = DestroyJobReply { message };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let req = request.into_inner();
        let id = req.id;

        self.store
            .update_node_heartbeat(id.clone())
            .map_err(|e| map_store_error("Error updating node heartbeat", e))?;

        if let Err(err) = handle_failed_allocations(
            &self.store,
            &self.allocation_failures,
            &id,
            &req.failed_allocations,
        ) {
            error!(node_id = %id, error = ?err, "Error handling failed allocations");
        }

        let desired_allocations = build_desired_allocations(&self.store, &id, &req.allocation_ids)
            .map_err(|e| map_store_error("Error building desired allocations", e))?;

        let reply = HeartbeatReply {
            message: "ack".into(),
            desired_allocations,
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        let req = request.into_inner();
        let id = req.id;

        self.store
            .update_node_health_checks(&id, &req.health_checks)
            .map_err(|e| map_store_error("Error updating node health checks", e))?;

        let reply = HealthCheckReply {
            message: "ack".into(),
        };

        Ok(Response::new(reply))
    }

    // ...

    #[instrument(skip_all)]
    async fn worker_pre_register(
        &self,
        request: Request<WorkerPreRegisterRequest>,
    ) -> Result<Response<WorkerPreRegisterReply>, Status> {
        let request = request.into_inner();
        let worker_type = request.worker_type;

        let worker_config = self
            .store
            .get_worker_type(worker_type.clone())
            .map_err(|e| map_store_error("Error getting worker type", e))?;

        let config = WorkerConfig {
            name: worker_type,
            pool: worker_config.pool,
            additional_node_checks: worker_config.additional_node_checks,
            attributes: worker_config.attributes,
            name_prefix: worker_config.name_prefix,
            resources: worker_config.resources,
            reserved_resources: worker_config.reserved_resources,
            max_allocations: worker_config.max_allocations,
        };

        let reply = WorkerPreRegisterReply {
            config: Some(config),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn worker_register(
        &self,
        request: Request<WorkerRegisterRequest>,
    ) -> Result<Response<WorkerRegisterReply>, Status> {
        let request = request.into_inner();
        let dynamic_attrs = json!({}).to_string();

        let node_msg = NodeMsg {
            id: request.name,
            fingerprint_attrs: request.fingerprint_attrs,
            config_attrs: request.config_attrs,
            dynamic_attrs,
            status: "active".to_string(),
            availability: "available".to_string(),
            pool: request.pool,
            last_heartbeat: String::new(),
            health_checks: request.health_checks,
        };

        self.store
            .insert_node(node_msg)
            .map_err(|e| map_store_error("Error registering worker", e))?;

        let reply = WorkerRegisterReply {
            message: "Worker registered".into(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_nodes(
        &self,
        _request: Request<GetNodesRequest>,
    ) -> Result<Response<GetNodesReply>, Status> {
        let nodes = self
            .store
            .get_nodes()
            .map_err(|e| map_store_error("Error getting nodes", e))?;

        let node_messages: Vec<NodeMsg> = nodes.into_iter().map(Into::into).collect();

        let reply = GetNodesReply {
            nodes: node_messages,
        };
        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeReply>, Status> {
        let id = request.into_inner().id;
        let node = self
            .store
            .get_node(&id)
            .map_err(|e| map_store_error("Error getting node", e))?;

        let reply = GetNodeReply {
            node: Some(node.into()),
        };
        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn restart_worker(
        &self,
        request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        let node_id = request.into_inner().node_id;
        let node = self.store.get_node(&node_id).map_err(|e| {
            error!(error = ?e, node_id = %node_id, "Failed to get node for restart");
            Status::not_found("Node not found")
        })?;

        let worker_address = worker_address_for_node(&node)
            .ok_or_else(|| Status::failed_precondition("Worker address missing"))?;

        let mut client = WorkerClient::connect(worker_address)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to connect to worker: {e}")))?;
        let response = client
            .restart_worker(tonic::Request::new(RestartWorkerRequest {
                node_id: node_id.clone(),
            }))
            .await
            .map_err(|e| Status::unavailable(format!("Worker restart failed: {e}")))?;

        let reply = RestartWorkerReply {
            message: response.into_inner().message,
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn update_node_dynamic(
        &self,
        request: Request<UpdateNodeDynamicRequest>,
    ) -> Result<Response<UpdateNodeDynamicReply>, Status> {
        let r = request.into_inner();
        let id = r.id;
        let dd_as_map: Map<String, Value> = serde_json::from_str(&r.dynamic_diff).map_err(|e| {
            error!("Error parsing dynamic diff: {:?}", e);
            Status::internal("Error parsing dynamic diff")
        })?;

        let node = self
            .store
            .update_node_dyamic(&id, &dd_as_map)
            .map_err(|e| map_store_error("Error updating node", e))?;
        let allocs = self
            .store
            .allocs_for_node(&id)
            .map_err(|e| map_store_error("Error getting allocations for node", e))?;
        let mut job_configs_to_eval = HashMap::new();
        for alloc in allocs {
            job_configs_to_eval
                .entry(alloc.job_config_id)
                .or_insert(alloc.job_id);
        }
        for (job_config_id, job_id) in job_configs_to_eval {
            self.store
                .enqueue_eval(
                    "job_update",
                    Some(&job_id),
                    Some(&job_config_id),
                    Some(&id),
                    Some(self.store.default_eval_priority),
                )
                .map_err(|e| map_store_error("Error enqueueing eval for node", e))?;
        }
        let reply = UpdateNodeDynamicReply {
            node: Some(node.into()),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_manager_configs(
        &self,
        _request: Request<GetManagerConfigsRequest>,
    ) -> Result<Response<GetManagerConfigsReply>, Status> {
        let configs = self
            .store
            .get_manager_configs()
            .map_err(|e| map_store_error("Error getting manager configs", e))?;

        let reply = GetManagerConfigsReply { configs };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_ledger(
        &self,
        _request: Request<GetLedgerRequest>,
    ) -> Result<Response<GetLedgerReply>, Status> {
        let ledger = self
            .store
            .get_ledger()
            .map_err(|e| map_store_error("Error getting ledger", e))?;

        let nodes: Vec<LedgerNode> = ledger.into_iter().map(Into::into).collect();

        let reply: GetLedgerReply = GetLedgerReply { nodes };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn put_kv(&self, request: Request<PutKvRequest>) -> Result<Response<PutKvReply>, Status> {
        let req = request.into_inner();
        let value_type = KvValueType::try_from(req.value_type).unwrap_or(KvValueType::Unspecified);
        let entry = self
            .store
            .put_kv(
                &req.key,
                &req.value,
                value_type,
                req.expected_version,
                req.lock_id.as_deref(),
            )
            .map_err(|e| map_store_error("Error putting kv entry", e))?;

        Ok(Response::new(PutKvReply { entry: Some(entry) }))
    }

    #[instrument(skip_all)]
    async fn get_kv(&self, request: Request<GetKvRequest>) -> Result<Response<GetKvReply>, Status> {
        let req = request.into_inner();
        let entry = self
            .store
            .get_kv(&req.key)
            .map_err(|e| map_store_error("Error getting kv entry", e))?;

        Ok(Response::new(GetKvReply { entry }))
    }

    #[instrument(skip_all)]
    async fn list_kv(
        &self,
        request: Request<ListKvRequest>,
    ) -> Result<Response<ListKvReply>, Status> {
        let req = request.into_inner();
        let entries = self
            .store
            .list_kv(&req.prefix)
            .map_err(|e| map_store_error("Error listing kv entries", e))?;

        Ok(Response::new(ListKvReply { entries }))
    }

    #[instrument(skip_all)]
    async fn delete_kv(
        &self,
        request: Request<DeleteKvRequest>,
    ) -> Result<Response<DeleteKvReply>, Status> {
        let req = request.into_inner();
        self.store
            .delete_kv(&req.key, req.expected_version, req.lock_id.as_deref())
            .map_err(|e| map_store_error("Error deleting kv entry", e))?;

        Ok(Response::new(DeleteKvReply {
            message: "kv entry deleted".to_string(),
        }))
    }

    #[instrument(skip_all)]
    async fn patch_kv(
        &self,
        request: Request<PatchKvRequest>,
    ) -> Result<Response<PatchKvReply>, Status> {
        let req = request.into_inner();
        let entry = self
            .store
            .patch_kv(
                &req.key,
                &req.patch_json,
                req.expected_version,
                req.lock_id.as_deref(),
            )
            .map_err(|e| map_store_error("Error patching kv entry", e))?;

        Ok(Response::new(PatchKvReply { entry: Some(entry) }))
    }

    #[instrument(skip_all)]
    async fn acquire_kv_lock(
        &self,
        request: Request<AcquireKvLockRequest>,
    ) -> Result<Response<AcquireKvLockReply>, Status> {
        let req = request.into_inner();
        let lock = self
            .store
            .acquire_kv_lock(
                &req.key,
                req.lock_id.as_deref(),
                req.owner.as_deref(),
                req.ttl_seconds,
                req.force,
            )
            .map_err(|e| map_store_error("Error acquiring kv lock", e))?;

        Ok(Response::new(AcquireKvLockReply { lock: Some(lock) }))
    }

    #[instrument(skip_all)]
    async fn renew_kv_lock(
        &self,
        request: Request<RenewKvLockRequest>,
    ) -> Result<Response<RenewKvLockReply>, Status> {
        let req = request.into_inner();
        let lock = self
            .store
            .renew_kv_lock(&req.key, &req.lock_id, req.ttl_seconds)
            .map_err(|e| map_store_error("Error renewing kv lock", e))?;

        Ok(Response::new(RenewKvLockReply { lock: Some(lock) }))
    }

    #[instrument(skip_all)]
    async fn release_kv_lock(
        &self,
        request: Request<ReleaseKvLockRequest>,
    ) -> Result<Response<ReleaseKvLockReply>, Status> {
        let req = request.into_inner();
        self.store
            .release_kv_lock(&req.key, &req.lock_id)
            .map_err(|e| map_store_error("Error releasing kv lock", e))?;

        Ok(Response::new(ReleaseKvLockReply {
            message: "kv lock released".to_string(),
        }))
    }

    #[instrument(skip_all)]
    async fn get_kv_lock(
        &self,
        request: Request<GetKvLockRequest>,
    ) -> Result<Response<GetKvLockReply>, Status> {
        let req = request.into_inner();
        let lock = self
            .store
            .get_kv_lock(&req.key)
            .map_err(|e| map_store_error("Error getting kv lock", e))?;

        Ok(Response::new(GetKvLockReply { lock }))
    }
}

fn worker_address_for_node(node: &Node) -> Option<String> {
    let attrs = node.config_attrs.as_object()?;
    attrs
        .get("workerAddress")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn handle_failed_allocations(
    store: &Store,
    failure_tracker: &Arc<Mutex<AllocationFailureTracker>>,
    node_id: &str,
    failed: &[FailedAllocation],
) -> R<()> {
    for failure in failed {
        let allocation_id = failure.allocation_id;
        let allocation_context = {
            let mut tracker = failure_tracker.lock().expect("allocation failure tracker");
            tracker.context_for_allocation(store, allocation_id)?
        };

        let Some(context) = allocation_context else {
            warn!(
                allocation_id,
                node_id, "Failed allocation not found; skipping reschedule"
            );
            continue;
        };

        store.remove_allocation_by_id(allocation_id)?;

        let move_decision = {
            let mut tracker = failure_tracker.lock().expect("allocation failure tracker");
            tracker.record_move(&context)?
        };

        match move_decision {
            MoveDecision::Reschedule { delay } => {
                let job_id = context.job_id.clone();
                let job_config_id = context.job_config_id.clone();
                let node_id = node_id.to_string();
                let store = store.clone();
                schedule_reschedule(store, job_id, job_config_id, node_id, delay);
            }
            MoveDecision::JobFailed => {
                store.mark_job_failed(&context.job_id)?;
                warn!(
                    allocation_id,
                    job_id = %context.job_id,
                    reason = %failure.reason,
                    "Move limit exceeded; marking job failed"
                );
            }
        }

        failure_tracker
            .lock()
            .expect("allocation failure tracker")
            .allocations
            .remove(&allocation_id);

        warn!(
            allocation_id,
            node_id,
            reason = %failure.reason,
            "Allocation reported failed; reschedule requested"
        );
    }

    Ok(())
}

enum MoveDecision {
    Reschedule { delay: Duration },
    JobFailed,
}

fn schedule_reschedule(
    store: Store,
    job_id: String,
    job_config_id: String,
    node_id: String,
    delay: Duration,
) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move {
                if delay.as_secs() > 0 {
                    tokio::time::sleep(delay).await;
                }
                if let Err(err) = store.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&job_config_id),
                    Some(&node_id),
                    None,
                ) {
                    error!(
                        job_id = %job_id,
                        job_config_id = %job_config_id,
                        error = ?err,
                        "Failed to enqueue reschedule after unhealthy allocation"
                    );
                }
            });
        }
        Err(_) => {
            let delay_for_thread = delay;
            let job_id = job_id.clone();
            let job_config_id = job_config_id.clone();
            let node_id = node_id.clone();
            std::thread::spawn(move || {
                if delay_for_thread.as_secs() > 0 {
                    std::thread::sleep(delay_for_thread);
                }
                if let Err(err) = store.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&job_config_id),
                    Some(&node_id),
                    None,
                ) {
                    error!(
                        job_id = %job_id,
                        job_config_id = %job_config_id,
                        error = ?err,
                        "Failed to enqueue reschedule after unhealthy allocation"
                    );
                }
            });
        }
    }
}

impl AllocationFailureTracker {
    fn context_for_allocation(
        &mut self,
        store: &Store,
        allocation_id: i64,
    ) -> R<Option<AllocationFailureContext>> {
        if let Some(context) = self.allocations.get(&allocation_id) {
            return Ok(Some(context.clone()));
        }

        let Some(allocation) = store.get_allocation_by_id(allocation_id)? else {
            return Ok(None);
        };

        let task_configs = store.cached_task_configs_for_job_config(&allocation.job_config_id)?;
        let task = task_configs
            .into_iter()
            .find(|(task_id, _)| *task_id == allocation.task_config_id)
            .map(|(_, task)| task)
            .context("Missing task config for allocation")?;

        let context = AllocationFailureContext {
            job_id: allocation.job_id,
            job_config_id: allocation.job_config_id,
            task_config_id: allocation.task_config_id,
            on_unhealthy: task.on_unhealthy,
        };
        self.allocations.insert(allocation_id, context.clone());
        Ok(Some(context))
    }

    fn record_move(&mut self, context: &AllocationFailureContext) -> R<MoveDecision> {
        let now = OffsetDateTime::now_utc();
        let key = TaskKey {
            job_config_id: context.job_config_id.clone(),
            task_config_id: context.task_config_id,
        };
        let history = self.move_history.entry(key).or_default();
        let window = time_duration(context.on_unhealthy.move_period);
        history.retain(|time| now - *time <= window);

        if let Some(limit) = context.on_unhealthy.move_count {
            if history.len() as u32 >= limit {
                return Ok(MoveDecision::JobFailed);
            }
        }

        history.push(now);
        Ok(MoveDecision::Reschedule {
            delay: context.on_unhealthy.move_delay,
        })
    }
}

fn time_duration(duration: Duration) -> TimeDuration {
    let secs = duration.as_secs() as i64;
    let nanos = duration.subsec_nanos() as i64;
    TimeDuration::seconds(secs) + TimeDuration::nanoseconds(nanos)
}

fn build_desired_allocations(
    store: &Store,
    node_id: &str,
    reported_allocations: &[i64],
) -> R<Vec<crate::structs::TaskAllocation>> {
    let desired = store.allocs_for_node(node_id)?;
    if desired.is_empty() {
        return Ok(Vec::new());
    }

    let reported: std::collections::HashSet<i64> = reported_allocations.iter().copied().collect();
    let mut task_cache: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
    let mut allocations = Vec::new();

    for allocation in desired {
        if reported.contains(&allocation.id) {
            continue;
        }

        let tasks = task_cache
            .entry(allocation.job_config_id.clone())
            .or_insert_with(HashMap::new);
        if tasks.is_empty() {
            let task_configs = store.get_task_configs_for_job_config(&allocation.job_config_id)?;
            for (id, config) in task_configs {
                tasks.insert(id, config);
            }
        }
        let task = tasks
            .get(&allocation.task_config_id)
            .with_context(|| format!("Missing task config {}", allocation.task_config_id))?;
        allocations.push(build_task_allocation(task, &allocation)?);
    }

    Ok(allocations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::worker_server::Worker;
    use crate::structs::{
        JobConfig, NodeMsg, PostWorkReply, PostWorkRequest, RestartWorkerReply,
        RestartWorkerRequest, Submission, TaskConfig,
    };
    use serde_json::json;
    use tempfile::Builder;
    use tokio::sync::oneshot;
    use tonic::transport::Server;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-manager-server-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn insert_node(store: &Store, id: &str) -> R<()> {
        let node = NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: json!({
                "memory_total": "8388608",
                "cpus": {
                    "cpu-1": { "frequency": "2400" },
                    "cpu-2": { "frequency": "2400" },
                }
            })
            .to_string(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        };
        store.insert_node(node)?;
        Ok(())
    }

    fn submission() -> Submission {
        Submission {
            path: "job.jsonnet".into(),
            content: "{}".into(),
            is_primary: true,
        }
    }

    #[derive(Debug, Clone)]
    struct TestWorker {
        called: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl Worker for TestWorker {
        async fn post_work(
            &self,
            _request: Request<PostWorkRequest>,
        ) -> Result<Response<PostWorkReply>, Status> {
            Err(Status::unimplemented("not used in test"))
        }

        async fn restart_worker(
            &self,
            request: Request<RestartWorkerRequest>,
        ) -> Result<Response<RestartWorkerReply>, Status> {
            let node_id = request.into_inner().node_id;
            let mut guard = self.called.lock().expect("mutex");
            *guard = Some(node_id);
            Ok(Response::new(RestartWorkerReply {
                message: "ok".into(),
            }))
        }
    }

    #[tokio::test]
    async fn manager_forwards_worker_restart() -> R<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);
        let worker_address = format!("http://{}", addr);

        let called = Arc::new(Mutex::new(None));
        let worker = TestWorker {
            called: called.clone(),
        };

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(crate::structs::worker_server::WorkerServer::new(worker))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let store = setup_store()?;
        let node = Node {
            id: "node-1".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            config_attrs: json!({ "workerAddress": worker_address }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        store.insert_node(NodeMsg::from(node))?;
        let manager = ManagerAgentService::new(store);

        let response = manager
            .restart_worker(Request::new(RestartWorkerRequest {
                node_id: "node-1".into(),
            }))
            .await?;

        assert_eq!(response.into_inner().message, "ok");
        let guard = called.lock().expect("mutex");
        assert_eq!(guard.as_deref(), Some("node-1"));

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[test]
    fn failed_allocations_enqueue_reschedule() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            name: "job".into(),
            tasks: vec![TaskConfig {
                on_unhealthy: TaskOnUnhealthyConfig {
                    move_delay: Duration::from_secs(0),
                    ..TaskOnUnhealthyConfig::default()
                },
                ..TaskConfig::new("task", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        let tracker = Arc::new(Mutex::new(AllocationFailureTracker::default()));
        handle_failed_allocations(
            &store,
            &tracker,
            "node-a",
            &[FailedAllocation {
                allocation_id: allocation.id,
                reason: "boom".into(),
            }],
        )?;
        std::thread::sleep(Duration::from_millis(10));

        assert!(store.get_allocation_by_id(allocation.id)?.is_none());
        let evals = store.get_evals()?;
        let eval = evals.first().context("missing eval")?;
        assert_eq!(eval.kind, "job_reschedule");
        assert_eq!(eval.job_config_id, job_config_id);
        assert_eq!(eval.node_name, "node-a");

        Ok(())
    }

    #[test]
    fn desired_allocations_ignore_reported() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            name: "job".into(),
            tasks: vec![TaskConfig {
                on_unhealthy: TaskOnUnhealthyConfig {
                    move_delay: Duration::from_secs(0),
                    ..TaskOnUnhealthyConfig::default()
                },
                ..TaskConfig::new("task", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        let desired = build_desired_allocations(&store, "node-a", &[])?;
        assert_eq!(desired.len(), 1);

        let desired = build_desired_allocations(&store, "node-a", &[allocation.id])?;
        assert!(desired.is_empty());

        Ok(())
    }
}
