use std::collections::HashMap;

use crate::prelude::*;

use crate::storage::store::{JobSubmitOutcome, Store};
use crate::structs::manager_server::Manager;
use crate::structs::{
    DestroyJobReply, DestroyJobRequest, GetJobInfoReply, GetJobInfoRequest, GetJobSpecReply,
    GetJobSpecRequest, GetLedgerReply, GetLedgerRequest, GetManagerConfigsReply,
    GetManagerConfigsRequest, GetNodeReply, GetNodeRequest, GetNodesReply, GetNodesRequest,
    HealthCheckReply, HealthCheckRequest, HeartbeatReply, HeartbeatRequest, JobConfig, LedgerNode,
    ListJobsReply, ListJobsRequest, NodeMsg, RunJobReply, RunJobRequest, StopJobReply,
    StopJobRequest, UpdateNodeDynamicReply, UpdateNodeDynamicRequest, WorkerConfig,
    WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterReply, WorkerRegisterRequest,
};

use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct ManagerAgentService {
    pub store: Store,
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
            println!("Error parsing jobspec: {:?}", e);
            Status::internal("Error parsing jobspec")
        })?;

        let force_reschedule = inner.force_reschedule;
        let submit_outcome = self.store.handle_job_submit(&config, &subs).map_err(|e| {
            error!("Error running job: {:?}", e);
            Status::internal("Error running job")
        })?;

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
                .map_err(|e| {
                    error!("Error enqueueing eval: {:?}", e);
                    Status::internal("Error running job")
                })?;
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

        let job = self.store.get_job(&name).map_err(|e| {
            error!("Error getting job: {:?}", e);
            Status::internal("Error getting job")
        })?;

        let reply = GetJobInfoReply { job: Some(job) };

        Ok(Response::new(reply))
    }

    async fn get_job_spec(
        &self,
        request: Request<GetJobSpecRequest>,
    ) -> Result<Response<GetJobSpecReply>, Status> {
        let name = request.into_inner().name;

        let config = self.store.get_job_config(&name).map_err(|e| {
            error!("Error getting job config: {:?}", e);
            Status::internal("Error getting job config")
        })?;

        let submissions = self
            .store
            .get_submissions_for_config(&config.id())
            .map_err(|e| {
                error!("Error getting submissions: {:?}", e);
                Status::internal("Error getting submissions")
            })?;

        let reply = GetJobSpecReply { submissions };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn list_jobs(
        &self,
        _request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsReply>, Status> {
        let jobs = self.store.get_jobs().map_err(|e| {
            error!("Error getting jobs: {:?}", e);
            Status::internal("Error getting jobs")
        })?;

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
            .map_err(|e| {
                error!("Error fetching job config for stop {}: {:?}", name, e);
                Status::internal("Error stopping job")
            })?
            .id();

        let _ = self.store.stop_job(&name).map_err(|e| {
            error!("Error stopping job: {:?}", e);
            Status::internal("Error stopping job")
        })?;

        self.store
            .enqueue_eval(
                "job_stop",
                Some(&name),
                Some(&job_config_id),
                None,
                Some(priority),
            )
            .map_err(|e| {
                error!("Error enqueueing eval: {:?}", e);
                Status::internal("Error stopping job")
            })?;

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

        let _ = self.store.destroy_job(&name).map_err(|e| {
            error!("Error deleting job: {:?}", e);
            Status::internal("Error deleting job")
        })?;

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
        let id = request.into_inner().id;

        self.store.update_node_heartbeat(id.clone()).map_err(|e| {
            error!("Error updading node heartbeat. Node ID: {}: {:?}", id, e);
            Status::internal("Error updading node heartbeat")
        })?;

        let reply = HeartbeatReply {
            message: "ack".into(),
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
            .map_err(|e| {
                error!(
                    "Error updading node health checks. Node ID: {}: {:?}",
                    id, e
                );
                Status::internal("Error updading node health checks")
            })?;

        let reply = HealthCheckReply {
            message: "ack".into(),
        };

        Ok(Response::new(reply))
    }

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
            .map_err(|e| {
                error!("Error getting worker type {}: {:?}", worker_type.clone(), e);
                Status::internal("Error getting worker type")
            })?;

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
            availability: "availabile".to_string(),
            pool: request.pool,
            last_heartbeat: String::new(),
            health_checks: request.health_checks,
        };

        self.store.insert_node(node_msg).map_err(|e| {
            error!("Error registering worker: {:?}", e);
            Status::internal("Error registering worker")
        })?;

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
        let nodes = self.store.get_nodes().map_err(|e| {
            error!("Error getting nodes: {:?}", e);
            Status::internal("Error getting nodes")
        })?;

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
        let node = self.store.get_node(&id).map_err(|e| {
            error!("Error getting nodes: {:?}", e);
            Status::internal(format!("Could not find node {id}"))
        })?;

        let reply = GetNodeReply {
            node: Some(node.into()),
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
            .expect("Can't find or update node");
        let allocs = self.store.allocs_for_node(&id).map_err(|e| {
            error!("Error getting allocations for node {}: {:?}", id, e);
            Status::internal("Error getting allocations for node")
        })?;
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
                .map_err(|e| {
                    error!("Error enqueueing eval for node {}: {:?}", id, e);
                    Status::internal("Error enqueueing eval for node")
                })?;
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
        let configs = self.store.get_manager_configs().map_err(|e| {
            error!("Error getting manager configs: {:?}", e);
            Status::internal("Error getting manager configs")
        })?;

        let reply = GetManagerConfigsReply { configs };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_ledger(
        &self,
        _request: Request<GetLedgerRequest>,
    ) -> Result<Response<GetLedgerReply>, Status> {
        let ledger = self.store.get_ledger().map_err(|e| {
            error!("Error getting ledger: {:?}", e);
            Status::internal("Error getting ledger")
        })?;

        let nodes: Vec<LedgerNode> = ledger.into_iter().map(Into::into).collect();

        let reply: GetLedgerReply = GetLedgerReply { nodes };

        Ok(Response::new(reply))
    }
}
