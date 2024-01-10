use crate::prelude::*;

use crate::storage::store::Store;
use crate::structs::manager_server::Manager;
use crate::structs::{
    GetJobStatusReply, GetJobStatusRequest, GetNodeReply, GetNodeRequest, GetNodesReply,
    GetNodesRequest, HealthCheckReply, HealthCheckRequest, HeartbeatReply, HeartbeatRequest,
    JobConfig, ListJobsReply, ListJobsRequest, NodeMsg, RunJobReply, RunJobRequest,
    UpdateNodeDynamicReply, UpdateNodeDynamicRequest, WorkerConfig, WorkerPreRegisterReply,
    WorkerPreRegisterRequest, WorkerRegisterReply, WorkerRegisterRequest,
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
        let js = request.into_inner().parsed_jobspec;
        let config: JobConfig =
            serde_json::from_str(&js).map_err(|e| Status::internal("Error parsing jobspec"))?;

        self.store.insert_job(&config).map_err(|e| {
            error!("Error saving job: {:?}", e);
            Status::internal("Error saving job")
        })?;

        let reply = RunJobReply {
            message: "You are in!".into(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusReply>, Status> {
        let name = request.into_inner().name;

        let job = self.store.get_job(&name).map_err(|e| {
            error!("Error getting job: {:?}", e);
            Status::internal("Error getting job")
        })?;

        let reply = GetJobStatusReply {
            message: format!("Got the job: {:?}", job.name),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn list_jobs(
        &self,
        _request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsReply>, Status> {
        let reply = ListJobsReply { jobs: vec![] };

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

        let mut attrs = HashMap::new();
        for (key, value) in &worker_config.attributes {
            match serde_json::to_string(value).ok() {
                Some(val_str) => {
                    attrs.insert(key.to_string(), val_str);
                }
                None => {
                    error!("Error serializing value for key: {}", key);
                }
            }
        }

        let config = WorkerConfig {
            name: worker_type,
            pool: worker_config.pool,
            additional_node_checks: worker_config.additional_node_checks,
            attributes: worker_config.attributes,
            name_prefix: worker_config.name_prefix,
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
            message: "You are in!".into(),
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
        let reply = UpdateNodeDynamicReply {
            node: Some(node.into()),
        };

        Ok(Response::new(reply))
    }
}
