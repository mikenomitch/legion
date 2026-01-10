use crate::prelude::*;
use crate::structs::{worker_server::Worker, PostWorkReply, PostWorkRequest};
use crate::worker::task_manager::WorkerTaskManager;
use tonic::{Request, Response, Status};
use std::sync::{Arc, Mutex};

pub struct WorkerAgentService {
    pub task_manager: Arc<Mutex<WorkerTaskManager>>,
}
#[tonic::async_trait]
impl Worker for WorkerAgentService {
    #[instrument(err, skip_all)]
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        info!("Got a request from {:?}", request.remote_addr());
        let allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("Missing allocation"))?;
        let task_config: serde_json::Value =
            serde_json::from_str(&allocation.task_config).map_err(|_| {
                Status::invalid_argument("Invalid task configuration JSON")
            })?;

        let task = crate::structs::TaskConfig {
            name: allocation.task_name,
            count: 1,
            kind: allocation.task_kind,
            config: task_config,
            resources: serde_json::json!({}),
            constraints: serde_json::json!({}),
            affinities: serde_json::json!({}),
        };
        let alloc = crate::structs::Allocation {
            id: allocation.allocation_id,
            status: "running".into(),
            resources: serde_json::json!({}),
            node_id: allocation.node_id,
            job_config_id: allocation.job_config_id,
            task_config_id: allocation.task_config_id,
            job_id: allocation.job_id,
        };

        {
            let mut manager = self.task_manager.lock().map_err(|_| {
                Status::internal("Task manager lock poisoned")
            })?;
            manager.run_allocation(&alloc, &task).map_err(|e| {
                Status::internal(format!("Failed to start task: {e}"))
            })?;
        }

        let reply = PostWorkReply {
            message: "Task accepted".into(),
        };

        Ok(Response::new(reply))
    }
}
