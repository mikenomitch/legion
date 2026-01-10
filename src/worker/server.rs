use crate::prelude::*;

use crate::structs::{worker_server::Worker, PostWorkReply, PostWorkRequest};
use crate::task::TaskAllocation;
use crate::worker::task_manager::TaskManager;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct WorkerAgentService {
    task_manager: Arc<TaskManager>,
}

impl WorkerAgentService {
    pub fn new(task_manager: Arc<TaskManager>) -> Self {
        WorkerAgentService { task_manager }
    }
}
#[tonic::async_trait]
impl Worker for WorkerAgentService {
    #[instrument(err, skip_all)]
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        info!("Got a work request from {:?}", request.remote_addr());
        let work = request
            .into_inner()
            .work
            .context("missing work payload")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let allocation =
            TaskAllocation::from_work(work).map_err(|e| Status::invalid_argument(e.to_string()))?;

        info!(
            allocation_id = allocation.allocation_id,
            task_name = allocation.task_name,
            task_kind = allocation.task_kind,
            "Received task allocation"
        );

        self.task_manager
            .prepare_and_start(allocation)
            .map_err(|e| Status::internal(e.to_string()))?;

        let reply = PostWorkReply {
            message: "task started".into(),
        };

        Ok(Response::new(reply))
    }
}
