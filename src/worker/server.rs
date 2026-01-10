use crate::prelude::*;

use crate::structs::{
    worker_server::Worker, LedgerAllocation, PostWorkReply, PostWorkRequest, TaskConfigMsg,
};
use crate::worker::task::TaskManager;
use tonic::{Request, Response, Status};

pub struct WorkerAgentService {
    pub task_manager: Arc<TaskManager>,
}
#[tonic::async_trait]
impl Worker for WorkerAgentService {
    #[instrument(err, skip_all)]
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        info!("Got a request from {:?}", request.remote_addr());
        let work = request
            .into_inner()
            .work
            .ok_or_else(|| Status::invalid_argument("missing work payload"))?;
        let allocation: LedgerAllocation = work
            .allocation
            .ok_or_else(|| Status::invalid_argument("missing allocation"))?;
        let task_config: TaskConfigMsg = work
            .task
            .ok_or_else(|| Status::invalid_argument("missing task config"))?;

        let allocation = allocation
            .into_allocation()
            .map_err(|e| Status::internal(format!("invalid allocation: {e}")))?;
        let task_config = task_config
            .into_task_config()
            .map_err(|e| Status::internal(format!("invalid task config: {e}")))?;

        self.task_manager
            .prepare_and_start(allocation, task_config)
            .map_err(|e| Status::internal(format!("failed to start task: {e}")))?;

        let reply = PostWorkReply {
            message: "I got work yay!".into(),
        };

        Ok(Response::new(reply))
    }
}
