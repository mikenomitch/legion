use crate::prelude::*;
use crate::structs::{
    worker_server::Worker, PostWorkReply, PostWorkRequest, RestartWorkerReply, RestartWorkerRequest,
};
use crate::worker::task_manager::WorkerTaskManager;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tonic::{Request, Response, Status};

pub struct WorkerAgentService {
    pub task_manager: Arc<Mutex<WorkerTaskManager>>,
    pub restart_tx: watch::Sender<bool>,
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

        {
            let mut manager = self
                .task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?;
            if let Err(err) = manager.run_task_allocation(&allocation) {
                manager.record_failure(allocation.allocation_id, err.to_string());
                return Err(Status::internal(format!("Failed to start task: {err}")));
            }
        }

        let reply = PostWorkReply {
            message: "Task accepted".into(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(err, skip_all)]
    async fn restart_worker(
        &self,
        _request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        let _ = self.restart_tx.send(true);
        Ok(Response::new(RestartWorkerReply {
            message: "Restart scheduled".into(),
        }))
    }
}
