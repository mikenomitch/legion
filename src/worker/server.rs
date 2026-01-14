use crate::prelude::*;
use crate::structs::{worker_server::Worker, PostWorkReply, PostWorkRequest};
use crate::worker::task_health::spawn_health_monitor;
use crate::worker::task_manager::WorkerTaskManager;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

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

        let started = {
            let mut manager = self
                .task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?;
            match manager.run_task_allocation(&allocation) {
                Ok(started) => started,
                Err(err) => {
                    manager.record_failure(allocation.allocation_id, err.to_string());
                    return Err(Status::internal(format!("Failed to start task: {err}")));
                }
            }
        };

        spawn_health_monitor(self.task_manager.clone(), started.0, started.1);

        let reply = PostWorkReply {
            message: "Task accepted".into(),
        };

        Ok(Response::new(reply))
    }
}
