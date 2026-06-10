use crate::prelude::*;
use crate::structs::{
    worker_server::Worker, PostWorkReply, PostWorkRequest, RestartWorkerReply, RestartWorkerRequest,
};
use crate::worker::task_health::spawn_health_monitor;
use crate::worker::task_manager::WorkerTaskManager;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

pub struct WorkerDaemonService {
    pub task_manager: Arc<Mutex<WorkerTaskManager>>,
}
#[tonic::async_trait]
impl Worker for WorkerDaemonService {
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

    #[instrument(err, skip_all)]
    async fn restart_worker(
        &self,
        request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        let node_id = request.into_inner().node_id;
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
