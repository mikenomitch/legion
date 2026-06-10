use crate::prelude::*;
use crate::structs::{
    worker_server::Worker, PostWorkReply, PostWorkRequest, RestartWorkerReply, RestartWorkerRequest,
};
use crate::worker::task_manager::{launch_task_allocation, WorkerTaskManager};
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

        // Synchronous failures (bad config, unknown kind) surface here;
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
