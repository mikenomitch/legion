use crate::prelude::*;
use crate::structs::{
    worker_server::Worker, ExecClientFrame, GetTaskLogsReply, GetTaskLogsRequest, PostWorkReply,
    PostWorkRequest, RestartWorkerReply, RestartWorkerRequest, TaskLogStream,
};
use crate::worker::exec::{handle_exec_task, ExecTaskStream};
use crate::worker::identity::WorkerIdentityManager;
use crate::worker::task_manager::{launch_task_allocation, WorkerTaskManager};
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status, Streaming};

pub struct WorkerDaemonService {
    pub task_manager: Arc<Mutex<WorkerTaskManager>>,
    /// Workload-identity delivery/refresh; `None` when this worker runs
    /// without a manager identity client (tests).
    pub identity: Option<Arc<WorkerIdentityManager>>,
}
#[tonic::async_trait]
impl Worker for WorkerDaemonService {
    #[instrument(err, skip_all)]
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        info!("Got a request from {:?}", request.remote_addr());
        let mut allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("Missing allocation"))?;

        // Fetch + deliver identity tokens before the task starts (env tokens
        // must be in the env map at exec time). A task must not start without
        // an identity it declared.
        let prepared = match self.identity.as_ref() {
            Some(identity) => match identity.prepare_task_allocation(&mut allocation).await {
                Ok(prepared) => prepared,
                Err(err) => {
                    let reason = format!("Failed to deliver workload identities: {err}");
                    self.task_manager
                        .lock()
                        .map_err(|_| Status::internal("Task manager lock poisoned"))?
                        .record_failure(allocation.allocation_id, reason.clone());
                    return Err(Status::internal(reason));
                }
            },
            None => Vec::new(),
        };

        let allocation_id = allocation.allocation_id;

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

        // Refresh loops are keyed by allocation id; identity_manager.retain
        // (run each heartbeat) prunes them if the allocation goes away.
        if let Some(identity) = self.identity.as_ref() {
            identity.spawn_refreshers(allocation_id, prepared, self.task_manager.clone());
        }

        let reply = PostWorkReply {
            message: "Task accepted".into(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(err, skip_all)]
    async fn get_task_logs(
        &self,
        request: Request<GetTaskLogsRequest>,
    ) -> Result<Response<GetTaskLogsReply>, Status> {
        let req = request.into_inner();
        let streams = {
            let manager = self
                .task_manager
                .lock()
                .map_err(|_| Status::internal("Task manager lock poisoned"))?;
            manager
                .read_allocation_logs(
                    req.allocation_id,
                    &req.stream,
                    req.offset,
                    req.max_bytes,
                    req.tail,
                )
                .map_err(|err| Status::internal(format!("Failed to read task logs: {err}")))?
        };

        let streams = streams
            .into_iter()
            .map(|stream| TaskLogStream {
                stream: stream.stream,
                content: stream.content,
                size_bytes: stream.size_bytes,
                truncated: stream.truncated,
                start_offset: stream.start_offset,
                next_offset: stream.next_offset,
            })
            .collect();

        Ok(Response::new(GetTaskLogsReply { streams }))
    }

    type ExecTaskStream = ExecTaskStream;

    #[instrument(skip_all)]
    async fn exec_task(
        &self,
        request: Request<Streaming<ExecClientFrame>>,
    ) -> Result<Response<Self::ExecTaskStream>, Status> {
        let stream = handle_exec_task(self.task_manager.clone(), request.into_inner()).await?;
        Ok(Response::new(stream))
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
