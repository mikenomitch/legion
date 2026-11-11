//! `IsolateWorker`: what a host asks the worker for (outbound HTTP, child
//! VMs, checkpoints, logs). One instance per host process, bound to the
//! cordon the worker started that process for: every request's `Origin`
//! must name a task of that cordon, so a compromised host can act only as
//! the jobs it already serves.

use super::supervisor::IsolateSupervisor;
use super::IsolateTaskRef;
use crate::prelude::*;
use crate::structs::isolate_host::isolate_worker_server::IsolateWorker;
use crate::structs::isolate_host::{
    checkpoint_chunk, CheckpointChunk, CheckpointHeader, EgressReply, EgressRequest,
    LoadCheckpointRequest, LogBatch, LogReply, Origin, SaveCheckpointReply, VmReply, VmRequest,
};
use std::sync::Weak;
use tonic::{Request, Response, Status, Streaming};

/// Bytes per chunk when streaming a checkpoint back.
const CHUNK_BYTES: usize = 1 << 20;

/// The largest checkpoint the worker accepts.
pub const MAX_CHECKPOINT_BYTES: u64 = 4 << 30;

/// The worker end of one host's worker socket.
#[derive(Clone)]
pub struct WorkerService {
    supervisor: Weak<IsolateSupervisor>,
    cordon: String,
}

impl WorkerService {
    pub fn new(supervisor: Weak<IsolateSupervisor>, cordon: &str) -> Self {
        Self {
            supervisor,
            cordon: cordon.to_string(),
        }
    }

    fn supervisor(&self) -> Result<Arc<IsolateSupervisor>, Status> {
        self.supervisor
            .upgrade()
            .ok_or_else(|| Status::unavailable("the worker is shutting down"))
    }

    /// The task an origin names, when it belongs to this host's cordon.
    fn task(&self, origin: Option<&Origin>) -> Result<(Arc<IsolateSupervisor>, IsolateTaskRef), Status> {
        let supervisor = self.supervisor()?;
        let origin = origin.ok_or_else(|| Status::invalid_argument("missing origin"))?;
        let task = supervisor
            .task_ref(&self.cordon, origin)
            .ok_or_else(|| {
                warn!(
                    cordon = %self.cordon,
                    job_id = %origin.job_id,
                    task = %origin.task_name,
                    "An isolate host asked for a task outside its cordon; refused"
                );
                Status::permission_denied(format!(
                    "task {}/{} is not served by cordon {}",
                    origin.job_id, origin.task_name, self.cordon
                ))
            })?;
        Ok((supervisor, task))
    }
}

type ChunkStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<CheckpointChunk, Status>> + Send>>;

#[tonic::async_trait]
impl IsolateWorker for WorkerService {
    async fn egress(
        &self,
        request: Request<EgressRequest>,
    ) -> Result<Response<EgressReply>, Status> {
        // ACL: node-local; the socket is the credential, the origin is
        // checked against the cordon.
        let request = request.into_inner();
        let (supervisor, task) = self.task(request.origin.as_ref())?;
        Ok(Response::new(supervisor.egress_backend().fetch(&task, request).await))
    }

    async fn vm(&self, request: Request<VmRequest>) -> Result<Response<VmReply>, Status> {
        // ACL: node-local, as `egress`.
        let request = request.into_inner();
        let (supervisor, task) = self.task(request.origin.as_ref())?;
        let vm_task = supervisor.vm_task_of(&task.job_id, &task.task_name);
        Ok(Response::new(
            supervisor
                .vm_backend()
                .call(&task, vm_task.as_deref(), request)
                .await,
        ))
    }

    async fn save_checkpoint(
        &self,
        request: Request<Streaming<CheckpointChunk>>,
    ) -> Result<Response<SaveCheckpointReply>, Status> {
        // ACL: node-local, as `egress`.
        let mut stream = request.into_inner();
        let mut header: Option<CheckpointHeader> = None;
        let mut body = Vec::new();
        while let Some(chunk) = stream.message().await? {
            match chunk.part {
                Some(checkpoint_chunk::Part::Header(first)) if header.is_none() => {
                    if first.bytes > MAX_CHECKPOINT_BYTES {
                        return Err(Status::resource_exhausted(format!(
                            "a checkpoint of {} bytes is over the {MAX_CHECKPOINT_BYTES}-byte \
                             maximum",
                            first.bytes
                        )));
                    }
                    body.reserve(first.bytes.min(64 << 20) as usize);
                    header = Some(first);
                }
                Some(checkpoint_chunk::Part::Header(_)) => {
                    return Err(Status::invalid_argument("a second checkpoint header"))
                }
                Some(checkpoint_chunk::Part::Data(data)) => {
                    if header.is_none() {
                        return Err(Status::invalid_argument("checkpoint data before its header"));
                    }
                    if body.len() as u64 + data.len() as u64 > MAX_CHECKPOINT_BYTES {
                        return Err(Status::resource_exhausted("the checkpoint is too large"));
                    }
                    body.extend_from_slice(&data);
                }
                None => {}
            }
        }
        let header = header.ok_or_else(|| Status::invalid_argument("an empty checkpoint"))?;
        let (supervisor, task) = self.task(header.origin.as_ref())?;
        if !task.session {
            return Err(Status::invalid_argument(
                "only a session isolate has a checkpoint",
            ));
        }
        let generation = supervisor.save_checkpoint(&task, &header, body).await?;
        Ok(Response::new(SaveCheckpointReply { generation }))
    }

    type LoadCheckpointStream = ChunkStream;

    async fn load_checkpoint(
        &self,
        request: Request<LoadCheckpointRequest>,
    ) -> Result<Response<Self::LoadCheckpointStream>, Status> {
        // ACL: node-local, as `egress`.
        let request = request.into_inner();
        let (supervisor, task) = self.task(request.origin.as_ref())?;
        let (header, body) = supervisor.load_checkpoint(&task).await?;
        let mut chunks = vec![Ok(CheckpointChunk {
            part: Some(checkpoint_chunk::Part::Header(header)),
        })];
        for piece in body.chunks(CHUNK_BYTES) {
            chunks.push(Ok(CheckpointChunk {
                part: Some(checkpoint_chunk::Part::Data(piece.to_vec())),
            }));
        }
        Ok(Response::new(Box::pin(futures::stream::iter(chunks))))
    }

    async fn log(&self, request: Request<Streaming<LogBatch>>) -> Result<Response<LogReply>, Status> {
        // ACL: node-local, as `egress`.
        let mut stream = request.into_inner();
        while let Some(batch) = stream.message().await? {
            let (_, task) = match self.task(batch.origin.as_ref()) {
                Ok(found) => found,
                Err(_) => continue,
            };
            for line in batch.lines {
                debug!(
                    job_id = %task.job_id,
                    task = %task.task_name,
                    isolate_id = %task.isolate_id,
                    stream = if line.stderr { "stderr" } else { "stdout" },
                    "{}",
                    String::from_utf8_lossy(&line.line).trim_end()
                );
            }
        }
        Ok(Response::new(LogReply {}))
    }
}
