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

/// The largest checkpoint the worker accepts: the state quota of one
/// session plus room for its metadata and the format's framing. A
/// checkpoint holds `legion:isolate/state` and session metadata only
/// (brief item 15), so anything larger is a misbehaving host.
pub const MAX_CHECKPOINT_BYTES: u64 = super::supervisor::STATE_QUOTA_BYTES + (256 << 10);

/// The most lines one `Log` batch may carry; the rest are dropped.
pub const MAX_LOG_LINES_PER_BATCH: usize = 4096;
/// Guest output one host may write to task logs per second, sustained
/// (bytes); bursts up to [`LOG_BURST_BYTES`]. Beyond it lines are dropped
/// and a marker line says how many.
pub const LOG_BYTES_PER_SEC: f64 = (1 << 20) as f64;
pub const LOG_BURST_BYTES: f64 = (8 << 20) as f64;

/// A token bucket over the bytes one host logs.
#[derive(Debug)]
struct LogBudget {
    tokens: f64,
    at: std::time::Instant,
    dropped: u64,
}

impl LogBudget {
    fn new() -> Self {
        Self {
            tokens: LOG_BURST_BYTES,
            at: std::time::Instant::now(),
            dropped: 0,
        }
    }

    /// Whether `bytes` more may be written now.
    fn take(&mut self, bytes: usize) -> bool {
        let now = std::time::Instant::now();
        let refill = now.duration_since(self.at).as_secs_f64() * LOG_BYTES_PER_SEC;
        self.tokens = (self.tokens + refill).min(LOG_BURST_BYTES);
        self.at = now;
        if self.tokens >= bytes as f64 {
            self.tokens -= bytes as f64;
            true
        } else {
            self.dropped += 1;
            false
        }
    }
}

/// The worker end of one host's worker socket.
#[derive(Clone)]
pub struct WorkerService {
    supervisor: Weak<IsolateSupervisor>,
    cordon: String,
    logs: Arc<Mutex<LogBudget>>,
}

impl WorkerService {
    pub fn new(supervisor: Weak<IsolateSupervisor>, cordon: &str) -> Self {
        Self {
            supervisor,
            cordon: cordon.to_string(),
            logs: Arc::new(Mutex::new(LogBudget::new())),
        }
    }

    fn supervisor(&self) -> Result<Arc<IsolateSupervisor>, Status> {
        self.supervisor
            .upgrade()
            .ok_or_else(|| Status::unavailable("the worker is shutting down"))
    }

    /// The task an origin names, when it belongs to this host's cordon.
    fn task(
        &self,
        origin: Option<&Origin>,
    ) -> Result<(Arc<IsolateSupervisor>, IsolateTaskRef), Status> {
        let supervisor = self.supervisor()?;
        let origin = origin.ok_or_else(|| Status::invalid_argument("missing origin"))?;
        let task = supervisor.task_ref(&self.cordon, origin).ok_or_else(|| {
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

fn lock_budget(budget: &Mutex<LogBudget>) -> std::sync::MutexGuard<'_, LogBudget> {
    budget.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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
        Ok(Response::new(
            supervisor.egress_backend().fetch(&task, request).await,
        ))
    }

    async fn vm(&self, request: Request<VmRequest>) -> Result<Response<VmReply>, Status> {
        // ACL: node-local, as `egress`.
        let request = request.into_inner();
        let (supervisor, task) = self.task(request.origin.as_ref())?;
        // Children are owned by the isolate's id: the host must name an
        // isolate the worker issued it for this very job and task.
        if !supervisor.issued(&self.cordon, &task) {
            warn!(
                cordon = %self.cordon,
                job_id = %task.job_id,
                task = %task.task_name,
                "An isolate host named an isolate the worker did not give it; VM call refused"
            );
            return Err(Status::permission_denied(format!(
                "{} {} of task {}/{} is not an isolate this worker gave the host",
                if task.session { "session" } else { "invocation" },
                task.isolate_id,
                task.job_id,
                task.task_name
            )));
        }
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
        // The header comes first and is checked (origin in this cordon, a
        // session isolate, a declared size within the cap) before any body
        // byte is kept; the body may not exceed what the header declared.
        let mut stream = request.into_inner();
        let mut found: Option<(CheckpointHeader, Arc<IsolateSupervisor>, IsolateTaskRef)> = None;
        let mut body = Vec::new();
        while let Some(chunk) = stream.message().await? {
            match chunk.part {
                Some(checkpoint_chunk::Part::Header(first)) if found.is_none() => {
                    let (supervisor, task) = self.task(first.origin.as_ref())?;
                    if !task.session || !supervisor.knows_session(&task.isolate_id) {
                        return Err(Status::invalid_argument(
                            "only a session isolate this worker holds has a checkpoint",
                        ));
                    }
                    if first.bytes > MAX_CHECKPOINT_BYTES {
                        return Err(Status::resource_exhausted(format!(
                            "a checkpoint of {} bytes is over the {MAX_CHECKPOINT_BYTES}-byte \
                             maximum",
                            first.bytes
                        )));
                    }
                    body.reserve(first.bytes as usize);
                    found = Some((first, supervisor, task));
                }
                Some(checkpoint_chunk::Part::Header(_)) => {
                    return Err(Status::invalid_argument("a second checkpoint header"))
                }
                Some(checkpoint_chunk::Part::Data(data)) => {
                    let Some((header, ..)) = found.as_ref() else {
                        return Err(Status::invalid_argument(
                            "checkpoint data before its header",
                        ));
                    };
                    if body.len() as u64 + data.len() as u64 > header.bytes {
                        return Err(Status::resource_exhausted(format!(
                            "the checkpoint is larger than the {} bytes its header declared",
                            header.bytes
                        )));
                    }
                    body.extend_from_slice(&data);
                }
                None => {}
            }
        }
        let (header, supervisor, task) =
            found.ok_or_else(|| Status::invalid_argument("an empty checkpoint"))?;
        if body.len() as u64 != header.bytes {
            return Err(Status::invalid_argument(format!(
                "the checkpoint is {} bytes, its header declared {}",
                body.len(),
                header.bytes
            )));
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

    async fn log(
        &self,
        request: Request<Streaming<LogBatch>>,
    ) -> Result<Response<LogReply>, Status> {
        // ACL: node-local, as `egress`.
        // Guest output goes to the envelope allocation's task log, each line
        // prefixed with its isolate (and tenant); see `guest_logs`.
        let mut stream = request.into_inner();
        while let Some(batch) = stream.message().await? {
            let (supervisor, task) = match self.task(batch.origin.as_ref()) {
                Ok(found) => found,
                Err(_) => continue,
            };
            let Some(logs) = supervisor.guest_logs() else {
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
                continue;
            };
            // Bounded: lines per batch, and bytes per host per second.
            let mut lines = batch.lines;
            let mut dropped = lines.len().saturating_sub(MAX_LOG_LINES_PER_BATCH) as u64;
            lines.truncate(MAX_LOG_LINES_PER_BATCH);
            {
                let mut budget = lock_budget(&self.logs);
                let before = budget.dropped;
                lines.retain(|line| {
                    budget.take(line.line.len().min(super::guest_logs::MAX_LINE_BYTES) + 64)
                });
                dropped += budget.dropped - before;
            }
            if dropped > 0 {
                lines.push(crate::structs::isolate_host::LogLine {
                    stderr: true,
                    line: format!(
                        "{dropped} lines dropped: over the isolate host's log rate or batch limit"
                    )
                    .into_bytes(),
                    unix_ms: 0,
                });
            }
            let prefix = super::guest_logs::line_prefix(&task.isolate_id, &task.tenant);
            let envelope = task.envelope_allocation_id;
            let written =
                tokio::task::spawn_blocking(move || logs.append(envelope, &prefix, &lines))
                    .await;
            if let Ok(Err(err)) = written {
                warn!(
                    job_id = %task.job_id,
                    task = %task.task_name,
                    allocation_id = envelope,
                    error = %format!("{err:#}"),
                    "Could not append isolate output to the task log"
                );
            }
        }
        Ok(Response::new(LogReply {}))
    }
}
