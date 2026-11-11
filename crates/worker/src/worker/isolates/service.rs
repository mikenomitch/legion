//! `IsolateWorker`: what a host asks the worker for (outbound HTTP, child
//! VMs, checkpoints, logs). One instance per host process, bound to the
//! cordon the worker started that process for: every request's `Origin`
//! must name a task of that cordon, so a compromised host can act only as
//! the jobs it already serves.
//!
//! A host is not trusted. Every `Origin` is checked by [`check_origin`]
//! before anything else looks at it (bounded ids in the alphabet the
//! worker mints, a valid tenant key), messages and concurrent calls are
//! bounded per host (`host::HostCallLimits`), request bodies held across
//! awaits are charged to a per-host byte budget, and log lines are
//! attributed only to isolates the worker gave the host, with the tenant
//! the worker recorded, never the one the host claims.

use super::supervisor::IsolateSupervisor;
use super::IsolateTaskRef;
use crate::prelude::*;
use crate::structs::isolate_host::isolate_worker_server::IsolateWorker;
use crate::structs::isolate_host::{
    checkpoint_chunk, CheckpointChunk, CheckpointHeader, EgressReply, EgressRequest,
    LoadCheckpointRequest, LogBatch, LogReply, Origin, SaveCheckpointReply, VmReply, VmRequest,
};
use std::collections::HashMap;
use std::sync::Weak;
use tonic::{Request, Response, Status, Streaming};

/// Bytes per chunk when streaming a checkpoint back.
const CHUNK_BYTES: usize = 1 << 20;

/// The largest checkpoint the worker accepts: the state quota of one
/// session plus room for its metadata and the format's framing. A
/// checkpoint holds `legion:isolate/state` and session metadata only
/// (brief item 15), so anything larger is a misbehaving host.
pub const MAX_CHECKPOINT_BYTES: u64 = super::supervisor::STATE_QUOTA_BYTES + (256 << 10);

/// The most lines one `Log` batch may carry after embedded newlines are
/// split; the rest are dropped.
pub const MAX_LOG_LINES_PER_BATCH: usize = super::guest_logs::MAX_LINES_PER_BATCH;
/// Guest output one (job, task) may write to task logs per second,
/// sustained (rendered bytes, prefix included); bursts up to
/// [`TASK_LOG_BURST_BYTES`]. Beyond it its lines are dropped and a marker
/// line says how many; other tasks of the host are not affected.
pub const TASK_LOG_BYTES_PER_SEC: f64 = (512 << 10) as f64;
pub const TASK_LOG_BURST_BYTES: f64 = (4 << 20) as f64;
/// The ceiling for all of one host's tasks together.
pub const HOST_LOG_BYTES_PER_SEC: f64 = (2 << 20) as f64;
pub const HOST_LOG_BURST_BYTES: f64 = (16 << 20) as f64;
/// The most (job, task) buckets one host keeps; idle ones go first.
pub const MAX_LOG_BUDGETS: usize = 1024;

/// The longest isolate id a host may name: the ids the worker mints are
/// far shorter (session and run ids are at most 128 bytes).
pub const MAX_ISOLATE_ID_BYTES: usize = 256;
/// The longest job id and task name a host may name.
pub const MAX_ORIGIN_JOB_BYTES: usize = 512;
pub const MAX_ORIGIN_TASK_BYTES: usize = 256;
/// The longest engine version and module id a checkpoint header may carry
/// (they are kept in the session book).
pub const MAX_CHECKPOINT_ENGINE_BYTES: usize = 128;
pub const MAX_CHECKPOINT_MODULE_BYTES: usize = 256;

/// Check an `Origin` before anything else uses it: job id and task name
/// bounded, the isolate id at most [`MAX_ISOLATE_ID_BYTES`] of
/// `[A-Za-z0-9._-]` (every id the worker mints or derives: run ids,
/// session ids, keyed instance ids), the tenant empty or a valid tenant key
/// (`validate_tenant_key`: at most 128 bytes of printable ASCII, no
/// spaces). Errors never echo the host's strings.
pub fn check_origin(origin: Option<&Origin>) -> Result<&Origin, Status> {
    let origin = origin.ok_or_else(|| Status::invalid_argument("missing origin"))?;
    if origin.job_id.is_empty() || origin.job_id.len() > MAX_ORIGIN_JOB_BYTES {
        return Err(Status::invalid_argument(format!(
            "the origin's job id is {} bytes; 1 to {MAX_ORIGIN_JOB_BYTES} allowed",
            origin.job_id.len()
        )));
    }
    if origin.task_name.is_empty() || origin.task_name.len() > MAX_ORIGIN_TASK_BYTES {
        return Err(Status::invalid_argument(format!(
            "the origin's task name is {} bytes; 1 to {MAX_ORIGIN_TASK_BYTES} allowed",
            origin.task_name.len()
        )));
    }
    if origin.isolate_id.len() > MAX_ISOLATE_ID_BYTES
        || !origin
            .isolate_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(Status::invalid_argument(format!(
            "the origin's isolate id ({} bytes) is not an id this worker issues: at most \
             {MAX_ISOLATE_ID_BYTES} bytes of [A-Za-z0-9._-]",
            origin.isolate_id.len()
        )));
    }
    if !origin.tenant.is_empty() {
        crate::structs::isolate::validate_tenant_key(&origin.tenant).map_err(|_| {
            Status::invalid_argument(format!(
                "the origin's tenant ({} bytes) is not a valid tenant key",
                origin.tenant.len()
            ))
        })?;
    }
    Ok(origin)
}

/// A checkpoint header's own strings, bounded before they are kept.
fn check_checkpoint_header(header: &CheckpointHeader) -> Result<(), Status> {
    let printable = |s: &str| s.bytes().all(|b| b.is_ascii_graphic());
    if header.engine_version.len() > MAX_CHECKPOINT_ENGINE_BYTES
        || !printable(&header.engine_version)
    {
        return Err(Status::invalid_argument(format!(
            "the checkpoint's engine version ({} bytes) is not at most \
             {MAX_CHECKPOINT_ENGINE_BYTES} bytes of printable ASCII",
            header.engine_version.len()
        )));
    }
    if header.module_id.len() > MAX_CHECKPOINT_MODULE_BYTES || !printable(&header.module_id) {
        return Err(Status::invalid_argument(format!(
            "the checkpoint's module id ({} bytes) is not at most \
             {MAX_CHECKPOINT_MODULE_BYTES} bytes of printable ASCII",
            header.module_id.len()
        )));
    }
    Ok(())
}

/// A token bucket over rendered log bytes.
#[derive(Debug, Clone)]
struct LogBucket {
    tokens: f64,
    at: std::time::Instant,
    rate: f64,
    burst: f64,
}

impl LogBucket {
    fn new(rate: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            at: std::time::Instant::now(),
            rate,
            burst,
        }
    }

    fn refill(&mut self, now: std::time::Instant) {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.at = now;
    }

    fn full(&self) -> bool {
        self.tokens >= self.burst
    }
}

/// One host's log budgets: a bucket per (job, task), and the host's
/// ceiling over all of them (security review N5: one tenant's flood must
/// not drop another task's lines). The per-task map is bounded: an idle
/// task's bucket (full again) is evicted first, then the least recently
/// used one.
#[derive(Debug)]
struct LogBudgets {
    host: LogBucket,
    tasks: HashMap<(String, String), LogBucket>,
}

impl LogBudgets {
    fn new() -> Self {
        Self {
            host: LogBucket::new(HOST_LOG_BYTES_PER_SEC, HOST_LOG_BURST_BYTES),
            tasks: HashMap::new(),
        }
    }

    /// Whether `bytes` more of `task`'s lines may be written now; charges
    /// both the task and the host when they may.
    fn take(&mut self, task: &(String, String), bytes: usize) -> bool {
        let now = std::time::Instant::now();
        self.host.refill(now);
        if !self.tasks.contains_key(task) && self.tasks.len() >= MAX_LOG_BUDGETS {
            for bucket in self.tasks.values_mut() {
                bucket.refill(now);
            }
            self.tasks.retain(|_, bucket| !bucket.full());
            if self.tasks.len() >= MAX_LOG_BUDGETS {
                if let Some(oldest) = self
                    .tasks
                    .iter()
                    .min_by_key(|(_, bucket)| bucket.at)
                    .map(|(key, _)| key.clone())
                {
                    self.tasks.remove(&oldest);
                }
            }
        }
        let bucket = self
            .tasks
            .entry(task.clone())
            .or_insert_with(|| LogBucket::new(TASK_LOG_BYTES_PER_SEC, TASK_LOG_BURST_BYTES));
        bucket.refill(now);
        let bytes = bytes as f64;
        if bucket.tokens >= bytes && self.host.tokens >= bytes {
            bucket.tokens -= bytes;
            self.host.tokens -= bytes;
            true
        } else {
            false
        }
    }
}

/// The worker end of one host's worker socket.
#[derive(Clone)]
pub struct WorkerService {
    supervisor: Weak<IsolateSupervisor>,
    cordon: String,
    logs: Arc<Mutex<LogBudgets>>,
    limits: super::host::HostCallLimits,
    /// Request bodies the worker holds for this host across awaits, in KiB.
    bodies: Arc<tokio::sync::Semaphore>,
}

impl WorkerService {
    pub fn new(
        supervisor: Weak<IsolateSupervisor>,
        cordon: &str,
        limits: super::host::HostCallLimits,
    ) -> Self {
        let kib = u32::try_from(limits.body_budget_bytes.div_ceil(1024)).unwrap_or(u32::MAX);
        Self {
            supervisor,
            cordon: cordon.to_string(),
            logs: Arc::new(Mutex::new(LogBudgets::new())),
            limits,
            bodies: Arc::new(tokio::sync::Semaphore::new(kib as usize)),
        }
    }

    /// What this host may send (the launcher serves the socket with it).
    pub fn limits(&self) -> super::host::HostCallLimits {
        self.limits
    }

    /// Charge `bytes` of request body to the host's budget for as long as
    /// the returned permit lives; refused at once (RESOURCE_EXHAUSTED)
    /// when the host already holds its budget: waiting would keep the
    /// decoded message alive anyway.
    fn hold_body(&self, bytes: usize) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        let kib = u32::try_from(bytes.div_ceil(1024)).unwrap_or(u32::MAX);
        self.bodies
            .clone()
            .try_acquire_many_owned(kib)
            .map_err(|_| {
                Status::resource_exhausted(format!(
                    "the isolate host has more than {} bytes of request bodies in flight; retry \
                 when some finish",
                    self.limits.body_budget_bytes
                ))
            })
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
        let origin = check_origin(origin)?;
        let supervisor = self.supervisor()?;
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

fn lock_budget(budget: &Mutex<LogBudgets>) -> std::sync::MutexGuard<'_, LogBudgets> {
    budget
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
        let _held = self.hold_body(request.body.len() + request.url.len())?;
        Ok(Response::new(
            supervisor.egress_backend().fetch(&task, request).await,
        ))
    }

    async fn vm(&self, request: Request<VmRequest>) -> Result<Response<VmReply>, Status> {
        // ACL: node-local, as `egress`.
        let request = request.into_inner();
        let (supervisor, mut task) = self.task(request.origin.as_ref())?;
        // Children are owned by the isolate's id: the host must name an
        // isolate the worker issued it for this very job and task, and the
        // owner carries the tenant the worker gave that isolate.
        let issued = supervisor.issued_tenant(&self.cordon, &task);
        if let Some(tenant) = issued.as_ref() {
            task.tenant = tenant.clone();
        }
        if issued.is_none() {
            warn!(
                cordon = %self.cordon,
                job_id = %task.job_id,
                task = %task.task_name,
                "An isolate host named an isolate the worker did not give it; VM call refused"
            );
            return Err(Status::permission_denied(format!(
                "{} {} of task {}/{} is not an isolate this worker gave the host",
                if task.session {
                    "session"
                } else {
                    "invocation"
                },
                task.isolate_id,
                task.job_id,
                task.task_name
            )));
        }
        let vm_task = supervisor.vm_task_of(&task.job_id, &task.task_name);
        let _held = self.hold_body(vm_request_bytes(&request))?;
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
                    check_checkpoint_header(&first)?;
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
        //
        // Nothing here is sized by a product of two host-chosen quantities:
        // the origin is bounded first; embedded newlines are split before
        // anything is budgeted; a batch keeps at most
        // `MAX_LINES_PER_BATCH` lines and `MAX_BATCH_BYTES` of escaped
        // text; the rate budget is charged for the rendered size, prefix
        // included.
        let mut stream = request.into_inner();
        while let Some(batch) = stream.message().await? {
            let origin = check_origin(batch.origin.as_ref())?;
            let (supervisor, mut task) = match self.task(Some(origin)) {
                Ok(found) => found,
                // A task that just left the node: its lines have nowhere
                // to go.
                Err(_) => continue,
            };
            // Only an isolate the worker gave this host (a session it
            // holds, an invocation in flight or just ended), attributed to
            // the tenant the worker recorded for it.
            let Some(tenant) = supervisor.log_tenant(&self.cordon, &task) else {
                warn!(
                    cordon = %self.cordon,
                    job_id = %task.job_id,
                    task = %task.task_name,
                    "An isolate host sent log lines for an isolate the worker did not give it; dropped"
                );
                continue;
            };
            task.tenant = tenant;
            let (lines, mut dropped) = super::guest_logs::split_lines(
                &batch.lines,
                super::guest_logs::MAX_LINES_PER_BATCH,
                super::guest_logs::MAX_BATCH_BYTES,
            );
            drop(batch);
            let Some(logs) = supervisor.guest_logs() else {
                for line in &lines {
                    debug!(
                        job_id = %task.job_id,
                        task = %task.task_name,
                        isolate_id = %task.isolate_id,
                        stream = if line.stderr { "stderr" } else { "stdout" },
                        "{}",
                        String::from_utf8_lossy(&line.text)
                    );
                }
                continue;
            };
            let prefix = super::guest_logs::line_prefix(&task.isolate_id, &task.tenant);
            let key = (task.job_id.clone(), task.task_name.clone());
            let mut kept = Vec::with_capacity(lines.len());
            {
                let mut budget = lock_budget(&self.logs);
                for line in lines {
                    if budget.take(&key, super::guest_logs::rendered_len(&prefix, &line)) {
                        kept.push(line);
                    } else {
                        dropped += 1;
                    }
                }
            }
            if dropped > 0 {
                kept.push(super::guest_logs::Line {
                    stderr: true,
                    text: format!(
                        "{dropped} lines dropped: over the task's log rate or the batch limit"
                    )
                    .into_bytes(),
                });
            }
            let envelope = task.envelope_allocation_id;
            let written =
                tokio::task::spawn_blocking(move || logs.append_lines(envelope, &prefix, &kept))
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

/// The bytes a child-VM request carries that the worker holds while the VM
/// answers: a file's content, an exec's stdin and argv.
fn vm_request_bytes(request: &VmRequest) -> usize {
    use crate::structs::isolate_host::vm_request::Op;
    match request.op.as_ref() {
        Some(Op::Put(put)) => put.content.len() + put.path.len(),
        Some(Op::Exec(exec)) => exec.stdin.len() + exec.argv.iter().map(String::len).sum::<usize>(),
        _ => 0,
    }
}
