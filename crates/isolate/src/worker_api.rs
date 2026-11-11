//! What the host asks of its worker (`IsolateWorker`), as a trait.
//!
//! The host library calls the worker only through [`WorkerApi`]. In the
//! binary it is [`GrpcWorker`], a client on the inherited worker socket;
//! tests hand the library an in-process fake. Every request carries the
//! isolate's `Origin`, and the worker decides what that origin may do.

use std::sync::Arc;

use async_trait::async_trait;
use legion_proto::isolate_host::isolate_worker_client::IsolateWorkerClient;
use legion_proto::isolate_host::{
    checkpoint_chunk, CheckpointChunk, CheckpointHeader, EgressReply, EgressRequest,
    LoadCheckpointRequest, LogBatch, Origin, SaveCheckpointReply, VmReply, VmRequest,
};
use tonic::transport::Channel;
use tonic::{Code, Status};

/// Checkpoint bodies go to the worker in pieces of this size.
pub const CHECKPOINT_CHUNK: usize = 256 << 10;

#[async_trait]
pub trait WorkerApi: Send + Sync + 'static {
    /// One outbound HTTP request. A policy refusal is `Ok` with `error` set.
    async fn egress(&self, req: EgressRequest) -> Result<EgressReply, Status>;
    /// One child-VM operation.
    async fn vm(&self, req: VmRequest) -> Result<VmReply, Status>;
    /// Store a session checkpoint (replaces the previous one on success).
    async fn save_checkpoint(
        &self,
        header: CheckpointHeader,
        body: Vec<u8>,
    ) -> Result<SaveCheckpointReply, Status>;
    /// A session's last checkpoint; `Ok(None)` when the worker has none
    /// (the session does not exist, or was never hibernated).
    async fn load_checkpoint(
        &self,
        origin: Origin,
    ) -> Result<Option<(CheckpointHeader, Vec<u8>)>, Status>;
    /// Guest output for the task's log stream. Best effort.
    async fn log(&self, batch: LogBatch) -> Result<(), Status>;
}

/// A worker that refuses everything: for a host with no worker connection
/// (tests, `compile`).
pub struct NoWorker;

#[async_trait]
impl WorkerApi for NoWorker {
    async fn egress(&self, _req: EgressRequest) -> Result<EgressReply, Status> {
        Err(Status::unavailable("no worker connection"))
    }
    async fn vm(&self, _req: VmRequest) -> Result<VmReply, Status> {
        Err(Status::unavailable("no worker connection"))
    }
    async fn save_checkpoint(
        &self,
        _header: CheckpointHeader,
        _body: Vec<u8>,
    ) -> Result<SaveCheckpointReply, Status> {
        Err(Status::unavailable("no worker connection"))
    }
    async fn load_checkpoint(
        &self,
        _origin: Origin,
    ) -> Result<Option<(CheckpointHeader, Vec<u8>)>, Status> {
        Ok(None)
    }
    async fn log(&self, _batch: LogBatch) -> Result<(), Status> {
        Ok(())
    }
}

/// `IsolateWorker` over the inherited worker socket. Calls run on the
/// host's I/O runtime (`rt`), never on a shard thread: the shard awaits the
/// join handle, which is runtime-agnostic.
pub struct GrpcWorker {
    client: IsolateWorkerClient<Channel>,
    rt: tokio::runtime::Handle,
}

impl GrpcWorker {
    pub fn new(channel: Channel, rt: tokio::runtime::Handle) -> Arc<Self> {
        Arc::new(Self {
            client: IsolateWorkerClient::new(channel)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            rt,
        })
    }

    async fn on_io<F, T>(&self, f: F) -> Result<T, Status>
    where
        F: std::future::Future<Output = Result<T, Status>> + Send + 'static,
        T: Send + 'static,
    {
        self.rt
            .spawn(f)
            .await
            .map_err(|e| Status::internal(format!("worker call task failed: {e}")))?
    }

    /// As `on_io`, but dropping the returned future aborts the call (the
    /// gRPC stream is reset, so the worker drops its side too): an egress
    /// or VM call whose invocation ended does not run on.
    async fn on_io_cancellable<F, T>(&self, f: F) -> Result<T, Status>
    where
        F: std::future::Future<Output = Result<T, Status>> + Send + 'static,
        T: Send + 'static,
    {
        struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
        impl<T> Drop for AbortOnDrop<T> {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let mut h = AbortOnDrop(self.rt.spawn(f));
        (&mut h.0)
            .await
            .map_err(|e| Status::internal(format!("worker call task failed: {e}")))?
    }
}

#[async_trait]
impl WorkerApi for GrpcWorker {
    async fn egress(&self, req: EgressRequest) -> Result<EgressReply, Status> {
        let mut c = self.client.clone();
        self.on_io_cancellable(async move { Ok(c.egress(req).await?.into_inner()) })
            .await
    }

    async fn vm(&self, req: VmRequest) -> Result<VmReply, Status> {
        let mut c = self.client.clone();
        self.on_io_cancellable(async move { Ok(c.vm(req).await?.into_inner()) })
            .await
    }

    async fn save_checkpoint(
        &self,
        header: CheckpointHeader,
        body: Vec<u8>,
    ) -> Result<SaveCheckpointReply, Status> {
        let mut c = self.client.clone();
        self.on_io(async move {
            let mut chunks = vec![CheckpointChunk {
                part: Some(checkpoint_chunk::Part::Header(header)),
            }];
            for piece in body.chunks(CHECKPOINT_CHUNK) {
                chunks.push(CheckpointChunk {
                    part: Some(checkpoint_chunk::Part::Data(piece.to_vec())),
                });
            }
            Ok(c.save_checkpoint(tokio_stream::iter(chunks))
                .await?
                .into_inner())
        })
        .await
    }

    async fn load_checkpoint(
        &self,
        origin: Origin,
    ) -> Result<Option<(CheckpointHeader, Vec<u8>)>, Status> {
        let mut c = self.client.clone();
        self.on_io(async move {
            let mut stream = match c
                .load_checkpoint(LoadCheckpointRequest {
                    origin: Some(origin),
                })
                .await
            {
                Ok(r) => r.into_inner(),
                Err(s) if s.code() == Code::NotFound => return Ok(None),
                Err(s) => return Err(s),
            };
            let mut header = None;
            let mut body = Vec::new();
            loop {
                match stream.message().await {
                    Ok(Some(chunk)) => match chunk.part {
                        Some(checkpoint_chunk::Part::Header(h)) => header = Some(h),
                        Some(checkpoint_chunk::Part::Data(d)) => body.extend_from_slice(&d),
                        None => {}
                    },
                    Ok(None) => break,
                    Err(s) if s.code() == Code::NotFound => return Ok(None),
                    Err(s) => return Err(s),
                }
            }
            match header {
                Some(h) => Ok(Some((h, body))),
                None => Err(Status::data_loss("checkpoint stream had no header")),
            }
        })
        .await
    }

    async fn log(&self, batch: LogBatch) -> Result<(), Status> {
        let mut c = self.client.clone();
        self.on_io(async move {
            c.log(tokio_stream::iter(vec![batch])).await?;
            Ok(())
        })
        .await
    }
}
