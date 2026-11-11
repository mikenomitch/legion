//! The gRPC shell: `IsolateHost` served on the inherited control socket,
//! `IsolateWorker` called on the inherited worker socket. Every method is a
//! line or two over [`Host`].

use std::os::fd::{FromRawFd, RawFd};
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use legion_proto::isolate_host::isolate_host_server::{IsolateHost, IsolateHostServer};
use legion_proto::isolate_host::*;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

use crate::host::Host;

pub struct HostService {
    host: Host,
}

impl HostService {
    pub fn new(host: Host) -> Self {
        Self { host }
    }
}

#[tonic::async_trait]
impl IsolateHost for HostService {
    async fn hello(&self, _r: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        Ok(Response::new(self.host.hello()))
    }

    async fn load_module(
        &self,
        r: Request<LoadModuleRequest>,
    ) -> Result<Response<LoadModuleReply>, Status> {
        let host = self.host.clone();
        // Mapping an artifact touches the disk: keep it off the reactor.
        tokio::task::spawn_blocking(move || host.load_module(r.into_inner()))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(Response::new)
    }

    async fn unload_module(
        &self,
        r: Request<UnloadModuleRequest>,
    ) -> Result<Response<UnloadModuleReply>, Status> {
        self.host.unload_module(r.into_inner()).map(Response::new)
    }

    async fn configure_task(
        &self,
        r: Request<TaskSpec>,
    ) -> Result<Response<ConfigureTaskReply>, Status> {
        self.host.configure_task(r.into_inner()).map(Response::new)
    }

    async fn remove_task(
        &self,
        r: Request<RemoveTaskRequest>,
    ) -> Result<Response<RemoveTaskReply>, Status> {
        self.host.remove_task(r.into_inner()).await.map(Response::new)
    }

    async fn invoke(&self, r: Request<InvokeRequest>) -> Result<Response<InvokeReply>, Status> {
        self.host.invoke(r.into_inner()).await.map(Response::new)
    }

    async fn open_session(
        &self,
        r: Request<OpenSessionRequest>,
    ) -> Result<Response<OpenSessionReply>, Status> {
        self.host.open_session(r.into_inner()).map(Response::new)
    }

    async fn close_session(
        &self,
        r: Request<CloseSessionRequest>,
    ) -> Result<Response<CloseSessionReply>, Status> {
        let r = r.into_inner();
        Ok(Response::new(CloseSessionReply {
            closed: self.host.close_session(&r.session_id, &r.reason).await,
        }))
    }

    async fn get_session(
        &self,
        r: Request<GetSessionRequest>,
    ) -> Result<Response<SessionInfo>, Status> {
        self.host
            .get_session(&r.into_inner().session_id)
            .map(Response::new)
    }

    async fn resize_session(
        &self,
        r: Request<ResizeSessionRequest>,
    ) -> Result<Response<ResizeSessionReply>, Status> {
        self.host
            .resize_session(r.into_inner())
            .await
            .map(Response::new)
    }

    async fn reclaim(&self, r: Request<ReclaimRequest>) -> Result<Response<ReclaimReply>, Status> {
        Ok(Response::new(self.host.reclaim(r.into_inner()).await))
    }

    async fn stats(&self, _r: Request<StatsRequest>) -> Result<Response<StatsReply>, Status> {
        Ok(Response::new(self.host.stats()))
    }

    async fn drain(&self, r: Request<DrainRequest>) -> Result<Response<DrainReply>, Status> {
        Ok(Response::new(self.host.drain(r.into_inner()).await))
    }
}

/// An inherited Unix socket as a tokio stream.
///
/// # Safety
/// `fd` must be an open, owned Unix stream socket nothing else uses.
pub unsafe fn inherited(fd: RawFd) -> std::io::Result<UnixStream> {
    let s = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    s.set_nonblocking(true)?;
    UnixStream::from_std(s)
}

/// A tonic channel over one already-connected stream. A reconnect after it
/// closes fails: the worker is gone and the host is about to exit.
pub async fn channel_over(stream: UnixStream) -> Result<Channel, tonic::transport::Error> {
    let slot = Arc::new(std::sync::Mutex::new(Some(stream)));
    Endpoint::from_static("http://isolate-worker")
        .connect_with_connector(tower::service_fn(move |_| {
            let s = slot.lock().unwrap().take();
            async move {
                s.map(TokioIo::new).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "the worker socket is closed",
                    )
                })
            }
        }))
        .await
}

/// Serve `IsolateHost` on one connection until it closes.
pub async fn serve(host: Host, control: UnixStream) -> Result<(), tonic::transport::Error> {
    let svc = IsolateHostServer::new(HostService::new(host))
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
    let incoming = tokio_stream::once(Ok::<_, std::io::Error>(control));
    Server::builder()
        .add_service(svc)
        .serve_with_incoming(incoming)
        .await
}
