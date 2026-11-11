//! The manager's half of delivering app view and action requests
//! (docs/apps.md, "Views and actions"; apps plan §6.5): one HTTP request to
//! an app allocation's UI port, carried over the worker plane by
//! `Worker.ForwardAppRequest`, the unary subset of a worker-plane tunnel.
//!
//! The manager has already authorized the viewer and signed the viewer
//! assertion the request carries; this module only moves bytes. It sits
//! behind a trait so tests can deliver to an app in the same process
//! without a worker, while everything before and after the hop (the checks,
//! the assertion, the view validation, the audit) stays the real code.

use crate::storage::store::Store;
use crate::structs::{ForwardAppRequestReply, ForwardAppRequestRequest};
use std::time::Duration;
use tonic::Status;

/// How long the manager waits for the worker's answer: the worker's own 10
/// seconds for the app, plus the hop.
pub const FORWARD_TIMEOUT: Duration = Duration::from_secs(15);

/// Delivers one request to the allocation it names, on `node_id`.
#[tonic::async_trait]
pub trait AppForwarder: Send + Sync + std::fmt::Debug {
    async fn forward(
        &self,
        store: &Store,
        node_id: &str,
        request: ForwardAppRequestRequest,
    ) -> Result<ForwardAppRequestReply, Status>;
}

/// The production forwarder: the worker plane, with the manager's
/// certificate, to the node's advertised worker address.
#[derive(Debug)]
pub struct WorkerPlaneForwarder;

#[tonic::async_trait]
impl AppForwarder for WorkerPlaneForwarder {
    async fn forward(
        &self,
        store: &Store,
        node_id: &str,
        request: ForwardAppRequestRequest,
    ) -> Result<ForwardAppRequestReply, Status> {
        let node = store.get_node(node_id).map_err(|err| {
            Status::unavailable(format!(
                "the app's allocation is on node {node_id}, which cannot be loaded: {err}"
            ))
        })?;
        let target = crate::manager::worker_plane::WorkerTarget::for_node(&node)?;
        let mut client =
            crate::manager::worker_plane::WorkerPlane::global().client_for(store, &target)?;
        let mut call = tonic::Request::new(request);
        call.set_timeout(FORWARD_TIMEOUT);
        client
            .forward_app_request(call)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| {
                crate::manager::worker_plane::worker_call_error(node_id, &target.address, status)
            })
    }
}
