//! Server side of the `RaftTransport` tonic service (phase A-1 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2): bridges inbound
//! openraft protocol RPCs to the right lane's `openraft::Raft` instance.
//!
//! The service is constructed at manager start (it shares the manager's
//! tonic listener) around an empty [`OnceLock`]; the raft driver populates
//! the [`RaftTransportRegistry`] when the engine boots. Until then every RPC
//! answers `Unavailable`, which the peer's network layer maps to
//! `Unreachable` so its openraft core backs off — a booting or non-raft
//! manager is indistinguishable from a briefly unreachable one, which is the
//! correct signal.
//!
//! Authentication: every RPC must present the cluster secret in request
//! metadata (`x-legion-raft-secret`, plan JC-7). Missing secret →
//! `Unauthenticated`; wrong secret → `PermissionDenied` (compared in
//! constant time). Raft-level errors from the local `Raft` instance are
//! serialized into the reply payload as `Result::Err` — gRPC statuses are
//! reserved for transport/auth/dispatch failures, mirroring the client side
//! in `network.rs`.

// gRPC handler signatures are fixed by the generated `RaftTransport` trait,
// so the large `tonic::Status` error variant cannot be boxed away here.
#![allow(clippy::result_large_err)]

use std::sync::{Arc, OnceLock};

use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::{Request, Response, Status};

use crate::storage::raft::types::TypeConfig;
use crate::storage::raft::{RaftTransportRegistry, RAFT_CLUSTER_SECRET_METADATA_KEY};
use crate::structs::raft_transport_server::RaftTransport;
use crate::structs::{
    JoinRaftClusterReply, JoinRaftClusterRequest, RaftLaneReply, RaftLaneRequest,
};

/// Constant-time byte comparison for the cluster secret, so the check does
/// not leak match length/prefix timing. (The secret is high-entropy and
/// minted at bootstrap; this is defense in depth on top of JC-7's stopgap
/// posture.)
pub(crate) fn secrets_match(presented: &[u8], expected: &[u8]) -> bool {
    if presented.len() != expected.len() {
        return false;
    }
    presented
        .iter()
        .zip(expected.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// The `RaftTransport` tonic service. Cheap to clone-construct per listener;
/// all state lives behind the shared registry cell. Public only so
/// `Store::raft_transport_server` can name it in a public signature (the
/// constructor and registry stay crate-internal).
pub struct RaftTransportService {
    registry: Arc<OnceLock<RaftTransportRegistry>>,
}

impl RaftTransportService {
    /// Builds the service around a registry cell. The driver keeps a clone
    /// of the same `Arc` and calls `.set(...)` on it once the per-lane
    /// `Raft` instances exist.
    pub(crate) fn new(registry: Arc<OnceLock<RaftTransportRegistry>>) -> Self {
        Self { registry }
    }

    /// Registry access + cluster-secret authentication for one request.
    fn authenticate<T>(&self, request: &Request<T>) -> Result<&RaftTransportRegistry, Status> {
        let registry = self
            .registry
            .get()
            .ok_or_else(|| Status::unavailable("raft engine not started on this manager"))?;
        let presented = request
            .metadata()
            .get(RAFT_CLUSTER_SECRET_METADATA_KEY)
            .ok_or_else(|| {
                Status::unauthenticated(
                    "missing cluster secret metadata (x-legion-raft-secret); \
                     RaftTransport is manager-plane internal",
                )
            })?;
        if !secrets_match(presented.as_bytes(), registry.cluster_secret.as_bytes()) {
            return Err(Status::permission_denied(
                "cluster secret mismatch; this manager belongs to a different raft cluster",
            ));
        }
        Ok(registry)
    }

    /// Decodes an inbound lane envelope, dispatches the request to the named
    /// lane's `Raft` via `call`, and encodes the raft-level
    /// `Result<Reply, Err>` into the reply payload.
    async fn dispatch<Req, Reply, Err, F>(
        &self,
        rpc_name: &'static str,
        request: Request<RaftLaneRequest>,
        call: impl FnOnce(openraft::Raft<TypeConfig>, Req) -> F,
    ) -> Result<Response<RaftLaneReply>, Status>
    where
        Req: DeserializeOwned,
        Reply: Serialize,
        Err: Serialize,
        F: std::future::Future<Output = Result<Reply, Err>>,
    {
        let registry = self.authenticate(&request)?;
        let envelope = request.into_inner();
        let raft = registry
            .lane_by_wire_name(&envelope.group_id)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "raft group `{}` is not active on this manager",
                    envelope.group_id
                ))
            })?
            .clone();
        let rpc: Req = serde_json::from_slice(&envelope.payload).map_err(|e| {
            Status::invalid_argument(format!(
                "undecodable {rpc_name} payload (mixed manager protocol versions?): {e}"
            ))
        })?;
        // Raft-level errors ride the payload as `Err`, not a gRPC status.
        let result = call(raft, rpc).await;
        let payload = serde_json::to_vec(&result)
            .map_err(|e| Status::internal(format!("failed encoding {rpc_name} reply: {e}")))?;
        Ok(Response::new(RaftLaneReply { payload }))
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftLaneRequest>,
    ) -> Result<Response<RaftLaneReply>, Status> {
        self.dispatch("AppendEntries", request, |raft, rpc| async move {
            raft.append_entries(rpc).await
        })
        .await
    }

    async fn vote(
        &self,
        request: Request<RaftLaneRequest>,
    ) -> Result<Response<RaftLaneReply>, Status> {
        self.dispatch(
            "Vote",
            request,
            |raft, rpc| async move { raft.vote(rpc).await },
        )
        .await
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftLaneRequest>,
    ) -> Result<Response<RaftLaneReply>, Status> {
        // Decoded through the base64 mirror the client encodes with (see
        // `network::WireInstallSnapshotRequest`): openraft's own request
        // type would render the chunk as a JSON number array.
        self.dispatch(
            "InstallSnapshot",
            request,
            |raft, rpc: crate::storage::raft::network::WireInstallSnapshotRequest| async move {
                raft.install_snapshot(rpc.into()).await
            },
        )
        .await
    }

    async fn join(
        &self,
        request: Request<JoinRaftClusterRequest>,
    ) -> Result<Response<JoinRaftClusterReply>, Status> {
        let registry = self.authenticate(&request)?;
        let handler = registry.join_handler.clone().ok_or_else(|| {
            // The driver installs the handler at engine start on every
            // node (a non-leader answers FailedPrecondition with a
            // leader hint); a registry without one is a transport-only
            // bring-up (e.g. tests).
            Status::unimplemented("raft join is not available on this manager")
        })?;
        handler.join(request.into_inner()).await.map(Response::new)
    }
}
