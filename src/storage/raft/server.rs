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
    JoinRaftClusterReply, JoinRaftClusterRequest, NodeLivenessDigestReply,
    NodeLivenessDigestRequest, RaftLaneReply, RaftLaneRequest,
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
        let version = registry
            .schema_version
            .load(std::sync::atomic::Ordering::Relaxed);
        Ok(stamp_schema_version(
            Response::new(RaftLaneReply { payload }),
            version,
        ))
    }
}

/// Stamp a transport reply with this build's schema version
/// (`x-legion-schema-version`), so the peer — the leader, on every
/// AppendEntries round — learns which schema every voter can apply. Read by
/// version-gated features such as the KV keyring's upgrade gate
/// (docs/kv-encryption.md, docs/upgrades.md).
pub(crate) fn stamp_schema_version<T>(mut response: Response<T>, version: u32) -> Response<T> {
    if let Ok(value) = version.to_string().parse() {
        response.metadata_mut().insert(
            crate::storage::raft::RAFT_SCHEMA_VERSION_METADATA_KEY,
            value,
        );
    }
    response
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

    async fn forward_lane_write(
        &self,
        request: Request<RaftLaneRequest>,
    ) -> Result<Response<RaftLaneReply>, Status> {
        let registry = self.authenticate(&request)?;
        let store = registry.forward_store.clone().ok_or_else(|| {
            Status::unimplemented("lane-write forwarding is not available on this manager")
        })?;
        let envelope = request.into_inner();
        let op: crate::storage::raft::forward::ForwardableLaneWrite =
            serde_json::from_slice(&envelope.payload).map_err(|e| {
                Status::invalid_argument(format!(
                    "undecodable ForwardLaneWrite payload (mixed manager protocol versions?): {e}"
                ))
            })?;
        // Sanity: the envelope's lane must be the op's own lane — a mismatch
        // is a bug on the sending side, not a leadership question.
        if envelope.group_id != op.lane().wire_name() {
            return Err(Status::invalid_argument(format!(
                "ForwardLaneWrite lane mismatch: envelope says `{}`, op `{}` belongs to `{}`",
                envelope.group_id,
                op.op_name(),
                op.lane().wire_name()
            )));
        }
        // The op is blocking store work (capture + quorum commit + local
        // apply); keep it off the transport's async workers.
        let outcome =
            tokio::task::spawn_blocking(move || crate::storage::raft::forward::execute(&store, op))
                .await
                .map_err(|e| Status::internal(format!("forwarded lane write panicked: {e}")))?;
        let payload = serde_json::to_vec(&outcome)
            .map_err(|e| Status::internal(format!("failed encoding ForwardOutcome: {e}")))?;
        Ok(Response::new(RaftLaneReply { payload }))
    }

    /// Merge a peer manager's liveness digest into this manager's table
    /// (liveness plan §6.3).
    ///
    /// Answered even when this manager does NOT lead the status lane: the
    /// sender needs a typed "not the leader" plus a hint so it can re-target,
    /// exactly as `ForwardLaneWrite` callers do. Refusing with a gRPC error
    /// instead would be indistinguishable, at the sender, from the peer being
    /// unreachable — and that difference is what decides whether the sender
    /// falls back to per-heartbeat replicated writes.
    async fn report_node_liveness(
        &self,
        request: Request<NodeLivenessDigestRequest>,
    ) -> Result<Response<NodeLivenessDigestReply>, Status> {
        let registry = self.authenticate(&request)?;
        let store = registry.forward_store.clone().ok_or_else(|| {
            Status::unimplemented("liveness digests are not available on this manager")
        })?;
        let digest = request.into_inner();
        if digest.observer_manager_id.is_empty() {
            return Err(Status::invalid_argument(
                "liveness digest is missing observer_manager_id",
            ));
        }
        if !store.is_raft_leader(crate::storage::Lane::Status) {
            return Ok(Response::new(NodeLivenessDigestReply {
                accepted: false,
                not_status_leader: true,
                leader_hint: store
                    .lane_leader_hint(crate::storage::Lane::Status)
                    .unwrap_or_default(),
                served_interval_seconds: None,
            }));
        }
        store
            .liveness()
            .observe_peer(&digest.observer_manager_id, digest.seen_node_ids);
        Ok(Response::new(NodeLivenessDigestReply {
            accepted: true,
            not_status_leader: false,
            leader_hint: String::new(),
            // The leader owns the cadence ladder; a follower relays this to
            // its own workers so adaptive cadence is not leader-only.
            served_interval_seconds: store
                .liveness()
                .served_interval()
                .map(|interval| interval.as_secs() as u32),
        }))
    }
}
