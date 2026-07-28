//! openraft network layer over the `RaftTransport` tonic service (phase A-1
//! of `docs/plans/durability-and-ops-execution-plan.md` §2).
//!
//! One [`ChannelPool`] is shared by every lane's [`RaftTransportNetworkFactory`],
//! so a 3-lane topology keeps a single HTTP/2 connection per peer manager
//! instead of one per (lane, peer). Payloads are the openraft 0.9 serde
//! types encoded with serde_json (see the `RaftTransport` service contract
//! in `proto/legion.proto`), with their binary fields base64'd — directly
//! for `StoreCommand::changeset`, and via [`WireInstallSnapshotRequest`]
//! for snapshot chunks, whose type openraft owns. Every call carries the
//! cluster secret in request metadata (plan JC-7).
//!
//! Error mapping (the part openraft's backoff behavior depends on):
//! - dial/connect failures, `Unavailable`, and deadline expiry map to
//!   [`Unreachable`] so openraft backs off before retrying a dead peer;
//! - malformed payloads/metadata and other gRPC status codes map to
//!   [`NetworkError`] (retried immediately);
//! - a reply that decodes to a raft-level `Err` becomes
//!   [`RPCError::RemoteError`], preserving the typed `RaftError` for the
//!   local openraft core (e.g. higher-vote rejections).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

use crate::storage::raft::types::{node_addr_host_port, TypeConfig};
use crate::storage::raft::{Lane, RAFT_CLUSTER_SECRET_METADATA_KEY};
use crate::structs::raft_transport_client::RaftTransportClient;
use crate::structs::{RaftLaneReply, RaftLaneRequest};

type NodeId = u64;

/// Maximum gRPC message size accepted/produced by the raft transport, on
/// BOTH sides (the tonic default of 4 MiB is far too small: openraft's
/// snapshot chunks and append-entries batches carry binary payloads, so the
/// old ceiling capped installable snapshots at ~1 MiB raw and made any
/// larger snapshot hot-retry forever — adversarial finding, 2026-07-24).
///
/// This is the documented ceiling: a snapshot chunk (or one append-entries
/// batch) must fit inside it *after* encoding. Both halves of the fix
/// matter — the binary payloads are base64'd (see
/// [`crate::storage::raft::types::wire_bytes`], 4/3 instead of ~4x), and
/// the limit is raised to 64 MiB, which leaves ~11x headroom over the
/// 4 MiB chunk size.
pub(crate) const RAFT_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Raw bytes per openraft snapshot chunk (`snapshot_max_chunk_size`). Kept
/// far below [`RAFT_MAX_MESSAGE_BYTES`] so the encoded chunk plus envelope
/// always fits (see `chunk_fits_transport_ceiling`).
pub(crate) const SNAPSHOT_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// Worst-case encoded expansion of a binary payload on the raft wire.
/// Base64 is exactly 4/3 plus padding; the constant keeps a safety factor
/// so the headroom tripwire stays honest if the encoding ever changes.
#[cfg_attr(not(test), allow(dead_code))] // consumed by the headroom tripwire test
pub(crate) const WIRE_BYTE_BLOWUP: u64 = 2;

/// Wire mirror of openraft's [`InstallSnapshotRequest`] whose chunk bytes
/// are base64-encoded instead of rendered as a JSON number array.
///
/// openraft derives plain serde on its own request type, so the only way to
/// change how `data` is encoded is to serialize a mirror. Field-for-field
/// identical otherwise; both ends of the RPC live in this repo (client
/// below, server in `server.rs`).
#[derive(Serialize, serde::Deserialize)]
pub(crate) struct WireInstallSnapshotRequest {
    pub(crate) vote: openraft::Vote<NodeId>,
    pub(crate) meta: openraft::SnapshotMeta<NodeId, BasicNode>,
    pub(crate) offset: u64,
    #[serde(with = "crate::storage::raft::types::wire_bytes")]
    pub(crate) data: Vec<u8>,
    pub(crate) done: bool,
}

impl From<InstallSnapshotRequest<TypeConfig>> for WireInstallSnapshotRequest {
    fn from(rpc: InstallSnapshotRequest<TypeConfig>) -> Self {
        Self {
            vote: rpc.vote,
            meta: rpc.meta,
            offset: rpc.offset,
            data: rpc.data,
            done: rpc.done,
        }
    }
}

impl From<WireInstallSnapshotRequest> for InstallSnapshotRequest<TypeConfig> {
    fn from(wire: WireInstallSnapshotRequest) -> Self {
        Self {
            vote: wire.vote,
            meta: wire.meta,
            offset: wire.offset,
            data: wire.data,
            done: wire.done,
        }
    }
}

/// Lazily-connected tonic channels keyed by `"host:port"`, shared across all
/// lanes' network factories (one connection pool per manager process).
///
/// `connect_lazy` never dials at creation time — connection establishment
/// (and its failures) happen on first use and surface as per-RPC statuses,
/// which is exactly what [`RaftNetworkFactory::new_client`]'s contract asks
/// for ("should not create a connection but rather a client that will
/// connect when required").
#[derive(Default)]
pub(crate) struct ChannelPool {
    channels: Mutex<HashMap<String, Channel>>,
    /// Partition test hook (`Store::raft_set_outbound_blocked`): while set,
    /// every outbound raft RPC from this node fails as [`Unreachable`]
    /// before dialing. Combined with dropping the node's inbound transport
    /// listener this fully isolates the node — the in-process equivalent of
    /// yanking its network cable, used by the durability tests
    /// (`tests/raft_durability.rs`).
    outbound_blocked: AtomicBool,
}

impl ChannelPool {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Blocks/unblocks every outbound raft RPC through this pool (see the
    /// field docs — test hook only).
    pub(crate) fn set_outbound_blocked(&self, blocked: bool) {
        self.outbound_blocked.store(blocked, Ordering::SeqCst);
    }

    fn outbound_blocked(&self) -> bool {
        self.outbound_blocked.load(Ordering::SeqCst)
    }

    /// Returns the pooled channel for `host_port`, creating it lazily.
    /// Fails only on a malformed address.
    fn get(&self, host_port: &str) -> Result<Channel, tonic::transport::Error> {
        let mut channels = self
            .channels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(channel) = channels.get(host_port) {
            return Ok(channel.clone());
        }
        // Plaintext h2c on the manager plane, consistent with the cluster's
        // current transport posture; TLS upgrades with the tracked
        // worker-mTLS follow-up (plan JC-7).
        let channel = Endpoint::from_shared(format!("http://{host_port}"))?.connect_lazy();
        channels.insert(host_port.to_string(), channel.clone());
        Ok(channel)
    }
}

/// Per-lane [`RaftNetworkFactory`]: stamps every client it creates with the
/// lane's wire name and the cluster secret, and hands all of them the shared
/// [`ChannelPool`].
pub(crate) struct RaftTransportNetworkFactory {
    lane: Lane,
    cluster_secret: String,
    pool: Arc<ChannelPool>,
}

impl RaftTransportNetworkFactory {
    pub(crate) fn new(lane: Lane, cluster_secret: String, pool: Arc<ChannelPool>) -> Self {
        Self {
            lane,
            cluster_secret,
            pool,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftTransportNetworkFactory {
    type Network = RaftTransportNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        RaftTransportNetwork {
            lane: self.lane,
            target,
            // `BasicNode.addr` is "name@host:port" (types.rs); dial only the
            // address part.
            host_port: node_addr_host_port(&node.addr).to_string(),
            cluster_secret: self.cluster_secret.clone(),
            pool: self.pool.clone(),
        }
    }
}

/// [`RaftNetwork`] connection to one target node for one lane.
pub(crate) struct RaftTransportNetwork {
    lane: Lane,
    target: NodeId,
    host_port: String,
    cluster_secret: String,
    pool: Arc<ChannelPool>,
}

/// Maps a gRPC status to the openraft transport error class. See the module
/// docs for the rationale per class.
fn status_to_rpc_error<E: std::error::Error>(
    status: &tonic::Status,
) -> RPCError<NodeId, BasicNode, E> {
    match status.code() {
        // The peer cannot be reached right now (dial failure, listener down,
        // deadline blown, engine not yet started on the peer): back off.
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
            RPCError::Unreachable(Unreachable::new(status))
        }
        // Everything else (auth rejection, undecodable payload, unknown
        // group, internal errors) is a hard protocol-level failure; report
        // as a network error so openraft retries without a backoff hold.
        _ => RPCError::Network(NetworkError::new(status)),
    }
}

impl RaftTransportNetwork {
    /// Sends one openraft protocol message: serde_json-encode the request,
    /// attach lane + secret, dial through the pool, decode the reply's
    /// `Result<Reply, RemoteErr>` and re-raise raft-level errors as
    /// [`RPCError::RemoteError`].
    async fn call<Req, Reply, RemoteErr>(
        &mut self,
        rpc_name: &'static str,
        request: &Req,
        option: RPCOption,
        send: impl AsyncFnOnce(
            &mut RaftTransportClient<Channel>,
            tonic::Request<RaftLaneRequest>,
        ) -> Result<tonic::Response<RaftLaneReply>, tonic::Status>,
    ) -> Result<Reply, RPCError<NodeId, BasicNode, RemoteErr>>
    where
        Req: Serialize,
        Reply: DeserializeOwned,
        RemoteErr: std::error::Error + DeserializeOwned,
    {
        if self.pool.outbound_blocked() {
            // Partition test hook: behave exactly like an unreachable peer
            // so openraft backs off, the same as a dead network would.
            return Err(RPCError::Unreachable(Unreachable::new(&OutboundBlocked)));
        }
        let payload =
            serde_json::to_vec(request).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let channel = self
            .pool
            .get(&self.host_port)
            // A malformed advertise address will not fix itself on immediate
            // retry: back off.
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let mut client = RaftTransportClient::new(channel)
            .max_decoding_message_size(RAFT_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(RAFT_MAX_MESSAGE_BYTES);

        let mut req = tonic::Request::new(RaftLaneRequest {
            group_id: self.lane.wire_name().to_string(),
            payload,
        });
        req.set_timeout(option.hard_ttl());
        let secret = self
            .cluster_secret
            .parse()
            .map_err(|_| RPCError::Network(NetworkError::new(&InvalidSecretMetadata)))?;
        req.metadata_mut()
            .insert(RAFT_CLUSTER_SECRET_METADATA_KEY, secret);

        let reply = send(&mut client, req)
            .await
            .map_err(|status| {
                tracing::debug!(
                    lane = self.lane.wire_name(),
                    target = self.target,
                    addr = %self.host_port,
                    rpc = rpc_name,
                    code = ?status.code(),
                    "raft transport rpc failed: {}",
                    status.message()
                );
                status_to_rpc_error(&status)
            })?
            .into_inner();

        let result: Result<Reply, RemoteErr> = serde_json::from_slice(&reply.payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

/// Error carried by the [`ChannelPool::set_outbound_blocked`] partition
/// test hook when it fails an outbound RPC.
#[derive(Debug)]
struct OutboundBlocked;

impl std::fmt::Display for OutboundBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("outbound raft transport is blocked (partition test hook)")
    }
}

impl std::error::Error for OutboundBlocked {}

/// Error stand-in for a cluster secret that is not valid ASCII gRPC
/// metadata (configuration error; secrets are minted as ASCII).
#[derive(Debug)]
struct InvalidSecretMetadata;

impl std::fmt::Display for InvalidSecretMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cluster secret is not valid ascii metadata")
    }
}

impl std::error::Error for InvalidSecretMetadata {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tripwire for the snapshot-size ceiling (adversarial finding,
    /// 2026-07-24): an encoded snapshot chunk (worst-case byte blowup)
    /// plus generous envelope overhead must fit inside the transport's
    /// message-size limit on both sides, or install-snapshot wedges in a
    /// hot retry loop. Fails if someone shrinks the limit or grows the
    /// chunk without keeping the headroom.
    #[test]
    fn chunk_fits_transport_ceiling() {
        let worst_case_encoded = SNAPSHOT_CHUNK_BYTES
            .checked_mul(WIRE_BYTE_BLOWUP)
            .and_then(|encoded| encoded.checked_add(1024 * 1024))
            .expect("no overflow");
        assert!(
            worst_case_encoded <= RAFT_MAX_MESSAGE_BYTES as u64,
            "snapshot chunk ({SNAPSHOT_CHUNK_BYTES} raw bytes, ~{WIRE_BYTE_BLOWUP}x encoding \
             blowup) must fit the {RAFT_MAX_MESSAGE_BYTES}-byte transport ceiling with room \
             for the envelope"
        );
        // And the ceiling itself must be far above tonic's 4 MiB default,
        // which is what silently capped snapshots before.
        assert!(RAFT_MAX_MESSAGE_BYTES > 4 * 1024 * 1024);
    }

    /// The install-snapshot mirror must round-trip field-for-field AND keep
    /// the chunk near base64's 4/3 — the whole reason it exists. A raw
    /// `Vec<u8>` through serde_json would be ~4x, which is what pushed a
    /// full-size chunk past the transport ceiling.
    #[test]
    fn install_snapshot_mirror_round_trips_without_json_blowup() {
        use openraft::{CommittedLeaderId, LogId, SnapshotMeta, StoredMembership, Vote};

        let data: Vec<u8> = (0..=255u8).cycle().take(256 * 1024).collect();
        let rpc = InstallSnapshotRequest::<TypeConfig> {
            vote: Vote::new(3, 7),
            meta: SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(3, 7), 42)),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-1".to_string(),
            },
            offset: 0,
            data: data.clone(),
            done: true,
        };
        let encoded =
            serde_json::to_vec(&WireInstallSnapshotRequest::from(rpc)).expect("encode chunk");
        assert!(
            (encoded.len() as u64) < data.len() as u64 * WIRE_BYTE_BLOWUP,
            "encoded chunk ({} bytes) must stay within the documented blowup of the raw \
             {} bytes",
            encoded.len(),
            data.len()
        );
        let decoded: InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice::<WireInstallSnapshotRequest>(&encoded)
                .expect("decode chunk")
                .into();
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.offset, 0);
        assert!(decoded.done);
        assert_eq!(decoded.meta.snapshot_id, "snap-1");
        assert_eq!(decoded.vote, Vote::new(3, 7));
    }
}

impl RaftNetwork<TypeConfig> for RaftTransportNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call("AppendEntries", &rpc, option, async |client, req| {
            client.append_entries(req).await
        })
        .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        // Encoded through the base64 mirror: a 4 MiB chunk would otherwise
        // render as ~16 MiB of JSON number array.
        let wire = WireInstallSnapshotRequest::from(rpc);
        self.call("InstallSnapshot", &wire, option, async |client, req| {
            client.install_snapshot(req).await
        })
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call("Vote", &rpc, option, async |client, req| {
            client.vote(req).await
        })
        .await
    }
}
