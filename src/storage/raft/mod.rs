//! Raft replication seam for the store (phase A-0 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2).
//!
//! This module holds the *capture seam*: the types and thread-local context
//! that let a store mutation run inside a SQLite-session capture savepoint on
//! the proposing node, be rolled back locally, and later be applied —
//! byte-identically — on every node's state machine once the changeset
//! commits through raft. The phase A-1 engine plugs into this seam via
//! [`ReplicationHandle`]; its transport half lives here too ([`types`] for
//! the openraft type config, [`network`] for the outbound client,
//! [`server`] for the inbound `RaftTransport` tonic service, wired together
//! by [`RaftTransportRegistry`]). The log store, state machine, and
//! `MultiRaftDriver` complete the engine.

pub(crate) mod capture;
pub(crate) mod driver;
pub(crate) mod forward;
pub(crate) mod log_store;
pub(crate) mod network;
pub(crate) mod ownership;
pub(crate) mod quorum;
// Public: `Store::raft_transport_server` names the service type in its
// signature so multi-node in-process tests can mount the transport on their
// own listeners.
pub mod server;
pub(crate) mod state_machine;
pub(crate) mod types;

use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{
    FetchRootKeyReply, FetchRootKeyRequest, JoinRaftClusterReply, JoinRaftClusterRequest,
};

/// Raft group ("lane") a replicated store operation belongs to.
///
/// The v1 default topology is `single` (one group carries every lane), but
/// every mutating method is classified up front so the lanes topology is a
/// config flip, not a re-audit (plan §A3, JC-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Lane {
    /// Job/config/KV/instance control-plane writes.
    Control,
    /// Placement, eval-queue, and rollout writes.
    Scheduler,
    /// Node heartbeat/health/liveness writes.
    Status,
}

impl Lane {
    /// Every lane, for driver bring-up and exhaustiveness checks.
    pub(crate) const ALL: [Lane; 3] = [Lane::Control, Lane::Scheduler, Lane::Status];

    /// Id stride reserved for `ownership::Owner::Commutative` tables whose
    /// primary key is MINTED by the writer rather than supplied by the
    /// caller (today: `scheduler_event`) — JC-42.
    ///
    /// Such a table's ids must be globally unique across lanes: two lanes
    /// capture concurrently from the same base state, so any id derived from
    /// local state (the old `MAX(id) + 1`) collides, and the JC-28 `OMIT`
    /// conflict policy then resolves the collision in apply order — a
    /// different row per replica, i.e. silent divergence invisible to the
    /// content hash (which excludes commutative tables).
    ///
    /// The structural fix is a disjoint residue class per lane: every id a
    /// lane mints satisfies `id % ID_STRIDE == lane.id_residue()`, so the
    /// lanes' id spaces are provably disjoint no matter what each lane picks
    /// for the rest of the id. The stride is `8` — a power of two (so the
    /// residue is the low [`Lane::ID_STRIDE_BITS`] bits) with headroom for
    /// more than the three lanes that exist today; growing past 8 lanes
    /// means growing this constant, which changes minted ids but not the
    /// cursor contract (ids were never dense — see `docs/event-stream.md`).
    pub(crate) const ID_STRIDE: i64 = 8;

    /// `log2(ID_STRIDE)`: the number of low bits an id reserves for its
    /// lane residue.
    pub(crate) const ID_STRIDE_BITS: u32 = 3;

    /// This lane's reserved id residue (see [`Lane::ID_STRIDE`]). Frozen
    /// alongside [`Lane::wire_name`]: changing a value changes which lane a
    /// historical id is attributed to.
    ///
    /// [`Lane::Control`] is residue `0`, which is also the residue used
    /// outside any capture (`local_disk` mode, and every non-raft caller) —
    /// so single-node behavior is unchanged by striding.
    pub(crate) fn id_residue(self) -> i64 {
        match self {
            Lane::Control => 0,
            Lane::Scheduler => 1,
            Lane::Status => 2,
        }
    }

    /// The lane's stable wire name, used as `group_id` on the
    /// `RaftTransport` proto service and as the per-lane data directory
    /// name. Lowercase and frozen: changing a value is a wire/disk format
    /// break.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Lane::Control => "control",
            Lane::Scheduler => "scheduler",
            Lane::Status => "status",
        }
    }

    /// Parses a `group_id` wire name back into a lane.
    pub(crate) fn from_wire_name(name: &str) -> Option<Lane> {
        Lane::ALL.into_iter().find(|lane| lane.wire_name() == name)
    }
}

/// Request-metadata key carrying the cluster secret on every
/// `RaftTransport` RPC (plan §A4, JC-7). Minted at bootstrap, distributed
/// only inside join tokens; checked by `server::RaftTransportService` and
/// attached by `network::RaftTransportNetwork`.
pub(crate) const RAFT_CLUSTER_SECRET_METADATA_KEY: &str = "x-legion-raft-secret";
/// Reply metadata key carrying the answering manager's schema version (the
/// migration count its build knows); see `raft::server::stamp_schema_version`.
pub(crate) const RAFT_SCHEMA_VERSION_METADATA_KEY: &str = "x-legion-schema-version";

/// Handles `RaftTransport.Join` requests on the leader. Implemented by the
/// driver's `EngineJoinHandler` (token validation, learner add on every
/// group, caught-up wait, voter promotion); the transport only dispatches
/// to it.
#[tonic::async_trait]
pub(crate) trait RaftJoinHandler: Send + Sync + 'static {
    async fn join(
        &self,
        request: JoinRaftClusterRequest,
    ) -> Result<JoinRaftClusterReply, tonic::Status>;

    /// Answers `RaftTransport.FetchRootKey`: the sealed KV root key for an
    /// existing member that lacks it (R3, docs/kv-encryption.md).
    async fn fetch_root_key(
        &self,
        request: FetchRootKeyRequest,
    ) -> Result<FetchRootKeyReply, tonic::Status>;

    /// Answers `RaftTransport.TriggerElection`: campaign for every raft
    /// group this manager runs, now (the target half of `transfer-leader`).
    async fn trigger_election(
        &self,
        request: crate::structs::TriggerElectionRequest,
    ) -> Result<crate::structs::TriggerElectionReply, tonic::Status>;
}

/// Everything the `RaftTransport` tonic service needs to route an inbound
/// RPC: the shared cluster secret it authenticates against, one
/// `openraft::Raft` handle per active lane, and the join handler.
///
/// The driver builds this once at engine start and installs it into the
/// server's `OnceLock` (see `server::RaftTransportService`); until then the
/// service answers `Unavailable`. In the default `single` topology only
/// [`Lane::Control`] is present and carries all tables (plan §A3, JC-2).
pub(crate) struct RaftTransportRegistry {
    pub(crate) cluster_secret: String,
    pub(crate) lanes: HashMap<Lane, openraft::Raft<types::TypeConfig>>,
    pub(crate) join_handler: Option<Arc<dyn RaftJoinHandler>>,
    /// Store handle for executing forwarded lane writes
    /// (`ForwardLaneWrite`, plan §8 D2). `None` in transport-only bring-ups
    /// (tests), where forwarding answers `Unimplemented`.
    pub(crate) forward_store: Option<Store>,
    /// The schema version this node advertises on every lane reply
    /// (`x-legion-schema-version`). Always `migrations::schema_version()`
    /// outside tests; the in-process mixed-version tests lower it through
    /// `Store::raft_set_advertised_schema_version` to stand in for an older
    /// build.
    pub(crate) schema_version: Arc<std::sync::atomic::AtomicU32>,
}

impl RaftTransportRegistry {
    /// Looks up the raft instance for a wire `group_id`.
    pub(crate) fn lane_by_wire_name(
        &self,
        group_id: &str,
    ) -> Option<&openraft::Raft<types::TypeConfig>> {
        self.lanes.get(&Lane::from_wire_name(group_id)?)
    }

    /// The raft GROUP that carries `lane` on this manager: the lane itself
    /// under `lanes` topology, and the one control group under `single`,
    /// where every lane rides it. This is the space a `ForwardLaneWrite`
    /// envelope's `group_id` lives in — senders address the group that
    /// commits the op, not the op's logical lane — so a receiver must
    /// compare in the same space.
    pub(crate) fn group_for(&self, lane: Lane) -> Lane {
        physical_group(self.lanes.len(), lane)
    }
}

/// The raft group that commits `lane` when `active_groups` groups run:
/// `single` topology has exactly one (control), which carries every lane.
/// Shared by the forwarding sender and receiver so they can never disagree
/// about which space a `group_id` is in.
pub(crate) fn physical_group(active_groups: usize, lane: Lane) -> Lane {
    if active_groups <= 1 {
        Lane::Control
    } else {
        lane
    }
}

impl std::fmt::Debug for RaftTransportRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftTransportRegistry")
            .field("lanes", &self.lanes.keys().collect::<Vec<_>>())
            .field("has_join_handler", &self.join_handler.is_some())
            .finish_non_exhaustive()
    }
}

/// The payload proposed to a lane's raft group: the exact row-level effects
/// (a SQLite session changeset) of one store operation, captured on the
/// leader. `op` is metadata for observability only — application is driven
/// entirely by the changeset bytes (JC-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoreCommand {
    pub(crate) lane: Lane,
    pub(crate) op: String,
    /// Base64 on the wire and in `log.sqlite` (both serde_json): see
    /// [`types::wire_bytes`] — a raw `Vec<u8>` would render as a JSON
    /// number array, ~4x larger.
    #[serde(with = "crate::storage::raft::types::wire_bytes")]
    pub(crate) changeset: Vec<u8>,
}

/// Raft state-machine response for a [`StoreCommand`]. Deliberately empty:
/// return values never cross raft — the proposer computed the closure's
/// return value at capture time and only waits for the commit.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub(crate) struct StoreResponse {}

/// A type-erased replicated operation: the public store method's body,
/// boxed so it can cross the channel to the lane's pipeline thread. The
/// `Box<dyn Any>` carries the closure's typed return value back to the
/// caller (downcast in [`Store::replicated`]).
pub(crate) type ReplicatedOp =
    Box<dyn FnOnce(&Store) -> R<Box<dyn std::any::Any + Send>> + Send + 'static>;

/// Signature of the pipeline's submit entry point: blocks the calling thread
/// until the operation is captured, committed by quorum, and applied
/// locally, then returns the closure's capture-time return value.
pub(crate) type SubmitFn =
    dyn Fn(Lane, &'static str, ReplicatedOp) -> R<Box<dyn std::any::Any + Send>> + Send + Sync;

/// Handle to the replication pipeline, installed on the store when the raft
/// engine starts (phase A-1).
pub(crate) struct ReplicationHandle {
    pub(crate) submit: Box<SubmitFn>,
}

impl std::fmt::Debug for ReplicationHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationHandle").finish_non_exhaustive()
    }
}
