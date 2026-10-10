//! `MultiRaftDriver` — the real raft engine behind `persistence.type = raft`
//! (phase A-1 of `docs/plans/durability-and-ops-execution-plan.md` §2).
//!
//! The driver owns one `openraft::Raft` per active lane (topology `single`:
//! one group, keyed [`Lane::Control`], carries every table; topology
//! `lanes`: three independent groups), a dedicated tokio runtime the raft
//! core tasks run on, and one **pipeline OS thread per lane** (JC-3) that
//! executes replicated store mutations:
//!
//! ```text
//! caller (any thread) ── job ──▶ pipeline thread:
//!     capture_mutation(store, f)         # savepoint + session, rolled back
//!     raft.client_write(StoreCommand)    # quorum commit; local apply done
//!     reply(value)                       # caller unblocks: read-your-writes
//! ```
//!
//! Because the pipeline thread serializes capture → propose → local-apply
//! per lane, every capture runs against the applied result of the previous
//! operation — the ordering invariant changeset replication depends on.
//!
//! # Runtime ownership and shutdown
//!
//! The engine's runtime is owned by the driver and shut down when the last
//! `Store` clone drops. To make that reachable, every `Store` clone handed
//! to the engine (state machines, pipeline threads) is a *detached* clone
//! ([`Store::raft_engine_clone`]): it shares connections/read-model/event
//! channels but holds neither the driver nor the replication handle, so no
//! reference cycle keeps the engine alive.
//!
//! # Cluster identity
//!
//! The cluster secret (JC-7) is minted at bootstrap and persisted to
//! `<data_dir>/cluster_secret`; the cluster id is derived from the secret
//! (`legion-raft-<fnv1a64(secret)>`), so every node sharing the secret
//! reports the same cluster id with zero extra coordination. Join tokens
//! are base64url(serde_json) payloads carrying `{cluster_id, secret,
//! leader_advertise_addr, expires_unix}` — the embedded secret *is* the
//! credential (validated by constant-time comparison plus expiry on the
//! leader), so no separate signature is needed; token confidentiality
//! equals secret confidentiality, the documented JC-7 stopgap posture.

use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{BasicNode, RaftMetrics, ServerState, SnapshotPolicy};
use rand::Rng;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::prelude::*;
use crate::storage::persistence::{
    PersistenceDriver, PersistenceRole, PersistenceStatus, RaftCommandOutcome, RaftHistoryEvent,
    RaftLaneStatus, RaftStatus,
};
use crate::storage::raft::log_store::RaftLogStore;
use crate::storage::raft::network::{ChannelPool, RaftTransportNetworkFactory};
use crate::storage::raft::state_machine::StoreStateMachine;
use crate::storage::raft::types::{
    encode_node_addr, fnv1a64, node_addr_host_port, split_node_addr, TypeConfig,
};
use crate::storage::raft::{
    capture, quorum, Lane, RaftJoinHandler, RaftTransportRegistry, ReplicatedOp, ReplicationHandle,
    StoreCommand, RAFT_CLUSTER_SECRET_METADATA_KEY,
};
use crate::storage::store::Store;
use crate::structs::{
    JoinRaftClusterReply, JoinRaftClusterRequest, RaftGroupMembership, RaftMemberInfo,
    RaftPersistenceConfigFull,
};

type NodeId = u64;
type Raft = openraft::Raft<TypeConfig>;

/// How long engine start waits for a leader to be known when this node is
/// (or was) a cluster member. A single-voter bootstrap elects itself
/// immediately; a restarted member re-elects within one election timeout.
const LEADER_WAIT: Duration = Duration::from_secs(15);
/// Ring-buffer capacity of the in-memory raft history (same as the
/// simulation-era driver kept).
const HISTORY_CAPACITY: usize = 512;
// openraft timing knobs.
//
// The engine reads these from config — `raft.heartbeatIntervalMs`,
// `raft.electionTimeoutMinMs`, `raft.electionTimeoutMaxMs` — off
// `self.cfg` where the openraft `Config` is built below. The defaults live
// in `crate::structs::config` next to the operator-facing field docs
// (`DEF_RAFT_HEARTBEAT_INTERVAL_MS` = 250, `DEF_RAFT_ELECTION_TIMEOUT_MIN_MS`
// = 600, `DEF_RAFT_ELECTION_TIMEOUT_MAX_MS` = 1200), and config load rejects
// a combination that would flap — see
// `crate::storage::raft::quorum::validate_raft_config`. Raise them for
// loaded managers, WAN links, virtualised or noisy hosts, and CI; the
// in-process multi-node test harnesses under tests/ do exactly that.
//
// Why the defaults are not the multi-raft plan's 50 / 150-300 ms table:
// that is unshippable, for a reason that is not about elections at all.
// openraft uses `heartbeat_interval` as the **RPC deadline for
// append-entries** (which doubles as the heartbeat). Legion replicates row
// changesets, so one entry can be tens of KiB and a batch several MiB —
// and a batch that cannot be encoded, sent, decoded, and answered inside
// the deadline is retried verbatim forever. Observed at 50 ms: a follower
// behind a compacted log received the same 1.4 MiB batch indefinitely, the
// leader never saw the conflict reply, so it never fell back to
// install-snapshot and the follower could NEVER catch up (adversarial
// finding, 2026-07-24; pinned by
// `snapshot_larger_than_the_old_transport_ceiling_installs`).
//
// 250 ms gives a worst-case `MAX_PAYLOAD_ENTRIES` batch ~2.5x headroom in a
// debug build. The election window is then kept as TIGHT as that allows
// (600-1200 ms, ~2.5-5x the heartbeat) rather than scaled up
// proportionally, because openraft derives the follower *leader lease* from
// `election_timeout_max`: a live leader refreshes the lease on every
// heartbeat, so a larger max makes every deliberate leadership move
// (`trigger_lane_election`, the daemon's lane-convergence loop) take
// proportionally longer to win. Failover still completes in ~1s, in the
// range of Consul's and etcd's defaults and far below any
// operator-visible timeout.

/// Per-chunk deadline for install-snapshot RPCs.
///
/// openraft's default is **200 ms**, which is a hard ceiling on snapshot
/// size, not a liveness knob: the deadline covers encoding a
/// [`network::SNAPSHOT_CHUNK_BYTES`] chunk, pushing it over the wire, and —
/// for the final chunk — the receiver's actual state-machine install. A
/// multi-megabyte snapshot cannot make 200 ms even on loopback, and the
/// timeout surfaces as an `Unreachable` peer, so openraft simply retries the
/// same chunk forever: a follower past the compaction horizon can NEVER
/// catch up and the cluster silently loses the ability to add or recover
/// members (adversarial finding, 2026-07-24 — reproduced by
/// `snapshot_larger_than_the_old_transport_ceiling_installs`).
///
/// 30s is sized for the worst realistic case: a 4 MiB chunk over a slow
/// link plus a whole-DB image install and read-model rebuild on the
/// receiver. The cost of the larger value is only how long a snapshot RPC
/// to a *dead* peer takes to fail; leader liveness still rides on
/// heartbeats, so elections are unaffected.
const INSTALL_SNAPSHOT_TIMEOUT_MS: u64 = 30_000;

/// Entries per append-entries RPC.
///
/// The companion knob to the heartbeat interval: since that interval is
/// also the append-entries deadline, the *batch* has to be small enough to
/// deliver inside it. Legion's entries are row changesets — individually
/// tens of KiB is normal — so openraft's default of 300 entries per batch
/// can reach double-digit megabytes and blow any sane deadline. 32 bounds a
/// worst-case batch to a few MiB while still amortizing round trips
/// (catching a follower up by 10k entries is ~300 sequential RPCs, seconds
/// of work, and the common case of small entries is unaffected).
const MAX_PAYLOAD_ENTRIES: u64 = 32;

/// The **anchor lane** for co-located lane leadership (JC-40): whichever
/// node leads this lane campaigns to acquire the others, so one node ends
/// up leading every lane. Control is the anchor because it is the lane
/// every cross-lane composite starts on (registration, submit, destroy,
/// node remove) and the lane the Join handler must run on.
pub(crate) const ANCHOR_LANE: Lane = Lane::Control;

/// Convergence campaign timing (JC-40), expressed in **election windows** —
/// multiples of the configured `raft.electionTimeoutMaxMs`, which is also
/// openraft's follower leader lease and the unit every natural election is
/// measured in. Scaling with that knob is deliberate: raising it is the
/// documented remedy for loaded managers, WAN links and CI, and a schedule
/// in fixed milliseconds would silently start campaigning *inside* the lease
/// the moment an operator applied that remedy.
///
/// Spacing before the FIRST campaign for a lane that has strayed, and the
/// floor the per-lane exponential backoff starts from.
const LANE_CONVERGE_BACKOFF_MIN_WINDOWS: u32 = 2;
/// Ceiling on the per-lane campaign backoff: 2 → 4 → 8 → 16 windows.
const LANE_CONVERGE_BACKOFF_MAX_WINDOWS: u32 = 16;
/// How long a campaign has to make this node the lane's leader before it is
/// scored as LOST. One full election window plus slack: a campaign that has
/// not won by then lost the race (or the incumbent refused to yield), and
/// re-issuing it before the verdict would only discard the votes the first
/// one collected.
const LANE_CONVERGE_VERDICT_WINDOWS: u32 = 2;
/// Consecutive lost campaigns before this node stands DOWN from converging
/// the lane at all for [`LANE_CONVERGE_COOLDOWN_WINDOWS`].
const LANE_CONVERGE_MAX_CAMPAIGNS: u32 = 3;
/// The stand-down window. Deliberately enormous next to an election window
/// (60x) — the point is that a lane whose convergence keeps failing gets
/// long, *uninterrupted* stretches in which raft's own randomized elections
/// run without a single foreign term bump, on hardware far slower than the
/// box the timings were chosen on.
const LANE_CONVERGE_COOLDOWN_WINDOWS: u32 = 60;
/// How many times the stand-down window may DOUBLE for a lane this node
/// keeps failing to take (60 → 480 election windows). A lane that refuses
/// convergence three times over is telling us its incumbent is the better
/// leader; further campaigns are pure term inflation on a lane that serves
/// fine, so they get rarer and rarer. Any win resets it.
const LANE_CONVERGE_COOLDOWN_MAX_DOUBLINGS: u32 = 3;

/// Poll cadence of the convergence loop. Cheap: one metrics-watch borrow per
/// lane, no I/O. The campaign SCHEDULE lives in the per-lane governor, not in
/// this interval.
const LANE_CONVERGE_IDLE_POLL: Duration = Duration::from_millis(500);

/// Ceiling on CONSECUTIVE "catching up" holds before a campaign may proceed
/// anyway (~12s at the 500ms poll cadence).
///
/// The catching-up signal is "the lane's log grew between two polls" — the
/// only log-currency signal openraft 0.9 gives a non-leader. But log growth
/// cannot distinguish *catch-up replay* from *ordinary replication traffic*,
/// so on a lane with sustained writes (status-lane heartbeats, a busy
/// control lane) the hold fires on every poll and convergence starves
/// forever — the standing split becomes the steady state exactly under the
/// load lanes exist for (adversarial finding, 2026-07-28). Capping the hold
/// keeps its value for the genuine case (bulk catch-up after a restart or
/// snapshot install finishes within a few polls) while guaranteeing a lane
/// under steady traffic still gets campaigned for. A campaign issued while
/// genuinely behind loses and costs the lane roughly one election window
/// (the campaigner's raised term deposes the incumbent via the replication
/// stream — openraft 0.9 has no pre-vote), which the verdict/backoff/
/// stand-down ladder above bounds to a small duty cycle; starvation has no
/// bound at all.
const LANE_CONVERGE_CATCHUP_HOLD_MAX_POLLS: u32 = 24;

/// How long a join's per-lane membership change waits for the anchor
/// leader's convergence loop to acquire a lane it does not lead yet (JC-40),
/// and how often it re-tries inside that window. Bounded so a join against a
/// genuinely wrong node still fails fast with a leader hint instead of
/// hanging the joiner's RPC.
const JOIN_LANE_CONVERGENCE_WAIT: Duration = Duration::from_secs(20);
const JOIN_LANE_RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// The lanes a topology activates. `single` (the v1 default, JC-2) runs one
/// group — keyed [`Lane::Control`] — that carries every table; `lanes` runs
/// the three independent groups.
pub(crate) fn lanes_for_topology(topology: &str) -> R<Vec<Lane>> {
    match topology {
        "" | "single" => Ok(vec![Lane::Control]),
        "lanes" => Ok(Lane::ALL.to_vec()),
        other => bail!("unknown raft topology `{other}` (expected \"single\" or \"lanes\")"),
    }
}

/// Take the exclusive advisory lock guarding `data_dir` against a second
/// concurrently running manager process. Without it, two processes pointed
/// at the same raft `data_dir` (an operator double-start) would both run
/// full raft cores against the same `log.sqlite` — two in-memory cores
/// clobbering one durable log/vote file — long before the manager-port bind
/// of the second process fails.
///
/// The lock is a dedicated SQLite database (`<data_dir>/lock.sqlite`) opened
/// with `locking_mode=EXCLUSIVE`: the first write acquires an exclusive OS
/// file lock that SQLite holds until the connection closes (process exit),
/// and a second process fails immediately (zero busy timeout) instead of
/// starting an engine. This piggybacks on SQLite's cross-platform locking —
/// no new dependency, works wherever the log store works.
fn acquire_data_dir_lock(data_dir: &Path) -> R<rusqlite::Connection> {
    let path = data_dir.join("lock.sqlite");
    let conn = rusqlite::Connection::open(&path)
        .with_context(|| format!("failed opening raft data dir lock {}", path.display()))?;
    conn.busy_timeout(Duration::from_millis(0))?;
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS holder (pid INTEGER NOT NULL);
         DELETE FROM holder;",
    )
    .and_then(|()| {
        conn.execute(
            "INSERT INTO holder (pid) VALUES (?1);",
            [std::process::id() as i64],
        )
        .map(|_| ())
    })
    .map_err(|e| {
        anyhow!(
            "another legion manager process appears to be running against raft data dir {} \
             (failed acquiring exclusive lock on {}: {e}); refusing to start a second raft \
             engine over the same log store",
            data_dir.display(),
            path.display()
        )
    })?;
    Ok(conn)
}

/// Key under which the state DB is stamped with the cluster identity it
/// belongs to (see [`check_cluster_stamp`]).
const CLUSTER_STAMP_KEY: &str = "cluster_id";

/// Bind the state DB to its cluster identity. On first engine start the DB
/// is stamped with the derived cluster id; on every later start the stamp
/// must match. This closes the split-brain path where an operator loses the
/// raft `data_dir` (secret file included) but keeps the state DB and
/// restarts with `bootstrap=true`: a fresh secret would silently mint a NEW
/// single-voter cluster over the OLD cluster's data while the surviving
/// members keep running the old cluster. `force_new_cluster=true` is the
/// explicit operator override that re-stamps the DB.
fn check_cluster_stamp(store: &Store, cluster_id: &str, force_new_cluster: bool) -> R<()> {
    use rusqlite::OptionalExtension;
    // Engine start runs before `set_raft_mode` arms the write guard, so a
    // direct write is legal here (this is node-local identity, never
    // replicated: every member independently stamps the same derived id).
    let conn = store.write_connection()?;
    let existing: Option<String> = conn
        .query_row(
            "SELECT value FROM raft_cluster WHERE key = ?1;",
            [CLUSTER_STAMP_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("failed reading raft cluster stamp")?;
    match existing {
        Some(stamped) if stamped == cluster_id => Ok(()),
        Some(stamped) if force_new_cluster => {
            warn!(
                old_cluster = %stamped,
                new_cluster = %cluster_id,
                "forceNewCluster set: re-stamping state DB onto a different raft cluster \
                 identity — make sure the old cluster is fully retired or this manager will \
                 split-brain against it"
            );
            conn.execute(
                "INSERT INTO raft_cluster (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
                rusqlite::params![CLUSTER_STAMP_KEY, cluster_id],
            )?;
            Ok(())
        }
        Some(stamped) => bail!(
            "this state DB belongs to raft cluster {stamped}, but this manager derived cluster \
             {cluster_id} (raft data dir wiped or a different clusterSecret?). Bootstrapping a \
             new cluster over an existing cluster's data would split-brain against any \
             surviving members. Restore the original raft data dir / clusterSecret, or set \
             persistence.raft.forceNewCluster=true to deliberately re-stamp this DB",
        ),
        None => {
            conn.execute(
                "INSERT INTO raft_cluster (key, value) VALUES (?1, ?2);",
                rusqlite::params![CLUSTER_STAMP_KEY, cluster_id],
            )?;
            Ok(())
        }
    }
}

/// Key under which the state DB is stamped with the raft topology it was
/// built under (see [`check_topology_stamp`]).
const TOPOLOGY_STAMP_KEY: &str = "topology";

/// The canonical spelling of a topology config value (`""` == `"single"`).
fn canonical_topology(topology: &str) -> &str {
    if topology.is_empty() {
        "single"
    } else {
        topology
    }
}

/// Bind the state DB to the topology its raft state was built under, the
/// JC-22-style companion to [`check_cluster_stamp`] (adversarial finding,
/// 2026-07-24). `config.rs` documents that the topology is fixed at first
/// engine start, but nothing enforced it: flipping `single` → `lanes` over
/// an existing data dir silently RESUMES the single-era control group —
/// whose log entries carry every lane's tables — alongside brand-new
/// scheduler/status groups that existing members never join (`had_any_state`
/// suppresses the join loop), yielding cross-lane replay onto tables the new
/// groups also write (the §7-C1 fatal shape), refused whole-DB snapshots,
/// and silently frozen scheduler/status state on members. First start stamps
/// the topology; every later start must match. `forceNewCluster=true` is the
/// explicit override for a deliberate rebuild and re-stamps.
fn check_topology_stamp(store: &Store, topology: &str, force_new_cluster: bool) -> R<()> {
    use rusqlite::OptionalExtension;
    let topology = canonical_topology(topology);
    // Engine start runs before `set_raft_mode` arms the write guard; the
    // stamp is node-local identity, never replicated.
    let conn = store.write_connection()?;
    let existing: Option<String> = conn
        .query_row(
            "SELECT value FROM raft_cluster WHERE key = ?1;",
            [TOPOLOGY_STAMP_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("failed reading raft topology stamp")?;
    match existing.as_deref() {
        Some(stamped) if stamped == topology => Ok(()),
        Some(stamped) if force_new_cluster => {
            warn!(
                old_topology = %stamped,
                new_topology = %topology,
                "forceNewCluster set: re-stamping state DB onto a different raft topology — \
                 the old topology's raft logs/snapshots in the data dir must be gone (fresh \
                 bootstrap), or the groups will replay entries built under the other topology"
            );
            conn.execute(
                "INSERT INTO raft_cluster (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
                rusqlite::params![TOPOLOGY_STAMP_KEY, topology],
            )?;
            Ok(())
        }
        Some(stamped) => bail!(
            "raft topology mismatch: this state DB's raft state was built under topology \
             `{stamped}`, but this manager is configured with topology `{topology}`. There is \
             no in-place migration between topologies (plan JC-43): the stamped topology's raft \
             log carries entries the configured topology's groups would mis-replay, and \
             existing members never join the newly-configured groups — those lanes would come \
             up blank and silently diverge. Either put `persistence.raft.topology` back to \
             \"{stamped}\", or migrate deliberately the JC-6 way: `legion state backup --out \
             <PATH>` on the leader, stop every manager, then bootstrap a fresh single node with \
             the new topology, an EMPTY raft data dir, and `--restore-from <PATH>` (a clean \
             data dir is required — `persistence.raft.forceNewCluster=true` re-stamps this DB \
             but does NOT make the old logs replayable)",
        ),
        None => {
            conn.execute(
                "INSERT INTO raft_cluster (key, value) VALUES (?1, ?2);",
                rusqlite::params![TOPOLOGY_STAMP_KEY, topology],
            )?;
            Ok(())
        }
    }
}

/// Name of the topology stamp file inside the raft data dir (JC-43).
/// Sits next to `cluster_secret` and is written the same way.
const TOPOLOGY_STAMP_FILE: &str = "topology.json";

/// What [`TOPOLOGY_STAMP_FILE`] records: the topology this data dir's raft
/// state was built under, plus the exact lane set that topology activated.
///
/// The lane set is stamped separately from the topology name rather than
/// re-derived from it, so that if a future release changes which lanes a
/// topology activates, an existing data dir is caught by the same wall
/// instead of quietly bringing up a group whose log does not exist.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TopologyStamp {
    topology: String,
    lanes: Vec<String>,
}

impl TopologyStamp {
    fn new(topology: &str, lanes: &[Lane]) -> Self {
        Self {
            topology: canonical_topology(topology).to_string(),
            lanes: lanes.iter().map(|l| l.wire_name().to_string()).collect(),
        }
    }

    fn lane_list(&self) -> String {
        if self.lanes.is_empty() {
            "<none>".to_string()
        } else {
            self.lanes.join(", ")
        }
    }
}

/// Bind a raft **data dir** to the topology it was bootstrapped with
/// (JC-43). This is the authoritative stamp: the data dir is where the
/// per-lane logs and snapshots live, so it is the thing that actually
/// cannot be reinterpreted under a different topology.
///
/// Switching `single` ↔ `lanes` over an existing data dir resumes the
/// stamped topology's groups — whose logs carry entries the new topology's
/// groups never wrote — and brings the *new* groups up blank, with no
/// existing member ever joining them (a restarted member suppresses its own
/// join loop once it has any state). The result is silently frozen lanes on
/// some members and cross-lane replay on others; nothing about it is
/// recoverable after the fact, so it is refused before the first log write.
///
/// A missing stamp writes one (fresh bootstrap, or a data dir predating
/// this check). There is deliberately NO `forceNewCluster` override: the
/// supported migration (JC-6's posture for engine changes) is
/// backup → empty data dir → restore, and an empty data dir has no stamp to
/// conflict with.
fn check_data_dir_topology_stamp(data_dir: &Path, topology: &str, lanes: &[Lane]) -> R<()> {
    let path = data_dir.join(TOPOLOGY_STAMP_FILE);
    let configured = TopologyStamp::new(topology, lanes);
    if path.exists() {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed reading raft topology stamp {}", path.display()))?;
        let stamped: TopologyStamp = serde_json::from_str(&raw).with_context(|| {
            format!(
                "raft topology stamp {} is corrupt; it records which topology this data dir's \
                 logs and snapshots were built under and cannot be guessed",
                path.display()
            )
        })?;
        let same_lanes = {
            let mut a = stamped.lanes.clone();
            let mut b = configured.lanes.clone();
            a.sort();
            b.sort();
            a == b
        };
        if stamped.topology == configured.topology && same_lanes {
            return Ok(());
        }
        bail!(topology_mismatch_message(
            &format!(
                "raft data dir {} was bootstrapped under topology `{}` (lanes: {})",
                data_dir.display(),
                stamped.topology,
                stamped.lane_list()
            ),
            &format!(
                "this manager is configured with topology `{}` (lanes: {})",
                configured.topology,
                configured.lane_list()
            ),
            &stamped.topology,
            data_dir,
        ));
    }
    let json = serde_json::to_vec_pretty(&configured)
        .context("failed encoding the raft topology stamp")?;
    std::fs::write(&path, &json)
        .with_context(|| format!("failed writing raft topology stamp {}", path.display()))?;
    info!(
        path = %path.display(),
        topology = %configured.topology,
        lanes = %configured.lane_list(),
        "Stamped raft data dir with its topology (JC-43: immutable for the life of the dir)"
    );
    Ok(())
}

/// The single actionable message for every topology-mismatch refusal —
/// start-time (data dir stamp, state DB stamp) and join-time (a joiner
/// configured differently from the cluster it is enrolling in). Naming both
/// sides and the ONE supported path matters more than brevity here: the
/// alternative outcomes are silent divergence, so an operator who hits this
/// needs to know exactly what to type.
fn topology_mismatch_message(
    stamped_side: &str,
    configured_side: &str,
    stamped_topology: &str,
    data_dir: &Path,
) -> String {
    format!(
        "raft topology mismatch: {stamped_side}, but {configured_side}. There is no in-place \
         migration between topologies (plan JC-43): the stamped topology's raft logs and \
         snapshots carry entries the configured topology's groups would mis-replay, and \
         existing members never join the newly-configured groups — those lanes would come up \
         blank and silently diverge. Either put `persistence.raft.topology` back to \
         \"{stamped_topology}\", or migrate deliberately the JC-6 way: (1) `legion state \
         backup --out <PATH>` against the current leader, (2) stop every manager, (3) start a \
         fresh single-node bootstrap with the new topology and an EMPTY raft data dir (delete \
         or move {}), passing `--restore-from <PATH>`, then (4) re-join the other managers \
         with a fresh join token.",
        data_dir.display()
    )
}

/// Payload of a `legion raft join-token` token. See the module docs for the
/// encoding and trust model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinTokenPayload {
    pub(crate) cluster_id: String,
    pub(crate) secret: String,
    pub(crate) leader_advertise_addr: String,
    pub(crate) expires_unix: i64,
}

const JOIN_TOKEN_PREFIX: &str = "lgnraft_";

impl JoinTokenPayload {
    pub(crate) fn encode(&self) -> R<String> {
        let json = serde_json::to_vec(self).context("failed encoding raft join token")?;
        Ok(format!(
            "{JOIN_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(json)
        ))
    }

    pub(crate) fn decode(token: &str) -> R<JoinTokenPayload> {
        let body = token
            .strip_prefix(JOIN_TOKEN_PREFIX)
            .context("raft join token is missing the lgnraft_ prefix")?;
        let json = URL_SAFE_NO_PAD
            .decode(body)
            .context("raft join token is not valid base64url")?;
        serde_json::from_slice(&json).context("raft join token payload is malformed")
    }

    pub(crate) fn is_expired(&self, now_unix: i64) -> bool {
        now_unix >= self.expires_unix
    }
}

/// Typed "this node is not the leader" error produced when a proposal is
/// rejected with `ForwardToLeader`. Carries the leader hint so the RPC layer
/// can map it to `FAILED_PRECONDITION` with actionable metadata (plan §A4,
/// JC-4). Under the `lanes` topology different lanes can have different
/// leaders, so the hint names WHICH lane's leader to contact (`lane` is
/// `None` in `single` topology, where one group has the one leader).
#[derive(Debug, Clone)]
pub struct NotRaftLeader {
    /// Wire name of the raft lane whose leader must be contacted, when the
    /// topology runs more than one group.
    pub lane: Option<String>,
    /// Manager string id of the current leader, when known.
    pub leader_name: Option<String>,
    /// `host:port` of the current leader, when known.
    pub leader_addr: Option<String>,
}

impl std::fmt::Display for NotRaftLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scope = match &self.lane {
            Some(lane) => format!(" for the {lane} lane"),
            None => String::new(),
        };
        match (&self.leader_name, &self.leader_addr) {
            (Some(name), Some(addr)) => write!(
                f,
                "this manager is not the raft leader{scope}; retry against its leader {name} at {addr}"
            ),
            (Some(name), None) => write!(
                f,
                "this manager is not the raft leader{scope}; retry against its leader {name}"
            ),
            _ => write!(
                f,
                "this manager is not the raft leader{scope} and no leader is currently known; retry shortly"
            ),
        }
    }
}

impl std::error::Error for NotRaftLeader {}

impl NotRaftLeader {
    /// Whether `err`'s cause chain carries a typed [`NotRaftLeader`]
    /// rejection. The seam every tolerate-and-reconcile call site uses
    /// (plan §8, Workstream D): a composite's cross-lane half may treat
    /// exactly this error as "deferred to that lane's reconciler", while
    /// every other error keeps failing loudly.
    pub(crate) fn in_chain(err: &anyhow::Error) -> bool {
        err.chain()
            .any(|cause| cause.downcast_ref::<NotRaftLeader>().is_some())
    }
}

/// A replicated mutation queued to a lane's pipeline thread.
struct PipelineJob {
    /// The lane the *operation* was classified as (metadata carried in the
    /// proposed `StoreCommand`; in `single` topology it may differ from the
    /// group actually carrying it).
    lane: Lane,
    op: &'static str,
    f: ReplicatedOp,
    reply: std_mpsc::Sender<R<Box<dyn Any + Send>>>,
    /// When the submitter enqueued this job, so the pipeline thread can
    /// record how long it queued (`legion_raft_pipeline_wait_seconds`).
    /// One pipeline thread per lane serializes every write on that lane, so
    /// queue time is a first-class latency component and is invisible in
    /// the capture/propose histograms that only start once a job is picked
    /// up.
    queued_at: std::time::Instant,
}

/// In-memory operator-facing raft history ring (real transitions, recorded
/// by the per-lane metrics watchers plus operator commands).
struct HistoryState {
    events: VecDeque<RaftHistoryEvent>,
    next_event_id: u64,
}

impl HistoryState {
    fn new() -> Self {
        Self {
            events: VecDeque::with_capacity(HISTORY_CAPACITY),
            next_event_id: 1,
        }
    }
}

/// Everything that exists only after `start_engine`.
struct Engine {
    /// Owned runtime the raft core tasks (and metrics watchers) run on.
    /// Taken on drop so it can be shut down from either sync or async
    /// contexts.
    runtime: StdMutex<Option<tokio::runtime::Runtime>>,
    handle: tokio::runtime::Handle,
    lanes: HashMap<Lane, Raft>,
    cluster_id: String,
    cluster_secret: String,
    /// Shared outbound channel pool, kept so the partition test hook
    /// (`Store::raft_set_outbound_blocked`) can reach it after start.
    pool: Arc<ChannelPool>,
    /// Exclusive advisory lock on the raft data dir (see
    /// [`acquire_data_dir_lock`]); held for the engine's lifetime so a
    /// second manager process pointed at the same data dir fails fast
    /// instead of running a competing raft core over the same log store.
    _data_dir_lock: StdMutex<rusqlite::Connection>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.lock().ok().and_then(|mut r| r.take()) else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            // Dropping (or blocking on) a runtime inside another runtime
            // panics; background shutdown is the async-context escape hatch.
            runtime.shutdown_background();
        } else {
            // Sync context (normal daemon/test teardown): wait so the log
            // stores and state machines are fully released before a
            // successor engine opens the same data_dir.
            runtime.shutdown_timeout(Duration::from_secs(3));
        }
    }
}

/// The real multi-group raft persistence driver. See the module docs.
pub(crate) struct MultiRaftDriver {
    /// Manager string identity in the cluster (config `raft.nodeId`,
    /// defaulting to the manager `host:port` id).
    node_id: String,
    /// `fnv1a64(node_id)` — the openraft `NodeId`.
    raft_id: NodeId,
    cfg: RaftPersistenceConfigFull,
    /// `host:port` peers dial for this node's raft transport.
    advertise_addr: String,
    /// Registry cell shared with the `RaftTransport` tonic service
    /// (`server::RaftTransportService`); populated at engine start.
    registry: Arc<OnceLock<RaftTransportRegistry>>,
    engine: OnceLock<Engine>,
    history: Arc<StdMutex<HistoryState>>,
    /// Millis timestamp of the most recent observed leader change.
    election_epoch_ms: Arc<AtomicI64>,
    /// Whether the JC-40 anchor-lane convergence loop campaigns (default
    /// `true`). Only the lanes battery's leadership-independence test flips
    /// it — see [`MultiRaftDriver::set_lane_convergence`].
    lane_convergence: Arc<std::sync::atomic::AtomicBool>,
    /// Convergence campaigns this node has issued per lane since the engine
    /// started (JC-40). Exposed through `Store::raft_lane_campaigns` so the
    /// lanes battery can assert the bound on term inflation directly rather
    /// than inferring it from raft terms.
    lane_campaigns: Arc<StdMutex<HashMap<Lane, u64>>>,
}

impl std::fmt::Debug for MultiRaftDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiRaftDriver")
            .field("node_id", &self.node_id)
            .field("raft_id", &self.raft_id)
            .field("topology", &self.cfg.topology)
            .field("started", &self.engine.get().is_some())
            .finish_non_exhaustive()
    }
}

impl MultiRaftDriver {
    /// `manager_addr` is the manager's `host:port` (fallback advertise
    /// address when `raft.advertiseAddr` is unset).
    pub(crate) fn new(
        node_id: String,
        manager_addr: &str,
        cfg: &RaftPersistenceConfigFull,
    ) -> Self {
        let advertise_addr = cfg
            .advertise_addr
            .clone()
            .unwrap_or_else(|| normalize_advertise_addr(manager_addr));
        Self {
            raft_id: fnv1a64(&node_id),
            node_id,
            cfg: cfg.clone(),
            advertise_addr,
            registry: Arc::new(OnceLock::new()),
            engine: OnceLock::new(),
            history: Arc::new(StdMutex::new(HistoryState::new())),
            election_epoch_ms: Arc::new(AtomicI64::new(0)),
            lane_convergence: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            lane_campaigns: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// The registry cell the `RaftTransport` tonic service reads from. The
    /// daemon mounts the service around this cell at listener build time;
    /// the driver populates it at engine start.
    pub(crate) fn transport_registry_cell(&self) -> Arc<OnceLock<RaftTransportRegistry>> {
        self.registry.clone()
    }

    // ── cluster identity ─────────────────────────────────────────────────

    /// Resolve the cluster secret: explicit config, previously persisted
    /// file, join token, or (bootstrap only) freshly minted and persisted to
    /// `<data_dir>/cluster_secret`.
    fn load_or_mint_cluster_secret(&self, data_dir: &Path) -> R<String> {
        if let Some(secret) = &self.cfg.cluster_secret {
            let secret = secret.trim();
            if !secret.is_empty() {
                return Ok(secret.to_string());
            }
        }
        let secret_path = data_dir.join("cluster_secret");
        if secret_path.exists() {
            let secret = std::fs::read_to_string(&secret_path)
                .with_context(|| format!("failed reading {}", secret_path.display()))?;
            let secret = secret.trim().to_string();
            if !secret.is_empty() {
                return Ok(secret);
            }
        }
        if let Some(token) = &self.cfg.join_token {
            let payload = JoinTokenPayload::decode(token)?;
            persist_cluster_secret(&secret_path, &payload.secret)?;
            return Ok(payload.secret);
        }
        if self.cfg.bootstrap {
            let secret: String = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(48)
                .map(char::from)
                .collect();
            persist_cluster_secret(&secret_path, &secret)?;
            info!(path = %secret_path.display(), "Minted raft cluster secret at bootstrap");
            return Ok(secret);
        }
        bail!(
            "raft cluster secret unavailable: set persistence.raft.clusterSecret, provide a \
             joinToken, or start the first manager with bootstrap=true (secret file checked at {})",
            secret_path.display()
        )
    }

    // ── status helpers ───────────────────────────────────────────────────

    /// Metrics of the headline group: `Lane::Control` exists in both
    /// topologies and is the one the single-struct `RaftStatus` reports.
    fn control_metrics(engine: &Engine) -> Option<RaftMetrics<NodeId, BasicNode>> {
        engine
            .lanes
            .get(&Lane::Control)
            .map(|raft| raft.metrics().borrow().clone())
    }

    fn push_history(
        history: &StdMutex<HistoryState>,
        event_type: &str,
        message: String,
        actor: &str,
        target: Option<String>,
        term: u64,
        leader_id: Option<String>,
    ) -> RaftHistoryEvent {
        let created_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string());
        let mut state = history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let event = RaftHistoryEvent {
            event_id: state.next_event_id,
            event_type: event_type.to_string(),
            message,
            actor: actor.to_string(),
            target,
            term,
            leader_id,
            created_at,
        };
        state.next_event_id += 1;
        if state.events.len() == HISTORY_CAPACITY {
            state.events.pop_front();
        }
        state.events.push_back(event.clone());
        event
    }

    /// Resolve an operator-supplied member target to `(name, Option<addr>)`.
    /// Accepts `"name@host:port"` (and `"name=host:port"`, the peers-config
    /// spelling) directly; a bare `"name"` resolves its address from the
    /// configured `raft.peers` entries when possible.
    fn resolve_target(&self, target: &str) -> (String, Option<String>) {
        for sep in ['@', '='] {
            if let Some((name, addr)) = target.rsplit_once(sep) {
                return (name.to_string(), Some(addr.to_string()));
            }
        }
        for peer in &self.cfg.peers {
            if let Some((name, addr)) = peer.split_once('=') {
                if name == target {
                    return (target.to_string(), Some(addr.to_string()));
                }
            }
        }
        (target.to_string(), None)
    }

    /// Run `fut` to completion on the engine runtime from any calling
    /// context. Operator commands arrive on tonic worker threads where
    /// `Handle::block_on` would panic, so the future is driven from a scoped
    /// helper thread instead (these commands are rare; a thread per call is
    /// fine).
    fn block_on_engine<F>(engine: &Engine, fut: F) -> R<F::Output>
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        let handle = engine.handle.clone();
        std::thread::scope(|scope| {
            scope
                .spawn(move || handle.block_on(fut))
                .join()
                .map_err(|_| anyhow!("raft command thread panicked"))
        })
    }

    /// Actively probe one raft member's transport listener: a fresh dial
    /// (TCP + h2 handshake) with [`MEMBER_PROBE_TIMEOUT`]. Used by the
    /// remove-member feasibility guard, where "alive" must mean "would ack
    /// a membership entry right now", not "acked something at some point
    /// under this leadership". Deliberately NOT the shared [`ChannelPool`]:
    /// its `connect_lazy` channels hide connection state, and the probe
    /// exists precisely to observe it.
    fn probe_member_reachable(engine: &Engine, host_port: &str) -> bool {
        let Ok(endpoint) = tonic::transport::Endpoint::from_shared(format!("http://{host_port}"))
        else {
            return false;
        };
        let endpoint = endpoint
            .connect_timeout(MEMBER_PROBE_TIMEOUT)
            .timeout(MEMBER_PROBE_TIMEOUT);
        Self::block_on_engine(engine, async move { endpoint.connect().await.is_ok() })
            .unwrap_or(false)
    }

    /// One lane's membership change, bounded by
    /// [`MEMBERSHIP_CHANGE_TIMEOUT`] and mapped to an operator-actionable
    /// error. On timeout the change may still be in flight — the error says
    /// what state that leaves the lane in and how to get out of it.
    fn membership_change_with_timeout<F>(
        engine: &Engine,
        command: &str,
        lane: Lane,
        target: &str,
        change: F,
    ) -> R<()>
    where
        F: std::future::Future<
                Output = Result<
                    openraft::raft::ClientWriteResponse<TypeConfig>,
                    RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
                >,
            > + Send,
    {
        let outcome = Self::block_on_engine(engine, async move {
            tokio::time::timeout(MEMBERSHIP_CHANGE_TIMEOUT, change).await
        })?;
        match outcome {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(map_write_error(command, Some(lane.wire_name()), e)
                .context(format!("on lane {}", lane.wire_name()))),
            Err(_elapsed) => bail!(
                "membership change removing {target} on lane {} did not commit within {}s. \
                 The most likely cause is that the RESULTING voter set cannot assemble a live \
                 quorum (a remaining voter died between the reachability probe and the \
                 commit), which leaves the lane in a joint configuration that keeps serving \
                 reads but cannot commit writes. Restore a remaining voter and the change \
                 will commit on its own; re-running the command after that is safe \
                 (membership changes are idempotent)",
                lane.wire_name(),
                MEMBERSHIP_CHANGE_TIMEOUT.as_secs()
            ),
        }
    }

    fn engine(&self) -> R<&Engine> {
        self.engine
            .get()
            .context("raft engine is not started on this manager")
    }

    /// Cheap leadership probe for a lane: whether this node currently leads
    /// the raft group carrying `lane` (one metrics-watch borrow, no I/O).
    /// In `single` topology every lane maps to the one `control` group, so
    /// all lanes answer identically. `false` before the engine starts.
    ///
    /// Used by the daemon's leader-only loops (eval workers, node status
    /// checker — plan JC-15): followers poll this and idle until a
    /// leadership change makes it flip.
    pub(crate) fn is_leader(&self, lane: Lane) -> bool {
        self.is_leader_inner(lane)
    }

    /// The typed not-a-leader error with the current leader hint filled in
    /// from the metrics of the group carrying `lane` (plan §A4, JC-4; per-
    /// lane leaders under `lanes` topology). Used by leader-only operations
    /// that are not raft proposals themselves (e.g. `Store::state_backup`),
    /// so they reject on non-leaders with the same actionable error shape a
    /// rejected write gets.
    pub(crate) fn not_leader_error(&self, lane: Lane) -> NotRaftLeader {
        let Some(engine) = self.engine.get() else {
            return NotRaftLeader {
                lane: None,
                leader_name: None,
                leader_addr: None,
            };
        };
        let group = if engine.lanes.len() == 1 {
            Lane::Control
        } else {
            lane
        };
        let lane_label = (engine.lanes.len() > 1).then(|| group.wire_name().to_string());
        match engine.lanes.get(&group) {
            Some(raft) => {
                let metrics = raft.metrics().borrow().clone();
                NotRaftLeader {
                    lane: lane_label,
                    ..not_leader_from_metrics(&metrics)
                }
            }
            None => NotRaftLeader {
                lane: lane_label,
                leader_name: None,
                leader_addr: None,
            },
        }
    }

    /// Forward one lane write to that lane's current leader over the
    /// `RaftTransport.ForwardLaneWrite` RPC (plan §8, D2). Returns the op's
    /// JSON-encoded return value on success. Every failure path is mapped
    /// so the CALLER can keep its original typed rejection: a missing
    /// leader hint, an unreachable target, a timeout, and a target that
    /// does not lead the lane either all come back as errors carrying a
    /// typed [`NotRaftLeader`] in the chain.
    pub(crate) fn forward_lane_write(
        &self,
        op: &crate::storage::raft::forward::ForwardableLaneWrite,
    ) -> R<serde_json::Value> {
        use crate::storage::raft::forward::ForwardOutcome;
        let engine = self.engine()?;
        let lane = op.lane();
        // In `single` topology every lane rides the one control group; the
        // envelope's group_id must name a group ACTIVE on the receiver.
        let group = if engine.lanes.len() == 1 {
            Lane::Control
        } else {
            lane
        };
        let hint = self.not_leader_error(lane);
        let Some(leader_addr) = hint.leader_addr.clone() else {
            return Err(anyhow::Error::new(hint).context(format!(
                "cannot forward {} — no leader is currently known for the {} lane",
                op.op_name(),
                lane.wire_name()
            )));
        };
        let payload = serde_json::to_vec(op)
            .with_context(|| format!("failed encoding forwarded op {}", op.op_name()))?;
        let secret: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = engine
            .cluster_secret
            .parse()
            .map_err(|_| anyhow!("cluster secret is not valid ascii metadata"))?;
        let group_id = group.wire_name().to_string();
        let op_name = op.op_name();
        let pool = Arc::clone(&engine.pool);
        let dial_addr = leader_addr.clone();
        let outcome = Self::block_on_engine(engine, async move {
            // Dial INSIDE the runtime. `ChannelPool::get` calls
            // `Endpoint::connect_lazy`, and building a tonic channel spawns
            // its connection task — which panics outright ("there is no
            // reactor running") on a thread with no runtime context. Most
            // callers of this function are synchronous (a write that lands
            // on a follower reaches it via `Store::forward_after_rejection`),
            // and the pool hides the hazard: it only bites on the FIRST
            // forward to a given leader address, so it surfaces right after
            // leadership moves to a manager this node has not forwarded to
            // before.
            let channel = pool
                .get(&dial_addr)
                .map_err(|e| anyhow!("malformed leader address {dial_addr}: {e}"))?;
            let mut client =
                crate::structs::raft_transport_client::RaftTransportClient::new(channel)
                    .max_decoding_message_size(
                        crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES,
                    )
                    .max_encoding_message_size(
                        crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES,
                    );
            let mut request =
                tonic::Request::new(crate::structs::RaftLaneRequest { group_id, payload });
            request.set_timeout(FORWARD_LANE_WRITE_TIMEOUT);
            request
                .metadata_mut()
                .insert(RAFT_CLUSTER_SECRET_METADATA_KEY, secret);
            Ok::<_, anyhow::Error>(client.forward_lane_write(request).await)
        })??;
        let reply = match outcome {
            Ok(reply) => reply.into_inner(),
            Err(status) => {
                return Err(anyhow::Error::new(hint).context(format!(
                    "forwarding {op_name} to the {} lane leader at {leader_addr} failed: \
                     {status}",
                    lane.wire_name()
                )));
            }
        };
        let outcome: ForwardOutcome = serde_json::from_slice(&reply.payload)
            .context("undecodable ForwardLaneWrite reply (mixed manager protocol versions?)")?;
        match outcome {
            ForwardOutcome::Ok(value) => Ok(value),
            ForwardOutcome::NotLeader {
                lane,
                leader_name,
                leader_addr,
            } => Err(anyhow::Error::new(NotRaftLeader {
                lane,
                leader_name,
                leader_addr,
            })
            .context(format!(
                "forwarded {op_name} but the target no longer leads the lane; retry"
            ))),
            ForwardOutcome::Failed(message) => Err(anyhow!(
                "forwarded {op_name} executed on the lane leader and failed: {message}"
            )),
        }
    }

    /// The lane's current leader address, when this node knows one.
    pub(crate) fn lane_leader_addr(&self, lane: Lane) -> Option<String> {
        self.not_leader_error(lane).leader_addr
    }

    /// Send one liveness digest to the status-lane leader (liveness plan
    /// §6.3).
    ///
    /// Async by design: the caller is the status-checker loop, which is
    /// already on the manager's runtime, and a digest is a fire-and-forget
    /// report rather than a write whose result anything waits on. Nothing here
    /// blocks a heartbeat.
    pub(crate) async fn send_liveness_digest(
        &self,
        observer_manager_id: String,
        node_ids: Vec<String>,
    ) -> crate::storage::liveness::DigestOutcome {
        use crate::storage::liveness::DigestOutcome;

        let Some(engine) = self.engine.get() else {
            return DigestOutcome::NoEngine;
        };
        let Some(leader_addr) = self.lane_leader_addr(Lane::Status) else {
            return DigestOutcome::NoLeader;
        };
        let Ok(secret) = engine
            .cluster_secret
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
        else {
            return DigestOutcome::Error("cluster secret is not valid ascii".into());
        };
        let channel = match engine.pool.get(&leader_addr) {
            Ok(channel) => channel,
            Err(err) => {
                return DigestOutcome::Error(format!(
                    "malformed status-lane leader address {leader_addr}: {err}"
                ))
            }
        };
        let mut client = crate::structs::raft_transport_client::RaftTransportClient::new(channel)
            .max_decoding_message_size(crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES);
        let mut request = tonic::Request::new(crate::structs::NodeLivenessDigestRequest {
            protocol_version: crate::constants::PROTOCOL_VERSION,
            observer_manager_id,
            observed_at_unix_ms: time::OffsetDateTime::now_utc().unix_timestamp() * 1_000,
            seen_node_ids: node_ids,
        });
        request.set_timeout(FORWARD_LANE_WRITE_TIMEOUT);
        request
            .metadata_mut()
            .insert(RAFT_CLUSTER_SECRET_METADATA_KEY, secret);
        match client.report_node_liveness(request).await {
            Ok(reply) => {
                let reply = reply.into_inner();
                if reply.accepted {
                    DigestOutcome::Accepted {
                        served_interval: reply
                            .served_interval_seconds
                            .filter(|seconds| *seconds > 0)
                            .map(|seconds| std::time::Duration::from_secs(u64::from(seconds))),
                    }
                } else {
                    DigestOutcome::NotLeader {
                        leader_hint: reply.leader_hint,
                    }
                }
            }
            // A manager that predates liveness digests answers `Unimplemented`
            // — the identical degradation `ForwardLaneWrite` shipped with, and
            // the identical remedy: finish the roll. Until then the sender
            // falls back to forwarding each heartbeat as a replicated write.
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                DigestOutcome::Unimplemented
            }
            Err(status) => DigestOutcome::Error(status.to_string()),
        }
    }

    /// Partition test hook: block/unblock every outbound raft transport RPC
    /// from this node (see `Store::raft_set_outbound_blocked` and
    /// `ChannelPool::set_outbound_blocked`). Errors before engine start.
    pub(crate) fn set_outbound_blocked(&self, blocked: bool) -> R<()> {
        self.engine()?.pool.set_outbound_blocked(blocked);
        Ok(())
    }

    /// Test hook (see `Store::raft_set_lane_convergence`): pause/resume the
    /// JC-40 anchor-lane convergence loop on this node. Production code
    /// never calls it — the lanes battery's leadership-INDEPENDENCE test
    /// needs to observe the transient divergent state that convergence
    /// exists to remove (JC-40: divergence is safe, merely converged away),
    /// which is impossible while the control leader keeps campaigning.
    pub(crate) fn set_lane_convergence(&self, enabled: bool) {
        self.lane_convergence
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Convergence campaigns issued for `lane` since the engine started
    /// (see `Store::raft_lane_campaigns`). `0` under `single` topology and
    /// for the anchor lane, which is never campaigned for.
    pub(crate) fn lane_campaigns(&self, lane: Lane) -> u64 {
        self.lane_campaigns
            .lock()
            .ok()
            .and_then(|counts| counts.get(&lane).copied())
            .unwrap_or(0)
    }

    /// Test hook (see `Store::raft_trigger_lane_election`): trigger an
    /// election on ONE lane's group on this node. The operator
    /// `trigger-election` command elects every lane at once; the lanes test
    /// battery needs a node that leads exactly one lane to prove elections
    /// are independent. Errors before engine start or for an inactive lane.
    pub(crate) fn trigger_lane_election(&self, lane: Lane) -> R<()> {
        let engine = self.engine()?;
        let raft = engine.lanes.get(&lane).with_context(|| {
            format!(
                "raft lane {} is not active under topology {}",
                lane.wire_name(),
                self.cfg.topology
            )
        })?;
        Self::block_on_engine(engine, raft.trigger().elect())?.map_err(|e| {
            anyhow!(
                "failed triggering election on lane {}: {e}",
                lane.wire_name()
            )
        })
    }

    /// Voters (by name) whose advertised schema version is unknown or below
    /// `required` — the KV keyring's upgrade gate (docs/kv-encryption.md,
    /// docs/upgrades.md). Peers advertise their version on every transport
    /// reply (`x-legion-schema-version`); a voter that has not answered yet,
    /// or runs a build that predates the header, is reported as missing.
    /// This node counts as its own build. Before the engine starts every
    /// voter is missing, which fails closed.
    pub(crate) fn voters_missing_schema_version(&self, required: u32) -> Vec<String> {
        let Some(engine) = self.engine.get() else {
            return vec!["(raft engine not started)".to_string()];
        };
        let Some(metrics) = Self::control_metrics(engine) else {
            return vec!["(control lane not started)".to_string()];
        };
        let membership = metrics.membership_config.membership().clone();
        let mut behind: Vec<String> = membership
            .voter_ids()
            .filter(|id| {
                if *id == self.raft_id {
                    crate::storage::migrations::schema_version() < required
                } else {
                    !matches!(engine.pool.schema_version_of(*id), Some(version) if version >= required)
                }
            })
            .map(|id| member_name(&metrics, id))
            .collect();
        behind.sort();
        behind.dedup();
        behind
    }

    /// Test hook for the upgrade gate: forget what `peer` advertised, as if
    /// it ran a build without the schema-version header. Reachable through
    /// `Store::raft_forget_peer_schema_version` so integration tests (which
    /// link the non-test library) can drive it.
    pub(crate) fn forget_peer_schema_version(&self, peer: NodeId) {
        if let Some(engine) = self.engine.get() {
            engine.pool.forget_schema_version(peer);
        }
    }

    fn is_leader_inner(&self, lane: Lane) -> bool {
        let Some(engine) = self.engine.get() else {
            return false;
        };
        let group = if engine.lanes.len() == 1 {
            Lane::Control
        } else {
            lane
        };
        engine
            .lanes
            .get(&group)
            .map(|raft| raft.metrics().borrow().state == ServerState::Leader)
            .unwrap_or(false)
    }
}

/// `0.0.0.0`/`::` binds are not dialable; advertise loopback instead (single
/// -node dev posture — real deployments set `raft.advertiseAddr`).
fn normalize_advertise_addr(manager_addr: &str) -> String {
    match manager_addr.rsplit_once(':') {
        Some(("0.0.0.0" | "::" | "[::]", port)) => format!("127.0.0.1:{port}"),
        _ => manager_addr.to_string(),
    }
}

fn persist_cluster_secret(path: &Path, secret: &str) -> R<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    std::fs::write(path, secret)
        .with_context(|| format!("failed persisting cluster secret to {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Cluster id derived from the shared secret: every node that shares the
/// secret computes the same id with no extra replicated state.
fn derive_cluster_id(secret: &str) -> String {
    format!("legion-raft-{:016x}", fnv1a64(secret))
}

fn map_server_state(state: ServerState) -> PersistenceRole {
    match state {
        ServerState::Leader => PersistenceRole::Primary,
        ServerState::Candidate => PersistenceRole::Candidate,
        ServerState::Learner | ServerState::Follower | ServerState::Shutdown => {
            PersistenceRole::Standby
        }
    }
}

/// How long the remove-member reachability probe waits for one member's
/// transport listener to answer a fresh dial. Short: the probe runs inline
/// in an operator command, and a member that cannot complete a TCP+h2
/// handshake in this window will not be acking membership entries either.
const MEMBER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Ceiling on one lane's membership change inside `remove-member`. A change
/// whose resulting config lacks a live quorum can NEVER commit (the joint
/// config needs a majority of the new voter set too), and openraft's
/// `change_membership` would block forever — the operator command must
/// instead come back with an actionable error naming the likely cause.
const MEMBERSHIP_CHANGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Deadline for one forwarded lane write (plan §8, D2): the transport hop
/// plus the leader's capture → quorum commit → local apply. Generous next
/// to a healthy pipeline (millisecond commits) while still bounding a
/// worker heartbeat RPC blocked behind a wedged lane; on expiry the origin
/// returns its original typed rejection.
const FORWARD_LANE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Operator-meaningful name for a raft member: the `name` half of its
/// `BasicNode.addr` (`"name@host:port"`), falling back to the bare addr or
/// the numeric id.
fn member_name(metrics: &RaftMetrics<NodeId, BasicNode>, id: NodeId) -> String {
    match metrics.membership_config.membership().get_node(&id) {
        Some(node) => {
            let (name, addr) = split_node_addr(&node.addr);
            if name.is_empty() {
                addr.to_string()
            } else {
                name.to_string()
            }
        }
        None => format!("{id:016x}"),
    }
}

/// Maps a failed `client_write`/membership call into an operator-actionable
/// error (leader hint for `ForwardToLeader`, plain context otherwise).
/// `lane_label` is the wire name of the group whose leader must be
/// contacted, when the topology runs more than one group — different lanes
/// can have different leaders, and the hint must name the right one.
fn map_write_error(
    op: &str,
    lane_label: Option<&str>,
    err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
) -> anyhow::Error {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => {
            let (leader_name, leader_addr) = match fwd.leader_node.as_ref() {
                Some(node) => {
                    let (name, addr) = split_node_addr(&node.addr);
                    (
                        Some(if name.is_empty() {
                            addr.to_string()
                        } else {
                            name.to_string()
                        }),
                        Some(addr.to_string()),
                    )
                }
                None => (None, None),
            };
            let context = match lane_label {
                Some(lane) => format!("raft op {op} must run on the {lane} lane's leader"),
                None => format!("raft op {op} must run on the leader"),
            };
            anyhow::Error::new(NotRaftLeader {
                lane: lane_label.map(str::to_owned),
                leader_name,
                leader_addr,
            })
            .context(context)
        }
        other => anyhow!("raft proposal failed for op {op}: {other}"),
    }
}

// ── pipeline ─────────────────────────────────────────────────────────────

/// The typed not-a-leader error built from a lane's current metrics (same
/// hint shape `map_write_error` produces from `ForwardToLeader`). The
/// caller fills in `lane` when the topology runs more than one group.
fn not_leader_from_metrics(metrics: &RaftMetrics<NodeId, BasicNode>) -> NotRaftLeader {
    let (leader_name, leader_addr) = match metrics.current_leader {
        Some(id) => {
            let addr = metrics
                .membership_config
                .membership()
                .get_node(&id)
                .map(|node| split_node_addr(&node.addr).1.to_string());
            (Some(member_name(metrics, id)), addr)
        }
        None => (None, None),
    };
    NotRaftLeader {
        lane: None,
        leader_name,
        leader_addr,
    }
}

/// One replicated mutation, executed on the lane's pipeline thread:
/// capture (rolled back locally) → propose → wait for quorum commit +
/// local apply (`client_write` resolves only after this node's state
/// machine applied the entry, so the caller gets read-your-writes).
fn execute_replicated(
    store: &Store,
    raft: &Raft,
    handle: &tokio::runtime::Handle,
    lane: Lane,
    op: &'static str,
    single_topology: bool,
    f: ReplicatedOp,
) -> R<Box<dyn Any + Send>> {
    // Reject non-leader proposals BEFORE opening a capture: `client_write`
    // would reject them anyway (ForwardToLeader), but only after this
    // follower ran a full capture — an open capture savepoint that blocks
    // (and before the capture/apply mutual exclusion, could destroy) the
    // committed entries this node is concurrently applying from the leader.
    // A leadership loss between this check and `client_write` still
    // surfaces as the same typed error through `map_write_error`.
    let lane_label = (!single_topology).then(|| lane.wire_name());
    // Engine metrics label: the raft GROUP the op commits through — under
    // `single` topology every lane rides the one `control` group, so its
    // capture/propose latency is that group's (docs/metrics.md, Raft).
    let metric_lane = if single_topology {
        Lane::Control.wire_name()
    } else {
        lane.wire_name()
    };
    let telemetry = crate::manager::telemetry::metrics();
    {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            telemetry
                .raft_ops_total
                .inc(&[metric_lane, op, "not_leader"]);
            let context = match lane_label {
                Some(lane) => format!("raft op {op} must run on the {lane} lane's leader"),
                None => format!("raft op {op} must run on the leader"),
            };
            return Err(anyhow::Error::new(NotRaftLeader {
                lane: lane_label.map(str::to_owned),
                ..not_leader_from_metrics(&metrics)
            })
            .context(context));
        }
    }
    let capture_started = std::time::Instant::now();
    let captured = capture::capture_mutation(store, lane, f);
    let capture_elapsed = capture_started.elapsed();
    telemetry
        .raft_capture_seconds
        .observe(&[metric_lane], capture_elapsed.as_secs_f64());
    let (value, changeset) = match captured {
        Ok(captured) => captured,
        Err(error) => {
            telemetry.raft_ops_total.inc(&[metric_lane, op, "error"]);
            return Err(error);
        }
    };
    if changeset.is_empty() {
        // The mutation touched no rows (e.g. an idempotent no-op): there is
        // no effect to replicate and nothing another replica could apply, so
        // skip the raft entry entirely.
        telemetry.raft_ops_total.inc(&[metric_lane, op, "empty"]);
        return Ok(value);
    }
    // Capture-time single-writer ownership guard (plan §7-C2, JC-26): every
    // table this changeset touches must be writable by an op on this lane.
    // A violation fails the op HERE — before proposal — in every topology
    // (phase C-2 flipped `OWNERSHIP_GUARD_ENFORCES`, ending the `single`
    // topology's warn-only grace window).
    crate::storage::raft::ownership::verify_changeset_ownership(
        op,
        lane,
        single_topology,
        &changeset,
    )?;
    let command = StoreCommand {
        lane,
        op: op.to_string(),
        changeset,
    };
    let propose_started = std::time::Instant::now();
    let write_result = handle.block_on(raft.client_write(command));
    let propose_elapsed = propose_started.elapsed();
    telemetry
        .raft_propose_apply_seconds
        .observe(&[metric_lane], propose_elapsed.as_secs_f64());
    match write_result {
        Ok(_) => telemetry.raft_ops_total.inc(&[metric_lane, op, "applied"]),
        Err(err) => {
            telemetry.raft_ops_total.inc(&[metric_lane, op, "error"]);
            return Err(map_write_error(op, lane_label, err));
        }
    }
    // Per-lane bottleneck trace: one debug line per replicated op with the
    // phase split (local capture vs propose→quorum→apply), so a slow lane
    // can be attributed without a profiler. Kept at debug — the histograms
    // above are the steady-state signal.
    debug!(
        lane = metric_lane,
        op,
        capture_ms = capture_elapsed.as_millis() as u64,
        propose_apply_ms = propose_elapsed.as_millis() as u64,
        "replicated op committed"
    );
    Ok(value)
}

fn pipeline_loop(
    store: Store,
    raft: Raft,
    handle: tokio::runtime::Handle,
    group: Lane,
    single_topology: bool,
    jobs: std_mpsc::Receiver<PipelineJob>,
) {
    let group_label = group.wire_name();
    let telemetry = crate::manager::telemetry::metrics();
    while let Ok(job) = jobs.recv() {
        let PipelineJob {
            lane,
            op,
            f,
            reply,
            queued_at,
        } = job;
        telemetry.raft_pipeline_depth.add(&[group_label], -1);
        telemetry
            .raft_pipeline_wait_seconds
            .observe(&[group_label], queued_at.elapsed().as_secs_f64());
        // A panic inside a captured store-method body must not kill the
        // pipeline thread: `capture_mutation` already rolled the capture
        // savepoint back before resuming the unwind, so the store is
        // consistent — convert the panic into an error reply and keep
        // serving (a dead pipeline thread would silently fail every later
        // write on this lane).
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_replicated(&store, &raft, &handle, lane, op, single_topology, f)
        }))
        .unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            error!(
                op,
                panic = %message,
                "replicated store operation panicked; capture was rolled back and the \
                 pipeline thread continues"
            );
            Err(anyhow!("replicated store op {op} panicked: {message}"))
        });
        // A dropped receiver just means the caller went away; the operation
        // is already committed (or failed) either way.
        let _ = reply.send(result);
    }
    debug!(
        group = group.wire_name(),
        "raft replication pipeline thread exiting"
    );
}

// ── join client (fresh node with a join token, plan §A4) ─────────────────

/// Background enrollment of a fresh node: retries the leader's
/// `RaftTransport.Join` RPC (1s interval) until the leader has added this
/// node as a learner on every lane, caught its log up, and promoted it to
/// voter. Permanent rejections (malformed token, wrong cluster, expired
/// token — `InvalidArgument`/`PermissionDenied`) stop the loop with a loud
/// error; everything else (leader not up yet, no leader elected, transient
/// transport failure) retries, since the joining manager typically starts
/// before its own listener — and sometimes before the leader — is
/// reachable.
#[allow(clippy::too_many_arguments)]
async fn join_cluster_loop(
    token: String,
    leader_addr: String,
    node_id: String,
    advertise_addr: String,
    cluster_secret: String,
    topology: String,
    history: Arc<StdMutex<HistoryState>>,
    store: Store,
) {
    use crate::structs::raft_transport_client::RaftTransportClient;

    let endpoint = match tonic::transport::Endpoint::from_shared(format!("http://{leader_addr}")) {
        Ok(endpoint) => endpoint,
        Err(e) => {
            error!(
                leader = %leader_addr,
                "Join token carries an undialable leader address: {e}"
            );
            return;
        }
    };
    let Ok(secret_value) = cluster_secret.parse::<tonic::metadata::MetadataValue<_>>() else {
        error!("Cluster secret is not valid ascii metadata; cannot join");
        return;
    };
    let channel = endpoint.connect_lazy();
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut request = tonic::Request::new(JoinRaftClusterRequest {
            join_token: token.clone(),
            node_id: node_id.clone(),
            advertise_addr: advertise_addr.clone(),
            // JC-43: the leader refuses a joiner whose topology differs
            // from the cluster's, with the same message a mismatched
            // restart gets.
            topology: topology.clone(),
        });
        request.metadata_mut().insert(
            crate::storage::raft::RAFT_CLUSTER_SECRET_METADATA_KEY,
            secret_value.clone(),
        );
        let mut client = RaftTransportClient::new(channel.clone());
        match client.join(request).await {
            Ok(reply) => {
                let reply = reply.into_inner();
                info!(
                    cluster_id = %reply.cluster_id,
                    raft_id = format!("{:016x}", reply.assigned_raft_id),
                    leader = %leader_addr,
                    "Joined raft cluster: added as learner on every lane and promoted to voter"
                );
                adopt_root_key_bundle(&store, &cluster_secret, &reply);
                MultiRaftDriver::push_history(
                    &history,
                    "membership",
                    format!(
                        "Joined cluster {} via join token (leader {leader_addr}); promoted to voter",
                        reply.cluster_id
                    ),
                    "system",
                    None,
                    0,
                    None,
                );
                return;
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::InvalidArgument | tonic::Code::PermissionDenied
                ) =>
            {
                error!(
                    code = ?status.code(),
                    leader = %leader_addr,
                    "Raft join rejected permanently — mint a fresh token with `legion raft \
                     join-token` on the leader and restart this manager: {}",
                    status.message()
                );
                return;
            }
            Err(status) => {
                // First failure and then every ~10s: enough signal without
                // flooding the log while the leader boots.
                if attempt % 10 == 1 {
                    warn!(
                        attempt,
                        code = ?status.code(),
                        leader = %leader_addr,
                        "Raft join attempt failed; retrying: {}",
                        status.message()
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Store the KV root key a leader handed this joiner (docs/kv-encryption.md),
/// then unwrap whatever data keys the snapshot install already brought. A
/// joiner that already holds root key material keeps it untouched.
fn adopt_root_key_bundle(store: &Store, cluster_secret: &str, reply: &JoinRaftClusterReply) {
    if reply.root_key_bundle.is_empty() {
        return;
    }
    let keyring = store.keyring();
    let material = match crate::storage::keyring::open_root_bundle(
        cluster_secret,
        &reply.cluster_id,
        &reply.root_key_bundle,
    ) {
        Ok(material) => material,
        Err(err) => {
            warn!(
                error = ?err,
                "The leader sent a KV root key this manager could not open; place the root key \
                 by hand (`legion keyring status` on the leader names it)"
            );
            return;
        }
    };
    match keyring.provider().import_material(&material) {
        Ok(true) => info!(
            location = %keyring.provider().location(),
            "Received the cluster's KV root key from the leader and stored it"
        ),
        Ok(false) => debug!("KV root key already present on this manager; leaving it untouched"),
        Err(err) => {
            warn!(error = ?err, "Failed storing the KV root key received from the leader");
            return;
        }
    }
    store.keyring_rows_changed();
}

// ── metrics → history projection ─────────────────────────────────────────

/// Watches one lane's metrics stream and appends real role / leader / term /
/// membership transitions to the operator history ring.
async fn watch_lane_transitions(
    lane: Lane,
    mut metrics: tokio::sync::watch::Receiver<RaftMetrics<NodeId, BasicNode>>,
    history: Arc<StdMutex<HistoryState>>,
    election_epoch_ms: Arc<AtomicI64>,
) {
    let mut prev = metrics.borrow().clone();
    loop {
        if metrics.changed().await.is_err() {
            return;
        }
        let cur = metrics.borrow().clone();
        let leader_name = cur.current_leader.map(|id| member_name(&cur, id));
        if cur.state != prev.state {
            MultiRaftDriver::push_history(
                &history,
                "role_change",
                format!(
                    "lane {}: role {:?} -> {:?} (term {})",
                    lane.wire_name(),
                    prev.state,
                    cur.state,
                    cur.current_term
                ),
                "system",
                None,
                cur.current_term,
                leader_name.clone(),
            );
        }
        if cur.current_leader != prev.current_leader {
            election_epoch_ms.store(
                (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64,
                Ordering::Relaxed,
            );
            let message = match &leader_name {
                Some(name) => format!(
                    "lane {}: {} became leader for term {}",
                    lane.wire_name(),
                    name,
                    cur.current_term
                ),
                None => format!(
                    "lane {}: leadership lost (term {})",
                    lane.wire_name(),
                    cur.current_term
                ),
            };
            MultiRaftDriver::push_history(
                &history,
                "election",
                message,
                "system",
                None,
                cur.current_term,
                leader_name.clone(),
            );
        }
        if cur.membership_config.membership() != prev.membership_config.membership() {
            let voters: Vec<String> = cur
                .membership_config
                .membership()
                .voter_ids()
                .map(|id| member_name(&cur, id))
                .collect();
            MultiRaftDriver::push_history(
                &history,
                "membership",
                format!(
                    "lane {}: membership changed; voters: [{}]",
                    lane.wire_name(),
                    voters.join(", ")
                ),
                "system",
                None,
                cur.current_term,
                leader_name,
            );
        }
        prev = cur;
    }
}

// ── lane-leadership convergence (JC-40) ──────────────────────────────────

/// Whether `raft` reports this node as the current leader of its group.
fn leads(raft: &Raft) -> bool {
    raft.metrics().borrow().state == ServerState::Leader
}

/// The campaign schedule, derived from the configured election window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneConvergeTiming {
    /// Backoff floor / spacing before the first campaign.
    min_backoff: Duration,
    /// Backoff ceiling.
    max_backoff: Duration,
    /// How long a campaign has to win before it is scored as lost.
    verdict: Duration,
    /// Stand-down window after [`Self::max_campaigns`] lost campaigns.
    cooldown: Duration,
    /// Consecutive lost campaigns before standing down.
    max_campaigns: u32,
}

impl LaneConvergeTiming {
    fn for_election_timeout(election_timeout_max_ms: u64) -> Self {
        let window = Duration::from_millis(election_timeout_max_ms.max(1));
        Self {
            min_backoff: window * LANE_CONVERGE_BACKOFF_MIN_WINDOWS,
            max_backoff: window * LANE_CONVERGE_BACKOFF_MAX_WINDOWS,
            verdict: window * LANE_CONVERGE_VERDICT_WINDOWS,
            cooldown: window * LANE_CONVERGE_COOLDOWN_WINDOWS,
            max_campaigns: LANE_CONVERGE_MAX_CAMPAIGNS,
        }
    }
}

/// `base` scaled by `factor`, floored at 1ms.
fn scaled(base: Duration, factor: f64) -> Duration {
    Duration::from_secs_f64((base.as_secs_f64() * factor).max(0.001))
}

/// ±25% jitter for the campaign schedule. A free function (never inlined
/// into the async loop) so no `ThreadRng` lives across an await point.
fn campaign_jitter() -> f64 {
    rand::thread_rng().gen_range(0.75..=1.25)
}

/// One lane's convergence-relevant state, sampled from openraft metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneObservation {
    /// This node leads the lane.
    led_here: bool,
    /// SOME node — possibly this one — currently leads the lane. `false`
    /// means an election is in flight somewhere in the cluster.
    has_leader: bool,
    /// This node's last log index on the lane. Used only to notice that the
    /// lane's log is still streaming in.
    last_log_index: u64,
}

impl LaneObservation {
    fn sample(raft: &Raft) -> Self {
        let watch = raft.metrics();
        let metrics = watch.borrow();
        Self {
            led_here: metrics.state == ServerState::Leader,
            has_leader: metrics.current_leader.is_some(),
            last_log_index: metrics.last_log_index.unwrap_or(0),
        }
    }
}

/// What the governor decided about one lane this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneConvergeAction {
    /// Issue a campaign for the lane now.
    Campaign,
    /// Leave the lane alone, and why.
    Hold(LaneConvergeHold),
}

/// Why a lane was left alone. Carried so the reason can be logged and, more
/// importantly, asserted in unit tests — "did not campaign" is only useful
/// evidence when the reason is pinned down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneConvergeHold {
    /// This node does not lead the anchor lane, so it is not the
    /// convergence authority (two campaigners would ping-pong the lane).
    NotAnchorLeader,
    /// Nothing to do — this node already leads the lane.
    Converged,
    /// A campaign this node issued has not been decided yet.
    CampaignInFlight,
    /// The lane has NO leader. See [`LaneConvergeGovernor::decide`].
    NaturalElection,
    /// The lane's log is still streaming into this node, so this node is
    /// demonstrably behind whoever it replicates from and cannot win.
    CatchingUp,
    /// Waiting out the per-lane exponential backoff.
    Backoff,
    /// Stood down after repeated lost campaigns.
    CoolingDown,
}

/// The per-lane campaign governor: the whole convergence policy, as a pure
/// state machine over sampled metrics so it can be unit-tested against a
/// fake clock.
#[derive(Debug)]
struct LaneConvergeGovernor {
    /// Consecutive campaigns that did not take the lane.
    lost: u32,
    /// Current spacing between campaigns.
    backoff: Duration,
    /// Earliest instant the next campaign may be issued.
    next_campaign_at: Option<Instant>,
    /// While set and unexpired: no campaign is issued for this lane at all.
    cooldown_until: Option<Instant>,
    /// Deadline by which the campaign this node last issued must have made
    /// it the lane's leader, or the campaign is scored as lost.
    verdict_due: Option<Instant>,
    /// The previous tick's log index, to notice a log still catching up.
    prev_log_index: Option<u64>,
    /// Consecutive polls held on the catching-up signal. Capped by
    /// [`LANE_CONVERGE_CATCHUP_HOLD_MAX_POLLS`]: log growth cannot tell
    /// catch-up replay from ordinary replication traffic, so an uncapped
    /// hold starves convergence on any busy lane.
    catching_up_holds: u32,
    /// Consecutive stand-downs, which lengthen the next cooldown. A lane
    /// this node keeps failing to take should be tried ever more rarely:
    /// the cluster is telling us the incumbent is the better leader, and
    /// every further campaign is pure term inflation on a lane that is
    /// serving fine.
    stand_downs: u32,
    /// Campaigns issued for this lane since the engine started.
    campaigns: u64,
    /// Latched for one tick when the lane just stood down, so the loop logs
    /// it once at warn instead of once per iteration.
    stood_down: bool,
}

impl LaneConvergeGovernor {
    fn new(timing: LaneConvergeTiming) -> Self {
        Self {
            lost: 0,
            backoff: timing.min_backoff,
            next_campaign_at: None,
            cooldown_until: None,
            verdict_due: None,
            prev_log_index: None,
            catching_up_holds: 0,
            stand_downs: 0,
            campaigns: 0,
            stood_down: false,
        }
    }

    /// The lane is held (or was taken): clear the whole schedule.
    fn settle(&mut self, timing: LaneConvergeTiming) {
        self.lost = 0;
        self.backoff = timing.min_backoff;
        self.next_campaign_at = None;
        self.cooldown_until = None;
        self.verdict_due = None;
        self.catching_up_holds = 0;
        self.stand_downs = 0;
    }

    /// The stand-down window, doubling per consecutive stand-down up to
    /// [`LANE_CONVERGE_COOLDOWN_MAX_DOUBLINGS`].
    fn cooldown(&self, timing: LaneConvergeTiming) -> Duration {
        timing.cooldown * (1 << self.stand_downs.min(LANE_CONVERGE_COOLDOWN_MAX_DOUBLINGS))
    }

    /// Score the campaign this node issued as lost: grow the backoff, and
    /// stand the lane down entirely once too many in a row have failed.
    fn score_lost(&mut self, now: Instant, timing: LaneConvergeTiming, jitter: f64) {
        self.verdict_due = None;
        self.lost += 1;
        if self.lost >= timing.max_campaigns {
            self.lost = 0;
            self.backoff = timing.min_backoff;
            self.next_campaign_at = None;
            self.cooldown_until = Some(now + scaled(self.cooldown(timing), jitter));
            self.stand_downs = self.stand_downs.saturating_add(1);
            self.stood_down = true;
        } else {
            self.next_campaign_at = Some(now + scaled(self.backoff, jitter));
            self.backoff = (self.backoff * 2).min(timing.max_backoff);
        }
    }

    fn take_stood_down(&mut self) -> bool {
        std::mem::take(&mut self.stood_down)
    }

    /// Decide what to do about this lane, given a fresh observation.
    ///
    /// # Why a LEADERLESS lane is NEVER campaigned for
    ///
    /// This is the invariant that makes convergence unable to break a lane
    /// (requirement: a lane must ALWAYS be able to elect). A lane without a
    /// leader is already holding an election: openraft's tick
    /// (`RaftCore::handle_tick_election`) campaigns on EVERY non-leader
    /// voter — this node included — once its randomized election timer
    /// expires, and it applies raft's own election policy while doing so:
    /// it defers behind the leader lease, and it delays by an extra
    /// `smaller_log_timeout` whenever a vote response revealed that some
    /// other voter has a longer log (openraft 0.9 `Engine::set_greater_log`).
    ///
    /// `Raft::trigger().elect()` — the only lever convergence has — bypasses
    /// ALL of that: it calls `Engine::elect()` directly, with no log check,
    /// no lease check, and no deference to an election already running. So
    /// on a leaderless lane a convergence campaign can only do harm: it
    /// re-arms this node's own candidacy at a higher term, discarding the
    /// votes the in-flight election had already collected, and (openraft 0.9
    /// has no pre-vote) drags every other voter's term up with it. Two
    /// campaigners on a 3-voter group with one member dead is a livelock:
    /// each holds its own vote at the same term, neither reaches quorum, and
    /// the term climbs forever. That is exactly what was observed on a
    /// 2-core CI runner — 132 terms in 210s on the status lane with the
    /// applied index frozen — and it is why the lane is now simply left to
    /// raft.
    ///
    /// Convergence therefore acts ONLY on a lane that already has a settled
    /// leader elsewhere, where a campaign is an optimization (co-locate the
    /// leader) rather than a repair. Losing such a campaign is NOT free:
    /// the leader lease makes the other voters reject the vote without
    /// adopting the higher term, but the campaigner's own term rose, so the
    /// incumbent's next append-entries to it comes back `HigherVote` and
    /// the incumbent steps down — roughly one election window of lane
    /// write-unavailability per lost campaign (verified against the
    /// openraft 0.9.24 source; there is no pre-vote). That cost is exactly
    /// what the verdict window, exponential backoff and escalating
    /// stand-downs bound to a small duty cycle.
    fn decide(
        &mut self,
        observation: LaneObservation,
        leads_anchor: bool,
        now: Instant,
        timing: LaneConvergeTiming,
        jitter: f64,
    ) -> LaneConvergeAction {
        // "Still catching up": the lane's log grew between two ticks, so
        // this node is receiving entries it did not have — it is behind
        // whoever it replicates from and cannot be the most up-to-date
        // voter. This is the only log-currency signal openraft 0.9 exposes
        // to a NON-leader (`RaftMetrics` carries this node's own
        // `last_log_index`; the replication/match view exists only on the
        // leader of that same group). The authoritative test is the
        // election itself, which is why a lost campaign also feeds the
        // backoff below.
        let catching_up = self
            .prev_log_index
            .is_some_and(|previous| observation.last_log_index > previous);
        self.prev_log_index = Some(observation.last_log_index);

        // Score the previous campaign first: winning clears the schedule,
        // losing grows the backoff and eventually stands the lane down.
        if self.verdict_due.is_some() {
            if observation.led_here {
                self.settle(timing);
            } else if self.verdict_due.is_some_and(|due| now >= due) {
                self.score_lost(now, timing, jitter);
            }
        }

        if !leads_anchor {
            // Not the convergence authority. Drop only the in-flight
            // campaign; KEEP the loss count, the grown backoff and any
            // cooldown. An anchor that flaps is itself a disruption source
            // — resetting the score on every flap meant a flapping anchor
            // fired an immediate campaign per re-acquisition and could
            // never accumulate the losses that trigger a stand-down
            // (adversarial finding, 2026-07-28), so the documented
            // "backoff → stand-down" ladder never engaged exactly when it
            // was needed most.
            self.verdict_due = None;
            return LaneConvergeAction::Hold(LaneConvergeHold::NotAnchorLeader);
        }
        if observation.led_here {
            self.settle(timing);
            return LaneConvergeAction::Hold(LaneConvergeHold::Converged);
        }
        if self.verdict_due.is_some() {
            return LaneConvergeAction::Hold(LaneConvergeHold::CampaignInFlight);
        }
        if !observation.has_leader {
            return LaneConvergeAction::Hold(LaneConvergeHold::NaturalElection);
        }
        if let Some(until) = self.cooldown_until {
            if now < until {
                return LaneConvergeAction::Hold(LaneConvergeHold::CoolingDown);
            }
            self.cooldown_until = None;
        }
        // First-campaign spacing: a fresh (or just-reset) schedule waits one
        // backoff before its first campaign instead of firing on the first
        // eligible poll. The lane's incumbent was elected moments ago in
        // the common case (this node just won the anchor after a failover),
        // and a campaign issued inside the incumbent's first leader lease
        // is a guaranteed loss — the constant's documentation always
        // promised this spacing; now decide() actually applies it.
        let Some(campaign_at) = self.next_campaign_at else {
            self.next_campaign_at = Some(now + scaled(self.backoff, jitter));
            return LaneConvergeAction::Hold(LaneConvergeHold::Backoff);
        };
        if now < campaign_at {
            return LaneConvergeAction::Hold(LaneConvergeHold::Backoff);
        }
        if catching_up {
            self.catching_up_holds = self.catching_up_holds.saturating_add(1);
            if self.catching_up_holds <= LANE_CONVERGE_CATCHUP_HOLD_MAX_POLLS {
                return LaneConvergeAction::Hold(LaneConvergeHold::CatchingUp);
            }
            // Held long enough: sustained log growth is ordinary
            // replication traffic, not catch-up (see the constant's docs).
            // Fall through and let the election be the judge.
        }
        self.catching_up_holds = 0;
        self.campaigns += 1;
        self.verdict_due = Some(now + timing.verdict);
        LaneConvergeAction::Campaign
    }
}

/// Anchor-lane convergence loop (JC-40) — one per node, `lanes` topology
/// only, started with the engine.
///
/// Legion has composite operations that write two lanes (registration =
/// control identity + status liveness; job submit = control tables + a
/// scheduler-lane eval enqueue). Every half is proposed from the calling
/// node, so under divergent per-lane leadership each composite half-fails on
/// the node that does not lead the other lane — and joins, `/readyz` and
/// remove-member feasibility want one node leading everything too. So
/// whichever node leads [`ANCHOR_LANE`] campaigns (`Raft::trigger().elect()`)
/// for the other lanes until one node leads them all.
///
/// This keeps the ENTIRE multi-raft win — the isolation comes from
/// independent logs, apply loops and replication pipelines per write class
/// (a control-write burst cannot stall the scheduler's log), not from where
/// the leaders sit.
///
/// # Convergence is BEST EFFORT, not an invariant
///
/// It used to be documented as a steady state the cluster always reaches.
/// It is not, and it cannot be: openraft 0.9 has no leader-transfer API, so
/// the only lever is a campaign, and a campaign is a *race* that this node
/// can lose — permanently, if its log for that lane is behind the incumbent
/// or the incumbent is simply healthier. What the loop guarantees instead:
///
/// 1. It NEVER campaigns for a leaderless lane, so it can never prevent or
///    disrupt an election (see [`LaneConvergeGovernor::decide`]). An
///    unreachable lane is a dead write path; that always outranks
///    co-location.
/// 2. Campaigns are per-lane, exponentially backed off, and stop entirely
///    for [`LANE_CONVERGE_COOLDOWN_WINDOWS`] election windows after
///    [`LANE_CONVERGE_MAX_CAMPAIGNS`] consecutive losses — so term
///    inflation from convergence is bounded (a handful of terms per
///    cooldown, per lane) and natural elections get uninterrupted stretches
///    far longer than any election timeout.
/// 3. Divergent leadership stays CORRECT: every cross-lane composite's
///    second half is rejected BEFORE its capture with the typed
///    `NotRaftLeader` leader hint, every first half is idempotent on retry,
///    and the daemon regenerates scheduling evals while lanes diverge.
///
/// # Why we do NOT force a step-down of the remote lane leader
///
/// openraft 0.9 has no leader-transfer API, so "forcing" would mean a side
/// channel telling the remote leader to abdicate: two nodes then act on the
/// same lane at once, and a stale anchor view turns that into leadership
/// ping-pong — for zero gain, since the campaign that follows the abdication
/// is the same campaign we already issue.
async fn lane_convergence_loop(
    node_id: String,
    lanes: HashMap<Lane, Raft>,
    enabled: Arc<std::sync::atomic::AtomicBool>,
    campaign_counts: Arc<StdMutex<HashMap<Lane, u64>>>,
    election_timeout_max_ms: u64,
) {
    let timing = LaneConvergeTiming::for_election_timeout(election_timeout_max_ms);
    let Some(anchor) = lanes.get(&ANCHOR_LANE).cloned() else {
        return;
    };
    let mut others: Vec<(Lane, Raft)> = lanes
        .iter()
        .filter(|(lane, _)| **lane != ANCHOR_LANE)
        .map(|(lane, raft)| (*lane, raft.clone()))
        .collect();
    if others.is_empty() {
        return;
    }
    others.sort_by_key(|(lane, _)| lane.wire_name());

    // Lifecycle-level logging only: `acquired` remembers which lanes this
    // node was last seen leading, so an acquisition logs once at info
    // instead of once per loop iteration.
    let mut acquired: Vec<Lane> = others
        .iter()
        .filter(|(_, raft)| leads(raft))
        .map(|(lane, _)| *lane)
        .collect();
    let mut governors: Vec<LaneConvergeGovernor> = others
        .iter()
        .map(|_| LaneConvergeGovernor::new(timing))
        .collect();
    loop {
        if !enabled.load(Ordering::Relaxed) {
            tokio::time::sleep(LANE_CONVERGE_IDLE_POLL).await;
            continue;
        }
        let leads_anchor = leads(&anchor);
        for (index, (lane, raft)) in others.iter().enumerate() {
            let observation = LaneObservation::sample(raft);
            let known = acquired.contains(lane);
            if observation.led_here && !known {
                acquired.push(*lane);
                // Only the anchor leader's acquisitions are convergence
                // events; a node that happens to lead one lane and not the
                // anchor is the state being converged AWAY, not a milestone.
                if leads_anchor {
                    info!(
                        node_id = %node_id,
                        lane = lane.wire_name(),
                        "Lane leadership converged: this node now leads the {} lane alongside \
                         the {} anchor lane",
                        lane.wire_name(),
                        ANCHOR_LANE.wire_name()
                    );
                }
            } else if !observation.led_here && known {
                acquired.retain(|held| held != lane);
            }

            let action = governors[index].decide(
                observation,
                leads_anchor,
                Instant::now(),
                timing,
                campaign_jitter(),
            );
            if governors[index].take_stood_down() {
                warn!(
                    node_id = %node_id,
                    lane = lane.wire_name(),
                    cooldown_seconds = governors[index]
                        .cooldown_until
                        .map(|until| until.saturating_duration_since(Instant::now()).as_secs())
                        .unwrap_or_default(),
                    "Lane leadership convergence is standing down for the {} lane after {} \
                     campaigns that did not take it; the lane keeps serving under its own \
                     leader and cross-lane operations retry against it (see \
                     docs/multi-raft.md)",
                    lane.wire_name(),
                    timing.max_campaigns
                );
            }
            let LaneConvergeAction::Campaign = action else {
                continue;
            };
            if let Ok(mut counts) = campaign_counts.lock() {
                counts.insert(*lane, governors[index].campaigns);
            }
            debug!(
                node_id = %node_id,
                lane = lane.wire_name(),
                "Campaigning to co-locate lane leadership on the control-lane leader"
            );
            if let Err(err) = raft.trigger().elect().await {
                debug!(
                    lane = lane.wire_name(),
                    error = %err,
                    "Convergence campaign could not be triggered; retrying after backoff"
                );
            }
        }
        tokio::time::sleep(LANE_CONVERGE_IDLE_POLL).await;
    }
}

/// The active lanes in a DETERMINISTIC order (sorted by wire name). Every
/// multi-lane membership sequence (join learner-add/promote, operator
/// add/remove) iterates in this order so a partial failure under divergent
/// lane leadership always stops at the same lane — retries make monotonic
/// progress instead of leaving nondeterministic per-lane membership drift
/// (HashMap iteration order; adversarial finding, 2026-07-24).
fn lanes_in_order(lanes: &HashMap<Lane, Raft>) -> Vec<(Lane, &Raft)> {
    let mut ordered: Vec<(Lane, &Raft)> = lanes.iter().map(|(lane, raft)| (*lane, raft)).collect();
    ordered.sort_by_key(|(lane, _)| lane.wire_name());
    ordered
}

// ── Join handling (RaftTransport.Join, plan §A4) ─────────────────────────

/// Leader-side handler for `RaftTransport.Join`: validates the join token,
/// adds the joiner as a **learner on every lane** (blocking until its log has
/// caught up with the leader), then promotes it to voter on every lane. One
/// token joins all lanes.
///
/// Errors map to gRPC statuses for the joining manager: a malformed token is
/// `InvalidArgument`, a wrong-cluster or expired token is `PermissionDenied`,
/// a raft-id collision is `FailedPrecondition`, and a `ForwardToLeader`
/// rejection is `FailedPrecondition` carrying the current leader's
/// name/address in the message (JC-4) so the joiner can redial.
struct EngineJoinHandler {
    node_id: String,
    cluster_id: String,
    cluster_secret: String,
    /// The cluster's topology, canonicalized. A joiner must present the
    /// same one (JC-43).
    topology: String,
    /// The raft data dir, named in the topology-mismatch message so the
    /// operator is told exactly which directory has to be emptied.
    data_dir: PathBuf,
    lanes: HashMap<Lane, Raft>,
    history: Arc<StdMutex<HistoryState>>,
    /// The KV keyring, so a join reply can carry the cluster's root key to
    /// the joiner when the provider exports it (docs/kv-encryption.md).
    keyring: crate::storage::keyring::Keyring,
}

impl EngineJoinHandler {
    /// The root key material for a joiner, sealed for transit — or empty
    /// when encryption is off or the provider keeps its key to itself
    /// (`exec`). Never fails a join: a bundle that cannot be built is
    /// logged and the joiner is told nothing, exactly like an `exec`
    /// cluster, and its keyring reports locked until the operator places
    /// the key.
    fn root_key_bundle(&self) -> Vec<u8> {
        if !self.keyring.enabled() {
            return Vec::new();
        }
        // A join can arrive before this leader's keyring loop has run
        // (every manager of a fresh cluster starts at once), so mint the
        // root key here rather than answer with nothing: a joiner only
        // receives the key with its join reply, and one that joined
        // empty-handed would be locked the moment the first data key
        // replicates. Providers that cannot mint (`env`, `exec`) still
        // answer with what they have.
        let provider = self.keyring.provider();
        let material = match provider.export_material() {
            Some(material) => material,
            None => {
                match provider.mint() {
                    Ok(Some(id)) => info!(
                        root_key_id = %id,
                        "Minted the KV root key ahead of a manager join"
                    ),
                    Ok(None) => {}
                    Err(err) => warn!(error = ?err, "Could not mint the KV root key for a join"),
                }
                let Some(material) = provider.export_material() else {
                    return Vec::new();
                };
                material
            }
        };
        match crate::storage::keyring::seal_root_bundle(
            &self.cluster_secret,
            &self.cluster_id,
            &material,
        ) {
            Ok(bundle) => bundle,
            Err(err) => {
                warn!(error = ?err, "Could not seal the KV root key for a joining manager");
                Vec::new()
            }
        }
    }
    /// Current membership of every lane, for the join reply's peer-table
    /// seed.
    fn group_memberships(&self) -> Vec<RaftGroupMembership> {
        let mut groups: Vec<RaftGroupMembership> = self
            .lanes
            .iter()
            .map(|(lane, raft)| {
                let metrics = raft.metrics().borrow().clone();
                let membership = metrics.membership_config.membership().clone();
                let voter_set: std::collections::HashSet<NodeId> = membership.voter_ids().collect();
                let members = membership
                    .nodes()
                    .map(|(id, node)| {
                        let (name, addr) = split_node_addr(&node.addr);
                        RaftMemberInfo {
                            raft_id: *id,
                            node_id: name.to_string(),
                            advertise_addr: addr.to_string(),
                            voter: voter_set.contains(id),
                        }
                    })
                    .collect();
                RaftGroupMembership {
                    group_id: lane.wire_name().to_string(),
                    members,
                }
            })
            .collect();
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        groups
    }

    /// JC-40: with co-located leadership the Join handler must run on the
    /// ANCHOR-lane leader. A join mutates membership on EVERY lane in
    /// sequence, so a receiver that leads only some of them would apply the
    /// learner-add/promotion to those and fail on the rest — a membership
    /// change half-applied across lanes, which no retry cleans up (the
    /// joiner would be a voter on one lane and unknown on another).
    ///
    /// Rejecting up front, before anything is issued, keeps the flow
    /// all-or-nothing at the lane the composite starts on: the joiner's
    /// retry loop redials the hinted leader (a `FailedPrecondition` is
    /// retryable there, unlike the config-mismatch refusals), and the
    /// anchor leader's convergence loop is already pulling the remaining
    /// lanes onto itself, so the retry lands on a node that leads all of
    /// them. openraft 0.9 has no server-side forwarding to do this for us —
    /// the hint IS the forward.
    // The error type is `tonic::Status` because this feeds the Join RPC's
    // reply directly; boxing it would only move the size to the call sites.
    #[allow(clippy::result_large_err)]
    fn require_anchor_leadership(&self, joiner: &str) -> Result<(), tonic::Status> {
        if self.lanes.len() < 2 {
            return Ok(());
        }
        let Some(anchor) = self.lanes.get(&ANCHOR_LANE) else {
            return Err(tonic::Status::failed_precondition(format!(
                "this manager does not run the {} lane; joins must be sent to a manager that \
                 does",
                ANCHOR_LANE.wire_name()
            )));
        };
        let (is_leader, hint) = {
            let metrics = anchor.metrics().borrow().clone();
            let hint = match metrics.current_leader {
                Some(id) => {
                    let name = member_name(&metrics, id);
                    let addr = metrics
                        .membership_config
                        .membership()
                        .get_node(&id)
                        .map(|node| split_node_addr(&node.addr).1.to_string())
                        .unwrap_or_default();
                    format!("; retry the join against leader {name} at {addr}")
                }
                None => "; no leader is currently known, retry shortly".to_string(),
            };
            (metrics.state == ServerState::Leader, hint)
        };
        if is_leader {
            return Ok(());
        }
        debug!(
            joiner,
            "Rejecting join: this manager does not lead the {} anchor lane",
            ANCHOR_LANE.wire_name()
        );
        Err(tonic::Status::failed_precondition(format!(
            "this manager is not the raft leader for the {} lane, which owns cluster joins \
             (lane leadership is co-located on the {} leader){hint}",
            ANCHOR_LANE.wire_name(),
            ANCHOR_LANE.wire_name()
        )))
    }

    /// Maps a failed membership-changing raft call to the joiner's gRPC
    /// status (leader hint for `ForwardToLeader`, internal otherwise).
    fn membership_status(
        stage: &str,
        lane: Lane,
        err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
    ) -> tonic::Status {
        match err {
            RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => {
                let hint = match fwd.leader_node.as_ref() {
                    Some(node) => {
                        let (name, addr) = split_node_addr(&node.addr);
                        format!("; retry against leader {name} at {addr}")
                    }
                    None => "; no leader is currently known, retry shortly".to_string(),
                };
                tonic::Status::failed_precondition(format!(
                    "this manager is not the raft leader (join {stage}, lane {}){hint}",
                    lane.wire_name()
                ))
            }
            other => tonic::Status::internal(format!(
                "raft join {stage} failed on lane {}: {other}",
                lane.wire_name()
            )),
        }
    }

    /// Runs one lane's membership change, retrying while the ONLY reason it
    /// failed is that this node has not acquired that lane's leadership yet
    /// (JC-40: the anchor leader's convergence loop is campaigning for it
    /// right now). Without this, a join that arrives during the convergence
    /// window fails on the first non-anchor lane after the anchor lane's
    /// change already committed — the half-applied membership the anchor
    /// precondition exists to prevent. Any other failure, and expiry of the
    /// window, map to the joiner's leader-hint status unchanged.
    ///
    /// `convergence_running` is `lanes.len() > 1`: under `single` topology
    /// no convergence loop exists, so a `ForwardToLeader` can never resolve
    /// by waiting — it fails fast with the leader hint instead of stalling
    /// the joiner for the full convergence window (adversarial finding,
    /// 2026-07-28).
    async fn membership_with_convergence_retry<T, F, Fut>(
        stage: &'static str,
        lane: Lane,
        deadline: std::time::Instant,
        convergence_running: bool,
        mut call: F,
    ) -> Result<T, tonic::Status>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<
            Output = Result<T, RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
        >,
    {
        loop {
            let err = match call().await {
                Ok(value) => return Ok(value),
                Err(err) => err,
            };
            let converging = convergence_running
                && matches!(
                    &err,
                    RaftError::APIError(ClientWriteError::ForwardToLeader(_))
                )
                && std::time::Instant::now() < deadline;
            if !converging {
                return Err(Self::membership_status(stage, lane, err));
            }
            debug!(
                stage,
                lane = lane.wire_name(),
                "Join membership change is waiting for lane-leadership convergence"
            );
            tokio::time::sleep(JOIN_LANE_RETRY_INTERVAL).await;
        }
    }

    /// Post-promotion verification (JC-40): the joiner must be a SETTLED
    /// voter on every lane before the join is reported successful.
    /// `change_membership` returning `Ok` means the change committed on that
    /// lane's leader; this re-reads each lane's membership so a join can
    /// never report success while some lane still carries the joiner as a
    /// learner (leaving it silently outside that lane's quorum).
    ///
    /// "Settled" means [`lane_membership_settled`]: the joiner is a voter
    /// AND the lane's membership is uniform rather than joint. A joint
    /// config already reports the joiner through `voter_ids()`, so the
    /// weaker check passed while a lane was still mid-change — and a lane
    /// left joint keeps requiring a quorum of the OLD configuration, which
    /// is how a 2-voter → 3-voter growth can look complete and yet be
    /// unable to elect the moment one of the two original voters dies.
    async fn wait_until_voter_on_every_lane(
        &self,
        member_id: NodeId,
        deadline: std::time::Instant,
    ) -> Result<(), tonic::Status> {
        loop {
            let missing: Vec<&'static str> = lanes_in_order(&self.lanes)
                .into_iter()
                .filter(|(_, raft)| {
                    !lane_membership_settled(
                        raft.metrics().borrow().membership_config.membership(),
                        member_id,
                    )
                })
                .map(|(lane, _)| lane.wire_name())
                .collect();
            if missing.is_empty() {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(tonic::Status::failed_precondition(format!(
                    "join promoted the member but lane(s) [{}] have not settled it as a \
                     voter (still a learner, or the membership change is still joint); \
                     retry the join (membership changes are idempotent)",
                    missing.join(", ")
                )));
            }
            tokio::time::sleep(JOIN_LANE_RETRY_INTERVAL).await;
        }
    }
}

/// Whether `membership` carries `member_id` as a voter in a SETTLED
/// (uniform, single-config) membership.
///
/// openraft applies a membership change in two steps — a joint config
/// holding the old and the new voter sets, then the uniform new config —
/// and `voter_ids()` returns the union across both, so it reports the
/// incoming member as a voter from the first step onwards. A lane stuck in
/// the joint step still needs a quorum of the OLD config to commit or elect,
/// which is a materially weaker cluster than the voter list suggests. Every
/// "is this member really in?" check therefore goes through here.
fn lane_membership_settled(
    membership: &openraft::Membership<NodeId, BasicNode>,
    member_id: NodeId,
) -> bool {
    membership.get_joint_config().len() == 1 && membership.voter_ids().any(|id| id == member_id)
}

/// Whether `membership` carries `member_id` as a voter in EVERY config of a
/// possibly-joint membership — the joint-intersection predicate.
///
/// A joint membership `[{a,b}, {a,b,c}]` commits and elects only with a
/// majority of EACH config, so `a` and `b` are voters under the strictest
/// rules in force while `c` (present only in the incoming config) is not
/// yet one. Used for the aggregate `raft status` voter list, where the
/// stricter [`lane_membership_settled`] (which requires the whole
/// membership to be uniform) would empty the voter list for EVERYONE the
/// moment any join enters its joint window.
fn voter_in_every_config(
    membership: &openraft::Membership<NodeId, BasicNode>,
    member_id: NodeId,
) -> bool {
    let configs = membership.get_joint_config();
    !configs.is_empty() && configs.iter().all(|config| config.contains(&member_id))
}

#[tonic::async_trait]
impl RaftJoinHandler for EngineJoinHandler {
    async fn join(
        &self,
        request: JoinRaftClusterRequest,
    ) -> Result<JoinRaftClusterReply, tonic::Status> {
        // Token validation: decodable, same cluster secret (constant-time),
        // not expired. The transport already authenticated the RPC's
        // metadata secret; re-validating the token itself means a leaked
        // advertise address plus a stale/foreign token is still not enough.
        let payload = JoinTokenPayload::decode(&request.join_token)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid join token: {e:#}")))?;
        if !crate::storage::raft::server::secrets_match(
            payload.secret.as_bytes(),
            self.cluster_secret.as_bytes(),
        ) {
            return Err(tonic::Status::permission_denied(
                "join token was minted for a different raft cluster",
            ));
        }
        if payload.is_expired(OffsetDateTime::now_utc().unix_timestamp()) {
            return Err(tonic::Status::permission_denied(
                "join token has expired; mint a new one with `legion raft join-token`",
            ));
        }
        let joiner = request.node_id.trim();
        let advertise = request.advertise_addr.trim();
        if joiner.is_empty() || advertise.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "join request requires node_id and advertise_addr",
            ));
        }

        // JC-43: topology is a property of the CLUSTER, not of a node. A
        // joiner configured differently would be enrolled as a voter in
        // groups it does not run (its lanes would never receive entries) or
        // would run groups nobody replicates to — and once its log advances
        // there is no way back. Refused with the same message a mismatched
        // restart gets. `InvalidArgument` (not FailedPrecondition) so the
        // joiner's retry loop stops loudly instead of hammering forever: no
        // amount of waiting fixes a config mismatch.
        let joiner_topology = canonical_topology(request.topology.trim());
        if joiner_topology != self.topology {
            return Err(tonic::Status::invalid_argument(topology_mismatch_message(
                &format!("this cluster runs topology `{}`", self.topology),
                &format!("joining manager `{joiner}` is configured with `{joiner_topology}`"),
                &self.topology,
                &self.data_dir,
            )));
        }

        // JC-40: joins mutate membership on every lane, so they run on the
        // anchor-lane leader or not at all (see require_anchor_leadership).
        self.require_anchor_leadership(joiner)?;

        let member_id = fnv1a64(joiner);
        let node = BasicNode::new(encode_node_addr(joiner, advertise));

        // Reject a derived-id collision with an existing member under a
        // different name (types.rs documents this guard on fnv1a64).
        for (lane, raft) in lanes_in_order(&self.lanes) {
            let metrics = raft.metrics().borrow().clone();
            if let Some(existing) = metrics.membership_config.membership().get_node(&member_id) {
                let (existing_name, _) = split_node_addr(&existing.addr);
                if !existing_name.is_empty() && existing_name != joiner {
                    return Err(tonic::Status::failed_precondition(format!(
                        "raft id collision on lane {}: joiner `{joiner}` derives the same raft \
                         id as existing member `{existing_name}`; rename the joining manager",
                        lane.wire_name()
                    )));
                }
            }
        }

        // Learner add on every lane, blocking until each lane's replication
        // to the joiner has caught up with the leader's log. Lanes this node
        // has not acquired yet are retried inside the convergence window
        // rather than failing the join outright (JC-40).
        let deadline = std::time::Instant::now() + JOIN_LANE_CONVERGENCE_WAIT;
        let convergence_running = self.lanes.len() > 1;
        for (lane, raft) in lanes_in_order(&self.lanes) {
            Self::membership_with_convergence_retry(
                "add-learner",
                lane,
                deadline,
                convergence_running,
                || raft.add_learner(member_id, node.clone(), true),
            )
            .await?;
        }
        // Voter-parity check on the RESULTING count (headline control
        // group), computed before promotion. Promotion is NEVER withheld —
        // quietly leaving the joiner a learner to preserve parity would
        // trade a visible warning for an invisible availability surprise
        // (plan §7-C3, JC-29) — but an even result is warned about loudly
        // and recorded in the audit history below.
        let parity_warning = self.lanes.get(&Lane::Control).and_then(|raft| {
            let m = raft.metrics().borrow().clone();
            let mut resulting: std::collections::BTreeSet<NodeId> =
                m.membership_config.membership().voter_ids().collect();
            resulting.insert(member_id);
            quorum::parity_advisory(resulting.len() as u64)
        });

        // Caught up everywhere: promote to voter on every lane, then verify
        // that every lane really carries the joiner as a voter before the
        // join reports success.
        for (lane, raft) in lanes_in_order(&self.lanes) {
            Self::membership_with_convergence_retry(
                "promote",
                lane,
                deadline,
                convergence_running,
                || {
                    let mut members = BTreeMap::new();
                    members.insert(member_id, node.clone());
                    raft.change_membership(openraft::ChangeMembers::AddVoters(members), false)
                },
            )
            .await?;
        }
        self.wait_until_voter_on_every_lane(member_id, deadline)
            .await?;

        if let Some(advisory) = &parity_warning {
            warn!(joiner, "{advisory}");
            MultiRaftDriver::push_history(
                &self.history,
                "membership",
                advisory.clone(),
                &self.node_id,
                Some(joiner.to_string()),
                self.lanes
                    .get(&Lane::Control)
                    .map(|raft| raft.metrics().borrow().current_term)
                    .unwrap_or(0),
                None,
            );
        }

        MultiRaftDriver::push_history(
            &self.history,
            "membership",
            format!(
                "Manager {joiner} ({advertise}) joined via join token and was promoted to voter"
            ),
            &self.node_id,
            Some(joiner.to_string()),
            self.lanes
                .get(&Lane::Control)
                .map(|raft| raft.metrics().borrow().current_term)
                .unwrap_or(0),
            None,
        );
        info!(
            joiner,
            advertise, "Raft join complete: learner caught up and promoted to voter"
        );

        Ok(JoinRaftClusterReply {
            assigned_raft_id: member_id,
            cluster_id: self.cluster_id.clone(),
            groups: self.group_memberships(),
            root_key_bundle: self.root_key_bundle(),
        })
    }
}

// ── PersistenceDriver ────────────────────────────────────────────────────

impl PersistenceDriver for MultiRaftDriver {
    fn start(&self) -> R<()> {
        // The raft engine needs the Store (migrated DB, state machines);
        // `Store::start_storage` calls `start_engine` after migrations
        // instead of this store-less hook.
        Ok(())
    }

    fn start_engine(&self, store: &Store) -> R<()> {
        if self.engine.get().is_some() {
            bail!("raft engine already started");
        }
        let data_dir = PathBuf::from(&self.cfg.data_dir);
        std::fs::create_dir_all(&data_dir)
            .with_context(|| format!("failed creating raft data dir {}", data_dir.display()))?;
        // Double-start protection: fail fast BEFORE any log/vote write if
        // another manager process already runs against this data dir.
        let data_dir_lock = acquire_data_dir_lock(&data_dir)?;
        let cluster_secret = self.load_or_mint_cluster_secret(&data_dir)?;
        let cluster_id = derive_cluster_id(&cluster_secret);
        // Bind the state DB to the cluster identity: refuse to (re)bootstrap
        // a different cluster over an existing cluster's data.
        check_cluster_stamp(store, &cluster_id, self.cfg.force_new_cluster)?;
        let lanes_list = lanes_for_topology(&self.cfg.topology)?;
        // JC-43, the authoritative half: the raft data dir holds the logs and
        // snapshots, so it — not just the state DB — is stamped with the
        // topology that built them. Checked BEFORE any log/vote write and
        // before the state DB stamp, so a mismatch never mutates anything.
        check_data_dir_topology_stamp(&data_dir, &self.cfg.topology, &lanes_list)?;
        // Bind the state DB to the topology too: catches the mirror-image
        // loss (data dir wiped, state DB kept) that the file stamp cannot
        // see (see check_topology_stamp).
        check_topology_stamp(store, &self.cfg.topology, self.cfg.force_new_cluster)?;

        // Engine runtime sizing (adversarial finding, 2026-07-25). The state
        // machine does its SQLite work synchronously on the async caller
        // (state_machine.rs "sync-inside-async"), so a lane's apply occupies
        // a worker thread while it runs, and every lane's state work
        // serializes on the one global capture mutex. The old hardcoded 2
        // threads therefore let two concurrent blocking operations pin the
        // ENTIRE pool, starving heartbeats, elections, replication and
        // timers on the remaining lanes.
        //
        // Two changes remove that. (a) The multi-second operations —
        // snapshot build and install — moved to tokio's blocking pool, so
        // the only state work left on a worker thread is one bounded
        // changeset apply, and openraft runs exactly ONE state-machine
        // worker task per lane: at most `lanes` threads can be inside an
        // apply at once. (b) Sizing at two threads per lane (floor 4, so
        // `single` keeps headroom for its core loop plus per-peer
        // replication tasks) therefore guarantees at least half the pool is
        // always free to drive the async work that must not stall.
        let worker_threads = (lanes_list.len() * 2).max(4);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("raft-engine")
            .enable_all()
            .build()
            .context("failed building raft engine runtime")?;
        let handle = runtime.handle().clone();

        // Election/heartbeat timing comes from config
        // (`raft.heartbeatIntervalMs` / `electionTimeoutMinMs` /
        // `electionTimeoutMaxMs`), defaulting to the constants documented
        // at the top of this file. Config load has already rejected a
        // combination that would flap, so these values are safe to hand to
        // openraft's own (weaker) `validate()` below.
        //
        // Snapshots + compaction (plan §A5 / §7-C3): every lane snapshots
        // after `raft.snapshotEntries` applied entries and purges its log
        // behind the snapshot, keeping a tail of `snapshot_entries / 2`
        // (clamped to [1, 1000]) entries so a slightly-behind follower
        // catches up by cheap log replay instead of a full snapshot install;
        // anything older is served by the snapshot. The snapshot's SCOPE
        // depends on topology: `single` builds whole-DB `VACUUM INTO`
        // images; `lanes` builds table-scoped dumps of each lane's owned
        // tables (sound because single-writer table ownership is enforced at
        // capture time — see state_machine.rs and ownership.rs).
        let snapshot_scope = if lanes_list.len() == 1 {
            crate::storage::raft::state_machine::SnapshotScope::WholeDb
        } else {
            crate::storage::raft::state_machine::SnapshotScope::LaneTables
        };
        let snapshot_entries = self.cfg.snapshot_entries.max(1);
        let snapshot_policy = SnapshotPolicy::LogsSinceLast(snapshot_entries);
        let max_in_snapshot_log_to_keep = (snapshot_entries / 2).clamp(1, 1000);
        let raft_config = Arc::new(
            openraft::Config {
                cluster_name: cluster_id.clone(),
                heartbeat_interval: self.cfg.heartbeat_interval_ms,
                election_timeout_min: self.cfg.election_timeout_min_ms,
                election_timeout_max: self.cfg.election_timeout_max_ms,
                snapshot_policy,
                max_in_snapshot_log_to_keep,
                // Snapshot chunks ride the transport envelope; the chunk
                // size must stay comfortably under the transport's
                // message-size ceiling or a large snapshot can NEVER install
                // (the oversized chunk is rejected server-side and openraft
                // hot-retries it forever — adversarial finding, 2026-07-24).
                // See network::RAFT_MAX_MESSAGE_BYTES and its headroom test.
                snapshot_max_chunk_size: crate::storage::raft::network::SNAPSHOT_CHUNK_BYTES,
                // ...and the chunk must also fit its DEADLINE, which
                // openraft defaults to 200ms — the second, subtler half of
                // the same ceiling. See INSTALL_SNAPSHOT_TIMEOUT_MS and
                // MAX_PAYLOAD_ENTRIES.
                install_snapshot_timeout: INSTALL_SNAPSHOT_TIMEOUT_MS,
                max_payload_entries: MAX_PAYLOAD_ENTRIES,
                ..Default::default()
            }
            .validate()
            .context("invalid openraft config")?,
        );

        let pool = Arc::new(ChannelPool::new());
        // Detached clone: shares DB/read-model/event channels with the real
        // store but holds no back-reference to this driver or the
        // replication handle (see module docs on shutdown).
        let engine_store = store.raft_engine_clone();
        let self_node = BasicNode::new(encode_node_addr(&self.node_id, &self.advertise_addr));
        let raft_id = self.raft_id;
        let bootstrap = self.cfg.bootstrap;

        // Raft::new / initialize are async; drive them on the (brand-new)
        // engine runtime from a scoped thread so this works whether
        // start_storage was called from sync or async context.
        let bring_up = async {
            let mut lanes: HashMap<Lane, Raft> = HashMap::new();
            let mut had_any_state = false;
            for lane in lanes_list.iter().copied() {
                let log_store = RaftLogStore::open(&data_dir, lane)?;
                let mut log_probe = log_store.clone();
                let had_state = openraft::storage::RaftLogStorage::get_log_state(&mut log_probe)
                    .await
                    .map_err(|e| anyhow!("failed reading raft log state: {e}"))?
                    .last_log_id
                    .is_some();
                had_any_state |= had_state;
                let state_machine = StoreStateMachine::new(
                    lane,
                    engine_store.clone(),
                    data_dir.join(lane.wire_name()).join("snapshots"),
                    snapshot_scope,
                );
                let network =
                    RaftTransportNetworkFactory::new(lane, cluster_secret.clone(), pool.clone());
                let raft = Raft::new(
                    raft_id,
                    raft_config.clone(),
                    network,
                    log_store,
                    state_machine,
                )
                .await
                .map_err(|e| {
                    anyhow!(
                        "failed starting raft core for lane {}: {e}",
                        lane.wire_name()
                    )
                })?;

                if bootstrap {
                    let mut members = BTreeMap::new();
                    members.insert(raft_id, self_node.clone());
                    match raft.initialize(members).await {
                        Ok(()) => info!(
                            lane = lane.wire_name(),
                            "Bootstrap-initialized raft group with self as sole voter"
                        ),
                        // Restart of an already-initialized node: exactly the
                        // idempotent case openraft documents as safe to ignore.
                        Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                            debug!(
                                lane = lane.wire_name(),
                                "Raft group already initialized; skipping bootstrap"
                            );
                        }
                        Err(e) => {
                            return Err(anyhow!(
                                "failed bootstrapping raft lane {}: {e}",
                                lane.wire_name()
                            ))
                        }
                    }
                }

                // A fresh bootstrap must know a leader (itself) before the
                // manager starts serving: a single-voter group elects
                // immediately, so a timeout means broken config — fail
                // loudly. A *restarted member* (had_state) may be unable to
                // learn the leader yet: its inbound transport shares the
                // manager listener, which only comes up after engine start,
                // so a behind-the-log follower cannot hear from the leader
                // until then. Waiting would deadlock startup — degrade to a
                // warning and proceed leaderless; the node catches up (or
                // wins its own election) once the listener serves. A blank
                // non-bootstrap node is awaiting a join and proceeds
                // leaderless.
                if bootstrap || had_state {
                    let wait_result = raft
                        .wait(Some(LEADER_WAIT))
                        .metrics(|m| m.current_leader.is_some(), "leader elected")
                        .await;
                    match wait_result {
                        Ok(_) => {}
                        Err(e) if had_state => warn!(
                            lane = lane.wire_name(),
                            error = %e,
                            "No raft leader known within {LEADER_WAIT:?}; proceeding \
                             leaderless — this restarted member will catch up once the \
                             manager listener is up and peers reconnect"
                        ),
                        Err(e) => {
                            return Err(anyhow!(
                                "timed out waiting for a raft leader on lane {}: {e}",
                                lane.wire_name()
                            ))
                        }
                    }
                }

                // JC-6 upgrade path (plan §A4): at bootstrap, any pre-raft
                // content of the state DB (an upgrade from local_disk or the
                // old simulated raft) exists only in this node's DB — no log
                // entry describes it. A joiner caught up by log replay alone
                // would silently miss ALL of it (until the first organic
                // snapshot at `snapshotEntries` entries), and later entries
                // touching those rows would conflict-abort on it. Force a
                // whole-DB snapshot as the log's base and purge the log up
                // to it, so every blank joiner is necessarily caught up by
                // snapshot install (which carries the full DB) before log
                // replay. Idempotent: skipped once any snapshot exists.
                // Under `lanes`, each lane's seed snapshot carries its own
                // owned tables, so together the three lanes' seed snapshots
                // cover the whole adopted baseline.
                if bootstrap {
                    let metrics = raft.metrics().borrow().clone();
                    // Leaderless restart (the warn path above) skips: with no
                    // leader nothing new applies, and the seed snapshot from
                    // the original bootstrap already exists in the common
                    // case.
                    if metrics.snapshot.is_none() && metrics.current_leader.is_some() {
                        // The election's first entries (membership/blank) may
                        // still be in flight; the snapshot needs an applied
                        // log id to anchor to.
                        raft.wait(Some(LEADER_WAIT))
                            .metrics(|m| m.last_applied.is_some(), "initial entry applied")
                            .await
                            .map_err(|e| {
                                anyhow!(
                                    "timed out waiting for the initial entry to apply before \
                                     the bootstrap seed snapshot on lane {}: {e}",
                                    lane.wire_name()
                                )
                            })?;
                        raft.trigger().snapshot().await.map_err(|e| {
                            anyhow!(
                                "failed triggering bootstrap seed snapshot on lane {}: {e}",
                                lane.wire_name()
                            )
                        })?;
                        raft.wait(Some(LEADER_WAIT))
                            .metrics(|m| m.snapshot.is_some(), "bootstrap seed snapshot built")
                            .await
                            .map_err(|e| {
                                anyhow!(
                                    "timed out building bootstrap seed snapshot on lane {}: {e}",
                                    lane.wire_name()
                                )
                            })?;
                    }
                    // The purge is re-issued on any bootstrap start that
                    // finds a snapshot with a NEVER-PURGED log — not only in
                    // the same start that built the seed. The two steps are
                    // separately durable, so a crash between "seed snapshot
                    // built" and "log purged behind it" used to strand the
                    // adopted baseline (adversarial finding F1, 2026-07-28):
                    // the restart saw a snapshot, skipped the whole block,
                    // and the log still started at index 1 — so a blank
                    // joiner was served by log REPLAY and silently missed
                    // every pre-raft row. `purged == 0` is exactly that
                    // signature: every organic snapshot purges behind itself
                    // (keeping its catch-up tail), so a mature cluster
                    // always has `purged > 0` and is never re-purged here
                    // (which would strip the tail the organic policy keeps).
                    let metrics = raft.metrics().borrow().clone();
                    if metrics.current_leader.is_some() {
                        let snapshot_index =
                            metrics.snapshot.map(|log_id| log_id.index).unwrap_or(0);
                        let purged = metrics.purged.map(|p| p.index).unwrap_or(0);
                        if snapshot_index > 0 && purged == 0 {
                            let upto = snapshot_index;
                            raft.trigger().purge_log(upto).await.map_err(|e| {
                                anyhow!(
                                    "failed purging log behind bootstrap seed snapshot on \
                                     lane {}: {e}",
                                    lane.wire_name()
                                )
                            })?;
                            raft.wait(Some(LEADER_WAIT))
                                .metrics(
                                    |m| m.purged.map(|p| p.index).unwrap_or(0) >= upto,
                                    "log purged to bootstrap seed snapshot",
                                )
                                .await
                                .map_err(|e| {
                                    anyhow!(
                                        "timed out purging log behind bootstrap seed snapshot \
                                         on lane {}: {e}",
                                        lane.wire_name()
                                    )
                                })?;
                            info!(
                                lane = lane.wire_name(),
                                purged_to = upto,
                                "Bootstrap seed snapshot in place: pre-raft state will reach \
                                 joiners via snapshot install"
                            );
                        }
                    }
                }
                lanes.insert(lane, raft);
            }
            Ok::<_, anyhow::Error>((lanes, had_any_state))
        };
        let (lanes, had_any_state) = std::thread::scope(|scope| {
            let handle = handle.clone();
            scope
                .spawn(move || handle.block_on(bring_up))
                .join()
                .map_err(|_| anyhow!("raft engine bring-up thread panicked"))
        })??;

        // Transport: inbound RPCs can now be dispatched to the lanes, and
        // Join requests (token validation → learner add on every lane →
        // caught-up wait → voter promotion) are handled by the engine.
        let join_handler = Arc::new(EngineJoinHandler {
            node_id: self.node_id.clone(),
            cluster_id: cluster_id.clone(),
            cluster_secret: cluster_secret.clone(),
            topology: canonical_topology(&self.cfg.topology).to_string(),
            data_dir: data_dir.clone(),
            lanes: lanes.clone(),
            history: self.history.clone(),
            keyring: store.keyring().clone(),
        });
        self.registry
            .set(RaftTransportRegistry {
                cluster_secret: cluster_secret.clone(),
                lanes: lanes.clone(),
                join_handler: Some(join_handler),
                forward_store: Some(store.forward_executor_clone()),
            })
            .map_err(|_| anyhow!("raft transport registry already populated"))?;

        // History: record REAL transitions from the metrics streams.
        for (lane, raft) in &lanes {
            handle.spawn(watch_lane_transitions(
                *lane,
                raft.metrics(),
                self.history.clone(),
                self.election_epoch_ms.clone(),
            ));
        }

        // Co-located lane leadership (JC-40): under a multi-lane topology,
        // the control-lane leader campaigns for the lanes it does not lead
        // until one node leads all three. Not spawned under `single` (one
        // group — nothing to converge).
        if lanes.len() > 1 {
            handle.spawn(lane_convergence_loop(
                self.node_id.clone(),
                lanes.clone(),
                self.lane_convergence.clone(),
                self.lane_campaigns.clone(),
                self.cfg.election_timeout_max_ms,
            ));
        }

        // Replication pipeline: one dedicated OS thread per group (JC-3).
        let single_topology = lanes_list.len() == 1;
        let mut senders: HashMap<Lane, std_mpsc::Sender<PipelineJob>> = HashMap::new();
        for (lane, raft) in &lanes {
            let (tx, rx) = std_mpsc::channel::<PipelineJob>();
            let thread_store = store.raft_engine_clone();
            let thread_raft = raft.clone();
            let thread_handle = handle.clone();
            let group = *lane;
            std::thread::Builder::new()
                .name(format!("raft-pipeline-{}", group.wire_name()))
                .spawn(move || {
                    pipeline_loop(
                        thread_store,
                        thread_raft,
                        thread_handle,
                        group,
                        single_topology,
                        rx,
                    )
                })
                .context("failed spawning raft pipeline thread")?;
            senders.insert(group, tx);
        }
        let single_group = single_topology;
        let submit = move |lane: Lane, op: &'static str, f: ReplicatedOp| {
            let group = if single_group { Lane::Control } else { lane };
            let sender = senders
                .get(&group)
                .ok_or_else(|| anyhow!("no raft pipeline for lane {}", group.wire_name()))?;
            let (reply_tx, reply_rx) = std_mpsc::channel();
            let telemetry = crate::manager::telemetry::metrics();
            telemetry.raft_pipeline_depth.add(&[group.wire_name()], 1);
            sender
                .send(PipelineJob {
                    lane,
                    op,
                    f,
                    reply: reply_tx,
                    queued_at: std::time::Instant::now(),
                })
                .map_err(|_| {
                    // The job never reached the (dead) pipeline thread, so
                    // nothing will decrement the depth for it.
                    telemetry.raft_pipeline_depth.add(&[group.wire_name()], -1);
                    anyhow!(
                        "raft replication pipeline for lane {} has shut down (op {op})",
                        group.wire_name()
                    )
                })?;
            reply_rx.recv().map_err(|_| {
                anyhow!("raft replication pipeline dropped op {op} without replying")
            })?
        };
        store.set_replication_handle(ReplicationHandle {
            submit: Box::new(submit),
        })?;
        // Arm the write_connection bypass guard LAST: from here on every
        // mutation must flow through replicated() or the apply path (JC-14).
        store.set_raft_mode(true);

        // A fresh node holding a join token enrolls itself (plan §A4): dial
        // the leader's `RaftTransport.Join` until it admits this node as a
        // learner on every lane and promotes it to voter. This runs in the
        // background because the leader must be able to dial *back* into
        // this node's transport to catch its log up, and that inbound
        // transport shares the manager listener, which only starts serving
        // after engine start returns. A node that already has raft state is
        // a returning member and needs no join.
        if !bootstrap && !had_any_state {
            if let Some(token) = self.cfg.join_token.clone() {
                let payload = JoinTokenPayload::decode(&token)?;
                handle.spawn(join_cluster_loop(
                    token,
                    payload.leader_advertise_addr,
                    self.node_id.clone(),
                    self.advertise_addr.clone(),
                    cluster_secret.clone(),
                    canonical_topology(&self.cfg.topology).to_string(),
                    self.history.clone(),
                    engine_store.clone(),
                ));
            }
        }

        Self::push_history(
            &self.history,
            "startup",
            format!(
                "Raft engine started: node {} ({:016x}), topology {}, lanes [{}]{}",
                self.node_id,
                self.raft_id,
                self.cfg.topology,
                lanes
                    .keys()
                    .map(|l| l.wire_name())
                    .collect::<Vec<_>>()
                    .join(", "),
                if bootstrap { ", bootstrap" } else { "" }
            ),
            "system",
            None,
            0,
            None,
        );

        self.engine
            .set(Engine {
                runtime: StdMutex::new(Some(runtime)),
                handle,
                lanes,
                cluster_id,
                cluster_secret,
                pool,
                _data_dir_lock: StdMutex::new(data_dir_lock),
            })
            .map_err(|_| anyhow!("raft engine already started"))?;

        info!(
            node_id = %self.node_id,
            raft_id = format!("{:016x}", self.raft_id),
            topology = %self.cfg.topology,
            advertise = %self.advertise_addr,
            "Started raft persistence engine"
        );
        Ok(())
    }

    fn status(&self) -> PersistenceStatus {
        let role = match self.engine.get().and_then(Self::control_metrics) {
            Some(metrics) => map_server_state(metrics.state),
            None => PersistenceRole::Standby,
        };
        PersistenceStatus {
            backend: "raft".to_string(),
            role,
            lease_held: role == PersistenceRole::Primary,
        }
    }

    fn raft_status(&self) -> Option<RaftStatus> {
        let peers: Vec<String> = self
            .cfg
            .peers
            .iter()
            .map(|p| {
                p.split_once('=')
                    .map(|(name, _)| name)
                    .unwrap_or(p)
                    .to_string()
            })
            .collect();
        let base = RaftStatus {
            enabled: true,
            backend: "raft".to_string(),
            node_id: self.node_id.clone(),
            cluster_id: String::new(),
            role: PersistenceRole::Standby,
            leader_id: None,
            term: 0,
            commit_index: 0,
            applied_index: 0,
            lease_held: false,
            voters: Vec::new(),
            learners: Vec::new(),
            peers,
            election_epoch_ms: self.election_epoch_ms.load(Ordering::Relaxed),
            join_token_ttl_seconds: self.cfg.join_token_ttl_seconds,
            raft_library: "openraft-0.9".to_string(),
            lanes: Vec::new(),
        };
        let Some(engine) = self.engine.get() else {
            // Engine not started yet: raft is configured but idle.
            return Some(base);
        };
        // Per-lane detail: one entry per active group, sorted by wire name
        // for stable output.
        let mut lane_statuses: Vec<RaftLaneStatus> = engine
            .lanes
            .iter()
            .map(|(lane, raft)| {
                let m = raft.metrics().borrow().clone();
                // Quorum math + advisory (plan §7-C3): one wording source for
                // every surface, see `quorum.rs`.
                let membership = m.membership_config.membership();
                let voters = membership.voter_ids().count() as u64;
                // A JOINT membership (change in flight) needs a majority of
                // EVERY config, so the honest numbers are the strictest
                // config's, not the union's: a joint {a,b} + {a,b,c} needs 2
                // and survives 0, while the union of three would claim it
                // survives 1.
                let configs: Vec<Vec<String>> = membership
                    .get_joint_config()
                    .iter()
                    .map(|config| config.iter().map(|id| member_name(&m, *id)).collect())
                    .collect();
                let (quorum, survives) = if configs.len() > 1 {
                    (
                        configs
                            .iter()
                            .map(|config| quorum::quorum_size(config.len() as u64))
                            .max()
                            .unwrap_or(0),
                        configs
                            .iter()
                            .map(|config| quorum::failures_survivable(config.len() as u64))
                            .min()
                            .unwrap_or(0),
                    )
                } else {
                    (
                        quorum::quorum_size(voters),
                        quorum::failures_survivable(voters),
                    )
                };
                RaftLaneStatus {
                    lane: lane.wire_name().to_string(),
                    role: map_server_state(m.state),
                    leader_id: m.current_leader.map(|id| member_name(&m, id)),
                    term: m.current_term,
                    commit_index: m.last_log_index.unwrap_or(0),
                    applied_index: m.last_applied.map(|l| l.index).unwrap_or(0),
                    voters,
                    quorum,
                    survives,
                    warning: quorum::joint_membership_advisory(&configs)
                        .or_else(|| quorum::voter_count_warning(voters, self.cfg.expected_voters)),
                }
            })
            .collect();
        lane_statuses.sort_by(|a, b| a.lane.cmp(&b.lane));
        // Aggregate leader line: with one group (or all lanes agreeing on a
        // leader) report that leader plainly; when lanes have DIFFERENT
        // leaders, summarize per lane so the headline never claims a single
        // leader that only holds one lane.
        let distinct_leaders: std::collections::BTreeSet<Option<&String>> = lane_statuses
            .iter()
            .map(|lane| lane.leader_id.as_ref())
            .collect();
        let split_leader_summary =
            (lane_statuses.len() > 1 && distinct_leaders.len() > 1).then(|| {
                lane_statuses
                    .iter()
                    .map(|lane| {
                        format!("{}={}", lane.lane, lane.leader_id.as_deref().unwrap_or("-"))
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            });
        let metrics = Self::control_metrics(engine)?;
        let membership = metrics.membership_config.membership().clone();
        // The aggregate voter list means "voter in EVERY config of EVERY
        // lane", not "voter on the anchor lane". Under `lanes` the two
        // differ exactly while a join is in flight or half-applied, and
        // that is the case the headline must not paper over: reading the
        // anchor lane alone reports a full 3-voter cluster while another
        // lane is still a 2-voter (or joint) group that survives zero
        // failures. Per-lane detail stays in `lanes`.
        //
        // Deliberately the JOINT-INTERSECTION predicate, not
        // `lane_membership_settled`: during a join's joint window the
        // settled predicate fails for EVERY member (the whole membership is
        // joint), so a healthy 3-voter cluster mid-join reported
        // `voters: []` and everyone as a learner (adversarial finding,
        // 2026-07-28). A member in every config of a joint membership is a
        // voter under the strictest quorum either config demands — the same
        // strictest-config arithmetic the per-lane quorum/survives numbers
        // use — while the incoming joiner (present only in the new config)
        // still waits for uniform settlement, preserving the original
        // no-overcount fix. Join gating and readiness keep the strict
        // settled predicate.
        let voter_set: std::collections::HashSet<NodeId> = membership
            .voter_ids()
            .filter(|id| {
                engine.lanes.values().all(|raft| {
                    voter_in_every_config(
                        raft.metrics().borrow().membership_config.membership(),
                        *id,
                    )
                })
            })
            .collect();
        let voters: Vec<String> = membership
            .voter_ids()
            .filter(|id| voter_set.contains(id))
            .map(|id| member_name(&metrics, id))
            .collect();
        let learners: Vec<String> = membership
            .nodes()
            .filter(|(id, _)| !voter_set.contains(id))
            .map(|(id, _)| member_name(&metrics, *id))
            .collect();
        let role = map_server_state(metrics.state);
        Some(RaftStatus {
            cluster_id: engine.cluster_id.clone(),
            role,
            leader_id: split_leader_summary
                .or_else(|| metrics.current_leader.map(|id| member_name(&metrics, id))),
            term: metrics.current_term,
            commit_index: metrics.last_log_index.unwrap_or(0),
            applied_index: metrics.last_applied.map(|l| l.index).unwrap_or(0),
            lease_held: role == PersistenceRole::Primary,
            voters,
            learners,
            lanes: lane_statuses,
            ..base
        })
    }

    fn raft_history(&self, limit: usize) -> Vec<RaftHistoryEvent> {
        let Ok(state) = self.history.lock() else {
            return Vec::new();
        };
        state
            .events
            .iter()
            .rev()
            .take(limit.clamp(1, 500))
            .cloned()
            .collect()
    }

    fn issue_raft_command(
        &self,
        command: &str,
        target: Option<&str>,
        actor: &str,
    ) -> R<RaftCommandOutcome> {
        let engine = self.engine()?;
        let metrics = Self::control_metrics(engine);
        let term = metrics.as_ref().map(|m| m.current_term).unwrap_or(0);
        let leader = metrics
            .as_ref()
            .and_then(|m| m.current_leader.map(|id| member_name(m, id)));
        // Voter-count ergonomics (plan §7-C3, JC-29): membership arms fill
        // this with the parity advisory when the RESULTING voter count is
        // even; it is logged, recorded in history, and threaded to the
        // caller through `IssueRaftCommandReply.warning`.
        let mut warning: Option<String> = None;

        let event = match command {
            "trigger-election" => {
                for (lane, raft) in lanes_in_order(&engine.lanes) {
                    Self::block_on_engine(engine, raft.trigger().elect())?.map_err(|e| {
                        anyhow!(
                            "failed triggering election on lane {}: {e}",
                            lane.wire_name()
                        )
                    })?;
                }
                Self::push_history(
                    &self.history,
                    "election",
                    format!("Manual election triggered by {actor} on every lane"),
                    actor,
                    target.map(str::to_string),
                    term,
                    leader,
                )
            }
            // openraft 0.9 has no leader-transfer / step-down API (it lands
            // in 0.10). The A-2 lifecycle phase implements these as a
            // remote-elect side RPC on the Join service; until then the gap
            // is reported honestly instead of simulated.
            "step-down" | "transfer-leader" => bail!(
                "raft command `{command}` is not supported by openraft 0.9 (no leader-transfer \
                 API); trigger-election on the desired leader is the current equivalent — a \
                 remote-elect operator RPC ships with the membership phase"
            ),
            "add-learner" | "add-voter" => {
                let target =
                    target.with_context(|| format!("{command} command requires a target"))?;
                let (name, addr) = self.resolve_target(target);
                let addr = addr.with_context(|| {
                    format!(
                        "cannot resolve an address for `{name}`: pass `name@host:port` or list \
                         the peer as `name=host:port` in persistence.raft.peers"
                    )
                })?;
                let member_id = fnv1a64(&name);
                let node = BasicNode::new(encode_node_addr(&name, &addr));
                // add-voter parity check on the headline control group: an
                // even RESULT is never refused, only warned about (JC-29).
                // add-learner leaves the voter count untouched.
                if command == "add-voter" {
                    if let Some(m) = metrics.as_ref() {
                        let mut resulting: std::collections::BTreeSet<NodeId> =
                            m.membership_config.membership().voter_ids().collect();
                        resulting.insert(member_id);
                        warning = quorum::parity_advisory(resulting.len() as u64);
                    }
                }
                for (lane, raft) in lanes_in_order(&engine.lanes) {
                    let result = if command == "add-learner" {
                        Self::block_on_engine(
                            engine,
                            raft.add_learner(member_id, node.clone(), false),
                        )?
                    } else {
                        let mut members = BTreeMap::new();
                        members.insert(member_id, node.clone());
                        Self::block_on_engine(
                            engine,
                            raft.change_membership(
                                openraft::ChangeMembers::AddVoters(members),
                                false,
                            ),
                        )?
                    };
                    result.map_err(|e| {
                        map_write_error(command, Some(lane.wire_name()), e)
                            .context(format!("on lane {}", lane.wire_name()))
                    })?;
                }
                Self::push_history(
                    &self.history,
                    "membership",
                    format!(
                        "{} {name} ({addr}) added by {actor}",
                        if command == "add-learner" {
                            "Learner"
                        } else {
                            "Voter"
                        }
                    ),
                    actor,
                    Some(name),
                    term,
                    leader,
                )
            }
            "remove-member" => {
                let target = target.context("remove-member command requires a target")?;
                let (name, _) = self.resolve_target(target);
                let member_id = fnv1a64(&name);
                // Self-removal is refused outright (adversarial finding,
                // 2026-07-28). The unled-lanes guard below makes the
                // all-lane leader the only node allowed to run a removal,
                // so decommissioning that leader would have to be
                // SELF-removal — and committing the self-excluding config
                // on the first lane deposes this node, so every later
                // lane's change fails and the removal half-applies by
                // construction (removed from some lanes, a voter on the
                // rest). The supported decommission order is: move
                // leadership first, then remove the now-follower from the
                // new leader.
                //
                // A SOLE voter removing itself falls through instead: the
                // two-step recipe is meaningless with no other manager,
                // and the removal guard below refuses it with the
                // zero-voters advisory, which is the actionable message.
                let sole_voter = engine
                    .lanes
                    .get(&ANCHOR_LANE)
                    .map(|raft| {
                        let m = raft.metrics().borrow().clone();
                        m.membership_config.membership().voter_ids().count() <= 1
                    })
                    .unwrap_or(false);
                if name == self.node_id && !sole_voter {
                    bail!(
                        "refusing to remove this manager ({name}) from the cluster it is \
                         currently leading: committing a self-excluding membership change \
                         deposes this node mid-removal and leaves it removed from some lanes \
                         but not others. Decommission in two steps instead: run \
                         `legion raft command trigger-election --target <another-manager>` \
                         until that manager leads every lane (`legion raft status`), then run \
                         `legion raft command remove-member --target {name}` there"
                    );
                }
                // Removal guard (plan §7-C3, JC-29 — the ONLY hard block):
                // computed BEFORE anything is issued. Refuses removals that
                // would leave zero voters or a quorum the currently-known-
                // alive members could not form (the membership change itself
                // could then never commit — a wedged cluster). An even
                // RESULT proceeds with a loud warning instead. A non-leader
                // skips the guard and falls through to the ForwardToLeader
                // hint from openraft.
                //
                // EVERY lane is evaluated, not just the headline control
                // group (JC-40): the removal below is applied to every
                // lane, and per-lane membership can legitimately differ for
                // a while (a join promotes lane by lane; a previous partial
                // removal stopped at some lane). Guarding only control would
                // let a removal that is safe there wedge the scheduler or
                // status lane — a lane whose quorum is gone can never commit
                // the membership change that would repair it.
                // Liveness for the feasibility guard is an ACTIVE probe of
                // each resulting voter's transport listener, not openraft's
                // replication view (adversarial finding, 2026-07-28: a
                // member that acked one entry at any point under the
                // current leadership counted as alive forever, so on a
                // quiet lane a member dead for hours passed the guard and
                // the removal wedged the lane in a joint config no quorum
                // could ever commit). The probe is a fresh dial with a
                // short timeout, memoized across lanes; this node counts as
                // alive without dialing itself.
                let mut probed: HashMap<NodeId, bool> = HashMap::new();
                let mut unled_lanes: Vec<&'static str> = Vec::new();
                for (lane, raft) in lanes_in_order(&engine.lanes) {
                    let m = raft.metrics().borrow().clone();
                    if m.state != ServerState::Leader {
                        unled_lanes.push(lane.wire_name());
                        continue;
                    }
                    let voter_set: std::collections::BTreeSet<NodeId> =
                        m.membership_config.membership().voter_ids().collect();
                    if !voter_set.contains(&member_id) {
                        continue;
                    }
                    let resulting: std::collections::BTreeSet<NodeId> = voter_set
                        .iter()
                        .copied()
                        .filter(|id| *id != member_id)
                        .collect();
                    let mut alive_resulting = 0u64;
                    for id in &resulting {
                        if *id == m.id {
                            alive_resulting += 1;
                            continue;
                        }
                        let reachable = *probed.entry(*id).or_insert_with(|| {
                            m.membership_config
                                .membership()
                                .get_node(id)
                                .map(|node| {
                                    Self::probe_member_reachable(
                                        engine,
                                        node_addr_host_port(&node.addr),
                                    )
                                })
                                .unwrap_or(false)
                        });
                        if reachable {
                            alive_resulting += 1;
                        }
                    }
                    quorum::removal_guard(&name, resulting.len() as u64, alive_resulting).map_err(
                        |advisory| {
                            if engine.lanes.len() > 1 {
                                anyhow!("{advisory} (evaluated on the {} lane)", lane.wire_name())
                            } else {
                                anyhow!(advisory)
                            }
                        },
                    )?;
                    // The parity advisory keeps naming the headline control
                    // group so the operator-facing wording is unchanged.
                    if lane == ANCHOR_LANE {
                        warning = quorum::parity_advisory(resulting.len() as u64);
                    }
                }
                // A multi-lane removal that this node cannot apply to every
                // lane would half-apply (removed here, still a voter there),
                // and the lanes it could not reach are exactly the ones whose
                // feasibility it could not evaluate. Refuse with the anchor
                // leader's hint instead; JC-40 convergence puts one node in a
                // position to run the whole removal.
                if !unled_lanes.is_empty() && engine.lanes.len() > 1 {
                    let hint = self.not_leader_error(ANCHOR_LANE);
                    bail!(
                        "refusing to remove {name}: this manager does not lead lane(s) [{}], so \
                         the removal would apply to some lanes and not others. Run it on the \
                         node that leads every lane — {hint}",
                        unled_lanes.join(", ")
                    );
                }
                let ids: std::collections::BTreeSet<NodeId> = [member_id].into();
                for (lane, raft) in lanes_in_order(&engine.lanes) {
                    // Demote/remove as voter first, then drop any learner
                    // membership entry. Each change is bounded by
                    // MEMBERSHIP_CHANGE_TIMEOUT: a change whose resulting
                    // config cannot assemble a live quorum never commits,
                    // and an unbounded wait would hang the operator command
                    // while the lane sits joint (the probe above makes this
                    // unlikely, but the member can die between probe and
                    // commit).
                    Self::membership_change_with_timeout(
                        engine,
                        command,
                        lane,
                        &name,
                        raft.change_membership(
                            openraft::ChangeMembers::RemoveVoters(ids.clone()),
                            false,
                        ),
                    )?;
                    Self::membership_change_with_timeout(
                        engine,
                        command,
                        lane,
                        &name,
                        raft.change_membership(
                            openraft::ChangeMembers::RemoveNodes(ids.clone()),
                            false,
                        ),
                    )?;
                }
                Self::push_history(
                    &self.history,
                    "membership",
                    format!("Member {name} removed by {actor}"),
                    actor,
                    Some(name),
                    term,
                    leader,
                )
            }
            other => bail!("unsupported raft command: {other}"),
        };

        // Even-parity advisory (JC-29): the command SUCCEEDED — warn loudly
        // in the log, drop the advisory into the audit history so the trail
        // shows when the cluster entered even parity, and thread it to the
        // caller so the CLI/UI print it on the command's own output.
        if let Some(advisory) = &warning {
            warn!(command, target = ?target, "{advisory}");
            Self::push_history(
                &self.history,
                "membership",
                advisory.clone(),
                actor,
                target.map(str::to_string),
                term,
                None,
            );
        }

        info!(command, actor, target = ?target, "Applied raft operator command");
        Ok(RaftCommandOutcome { event, warning })
    }

    fn issue_raft_join_token(&self, actor: &str) -> R<(String, String)> {
        let engine = self.engine()?;
        // Prefer the current leader's dialable address (a token minted on a
        // follower should still point the joiner at the leader); fall back
        // to this node's advertise address.
        let leader_advertise_addr = Self::control_metrics(engine)
            .and_then(|m| {
                m.current_leader.and_then(|id| {
                    m.membership_config
                        .membership()
                        .get_node(&id)
                        .map(|node| split_node_addr(&node.addr).1.to_string())
                })
            })
            .unwrap_or_else(|| self.advertise_addr.clone());
        let ttl = self.cfg.join_token_ttl_seconds;
        let expires_at = OffsetDateTime::now_utc() + time::Duration::seconds(ttl as i64);
        let payload = JoinTokenPayload {
            cluster_id: engine.cluster_id.clone(),
            secret: engine.cluster_secret.clone(),
            leader_advertise_addr,
            expires_unix: expires_at.unix_timestamp(),
        };
        let token = payload.encode()?;
        let expires = expires_at.format(&time::format_description::well_known::Rfc3339)?;
        Self::push_history(
            &self.history,
            "join_token",
            format!("Issued raft join token (ttl={ttl}s)"),
            actor,
            None,
            Self::control_metrics(engine)
                .map(|m| m.current_term)
                .unwrap_or(0),
            None,
        );
        Ok((token, expires))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lanes_for_topology_maps_single_and_lanes() {
        assert_eq!(lanes_for_topology("single").unwrap(), vec![Lane::Control]);
        assert_eq!(lanes_for_topology("").unwrap(), vec![Lane::Control]);
        assert_eq!(lanes_for_topology("lanes").unwrap(), Lane::ALL.to_vec());
        assert!(lanes_for_topology("sharded").is_err());
    }

    // ── JC-40 convergence governor ───────────────────────────────────────
    //
    // The governor is a pure state machine over sampled metrics precisely so
    // these can drive it against a fake clock: the properties that matter
    // (never disturb an election, bounded term inflation) are impossible to
    // pin down from a live cluster without waiting minutes per assertion.

    /// Test timing: one election "window" is 1s, so the schedule is
    /// min/verdict 2s, ceiling 16s, cooldown 60s, 3 campaigns.
    #[cfg(test)]
    fn test_timing() -> LaneConvergeTiming {
        LaneConvergeTiming::for_election_timeout(1_000)
    }

    /// A lane with a settled leader ELSEWHERE — the only shape convergence
    /// ever acts on.
    #[cfg(test)]
    fn strayed(last_log_index: u64) -> LaneObservation {
        LaneObservation {
            led_here: false,
            has_leader: true,
            last_log_index,
        }
    }

    /// A lane this node leads.
    #[cfg(test)]
    fn led(last_log_index: u64) -> LaneObservation {
        LaneObservation {
            led_here: true,
            has_leader: true,
            last_log_index,
        }
    }

    /// A lane with NO leader anywhere: an election is in flight.
    #[cfg(test)]
    fn leaderless(last_log_index: u64) -> LaneObservation {
        LaneObservation {
            led_here: false,
            has_leader: false,
            last_log_index,
        }
    }

    /// Drive the governor once with no jitter, at `now`.
    #[cfg(test)]
    fn tick(
        governor: &mut LaneConvergeGovernor,
        observation: LaneObservation,
        now: Instant,
    ) -> LaneConvergeAction {
        governor.decide(observation, true, now, test_timing(), 1.0)
    }

    /// Only the anchor-lane leader converges, and it never campaigns for a
    /// lane it already leads.
    #[test]
    fn convergence_acts_only_as_the_anchor_leader_and_only_on_a_strayed_lane() {
        let timing = test_timing();
        let now = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        assert_eq!(
            governor.decide(strayed(9), false, now, timing, 1.0),
            LaneConvergeAction::Hold(LaneConvergeHold::NotAnchorLeader)
        );
        let mut governor = LaneConvergeGovernor::new(timing);
        assert_eq!(
            tick(&mut governor, led(9), now),
            LaneConvergeAction::Hold(LaneConvergeHold::Converged)
        );
        // A lane led elsewhere, log quiescent: this is the one case that
        // campaigns — after the first-campaign spacing (one backoff), never
        // on the first eligible poll (the incumbent's leader lease is
        // typically still live right after a failover).
        let mut governor = LaneConvergeGovernor::new(timing);
        assert_eq!(
            tick(&mut governor, strayed(9), now),
            LaneConvergeAction::Hold(LaneConvergeHold::Backoff)
        );
        assert_eq!(
            tick(&mut governor, strayed(9), now + timing.min_backoff),
            LaneConvergeAction::Campaign
        );
    }

    /// The first-campaign spacing the timing constants always documented is
    /// actually applied: a fresh governor never campaigns before one
    /// backoff has elapsed, no matter how eligible the lane looks.
    #[test]
    fn a_fresh_governor_waits_one_backoff_before_its_first_campaign() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        let mut first_campaign = None;
        for step in 0..20u64 {
            let elapsed = Duration::from_millis(500 * step);
            if tick(&mut governor, strayed(9), start + elapsed) == LaneConvergeAction::Campaign {
                first_campaign = Some(elapsed);
                break;
            }
        }
        let first_campaign = first_campaign.expect("the lane must eventually be campaigned for");
        assert!(
            first_campaign >= timing.min_backoff,
            "campaigned at {first_campaign:?}, inside the {:?} first-campaign spacing",
            timing.min_backoff
        );
    }

    /// An anchor that flaps must not reset the loss ladder: the losses
    /// accumulated before the flap still count toward the stand-down, and
    /// re-acquiring the anchor does not fire an immediate campaign.
    /// (Pre-fix, `NotAnchorLeader` zeroed `lost` and cleared the schedule,
    /// so a flapping anchor campaigned immediately per re-acquisition and
    /// no stand-down could ever engage.)
    #[test]
    fn anchor_flap_keeps_the_loss_ladder_and_the_spacing() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        // Drive to the first campaign and score it lost: the schedule now
        // holds one loss, a grown backoff, and a next-campaign time.
        let mut first_campaign = None;
        for step in 0..40u64 {
            let at = start + Duration::from_millis(500 * step);
            if tick(&mut governor, strayed(9), at) == LaneConvergeAction::Campaign {
                first_campaign = Some(at);
                break;
            }
        }
        let first_campaign = first_campaign.expect("no first campaign");
        let scored_at = first_campaign + timing.verdict + Duration::from_millis(500);
        tick(&mut governor, strayed(9), scored_at);
        assert_eq!(governor.lost, 1, "the campaign must have scored lost");
        let next_due = governor
            .next_campaign_at
            .expect("a lost campaign must schedule the next one");
        // Flap the anchor away and back while the backoff is still waiting.
        let flap = scored_at + Duration::from_millis(500);
        assert!(flap < next_due, "flap must land inside the backoff window");
        assert_eq!(
            governor.decide(strayed(9), false, flap, timing, 1.0),
            LaneConvergeAction::Hold(LaneConvergeHold::NotAnchorLeader)
        );
        let back = flap + Duration::from_millis(500);
        assert!(back < next_due);
        assert_eq!(
            tick(&mut governor, strayed(9), back),
            LaneConvergeAction::Hold(LaneConvergeHold::Backoff),
            "re-acquiring the anchor must resume the schedule, not restart it"
        );
        assert_eq!(governor.lost, 1, "the flap must not reset the loss ladder");
        // The ladder still completes: keep driving and the lane stands down
        // after max_campaigns total losses, flap notwithstanding.
        let mut stood_down = false;
        for later in 0..600u64 {
            let at = back + Duration::from_millis(500 * later);
            tick(&mut governor, strayed(9), at);
            if governor.take_stood_down() {
                stood_down = true;
                break;
            }
        }
        assert!(stood_down, "the stand-down never engaged after the flap");
    }

    /// THE invariant: convergence never campaigns into a leaderless lane,
    /// no matter how long it stays leaderless.
    ///
    /// A leaderless lane is already holding an election on every voter's own
    /// randomized timer — including this node's, run by openraft's tick with
    /// raft's real policy (leader lease, `smaller_log_timeout` deference to a
    /// greater log). `trigger().elect()` bypasses all of it, re-arms this
    /// node's candidacy at a higher term and discards the votes already
    /// collected. Two campaigners on a 3-voter group with one member dead
    /// never reach quorum, and the term climbs forever — 132 terms in 210s
    /// on a 2-core runner, applied index frozen. So: never.
    #[test]
    fn convergence_never_campaigns_for_a_leaderless_lane() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        // Ten minutes of a leaderless lane, at every cadence the loop polls.
        for step in 0..1_200u64 {
            let now = start + Duration::from_millis(500 * step);
            assert_eq!(
                tick(&mut governor, leaderless(9), now),
                LaneConvergeAction::Hold(LaneConvergeHold::NaturalElection),
                "campaigned into an in-flight election at step {step}"
            );
        }
        assert_eq!(governor.campaigns, 0);
    }

    /// A lane whose log is still streaming into this node cannot be won —
    /// this node is behind whoever it replicates from — so no campaign is
    /// issued while the log grows. This is the only log-currency signal
    /// openraft 0.9 gives a non-leader, and it cannot tell catch-up replay
    /// from ordinary replication traffic, so the hold is CAPPED (see the
    /// sustained-traffic test below).
    #[test]
    fn convergence_holds_off_while_this_nodes_lane_log_is_catching_up() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        // Past the first-campaign spacing with a quiescent log: campaigns.
        assert_eq!(
            tick(&mut governor, strayed(10), start),
            LaneConvergeAction::Hold(LaneConvergeHold::Backoff)
        );
        assert_eq!(
            tick(&mut governor, strayed(10), start + timing.min_backoff),
            LaneConvergeAction::Campaign
        );
        // Win it, then lose it: the re-armed spacing waits first, then the
        // growing log holds the campaign off, and a quiescent log releases
        // it.
        assert_eq!(
            tick(&mut governor, led(11), start + Duration::from_secs(3)),
            LaneConvergeAction::Hold(LaneConvergeHold::Converged)
        );
        assert_eq!(
            tick(&mut governor, strayed(12), start + Duration::from_secs(4)),
            LaneConvergeAction::Hold(LaneConvergeHold::Backoff)
        );
        let eligible = start + Duration::from_secs(4) + timing.min_backoff;
        assert_eq!(
            tick(&mut governor, strayed(13), eligible),
            LaneConvergeAction::Hold(LaneConvergeHold::CatchingUp)
        );
        assert_eq!(
            tick(
                &mut governor,
                strayed(14),
                eligible + Duration::from_millis(500)
            ),
            LaneConvergeAction::Hold(LaneConvergeHold::CatchingUp)
        );
        assert_eq!(
            tick(
                &mut governor,
                strayed(14),
                eligible + Duration::from_secs(1)
            ),
            LaneConvergeAction::Campaign
        );
    }

    /// The catching-up hold is capped: a lane under SUSTAINED write traffic
    /// grows its log on every poll, and an uncapped hold would starve
    /// convergence forever — the standing split would become the steady
    /// state exactly under the load lanes exist for (adversarial finding,
    /// 2026-07-28). After `LANE_CONVERGE_CATCHUP_HOLD_MAX_POLLS`
    /// consecutive holds the campaign proceeds and the election is the
    /// judge.
    #[test]
    fn sustained_log_growth_cannot_starve_convergence() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        let mut campaigned_at_step = None;
        // A lane led elsewhere whose log grows every single poll, forever.
        for step in 0..LANE_CONVERGE_CATCHUP_HOLD_MAX_POLLS as u64 + 40 {
            let at = start + Duration::from_millis(500 * step);
            if tick(&mut governor, strayed(step), at) == LaneConvergeAction::Campaign {
                campaigned_at_step = Some(step);
                break;
            }
        }
        assert!(
            campaigned_at_step.is_some(),
            "a busy lane was never campaigned for: convergence starved"
        );
    }

    /// Bounded term inflation: campaigns back off exponentially and then
    /// stop entirely for a cooldown far longer than any election timeout, so
    /// a lane convergence can never take gets long, UNINTERRUPTED windows in
    /// which raft's own elections run without a foreign term bump.
    #[test]
    fn convergence_backs_off_and_then_stands_down_for_a_long_cooldown() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        let mut campaign_times: Vec<Duration> = Vec::new();
        let mut longest_quiet = Duration::ZERO;
        let mut last_campaign = Duration::ZERO;
        // 30 minutes of a lane this node can never take, polled at the
        // loop's real cadence.
        for step in 0..3_600u64 {
            let elapsed = Duration::from_millis(500 * step);
            if tick(&mut governor, strayed(9), start + elapsed) == LaneConvergeAction::Campaign {
                longest_quiet = longest_quiet.max(elapsed - last_campaign);
                last_campaign = elapsed;
                campaign_times.push(elapsed);
            }
        }
        // Term inflation from convergence is a handful of terms per half
        // hour, not hundreds — and it thins out as the stand-downs escalate.
        // (The pre-fix loop produced 132 terms in 210 SECONDS on the same
        // shape of failure.)
        assert!(
            campaign_times.len() <= 20,
            "unbounded campaigning: {} campaigns in 30 minutes ({campaign_times:?})",
            campaign_times.len()
        );
        // Campaigns arrive in bursts of at most `max_campaigns` separated by
        // stand-downs, and each stand-down is longer than the one before it
        // until the doubling cap — so a lane that keeps refusing convergence
        // is tried ever more rarely instead of forever at a fixed rate.
        let stand_downs: Vec<Duration> = campaign_times
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .filter(|gap| *gap >= timing.cooldown)
            .collect();
        assert!(
            stand_downs.len() >= 3,
            "expected several stand-downs, saw {stand_downs:?}"
        );
        for pair in stand_downs.windows(2) {
            assert!(
                pair[1] >= pair[0],
                "stand-down {:?} is shorter than the previous {:?}",
                pair[1],
                pair[0]
            );
        }
        assert!(
            stand_downs.last().copied().unwrap_or_default() > stand_downs[0],
            "stand-downs never escalated: {stand_downs:?}"
        );
        assert_eq!(campaign_times.len() as u64, governor.campaigns);
        // And natural elections get windows an order of magnitude longer
        // than the election timeout they need.
        assert!(
            longest_quiet >= timing.cooldown,
            "longest uninterrupted window {longest_quiet:?} is shorter than the {:?} cooldown",
            timing.cooldown
        );
        // Campaigns are spaced by at least the verdict window: a campaign is
        // never re-issued before the previous one has been decided (that
        // would discard the votes it collected).
        for pair in campaign_times.windows(2) {
            assert!(
                pair[1] - pair[0] >= timing.verdict,
                "campaigns {:?} and {:?} are closer than the verdict window",
                pair[0],
                pair[1]
            );
        }
    }

    /// Winning clears the whole schedule, so a lane that strays again later
    /// is reacquired promptly rather than serving out an old cooldown.
    #[test]
    fn taking_the_lane_resets_the_campaign_schedule() {
        let timing = test_timing();
        let start = Instant::now();
        let mut governor = LaneConvergeGovernor::new(timing);
        // Burn through to the stand-down.
        for step in 0..200u64 {
            tick(
                &mut governor,
                strayed(9),
                start + Duration::from_millis(500 * step),
            );
        }
        assert!(governor.cooldown_until.is_some(), "expected a stand-down");
        let cooled = start + Duration::from_secs(100);
        assert_eq!(
            tick(&mut governor, led(9), cooled),
            LaneConvergeAction::Hold(LaneConvergeHold::Converged)
        );
        assert!(governor.cooldown_until.is_none());
        assert_eq!(governor.backoff, timing.min_backoff);
        // Straying again re-campaigns after just the first-campaign spacing
        // (one min backoff) rather than serving out the old cooldown.
        assert_eq!(
            tick(
                &mut governor,
                strayed(9),
                cooled + Duration::from_millis(500)
            ),
            LaneConvergeAction::Hold(LaneConvergeHold::Backoff)
        );
        assert_eq!(
            tick(
                &mut governor,
                strayed(9),
                cooled + Duration::from_millis(500) + timing.min_backoff
            ),
            LaneConvergeAction::Campaign
        );
    }

    /// The whole schedule scales with `raft.electionTimeoutMaxMs` — raising
    /// it is the documented remedy for loaded managers, WAN links and CI,
    /// and a schedule in fixed milliseconds would start campaigning inside
    /// the (now longer) leader lease the moment an operator applied it.
    #[test]
    fn convergence_timing_scales_with_the_configured_election_window() {
        for election_max in [
            crate::structs::config::DEF_RAFT_ELECTION_TIMEOUT_MAX_MS,
            2_000,
            8_000,
        ] {
            let timing = LaneConvergeTiming::for_election_timeout(election_max);
            let window = Duration::from_millis(election_max);
            assert!(
                timing.min_backoff > window,
                "the first campaign must land outside the {window:?} leader lease"
            );
            assert!(timing.verdict > window);
            assert!(timing.max_backoff >= timing.min_backoff);
            assert!(
                timing.cooldown >= window * 30,
                "the stand-down must dwarf an election timeout"
            );
        }
    }

    /// Jitter decorrelates the campaign from the incumbent's own election
    /// timer without ever collapsing to a zero-length wait.
    #[test]
    fn campaign_jitter_stays_in_window() {
        for _ in 0..256 {
            let factor = campaign_jitter();
            assert!((0.75..=1.25).contains(&factor), "{factor} out of window");
        }
        assert!(scaled(Duration::from_secs(2), 0.75) >= Duration::from_millis(1_500));
        assert!(scaled(Duration::ZERO, 1.0) > Duration::ZERO);
    }

    /// During a join's joint window the incumbents must keep counting as
    /// voters (they are voters under the strictest config in force) while
    /// the incoming member does not — and the strict settled predicate must
    /// stay false for everyone until the membership is uniform again.
    #[test]
    fn joint_membership_voter_predicates() {
        use std::collections::{BTreeMap, BTreeSet};
        let node = |name: &str| BasicNode {
            addr: format!("{name}@127.0.0.1:0"),
        };
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, node("a"));
        nodes.insert(2u64, node("b"));
        nodes.insert(3u64, node("c"));
        let old_config: BTreeSet<NodeId> = [1, 2].into_iter().collect();
        let new_config: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        let joint = openraft::Membership::<NodeId, BasicNode>::new(
            vec![old_config, new_config.clone()],
            nodes.clone(),
        );
        for id in [1u64, 2, 3] {
            assert!(
                !lane_membership_settled(&joint, id),
                "member {id} must not be SETTLED while the membership is joint"
            );
        }
        assert!(voter_in_every_config(&joint, 1));
        assert!(voter_in_every_config(&joint, 2));
        assert!(
            !voter_in_every_config(&joint, 3),
            "the incoming member is not yet a voter under the strictest config"
        );

        let uniform = openraft::Membership::<NodeId, BasicNode>::new(vec![new_config], nodes);
        for id in [1u64, 2, 3] {
            assert!(lane_membership_settled(&uniform, id));
            assert!(voter_in_every_config(&uniform, id));
        }
    }

    #[test]
    fn join_token_round_trips_and_expires() {
        let payload = JoinTokenPayload {
            cluster_id: "legion-raft-abc".into(),
            secret: "s3cret".into(),
            leader_advertise_addr: "10.0.0.5:4646".into(),
            expires_unix: 1_000,
        };
        let token = payload.encode().unwrap();
        assert!(token.starts_with(JOIN_TOKEN_PREFIX));
        let decoded = JoinTokenPayload::decode(&token).unwrap();
        assert_eq!(decoded.cluster_id, payload.cluster_id);
        assert_eq!(decoded.secret, payload.secret);
        assert_eq!(decoded.leader_advertise_addr, payload.leader_advertise_addr);
        assert!(decoded.is_expired(1_000));
        assert!(!decoded.is_expired(999));
        assert!(JoinTokenPayload::decode("nonsense").is_err());
    }

    #[test]
    fn advertise_addr_normalizes_wildcard_binds() {
        assert_eq!(normalize_advertise_addr("0.0.0.0:4646"), "127.0.0.1:4646");
        assert_eq!(normalize_advertise_addr("[::]:4646"), "127.0.0.1:4646");
        assert_eq!(normalize_advertise_addr("10.1.2.3:4646"), "10.1.2.3:4646");
    }

    #[test]
    fn cluster_id_is_deterministic_from_secret() {
        assert_eq!(derive_cluster_id("abc"), derive_cluster_id("abc"));
        assert_ne!(derive_cluster_id("abc"), derive_cluster_id("abd"));
        assert!(derive_cluster_id("abc").starts_with("legion-raft-"));
    }

    /// Adversarial regression (double-start): a second engine over the same
    /// raft data dir must fail fast on the exclusive lock instead of running
    /// a competing raft core against the same log store.
    #[test]
    fn data_dir_lock_excludes_a_second_holder() -> R<()> {
        let dir = tempfile::tempdir()?;
        let first = acquire_data_dir_lock(dir.path())?;
        let second = acquire_data_dir_lock(dir.path());
        let rendered = format!("{:#}", second.expect_err("second holder must be rejected"));
        assert!(rendered.contains("another legion manager"), "{rendered}");
        // Releasing the first holder (process exit) frees the lock.
        drop(first);
        acquire_data_dir_lock(dir.path())
            .context("lock must be reacquirable after the holder exits")?;
        Ok(())
    }

    fn setup_store() -> R<Store> {
        let dir = tempfile::Builder::new()
            .prefix("khepri-raft-driver-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();
        let config = crate::structs::ManagerConfigFull {
            store_path: dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        Store::start_storage(&config)
    }

    /// Adversarial regression (unguarded topology switch): the topology is
    /// stamped into the state DB at first engine start and a mismatched
    /// restart is refused — flipping `single` ↔ `lanes` over existing raft
    /// state would resume the old topology's log against the new topology's
    /// groups (cross-lane replay / frozen lanes). `forceNewCluster` is the
    /// deliberate-rebuild override.
    #[test]
    fn topology_stamp_refuses_a_flipped_topology() -> R<()> {
        let store = setup_store()?;

        // First start stamps ("" canonicalizes to "single"); a matching
        // restart passes, spelled either way.
        check_topology_stamp(&store, "", false)?;
        check_topology_stamp(&store, "single", false)?;
        check_topology_stamp(&store, "", false)?;

        // Flipping to lanes over the same state DB is refused with the
        // migration guidance.
        let err = check_topology_stamp(&store, "lanes", false)
            .expect_err("a topology flip over existing raft state must be refused");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("`single`"), "{rendered}");
        assert!(rendered.contains("`lanes`"), "{rendered}");
        assert!(rendered.contains("forceNewCluster"), "{rendered}");

        // forceNewCluster re-stamps; the new topology then passes and the
        // OLD one is refused.
        check_topology_stamp(&store, "lanes", true)?;
        check_topology_stamp(&store, "lanes", false)?;
        assert!(check_topology_stamp(&store, "single", false).is_err());
        Ok(())
    }

    #[test]
    fn resolve_target_handles_all_spellings() {
        let cfg = RaftPersistenceConfigFull {
            peers: vec!["mgr-b=10.0.0.2:4646".to_string(), "mgr-c".to_string()],
            ..Default::default()
        };
        let driver = MultiRaftDriver::new("mgr-a".into(), "10.0.0.1:4646", &cfg);
        assert_eq!(
            driver.resolve_target("mgr-x@10.9.9.9:4646"),
            ("mgr-x".to_string(), Some("10.9.9.9:4646".to_string()))
        );
        assert_eq!(
            driver.resolve_target("mgr-b"),
            ("mgr-b".to_string(), Some("10.0.0.2:4646".to_string()))
        );
        assert_eq!(driver.resolve_target("mgr-c"), ("mgr-c".to_string(), None));
    }
}
