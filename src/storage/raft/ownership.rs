//! Single-writer table ownership for lane-partitioned raft replication
//! (plan §7-C2, JC-26..28 of
//! `docs/plans/durability-and-ops-execution-plan.md`).
//!
//! With row-changeset replication, two independent raft logs writing the
//! same table can apply in different orders on different replicas — silent
//! divergence or wedged conflicts (plan §7-C1). The fix is structural:
//! **every table has exactly one writing lane**, declared here and enforced
//! at capture time by [`verify_changeset_ownership`] — an op whose captured
//! changeset touches a table outside its lane fails BEFORE proposal, in
//! every topology, so a misclassification can never ship (JC-26).
//!
//! Beyond lane ownership there are three special classes:
//!
//! - [`Owner::Commutative`] (JC-28): insert-only tables with globally-unique
//!   PKs (`scheduler_event`, `idempotency_record`). Any lane may INSERT;
//!   applies resolve insert-exists / delete-missing conflicts with OMIT
//!   (insert-if-absent is order-independent), pruning is local per-node
//!   maintenance outside raft, and the tables are excluded from the replica
//!   content hash. **The "globally unique" premise is load-bearing and is
//!   not free** — see the audit in [`COMMUTATIVE_PK_AUDIT`] (JC-42).
//! - [`Owner::LocalOnly`]: node-local work state that never rides a
//!   changeset, written through the scoped `Store::local_write` seam only and
//!   filtered out of changeset application entirely. **No table is
//!   classified this way today** — JC-27 put the eval queue here and JC-41
//!   reversed that (acknowledged work is not regenerable), so the class is
//!   currently empty and pinned so by [`LOCAL_ONLY_AUDIT`]. The mechanism is
//!   deliberately retained: the `local_write` seam is still the only legal
//!   path for genuinely node-local maintenance (today, the JC-28 commutative
//!   pruning of `scheduler_event`), and a future node-local table would be
//!   classified here rather than inventing a second bypass.
//! - [`Owner::Internal`]: raft's own node-local bookkeeping
//!   (`raft_applied`, `raft_cluster`), written directly by the engine and
//!   never part of any changeset.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;

use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetItem, ChangesetIter, ConflictAction, ConflictType};

use crate::prelude::*;
use crate::storage::raft::Lane;

/// Whether the capture-time ownership guard REJECTS violating ops in the
/// `single` topology.
///
/// Phase C-2 split every composite op at its lane boundaries (wrapper-level
/// multi-op composites, eval-mediated reschedule tails, the rollout lane
/// consolidation, and the cross-lane FK removal), so this is now `true`:
/// a violating op fails before proposal in EVERY topology. Multi-group
/// (`lanes`) topologies always enforced regardless of this flag — in
/// `single` the violation would still be totally ordered, but rejecting it
/// everywhere is what keeps a misclassification from ever shipping and
/// then detonating on the first lanes-topology cluster (JC-26).
pub(crate) const OWNERSHIP_GUARD_ENFORCES: bool = true;

/// Audit of the [`Owner::Commutative`] class's load-bearing premise —
/// "insert-only tables with globally-unique PKs" (JC-28) — re-done when
/// JC-42 found the premise FALSE for `scheduler_event`. A commutative
/// table is only safe if **no two lanes can ever produce the same PK with
/// different row content**: the OMIT policy keeps whichever insert applied
/// first, and lanes apply in different orders on different replicas, so a
/// PK collision with differing content is silent divergence — invisible to
/// the replica content hash, which excludes these tables.
///
/// - **`scheduler_event`** — PK is MINTED by the writer, so it was the
///   violation: `MAX(id) + 1` computed inside the capture gave two lanes
///   capturing from the same base state the SAME id with different bodies.
///   Fixed by [`Lane::ID_STRIDE`]: a minted id always carries its lane's
///   residue (`id % ID_STRIDE == lane.id_residue()`), so the lanes' id
///   spaces are disjoint by construction. Any FUTURE commutative table with
///   a writer-minted key must do the same.
/// - **`idempotency_record`** — PK is the CALLER-SUPPLIED idempotency key
///   (`key TEXT PRIMARY KEY`), never derived from local state, so it is
///   genuinely globally unique and needs no stride. Two requests carrying
///   the same key are the deduplication case the table exists for: the
///   row content is a function of the key's operation, and both writers
///   go through the single `Store::idempotency_record` op, which proposes
///   on [`Lane::Control`] — one log, one total order, identical outcome on
///   every replica. A future op inserting the same key from a DIFFERENT
///   lane with a different `response_message` would reintroduce the
///   order-dependent hazard; the capture-time guard cannot catch that, so
///   it is called out here.
///
/// This constant exists so the audit has a name the map can point at; it is
/// the list of commutative tables in audit order.
#[cfg_attr(not(test), allow(dead_code))] // consumed by the coverage test below
pub(crate) const COMMUTATIVE_PK_AUDIT: &[&str] = &["scheduler_event", "idempotency_record"];

/// Audit of the [`Owner::LocalOnly`] class — tables deliberately EXEMPT from
/// replication (JC-41).
///
/// It is empty, and the coverage test below pins it that way: every table in
/// the schema is either owned by a lane, commutative, or raft-internal, so
/// nothing user-visible lives outside the replicated set. JC-27 had put
/// `eval` here on the premise that evals are regenerable work; the
/// adversarial pass disproved the premise (an acknowledged `job stop`'s
/// teardown, a mid-flight rollout tick, a config update's previous-version
/// cleanup and a KV-triggered restart each ride an eval whose loss is
/// permanent and user-visible, and the regeneration hook cannot reconstruct
/// operator intent it never recorded), so JC-41 moved `eval` back onto the
/// scheduler lane.
///
/// Adding an entry here is therefore a real durability decision, not a
/// classification detail: it must come with the argument for why losing
/// every row of that table on a leader failover is acceptable.
#[cfg_attr(not(test), allow(dead_code))] // consumed by the coverage test below
pub(crate) const LOCAL_ONLY_AUDIT: &[&str] = &[];

/// Who may write a table (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner {
    /// Exactly this lane's ops may write the table.
    Lane(Lane),
    /// Any lane may INSERT (never UPDATE/DELETE inside a changeset);
    /// applied with per-table OMIT conflict policy; pruned locally (JC-28).
    Commutative,
    /// Node-local, never replicated; written only through
    /// `Store::local_write`. Currently unused — see [`LOCAL_ONLY_AUDIT`]
    /// (JC-41 emptied the class; the mechanism is kept for genuinely
    /// node-local maintenance and future node-local tables).
    #[allow(dead_code)] // no table is classified LocalOnly today (LOCAL_ONLY_AUDIT)
    LocalOnly,
    /// Raft engine bookkeeping, node-local, never inside a changeset.
    Internal,
}

/// The single-writer ownership map: every table `src/storage/migrations.rs`
/// creates, exactly once. The coverage test below walks `sqlite_master` of a
/// fresh store and fails if a table is missing here (or listed here without
/// existing / being declared pending) — adding a table without classifying
/// it does not compile^W pass CI.
///
/// Lane assignments follow plan §7-C2 and the existing `Store::replicated`
/// lane classification (tests/integration/raft_seam.rs).
const TABLE_OWNERS: &[(&str, Owner)] = &[
    // ── control lane: operator/user intent ───────────────────────────────
    ("job", Owner::Lane(Lane::Control)),
    ("job_config", Owner::Lane(Lane::Control)),
    ("submission", Owner::Lane(Lane::Control)),
    ("job_config_submission", Owner::Lane(Lane::Control)),
    ("task_config", Owner::Lane(Lane::Control)),
    ("job_update", Owner::Lane(Lane::Control)),
    ("manager_config", Owner::Lane(Lane::Control)),
    ("kv_entry", Owner::Lane(Lane::Control)),
    ("kv_lock", Owner::Lane(Lane::Control)),
    ("job_kv_reference", Owner::Lane(Lane::Control)),
    // Job grants (apps plan §6.4): written by the submit entry that stores
    // the version they grant, and by revocation.
    ("job_grant", Owner::Lane(Lane::Control)),
    // App records (apps plan §6.3): operator intent, written by the app
    // RPCs and the app reconciler, next to the jobs it submits.
    ("app", Owner::Lane(Lane::Control)),
    ("saved_state_query", Owner::Lane(Lane::Control)),
    ("signing_key", Owner::Lane(Lane::Control)),
    // KV encryption keyring (docs/kv-encryption.md): the wrapped data keys.
    // Control lane with `kv_entry`, so a rewrap batch (value rows + key
    // state) is one changeset.
    ("keyring_key", Owner::Lane(Lane::Control)),
    // Job templates are operator intent, like the job specs they render
    // into: written only by UpsertJobTemplate/DeleteJobTemplate, and read
    // (never written) on the submit path.
    ("job_template", Owner::Lane(Lane::Control)),
    ("job_template_version", Owner::Lane(Lane::Control)),
    // `node` is identity/pool/attrs/availability — control-class writes
    // (registration, cordon). Its liveness columns moved to `node_health`
    // in phase C-1; phase C-2 split the status ops' former inline
    // `availability` flips into the control-lane `sync_node_availability`
    // op, so the table is single-written by this lane.
    ("node", Owner::Lane(Lane::Control)),
    ("worker_type", Owner::Lane(Lane::Control)),
    ("acl_policy", Owner::Lane(Lane::Control)),
    // Network policy objects (docs/network-handlers.md): applied and deleted
    // through the control lane like ACL policies; the generation counter is
    // bumped inside the same replicated write.
    ("net_policy", Owner::Lane(Lane::Control)),
    ("net_policy_meta", Owner::Lane(Lane::Control)),
    // Network plugin objects (docs/network-plugins.md): like policy objects.
    ("net_plugin", Owner::Lane(Lane::Control)),
    ("net_plugin_meta", Owner::Lane(Lane::Control)),
    // Namespaces (docs/namespaces.md): operator intent, written by
    // ApplyNamespace/DeleteNamespace. Control lane with `job`, so the
    // "delete only if empty" check and the delete are one entry.
    ("namespace", Owner::Lane(Lane::Control)),
    // Namespace policies: cluster governance objects, read by admission
    // inside the control-lane submit and namespace-create entries.
    ("namespace_policy", Owner::Lane(Lane::Control)),
    ("net_mesh_ca", Owner::Lane(Lane::Control)),
    // The leader's view of the cluster CA (docs/cluster-ca.md): written
    // when the control-lane leader's provider reports a new CA.
    ("cluster_ca_record", Owner::Lane(Lane::Control)),
    ("acl_token", Owner::Lane(Lane::Control)),
    ("acl_user", Owner::Lane(Lane::Control)),
    ("join_token", Owner::Lane(Lane::Control)),
    ("exec_key", Owner::Lane(Lane::Control)),
    // Boot tickets are capabilities an operator (or a platform's minter)
    // hands out, like join tokens — but they are SPENT by ratification,
    // which is a scheduler-lane plan apply. They live on the scheduler lane
    // so the spend rides the same entry as the placement it authorizes
    // (sandbox-scaling plan, Phase 1): a refused ratification no longer
    // burns a single-use ticket, and an edge start is one entry, not two.
    // `spend_boot_ticket` is still a conditional UPDATE on one lane, which
    // is what keeps single-use enforcement globally serialized rather than
    // per-manager.
    ("boot_ticket", Owner::Lane(Lane::Scheduler)),
    // Count overrides (`legion job scale`) are operator/autoscaler INTENT,
    // like the job specs they layer over — and their lifecycle is coupled
    // to the submit path: `handle_job_submit` arbitrates override
    // ownership (`base_count`) inside the same control-lane op that writes
    // the new config, which a scheduler-owned table would forbid (the plan
    // leaned scheduler-lane; see the autoscaler calls doc for the
    // deviation). The scheduler only READS effective counts; the
    // reconciling `job_scale` eval is its own sequential scheduler-lane
    // enqueue, exactly the JC-33 wrapper-sequential shape stop_job uses.
    ("task_scale", Owner::Lane(Lane::Control)),
    // Task autoscaling's state rides Control beside `task_scale` (plan
    // non-goal: no new lane): every autoscaler write is a `task_scale`
    // write plus its history row in one entry, and the defaults and pauses
    // are operator intent read by the same submit and scale ops.
    ("scaling_counter", Owner::Lane(Lane::Control)),
    ("scaling_defaults", Owner::Lane(Lane::Control)),
    ("scale_history", Owner::Lane(Lane::Control)),
    ("scaling_pause", Owner::Lane(Lane::Control)),
    ("scaling_policy_snapshot", Owner::Lane(Lane::Control)),
    ("scaling_policy", Owner::Lane(Lane::Control)),
    // ── scheduler lane: placement + rollout progression ──────────────────
    ("allocation", Owner::Lane(Lane::Scheduler)),
    ("allocation_epoch", Owner::Lane(Lane::Scheduler)),
    ("allocation_epoch_partition", Owner::Lane(Lane::Scheduler)),
    ("plan_submission", Owner::Lane(Lane::Scheduler)),
    // `allocation_stop_notice` records WHY an allocation was removed, for the
    // owning worker's next heartbeat. Every writer is an allocation-removal
    // op, and all eight of them — `remove_allocations`,
    // `remove_allocation_by_id`, `remove_allocs_for_node`,
    // `remove_allocs_for_job`, `remove_allocs_for_job_config`,
    // `destroy_job_scheduler_cleanup`, and the keyed-instance stop/destroy
    // pair — propose on the scheduler lane, writing the notice in the same
    // transaction as the DELETE it describes. Expiry rides those same
    // transactions, so nothing outside this lane ever writes the table. NOT
    // commutative: the insert is an upsert (`INSERT OR REPLACE`), which the
    // commutative class forbids.
    ("allocation_stop_notice", Owner::Lane(Lane::Scheduler)),
    // `allocation_history` remembers removed allocations (job, task, node)
    // for `legion job logs`. Same writers as `allocation_stop_notice`: every
    // removal op inserts in the transaction that deletes the row and expires
    // old rows there too. NOT commutative (`INSERT OR REPLACE`).
    ("allocation_history", Owner::Lane(Lane::Scheduler)),
    // `reschedule_hold` records a failed allocation's deferred replacement
    // and doubles as the task's move history. Written by the
    // failed-allocation removal op (`remove_failed_allocation`) in the
    // transaction that deletes the allocation, read by the planner, cleared
    // by `job_update` evals and StartInstance, and deleted by job destroy's
    // scheduler cleanup: all scheduler-lane ops. NOT commutative (rows are
    // updated and deleted).
    ("reschedule_hold", Owner::Lane(Lane::Scheduler)),
    // `gang_wait` records when a gang started waiting for its quorum and
    // its placement deadline (docs/gang-scheduling.md). Written by the eval
    // completion op that blocks the gang's eval (`write_blocked_eval`),
    // deleted by the scheduler after a plan reaches the quorum, by the
    // `job_stop`/`job_update` evals, by the leader's timeout pass, and by job
    // destroy's scheduler cleanup: all scheduler-lane ops. NOT commutative
    // (rows are deleted).
    ("gang_wait", Owner::Lane(Lane::Scheduler)),
    // `gang_member` holds each gang allocation's rank, size, and leader
    // address. Written in the same transaction as the allocation row (the
    // plan apply and every other placement path), pruned there and by job
    // destroy's scheduler cleanup: all scheduler-lane ops, like `allocation`.
    // NOT commutative (ranks are assigned from the live set).
    ("gang_member", Owner::Lane(Lane::Scheduler)),
    // `node_alloc_usage` caches the aggregated allocation usage of a node
    // for the plan-apply capacity recheck, next to a fingerprint of the rows
    // it was derived from (see the migration that creates it). Written by
    // exactly one op — `apply_plan_submission`, at the end of a plan it
    // applied — which is a scheduler-lane op writing it in the same capture
    // as the placements it describes. Derived state, so a replica needs no
    // special handling: it receives the leader's rows like any other, and a
    // new leader's first plan revalidates them against the live allocation
    // rows before trusting them.
    ("node_alloc_usage", Owner::Lane(Lane::Scheduler)),
    ("instance", Owner::Lane(Lane::Scheduler)),
    // `volume` rides the scheduler lane: a volume's lifecycle is bound to its
    // instance (`instance` is already scheduler-owned, and `place_alloc`
    // mints the instance's volume row inside the very same capture as the
    // placement), and its state machine (pending -> created -> destroying ->
    // destroyed) is driven by placement and the reclaim reaper — both
    // scheduler-lane concerns.
    //
    // Writers, all scheduler-lane ops: `place_alloc`
    // (`mint_instance_volume_tx`), `sync_task_volume_claims` (the submit
    // composite's scheduler-lane tail — see `Store::handle_job_submit`),
    // `record_volume_mounted` / `record_volume_error` /
    // `mark_volume_destroyed` (worker volume reports),
    // `reap_orphaned_volumes` (the daemon's reclaim reaper) and
    // `delete_volume` (operator reclaim, which also detaches
    // `instance.storage_handle` — same lane, so the detach and the state flip
    // stay one atomic op).
    ("volume", Owner::Lane(Lane::Scheduler)),
    // `snapshot` and `snapshot_alias` ride the scheduler lane with
    // `instance`: RestoreInstance writes `instance.restore_from` and reads the
    // snapshot in one op, placement pins a restoring instance to its
    // snapshot's node, and the worker reports that advance a snapshot's state
    // machine (capture, delete, restore outcomes) are scheduler-lane ops like
    // the volume reports beside them. Writers: `create_snapshot`,
    // `apply_snapshot_report`, `delete_snapshot`, `expire_snapshots`,
    // `restore_keyed_instance`, and job destroy's scheduler cleanup.
    ("snapshot", Owner::Lane(Lane::Scheduler)),
    ("snapshot_alias", Owner::Lane(Lane::Scheduler)),
    // The eval queue is REPLICATED, on the lane that consumes it (JC-41,
    // reversing JC-27). Evals are not regenerable work: an acknowledged
    // `job stop`'s teardown tail, a mid-flight rollout tick, a config
    // update's previous-version cleanup and a KV-triggered restart each ride
    // an eval whose loss is permanent and user-visible. The scheduler is the
    // single writer — enqueue/claim/complete/requeue/prune are all scheduler
    // ops — so an op on another lane that needs an eval enqueues it as its
    // OWN sequential scheduler-lane op after its own op commits (JC-33's
    // wrapper-sequential shape); enqueueing INSIDE a foreign lane's capture
    // is exactly what the guard below rejects.
    ("eval", Owner::Lane(Lane::Scheduler)),
    // C-2 consolidated ALL rollout mutations onto the scheduler lane:
    // `create_rollout`, `update_rollout_status`,
    // `update_rollout_config_json`, `prune_rollouts`,
    // `reconcile_incomplete_rollouts`, and destroy_job's scheduler cleanup
    // all propose there — rollout_tick evals drive progression, so the
    // scheduler is the natural single writer.
    ("rollout", Owner::Lane(Lane::Scheduler)),
    // `rollout_event` rides the rollout's lane (C-2): rows are written only
    // by `append_rollout_event` (rollout progression context) and deleted
    // by `prune_rollouts` retention + destroy_job's scheduler cleanup —
    // one writing lane, so no commutative special-case is needed.
    ("rollout_event", Owner::Lane(Lane::Scheduler)),
    // ── status lane: worker-reported state + event mirrors ───────────────
    // `task_status` is seeded by the submit composite's status-lane half
    // (`seed_task_statuses`), transitioned by placement follow-ups
    // (`update_task_statuses_for_allocations`) and worker reports
    // (`set_task_status`), and cleared by destroy_job's status cleanup —
    // all Status-lane ops since C-2 (the FK cascades from control tables
    // are gone).
    ("task_status", Owner::Lane(Lane::Status)),
    // `allocation_health` lost its FK cascades in C-2: rows are upserted by
    // heartbeats and deleted by destroy_job's status cleanup plus the
    // `prune_allocation_health_orphans` sweep — all Status-lane ops.
    ("allocation_health", Owner::Lane(Lane::Status)),
    // `node_health` (phase C-1): the liveness half of the node split —
    // status/heartbeat/lost markers/health checks, written only by the
    // status lane (heartbeats, health checks, registration's status-lane
    // seed op, lost marks, and the orphan sweep).
    ("node_health", Owner::Lane(Lane::Status)),
    // ── commutative class (JC-28) ────────────────────────────────────────
    ("scheduler_event", Owner::Commutative),
    ("idempotency_record", Owner::Commutative),
    // ── local-only: EMPTY (JC-41 moved `eval` back onto the scheduler
    //    lane; see LOCAL_ONLY_AUDIT) ───────────────────────────────────────
    // ── raft-internal bookkeeping ────────────────────────────────────────
    ("raft_applied", Owner::Internal),
    ("raft_cluster", Owner::Internal),
];

/// Tables classified above that do not exist in the schema YET. Tolerated by
/// the coverage test until their creating phase lands, at which point the
/// test forces this list back to empty (exactness).
#[cfg_attr(not(test), allow(dead_code))] // consumed by the coverage test below
const PENDING_TABLES: &[&str] = &[
    // Empty since phase C-1 landed `node_health`: the map is exact — every
    // classified table exists and every existing table is classified.
];

/// The owning writer of `table`, or `None` for a table this map has never
/// heard of — which the guard treats as a violation (fail loudly, JC-26)
/// and the coverage test treats as an unclassified schema addition.
pub(crate) fn owner_lane(table: &str) -> Option<Owner> {
    TABLE_OWNERS
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, owner)| *owner)
}

/// The tables a LANE's table-scoped snapshot dumps and installs (plan
/// §7-C2 "per-lane snapshots become trivially sound"):
///
/// - every table the lane owns ([`Owner::Lane`] — single-writer, so the
///   lane's own snapshot + log fully reconstruct them);
/// - the [`Owner::Commutative`] tables ride the CONTROL lane's snapshot
///   (JC-28): they are written from every lane, but insert-if-absent
///   applies with OMIT, so other lanes' inserts replay harmlessly on top
///   of whatever window the control snapshot carried. Because sibling
///   lanes' ALREADY-APPLIED inserts never replay (they sit at or below
///   those lanes' applied markers), install lays the dump's commutative
///   rows on top with `INSERT OR IGNORE` and deletes nothing — only
///   lane-owned tables are delete-and-replaced (see
///   `state_machine::install_lane_snapshot`).
///
/// LocalOnly (none today) and Internal (`raft_applied`, `raft_cluster`)
/// tables are node-local and never travel in any snapshot (the lane's
/// applied marker is carried in the snapshot *metadata* and written by
/// install). `eval` is scheduler-owned since JC-41, so it rides the
/// SCHEDULER lane's snapshot like every other queue-shaped table — a
/// follower caught up by install therefore holds the pending queue.
pub(crate) fn snapshot_tables_for_lane(lane: Lane) -> Vec<&'static str> {
    TABLE_OWNERS
        .iter()
        .filter(|(_, owner)| match owner {
            Owner::Lane(owning) => *owning == lane,
            Owner::Commutative => lane == Lane::Control,
            Owner::LocalOnly | Owner::Internal => false,
        })
        .map(|(name, _)| *name)
        .collect()
}

/// Whether a table's rows may be applied from a replicated changeset at all.
/// LocalOnly (none today, see [`LOCAL_ONLY_AUDIT`]) and Internal tables are
/// filtered out of application entirely: their rows are node-local, so a
/// changeset that somehow carried them must not create/delete local rows on
/// replicas. `eval` is NOT filtered since JC-41 — the queue is replicated
/// scheduler-lane state and must apply on every replica.
pub(crate) fn table_applies_from_changeset(table: &str) -> bool {
    !matches!(
        owner_lane(table),
        Some(Owner::LocalOnly) | Some(Owner::Internal)
    )
}

/// Per-table conflict policy for changeset application (JC-28).
///
/// Commutative tables resolve the two order-dependent-looking-but-benign
/// conflicts with OMIT: INSERT of an already-present row
/// (`SQLITE_CHANGESET_CONFLICT`) and DELETE of an already-absent row
/// (`SQLITE_CHANGESET_NOTFOUND`) — insert-if-absent / delete-if-present is
/// order-independent. Everything else stays ABORT: on any other table a
/// conflict means the replica diverged from the leader, which changeset
/// replication makes impossible by construction, so "resolving" it would
/// paper over corruption.
pub(crate) fn resolve_apply_conflict(
    conflict_type: ConflictType,
    item: &ChangesetItem,
) -> ConflictAction {
    let (table, op_code) = match item.op() {
        Ok(op) => (op.table_name().to_string(), op.code()),
        Err(err) => {
            error!(error = %err, "changeset conflict handler failed reading the conflicting op");
            return ConflictAction::SQLITE_CHANGESET_ABORT;
        }
    };
    if owner_lane(&table) == Some(Owner::Commutative) {
        match (&conflict_type, op_code) {
            (ConflictType::SQLITE_CHANGESET_CONFLICT, Action::SQLITE_INSERT) => {
                debug!(
                    table,
                    "commutative insert already present at apply; omitting (order-independent)"
                );
                return ConflictAction::SQLITE_CHANGESET_OMIT;
            }
            (ConflictType::SQLITE_CHANGESET_NOTFOUND, Action::SQLITE_DELETE) => {
                debug!(
                    table,
                    "commutative delete already absent at apply (locally pruned); omitting"
                );
                return ConflictAction::SQLITE_CHANGESET_OMIT;
            }
            _ => {}
        }
    }
    error!(
        ?conflict_type,
        table, "changeset apply conflict: replica state diverged from leader"
    );
    ConflictAction::SQLITE_CHANGESET_ABORT
}

/// One capture-time ownership violation (a `(table, action)` pair outside
/// the op's lane).
fn describe_violation(table: &str, action: Action, lane: Lane) -> Option<String> {
    let action_name = match action {
        Action::SQLITE_INSERT => "INSERT",
        Action::SQLITE_UPDATE => "UPDATE",
        Action::SQLITE_DELETE => "DELETE",
        _ => "UNKNOWN",
    };
    match owner_lane(table) {
        Some(Owner::Lane(owner)) if owner == lane => None,
        Some(Owner::Lane(owner)) => Some(format!(
            "{action_name} on `{table}` (owned by the {} lane)",
            owner.wire_name()
        )),
        Some(Owner::Commutative) if action == Action::SQLITE_INSERT => None,
        Some(Owner::Commutative) => Some(format!(
            "{action_name} on commutative table `{table}` (only INSERT commutes; \
             UPDATE/DELETE must be local maintenance)"
        )),
        // Unreachable today (LOCAL_ONLY_AUDIT is empty), retained with the
        // class itself so a future node-local table is guarded from day one.
        Some(Owner::LocalOnly) => Some(format!(
            "{action_name} on local-only table `{table}` (never replicated; use \
             Store::local_write)"
        )),
        Some(Owner::Internal) => Some(format!(
            "{action_name} on raft-internal table `{table}` (engine bookkeeping, never \
             inside a changeset)"
        )),
        None => Some(format!(
            "{action_name} on unclassified table `{table}` (add it to TABLE_OWNERS in \
             src/storage/raft/ownership.rs)"
        )),
    }
}

/// Ops that already emitted their single-topology grace warning, so the log
/// is not flooded by hot-path composites (rate limit: once per op name per
/// process).
static WARNED_OPS: StdMutex<BTreeSet<&'static str>> = StdMutex::new(BTreeSet::new());

/// Process-global count of capture-time ownership-guard violations
/// ([`verify_changeset_ownership`] finding a non-empty violation set),
/// counted whether the violation was rejected (enforcing) or warned away
/// (the dormant grace path). Test hook (`Store::raft_ownership_guard_
/// violations`): the lanes durability battery asserts its workloads drive
/// this to exactly zero — a green run must mean "no op ever tripped the
/// guard", not "trips were retried into silence".
static VIOLATION_COUNT: AtomicU64 = AtomicU64::new(0);

/// Read the process-global violation counter (see [`VIOLATION_COUNT`]).
pub(crate) fn violation_count() -> u64 {
    VIOLATION_COUNT.load(Ordering::SeqCst)
}

/// The capture-time ownership guard (JC-26): verify that every table touched
/// by `changeset` is writable by an op on `lane`, per the map above.
///
/// Violations FAIL the op before proposal, in every topology since phase
/// C-2 flipped [`OWNERSHIP_GUARD_ENFORCES`]. (The warn-once grace path
/// below is retained, dormant, as the documented shape of any future
/// migration window: setting the flag back to `false` re-opens grace in
/// `single` topology only — `lanes` always enforces.)
pub(crate) fn verify_changeset_ownership(
    op: &'static str,
    lane: Lane,
    single_topology: bool,
    changeset: &[u8],
) -> R<()> {
    if changeset.is_empty() {
        return Ok(());
    }
    let mut violations: BTreeSet<String> = BTreeSet::new();
    let reader: &mut dyn std::io::Read = &mut &*changeset;
    let mut iter = ChangesetIter::start_strm(&reader)
        .context("failed opening changeset iterator for the ownership guard")?;
    while let Some(item) = iter
        .next()
        .context("failed iterating changeset for the ownership guard")?
    {
        let item_op = item.op()?;
        if let Some(violation) = describe_violation(item_op.table_name(), item_op.code(), lane) {
            violations.insert(violation);
        }
    }
    if violations.is_empty() {
        return Ok(());
    }
    VIOLATION_COUNT.fetch_add(1, Ordering::SeqCst);
    let detail = violations.into_iter().collect::<Vec<_>>().join("; ");
    let enforce = OWNERSHIP_GUARD_ENFORCES || !single_topology;
    if enforce {
        bail!(
            "single-writer ownership violation in op `{op}` (proposed on the {} lane): \
             {detail}. Every table has exactly one writing lane — see plan §7-C2 (JC-26) in \
             docs/plans/durability-and-ops-execution-plan.md; the op was rejected before \
             proposal",
            lane.wire_name()
        );
    }
    let first_warning = WARNED_OPS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(op);
    if first_warning {
        warn!(
            op,
            lane = lane.wire_name(),
            %detail,
            "single-writer ownership violation (tolerated in `single` topology until phase \
             C-2 flips OWNERSHIP_GUARD_ENFORCES; see plan §7-C2 / JC-26). This op must stop \
             writing foreign-lane tables before `lanes` topology can be enabled"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::raft::capture;
    use crate::storage::store::Store;
    use crate::structs::{KvValueType, ManagerConfigFull};

    fn setup_store() -> R<Store> {
        let dir = tempfile::Builder::new()
            .prefix("khepri-raft-ownership-")
            .tempdir_in(crate::test_scratch::root())
            .context("Error creating temp store directory")?
            .keep();
        let config = ManagerConfigFull {
            store_path: dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        Store::start_storage(&config)
    }

    /// Exhaustive coverage: every table a fresh store's schema contains is
    /// classified, and every classified table either exists or is declared
    /// pending. Adding a table to migrations.rs without classifying it here
    /// fails this test; landing a pending table without emptying
    /// `PENDING_TABLES` fails it too (the map must become exact).
    #[test]
    fn ownership_map_covers_every_migrated_table_exactly() -> R<()> {
        let store = setup_store()?;
        let conn = store.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
             ORDER BY name;",
        )?;
        let tables: Vec<String> =
            Store::collect_rows(stmt.query_map([], |row| row.get::<_, String>(0))?)?;
        assert!(
            tables.len() > 20,
            "schema walk looks broken: only {} tables",
            tables.len()
        );

        let unclassified: Vec<&String> = tables
            .iter()
            .filter(|table| owner_lane(table).is_none())
            .collect();
        assert!(
            unclassified.is_empty(),
            "tables missing from the single-writer ownership map (classify them in \
             src/storage/raft/ownership.rs TABLE_OWNERS): {unclassified:?}"
        );

        let existing: std::collections::BTreeSet<&str> =
            tables.iter().map(String::as_str).collect();
        for (name, _) in TABLE_OWNERS {
            if PENDING_TABLES.contains(name) {
                assert!(
                    !existing.contains(name),
                    "table `{name}` is declared pending but exists in the schema now — its \
                     creating phase landed; remove it from PENDING_TABLES so the map is exact"
                );
            } else {
                assert!(
                    existing.contains(name),
                    "table `{name}` is classified in TABLE_OWNERS but does not exist in the \
                     schema (renamed or dropped?)"
                );
            }
        }
        // No duplicate map entries hiding a conflicting classification.
        let mut seen = std::collections::BTreeSet::new();
        for (name, _) in TABLE_OWNERS {
            assert!(seen.insert(*name), "duplicate TABLE_OWNERS entry: {name}");
        }

        // Exactness of the replication EXEMPTION (JC-41): the local-only
        // class must match its audit list — which is empty, so every table
        // in the schema is replicated (lane-owned or commutative) or raft
        // bookkeeping. `eval` in particular is scheduler-lane state again:
        // it rides scheduler snapshots, applies on every replica, and is
        // part of the replica content hash.
        let local_only: BTreeSet<&str> = TABLE_OWNERS
            .iter()
            .filter(|(_, owner)| *owner == Owner::LocalOnly)
            .map(|(name, _)| *name)
            .collect();
        let audited: BTreeSet<&str> = LOCAL_ONLY_AUDIT.iter().copied().collect();
        assert_eq!(
            local_only, audited,
            "every LocalOnly table must appear in LOCAL_ONLY_AUDIT with the argument for why \
             losing its rows on a leader failover is acceptable (JC-41)"
        );
        assert_eq!(
            owner_lane("eval"),
            Some(Owner::Lane(Lane::Scheduler)),
            "the eval queue is replicated on the scheduler lane (JC-41 reverses JC-27)"
        );
        Ok(())
    }

    /// Per-lane snapshot table sets (plan §7-C2): pairwise disjoint, and
    /// their union is exactly the replicated tables — every Lane-owned and
    /// Commutative table is snapshotted by exactly one lane, and no
    /// LocalOnly/Internal table is snapshotted by any lane. A table that
    /// fell out of every lane's snapshot would silently never reach
    /// laggards catching up by install under `lanes` topology.
    #[test]
    fn lane_snapshot_table_sets_partition_the_replicated_tables() {
        let mut seen: std::collections::BTreeMap<&str, Lane> = std::collections::BTreeMap::new();
        for lane in Lane::ALL {
            for table in snapshot_tables_for_lane(lane) {
                if let Some(previous) = seen.insert(table, lane) {
                    panic!(
                        "table `{table}` is snapshotted by both {} and {} lanes",
                        previous.wire_name(),
                        lane.wire_name()
                    );
                }
            }
        }
        for (name, owner) in TABLE_OWNERS {
            match owner {
                Owner::Lane(_) | Owner::Commutative => assert!(
                    seen.contains_key(name),
                    "replicated table `{name}` is in no lane's snapshot set"
                ),
                Owner::LocalOnly | Owner::Internal => assert!(
                    !seen.contains_key(name),
                    "node-local table `{name}` must not travel in any lane snapshot"
                ),
            }
        }
        // The commutative tables ride the control lane's snapshot (JC-28).
        assert_eq!(seen.get("scheduler_event"), Some(&Lane::Control));
        assert_eq!(seen.get("idempotency_record"), Some(&Lane::Control));
        // The eval queue rides the scheduler lane's snapshot (JC-41): a
        // follower caught up by install must hold the pending queue, or a
        // failover loses acknowledged work exactly as JC-27 did.
        assert_eq!(seen.get("eval"), Some(&Lane::Scheduler));
    }

    /// JC-42: the lane id residues are distinct and inside the stride, and
    /// the stride is a power of two matching `ID_STRIDE_BITS` with headroom
    /// above the lane count. These are the premises that make minted
    /// commutative ids provably collision-free across lanes; a fourth lane
    /// added without a residue (or with a duplicate one) fails here.
    #[test]
    fn lane_id_residues_are_distinct_and_inside_the_stride() {
        assert_eq!(
            Lane::ID_STRIDE,
            1 << Lane::ID_STRIDE_BITS,
            "ID_STRIDE_BITS must be log2(ID_STRIDE) — minters build ids by shifting"
        );
        assert!(
            (Lane::ALL.len() as i64) <= Lane::ID_STRIDE,
            "every lane needs its own residue class"
        );
        let mut seen: BTreeSet<i64> = BTreeSet::new();
        for lane in Lane::ALL {
            let residue = lane.id_residue();
            assert!(
                (0..Lane::ID_STRIDE).contains(&residue),
                "{} lane's residue {residue} is outside the stride",
                lane.wire_name()
            );
            assert!(
                seen.insert(residue),
                "duplicate id residue {residue} on the {} lane",
                lane.wire_name()
            );
        }
        // Outside a capture the control residue is used, so `local_disk`
        // ids keep the control lane's class (see `capture::current_capture_lane`).
        assert_eq!(Lane::Control.id_residue(), 0);
    }

    /// The commutative-PK audit (JC-42) covers exactly the commutative
    /// tables: classifying a new table `Commutative` without auditing how
    /// its primary key stays globally unique across lanes fails here.
    #[test]
    fn commutative_pk_audit_covers_every_commutative_table() {
        let classified: BTreeSet<&str> = TABLE_OWNERS
            .iter()
            .filter(|(_, owner)| *owner == Owner::Commutative)
            .map(|(name, _)| *name)
            .collect();
        let audited: BTreeSet<&str> = COMMUTATIVE_PK_AUDIT.iter().copied().collect();
        assert_eq!(
            classified, audited,
            "every Commutative table must appear in COMMUTATIVE_PK_AUDIT with the argument \
             for why its PK is globally unique across lanes (JC-42)"
        );
    }

    /// Guard accepts an op whose changeset stays inside its own lane, and a
    /// commutative INSERT from any lane.
    #[test]
    fn guard_accepts_owned_tables_and_commutative_inserts() -> R<()> {
        let store = setup_store()?;
        // put_kv writes kv_entry (control) and stages a scheduler_event
        // INSERT (commutative) — both fine on the control lane.
        let ((), changeset) = capture::capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("own/key", "v", KvValueType::String, None, None)?;
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
                 VALUES (42, 'test.kind', 'info', 'm', '{}', '2026-01-01T00:00:00Z');",
                [],
            )?;
            Ok(())
        })?;
        verify_changeset_ownership("guard_ok", Lane::Control, false, &changeset)?;
        Ok(())
    }

    /// Foreign-lane writes are rejected in EVERY topology since C-2 flipped
    /// the enforcement flag: lanes because divergence is real, single so a
    /// misclassification can never ship.
    #[test]
    fn guard_rejects_foreign_lane_writes_when_enforcing() -> R<()> {
        let store = setup_store()?;
        // A raw INSERT into `plan_submission` (scheduler-owned, no FKs)
        // from a control-lane op.
        let ((), changeset) = capture::capture_mutation(&store, Lane::Control, |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO plan_submission (job_config_id, mode, placements, expected_epoch,
                                              status, created_at)
                 VALUES ('jc-1', 'fresh', '[]', 0, 'pending', '2026-01-01T00:00:00Z');",
                [],
            )?;
            Ok(())
        })?;

        let err = verify_changeset_ownership("guard_foreign", Lane::Control, false, &changeset)
            .expect_err("cross-lane write must be rejected when enforcing");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("guard_foreign"), "{rendered}");
        assert!(rendered.contains("control"), "{rendered}");
        assert!(rendered.contains("plan_submission"), "{rendered}");
        assert!(rendered.contains("scheduler"), "{rendered}");
        assert!(rendered.contains("§7-C2"), "{rendered}");

        // Single topology enforces too (C-2 flipped the flag): the same
        // violation is rejected, not warned away.
        const {
            assert!(
                OWNERSHIP_GUARD_ENFORCES,
                "phase C-2 flipped enforcement on everywhere; it must stay on"
            )
        };
        let err = verify_changeset_ownership("guard_foreign", Lane::Control, true, &changeset)
            .expect_err("single topology must reject cross-lane writes since C-2");
        assert!(format!("{err:#}").contains("plan_submission"), "{err:#}");
        Ok(())
    }

    /// Commutative tables commute for INSERT only: UPDATE/DELETE inside a
    /// changeset are violations (they are local maintenance, JC-28). And
    /// since JC-41 the eval queue is ordinary scheduler-lane state: an eval
    /// write is fine in a SCHEDULER capture and a violation in any other
    /// lane's — which is what forces cross-lane enqueues to be their own
    /// sequential scheduler op instead of riding a foreign changeset.
    #[test]
    fn guard_rejects_commutative_mutations_and_foreign_lane_eval_writes() -> R<()> {
        let store = setup_store()?;
        store.emit_event(crate::storage::events::EventDraft::info(
            "test.kind",
            "seeded",
        ));

        // DELETE on a commutative table inside a capture.
        let ((), delete_changeset) = capture::capture_mutation(&store, Lane::Control, |s| {
            let conn = s.write_connection()?;
            conn.execute("DELETE FROM scheduler_event;", [])?;
            Ok(())
        })?;
        let err =
            verify_changeset_ownership("guard_prune", Lane::Control, false, &delete_changeset)
                .expect_err("commutative DELETE must be rejected");
        assert!(
            format!("{err:#}").contains("only INSERT commutes"),
            "{err:#}"
        );

        // An eval write inside a capture: legal on the owning lane...
        let ((), eval_changeset) = capture::capture_mutation(&store, Lane::Scheduler, |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO eval (status, type, priority, ready_at)
                 VALUES ('pending', 'job_submit', 50, '2026-01-01T00:00:00Z');",
                [],
            )?;
            Ok(())
        })?;
        verify_changeset_ownership("guard_eval_own", Lane::Scheduler, false, &eval_changeset)?;

        // ...and rejected from any other lane's capture, which is what stops
        // a control/status op from enqueueing an eval INSIDE its own
        // changeset instead of as a sequential scheduler op (JC-41 × JC-33).
        let err =
            verify_changeset_ownership("guard_eval_foreign", Lane::Control, false, &eval_changeset)
                .expect_err("a control-lane op must not write the scheduler-owned eval queue");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("eval"), "{rendered}");
        assert!(rendered.contains("scheduler"), "{rendered}");
        Ok(())
    }
}
