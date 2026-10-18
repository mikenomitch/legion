//! Lane-write forwarding (plan §8, D2 — standing-split resilience).
//!
//! A manager that receives a write it cannot propose — it does not lead the
//! owning lane — forwards the *operation* (name + full arguments) to that
//! lane's leader over the `RaftTransport.ForwardLaneWrite` RPC, instead of
//! failing the caller. This exists for writes whose loss cascades and whose
//! callers cannot chase per-lane leaders:
//!
//! - **worker-plane liveness** (heartbeats, health checks, allocation-health
//!   snapshots): workers dial ONE fixed manager forever; a rejection storm
//!   marks healthy nodes lost within seconds and self-fences their stateful
//!   allocations. A heartbeat is *new information* — no reconciler can
//!   re-derive it — so it must reach the leader, not wait for one;
//! - **worker failure reports** (`remove_allocation_by_id`,
//!   `mark_job_failed`, the reschedule eval): swallowed rejections silently
//!   destroyed the report while the worker believed it delivered;
//! - **the status checker's cross-lane halves** (`sync_node_availability`):
//!   a lost node otherwise stays schedulable while its rows are gone.
//!
//! Design rules, in order of importance:
//!
//! 1. **Forwarded ops carry their full arguments.** The executor never
//!    derives payload from its own replica, which may lag the origin's —
//!    the only reads inside these ops are their pre-existing EXISTS guards,
//!    whose miss is repaired by the next report (the pre-split contract).
//! 2. **The executor runs the NON-forwarding variant.** If the target lost
//!    leadership between the origin's hint and the call, the typed
//!    not-the-leader outcome rides back to the origin — one bounded hop,
//!    never a forwarding loop.
//! 3. **Failure surfaces the ORIGINAL typed rejection.** A forward that
//!    cannot complete (no leader known, unreachable, timeout) returns the
//!    origin's own `NotRaftLeader`, so callers and their tolerance logic
//!    see exactly the error surface they saw before forwarding existed.
//!
//! Client-plane RPCs deliberately do NOT forward (plan §8, D3): clients
//! follow the typed leader hint to the control anchor, and every op that
//! matters to them is anchored there.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::prelude::*;
use crate::storage::raft::driver::NotRaftLeader;
use crate::storage::raft::Lane;
use crate::storage::store::Store;
use crate::structs::{AllocationHealth, StopCause};

/// A store write that may be forwarded to its lane's leader. Closed enum:
/// adding a variant is adding a wire message (serde_json on the manager
/// plane, versioned by PROTOCOL_VERSION like the rest of the raft
/// transport), so variants carry stable, explicit fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ForwardableLaneWrite {
    /// `Store::update_node_heartbeat` — status lane.
    NodeHeartbeat { id: String },
    /// `Store::update_node_health_checks` — status lane.
    NodeHealthChecks { id: String, health_checks: String },
    /// The status half of `Store::mark_node_ready` — status lane.
    MarkNodeReady { id: String, last_heartbeat: String },
    /// The status half of `Store::mark_node_lost` — status lane.
    MarkNodeLost { id: String },
    /// `Store::upsert_allocation_health_snapshots` — status lane.
    AllocationHealthSnapshots {
        node_id: String,
        snapshots: Vec<AllocationHealth>,
    },
    /// `Store::sync_node_availability` (the control half of the mark-ready/
    /// mark-lost composites) — control lane.
    SyncNodeAvailability {
        ids: Vec<String>,
        availability: String,
    },
    /// `Store::mark_job_failed` — control lane.
    MarkJobFailed { job_id: String },
    /// The decision half of `Store::gang_member_lost` — scheduler lane. A
    /// worker's failure report lost a gang member: the leader reads the
    /// gang's survivors from its own state (it is the decision, made where
    /// the authoritative copy lives) and restarts them as the gang's
    /// `onMemberLoss` says, or (`stop_job`) stops every allocation of a job
    /// the origin just marked failed. Returns the `GangLoss`, when the task
    /// is a gang its job still wants.
    GangMemberLost {
        job_id: String,
        job_config_id: String,
        task_config_id: i64,
        exhausted: bool,
        stop_job: bool,
    },
    /// `Store::remove_allocation_by_id` — scheduler lane. Returns the
    /// removed allocation, when one existed.
    ///
    /// `cause` is why the allocation is going away, recorded as a stop notice
    /// for the owning worker. It defaults on deserialize so a forward sent by
    /// a manager that predates stop notices still applies here, landing as
    /// [`StopCause::Unknown`] — the same value the worker would have used
    /// with no notice at all.
    RemoveAllocationById {
        allocation_id: i64,
        #[serde(default)]
        cause: StopCause,
        /// The `last_stop` detail and hint (the memory pressure path). Absent
        /// from a manager that predates it, which records the generic
        /// sentence for `cause` instead.
        #[serde(default)]
        explanation: Option<crate::storage::store::StopExplanation>,
    },
    /// `Store::remove_failed_allocation` — scheduler lane. A worker's
    /// failure report (or a pressure eviction that spends the move budget):
    /// removes the allocation and records the move budget's verdict, and
    /// the reschedule hold when it defers the replacement. Returns the
    /// removed allocation and the verdict, when the allocation existed.
    RemoveFailedAllocation {
        allocation_id: i64,
        cause: StopCause,
        explanation: Option<crate::storage::store::StopExplanation>,
        failure: crate::storage::store::FailedMove,
    },
    /// `Store::enqueue_eval_after_delay` — scheduler lane. Returns the
    /// enqueued eval.
    EnqueueEval {
        kind: String,
        job_id: Option<String>,
        job_config_id: Option<String>,
        node_id: Option<String>,
        priority: Option<i64>,
        delay_ms: u64,
    },
    /// `Store::seed_keyed_instances` — scheduler lane. Returns the ids
    /// actually seeded (existing ids in any state are skipped).
    SeedKeyedInstances {
        job_id: String,
        group: String,
        task_name: String,
        ids: Vec<String>,
    },
    /// `Store::record_volume_mounted` — scheduler lane.
    RecordVolumeMounted {
        volume_id: String,
        node_id: String,
        mount_path: String,
        node_local: bool,
        fs: Option<String>,
        attrs: std::collections::BTreeMap<String, Value>,
    },
    /// `Store::record_volume_error` — scheduler lane.
    RecordVolumeError { volume_id: String, error: String },
    /// `Store::mark_volume_destroyed` — scheduler lane.
    MarkVolumeDestroyed { volume_id: String },
    /// `Store::complete_allocation`'s scheduler-lane half. Returns the
    /// completed allocation and the task status it decided, when the
    /// allocation existed; the origin writes that status itself
    /// (`SetTaskStatus`, status lane).
    CompleteAllocation { allocation_id: i64, success: bool },
    /// `Store::set_task_status` — status lane. A batch completion's second
    /// half, and a task failed for good, both from worker reports.
    SetTaskStatus {
        task_config_id: i64,
        status: crate::structs::TaskStatusState,
    },
    /// `Store::apply_snapshot_report` — scheduler lane. Worker snapshot
    /// reports ride heartbeats, which can land on any manager. Returns the
    /// applied outcome, for the caller's events.
    ApplySnapshotReport {
        node_id: String,
        report: crate::storage::store::SnapshotReportRecord,
    },
    /// `Store::begin_suspend` — scheduler lane. The idle sweep runs on the
    /// manager that holds a node's usage report, which is whichever manager
    /// the node heartbeats to. Returns the inserted snapshot row.
    BeginSuspend {
        new: crate::storage::store::NewSnapshot,
    },
    /// `Store::emit_event_on(Lane::Status, ..)` — status lane. A standalone
    /// event from a manager that leads no lane (a follower in `single`
    /// topology), such as the `node.pressure` events a worker's heartbeat
    /// carries.
    EmitEvent {
        draft: crate::storage::events::EventDraft,
    },
    /// `Store::resize_envelope` — scheduler lane. A node's elastic envelope
    /// resize, from its heartbeat, which can land on any manager. Returns
    /// the outcome.
    ResizeEnvelope {
        node_id: String,
        request: crate::storage::store::EnvelopeResizeRequest,
    },
}

impl ForwardableLaneWrite {
    /// The lane whose leader must execute this op — the same lane its
    /// non-forwarding variant proposes on.
    pub(crate) fn lane(&self) -> Lane {
        match self {
            ForwardableLaneWrite::NodeHeartbeat { .. }
            | ForwardableLaneWrite::NodeHealthChecks { .. }
            | ForwardableLaneWrite::MarkNodeReady { .. }
            | ForwardableLaneWrite::MarkNodeLost { .. }
            | ForwardableLaneWrite::AllocationHealthSnapshots { .. }
            | ForwardableLaneWrite::SetTaskStatus { .. }
            | ForwardableLaneWrite::EmitEvent { .. } => Lane::Status,
            ForwardableLaneWrite::SyncNodeAvailability { .. }
            | ForwardableLaneWrite::MarkJobFailed { .. } => Lane::Control,
            ForwardableLaneWrite::RemoveAllocationById { .. }
            | ForwardableLaneWrite::GangMemberLost { .. }
            | ForwardableLaneWrite::RemoveFailedAllocation { .. }
            | ForwardableLaneWrite::EnqueueEval { .. }
            | ForwardableLaneWrite::SeedKeyedInstances { .. }
            | ForwardableLaneWrite::RecordVolumeMounted { .. }
            | ForwardableLaneWrite::RecordVolumeError { .. }
            | ForwardableLaneWrite::MarkVolumeDestroyed { .. }
            | ForwardableLaneWrite::CompleteAllocation { .. }
            | ForwardableLaneWrite::ApplySnapshotReport { .. }
            | ForwardableLaneWrite::BeginSuspend { .. }
            | ForwardableLaneWrite::ResizeEnvelope { .. } => Lane::Scheduler,
        }
    }

    /// Stable name for logs and error contexts.
    pub(crate) fn op_name(&self) -> &'static str {
        match self {
            ForwardableLaneWrite::NodeHeartbeat { .. } => "update_node_heartbeat",
            ForwardableLaneWrite::NodeHealthChecks { .. } => "update_node_health_checks",
            ForwardableLaneWrite::MarkNodeReady { .. } => "mark_node_ready",
            ForwardableLaneWrite::MarkNodeLost { .. } => "mark_node_lost",
            ForwardableLaneWrite::AllocationHealthSnapshots { .. } => {
                "upsert_allocation_health_snapshots"
            }
            ForwardableLaneWrite::SyncNodeAvailability { .. } => "sync_node_availability",
            ForwardableLaneWrite::MarkJobFailed { .. } => "mark_job_failed",
            ForwardableLaneWrite::GangMemberLost { .. } => "gang_member_lost",
            ForwardableLaneWrite::RemoveAllocationById { .. } => "remove_allocation_by_id",
            ForwardableLaneWrite::RemoveFailedAllocation { .. } => "remove_failed_allocation",
            ForwardableLaneWrite::EnqueueEval { .. } => "enqueue_eval",
            ForwardableLaneWrite::SeedKeyedInstances { .. } => "seed_keyed_instances",
            ForwardableLaneWrite::RecordVolumeMounted { .. } => "record_volume_mounted",
            ForwardableLaneWrite::RecordVolumeError { .. } => "record_volume_error",
            ForwardableLaneWrite::MarkVolumeDestroyed { .. } => "mark_volume_destroyed",
            ForwardableLaneWrite::CompleteAllocation { .. } => "complete_allocation",
            ForwardableLaneWrite::SetTaskStatus { .. } => "set_task_status",
            ForwardableLaneWrite::ApplySnapshotReport { .. } => "apply_snapshot_report",
            ForwardableLaneWrite::BeginSuspend { .. } => "begin_suspend",
            ForwardableLaneWrite::EmitEvent { .. } => "emit_event",
            ForwardableLaneWrite::ResizeEnvelope { .. } => "resize_envelope",
        }
    }
}

/// Wire outcome of a forwarded lane write (the `ForwardLaneWrite` reply
/// payload).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardOutcome {
    /// The op executed on the target; carries its JSON-encoded return value
    /// (`null` for `()`-returning ops).
    Ok(Value),
    /// The target does not lead the lane either; carries ITS leader hint so
    /// the origin re-raises a fully-typed `NotRaftLeader`.
    NotLeader {
        lane: Option<String>,
        leader_name: Option<String>,
        leader_addr: Option<String>,
    },
    /// The op executed and failed with a real (non-leadership) error.
    Failed(String),
}

/// Executes a forwarded op against the local store using the NON-forwarding
/// variants (rule 2 above), mapping the result onto the wire outcome.
pub(crate) fn execute(store: &Store, op: ForwardableLaneWrite) -> ForwardOutcome {
    fn done<T: Serialize>(result: R<T>) -> ForwardOutcome {
        match result {
            Ok(value) => match serde_json::to_value(value) {
                Ok(value) => ForwardOutcome::Ok(value),
                Err(err) => ForwardOutcome::Failed(format!(
                    "failed encoding forwarded op return value: {err}"
                )),
            },
            Err(err) => match err.chain().find_map(|c| c.downcast_ref::<NotRaftLeader>()) {
                Some(not_leader) => ForwardOutcome::NotLeader {
                    lane: not_leader.lane.clone(),
                    leader_name: not_leader.leader_name.clone(),
                    leader_addr: not_leader.leader_addr.clone(),
                },
                None => ForwardOutcome::Failed(format!("{err:#}")),
            },
        }
    }
    match op {
        ForwardableLaneWrite::NodeHeartbeat { id } => done(store.update_node_heartbeat_local(id)),
        ForwardableLaneWrite::NodeHealthChecks { id, health_checks } => {
            done(store.update_node_health_checks_local(&id, &health_checks))
        }
        ForwardableLaneWrite::MarkNodeReady { id, last_heartbeat } => {
            done(store.mark_node_ready_status_local(&id, &last_heartbeat))
        }
        ForwardableLaneWrite::MarkNodeLost { id } => done(store.mark_node_lost_status_local(&id)),
        ForwardableLaneWrite::AllocationHealthSnapshots { node_id, snapshots } => {
            done(store.upsert_allocation_health_snapshots_local(&node_id, &snapshots))
        }
        ForwardableLaneWrite::SyncNodeAvailability { ids, availability } => {
            done(store.sync_node_availability_local(&ids, &availability))
        }
        ForwardableLaneWrite::MarkJobFailed { job_id } => {
            done(store.mark_job_failed_local(&job_id))
        }
        ForwardableLaneWrite::GangMemberLost {
            job_id,
            job_config_id,
            task_config_id,
            exhausted,
            stop_job,
        } => done(store.gang_loss_op_local(
            &job_id,
            &job_config_id,
            task_config_id,
            exhausted,
            stop_job,
        )),
        ForwardableLaneWrite::RemoveAllocationById {
            allocation_id,
            cause,
            explanation,
        } => done(store.remove_allocation_by_id_local(allocation_id, cause, explanation)),
        ForwardableLaneWrite::RemoveFailedAllocation {
            allocation_id,
            cause,
            explanation,
            failure,
        } => done(store.remove_failed_allocation_local(allocation_id, cause, explanation, failure)),
        ForwardableLaneWrite::EnqueueEval {
            kind,
            job_id,
            job_config_id,
            node_id,
            priority,
            delay_ms,
        } => done(store.enqueue_eval_after_delay_local(
            &kind,
            job_id.as_deref(),
            job_config_id.as_deref(),
            node_id.as_deref(),
            priority,
            std::time::Duration::from_millis(delay_ms),
        )),
        ForwardableLaneWrite::SeedKeyedInstances {
            job_id,
            group,
            task_name,
            ids,
        } => done(store.seed_keyed_instances_local(&job_id, &group, &task_name, &ids)),
        ForwardableLaneWrite::RecordVolumeMounted {
            volume_id,
            node_id,
            mount_path,
            node_local,
            fs,
            attrs,
        } => done(store.record_volume_mounted_local(
            &volume_id,
            &node_id,
            &mount_path,
            node_local,
            fs.as_deref(),
            &attrs,
        )),
        ForwardableLaneWrite::RecordVolumeError { volume_id, error } => {
            done(store.record_volume_error_local(&volume_id, &error))
        }
        ForwardableLaneWrite::MarkVolumeDestroyed { volume_id } => {
            done(store.mark_volume_destroyed_local(&volume_id))
        }
        ForwardableLaneWrite::CompleteAllocation {
            allocation_id,
            success,
        } => done(store.complete_allocation_local(allocation_id, success)),
        ForwardableLaneWrite::SetTaskStatus {
            task_config_id,
            status,
        } => done(store.set_task_status_local(task_config_id, status)),
        ForwardableLaneWrite::ApplySnapshotReport { node_id, report } => {
            done(store.apply_snapshot_report_local(&node_id, &report))
        }
        ForwardableLaneWrite::BeginSuspend { new } => done(store.begin_suspend_local(new)),
        ForwardableLaneWrite::EmitEvent { draft } => done(store.emit_event_on(Lane::Status, draft)),
        ForwardableLaneWrite::ResizeEnvelope { node_id, request } => {
            done(store.resize_envelope_local(&node_id, &request))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire enum must round-trip: it is a manager-plane wire format
    /// (versioned by PROTOCOL_VERSION), so an encode/decode asymmetry is a
    /// cross-version incident, not a bug fix.
    #[test]
    fn forwardable_ops_round_trip_and_map_lanes() {
        let ops = vec![
            ForwardableLaneWrite::NodeHeartbeat { id: "n1".into() },
            ForwardableLaneWrite::NodeHealthChecks {
                id: "n1".into(),
                health_checks: "[]".into(),
            },
            ForwardableLaneWrite::MarkNodeReady {
                id: "n1".into(),
                last_heartbeat: "2026-07-28T00:00:00Z".into(),
            },
            ForwardableLaneWrite::AllocationHealthSnapshots {
                node_id: "n1".into(),
                snapshots: Vec::new(),
            },
            ForwardableLaneWrite::SyncNodeAvailability {
                ids: vec!["n1".into()],
                availability: "available".into(),
            },
            ForwardableLaneWrite::MarkJobFailed {
                job_id: "job".into(),
            },
            ForwardableLaneWrite::GangMemberLost {
                job_id: "job".into(),
                job_config_id: "job-1".into(),
                task_config_id: 3,
                exhausted: false,
                stop_job: false,
            },
            ForwardableLaneWrite::RemoveAllocationById {
                allocation_id: 7,
                cause: StopCause::Unknown,
                explanation: None,
            },
            ForwardableLaneWrite::RemoveFailedAllocation {
                allocation_id: 8,
                cause: StopCause::NodePressure,
                explanation: None,
                failure: crate::storage::store::FailedMove {
                    policy: crate::storage::store::MovePolicy {
                        move_delay_ms: 30_000,
                        move_count: Some(3),
                        move_period_ms: 1_800_000,
                    },
                    reason: "boom".into(),
                },
            },
            ForwardableLaneWrite::EnqueueEval {
                kind: "job_reschedule".into(),
                job_id: Some("job".into()),
                job_config_id: Some("cfg".into()),
                node_id: None,
                priority: None,
                delay_ms: 0,
            },
            ForwardableLaneWrite::RecordVolumeMounted {
                volume_id: "v1".into(),
                node_id: "n1".into(),
                mount_path: "/mnt".into(),
                node_local: true,
                fs: None,
                attrs: Default::default(),
            },
            ForwardableLaneWrite::RecordVolumeError {
                volume_id: "v1".into(),
                error: "boom".into(),
            },
            ForwardableLaneWrite::MarkVolumeDestroyed {
                volume_id: "v1".into(),
            },
            ForwardableLaneWrite::CompleteAllocation {
                allocation_id: 9,
                success: true,
            },
            ForwardableLaneWrite::SetTaskStatus {
                task_config_id: 9,
                status: crate::structs::TaskStatusState::Success,
            },
            ForwardableLaneWrite::ApplySnapshotReport {
                node_id: "n1".into(),
                report: crate::storage::store::SnapshotReportRecord {
                    snapshot_id: "snap-1".into(),
                    op: "capture".into(),
                    ok: true,
                    error: String::new(),
                    manifest_json: "{}".into(),
                    manifest_url: String::new(),
                    duration_ms: 1,
                    total_bytes: 2,
                    allocation_id: 3,
                    pause_ms: 4,
                    local_suspension: true,
                },
            },
            ForwardableLaneWrite::BeginSuspend {
                new: crate::storage::store::NewSnapshot {
                    snapshot_id: "snap-2".into(),
                    job_id: "job".into(),
                    task_name: "task".into(),
                    instance_id: "user-42".into(),
                    name: None,
                    kind: crate::structs::checkpoint::SnapshotKind::Memory,
                    purpose: crate::structs::snapshot::SnapshotPurpose::Suspend,
                    node_id: "n1".into(),
                    allocation_id: 4,
                    expires_at: None,
                    suspend_reason: Some("idle".into()),
                    durability: crate::structs::snapshot::SnapshotDurability::Local,
                },
            },
            ForwardableLaneWrite::EmitEvent {
                draft: crate::storage::events::EventDraft::warn("node.pressure", "evicted")
                    .node("n1")
                    .allocation(5)
                    .meta(serde_json::json!({ "step": "evict-bursting" })),
            },
            ForwardableLaneWrite::ResizeEnvelope {
                node_id: "n1".into(),
                request: crate::storage::store::EnvelopeResizeRequest {
                    allocation_id: 6,
                    from: [("cpu".to_string(), 1000.0)].into(),
                    to: [("cpu".to_string(), 1500.0)].into(),
                },
            },
        ];
        for op in ops {
            let bytes = serde_json::to_vec(&op).expect("encode");
            let decoded: ForwardableLaneWrite = serde_json::from_slice(&bytes).expect("decode");
            assert_eq!(decoded.lane(), op.lane());
            assert_eq!(decoded.op_name(), op.op_name());
        }
    }

    /// Status-lane ops are the liveness path, control carries the
    /// availability sync, scheduler carries the failure-report tail — a
    /// misrouted variant would propose on a lane that rejects it.
    #[test]
    fn lanes_are_the_ones_the_local_variants_propose_on() {
        assert_eq!(
            ForwardableLaneWrite::NodeHeartbeat { id: String::new() }.lane(),
            Lane::Status
        );
        assert_eq!(
            ForwardableLaneWrite::SyncNodeAvailability {
                ids: Vec::new(),
                availability: String::new()
            }
            .lane(),
            Lane::Control
        );
        assert_eq!(
            ForwardableLaneWrite::EnqueueEval {
                kind: String::new(),
                job_id: None,
                job_config_id: None,
                node_id: None,
                priority: None,
                delay_ms: 0
            }
            .lane(),
            Lane::Scheduler
        );
    }
}
