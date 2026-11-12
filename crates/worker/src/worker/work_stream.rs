//! The worker half of `Manager.StreamWork` (sandbox-scaling plan §6.13,
//! Phase 2; docs/architecture-managers-workers.md, "Delivery").
//!
//! The worker holds one stream to its manager and keeps a **mirror** of its
//! desired set: the manager's sequenced deltas patch it, and a full resync
//! replaces it. New allocations from a delta launch at once, the way a
//! `PostWork` push used to start them; everything else — stops, payload
//! changes, a resync, the identity and datapath feeds — goes through the
//! heartbeat's reconcile, which reads the mirror instead of a desired set
//! on the reply (`HeartbeatReply.desired_via_stream`), and which such a
//! message runs at once instead of at the next beat.
//!
//! The heartbeat carries the stream position and a digest of the mirror
//! (`HeartbeatRequest.work_stream`). A manager that disagrees answers
//! `work_resync` and sends a full resync on the stream; a manager that does
//! not recognize the stream (another epoch, no stream attached) answers with
//! the full desired set, which the heartbeat reconciles as it always did.
//!
//! A message can also push checkpoint work (`checkpoint_requests`). That is
//! not part of the mirror: it is handed to the caller to start at once, and
//! the heartbeat's full checkpoint set stays the record.
//!
//! **Resume** (revisions-and-watches plan §7): the mirror keeps the
//! `revision` of the last message it applied, across a dropped stream, and a
//! reconnect sends it. A manager that cannot replay but still knows what
//! changed on the node since then answers with one `resumed_from` message
//! carrying only those changes, applied on top of the mirror. A resume
//! that does not continue from the mirror's revision is refused: the stream
//! is dropped and reopened with no position, which gets a full resync.

use crate::prelude::*;
use crate::structs::worker_protocol::WorkDigest;
use crate::structs::{
    manager_client::ManagerClient, stream_work_request, AllocationHealth, CheckpointRequest,
    StopNotice, StreamWorkAck, StreamWorkOpen, StreamWorkReply, StreamWorkReport,
    StreamWorkRequest, TaskAllocation, WorkStreamState,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tonic::transport::Channel;

/// Reconnect backoff bounds for a dropped stream.
const STREAM_RETRY_MIN: Duration = Duration::from_millis(250);
const STREAM_RETRY_MAX: Duration = Duration::from_secs(10);

/// How many health transitions wait for the stream before newer ones are
/// dropped (the heartbeat carries them all anyway).
const REPORT_QUEUE: usize = 1024;

/// How many health transitions one `report` frame carries at most.
const REPORT_BATCH: usize = 256;

/// The task manager's end of the health reports the stream sends between
/// heartbeats (`StreamWorkReport`).
pub type ReportSender = tokio::sync::mpsc::Sender<AllocationHealth>;

/// The stream's end of [`ReportSender`].
pub type ReportReceiver = tokio::sync::mpsc::Receiver<AllocationHealth>;

/// A channel for health transitions, from the task manager to the stream.
pub fn report_channel() -> (ReportSender, ReportReceiver) {
    tokio::sync::mpsc::channel(REPORT_QUEUE)
}

/// The worker's copy of its desired set, as the stream has delivered it.
#[derive(Debug, Default)]
pub struct WorkMirror {
    epoch: u64,
    seq: u64,
    allocations: BTreeMap<i64, TaskAllocation>,
    digest: WorkDigest,
    /// Stop notices received since the heartbeat last took them.
    stops: Vec<StopNotice>,
    /// A resync has been applied on the current stream, so the mirror is the
    /// manager's desired set and the heartbeat may report it.
    live: bool,
    /// The `revision` of the last message applied, kept across a dropped
    /// stream for the reconnect to resume from. Empty: nothing to resume.
    revision: String,
}

/// Shared between the stream task and the heartbeat loop.
pub type SharedMirror = Arc<Mutex<WorkMirror>>;

/// What one applied message changed, for the caller to act on.
#[derive(Debug, Default)]
pub struct Applied {
    /// The policy assignments the message carried for its upserts
    /// (`StreamWorkReply.net_policies`), for the launches of `added`.
    pub net_policies: Vec<crate::structs::AllocNetPolicy>,
    /// Allocations the mirror did not hold before this message.
    pub added: Vec<TaskAllocation>,
    /// The message was a full resync.
    pub resync: bool,
    /// The message stopped something or changed a held payload: the
    /// heartbeat's reconcile has work to do now rather than at its next
    /// beat.
    pub reconcile: bool,
    /// Checkpoint work the message pushed, to start now. Passed on even
    /// from a message the mirror ignored: a push is the manager's current
    /// view whatever its position, and the worker deduplicates it.
    pub checkpoint_requests: Vec<CheckpointRequest>,
    /// Endpoint sets a push-only message carried (mesh-control-path plan
    /// §6.1), for the datapath to apply now. Not part of the mirror.
    pub endpoint_feed: Option<crate::structs::EndpointFeed>,
    /// The runtime disables a push-only message carried
    /// (docs/runtime-disable.md): the whole set, replacing what the worker
    /// holds. Not part of the mirror.
    pub runtime_states: Option<crate::structs::RuntimeStateSet>,
    /// Owner notices a push-only message carried (docs/child-sessions.md):
    /// what happened to the owners of child sessions this node holds. Not
    /// part of the mirror.
    pub owner_notices: Vec<crate::structs::OwnerNotice>,
    /// The message was a resume that does not continue from this mirror's
    /// revision: the stream must be reopened with no position.
    pub resume_refused: bool,
}

impl WorkMirror {
    /// Apply one message. Messages from another epoch, or out of sequence,
    /// are ignored: the manager resyncs a stream it cannot continue, and
    /// the heartbeat digest catches anything that slips through.
    pub fn apply(&mut self, message: &StreamWorkReply) -> Applied {
        let mut applied = Applied {
            resync: message.resync,
            reconcile: message.resync,
            checkpoint_requests: message.checkpoint_requests.clone(),
            net_policies: message.net_policies.clone(),
            ..Default::default()
        };
        if message.resync {
            let before = std::mem::take(&mut self.allocations);
            for allocation in &message.upserts {
                if !before.contains_key(&allocation.allocation_id) {
                    applied.added.push(allocation.clone());
                }
                self.allocations
                    .insert(allocation.allocation_id, allocation.clone());
            }
            self.digest = WorkDigest::of(self.allocations.keys().copied());
            self.epoch = message.epoch;
            self.seq = message.seq;
            self.live = true;
            self.revision = message.revision.clone();
            return applied;
        }
        if !message.resumed_from.is_empty() {
            // A resume continues from the revision this mirror last applied,
            // whatever epoch that came from; anything else would apply
            // changes to a set they were not computed against.
            if self.revision.is_empty() || self.revision != message.resumed_from {
                applied.resume_refused = true;
                return applied;
            }
            self.epoch = message.epoch;
            self.seq = message.seq.saturating_sub(1);
            self.live = true;
            applied.reconcile = true;
        } else if !self.live || message.epoch != self.epoch || message.seq != self.seq + 1 {
            return applied;
        }
        for allocation in &message.upserts {
            if self
                .allocations
                .insert(allocation.allocation_id, allocation.clone())
                .is_none()
            {
                self.digest.insert(allocation.allocation_id);
                applied.added.push(allocation.clone());
            } else {
                applied.reconcile = true;
            }
        }
        applied.reconcile |= !message.stops.is_empty();
        for stop in &message.stops {
            if self.allocations.remove(&stop.allocation_id).is_some() {
                self.digest.remove(stop.allocation_id);
            }
            self.stops.push(stop.clone());
        }
        self.seq = message.seq;
        self.revision = message.revision.clone();
        applied
    }

    /// The revision a reconnect resumes from: the last message applied,
    /// kept across a dropped stream. Empty when there is nothing to resume.
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Forget the revision, so the next stream asks for a full resync (a
    /// resume was refused).
    pub fn forget_revision(&mut self) {
        self.revision.clear();
    }

    /// The position and digest a heartbeat reports, when the mirror is live.
    pub fn state(&self) -> Option<WorkStreamState> {
        self.live.then(|| WorkStreamState {
            epoch: self.epoch,
            seq: self.seq,
            digest: self.digest.render(),
        })
    }

    /// Where a reconnecting stream asks to resume from.
    pub fn position(&self) -> (u64, u64) {
        if self.live {
            (self.epoch, self.seq)
        } else {
            (0, 0)
        }
    }

    /// The stream dropped: until a new one delivers, the heartbeat reports
    /// no position and reconciles from the manager's full desired set.
    pub fn mark_down(&mut self) {
        self.live = false;
    }

    /// The desired set the heartbeat reconciles against.
    pub fn desired(&self) -> Vec<TaskAllocation> {
        self.allocations.values().cloned().collect()
    }

    /// The stop notices to deliver with the next reconcile.
    pub fn take_stops(&mut self) -> Vec<StopNotice> {
        std::mem::take(&mut self.stops)
    }

    /// Put back stop notices a failed heartbeat took.
    pub fn restore_stops(&mut self, mut stops: Vec<StopNotice>) {
        stops.append(&mut self.stops);
        self.stops = stops;
    }
}

/// Keep a `StreamWork` stream open to `client` for `node_id`, forever,
/// reconnecting with backoff. Each applied message updates `mirror`, and
/// `on_applied` runs with what it changed (launching new allocations,
/// waking the heartbeat).
///
/// Health transitions on `reports` go to the manager as `report` frames
/// while the stream is up. Ones queued while it was down are dropped when it
/// reopens: the heartbeat has carried them since.
pub async fn run_work_stream<F, A>(
    client: ManagerClient<Channel>,
    attach: A,
    node_id: String,
    mirror: SharedMirror,
    mut reports: ReportReceiver,
    on_applied: F,
) where
    F: Fn(Applied) + Send + Sync + 'static,
    A: Fn(WorkRequest) -> WorkRequest + Send + Sync + 'static,
{
    let mut backoff = STREAM_RETRY_MIN;
    loop {
        match stream_once(
            &client,
            &attach,
            &node_id,
            &mirror,
            &mut reports,
            &on_applied,
        )
        .await
        {
            Ok(()) => {
                debug!(node_id = %node_id, "Work stream ended; reconnecting");
                backoff = STREAM_RETRY_MIN;
            }
            Err(status) => {
                debug!(
                    node_id = %node_id,
                    code = %status.code(),
                    retry_in_ms = backoff.as_millis() as u64,
                    "Work stream failed; retrying"
                );
            }
        }
        mirror.lock().unwrap_or_else(|p| p.into_inner()).mark_down();
        tokio::time::sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, STREAM_RETRY_MAX);
    }
}

/// The request a stream opens with, before its credential is attached.
pub type WorkRequest = tonic::Request<tokio_stream::wrappers::ReceiverStream<StreamWorkRequest>>;

/// One stream's life: open, apply and ack until it ends.
async fn stream_once<F, A>(
    client: &ManagerClient<Channel>,
    attach: &A,
    node_id: &str,
    mirror: &SharedMirror,
    reports: &mut ReportReceiver,
    on_applied: &F,
) -> Result<(), tonic::Status>
where
    F: Fn(Applied),
    A: Fn(WorkRequest) -> WorkRequest,
{
    let ((last_epoch, last_seq), last_revision) = {
        let mirror = mirror.lock().unwrap_or_else(|p| p.into_inner());
        (mirror.position(), mirror.revision().to_string())
    };
    let (tx, rx) = tokio::sync::mpsc::channel::<StreamWorkRequest>(64);
    tx.send(StreamWorkRequest {
        frame: Some(stream_work_request::Frame::Open(StreamWorkOpen {
            node_id: node_id.to_string(),
            last_epoch,
            last_seq,
            last_revision,
        })),
    })
    .await
    .map_err(|_| tonic::Status::cancelled("work stream request channel closed"))?;
    let request = attach(tonic::Request::new(
        tokio_stream::wrappers::ReceiverStream::new(rx),
    ));
    let mut inbound = client.clone().stream_work(request).await?.into_inner();
    // Transitions queued while no stream was up are already on a heartbeat.
    while reports.try_recv().is_ok() {}
    loop {
        let message = tokio::select! {
            message = inbound.message() => match message? {
                Some(message) => message,
                None => break,
            },
            Some(first) = reports.recv() => {
                let mut allocation_health = vec![first];
                while allocation_health.len() < REPORT_BATCH {
                    match reports.try_recv() {
                        Ok(more) => allocation_health.push(more),
                        Err(_) => break,
                    }
                }
                let _ = tx
                    .send(StreamWorkRequest {
                        frame: Some(stream_work_request::Frame::Report(StreamWorkReport {
                            allocation_health,
                        })),
                    })
                    .await;
                continue;
            }
        };
        // A push-only message has no position: hand its pushes on, and
        // neither apply it to the mirror nor acknowledge it.
        if message.push_only {
            on_applied(Applied {
                endpoint_feed: message.endpoint_feed,
                runtime_states: message.runtime_states,
                owner_notices: message.owner_notices,
                ..Default::default()
            });
            continue;
        }
        let applied = mirror
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply(&message);
        if applied.resume_refused {
            mirror
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .forget_revision();
            return Err(tonic::Status::aborted(
                "the manager resumed from a revision this worker did not apply; reopening for a \
                 full resync",
            ));
        }
        // Acked once applied, so a reconnect resumes after it.
        let _ = tx
            .send(StreamWorkRequest {
                frame: Some(stream_work_request::Frame::Ack(StreamWorkAck {
                    epoch: message.epoch,
                    seq: message.seq,
                })),
            })
            .await;
        on_applied(applied);
    }
    Ok(())
}

#[cfg(test)]
mod work_stream_test {
    use super::*;

    fn payload(id: i64) -> TaskAllocation {
        TaskAllocation {
            allocation_id: id,
            ..Default::default()
        }
    }

    fn message(seq: u64, resync: bool, upserts: &[i64], stops: &[i64]) -> StreamWorkReply {
        StreamWorkReply {
            epoch: 7,
            seq,
            resync,
            upserts: upserts.iter().map(|id| payload(*id)).collect(),
            stops: stops
                .iter()
                .map(|id| StopNotice {
                    allocation_id: *id,
                    cause: "stopped".into(),
                    ..Default::default()
                })
                .collect(),
            digest: String::new(),
            checkpoint_requests: Vec::new(),
            revision: format!("r{seq}"),
            resumed_from: String::new(),
            ..Default::default()
        }
    }

    #[test]
    fn a_resume_applies_on_top_of_the_mirror_only_from_its_revision() {
        let mut mirror = WorkMirror::default();
        mirror.apply(&message(1, true, &[1, 2], &[]));
        mirror.apply(&message(2, false, &[3], &[]));
        assert_eq!(mirror.revision(), "r2");
        mirror.mark_down();
        assert_eq!(mirror.position(), (0, 0), "no replay position while down");
        assert_eq!(mirror.revision(), "r2", "the revision outlives the stream");

        // A new manager (another epoch) resumes from r2: 1 stopped, 4 new.
        let mut resume = message(1, false, &[4], &[1]);
        resume.epoch = 99;
        resume.resumed_from = "r2".into();
        resume.revision = "r9".into();
        let applied = mirror.apply(&resume);
        assert!(!applied.resume_refused);
        assert!(applied.reconcile);
        assert_eq!(
            applied
                .added
                .iter()
                .map(|a| a.allocation_id)
                .collect::<Vec<_>>(),
            vec![4]
        );
        let ids: Vec<i64> = mirror
            .desired()
            .iter()
            .map(|allocation| allocation.allocation_id)
            .collect();
        assert_eq!(ids, vec![2, 3, 4]);
        let state = mirror.state().expect("live after a resume");
        assert_eq!((state.epoch, state.seq), (99, 1));
        assert_eq!(state.digest, WorkDigest::of([2, 3, 4]).render());
        assert_eq!(mirror.revision(), "r9");
        // Deltas continue in the new epoch.
        let mut next = message(2, false, &[5], &[]);
        next.epoch = 99;
        mirror.apply(&next);
        assert_eq!(mirror.desired().len(), 4);

        // A resume from a revision the mirror did not apply is refused.
        let mut stale = message(1, false, &[], &[]);
        stale.epoch = 100;
        stale.resumed_from = "r1".into();
        assert!(mirror.apply(&stale).resume_refused);
        mirror.forget_revision();
        assert_eq!(mirror.revision(), "");
    }

    #[test]
    fn a_resync_then_deltas_build_the_mirror_and_report_new_allocations() {
        let mut mirror = WorkMirror::default();
        assert!(mirror.state().is_none(), "not live before the first resync");
        let applied = mirror.apply(&message(1, true, &[1, 2], &[]));
        assert!(applied.resync);
        assert_eq!(applied.added.len(), 2);
        let applied = mirror.apply(&message(2, false, &[3, 2], &[1]));
        assert!(applied.reconcile, "a stop and an update need the reconcile");
        assert!(!mirror.apply(&message(3, false, &[4], &[])).reconcile);
        assert_eq!(
            applied
                .added
                .iter()
                .map(|a| a.allocation_id)
                .collect::<Vec<_>>(),
            vec![3],
            "an update of a held allocation is not new"
        );
        let state = mirror.state().expect("live");
        assert_eq!((state.epoch, state.seq), (7, 3));
        assert_eq!(state.digest, WorkDigest::of([2, 3, 4]).render());
        assert_eq!(
            mirror
                .desired()
                .iter()
                .map(|a| a.allocation_id)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(mirror.take_stops().len(), 1);
        assert!(mirror.take_stops().is_empty());
    }

    #[test]
    fn out_of_sequence_and_foreign_epoch_messages_are_ignored() {
        let mut mirror = WorkMirror::default();
        mirror.apply(&message(1, true, &[1], &[]));
        // A gap.
        assert!(mirror.apply(&message(3, false, &[9], &[])).added.is_empty());
        // Another epoch.
        let mut foreign = message(2, false, &[9], &[]);
        foreign.epoch = 8;
        assert!(mirror.apply(&foreign).added.is_empty());
        assert_eq!(mirror.state().expect("live").seq, 1);
        // Down: the position resets so a reconnect resyncs.
        mirror.mark_down();
        assert_eq!(mirror.position(), (0, 0));
        assert!(mirror.state().is_none());
    }
}
