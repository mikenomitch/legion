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

use crate::prelude::*;
use crate::storage::work_log::WorkDigest;
use crate::structs::{
    manager_client::ManagerClient, stream_work_request, StopNotice, StreamWorkAck, StreamWorkOpen,
    StreamWorkReply, StreamWorkRequest, TaskAllocation, WorkStreamState,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tonic::transport::Channel;

/// Reconnect backoff bounds for a dropped stream.
const STREAM_RETRY_MIN: Duration = Duration::from_millis(250);
const STREAM_RETRY_MAX: Duration = Duration::from_secs(10);

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
}

/// Shared between the stream task and the heartbeat loop.
pub type SharedMirror = Arc<Mutex<WorkMirror>>;

/// What one applied message changed, for the caller to act on.
#[derive(Debug, Default)]
pub struct Applied {
    /// Allocations the mirror did not hold before this message.
    pub added: Vec<TaskAllocation>,
    /// The message was a full resync.
    pub resync: bool,
    /// The message stopped something or changed a held payload: the
    /// heartbeat's reconcile has work to do now rather than at its next
    /// beat.
    pub reconcile: bool,
}

impl WorkMirror {
    /// Apply one message. Messages from another epoch, or out of sequence,
    /// are ignored: the manager resyncs a stream it cannot continue, and
    /// the heartbeat digest catches anything that slips through.
    pub fn apply(&mut self, message: &StreamWorkReply) -> Applied {
        let mut applied = Applied {
            resync: message.resync,
            reconcile: message.resync,
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
            return applied;
        }
        if !self.live || message.epoch != self.epoch || message.seq != self.seq + 1 {
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
        applied
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
pub async fn run_work_stream<F, A>(
    client: ManagerClient<Channel>,
    attach: A,
    node_id: String,
    mirror: SharedMirror,
    on_applied: F,
) where
    F: Fn(Applied) + Send + Sync + 'static,
    A: Fn(WorkRequest) -> WorkRequest + Send + Sync + 'static,
{
    let mut backoff = STREAM_RETRY_MIN;
    loop {
        match stream_once(&client, &attach, &node_id, &mirror, &on_applied).await {
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
    on_applied: &F,
) -> Result<(), tonic::Status>
where
    F: Fn(Applied),
    A: Fn(WorkRequest) -> WorkRequest,
{
    let (last_epoch, last_seq) = mirror.lock().unwrap_or_else(|p| p.into_inner()).position();
    let (tx, rx) = tokio::sync::mpsc::channel::<StreamWorkRequest>(64);
    tx.send(StreamWorkRequest {
        frame: Some(stream_work_request::Frame::Open(StreamWorkOpen {
            node_id: node_id.to_string(),
            last_epoch,
            last_seq,
        })),
    })
    .await
    .map_err(|_| tonic::Status::cancelled("work stream request channel closed"))?;
    let request = attach(tonic::Request::new(
        tokio_stream::wrappers::ReceiverStream::new(rx),
    ));
    let mut inbound = client.clone().stream_work(request).await?.into_inner();
    while let Some(message) = inbound.message().await? {
        let applied = mirror
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply(&message);
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
        }
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
