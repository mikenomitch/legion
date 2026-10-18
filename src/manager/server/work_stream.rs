//! `Manager.StreamWork`: sequenced delivery of work to workers
//! (sandbox-scaling plan §6.13, Phase 2; docs/architecture-managers-workers.md,
//! "Delivery").
//!
//! The log itself — sequence numbers, the dirty set, what the worker holds,
//! the replay buffer — is [`crate::storage::work_log::WorkLog`]. This module
//! turns its pending work into messages: it reads the changed allocations
//! from the store when a message is sent, builds each payload with the same
//! [`super::desired_payload`] the heartbeat and the push use, and streams
//! the result. A message also pushes the node's checkpoint work that became
//! outstanding since the last one (checkpoints-on-stream plan), built by the
//! same `build_checkpoint_requests` the heartbeat reply uses.

use super::*;
use crate::storage::work_log::{NodeWorkLog, PendingWork, WorkLog};
use crate::structs::{stream_work_request, StopNotice, StreamWorkReply, StreamWorkRequest};

/// Messages buffered toward one worker before the sender waits for it.
const WORK_STREAM_BUFFER: usize = 64;

/// Pause after a failed store read before the resync it queued is built.
const WORK_STREAM_RETRY: std::time::Duration = std::time::Duration::from_millis(100);

/// Build the next message for `node_id` from its log, or `None` when there
/// is nothing to send. Blocking: it reads the store.
pub(super) fn next_work_message(
    store: &Store,
    work_log: &WorkLog,
    log: &NodeWorkLog,
    session: u64,
    node_id: &str,
) -> R<Option<StreamWorkReply>> {
    let (resync, upserts, stops) = match WorkLog::take_pending(log, session) {
        PendingWork::Nothing => (false, Vec::new(), Vec::new()),
        PendingWork::Resync => (
            true,
            build_desired_allocations(store, node_id, &[])?,
            Vec::new(),
        ),
        PendingWork::Changed(ids) => {
            let mut task_cache = DesiredTaskCache::default();
            let mut upserts = Vec::new();
            let mut gone = Vec::new();
            for allocation_id in ids {
                let still_desired =
                    store
                        .get_allocation_by_id(allocation_id)?
                        .filter(|allocation| {
                            allocation.node_id == node_id
                                && crate::constants::allocation_status_is_dispatchable(
                                    &allocation.status,
                                )
                        });
                match still_desired {
                    Some(allocation) => {
                        upserts.push(desired_payload(store, &allocation, &mut task_cache)?);
                    }
                    None if WorkLog::holds(log, allocation_id) => gone.push(allocation_id),
                    // Never in this worker's desired set (a held reservation,
                    // an allocation that left before it was ever sent).
                    None => {}
                }
            }
            let stops = if gone.is_empty() {
                Vec::new()
            } else {
                // Why each one went: the stop notice its removal recorded,
                // when there is one.
                let notices: HashMap<i64, StopNotice> = store
                    .stop_notices_for_node(node_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|notice| {
                        (
                            notice.allocation_id,
                            StopNotice {
                                allocation_id: notice.allocation_id,
                                cause: notice.cause.as_str().to_string(),
                                preemptor_job: notice.preemptor_job,
                                preemptor_band: notice.preemptor_band,
                            },
                        )
                    })
                    .collect();
                gone.into_iter()
                    .map(|allocation_id| {
                        notices.get(&allocation_id).cloned().unwrap_or(StopNotice {
                            allocation_id,
                            ..Default::default()
                        })
                    })
                    .collect()
            };
            (false, upserts, stops)
        }
    };
    // Checkpoint work the node has not been pushed yet (checkpoints-on-stream
    // plan), read only when the projection saw the node's snapshot rows
    // move, a warm was queued, or the stream is new. Taken after the
    // allocation half, so a failed read above leaves the mark for the
    // retry. The same builder as the heartbeat reply, so the same requests
    // and part keys; it never fails.
    let checkpoints = if WorkLog::take_checkpoint_work(log) {
        WorkLog::unpushed_checkpoints(
            log,
            snapshot_rpcs::build_checkpoint_requests(store, node_id),
        )
    } else {
        Vec::new()
    };
    Ok(work_log.commit_message(log, resync, upserts, stops, checkpoints))
}

impl ManagerDaemonService {
    /// The `StreamWork` handler: attach the worker's stream to its node's
    /// log, replay or resync, then send each new message as the log gains
    /// work, until the worker goes away or its credential stops being
    /// valid.
    pub(super) async fn stream_work_rpc(
        &self,
        request: Request<tonic::Streaming<StreamWorkRequest>>,
    ) -> Result<Response<ReceiverStream<Result<StreamWorkReply, Status>>>, Status> {
        // ACL: worker-plane endpoint. Requires the node token minted at
        // registration (or a management token), bound to the node named in
        // the first frame.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let credential = self.stream_credential(&request);
        let mut inbound = request.into_inner();
        let open = match inbound.message().await? {
            Some(StreamWorkRequest {
                frame: Some(stream_work_request::Frame::Open(open)),
            }) => open,
            _ => {
                return Err(Status::invalid_argument(
                    "the first StreamWork frame must be `open`",
                ))
            }
        };
        if open.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("StreamWork open names no node"));
        }
        acl::enforce_node_binding(&identity, &open.node_id)?;

        let store = self.store.clone();
        let work_log = store.work_log();
        let node_id = open.node_id.clone();
        let attached = work_log.attach(&node_id, open.last_epoch, open.last_seq);
        let (tx, rx) = tokio::sync::mpsc::channel(WORK_STREAM_BUFFER);
        info!(
            node_id = %node_id,
            replay = attached.replay.len(),
            last_seq = open.last_seq,
            "Work stream attached"
        );

        tokio::spawn(async move {
            let log = attached.log;
            let session = attached.session;
            for message in attached.replay {
                if tx.send(Ok(message)).await.is_err() {
                    work_log.detach(&node_id, session);
                    return;
                }
            }
            let mut reauth = tokio::time::interval(credential.interval);
            reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            reauth.tick().await; // the first tick fires immediately
            loop {
                if !WorkLog::is_current(&log, session) {
                    // A newer stream for the node took over.
                    break;
                }
                // Send everything pending before waiting again. A change that
                // lands while a message is being built re-arms the wake, so
                // nothing noted after `take_pending` can be missed.
                let built = {
                    let store = store.clone();
                    let work_log = work_log.clone();
                    let log = log.clone();
                    let node_id = node_id.clone();
                    tokio::task::spawn_blocking(move || {
                        next_work_message(&store, &work_log, &log, session, &node_id)
                    })
                    .await
                };
                match built {
                    Ok(Ok(Some(message))) => {
                        if tx.send(Ok(message)).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(err)) => {
                        // A store read failed and the changes it was reading
                        // are gone from the log, so the worker gets a full
                        // resync on the next wake.
                        warn!(node_id = %node_id, error = ?err, "Building a work message failed; resyncing");
                        WorkLog::request_resync(&log);
                        // Not in a tight loop while the store is failing.
                        tokio::time::sleep(WORK_STREAM_RETRY).await;
                    }
                    Err(join_error) => {
                        warn!(node_id = %node_id, error = ?join_error, "Work message task failed");
                        break;
                    }
                }
                tokio::select! {
                    _ = log.wake.notified() => {}
                    frame = inbound.message() => match frame {
                        Ok(Some(StreamWorkRequest {
                            frame: Some(stream_work_request::Frame::Ack(ack)),
                        })) => work_log.ack(&log, ack.epoch, ack.seq),
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => break,
                    },
                    _ = tx.closed() => break,
                    _ = reauth.tick() => {
                        let still_allowed = credential
                            .reauthenticate(&store)
                            .and_then(|identity| {
                                acl::enforce_worker_plane(&identity)?;
                                acl::enforce_node_binding(&identity, &node_id)
                            });
                        if let Err(status) = still_allowed {
                            let _ = tx.send(Err(status)).await;
                            break;
                        }
                    }
                }
            }
            work_log.detach(&node_id, session);
            debug!(node_id = %node_id, "Work stream detached");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
