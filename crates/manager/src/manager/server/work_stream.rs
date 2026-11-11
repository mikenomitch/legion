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
//!
//! Endpoint sets that changed for a service the node consumes go out as
//! separate push-only messages (mesh-control-path plan §6.1,
//! [`super::endpoint_feed::endpoint_push`]), woken by the endpoint index
//! rather than the work log, outside the node's sequence.

use super::*;
use crate::storage::revisions::Revision;
use crate::storage::work_log::{NodeWorkLog, PendingWork, WorkLog};
use crate::structs::{stream_work_request, StopNotice, StreamWorkReply, StreamWorkRequest};

/// How long a resuming stream waits for this manager's replica to catch up
/// to the worker's revision before it gives up and resyncs (a worker that
/// last streamed from the old leader can be a few entries ahead of a
/// follower).
const RESUME_CATCH_UP: std::time::Duration = std::time::Duration::from_secs(5);

/// Why each of `gone` left the node's desired set: the stop notice its
/// removal recorded, when there is one.
fn stop_notices_for(store: &Store, node_id: &str, gone: Vec<i64>) -> Vec<StopNotice> {
    if gone.is_empty() {
        return Vec::new();
    }
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
}

/// The payloads of `ids` still desired on `node_id`, for a heartbeat feed
/// that needs payloads a resume did not resend
/// ([`WorkLog::held_payloads`]). Best effort: an allocation that cannot be
/// read is left out, as it would be from the desired set.
pub(super) fn load_held_payloads(
    store: &Store,
    node_id: &str,
    ids: &[i64],
) -> Vec<crate::structs::TaskAllocation> {
    let mut task_cache = DesiredTaskCache::default();
    ids.iter()
        .filter_map(|id| store.get_allocation_by_id(*id).ok().flatten())
        .filter(|allocation| {
            allocation.node_id == node_id
                && crate::constants::allocation_status_is_dispatchable(&allocation.status)
        })
        .filter_map(|allocation| desired_payload(store, &allocation, &mut task_cache).ok())
        .collect()
}

/// Messages buffered toward one worker before the sender waits for it.
const WORK_STREAM_BUFFER: usize = 64;

/// Pause after a failed store read before the resync it queued is built.
/// How often a stream looks again at the owners of its node's child
/// sessions (docs/child-sessions.md) when nothing else woke it. Only runs
/// while some session in the index carries an owner.
const OWNER_NOTICE_TICK: std::time::Duration = std::time::Duration::from_secs(1);

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
    // The revision this message reflects, and the work it carries, taken
    // together under the journal's publish lock: every change at or below
    // the revision is either in this work or was sent before.
    let pending = {
        let hub = store.revisions();
        let _publishing = hub.publishing();
        WorkLog::note_revision(log, hub.recorded().render());
        WorkLog::take_pending(log, session)
    };
    let (resync, upserts, stops) = match pending {
        PendingWork::Nothing => (false, Vec::new(), Vec::new()),
        PendingWork::Resume { from, changed } => {
            // What changed on the node since the worker's revision: upsert
            // what is still desired, stop the rest. The worker holds the
            // node's set as of that revision, so this brings it to now.
            let desired: Vec<crate::structs::Allocation> = store
                .allocs_for_node(node_id)?
                .into_iter()
                .filter(|allocation| {
                    crate::constants::allocation_status_is_dispatchable(&allocation.status)
                })
                .collect();
            let desired_ids: BTreeSet<i64> =
                desired.iter().map(|allocation| allocation.id).collect();
            let mut task_cache = DesiredTaskCache::default();
            let mut upserts = Vec::new();
            for allocation in &desired {
                if changed.contains(&allocation.id) {
                    upserts.push(desired_payload(store, allocation, &mut task_cache)?);
                }
            }
            let gone: Vec<i64> = changed
                .iter()
                .filter(|id| !desired_ids.contains(id))
                .copied()
                .collect();
            let stops = stop_notices_for(store, node_id, gone);
            let checkpoints = if WorkLog::take_checkpoint_work(log) {
                WorkLog::unpushed_checkpoints(
                    log,
                    snapshot_rpcs::build_checkpoint_requests(store, node_id),
                )
            } else {
                Vec::new()
            };
            return Ok(Some(work_log.commit_resume(
                log,
                from,
                upserts,
                stops,
                desired_ids,
                checkpoints,
            )));
        }
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
            (false, upserts, stop_notices_for(store, node_id, gone))
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
        let credential = self.stream_credential(&request, &identity)?;
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
        // A worker from another epoch (a restart, a failover, another
        // manager) that names its last revision can resume instead of
        // resyncing (revisions-and-watches plan §7). Let this replica catch
        // up to that revision first: a worker that last streamed from the
        // old leader can be a few entries ahead of a follower.
        let resume_from = (open.last_epoch != work_log.epoch())
            .then(|| Revision::parse(&open.last_revision).ok())
            .flatten();
        let resume_from = match resume_from {
            Some(revision) if store.wait_for_revision(&revision, RESUME_CATCH_UP).await => {
                Some(revision)
            }
            Some(revision) => {
                debug!(
                    node_id = %node_id,
                    revision = %revision,
                    "This manager did not catch up to the worker's revision; resyncing"
                );
                None
            }
            None => None,
        };
        let attached = {
            let hub = store.revisions();
            let _publishing = hub.publishing();
            work_log.attach_resuming(&node_id, open.last_epoch, open.last_seq, || {
                let revision = resume_from?;
                hub.node_changes_since(&node_id, &revision)
                    .map(|changed| (open.last_revision.clone(), changed))
            })
        };
        let (tx, rx) = tokio::sync::mpsc::channel(WORK_STREAM_BUFFER);
        info!(
            node_id = %node_id,
            replay = attached.replay.len(),
            last_seq = open.last_seq,
            last_revision = %open.last_revision,
            "Work stream attached"
        );

        let endpoint_wake = store.endpoint_index().node_wake(&node_id);
        // The runtime disables (docs/runtime-disable.md): pushed when the
        // stream opens while any is in force, and again whenever this
        // replica's set changes. A stream that opens with nothing disabled
        // gets no push; the heartbeat reply carries the (empty) set.
        let mut runtime_states = store.runtime_state_watch();
        tokio::spawn(async move {
            let log = attached.log;
            let session = attached.session;
            // Ahead of the replay: the node must hold a disable before it
            // is handed work it could start.
            let disables = store.runtime_disables();
            if !disables.is_empty() {
                let push = StreamWorkReply {
                    epoch: store.work_log().epoch(),
                    push_only: true,
                    runtime_states: Some(disables.to_proto()),
                    ..Default::default()
                };
                if tx.send(Ok(push)).await.is_err() {
                    work_log.detach(&node_id, session);
                    return;
                }
            }
            for message in attached.replay {
                if tx.send(Ok(message)).await.is_err() {
                    work_log.detach(&node_id, session);
                    return;
                }
            }
            let mut reauth = tokio::time::interval(credential.interval);
            reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            reauth.tick().await; // the first tick fires immediately
                                 // Set when the wait below saw the set of runtime disables
                                 // change (awaiting the change marks it seen).
            let mut runtime_states_changed = false;
            // What was last pushed about the owners of this node's child
            // sessions, so an unchanged set is not pushed again (the
            // heartbeat reply repeats it until the node acts).
            let mut last_owner_notices: Vec<crate::structs::OwnerNotice> = Vec::new();
            let mut owner_tick = tokio::time::interval(OWNER_NOTICE_TICK);
            owner_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                if !WorkLog::is_current(&log, session) {
                    // A newer stream for the node took over.
                    break;
                }
                // A changed set of runtime disables, ahead of any work the
                // node could start: push-only, outside the sequence, never
                // replayed.
                if std::mem::take(&mut runtime_states_changed)
                    || runtime_states.has_changed().unwrap_or(false)
                {
                    runtime_states.borrow_and_update();
                    let push = StreamWorkReply {
                        epoch: store.work_log().epoch(),
                        push_only: true,
                        runtime_states: Some(store.runtime_disables().to_proto()),
                        ..Default::default()
                    };
                    if tx.send(Ok(push)).await.is_err() {
                        break;
                    }
                }
                // Owner notices for this node's child sessions
                // (docs/child-sessions.md): pushed whenever the stream
                // wakes, which any change to the node's own work does, and
                // on the tick below for owners on other nodes. Only a node
                // that reports a child is ever looked at.
                if crate::manager::children::node_has_children(&store, &node_id) {
                    let owner_notices = crate::manager::children::owner_notices(&store, &node_id);
                    if !owner_notices.is_empty() && owner_notices != last_owner_notices {
                        let push = StreamWorkReply {
                            epoch: store.work_log().epoch(),
                            push_only: true,
                            owner_notices: owner_notices.clone(),
                            ..Default::default()
                        };
                        if tx.send(Ok(push)).await.is_err() {
                            break;
                        }
                    }
                    last_owner_notices = owner_notices;
                }
                // Changed endpoint sets first (mesh-control-path plan §6.1):
                // a backend that went away should leave the node's lists
                // before anything else is built. Push-only, outside the
                // sequence, never replayed.
                if store.endpoint_index().has_pending(&node_id) {
                    let pushed = {
                        let store = store.clone();
                        let node_id = node_id.clone();
                        tokio::task::spawn_blocking(move || {
                            super::endpoint_feed::endpoint_push(&store, &node_id)
                        })
                        .await
                    };
                    if let Ok(Some(push)) = pushed {
                        if tx.send(Ok(push)).await.is_err() {
                            break;
                        }
                    }
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
                    _ = endpoint_wake.notified() => {}
                    // Owners on other nodes change without waking this
                    // node's log; a node with no child never takes this.
                    _ = owner_tick.tick(), if store.sessions().any_owned() => {}
                    // A closed channel means the store is going away.
                    changed = runtime_states.changed() => match changed {
                        Ok(()) => runtime_states_changed = true,
                        Err(_) => break,
                    },
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
