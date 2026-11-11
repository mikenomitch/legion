//! The batched, asynchronous audit journal writer
//! (docs/audit-journal.md, "Durability").
//!
//! The governance layer hands each record to a bounded queue and returns;
//! a writer task flushes the queue every `governance.audit.flushInterval`
//! (250 ms) or every [`BATCH`] records as ONE replicated entry, so journaling
//! adds no raft round trip to the request it records. The cost is a window:
//! a manager that crashes loses the records it had not flushed yet (at most
//! one interval's worth), and a full queue drops records (counted on
//! `legion_governance_events_total{event="audit_dropped"}`).
//!
//! A raft manager that leads no lane cannot write the journal itself (the
//! table is commutative, but every insert still rides some lane's log). The
//! requests that END on such a manager are the ones it refuses itself —
//! denials and validation errors, since writes are forwarded to the leader
//! and journaled there — so it forwards their batch to the status-lane
//! leader (`ForwardableLaneWrite::AppendAudit`), which journals it. Only
//! when that forward cannot complete (no leader known, unreachable) do the
//! records go to the `audit` tracing target instead, counted as
//! `audit_unjournaled`.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::prelude::*;
use crate::storage::governance_store::AuditRecord;
use crate::storage::store::Store;
use crate::structs::governance::GovernanceConfig;

/// Records per replicated batch.
const BATCH: usize = 256;
/// Queue bound; beyond it records are dropped and counted.
const QUEUE: usize = 16_384;
/// How often local retention pruning runs.
const PRUNE_EVERY: Duration = Duration::from_secs(300);
/// How long approval requests are kept past their expiry.
const APPROVAL_KEEP: Duration = Duration::from_secs(7 * 24 * 3600);

enum Message {
    Record(Box<AuditRecord>),
    Flush(oneshot::Sender<()>),
}

/// Handle to the journal writer.
#[derive(Clone)]
pub struct JournalHandle {
    tx: mpsc::Sender<Message>,
}

impl JournalHandle {
    /// Queues a record. Never blocks the request path.
    pub fn submit(&self, record: AuditRecord) {
        if self.tx.try_send(Message::Record(Box::new(record))).is_err() {
            super::governance_event("audit_dropped");
            warn!(target: "audit", "audit journal queue is full; a record was dropped");
        }
    }

    /// Waits until every record queued before this call is written.
    pub async fn flush(&self) {
        let (done, wait) = oneshot::channel();
        if self.tx.send(Message::Flush(done)).await.is_ok() {
            let _ = wait.await;
        }
    }
}

/// Starts the writer on the current tokio runtime.
pub fn start(store: Store, config: &GovernanceConfig) -> JournalHandle {
    let (tx, rx) = mpsc::channel(QUEUE);
    let interval = config.audit_flush_interval().max(Duration::from_millis(10));
    let retain_rows = config.audit_retain_events();
    let retain_age = config.audit_retain_age();
    tokio::spawn(run(store, rx, interval, retain_rows, retain_age));
    JournalHandle { tx }
}

async fn run(
    store: Store,
    mut rx: mpsc::Receiver<Message>,
    interval: Duration,
    retain_rows: u64,
    retain_age: Duration,
) {
    let mut buffer: Vec<AuditRecord> = Vec::new();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_prune = tokio::time::Instant::now();
    let mut parents: HashMap<String, String> = HashMap::new();
    loop {
        tokio::select! {
            message = rx.recv() => match message {
                Some(Message::Record(record)) => {
                    buffer.push(*record);
                    if buffer.len() >= BATCH {
                        write(&store, &mut buffer, &mut parents).await;
                    }
                }
                Some(Message::Flush(done)) => {
                    write(&store, &mut buffer, &mut parents).await;
                    let _ = done.send(());
                }
                None => {
                    write(&store, &mut buffer, &mut parents).await;
                    return;
                }
            },
            _ = ticker.tick() => {
                write(&store, &mut buffer, &mut parents).await;
                if last_prune.elapsed() >= PRUNE_EVERY {
                    last_prune = tokio::time::Instant::now();
                    prune(&store, retain_rows, retain_age).await;
                }
            }
        }
    }
}

async fn write(
    store: &Store,
    buffer: &mut Vec<AuditRecord>,
    parents: &mut HashMap<String, String>,
) {
    if buffer.is_empty() {
        return;
    }
    let mut batch = std::mem::take(buffer);
    // Child tokens: name the parent beside the accessor.
    for record in &mut batch {
        if record.accessor_id.is_empty() {
            continue;
        }
        if !parents.contains_key(&record.accessor_id) {
            if parents.len() > 10_000 {
                parents.clear();
            }
            let parent = store
                .get_token_meta(&record.accessor_id)
                .ok()
                .flatten()
                .map(|meta| meta.parent_accessor_id)
                .unwrap_or_default();
            parents.insert(record.accessor_id.clone(), parent);
        }
        record.parent_accessor_id = parents[&record.accessor_id].clone();
    }
    let count = batch.len() as u64;
    // A manager leading no lane forwards the batch to a lane leader; kept
    // so the records can still be logged if the forward does not complete.
    let forwarding = (!store.can_write_audit()).then(|| batch.clone());
    let store = store.clone();
    let result = tokio::task::spawn_blocking(move || store.append_audit_events(batch)).await;
    match result {
        Ok(Ok(())) => crate::manager::telemetry::metrics()
            .governance_events_total
            .inc_by(&["audit_written"], count),
        Ok(Err(err)) if forwarding.is_some() => {
            debug!(target: "audit", error = %err, records = count, "audit batch could not be forwarded to a lane leader");
            for record in forwarding.iter().flatten() {
                info!(
                    target: "audit",
                    operation = %record.operation,
                    principal = %record.principal,
                    accessor_id = %record.accessor_id,
                    target = %record.target,
                    outcome = %record.outcome,
                    "audit record (this manager leads no raft lane and could not reach a lane \
                     leader, so it is not journaled)"
                );
            }
            crate::manager::telemetry::metrics()
                .governance_events_total
                .inc_by(&["audit_unjournaled"], count);
        }
        Ok(Err(err)) => {
            warn!(target: "audit", error = %err, records = count, "audit batch could not be journaled");
            crate::manager::telemetry::metrics()
                .governance_events_total
                .inc_by(&["audit_dropped"], count);
        }
        Err(err) => warn!(target: "audit", error = %err, "audit writer task failed"),
    }
}

async fn prune(store: &Store, retain_rows: u64, retain_age: Duration) {
    let store = store.clone();
    let _ = tokio::task::spawn_blocking(move || {
        let now = super::now_ms();
        if let Err(err) =
            store.prune_audit_events_local(retain_rows, now - retain_age.as_millis() as i64)
        {
            debug!(error = %err, "audit journal prune failed");
        }
        if store.is_raft_leader(crate::storage::Lane::Control) {
            if let Err(err) = store.prune_approvals(now - APPROVAL_KEEP.as_millis() as i64) {
                debug!(error = %err, "approval prune failed");
            }
        }
    })
    .await;
}
