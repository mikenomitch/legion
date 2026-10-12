//! Blocked evals, the store's half (sandbox-scaling plan §6.15, Phase 3;
//! docs/scheduler-and-eval-loop.md, "Blocked evals").
//!
//! An eval whose plan found no room ([`PlacementBlocked`]) is written
//! `status = 'blocked'` with its key and detail, in the settle entry that
//! used to mark it `failed` ([`EvalCompletion::Blocked`]), and any older
//! blocked eval of the same job goes in the same write: one blocked eval per
//! job, the newest.
//!
//! **Capacity events** are what can wake one: a node registering, a node's
//! pool, availability, or attributes changing, a node becoming ready, and
//! allocations leaving a node. The changeset projection records them on
//! every replica ([`Store::note_capacity_events`]) in a short in-memory
//! log, numbered. Only the scheduler-lane leader acts on them:
//!
//! - **The wake pass** ([`Store::wake_blocked_evals`]) takes the events it
//!   has not handled, and returns to `pending` every blocked eval that one of
//!   their nodes could help ([`crate::manager::blocked::node_could_help`]),
//!   in one scheduler-lane entry. Nothing else wakes a blocked eval, so a
//!   node joining one pool re-plans only the jobs blocked on that pool's
//!   class, plus escaped ones.
//! - **Missed unblocks.** A claim notes the log's position. An eval that
//!   then blocks checks the events after it first: capacity that appeared
//!   while its plan ran, on a node that could help, sends it straight back
//!   to `pending` instead ([`Store::completion_for_blocked`]), so an unblock
//!   during the job's own eval is never lost.
//! - **Leader change.** The blocked rows are replicated; the log is not, but
//!   every replica keeps its own copy. A manager that takes the scheduler
//!   lane replays the last [`CAPACITY_REPLAY_WINDOW`] of it
//!   ([`Store::begin_blocked_wake_leadership`]), which covers whatever the
//!   old leader had not acted on when it stopped.
//!
//! The index from key to blocked eval is the rows themselves (`status =
//! 'blocked'`), read only when a pass has events to match: at most one row
//! per job, so the read is small, and there is no second copy to keep
//! consistent across a failover.

use super::*;
use crate::manager::blocked::{key_kind, node_could_help, PlacementBlocked};
use std::collections::VecDeque;

/// Capacity events every replica keeps, newest last.
const CAPACITY_EVENTS_MAX: usize = 8192;

/// How long a capacity event is kept, so a new leader can replay it.
const CAPACITY_EVENTS_TTL: Duration = Duration::from_secs(300);

/// How far back a manager that just took the scheduler lane replays
/// capacity events: far longer than any wake pass the old leader could have
/// left unfinished.
pub const CAPACITY_REPLAY_WINDOW: Duration = Duration::from_secs(60);

/// One change that may have made room.
#[derive(Debug, Clone)]
struct CapacityEvent {
    seq: u64,
    at: Instant,
    node_id: String,
    /// `register`, `node`, `ready`, or `freed`.
    kind: &'static str,
}

/// The capacity-event log and the wake pass's position in it.
#[derive(Debug, Default)]
pub(crate) struct CapacityEvents {
    last_seq: u64,
    recent: VecDeque<CapacityEvent>,
    /// Every event up to here has been matched against the blocked evals.
    processed: u64,
}

/// One blocked eval, as its row records it.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockedEval {
    pub id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub key: String,
    /// `{task, taskConfigId, cpu, memory, pinnedNode, message}`.
    pub detail: Value,
    /// RFC 3339.
    pub blocked_at: String,
}

impl BlockedEval {
    /// The `Job.blocked` block for this eval.
    pub fn to_job_blocked(&self) -> crate::structs::JobBlocked {
        let field = |name: &str| self.detail.get(name).filter(|value| !value.is_null());
        let mut shortfall = std::collections::HashMap::new();
        if let Some(cpu) = field("cpu") {
            shortfall.insert("cpu".to_string(), format!("{}MHz", plain(cpu)));
        }
        if let Some(memory) = field("memory") {
            shortfall.insert("memory".to_string(), format!("{}MiB", plain(memory)));
        }
        crate::structs::JobBlocked {
            key: self.key.clone(),
            task: field("task").map(plain).unwrap_or_default(),
            shortfall,
            message: field("message").map(plain).unwrap_or_default(),
            since: OffsetDateTime::parse(&self.blocked_at, &Rfc3339)
                .map(|at| at.unix_timestamp())
                .unwrap_or_default(),
            eval_id: self.id.clone(),
        }
    }
}

/// A JSON scalar as text, without a string's quotes.
fn plain(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

impl Store {
    fn lock_capacity_events(&self) -> std::sync::MutexGuard<'_, CapacityEvents> {
        self.capacity_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record capacity events from a committed changeset. Every replica
    /// keeps them; the scheduler-lane leader's wake pass acts on them.
    pub(crate) fn note_capacity_events(&self, events: Vec<(String, &'static str)>) {
        if events.is_empty() {
            return;
        }
        let now = Instant::now();
        {
            let mut log = self.lock_capacity_events();
            for (node_id, kind) in events {
                log.last_seq += 1;
                let seq = log.last_seq;
                log.recent.push_back(CapacityEvent {
                    seq,
                    at: now,
                    node_id,
                    kind,
                });
            }
            while log.recent.len() > CAPACITY_EVENTS_MAX
                || log
                    .recent
                    .front()
                    .is_some_and(|event| now.duration_since(event.at) > CAPACITY_EVENTS_TTL)
            {
                log.recent.pop_front();
            }
        }
        self.capacity_notify.notify_one();
    }

    /// The capacity log's position: a claim records it so a blocking eval
    /// can see what changed while it ran.
    pub(crate) fn capacity_seq(&self) -> u64 {
        self.lock_capacity_events().last_seq
    }

    /// Woken whenever a capacity event is recorded.
    pub fn capacity_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.capacity_notify)
    }

    /// The distinct nodes with capacity events after `seq`, each with the
    /// kind of its first such event, in order.
    fn capacity_nodes_after(&self, seq: u64) -> Vec<(String, &'static str)> {
        let log = self.lock_capacity_events();
        let mut seen = HashSet::new();
        log.recent
            .iter()
            .filter(|event| event.seq > seq)
            .filter(|event| seen.insert(event.node_id.clone()))
            .map(|event| (event.node_id.clone(), event.kind))
            .collect()
    }

    /// This manager just took the scheduler lane (or started on it): replay
    /// the capacity events of the last [`CAPACITY_REPLAY_WINDOW`], which
    /// covers any wake the old leader had not made.
    pub fn begin_blocked_wake_leadership(&self) {
        let now = Instant::now();
        let mut log = self.lock_capacity_events();
        let first_recent = log
            .recent
            .iter()
            .find(|event| now.duration_since(event.at) <= CAPACITY_REPLAY_WINDOW)
            .map(|event| event.seq);
        log.processed = match first_recent {
            Some(seq) => seq - 1,
            None => log.last_seq,
        };
    }

    /// Every blocked eval, oldest first.
    pub fn blocked_evals(&self) -> R<Vec<BlockedEval>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT id, job_id, job_config_id, blocked_key, blocked_detail, blocked_at
             FROM eval WHERE status = 'blocked' ORDER BY id;",
        )?;
        let rows = stmt.query_map([], |row| {
            let detail: Option<String> = row.get(4)?;
            Ok(BlockedEval {
                id: row.get::<_, i64>(0)?.to_string(),
                job_id: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                job_config_id: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                key: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                detail: detail
                    .and_then(|raw| serde_json::from_str(&raw).ok())
                    .unwrap_or(Value::Null),
                blocked_at: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
            })
        })?;
        Self::collect_rows(rows)
    }

    /// The blocked eval of each job that has one (`job status`, `job list`).
    pub fn blocked_evals_by_job(&self) -> R<HashMap<String, BlockedEval>> {
        Ok(self
            .blocked_evals()?
            .into_iter()
            .filter(|blocked| !blocked.job_id.is_empty())
            .map(|blocked| (blocked.job_id.clone(), blocked))
            .collect())
    }

    /// Report `status: blocked` and what each job waits for on the jobs
    /// that have a blocked eval and are neither stopped nor failed. A read
    /// overlay: the job row keeps its stored status.
    pub fn overlay_blocked_jobs(&self, jobs: &mut [crate::structs::Job]) -> R<()> {
        let blocked = self.blocked_evals_by_job()?;
        if blocked.is_empty() {
            return Ok(());
        }
        for job in jobs {
            if matches!(job.status.as_str(), "stopped" | "failed") {
                continue;
            }
            if let Some(eval) = blocked.get(&job.id) {
                job.status = "blocked".to_string();
                job.blocked = Some(eval.to_job_blocked());
            }
        }
        Ok(())
    }

    /// The completion for an eval whose plan was blocked: `Blocked`, unless
    /// capacity a node could have helped with appeared while it ran, in which
    /// case it goes straight back to `pending` (a missed unblock).
    pub fn completion_for_blocked(
        &self,
        eval: &Eval,
        blocked: &PlacementBlocked,
        event: EventDraft,
    ) -> EvalCompletion {
        let since = self.claim_capacity_seq(&eval.id);
        if let Some((node_id, cause)) = self.first_helpful_node(
            &blocked.key,
            &eval.job_config_id,
            &blocked.detail,
            &self.capacity_nodes_after(since),
        ) {
            info!(
                eval_id = %eval.id,
                key = %blocked.key,
                node_id = %node_id,
                cause,
                "Capacity appeared while the eval ran; requeueing instead of blocking"
            );
            crate::manager::telemetry::metrics()
                .evals_unblocked_total
                .inc(&["missed"]);
            return EvalCompletion::Released {
                delay: Duration::ZERO,
                event: Some(crate::storage::events::drafts::eval_unblocked(
                    &eval.id,
                    &eval.job_id,
                    &blocked.key,
                    "missed",
                    &node_id,
                )),
            };
        }
        EvalCompletion::Blocked {
            key: blocked.key.clone(),
            detail: blocked.detail.clone(),
            event,
        }
    }

    /// The first of `nodes` (with the kind of its event) that is available
    /// and that a blocked eval with this key and detail could use.
    fn first_helpful_node(
        &self,
        key: &str,
        job_config_id: &str,
        detail: &Value,
        nodes: &[(String, &'static str)],
    ) -> Option<(String, &'static str)> {
        if nodes.is_empty() {
            return None;
        }
        let available = self.available_nodes().ok()?;
        let task = self.blocked_task(job_config_id, detail);
        nodes.iter().find_map(|(node_id, kind)| {
            let node = available.get(node_id)?;
            node_could_help(key, task.as_ref(), node).then(|| (node_id.clone(), *kind))
        })
    }

    /// The task a blocked eval could not place, from the read model.
    fn blocked_task(
        &self,
        job_config_id: &str,
        detail: &Value,
    ) -> Option<crate::structs::job::TaskConfig> {
        let task_config_id = detail.get("taskConfigId").and_then(Value::as_i64)?;
        self.cached_task_configs_for_job_config(job_config_id)
            .ok()?
            .into_iter()
            .find(|(id, _)| *id == task_config_id)
            .map(|(_, task)| task)
    }

    /// Write a handled eval `blocked` (inside the settle's write), replacing
    /// any older blocked eval of the same job.
    pub(crate) fn write_blocked_eval(
        &self,
        eval_id: &str,
        key: &str,
        detail: &Value,
        event: EventDraft,
    ) -> R<()> {
        let mut conn: MutexGuard<'_, Connection> = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.prepare_cached(
            "DELETE FROM eval
             WHERE status = 'blocked' AND id != ?1
               AND COALESCE(job_id, '') != ''
               AND job_id = (SELECT job_id FROM eval WHERE id = ?1);",
        )?
        .execute([eval_id])?;
        tx.prepare_cached(
            "UPDATE eval
             SET status = 'blocked', blocked_key = ?2, blocked_detail = ?3, blocked_at = ?4
             WHERE id = ?1;",
        )?
        .execute(params![
            eval_id,
            key,
            serde_json::to_string(detail)?,
            Self::sortable_now()?
        ])?;
        let staged = self.stage_event_locked(&tx, event);
        tx.commit()?;
        self.publish_staged(staged);
        Ok(())
    }

    /// A job's evals settled its desired state: its blocked eval, if any, is
    /// waiting for nothing.
    pub(crate) fn delete_job_blocked_evals_tx(conn: &Connection, eval_id: &str) -> R<()> {
        conn.prepare_cached(
            "DELETE FROM eval
             WHERE status = 'blocked' AND id != ?1
               AND COALESCE(job_id, '') != ''
               AND job_id = (SELECT job_id FROM eval WHERE id = ?1);",
        )?
        .execute([eval_id])?;
        Ok(())
    }

    /// The wake pass (leader only): match the capacity events not yet
    /// handled against every blocked eval, and return the ones a node could
    /// help to `pending` in one scheduler-lane entry. Returns how many woke.
    #[instrument(err, skip_all)]
    pub fn wake_blocked_evals(&self) -> R<usize> {
        let (processed, upto) = {
            let log = self.lock_capacity_events();
            (log.processed, log.last_seq)
        };
        if upto == processed {
            return Ok(0);
        }
        let nodes = self.capacity_nodes_after(processed);
        let blocked = self.blocked_evals()?;
        let mut woken: Vec<(BlockedEval, String, &'static str)> = Vec::new();
        for eval in &blocked {
            if let Some((node_id, cause)) =
                self.first_helpful_node(&eval.key, &eval.job_config_id, &eval.detail, &nodes)
            {
                woken.push((eval.clone(), node_id, cause));
            }
        }
        if !woken.is_empty() {
            let wakes: Vec<(String, String, String, &'static str, String)> = woken
                .iter()
                .map(|(eval, node_id, cause)| {
                    (
                        eval.id.clone(),
                        eval.job_id.clone(),
                        eval.key.clone(),
                        *cause,
                        node_id.clone(),
                    )
                })
                .collect();
            self.replicated(Lane::Scheduler, "unblock_evals", move |s| {
                s.unblock_evals_apply(wakes)
            })?;
            let now = OffsetDateTime::now_utc();
            let metrics = crate::manager::telemetry::metrics();
            for (eval, node_id, cause) in &woken {
                metrics.evals_unblocked_total.inc(&[cause]);
                if let Ok(at) = OffsetDateTime::parse(&eval.blocked_at, &Rfc3339) {
                    metrics
                        .blocked_wait_seconds
                        .observe((now - at).as_seconds_f64().max(0.0));
                }
                info!(
                    eval_id = %eval.id,
                    job_id = %eval.job_id,
                    key = %eval.key,
                    node_id = %node_id,
                    cause,
                    "Unblocked eval"
                );
            }
        }
        self.lock_capacity_events().processed = upto;
        self.publish_blocked_gauge(&blocked, woken.len());
        Ok(woken.len())
    }

    /// Return every blocked eval to `pending` (cause `startup`): run once
    /// when the manager process starts as the scheduler's writer. Its
    /// capacity log starts empty, so it cannot tell what changed while it
    /// was down or between a commit and the wake pass a crash cut short; one
    /// re-plan per blocked job is the price of never stranding one.
    #[instrument(err, skip_all)]
    pub fn wake_all_blocked_evals(&self) -> R<usize> {
        let blocked = self.blocked_evals()?;
        if blocked.is_empty() {
            return Ok(0);
        }
        let wakes: Vec<(String, String, String, &'static str, String)> = blocked
            .iter()
            .map(|eval| {
                (
                    eval.id.clone(),
                    eval.job_id.clone(),
                    eval.key.clone(),
                    "startup",
                    String::new(),
                )
            })
            .collect();
        self.replicated(Lane::Scheduler, "unblock_evals", move |s| {
            s.unblock_evals_apply(wakes)
        })?;
        crate::manager::telemetry::metrics()
            .evals_unblocked_total
            .inc_by(&["startup"], blocked.len() as u64);
        Ok(blocked.len())
    }

    /// `legion_evals_blocked{key_kind}` after a pass that woke some of
    /// `blocked`.
    fn publish_blocked_gauge(&self, blocked: &[BlockedEval], woken: usize) {
        let mut counts: BTreeMap<&'static str, i64> = [("class", 0), ("node", 0), ("escaped", 0)]
            .into_iter()
            .collect();
        for eval in blocked {
            *counts.entry(key_kind(&eval.key)).or_default() += 1;
        }
        // The woken ones are pending now; a later pass recounts exactly.
        let mut remaining = woken as i64;
        for count in counts.values_mut() {
            let take = remaining.min(*count);
            *count -= take;
            remaining -= take;
        }
        let gauge = &crate::manager::telemetry::metrics().evals_blocked;
        for (kind, count) in counts {
            gauge.set(&[kind], count);
        }
    }

    fn unblock_evals_apply(
        &self,
        wakes: Vec<(String, String, String, &'static str, String)>,
    ) -> R<()> {
        let mut conn: MutexGuard<'_, Connection> = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let now = Self::sortable_now()?;
        let mut staged = Vec::new();
        for (eval_id, job_id, key, cause, node_id) in wakes {
            let changed = tx
                .prepare_cached(
                    "UPDATE eval
                     SET status = 'pending', ready_at = ?2,
                         blocked_key = NULL, blocked_detail = NULL, blocked_at = NULL
                     WHERE id = ?1 AND status = 'blocked';",
                )?
                .execute(params![eval_id, now])?;
            if changed > 0 {
                staged.push(self.stage_event_locked(
                    &tx,
                    crate::storage::events::drafts::eval_unblocked(
                        &eval_id, &job_id, &key, cause, &node_id,
                    ),
                ));
            }
        }
        tx.commit()?;
        for event in staged {
            self.publish_staged(event);
        }
        Ok(())
    }
}

#[cfg(test)]
mod blocked_evals_test {
    use super::*;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&crate::structs::config::ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    #[test]
    fn the_capacity_log_numbers_events_and_replays_a_recent_window() -> R<()> {
        let (_dir, store) = store()?;
        assert_eq!(store.capacity_seq(), 0);
        store.note_capacity_events(vec![("a".into(), "register"), ("b".into(), "freed")]);
        store.note_capacity_events(vec![("a".into(), "freed")]);
        assert_eq!(store.capacity_seq(), 3);
        assert_eq!(
            store.capacity_nodes_after(0),
            vec![("a".to_string(), "register"), ("b".to_string(), "freed")],
            "one entry per node, its first event's kind"
        );
        assert_eq!(
            store.capacity_nodes_after(2),
            vec![("a".to_string(), "freed")]
        );
        // Nothing processed yet; a new leader replays all of it (recent).
        store.lock_capacity_events().processed = 3;
        store.begin_blocked_wake_leadership();
        assert_eq!(store.lock_capacity_events().processed, 0);
        Ok(())
    }
}
