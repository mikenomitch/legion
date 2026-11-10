//! Eval claims as leader memory (sandbox-scaling plan, Phase 1).
//!
//! A claim used to be a scheduler-lane raft entry: `pending → in_flight`,
//! plus a sweep deleting the pending duplicates the claim subsumed. On an
//! idle cluster every placement paid it as a round of its own (under load it
//! rode the previous batch's settle). It bought one thing — a crashed
//! leader's half-handled evals were recognizable as `in_flight` and moved
//! back to `pending` by `requeue_in_flight_evals` on the next leader — and
//! that recovery is exactly what an unclaimed `pending` row gets for free:
//! the next leader runs it. So a claim is now a mark in the leader's
//! memory, the same kind of soft state as node liveness, and writes
//! nothing.
//!
//! What stays the same:
//! - **Ordering and batching.** A claim takes the head of
//!   `priority DESC, id ASC` among ready, unclaimed rows, in fair-share
//!   batches ([`EvalBatchLimits`]).
//! - **Duplicate collapse.** The claim still subsumes pending duplicates
//!   that exist when it is taken (same kind, same scope, already ready);
//!   they are held with it and deleted by its settle, in the same write.
//! - **Exclusivity.** Two eval workers never hold one eval, because claims
//!   are taken under one lock.
//! - **Recovery.** A leader that dies or steps down forgets its claims; the
//!   rows are still `pending`, so the next leader runs them, as it ran
//!   `in_flight` rows before. Handlers are idempotent reconciles, which the
//!   old recovery relied on too.
//!
//! A claimed eval's row reads `pending` in the store. [`Store::get_evals`]
//! overlays `in_flight` for the evals this manager has claimed, so listings
//! on the leader still show work in progress.
//!
//! **Claim epochs.** Forgetting every claim ([`Store::clear_eval_claims`],
//! on losing or gaining scheduler-lane leadership) does not stop a handler
//! that is already running: it finishes and settles. If leadership left
//! and came back meanwhile, its eval may have been claimed again by
//! another worker, and the old handler's settle would write over the new
//! claimant's (delete a row it still holds, mark `failed` work it is about
//! to retry, release its claim). So every clear starts a new epoch, every
//! claim records the epoch it was taken in, and the eval loop settles
//! through [`Store::complete_claimed_evals_and_claim_next`], which writes
//! an outcome only while the eval is still claimed in the epoch the
//! worker claimed it in. A stale outcome is dropped: the row is still
//! `pending`, and whoever holds it now (or the next claim) runs it, which
//! is the same recovery an abandoned batch gets.

use super::*;

/// The evals this manager's eval workers hold.
#[derive(Debug, Default)]
pub(crate) struct EvalClaims {
    /// Claimed eval id → the pending duplicates its claim subsumes.
    claimed: HashMap<String, Vec<String>>,
    /// Every id held, claimed or subsumed: what a claim must skip.
    held: HashSet<String>,
    /// The capacity log's position when each claim was taken, so an eval
    /// that blocks can see the capacity that appeared while it ran
    /// (`store/blocked_evals.rs`, "Missed unblocks").
    capacity_seq: HashMap<String, u64>,
    /// The current claim epoch: bumped by every clear (see the module docs).
    epoch: u64,
    /// The epoch each claimed eval was claimed in.
    claim_epoch: HashMap<String, u64>,
}

/// How often the O(pending) full count may refresh `legion_eval_queue_depth`.
const QUEUE_DEPTH_REFRESH_INTERVAL_MS: u64 = 1_000;

/// When the queue-depth gauge last took a full count, in [`process_millis`]
/// (0 = never). Process-wide, like the gauge it rate-limits.
static QUEUE_DEPTH_REFRESHED_AT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Milliseconds since this process first asked, for cheap rate limits.
fn process_millis() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// The head of the ready queue for a claim. `+ready_at` keeps `ready_at`
/// out of index selection: with it eligible, SQLite ranges over
/// `(status, ready_at, ...)` and sorts every ready row to find the head.
/// Without it the walk follows `(status, priority DESC, id ASC)` (or, for
/// the non-planning kinds, `idx_eval_pending_kind_order`) and stops after
/// the batch. The non-planning kind list is a compile-time constant, never
/// caller input, so it is built into the statement.
fn claim_head_sql(scope: ClaimScope) -> String {
    let kind_filter = match scope {
        ClaimScope::Any => String::new(),
        ClaimScope::NonPlanning => format!(
            " AND type IN ({})",
            crate::manager::concurrency::NON_PLANNING_EVAL_KINDS
                .iter()
                .map(|kind| format!("'{kind}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    format!(
        "SELECT id, status, type, job_id, job_config_id, node_id, priority, ready_at
         FROM eval
         WHERE status = 'pending' AND +ready_at <= ?1{kind_filter}
         ORDER BY priority DESC, id ASC
         LIMIT ?2;"
    )
}

/// The pending duplicates one claimed eval subsumes: same kind, same
/// (job, job config, node) scope, already claimable. `IS` rather than
/// `COALESCE(col, '') = ?`: every eval writer stores an absent scope as
/// NULL, never '', so `IS` matches the same rows, and unlike the COALESCE
/// it is an equality the `idx_eval_pending_scope` seek can use. The
/// COALESCE form scanned the pending set once per claimed eval.
const SUBSUME_SQL: &str = "SELECT id FROM eval
     WHERE status = 'pending'
       AND type = ?2
       AND job_id IS ?4
       AND job_config_id IS ?5
       AND node_id IS ?6
       AND +ready_at <= ?3
       AND id != ?1;";

/// Pending rows, counted no further than `?1`.
const PENDING_CAPPED_SQL: &str =
    "SELECT COUNT(*) FROM (SELECT 1 FROM eval WHERE status = 'pending' LIMIT ?1);";

/// An eval's scope column as the claim queries bind it: `""` (how
/// [`Store::eval_from_row`] reads a NULL) back to NULL.
fn scope_param(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

impl Store {
    fn lock_eval_claims(&self) -> std::sync::MutexGuard<'_, EvalClaims> {
        self.eval_claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Claim the next ready eval, if any (a batch of one).
    #[instrument(err, skip_all)]
    pub fn claim_eval(&self) -> R<Option<Eval>> {
        let one = EvalBatchLimits { max: 1, workers: 1 };
        Ok(self.claim_up_to(one, ClaimScope::Any)?.0.into_iter().next())
    }

    /// Claim a fair-share batch of ready evals ([`EvalBatchLimits`]).
    ///
    /// Batch size is a fair share of what is *actually* queued and the loop
    /// never waits for the batch to fill, so an idle cluster still claims
    /// exactly one eval. Ordering is preserved ACROSS the batch: the batch
    /// is the head of `priority DESC, id ASC`, so a lower-priority eval is
    /// only ever reached after every higher-priority one.
    ///
    /// `scope` narrows the claim to eval kinds that never invoke the planner
    /// ([`ClaimScope::NonPlanning`]): what an eval worker the
    /// adaptive-concurrency gate has throttled claims instead of blocking
    /// (docs/adaptive-scheduler-concurrency.md). Ordering among the rows it
    /// can see, and duplicate collapse, are identical.
    #[instrument(err, skip_all)]
    pub fn claim_evals(&self, limits: EvalBatchLimits, scope: ClaimScope) -> R<Vec<Eval>> {
        Ok(self.claim_up_to(limits, scope)?.0)
    }

    /// [`Self::claim_evals`], also returning the claim epoch the batch was
    /// taken in, read under the same lock as the claims themselves. Settle
    /// the batch with [`Self::complete_claimed_evals_and_claim_next`] and
    /// that epoch, so a settle that outlived a leadership flap cannot write
    /// over a newer claim of the same eval.
    #[instrument(err, skip_all)]
    pub fn claim_evals_in_epoch(
        &self,
        limits: EvalBatchLimits,
        scope: ClaimScope,
    ) -> R<(Vec<Eval>, u64)> {
        self.claim_up_to(limits, scope)
    }

    /// The claim itself. Everything here runs under the claims mutex, so
    /// every statement is bounded by the batch, never by the queue
    /// (architecture review SCH3): the count stops at the fair share's
    /// saturation point, the head query walks `priority DESC, id ASC` in
    /// index order, and the duplicate lookup is an equality seek on the
    /// pending-scope index. `EXPLAIN QUERY PLAN` for each is pinned by
    /// `eval_claims_test::claim_queries_use_their_indexes`.
    fn claim_up_to(&self, limits: EvalBatchLimits, scope: ClaimScope) -> R<(Vec<Eval>, u64)> {
        let started = Instant::now();
        let capacity_seq = self.capacity_seq();
        let mut claims = self.lock_eval_claims();
        let epoch = claims.epoch;
        let conn = self.read_connection()?;
        let now = Self::sortable_now()?;
        let held = claims.held.len() as i64;
        // The fair share stops growing once the unclaimed queue reaches
        // `workers × max`, so counting past that point buys nothing: the
        // count is exact below the cap and "at least the cap" at it.
        let cap = held.saturating_add(limits.saturation() as i64);
        let pending = Self::pending_eval_count_capped(&conn, cap)?;
        let exact = pending < cap;
        let unclaimed = (pending - held).max(0);
        if unclaimed == 0 {
            drop(claims);
            if exact {
                crate::telemetry::metrics().eval_queue_depth.set(0);
            }
            return Ok((Vec::new(), epoch));
        }
        let take = limits.share_of(unclaimed);
        let candidates: Vec<Eval> = {
            let mut stmt = conn.prepare_cached(&claim_head_sql(scope))?;
            let rows = stmt.query_map(params![now, (take + claims.held.len()) as i64], |row| {
                self.eval_from_row(row)
            })?;
            Self::collect_rows(rows)?
        };
        let mut claimed = Vec::with_capacity(take);
        for mut eval in candidates {
            if claimed.len() >= take {
                break;
            }
            if claims.held.contains(&eval.id) {
                continue;
            }
            // Collapse only work this claim genuinely subsumes (see
            // `SUBSUME_SQL`). The reasons each part matters are on the old
            // claim's sweep in the history and in
            // docs/scheduler-and-eval-loop.md ("Claims").
            let subsumed: Vec<String> = {
                let mut stmt = conn.prepare_cached(SUBSUME_SQL)?;
                let rows = stmt.query_map(
                    params![
                        eval.id,
                        eval.kind,
                        now,
                        scope_param(&eval.job_id),
                        scope_param(&eval.job_config_id),
                        scope_param(&eval.node_name)
                    ],
                    |row| row.get::<_, i64>(0).map(|id| id.to_string()),
                )?;
                Self::collect_rows(rows)?
                    .into_iter()
                    .filter(|id| !claims.held.contains(id))
                    .collect()
            };
            claims.held.insert(eval.id.clone());
            claims.held.extend(subsumed.iter().cloned());
            claims.claimed.insert(eval.id.clone(), subsumed);
            claims.capacity_seq.insert(eval.id.clone(), capacity_seq);
            claims.claim_epoch.insert(eval.id.clone(), epoch);
            eval.status = "in_flight".to_string();
            claimed.push(eval);
        }
        let held_after = claims.held.len();
        drop(claims);
        // The gauge: exact when the capped count was, otherwise a full count
        // at most once per interval, outside the claims mutex.
        if exact {
            crate::telemetry::metrics()
                .eval_queue_depth
                .set((pending - held_after as i64).max(0));
        } else {
            Self::refresh_eval_queue_depth(&conn, held_after, false)?;
        }
        if !claimed.is_empty() {
            debug!(
                claimed = claimed.len(),
                duration_ms = started.elapsed().as_millis() as u64,
                "Claimed evals"
            );
        }
        Ok((claimed, epoch))
    }

    /// Pending rows, counted no further than `cap`: exact below it, `cap`
    /// at or above it. O(min(pending, cap)) on the status index, where the
    /// full count it replaced was O(pending) on every claim.
    fn pending_eval_count_capped(conn: &Connection, cap: i64) -> R<i64> {
        Ok(conn
            .prepare_cached(PENDING_CAPPED_SQL)?
            .query_row([cap.max(0)], |row| row.get(0))?)
    }

    /// Set `legion_eval_queue_depth` from a full count of pending rows less
    /// the `held` ones this manager has claimed. The count is O(pending), so
    /// unless `force`d it runs at most once per
    /// [`QUEUE_DEPTH_REFRESH_INTERVAL_MS`] per process (the gauge is
    /// process-wide too); in between, the claim path's exact small counts
    /// and the enqueue path's `+1` keep it moving.
    pub(crate) fn refresh_eval_queue_depth(conn: &Connection, held: usize, force: bool) -> R<()> {
        use std::sync::atomic::Ordering;
        let now_ms = process_millis().max(1);
        let last = QUEUE_DEPTH_REFRESHED_AT_MS.load(Ordering::Relaxed);
        if !force && last != 0 && now_ms.saturating_sub(last) < QUEUE_DEPTH_REFRESH_INTERVAL_MS {
            return Ok(());
        }
        QUEUE_DEPTH_REFRESHED_AT_MS.store(now_ms, Ordering::Relaxed);
        let pending = Self::pending_eval_count_tx(conn)?;
        crate::telemetry::metrics()
            .eval_queue_depth
            .set((pending - held as i64).max(0));
        Ok(())
    }

    /// The pending duplicates a claim of `eval_id` subsumes, which its
    /// settle deletes. Empty for an eval this manager has not claimed.
    pub(crate) fn subsumed_by_claim(&self, eval_id: &str) -> Vec<String> {
        self.lock_eval_claims()
            .claimed
            .get(eval_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Forget claims whose outcome has been written (or that must be
    /// re-run): the ids become claimable again if their rows still exist.
    pub(crate) fn release_eval_claims<'a>(&self, eval_ids: impl IntoIterator<Item = &'a str>) {
        let mut claims = self.lock_eval_claims();
        for eval_id in eval_ids {
            if let Some(subsumed) = claims.claimed.remove(eval_id) {
                for id in subsumed {
                    claims.held.remove(&id);
                }
            }
            claims.held.remove(eval_id);
            claims.capacity_seq.remove(eval_id);
            claims.claim_epoch.remove(eval_id);
        }
    }

    /// [`Self::release_eval_claims`], but only for the evals still claimed
    /// in `epoch`: a worker whose claims were cleared under it must not
    /// release a newer claim of the same eval.
    pub(crate) fn release_eval_claims_from_epoch<'a>(
        &self,
        eval_ids: impl IntoIterator<Item = &'a str>,
        epoch: u64,
    ) {
        let current: Vec<&str> = {
            let claims = self.lock_eval_claims();
            eval_ids
                .into_iter()
                .filter(|id| claims.claim_epoch.get(*id) == Some(&epoch))
                .collect()
        };
        self.release_eval_claims(current);
    }

    /// Split `completions` into those whose eval is still claimed in
    /// `epoch` and the ids of those that are not (their claim was cleared,
    /// and perhaps taken again, while the handler ran).
    pub(crate) fn completions_claimed_in_epoch(
        &self,
        completions: Vec<(String, EvalCompletion)>,
        epoch: u64,
    ) -> (Vec<(String, EvalCompletion)>, Vec<String>) {
        let claims = self.lock_eval_claims();
        let mut current = Vec::with_capacity(completions.len());
        let mut stale = Vec::new();
        for (id, completion) in completions {
            if claims.claim_epoch.get(&id) == Some(&epoch) {
                current.push((id, completion));
            } else {
                stale.push(id);
            }
        }
        (current, stale)
    }

    /// The current claim epoch (see the module docs).
    pub fn eval_claim_epoch(&self) -> u64 {
        self.lock_eval_claims().epoch
    }

    /// Forget every claim: this manager stopped leading the scheduler lane,
    /// or just started, and whatever it held is the next leader's to run.
    pub fn clear_eval_claims(&self) {
        let mut claims = self.lock_eval_claims();
        claims.claimed.clear();
        claims.held.clear();
        claims.capacity_seq.clear();
        claims.claim_epoch.clear();
        // Handlers already running keep their old epoch, so their settles
        // can no longer touch whatever is claimed from here on.
        claims.epoch = claims.epoch.wrapping_add(1);
    }

    /// The capacity log's position when `eval_id` was claimed; the current
    /// position for an eval this manager did not claim (nothing it could
    /// have missed).
    pub(crate) fn claim_capacity_seq(&self, eval_id: &str) -> u64 {
        let claimed = self.lock_eval_claims().capacity_seq.get(eval_id).copied();
        claimed.unwrap_or_else(|| self.capacity_seq())
    }

    /// Every eval id this manager holds.
    pub(crate) fn held_eval_ids(&self) -> HashSet<String> {
        self.lock_eval_claims().held.clone()
    }

    /// Whether this manager's eval workers hold `eval_id` as a claim.
    pub(crate) fn eval_is_claimed(&self, eval_id: &str) -> bool {
        self.lock_eval_claims().claimed.contains_key(eval_id)
    }

    /// Delete the duplicates a settled claim subsumed, inside the settle's
    /// write.
    pub(crate) fn delete_subsumed_evals_tx(conn: &Connection, ids: &[String]) -> R<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut stmt =
            conn.prepare_cached("DELETE FROM eval WHERE id = ?1 AND status = 'pending';")?;
        for id in ids {
            stmt.execute([id])?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod eval_claims_test {
    use super::*;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&crate::structs::config::ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    fn pending(store: &Store) -> R<i64> {
        let conn = store.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM eval WHERE status = 'pending';",
            [],
            |row| row.get(0),
        )?)
    }

    /// A claim writes nothing, holds its eval against a second claim, and
    /// shows `in_flight` in listings; settling deletes it and the duplicates
    /// it subsumed in one write.
    #[test]
    fn a_claim_is_memory_and_its_settle_deletes_its_duplicates() -> R<()> {
        let (_dir, store) = store()?;
        let first = store.enqueue_eval("node_lost_reschedule", None, None, Some("n1"), Some(30))?;
        store.enqueue_eval("node_lost_reschedule", None, None, Some("n1"), Some(30))?;
        store.enqueue_eval("node_lost_reschedule", None, None, Some("n2"), Some(30))?;

        let claimed = store.claim_eval()?.context("claimable")?;
        assert_eq!(claimed.id, first.id);
        assert_eq!(claimed.status, "in_flight");
        assert_eq!(pending(&store)?, 3, "claiming wrote nothing");
        let listed = store.get_evals()?;
        assert_eq!(
            listed
                .iter()
                .filter(|eval| eval.status == "in_flight")
                .count(),
            1
        );

        // The duplicate for n1 is held with the claim; n2 is still free.
        let next = store.claim_eval()?.context("another claimable")?;
        assert_eq!(next.node_name, "n2");
        assert!(store.claim_eval()?.is_none());

        store.complete_eval(&claimed.id, true)?;
        store.complete_eval(&next.id, true)?;
        assert_eq!(pending(&store)?, 0, "settles deleted the duplicate too");
        Ok(())
    }

    fn query_plan(store: &Store, sql: &str) -> R<String> {
        let conn = store.read_connection()?;
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        // The plan does not depend on the values, only on their presence.
        let nulls = std::iter::repeat_n(rusqlite::types::Null, stmt.parameter_count());
        let rows = stmt.query_map(rusqlite::params_from_iter(nulls), |row| {
            row.get::<_, String>(3)
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?.join(" | "))
    }

    /// Every statement a claim runs holds the claims mutex, so none may scan
    /// or sort the pending queue (architecture review SCH3). Pinned by query
    /// plan, which here is a property of the schema and the SQL rather than
    /// of the data: Legion never runs `ANALYZE`, so SQLite plans without
    /// table statistics.
    #[test]
    fn claim_queries_use_their_indexes() -> R<()> {
        let (_dir, store) = store()?;

        let head = query_plan(&store, &claim_head_sql(ClaimScope::Any))?;
        assert!(head.contains("idx_eval_status_priority_id"), "{head}");
        assert!(
            !head.contains("TEMP B-TREE"),
            "the head must not sort: {head}"
        );
        assert!(!head.contains("SCAN eval"), "{head}");

        let non_planning = query_plan(&store, &claim_head_sql(ClaimScope::NonPlanning))?;
        assert!(
            non_planning.contains("idx_eval_pending_kind_order"),
            "the throttled claim seeks its kinds: {non_planning}"
        );
        assert!(!non_planning.contains("SCAN eval"), "{non_planning}");

        let subsume = query_plan(&store, SUBSUME_SQL)?;
        assert!(
            subsume.contains("idx_eval_pending_scope")
                && subsume.contains("job_id=?")
                && subsume.contains("node_id=?"),
            "the duplicate lookup seeks the whole scope: {subsume}"
        );

        let count = query_plan(&store, PENDING_CAPPED_SQL)?;
        assert!(!count.contains("SCAN eval"), "{count}");
        Ok(())
    }

    /// `IS` matching treats an absent scope (NULL) as a scope of its own: a
    /// job-scoped eval never subsumes a node-scoped one of the same kind, and
    /// blank scopes written by any path are stored as NULL so they match.
    #[test]
    fn duplicate_collapse_matches_absent_scopes_exactly() -> R<()> {
        let (_dir, store) = store()?;
        let node_scoped =
            store.enqueue_eval("node_lost_reschedule", None, None, Some("n1"), None)?;
        store.enqueue_eval(
            "node_lost_reschedule",
            Some(" "),
            Some(""),
            Some("n1"),
            None,
        )?;
        store.enqueue_eval("node_lost_reschedule", Some("web"), None, Some("n1"), None)?;

        let claimed = store.claim_eval()?.context("claimable")?;
        assert_eq!(claimed.id, node_scoped.id);
        assert_eq!(
            store.subsumed_by_claim(&claimed.id).len(),
            1,
            "the blank-scoped duplicate collapses, the job-scoped eval does not"
        );
        let next = store.claim_eval()?.context("the job-scoped eval")?;
        assert_eq!(next.job_id, "web");
        assert!(store.claim_eval()?.is_none());
        Ok(())
    }

    /// Releasing a claim (a worker gave up, or the manager stepped down)
    /// makes the eval claimable again, and a held eval never reads as
    /// ready work.
    #[test]
    fn released_and_cleared_claims_are_claimable_again() -> R<()> {
        let (_dir, store) = store()?;
        store.enqueue_eval("job_submit", Some("web"), Some("jc-1"), None, Some(50))?;
        let claimed = store.claim_eval()?.context("claimable")?;
        assert_eq!(store.next_pending_eval_delay()?, None);
        store.release_eval_claims([claimed.id.as_str()]);
        assert_eq!(
            store.next_pending_eval_delay()?,
            Some(Duration::ZERO),
            "released: ready again"
        );
        let again = store.claim_eval()?.context("claimable again")?;
        assert_eq!(again.id, claimed.id);
        store.clear_eval_claims();
        assert_eq!(store.claim_eval()?.context("after clear")?.id, claimed.id);
        Ok(())
    }

    /// A handler that outlived a clear of the claims (leadership lost and
    /// regained while it ran) settles against the epoch it claimed in, so
    /// its outcome cannot delete, fail, or release a newer claim of the
    /// same eval; the newer claimant's settle still lands.
    #[test]
    fn a_settle_from_a_cleared_epoch_cannot_touch_a_newer_claim() -> R<()> {
        let (_dir, store) = store()?;
        let eval = store.enqueue_eval("job_submit", Some("web"), Some("jc-1"), None, Some(50))?;
        let limits = EvalBatchLimits { max: 1, workers: 1 };
        let (old, old_epoch) = store.claim_evals_in_epoch(limits, ClaimScope::Any)?;
        assert_eq!(old[0].id, eval.id);

        store.clear_eval_claims();
        let (new, new_epoch) = store.claim_evals_in_epoch(limits, ClaimScope::Any)?;
        assert_eq!(new[0].id, eval.id, "claimed again in the new epoch");
        assert_ne!(old_epoch, new_epoch);

        let done = |claimed: &Eval| EvalCompletion::Finished {
            succeeded: true,
            event: crate::storage::events::drafts::eval_completed(claimed, "done", Duration::ZERO),
        };
        let (next, _) = store.complete_claimed_evals_and_claim_next(
            vec![(eval.id.clone(), done(&old[0]))],
            old_epoch,
            limits,
            ClaimScope::Any,
        )?;
        assert!(next.is_empty());
        assert_eq!(pending(&store)?, 1, "the stale settle wrote nothing");
        assert!(
            store.eval_is_claimed(&eval.id),
            "nor released the newer claim"
        );
        store.release_eval_claims_from_epoch([eval.id.as_str()], old_epoch);
        assert!(
            store.eval_is_claimed(&eval.id),
            "a stale release is ignored too"
        );

        store.complete_claimed_evals_and_claim_next(
            vec![(eval.id.clone(), done(&new[0]))],
            new_epoch,
            limits,
            ClaimScope::Any,
        )?;
        assert_eq!(pending(&store)?, 0, "the current claimant settles it");
        assert!(!store.eval_is_claimed(&eval.id));
        Ok(())
    }
}
