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
        Ok(self.claim_up_to(|_| 1, ClaimScope::Any)?.into_iter().next())
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
        self.claim_up_to(|pending| limits.share_of(pending), scope)
    }

    fn claim_up_to(&self, take_for: impl FnOnce(i64) -> usize, scope: ClaimScope) -> R<Vec<Eval>> {
        let started = Instant::now();
        let capacity_seq = self.capacity_seq();
        let mut claims = self.lock_eval_claims();
        let conn = self.read_connection()?;
        let now = Self::sortable_now()?;
        let pending = Self::pending_eval_count_tx(&conn)?;
        let unclaimed = (pending - claims.held.len() as i64).max(0);
        let take = take_for(unclaimed);
        if unclaimed == 0 || take == 0 {
            return Ok(Vec::new());
        }
        let candidates: Vec<Eval> = {
            // The non-planning kind list is a compile-time constant, never
            // caller input, so it is built into the statement.
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
            let mut stmt = conn.prepare_cached(&format!(
                "SELECT id, status, type, job_id, job_config_id, node_id, priority, ready_at
                 FROM eval
                 WHERE status = 'pending' AND ready_at <= ?1{kind_filter}
                 ORDER BY priority DESC, id ASC
                 LIMIT ?2;"
            ))?;
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
            // Collapse only work this claim genuinely subsumes: same kind,
            // same (job, job config, node) scope with NULL and '' alike, and
            // already claimable. The reasons each part matters are on the
            // old claim's sweep in git history and in
            // docs/scheduler-and-eval-loop.md ("Claims").
            let subsumed: Vec<String> = {
                let mut stmt = conn.prepare_cached(
                    "SELECT id FROM eval
                     WHERE status = 'pending'
                       AND id != ?1
                       AND type = ?2
                       AND ready_at <= ?3
                       AND COALESCE(job_id, '') = ?4
                       AND COALESCE(job_config_id, '') = ?5
                       AND COALESCE(node_id, '') = ?6;",
                )?;
                let rows = stmt.query_map(
                    params![
                        eval.id,
                        eval.kind,
                        now,
                        eval.job_id.trim(),
                        eval.job_config_id.trim(),
                        eval.node_name.trim()
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
            eval.status = "in_flight".to_string();
            claimed.push(eval);
        }
        let depth = (pending - claims.held.len() as i64).max(0);
        drop(claims);
        crate::manager::telemetry::metrics()
            .eval_queue_depth
            .set(depth);
        if !claimed.is_empty() {
            info!(
                claimed = claimed.len(),
                duration_ms = started.elapsed().as_millis() as u64,
                "Claimed evals"
            );
        }
        Ok(claimed)
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
        }
    }

    /// Forget every claim: this manager stopped leading the scheduler lane,
    /// or just started, and whatever it held is the next leader's to run.
    pub fn clear_eval_claims(&self) {
        let mut claims = self.lock_eval_claims();
        claims.claimed.clear();
        claims.held.clear();
        claims.capacity_seq.clear();
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
}
