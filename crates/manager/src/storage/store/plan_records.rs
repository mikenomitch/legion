//! What `plan_submission` keeps (architecture review SCH2, SCH5;
//! internal_docs/plans/placement-and-commit-cost-plan.md §6.1).
//!
//! The table used to be a row per plan, written before the apply and
//! read back by it, then kept forever: every placement's ranked candidates
//! as JSON, through the raft log and into every snapshot. It is now a
//! bounded record of what a person might need to look at:
//!
//! - a plan applied through the fused submit-and-apply op (every
//!   production plan, and edge-boot ratification) writes **no row** — the
//!   walk consumes the request in memory;
//! - a rejected plan writes one row, `status = 'rejected'` with its reason,
//!   for the rejection-classification query in
//!   docs/diagnosing-scheduler-performance.md;
//! - a staged plan (the two-step `insert_plan_submission` +
//!   `apply_plan_submission` API, which tests and benchmarks use) is a
//!   `queued` row until its apply deletes it, or marks it rejected.
//!
//! Retention is an id window: each insert deletes rows more than
//! [`PLAN_SUBMISSION_RETAINED`] ids below its own, a bounded number per
//! insert, inside the same replicated op. An orphaned staged row (never
//! applied) leaves the window like any other. The scheduler-lane leader
//! also sweeps the table on startup and on gaining leadership
//! ([`Store::prune_plan_submissions`]), which drains what an older data
//! directory accumulated.

use super::*;

/// How many plan records the id window keeps: about the last thousand
/// rejections and staged plans. Enough to classify a rejection storm by
/// reason; small enough that the table never shows up in a snapshot.
pub const PLAN_SUBMISSION_RETAINED: i64 = 1_000;

/// Rows one insert may delete from below the window. More than one, so a
/// backlog (an older data directory) drains faster than it grows; bounded,
/// so no single plan's commit pays for the whole backlog.
const PRUNE_PER_INSERT: i64 = 64;

/// Rows one leader sweep op deletes. Bounds the replicated changeset.
const SWEEP_BATCH: i64 = 5_000;

/// A staged (`queued`) row older than this was left by a caller that
/// staged a plan and never applied it — nothing in a manager stages a plan
/// and waits minutes to apply it — so the leader sweep deletes it.
const ORPHAN_AGE: TimeDuration = TimeDuration::minutes(10);

/// Where a plan's record lives while the walk runs.
pub(super) enum PlanRecord<'a> {
    /// A staged row: deleted when the plan applies, marked when rejected.
    Staged(i64),
    /// No row (the fused path): one is written only if the plan is
    /// rejected, from the request the walk is consuming.
    Unwritten(&'a PlanApplyRequest),
}

impl Store {
    /// Insert one `plan_submission` row and keep the id window, on the
    /// caller's connection (inside its transaction). Returns the row id.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn insert_plan_record_on(
        conn: &Connection,
        job_config_id: &str,
        mode: &str,
        placements_json: &str,
        expected_versions: &BTreeMap<String, i64>,
        evictions: &[PlanEviction],
        target_instance: &str,
        rejection: Option<&str>,
    ) -> R<i64> {
        let created_at = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let evictions_json = serde_json::to_string(evictions)?;
        let expected_vector_json = serde_json::to_string(expected_versions)?;
        let expected_job_epoch = *expected_versions
            .get(&Self::scope_vector_key("job_config", job_config_id))
            .unwrap_or(&0);
        let status = if rejection.is_some() {
            "rejected"
        } else {
            "queued"
        };
        let id: i64 = conn
            .prepare_cached(
                "INSERT INTO plan_submission(job_config_id, mode, placements, expected_epoch, \
                 expected_version_vector, status, rejection_reason, created_at, evictions, \
                 target_instance) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) RETURNING id;",
            )?
            .query_row(
                params![
                    job_config_id,
                    mode,
                    placements_json,
                    expected_job_epoch,
                    expected_vector_json,
                    status,
                    rejection,
                    created_at,
                    evictions_json,
                    target_instance
                ],
                |row| row.get(0),
            )?;
        // Rowid range from the oldest row: O(rows deleted), which in steady
        // state is zero or one.
        conn.prepare_cached(
            "DELETE FROM plan_submission WHERE id IN (
                 SELECT id FROM plan_submission WHERE id <= ?1 ORDER BY id LIMIT ?2
             );",
        )?
        .execute(params![id - PLAN_SUBMISSION_RETAINED, PRUNE_PER_INSERT])?;
        Ok(id)
    }

    /// Record a rejected plan: mark the staged row, or write the one row a
    /// fused plan keeps. Returns the record's id.
    pub(super) fn record_rejected_plan_on(
        conn: &Connection,
        record: &PlanRecord<'_>,
        job_config_id: &str,
        expected_versions: &BTreeMap<String, i64>,
        reason: &str,
    ) -> R<i64> {
        match record {
            PlanRecord::Staged(id) => {
                conn.prepare_cached(
                    "UPDATE plan_submission SET status = 'rejected', rejection_reason = ?1 \
                     WHERE id = ?2;",
                )?
                .execute(params![reason, id])?;
                Ok(*id)
            }
            PlanRecord::Unwritten(request) => Self::insert_plan_record_on(
                conn,
                job_config_id,
                &request.mode,
                &request.placements.to_json()?,
                expected_versions,
                &request.evictions,
                &request.target_instance,
                Some(reason),
            ),
        }
    }

    /// An applied plan keeps no record: a staged row is deleted by the
    /// apply that consumed it, in the same transaction.
    pub(super) fn finish_applied_plan_on(conn: &Connection, record: &PlanRecord<'_>) -> R<()> {
        if let PlanRecord::Staged(id) = record {
            conn.prepare_cached("DELETE FROM plan_submission WHERE id = ?1;")?
                .execute([id])?;
        }
        Ok(())
    }

    /// Delete every plan record outside the retention window, and staged
    /// rows nobody applied, in bounded batches (one scheduler-lane op each).
    /// The leader runs this on startup and on gaining scheduler-lane
    /// leadership. Returns the rows deleted.
    #[instrument(err, skip_all)]
    pub fn prune_plan_submissions(&self) -> R<u64> {
        let mut total = 0u64;
        loop {
            let deleted = self.replicated(Lane::Scheduler, "prune_plan_submissions", move |s| {
                s.prune_plan_submissions_apply()
            })?;
            total += deleted;
            if deleted < SWEEP_BATCH as u64 {
                break;
            }
        }
        if total > 0 {
            info!(deleted = total, "Pruned plan_submission records");
        }
        Ok(total)
    }

    fn prune_plan_submissions_apply(&self) -> R<u64> {
        let conn = self.write_connection()?;
        let cutoff = (OffsetDateTime::now_utc() - ORPHAN_AGE).format(&Rfc3339)?;
        let deleted = conn.execute(
            "DELETE FROM plan_submission WHERE id IN (
                 SELECT id FROM plan_submission
                 WHERE id <= (SELECT COALESCE(MAX(id), 0) FROM plan_submission) - ?1
                    OR (status = 'queued' AND created_at < ?2)
                 ORDER BY id LIMIT ?3
             );",
            params![PLAN_SUBMISSION_RETAINED, cutoff, SWEEP_BATCH],
        )?;
        Ok(deleted as u64)
    }
}

#[cfg(test)]
mod plan_records_test {
    use super::*;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&crate::structs::config::ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    fn rows(store: &Store) -> R<Vec<(i64, String)>> {
        let conn = store.read_connection()?;
        let mut stmt = conn.prepare("SELECT id, status FROM plan_submission ORDER BY id;")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn insert_raw(store: &Store, status: &str, created_at: &str) -> R<()> {
        let conn = store.write_connection()?;
        conn.execute(
            "INSERT INTO plan_submission (job_config_id, mode, placements, expected_epoch, \
             status, created_at) VALUES ('jc', 'fresh', '[]', 0, ?1, ?2);",
            params![status, created_at],
        )?;
        Ok(())
    }

    /// The window holds at most `PLAN_SUBMISSION_RETAINED` rows however
    /// many plans are recorded, and drains a backlog faster than it grows.
    #[test]
    fn the_id_window_bounds_the_table() -> R<()> {
        let (_dir, store) = store()?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        for _ in 0..1_200 {
            insert_raw(&store, "applied", &now)?;
        }
        assert_eq!(
            rows(&store)?.len(),
            1_200,
            "raw history, as an old directory has"
        );

        let conn = store.write_connection()?;
        for _ in 0..10 {
            Store::insert_plan_record_on(
                &conn,
                "jc",
                "fresh",
                "[]",
                &BTreeMap::new(),
                &[],
                "",
                Some("stale"),
            )?;
        }
        drop(conn);
        let left = rows(&store)?;
        assert_eq!(left.len() as i64, PLAN_SUBMISSION_RETAINED);
        let newest = left.last().context("rows")?.0;
        assert!(left
            .iter()
            .all(|(id, _)| *id > newest - PLAN_SUBMISSION_RETAINED));
        Ok(())
    }

    /// The leader sweep removes history outside the window and staged rows
    /// nobody applied, and leaves recent staged rows alone.
    #[test]
    fn the_sweep_removes_orphans_and_history() -> R<()> {
        let (_dir, store) = store()?;
        let old = (OffsetDateTime::now_utc() - TimeDuration::hours(1)).format(&Rfc3339)?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        insert_raw(&store, "queued", &old)?;
        insert_raw(&store, "queued", &now)?;
        insert_raw(&store, "rejected", &old)?;
        assert_eq!(
            store.prune_plan_submissions()?,
            1,
            "only the stale staged row"
        );
        let left = rows(&store)?;
        assert_eq!(
            left.iter()
                .map(|(_, status)| status.as_str())
                .collect::<Vec<_>>(),
            vec!["queued", "rejected"]
        );
        Ok(())
    }
}
