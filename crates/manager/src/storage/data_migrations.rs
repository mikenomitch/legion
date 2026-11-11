//! Data migrations: one-time rewrites of existing rows, applied through the
//! raft log (raft-write-path plan §6.5).
//!
//! `migrations.rs` holds **schema steps** only — DDL, deterministic table
//! rebuilds (`INSERT INTO new SELECT * FROM old`) and constant seed rows —
//! which every manager runs locally at startup. A schema step's result does
//! not depend on *when* a replica runs it: rows present at that moment are
//! copied as they are, and later log entries apply on top identically.
//!
//! A **data step** is different: a backfill or cleanup computes new values
//! from the rows present when it runs. Run locally at startup, two replicas
//! that migrate at different applied indexes compute different values — a
//! lagging follower backfills a set of rows the leader never saw together,
//! or misses rows that later entries add. So data steps run as ONE
//! replicated control-lane op (`Store::run_data_migrations`): the leader
//! runs every pending step inside one capture and records each in the
//! replicated `data_migration` table, and every replica applies the
//! resulting rows at the same log index. Pending is read inside the capture,
//! after the leader caught up on its term (R1), so a step runs exactly once
//! cluster-wide even across failovers.
//!
//! When it runs:
//! - **raft**: the driver's data-migration task on the control-lane leader,
//!   once every voter advertises this build's schema version (so every
//!   replica has the columns the step writes);
//! - **`local_disk`**: inline at `Store::start_storage`;
//! - **a brand-new database** records every step as done when it is
//!   created: there are no rows to migrate, and every replica's copy of the
//!   cluster state descends from the bootstrap node's (joiners install its
//!   snapshot), so the record replicates with it.
//!
//! Adding one: append a [`DataMigration`] with a new, never-reused `id`. Its
//! `run` must touch control-lane tables only (it runs as a control-lane op,
//! and the ownership guard rejects anything else) and must be correct on a
//! database that has also seen writes from the new build — it may run long
//! after the schema step it pairs with.

use rusqlite::{params, Connection};

use crate::prelude::*;

/// One data step (module docs).
pub(crate) struct DataMigration {
    /// Stable, unique, never reused: recorded in `data_migration.id`.
    pub(crate) id: &'static str,
    /// The rewrite itself, run inside the replicated capture.
    pub(crate) run: fn(&Connection) -> R<()>,
}

/// Every data step, in the order they run.
pub(crate) const DATA_MIGRATIONS: &[DataMigration] = &[
    DataMigration {
        id: "job-updated-at-backfill",
        run: backfill_job_updated_at,
    },
    DataMigration {
        id: "app-uid-backfill",
        run: backfill_app_uids,
    },
    DataMigration {
        id: "scaling-policy-state-cleanup",
        run: drop_scaling_policy_state,
    },
];

/// Job mtime (job-updated-at plan §2): rows from before `job.updated_at`
/// existed have no mtime to recover, and their creation time is the only
/// honest lower bound. Guarded on `typeof(created_at)` so a row whose
/// created_at was written as text (only old migration fixtures do this)
/// keeps a NULL `updated_at` rather than a string in an INTEGER column —
/// `job_from_row` falls back to created_at for those.
fn backfill_job_updated_at(conn: &Connection) -> R<()> {
    conn.execute(
        "UPDATE job SET updated_at = created_at
         WHERE updated_at IS NULL AND typeof(created_at) = 'integer';",
        [],
    )?;
    Ok(())
}

/// Each app's own uid (docs/apps.md, "Trust model"): apps installed before
/// the column existed hold `0`; each gets the lowest free uid from
/// [`APP_UID_BASE`](crate::structs::app::APP_UID_BASE) up, in name order —
/// the same rule an install applies, so uids assigned by the new build in
/// the meantime are never reused.
fn backfill_app_uids(conn: &Connection) -> R<()> {
    use crate::structs::app::{APP_UID_BASE, APP_UID_COUNT};
    let unassigned: Vec<String> = {
        let mut stmt = conn.prepare("SELECT name FROM app WHERE uid = 0 ORDER BY name;")?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        rows.collect::<Result<_, _>>()?
    };
    if unassigned.is_empty() {
        return Ok(());
    }
    let mut taken: std::collections::BTreeSet<u32> = {
        let mut stmt = conn.prepare("SELECT uid FROM app WHERE uid >= ?1;")?;
        let rows = stmt.query_map([APP_UID_BASE], |row| row.get(0))?;
        rows.collect::<Result<_, _>>()?
    };
    let mut next = APP_UID_BASE;
    for name in unassigned {
        while taken.contains(&next) {
            next += 1;
        }
        if next >= APP_UID_BASE + APP_UID_COUNT {
            bail!("every app uid is taken; app {name} keeps uid 0 until one is uninstalled");
        }
        conn.execute(
            "UPDATE app SET uid = ?1 WHERE name = ?2;",
            params![next, name],
        )?;
        taken.insert(next);
    }
    Ok(())
}

/// The autoscaler's standalone policies became object kinds (app-object-kinds
/// plan §8 O2): the scaling state kept under `policy:<id>` keys goes. Object
/// targets key theirs `object:<uid>`, which this keeps, since it may run
/// after the new build wrote some.
fn drop_scaling_policy_state(conn: &Connection) -> R<()> {
    conn.execute_batch(
        "DELETE FROM scaling_action WHERE policy NOT LIKE 'object:%';
         DELETE FROM scaling_pause WHERE job_id LIKE 'policy:%';
         DELETE FROM scale_history WHERE job_id LIKE 'policy:%';
         DELETE FROM scaling_counter WHERE name = 'scaling_policy';",
    )?;
    Ok(())
}

/// The data steps not yet recorded on `conn`, in order.
pub(crate) fn pending_on(conn: &Connection) -> R<Vec<&'static DataMigration>> {
    let done: std::collections::BTreeSet<String> = {
        let mut stmt = conn.prepare("SELECT id FROM data_migration;")?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        rows.collect::<Result<_, _>>()?
    };
    Ok(DATA_MIGRATIONS
        .iter()
        .filter(|step| !done.contains(step.id))
        .collect())
}

/// Run every pending data step on `conn` and record each, returning the ids
/// that ran. The caller provides the transaction (a replicated capture, so
/// the effects and the records ride one entry).
pub(crate) fn run_pending_on(conn: &Connection) -> R<Vec<&'static str>> {
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let mut ran = Vec::new();
    for step in pending_on(conn)? {
        (step.run)(conn).with_context(|| format!("data migration {} failed", step.id))?;
        conn.execute(
            "INSERT INTO data_migration (id, applied_at) VALUES (?1, ?2);",
            params![step.id, now],
        )?;
        ran.push(step.id);
    }
    Ok(ran)
}

/// Record every data step as done without running it: a brand-new database
/// has no rows to migrate (module docs). `applied_at = 0` marks "recorded
/// at creation, nothing ran", and keeps independently created databases
/// byte-identical in this table (a wall-clock stamp would differ per node
/// until a snapshot install replaced it).
pub(crate) fn mark_all_done(conn: &Connection) -> R<()> {
    for step in DATA_MIGRATIONS {
        conn.execute(
            "INSERT OR IGNORE INTO data_migration (id, applied_at) VALUES (?1, 0);",
            params![step.id],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrations::get_migrations;

    fn migrated() -> R<Connection> {
        let mut conn = Connection::open_in_memory()?;
        get_migrations().to_latest(&mut conn)?;
        Ok(conn)
    }

    #[test]
    fn data_migration_ids_are_unique() {
        let ids: std::collections::BTreeSet<&str> =
            DATA_MIGRATIONS.iter().map(|step| step.id).collect();
        assert_eq!(ids.len(), DATA_MIGRATIONS.len());
    }

    /// A step runs once: the record makes a second run a no-op, even when
    /// rows it would have rewritten appear afterwards.
    #[test]
    fn a_data_step_runs_once_and_is_recorded() -> R<()> {
        let conn = migrated()?;
        conn.execute(
            "INSERT INTO job (id, name, status, created_at) VALUES ('old', 'old', 'done', 100);",
            [],
        )?;
        assert_eq!(pending_on(&conn)?.len(), DATA_MIGRATIONS.len());
        let ran = run_pending_on(&conn)?;
        assert_eq!(ran.len(), DATA_MIGRATIONS.len());
        let updated_at: Option<i64> =
            conn.query_row("SELECT updated_at FROM job WHERE id = 'old';", [], |row| {
                row.get(0)
            })?;
        assert_eq!(updated_at, Some(100));
        assert!(pending_on(&conn)?.is_empty());

        conn.execute(
            "INSERT INTO job (id, name, status, created_at) VALUES ('new', 'new', 'done', 200);",
            [],
        )?;
        assert!(run_pending_on(&conn)?.is_empty());
        let updated_at: Option<i64> =
            conn.query_row("SELECT updated_at FROM job WHERE id = 'new';", [], |row| {
                row.get(0)
            })?;
        assert_eq!(updated_at, None, "a recorded step never runs again");
        Ok(())
    }

    /// The app uid backfill gives each unassigned app the lowest free uid in
    /// name order and never reuses one the new build already handed out.
    #[test]
    fn app_uid_backfill_skips_assigned_uids() -> R<()> {
        use crate::structs::app::APP_UID_BASE;
        let conn = migrated()?;
        let columns: Vec<(String, i64)> = {
            let mut stmt =
                conn.prepare("SELECT name, \"notnull\" FROM pragma_table_info('app');")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<Result<_, _>>()?
        };
        // Insert minimal rows: every NOT NULL column without a default gets
        // a placeholder.
        let insert = |name: &str, uid: u32| -> R<()> {
            let mut names = Vec::new();
            let mut values = Vec::new();
            for (column, notnull) in &columns {
                match column.as_str() {
                    "name" => {
                        names.push("name".to_string());
                        values.push(format!("'{name}'"));
                    }
                    "uid" => {
                        names.push("uid".to_string());
                        values.push(uid.to_string());
                    }
                    other if *notnull == 1 => {
                        names.push(format!("\"{other}\""));
                        values.push("''".to_string());
                    }
                    _ => {}
                }
            }
            conn.execute(
                &format!(
                    "INSERT INTO app ({}) VALUES ({});",
                    names.join(", "),
                    values.join(", ")
                ),
                [],
            )?;
            Ok(())
        };
        insert("zeta", 0)?;
        insert("alpha", 0)?;
        insert("fresh", APP_UID_BASE)?;
        backfill_app_uids(&conn)?;
        let uid = |name: &str| -> R<u32> {
            Ok(
                conn.query_row("SELECT uid FROM app WHERE name = ?1;", [name], |row| {
                    row.get(0)
                })?,
            )
        };
        assert_eq!(uid("fresh")?, APP_UID_BASE);
        assert_eq!(uid("alpha")?, APP_UID_BASE + 1);
        assert_eq!(uid("zeta")?, APP_UID_BASE + 2);
        Ok(())
    }

    /// The scaling cleanup removes old-format state and keeps object-keyed
    /// rows the new build may already have written.
    #[test]
    fn scaling_cleanup_keeps_object_targets() -> R<()> {
        let conn = migrated()?;
        for policy in ["object:7", "legacy-policy"] {
            conn.execute(
                "INSERT INTO scaling_action (policy, version, state, intent, opened_ms)
                 VALUES (?1, 1, 'open', '{}', 0);",
                [policy],
            )?;
        }
        drop_scaling_policy_state(&conn)?;
        let left: Vec<String> = {
            let mut stmt = conn.prepare("SELECT policy FROM scaling_action;")?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            rows.collect::<Result<_, _>>()?
        };
        assert_eq!(left, vec!["object:7".to_string()]);
        Ok(())
    }
}
