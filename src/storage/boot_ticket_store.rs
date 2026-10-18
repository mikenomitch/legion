//! Storage for boot tickets (docs/edge-boot.md).
//!
//! **Ticket bytes are never stored.** A row holds only the metadata an
//! operator needs to audit a start (who minted it, for what, until when) and
//! the counter the manager needs to enforce single use. The JWT itself is
//! returned once, at mint, and after that only its `jti` ever appears —
//! in this table, in the audit events, and in log lines.
//!
//! Rows are pruned once they expire: an expired ticket can never be redeemed,
//! so its row has no further job, and the table stays bounded by the live
//! capability set rather than by history.

use crate::prelude::*;
use crate::storage::store::Store;
use crate::storage::Lane;
use rusqlite::{named_params, OptionalExtension, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// One outstanding boot ticket, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootTicketRecord {
    pub jti: String,
    pub job: String,
    pub task: String,
    pub job_config_id: String,
    /// Exactly one instance, or empty.
    pub instance_id: String,
    /// Any instance with this prefix, or empty.
    pub instance_prefix: String,
    pub pool: String,
    /// Gate ceiling, as its string form (`stage`/`boot`/`serve`).
    pub gate: String,
    pub uses: u32,
    pub uses_spent: u32,
    pub expires_at_unix: i64,
    /// ACL accessor id that minted it — who to ask about a start you did not
    /// expect. Never a token secret.
    pub accessor_id: String,
    pub created_at: String,
}

impl BootTicketRecord {
    pub fn uses_remaining(&self) -> u32 {
        self.uses.saturating_sub(self.uses_spent)
    }
}

const BOOT_TICKET_COLUMNS: &str = "jti, job, task, job_config_id, instance_id, instance_prefix, \
     pool, gate, uses, uses_spent, expires_at_unix, accessor_id, created_at";

impl Store {
    fn boot_ticket_from_row(row: &Row<'_>) -> rusqlite::Result<BootTicketRecord> {
        Ok(BootTicketRecord {
            jti: row.get(0)?,
            job: row.get(1)?,
            task: row.get(2)?,
            job_config_id: row.get(3)?,
            instance_id: row.get(4)?,
            instance_prefix: row.get(5)?,
            pool: row.get(6)?,
            gate: row.get(7)?,
            uses: row.get::<_, i64>(8)? as u32,
            uses_spent: row.get::<_, i64>(9)? as u32,
            expires_at_unix: row.get(10)?,
            accessor_id: row.get(11)?,
            created_at: row.get(12)?,
        })
    }

    /// Record a freshly minted ticket.
    #[instrument(err, skip_all, fields(jti = record.jti, job = record.job))]
    pub fn insert_boot_ticket(&self, record: &BootTicketRecord) -> R<()> {
        let record = record.clone();
        self.replicated(Lane::Scheduler, "insert_boot_ticket", move |s| {
            s.insert_boot_ticket_apply(&record)
        })
    }

    fn insert_boot_ticket_apply(&self, record: &BootTicketRecord) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT INTO boot_ticket (jti, job, task, job_config_id, instance_id, \
             instance_prefix, pool, gate, uses, uses_spent, expires_at_unix, accessor_id, \
             created_at) VALUES (:jti, :job, :task, :job_config_id, :instance_id, \
             :instance_prefix, :pool, :gate, :uses, :uses_spent, :expires_at_unix, \
             :accessor_id, :created_at);",
            named_params! {
                ":jti": record.jti,
                ":job": record.job,
                ":task": record.task,
                ":job_config_id": record.job_config_id,
                ":instance_id": record.instance_id,
                ":instance_prefix": record.instance_prefix,
                ":pool": record.pool,
                ":gate": record.gate,
                ":uses": record.uses as i64,
                ":uses_spent": record.uses_spent as i64,
                ":expires_at_unix": record.expires_at_unix,
                ":accessor_id": record.accessor_id,
                ":created_at": record.created_at,
            },
        )?;
        Ok(())
    }

    pub fn get_boot_ticket(&self, jti: &str) -> R<Option<BootTicketRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!("SELECT {BOOT_TICKET_COLUMNS} FROM boot_ticket WHERE jti = ?;"),
            [jti],
            Self::boot_ticket_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Live tickets, newest first. `job` empty lists every ticket.
    #[instrument(err, skip_all)]
    pub fn list_boot_tickets(&self, job: &str, now_unix: i64) -> R<Vec<BootTicketRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {BOOT_TICKET_COLUMNS} FROM boot_ticket \
             WHERE (?1 = '' OR job = ?1) AND expires_at_unix > ?2 \
             ORDER BY created_at DESC, jti ASC;"
        ))?;
        let rows = stmt.query_map(rusqlite::params![job, now_unix], Self::boot_ticket_from_row)?;
        Self::collect_rows(rows)
    }

    /// Spend one redemption of a ticket, atomically.
    ///
    /// This is what makes a ticket globally single-use: the conditional
    /// `UPDATE` either wins or does nothing, so two nodes presenting the same
    /// ticket both boot and exactly one of them is confirmed — even when the
    /// first has already exited. Returns whether the redemption was granted.
    ///
    /// A ticket the manager has no row for is refused: it either expired and
    /// was pruned, or it was minted by a different cluster. Both are reasons
    /// to say no.
    #[instrument(err, skip_all, fields(jti))]
    pub fn spend_boot_ticket(&self, jti: &str, now_unix: i64) -> R<bool> {
        let jti = jti.to_owned();
        self.replicated(Lane::Scheduler, "spend_boot_ticket", move |s| {
            s.spend_boot_ticket_apply(&jti, now_unix)
        })
    }

    fn spend_boot_ticket_apply(&self, jti: &str, now_unix: i64) -> R<bool> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE boot_ticket SET uses_spent = uses_spent + 1 \
             WHERE jti = ?1 AND uses_spent < uses AND expires_at_unix > ?2;",
            rusqlite::params![jti, now_unix],
        )?;
        Ok(updated == 1)
    }

    /// Drop tickets that can no longer be redeemed. Called on the same
    /// maintenance tick that expires signing keys.
    #[instrument(err, skip_all)]
    pub fn prune_expired_boot_tickets(&self, now_unix: i64) -> R<usize> {
        self.replicated(Lane::Scheduler, "prune_expired_boot_tickets", move |s| {
            let conn = s.write_connection()?;
            let removed = conn.execute(
                "DELETE FROM boot_ticket WHERE expires_at_unix <= ?1;",
                rusqlite::params![now_unix],
            )?;
            Ok(removed)
        })
    }

    /// RFC3339 stamp for a ticket row.
    pub fn boot_ticket_now() -> R<String> {
        Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
    }
}

#[cfg(test)]
mod boot_ticket_store_test {
    use super::*;

    fn record(jti: &str, uses: u32, expires_at_unix: i64) -> BootTicketRecord {
        BootTicketRecord {
            jti: jti.into(),
            job: "agent-sandboxes".into(),
            task: "workspace".into(),
            job_config_id: "jc-1".into(),
            instance_id: "user-42".into(),
            instance_prefix: String::new(),
            pool: "edge-eu".into(),
            gate: "boot".into(),
            uses,
            uses_spent: 0,
            expires_at_unix,
            accessor_id: "acc-1".into(),
            created_at: "2026-07-31T00:00:00Z".into(),
        }
    }

    fn store() -> Store {
        let store_dir = tempfile::Builder::new()
            .prefix("legion-boot-ticket-")
            .tempdir_in(crate::test_scratch::root())
            .expect("temp dir")
            .keep();
        let config = crate::structs::ManagerConfigFull {
            store_path: store_dir.to_str().expect("path").to_string(),
            ..Default::default()
        };
        Store::start_storage(&config).expect("store")
    }

    #[test]
    fn a_single_use_ticket_is_spent_exactly_once() -> R<()> {
        let store = store();
        store.insert_boot_ticket(&record("tkt_a", 1, 2_000_000))?;
        // The losing side of a two-node race: both booted, one is confirmed.
        assert!(store.spend_boot_ticket("tkt_a", 1_000_000)?);
        assert!(!store.spend_boot_ticket("tkt_a", 1_000_000)?);
        assert_eq!(store.get_boot_ticket("tkt_a")?.expect("row").uses_spent, 1);
        Ok(())
    }

    #[test]
    fn a_multi_use_ticket_spends_down_to_zero() -> R<()> {
        let store = store();
        store.insert_boot_ticket(&record("tkt_b", 3, 2_000_000))?;
        for _ in 0..3 {
            assert!(store.spend_boot_ticket("tkt_b", 1_000_000)?);
        }
        assert!(!store.spend_boot_ticket("tkt_b", 1_000_000)?);
        assert_eq!(
            store
                .get_boot_ticket("tkt_b")?
                .expect("row")
                .uses_remaining(),
            0
        );
        Ok(())
    }

    #[test]
    fn an_expired_ticket_cannot_be_spent_or_listed() -> R<()> {
        let store = store();
        store.insert_boot_ticket(&record("tkt_c", 1, 1_000_000))?;
        assert!(!store.spend_boot_ticket("tkt_c", 1_000_001)?);
        assert!(store.list_boot_tickets("", 1_000_001)?.is_empty());
        assert_eq!(store.prune_expired_boot_tickets(1_000_001)?, 1);
        assert!(store.get_boot_ticket("tkt_c")?.is_none());
        Ok(())
    }

    #[test]
    fn an_unknown_ticket_is_refused() -> R<()> {
        // A ticket minted by another cluster, or one already pruned. Either
        // way the manager has no reason to trust it.
        let store = store();
        assert!(!store.spend_boot_ticket("tkt_nope", 1_000_000)?);
        Ok(())
    }

    #[test]
    fn listing_filters_by_job() -> R<()> {
        let store = store();
        store.insert_boot_ticket(&record("tkt_d", 1, 2_000_000))?;
        let mut other = record("tkt_e", 1, 2_000_000);
        other.job = "other-job".into();
        store.insert_boot_ticket(&other)?;

        assert_eq!(store.list_boot_tickets("", 1_000_000)?.len(), 2);
        let filtered = store.list_boot_tickets("other-job", 1_000_000)?;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].jti, "tkt_e");
        Ok(())
    }
}
