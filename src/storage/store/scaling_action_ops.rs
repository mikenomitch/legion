//! Action intents for targets outside Legion (`RecordScalingAction`;
//! docs/autoscaling.md, "No duplicate actions"; autoscaler-v2 plan §6.9,
//! calls entry 12): one `scaling_action` row per standalone policy, on the
//! Control lane with the rest of the scaling state.
//!
//! Before the autoscaler calls a target plugin (`target/scale`,
//! `target/terminate`) or starts a drain, it **opens an intent**: a
//! compare-and-set against the row's version. Only the writer whose
//! expected version still matches wins and makes the call; a replica whose
//! lease overlapped loses, and reads the winner's intent back instead of
//! acting. When the call returns, the winner **closes** the intent with its
//! outcome, again compare-and-set. An intent a crashed replica left open is
//! closed by the next owner (`reconciled`) once it has checked the target's
//! status.
//!
//! - **Versions** are values of the `scaling_action` counter in
//!   `scaling_counter`: they never repeat, across a policy's delete and
//!   recreate (calls entry 41's rule).
//! - **History** rows ride the same transaction: the intent's `scaled` /
//!   `drain` / `terminate` / `forget` / `uncordon` rows, and a `failed`
//!   row when it closes failed.

use rusqlite::{params, Connection, OptionalExtension, Row};

use super::*;

/// The counter action versions come from.
const SCALING_ACTION_COUNTER: &str = "scaling_action";

/// One `scaling_action` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalingActionRow {
    /// The standalone policy's canonical id.
    pub policy: String,
    pub version: u64,
    /// `open`, `succeeded`, `failed`, or `reconciled`.
    pub state: String,
    /// The intent, as the `ScalingActionIntent` JSON the app sent.
    pub intent: String,
    /// The app instance that opened it.
    pub owner: String,
    pub principal: String,
    pub opened_ms: i64,
    pub closed_ms: Option<i64>,
    /// What closing it said.
    pub detail: String,
}

/// What an intent write did.
#[derive(Debug, Clone, PartialEq)]
pub enum ActionWrite {
    /// The write landed: the row now.
    Won(ScalingActionRow),
    /// The version had moved: nothing was written; the row as it is.
    Lost(Option<ScalingActionRow>),
}

/// A history row an intent write records with it.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionHistory {
    pub from: u32,
    pub to: u32,
    pub kind: String,
    pub applied: bool,
    pub history: NewHistory,
}

const COLUMNS: &str =
    "policy, version, state, intent, owner, principal, opened_ms, closed_ms, detail";

fn from_row(row: &Row<'_>) -> Result<ScalingActionRow, rusqlite::Error> {
    Ok(ScalingActionRow {
        policy: row.get(0)?,
        version: row.get::<_, i64>(1)?.max(0) as u64,
        state: row.get(2)?,
        intent: row.get(3)?,
        owner: row.get(4)?,
        principal: row.get(5)?,
        opened_ms: row.get(6)?,
        closed_ms: row.get(7)?,
        detail: row.get(8)?,
    })
}

impl Store {
    fn scaling_action_tx(conn: &Connection, policy: &str) -> R<Option<ScalingActionRow>> {
        Ok(conn
            .prepare_cached(&format!(
                "SELECT {COLUMNS} FROM scaling_action WHERE policy = ?1;"
            ))?
            .query_row([policy], from_row)
            .optional()?)
    }

    /// One policy's action row.
    pub fn scaling_action(&self, policy: &str) -> R<Option<ScalingActionRow>> {
        let conn = self.read_connection()?;
        Self::scaling_action_tx(&conn, policy)
    }

    /// Every action row.
    pub fn scaling_actions(&self) -> R<Vec<ScalingActionRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {COLUMNS} FROM scaling_action ORDER BY policy;"
        ))?;
        let rows = stmt.query_map([], from_row)?;
        Self::collect_rows(rows)
    }

    /// A cheap fingerprint: the rows' count and the highest version.
    pub fn scaling_action_fingerprint(&self) -> R<(i64, i64)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM scaling_action;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    fn write_history_rows_tx(
        conn: &Connection,
        target_key: &str,
        principal: &str,
        now_ms: i64,
        rows: &[ActionHistory],
    ) -> R<()> {
        for row in rows {
            Self::insert_history_tx(
                conn,
                target_key,
                "",
                "",
                now_ms,
                row.from,
                row.to,
                "autoscaler",
                principal,
                &row.kind,
                row.applied,
                &row.history,
                "",
            )?;
        }
        Ok(())
    }

    /// Open an intent on `policy` if its row is still at
    /// `expected_version` (0: no row yet), with its history rows, in one
    /// write.
    #[allow(clippy::too_many_arguments)]
    #[instrument(err, skip(self, intent, history))]
    pub fn open_scaling_action(
        &self,
        policy: &str,
        expected_version: u64,
        intent: &str,
        owner: &str,
        principal: &str,
        now_ms: i64,
        history: Vec<ActionHistory>,
    ) -> R<ActionWrite> {
        let (policy, intent, owner, principal) = (
            policy.to_owned(),
            intent.to_owned(),
            owner.to_owned(),
            principal.to_owned(),
        );
        self.replicated(Lane::Control, "open_scaling_action", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::scaling_action_tx(&tx, &policy)?;
            if current.as_ref().map_or(0, |row| row.version) != expected_version {
                return Ok(ActionWrite::Lost(current));
            }
            let version = Self::next_scaling_counter_tx(&tx, SCALING_ACTION_COUNTER)?;
            tx.execute(
                "INSERT INTO scaling_action (policy, version, state, intent, owner, principal, \
                 opened_ms, closed_ms, detail) VALUES (?1, ?2, 'open', ?3, ?4, ?5, ?6, NULL, '') \
                 ON CONFLICT(policy) DO UPDATE SET version = ?2, state = 'open', intent = ?3, \
                 owner = ?4, principal = ?5, opened_ms = ?6, closed_ms = NULL, detail = '';",
                params![policy, version as i64, intent, owner, principal, now_ms],
            )?;
            let key = crate::manager::autoscale::policy_target_key(&policy);
            Self::write_history_rows_tx(&tx, &key, &principal, now_ms, &history)?;
            let row = Self::scaling_action_tx(&tx, &policy)?
                .ok_or_else(|| anyhow::anyhow!("scaling_action row vanished in its own write"))?;
            tx.commit()?;
            Ok(ActionWrite::Won(row))
        })
    }

    /// Close the intent at `version` with `state` (`succeeded`, `failed`,
    /// `reconciled`), if it is still that version.
    #[allow(clippy::too_many_arguments)]
    #[instrument(err, skip(self, detail, history))]
    pub fn close_scaling_action(
        &self,
        policy: &str,
        version: u64,
        state: &str,
        detail: &str,
        principal: &str,
        now_ms: i64,
        history: Vec<ActionHistory>,
    ) -> R<ActionWrite> {
        let (policy, state, detail, principal) = (
            policy.to_owned(),
            state.to_owned(),
            detail.to_owned(),
            principal.to_owned(),
        );
        self.replicated(Lane::Control, "close_scaling_action", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::scaling_action_tx(&tx, &policy)?;
            if current.as_ref().map(|row| row.version) != Some(version) {
                return Ok(ActionWrite::Lost(current));
            }
            let next = Self::next_scaling_counter_tx(&tx, SCALING_ACTION_COUNTER)?;
            tx.execute(
                "UPDATE scaling_action SET version = ?2, state = ?3, closed_ms = ?4, detail = ?5 \
                 WHERE policy = ?1;",
                params![policy, next as i64, state, now_ms, detail],
            )?;
            let key = crate::manager::autoscale::policy_target_key(&policy);
            Self::write_history_rows_tx(&tx, &key, &principal, now_ms, &history)?;
            let row = Self::scaling_action_tx(&tx, &policy)?
                .ok_or_else(|| anyhow::anyhow!("scaling_action row vanished in its own write"))?;
            tx.commit()?;
            Ok(ActionWrite::Won(row))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    fn scaled(from: u32, to: u32) -> ActionHistory {
        ActionHistory {
            from,
            to,
            kind: "scaled".to_string(),
            applied: true,
            history: NewHistory {
                summary: format!("{from} -> {to}"),
                decision_id: "d-1".to_string(),
                ..Default::default()
            },
        }
    }

    /// Two writers holding the same version: exactly one intent lands, the
    /// other reads the winner's row; closing is compare-and-set too, and
    /// versions never repeat across a policy's delete.
    #[test]
    fn intents_are_compare_and_set_test() -> R<()> {
        let (_dir, store) = store()?;
        let ActionWrite::Won(first) = store.open_scaling_action(
            "workers",
            0,
            "{}",
            "alloc-1",
            "app",
            10,
            vec![scaled(0, 3)],
        )?
        else {
            panic!("the first intent wins");
        };
        assert_eq!(
            (first.state.as_str(), first.owner.as_str()),
            ("open", "alloc-1")
        );
        // The second replica read version 0 too: it loses, and sees the row.
        let lost = store.open_scaling_action(
            "workers",
            0,
            "{}",
            "alloc-2",
            "app",
            11,
            vec![scaled(0, 3)],
        )?;
        assert_eq!(lost, ActionWrite::Lost(Some(first.clone())));
        // One history row: the winner's.
        let key = crate::manager::autoscale::policy_target_key("workers");
        let rows = store.scale_history(&key, Some(""), Some(""), 10)?;
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].kind.as_str(), rows[0].to), ("scaled", 3));

        // Closing names the version; a stale close loses.
        assert!(matches!(
            store.close_scaling_action(
                "workers",
                first.version + 100,
                "succeeded",
                "",
                "app",
                12,
                vec![]
            )?,
            ActionWrite::Lost(Some(_))
        ));
        let ActionWrite::Won(closed) = store.close_scaling_action(
            "workers",
            first.version,
            "succeeded",
            "",
            "app",
            12,
            vec![],
        )?
        else {
            panic!("the owner's close wins");
        };
        assert!(closed.version > first.version);
        assert_eq!(
            (closed.state.as_str(), closed.closed_ms),
            ("succeeded", Some(12))
        );
        assert_eq!(
            store.scaling_action_fingerprint()?,
            (1, closed.version as i64)
        );

        // The policy's delete takes its action row with it.
        store.put_scaling_policy("", "workers", "nodes", "{}", "", "op", 13)?;
        store.delete_scaling_policy("", "workers")?;
        assert!(store.scaling_action("workers")?.is_none());
        let ActionWrite::Won(again) =
            store.open_scaling_action("workers", 0, "{}", "alloc-1", "app", 14, vec![])?
        else {
            panic!("a fresh row starts at 0");
        };
        assert!(again.version > closed.version, "versions never repeat");
        Ok(())
    }
}
