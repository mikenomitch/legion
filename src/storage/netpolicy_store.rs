//! Storage for network policy objects (`legion net policy`).
//!
//! A policy is stored as its canonical JSON (`NetPolicyObject`, already
//! normalized) plus the columns the list view and the selector pre-filter
//! need. Writes are raft-replicated on the control lane, exactly like ACL
//! policies; reads are local. `generation` is a monotonic counter bumped on
//! every write so the heartbeat builder can answer "has anything changed
//! since I last merged?" without re-reading the table.

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::netpolicy::{find_default_tie, NetPolicyObject};

use rusqlite::{named_params, OptionalExtension, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// One stored policy: the object plus bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetPolicyRecord {
    pub object: NetPolicyObject,
    pub created_at: String,
    pub updated_at: String,
}

impl Store {
    fn net_policy_from_row(row: &Row<'_>) -> rusqlite::Result<NetPolicyRecord> {
        let body: String = row.get(0)?;
        let object: NetPolicyObject = serde_json::from_str(&body).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?;
        Ok(NetPolicyRecord {
            object,
            created_at: row.get(1)?,
            updated_at: row.get(2)?,
        })
    }

    /// Create or replace a policy. The object is normalized and validated
    /// here (the CLI already did, but the store is the gate), and a `default`
    /// policy that would tie with an existing one on the same priority and an
    /// overlapping selector is rejected — ties are resolved at `apply`, never
    /// at runtime.
    #[instrument(err, skip_all, fields(policy = %object.name))]
    pub fn upsert_net_policy(&self, object: NetPolicyObject) -> R<NetPolicyRecord> {
        self.replicated(Lane::Control, "upsert_net_policy", move |s| {
            s.upsert_net_policy_apply(object.clone())
        })
    }

    fn upsert_net_policy_apply(&self, mut object: NetPolicyObject) -> R<NetPolicyRecord> {
        object
            .normalize_and_validate()
            .map_err(|e| invalid_input(format!("invalid net policy: {e}")))?;
        let existing: Vec<NetPolicyObject> = self
            .list_net_policies()?
            .into_iter()
            .map(|record| record.object)
            .collect();
        if let Some(other) = find_default_tie(&object, &existing) {
            return Err(invalid_input(format!(
                "net policy '{}' would tie with default policy '{}' (both priority {}, overlapping \
                 selectors); give one a different priority so the winner is never a coin flip",
                object.name, other.name, object.priority
            )));
        }
        let body = serde_json::to_string(&object)?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT INTO net_policy (name, priority, enforcement, body, created_at, updated_at)
             VALUES (:name, :priority, :enforcement, :body, :now, :now)
             ON CONFLICT(name) DO UPDATE SET
                priority = excluded.priority,
                enforcement = excluded.enforcement,
                body = excluded.body,
                updated_at = excluded.updated_at;",
            named_params! {
                ":name": object.name,
                ":priority": object.priority,
                ":enforcement": object.enforcement,
                ":body": body,
                ":now": now,
            },
        )?;
        conn.execute(
            "UPDATE net_policy_meta SET generation = generation + 1 WHERE id = 1;",
            [],
        )?;
        conn.query_row(
            "SELECT body, created_at, updated_at FROM net_policy WHERE name = ?;",
            [object.name.as_str()],
            Self::net_policy_from_row,
        )
        .map_err(Into::into)
    }

    #[instrument(err, skip_all, fields(policy = name))]
    pub fn get_net_policy(&self, name: &str) -> R<Option<NetPolicyRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT body, created_at, updated_at FROM net_policy WHERE name = ?;",
            [name],
            Self::net_policy_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Every policy, `require` first then by priority descending then name —
    /// the order the merge applies them, so a listing reads as the merge.
    #[instrument(err, skip_all)]
    pub fn list_net_policies(&self) -> R<Vec<NetPolicyRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT body, created_at, updated_at FROM net_policy
             ORDER BY CASE enforcement WHEN 'require' THEN 0 ELSE 1 END, priority DESC, name ASC;",
        )?;
        let rows = stmt.query_map([], Self::net_policy_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(policy = name))]
    pub fn delete_net_policy(&self, name: &str) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_net_policy", move |s| {
            s.delete_net_policy_apply(&name)
        })
    }

    fn delete_net_policy_apply(&self, name: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM net_policy WHERE name = ?;", [name])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "net_policy",
                name,
            )));
        }
        conn.execute(
            "UPDATE net_policy_meta SET generation = generation + 1 WHERE id = 1;",
            [],
        )?;
        Ok(())
    }

    /// Monotonic counter bumped on every policy write. `0` on a cluster that
    /// has never had a policy — the heartbeat builder's zero-cost fast path.
    pub fn net_policy_generation(&self) -> R<u64> {
        let conn = self.read_connection()?;
        let generation: i64 = conn
            .query_row(
                "SELECT generation FROM net_policy_meta WHERE id = 1;",
                [],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(generation.max(0) as u64)
    }

    /// How many policies exist — with the generation, enough for the
    /// heartbeat fast path to skip the merge entirely on clusters that have
    /// no policies and no task declaring one.
    pub fn net_policy_count(&self) -> R<u64> {
        let conn = self.read_connection()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM net_policy;", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }
}

fn invalid_input(message: String) -> anyhow::Error {
    anyhow::Error::new(StorageError::InvalidInput { message })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use serde_json::json;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-net-policy-store-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        Store::start_storage(&store_config)
    }

    fn policy(name: &str, enforcement: &str, priority: i64) -> NetPolicyObject {
        serde_json::from_value(json!({
            "name": name, "enforcement": enforcement, "priority": priority,
            "egress": { "allow": ["*.pypi.org"] }
        }))
        .unwrap()
    }

    #[test]
    fn upsert_list_get_delete_roundtrip() -> R<()> {
        let store = setup_store()?;
        assert_eq!(store.net_policy_generation()?, 0);
        assert_eq!(store.net_policy_count()?, 0);

        let saved = store.upsert_net_policy(policy("sandbox", "require", 100))?;
        assert_eq!(saved.object.name, "sandbox");
        assert_eq!(
            saved.object.egress.as_ref().unwrap().policy.as_deref(),
            Some("deny")
        );
        assert_eq!(store.net_policy_generation()?, 1);

        store.upsert_net_policy(policy("team", "default", 5))?;
        let listed = store.list_net_policies()?;
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].object.name, "sandbox", "require sorts first");
        assert!(store.get_net_policy("team")?.is_some());

        // Replace keeps created_at, bumps generation.
        let mut updated = policy("team", "default", 7);
        updated.description = "changed".into();
        let saved = store.upsert_net_policy(updated)?;
        assert_eq!(saved.object.priority, 7);
        assert_eq!(store.net_policy_generation()?, 3);

        store.delete_net_policy("team")?;
        assert!(store.get_net_policy("team")?.is_none());
        assert!(
            store.delete_net_policy("team").is_err(),
            "deleting twice is not found"
        );
        assert_eq!(store.net_policy_count()?, 1);
        Ok(())
    }

    #[test]
    fn invalid_objects_and_default_ties_are_rejected_at_apply() -> R<()> {
        let store = setup_store()?;
        let bad: NetPolicyObject = serde_json::from_value(json!({ "name": "x" })).unwrap();
        let err = store.upsert_net_policy(bad).unwrap_err().to_string();
        assert!(err.contains("nothing to apply"), "{err}");

        store.upsert_net_policy(policy("a", "default", 5))?;
        let err = store
            .upsert_net_policy(policy("b", "default", 5))
            .unwrap_err()
            .to_string();
        assert!(err.contains("would tie"), "{err}");
        // A different priority, or a require policy, is fine.
        store.upsert_net_policy(policy("b", "default", 6))?;
        store.upsert_net_policy(policy("c", "require", 5))?;
        Ok(())
    }
}
