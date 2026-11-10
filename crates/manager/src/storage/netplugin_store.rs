//! Storage for network plugin objects (`legion net plugin`).
//!
//! A plugin is stored as its canonical JSON (`NetPluginObject`, already
//! normalized) plus the columns the list view needs. Writes are
//! raft-replicated on the control lane, like policy objects; reads are local.
//! Every write bumps `net_plugin_meta.generation`, and the row stored by a
//! write takes the new generation as its `revision`: a revision names one
//! version of one plugin forever, so a node holding `(name, revision)` knows
//! whether what it runs is current.

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::netplugin::NetPluginObject;

use rusqlite::{named_params, OptionalExtension, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// One stored plugin: the object plus bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetPluginRecord {
    pub object: NetPluginObject,
    /// The cluster-wide plugin generation this version was written at.
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
}

impl Store {
    fn net_plugin_from_row(row: &Row<'_>) -> rusqlite::Result<NetPluginRecord> {
        let body: String = row.get(0)?;
        let object: NetPluginObject = serde_json::from_str(&body).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?;
        let revision: i64 = row.get(1)?;
        Ok(NetPluginRecord {
            object,
            revision: revision.max(0) as u64,
            created_at: row.get(2)?,
            updated_at: row.get(3)?,
        })
    }

    /// Create or replace a plugin. The object is normalized and validated
    /// here (the CLI and the RPC already did, but the store is the gate).
    #[instrument(err, skip_all, fields(plugin = %object.name))]
    pub fn upsert_net_plugin(&self, object: NetPluginObject) -> R<NetPluginRecord> {
        self.replicated(Lane::Control, "upsert_net_plugin", move |s| {
            s.upsert_net_plugin_apply(object.clone())
        })
    }

    fn upsert_net_plugin_apply(&self, mut object: NetPluginObject) -> R<NetPluginRecord> {
        object.normalize_and_validate().map_err(|e| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("invalid net plugin: {e}"),
            })
        })?;
        let body = serde_json::to_string(&object)?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        conn.execute(
            "UPDATE net_plugin_meta SET generation = generation + 1 WHERE id = 1;",
            [],
        )?;
        let revision: i64 = conn.query_row(
            "SELECT generation FROM net_plugin_meta WHERE id = 1;",
            [],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO net_plugin (name, usable_by, revision, body, created_at, updated_at)
             VALUES (:name, :usable_by, :revision, :body, :now, :now)
             ON CONFLICT(name) DO UPDATE SET
                usable_by = excluded.usable_by,
                revision = excluded.revision,
                body = excluded.body,
                updated_at = excluded.updated_at;",
            named_params! {
                ":name": object.name,
                ":usable_by": object.usable_by,
                ":revision": revision,
                ":body": body,
                ":now": now,
            },
        )?;
        conn.query_row(
            "SELECT body, revision, created_at, updated_at FROM net_plugin WHERE name = ?;",
            [object.name.as_str()],
            Self::net_plugin_from_row,
        )
        .map_err(Into::into)
    }

    #[instrument(err, skip_all, fields(plugin = name))]
    pub fn get_net_plugin(&self, name: &str) -> R<Option<NetPluginRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT body, revision, created_at, updated_at FROM net_plugin WHERE name = ?;",
            [name],
            Self::net_plugin_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Every plugin, by name.
    #[instrument(err, skip_all)]
    pub fn list_net_plugins(&self) -> R<Vec<NetPluginRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT body, revision, created_at, updated_at FROM net_plugin ORDER BY name ASC;",
        )?;
        let rows = stmt.query_map([], Self::net_plugin_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(plugin = name))]
    pub fn delete_net_plugin(&self, name: &str) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_net_plugin", move |s| {
            s.delete_net_plugin_apply(&name)
        })
    }

    fn delete_net_plugin_apply(&self, name: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM net_plugin WHERE name = ?;", [name])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "net_plugin",
                name,
            )));
        }
        conn.execute(
            "UPDATE net_plugin_meta SET generation = generation + 1 WHERE id = 1;",
            [],
        )?;
        Ok(())
    }

    /// Monotonic counter bumped on every plugin write; `0` on a cluster that
    /// has never had one.
    pub fn net_plugin_generation(&self) -> R<u64> {
        let conn = self.read_connection()?;
        let generation: i64 = conn
            .query_row(
                "SELECT generation FROM net_plugin_meta WHERE id = 1;",
                [],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(generation.max(0) as u64)
    }
}

#[cfg(test)]
mod netplugin_store_test {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-net-plugin-store-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();
        let config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        Store::start_storage(&config)
    }

    fn plugin(name: &str) -> NetPluginObject {
        serde_json::from_value(json!({
            "name": name,
            "module": {
                "source": "https://example.com/p.wasm",
                "checksum": format!("sha256:{}", "b".repeat(64)),
            },
            "stages": ["request"],
        }))
        .unwrap()
    }

    #[test]
    fn plugins_round_trip_with_revisions_that_never_repeat_test() -> R<()> {
        let store = setup_store()?;
        assert_eq!(store.net_plugin_generation()?, 0);
        let first = store.upsert_net_plugin(plugin("a"))?;
        assert_eq!(first.revision, 1);
        assert_eq!(first.object.limits.deadline.as_deref(), Some("10ms"));
        let second = store.upsert_net_plugin(plugin("a"))?;
        assert_eq!(second.revision, 2);
        assert_eq!(second.created_at, first.created_at);
        store.upsert_net_plugin(plugin("b"))?;
        assert_eq!(
            store
                .list_net_plugins()?
                .iter()
                .map(|r| r.object.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        store.delete_net_plugin("a")?;
        assert!(store.get_net_plugin("a")?.is_none());
        let again = store.upsert_net_plugin(plugin("a"))?;
        assert!(
            again.revision > second.revision,
            "a re-created plugin gets a new revision"
        );
        assert!(store.delete_net_plugin("missing").is_err());
        let mut bad = plugin("c");
        bad.stages = vec!["nope".into()];
        assert!(store.upsert_net_plugin(bad).is_err());
        Ok(())
    }
}
