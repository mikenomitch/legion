//! Standalone scaling policies (`legion scaling apply`; docs/autoscaling.md,
//! "Standalone policies"; autoscaler-v2 plan §6.2, calls entry 8): one row
//! per policy in `scaling_policy`, on the Control lane with the rest of the
//! scaling state.
//!
//! - **As written.** `spec` is the policy's JSON exactly as applied; the
//!   effective policy is resolved live, like a task's `autoscale`, so a
//!   defaults change reaches it with no re-apply.
//! - **Versions** are values of the `scaling_policy` counter in
//!   `scaling_counter`: they never repeat, across a delete and a recreate.
//!   Applying the same spec again writes nothing and keeps the version.
//! - **Delete** removes the policy's pause with it. Its history stays,
//!   capped like a task's.

use rusqlite::{params, Connection, OptionalExtension, Row};

use super::*;

/// The counter policy versions come from.
const SCALING_POLICY_COUNTER: &str = "scaling_policy";

/// One `scaling_policy` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalingPolicyRow {
    /// The namespace; empty for a cluster-scoped policy (a node group).
    pub namespace: String,
    pub name: String,
    /// `external` or `nodes`.
    pub kind: String,
    pub version: u64,
    /// The policy as written (JSON).
    pub spec: String,
    pub description: String,
    pub created_by: String,
    pub created_ms: i64,
    pub updated_by: String,
    pub updated_ms: i64,
}

impl ScalingPolicyRow {
    /// The canonical id: `name` in `default` (and for a node group),
    /// `namespace/name` elsewhere.
    pub fn id(&self) -> String {
        crate::autoscaler::policy::policy_id(&self.namespace, &self.name)
    }
}

/// What an apply did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutScalingPolicy {
    /// The version now stored.
    pub version: u64,
    /// The policy did not exist.
    pub created: bool,
    /// The spec changed (false: the same spec again, nothing written).
    pub changed: bool,
    /// The version before, when there was one.
    pub previous: Option<u64>,
}

const COLUMNS: &str = "namespace, name, kind, version, spec, description, created_by, \
                       created_ms, updated_by, updated_ms";

fn from_row(row: &Row<'_>) -> Result<ScalingPolicyRow, rusqlite::Error> {
    Ok(ScalingPolicyRow {
        namespace: row.get(0)?,
        name: row.get(1)?,
        kind: row.get(2)?,
        version: row.get::<_, i64>(3)?.max(0) as u64,
        spec: row.get(4)?,
        description: row.get(5)?,
        created_by: row.get(6)?,
        created_ms: row.get(7)?,
        updated_by: row.get(8)?,
        updated_ms: row.get(9)?,
    })
}

impl Store {
    fn scaling_policy_tx(
        conn: &Connection,
        namespace: &str,
        name: &str,
    ) -> R<Option<ScalingPolicyRow>> {
        Ok(conn
            .prepare_cached(&format!(
                "SELECT {COLUMNS} FROM scaling_policy WHERE namespace = ?1 AND name = ?2;"
            ))?
            .query_row(params![namespace, name], from_row)
            .optional()?)
    }

    /// Every standalone policy, by namespace and name.
    pub fn scaling_policies(&self) -> R<Vec<ScalingPolicyRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {COLUMNS} FROM scaling_policy ORDER BY namespace, name;"
        ))?;
        let rows = stmt.query_map([], from_row)?;
        Self::collect_rows(rows)
    }

    /// One standalone policy.
    pub fn scaling_policy(&self, namespace: &str, name: &str) -> R<Option<ScalingPolicyRow>> {
        let conn = self.read_connection()?;
        Self::scaling_policy_tx(&conn, namespace, name)
    }

    /// Create or replace a standalone policy. Applying the spec it already
    /// has writes nothing.
    #[allow(clippy::too_many_arguments)]
    #[instrument(err, skip(self, spec, description))]
    pub fn put_scaling_policy(
        &self,
        namespace: &str,
        name: &str,
        kind: &str,
        spec: &str,
        description: &str,
        principal: &str,
        now_ms: i64,
    ) -> R<PutScalingPolicy> {
        let (namespace, name, kind, spec, description, principal) = (
            namespace.to_owned(),
            name.to_owned(),
            kind.to_owned(),
            spec.to_owned(),
            description.to_owned(),
            principal.to_owned(),
        );
        self.replicated(Lane::Control, "put_scaling_policy", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::scaling_policy_tx(&tx, &namespace, &name)?;
            if let Some(row) = &current {
                if row.spec == spec && row.kind == kind && row.description == description {
                    return Ok(PutScalingPolicy {
                        version: row.version,
                        created: false,
                        changed: false,
                        previous: Some(row.version),
                    });
                }
            }
            let version = Self::next_scaling_counter_tx(&tx, SCALING_POLICY_COUNTER)?;
            tx.execute(
                "INSERT INTO scaling_policy (namespace, name, kind, version, spec, description, \
                 created_by, created_ms, updated_by, updated_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?7, ?8) \
                 ON CONFLICT(namespace, name) DO UPDATE SET kind = ?3, version = ?4, spec = ?5, \
                 description = ?6, updated_by = ?7, updated_ms = ?8;",
                params![
                    namespace,
                    name,
                    kind,
                    version as i64,
                    spec,
                    description,
                    principal,
                    now_ms
                ],
            )?;
            tx.commit()?;
            Ok(PutScalingPolicy {
                version,
                created: current.is_none(),
                changed: true,
                previous: current.map(|row| row.version),
            })
        })
    }

    /// Delete a standalone policy and its pause. Returns the deleted row.
    #[instrument(err, skip(self))]
    pub fn delete_scaling_policy(
        &self,
        namespace: &str,
        name: &str,
    ) -> R<Option<ScalingPolicyRow>> {
        let (namespace, name) = (namespace.to_owned(), name.to_owned());
        self.replicated(Lane::Control, "delete_scaling_policy", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::scaling_policy_tx(&tx, &namespace, &name)?;
            if let Some(row) = &current {
                tx.execute(
                    "DELETE FROM scaling_policy WHERE namespace = ?1 AND name = ?2;",
                    params![namespace, name],
                )?;
                let key = crate::manager::autoscale::policy_target_key(&row.id());
                tx.execute("DELETE FROM scaling_pause WHERE job_id = ?1;", [&key])?;
            }
            tx.commit()?;
            Ok(current)
        })
    }

    /// A cheap fingerprint of the standalone policies: their count and the
    /// highest version (versions only grow, so any apply or delete changes
    /// one of them).
    pub fn scaling_policy_fingerprint(&self) -> R<(i64, i64)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM scaling_policy;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    /// A cheap fingerprint of the pauses: their count and newest creation.
    pub fn scaling_pause_fingerprint(&self) -> R<(i64, i64)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(created_ms), 0) FROM scaling_pause;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
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

    #[test]
    fn versions_grow_and_an_unchanged_apply_writes_nothing_test() -> R<()> {
        let (_dir, store) = store()?;
        let first =
            store.put_scaling_policy("media", "render", "external", "{\"a\":1}", "", "alice", 1)?;
        assert!(first.created && first.changed);
        let same =
            store.put_scaling_policy("media", "render", "external", "{\"a\":1}", "", "bob", 2)?;
        assert_eq!(same.version, first.version);
        assert!(!same.changed && !same.created);
        let next =
            store.put_scaling_policy("media", "render", "external", "{\"a\":2}", "", "bob", 3)?;
        assert!(next.version > first.version && next.changed && !next.created);
        assert_eq!(next.previous, Some(first.version));
        let row = store.scaling_policy("media", "render")?.context("stored")?;
        assert_eq!(
            (row.created_by.as_str(), row.updated_by.as_str()),
            ("alice", "bob")
        );
        assert_eq!(row.id(), "media/render");

        // A delete takes the pause with it; a recreate gets a new version.
        let key = crate::manager::autoscale::policy_target_key("media/render");
        store.set_scaling_pause(&key, "", "", None, "maintenance", "alice", 4)?;
        assert_eq!(store.scaling_pauses()?.len(), 1);
        let deleted = store
            .delete_scaling_policy("media", "render")?
            .context("deleted")?;
        assert_eq!(deleted.version, next.version);
        assert!(store.scaling_pauses()?.is_empty());
        assert!(store.delete_scaling_policy("media", "render")?.is_none());
        let again =
            store.put_scaling_policy("media", "render", "external", "{\"a\":1}", "", "carol", 5)?;
        assert!(again.created && again.version > next.version);
        assert_eq!(
            store.scaling_policy_fingerprint()?,
            (1, again.version as i64)
        );
        Ok(())
    }
}
