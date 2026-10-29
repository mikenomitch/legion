//! Objects of app-declared kinds (`legion object`; docs/app-objects.md;
//! app-object-kinds plan §6.3): one `app_object` row per object, on the
//! Control lane.
//!
//! - **As written.** `spec` is the JSON exactly as applied; defaults and
//!   validity are worked out on read against the kind installed now.
//! - **Generation and version.** `generation` starts at 1 and bumps when
//!   the spec changes. `version` is a value of the `app_object` counter in
//!   `scaling_counter`, new on every write of the row (spec, labels, or
//!   status), so it never repeats, across a delete and a recreate: it is
//!   the compare-and-set token. Applying what is stored writes nothing and
//!   keeps both.
//! - **Status** is the owning app's: written by `update_app_object_status`
//!   only, which never touches the spec.

use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension, Row};

use super::*;

/// The counter object versions come from.
const APP_OBJECT_COUNTER: &str = "app_object";

/// Which object: its app, kind (as declared), namespace (empty for a
/// cluster kind), and name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub app: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

/// One `app_object` row.
#[derive(Debug, Clone, PartialEq)]
pub struct AppObjectRow {
    pub key: ObjectKey,
    /// The spec as written (JSON).
    pub spec: String,
    pub labels: BTreeMap<String, String>,
    /// The status the app last wrote (JSON), empty for none.
    pub status: String,
    pub observed_generation: u64,
    pub generation: u64,
    pub version: u64,
    pub created_by: String,
    pub created_ms: i64,
    pub updated_by: String,
    pub updated_ms: i64,
    pub status_updated_by: String,
    pub status_updated_ms: i64,
    /// `<app>@<manifest digest>` that checked the spec at its last write.
    pub validated_by: String,
}

/// A write's compare-and-set condition on the stored version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCas {
    /// Write whatever is stored.
    Any,
    /// Only when nothing is stored (`expected_version = 0`).
    Missing,
    /// Only when the stored version is this one.
    Version(u64),
}

impl VersionCas {
    /// From a request's optional `expected_version`.
    pub fn from_expected(expected: Option<u64>) -> Self {
        match expected {
            None => VersionCas::Any,
            Some(0) => VersionCas::Missing,
            Some(version) => VersionCas::Version(version),
        }
    }

    fn holds(&self, current: Option<&AppObjectRow>) -> bool {
        match (self, current) {
            (VersionCas::Any, _) => true,
            (VersionCas::Missing, current) => current.is_none(),
            (VersionCas::Version(version), Some(row)) => row.version == *version,
            (VersionCas::Version(_), None) => false,
        }
    }
}

/// What an apply did.
#[derive(Debug, Clone, PartialEq)]
pub enum PutObject {
    /// Written: created, or a changed spec or labels.
    Written {
        row: AppObjectRow,
        previous: Option<AppObjectRow>,
    },
    /// The spec and labels were already stored: nothing written.
    Unchanged { row: AppObjectRow },
    /// The compare-and-set failed; the stored row, if any.
    Conflict { current: Option<AppObjectRow> },
}

/// What a delete did.
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteObject {
    Deleted(AppObjectRow),
    Missing,
    Conflict(AppObjectRow),
}

/// A status write's compare-and-set condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCas {
    Any,
    /// The stored version must be this one.
    Version(u64),
    /// The stored generation must be this one: "this status is for the
    /// spec I saw".
    Generation(u64),
}

/// What a status write did.
#[derive(Debug, Clone, PartialEq)]
pub enum PutStatus {
    /// Written; `changed` is false when the status and observed generation
    /// were already stored (the version still moves).
    Written { row: AppObjectRow, changed: bool },
    Missing,
    Conflict(AppObjectRow),
}

/// The new spec and labels an apply writes.
#[derive(Debug, Clone)]
pub struct ObjectWrite {
    pub key: ObjectKey,
    pub spec: String,
    pub labels: BTreeMap<String, String>,
    pub principal: String,
    pub now_ms: i64,
    pub validated_by: String,
    pub cas: VersionCas,
}

const COLUMNS: &str = "app, kind, namespace, name, spec, labels, status, observed_generation, \
                       generation, version, created_by, created_ms, updated_by, updated_ms, \
                       status_updated_by, status_updated_ms, validated_by";

fn from_row(row: &Row<'_>) -> Result<AppObjectRow, rusqlite::Error> {
    let labels: String = row.get(5)?;
    Ok(AppObjectRow {
        key: ObjectKey {
            app: row.get(0)?,
            kind: row.get(1)?,
            namespace: row.get(2)?,
            name: row.get(3)?,
        },
        spec: row.get(4)?,
        labels: serde_json::from_str(&labels).unwrap_or_default(),
        status: row.get(6)?,
        observed_generation: row.get::<_, i64>(7)?.max(0) as u64,
        generation: row.get::<_, i64>(8)?.max(0) as u64,
        version: row.get::<_, i64>(9)?.max(0) as u64,
        created_by: row.get(10)?,
        created_ms: row.get(11)?,
        updated_by: row.get(12)?,
        updated_ms: row.get(13)?,
        status_updated_by: row.get(14)?,
        status_updated_ms: row.get(15)?,
        validated_by: row.get(16)?,
    })
}

/// Which rows a listing reads.
#[derive(Debug, Clone, Default)]
pub struct ObjectFilter {
    pub app: Option<String>,
    /// The kind as declared; needs `app`.
    pub kind: Option<String>,
}

impl Store {
    fn app_object_tx(conn: &Connection, key: &ObjectKey) -> R<Option<AppObjectRow>> {
        Ok(conn
            .prepare_cached(&format!(
                "SELECT {COLUMNS} FROM app_object \
                 WHERE app = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4;"
            ))?
            .query_row(
                params![key.app, key.kind, key.namespace, key.name],
                from_row,
            )
            .optional()?)
    }

    /// One object.
    pub fn app_object(&self, key: &ObjectKey) -> R<Option<AppObjectRow>> {
        let conn = self.read_connection()?;
        Self::app_object_tx(&conn, key)
    }

    /// Objects, by app, kind, namespace, and name: every one, an app's, or
    /// one kind's.
    pub fn app_objects(&self, filter: &ObjectFilter) -> R<Vec<AppObjectRow>> {
        let conn = self.read_connection()?;
        let (sql, args): (String, Vec<String>) = match (&filter.app, &filter.kind) {
            (Some(app), Some(kind)) => (
                format!(
                    "SELECT {COLUMNS} FROM app_object WHERE app = ?1 AND kind = ?2 \
                     ORDER BY app, kind, namespace, name;"
                ),
                vec![app.clone(), kind.clone()],
            ),
            (Some(app), None) => (
                format!(
                    "SELECT {COLUMNS} FROM app_object WHERE app = ?1 \
                     ORDER BY app, kind, namespace, name;"
                ),
                vec![app.clone()],
            ),
            _ => (
                format!("SELECT {COLUMNS} FROM app_object ORDER BY app, kind, namespace, name;"),
                Vec::new(),
            ),
        };
        let mut stmt = conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(args.iter()), from_row)?;
        Self::collect_rows(rows)
    }

    /// How many objects each (app, kind) holds.
    pub fn app_object_counts(&self) -> R<BTreeMap<(String, String), u64>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT app, kind, COUNT(*) FROM app_object GROUP BY app, kind ORDER BY app, kind;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                row.get::<_, i64>(2)?.max(0) as u64,
            ))
        })?;
        let mut counts = BTreeMap::new();
        for row in rows {
            let (key, count) = row?;
            counts.insert(key, count);
        }
        Ok(counts)
    }

    /// A cheap fingerprint of the objects: their count and the highest
    /// version (versions only grow, so any write or delete changes one).
    pub fn app_object_fingerprint(&self) -> R<(i64, i64)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM app_object;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    /// Create or update an object's spec and labels, compare-and-set on
    /// `write.cas`. Writing what is stored writes nothing.
    #[instrument(err, skip_all, fields(app = %write.key.app, kind = %write.key.kind, namespace = %write.key.namespace, name = %write.key.name))]
    pub fn put_app_object(&self, write: ObjectWrite) -> R<PutObject> {
        self.replicated(Lane::Control, "put_app_object", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::app_object_tx(&tx, &write.key)?;
            if !write.cas.holds(current.as_ref()) {
                return Ok(PutObject::Conflict { current });
            }
            if let Some(row) = &current {
                if row.spec == write.spec && row.labels == write.labels {
                    return Ok(PutObject::Unchanged { row: row.clone() });
                }
            }
            let version = Self::next_scaling_counter_tx(&tx, APP_OBJECT_COUNTER)?;
            let labels = serde_json::to_string(&write.labels)?;
            let generation = match &current {
                None => 1,
                Some(row) if row.spec != write.spec => row.generation + 1,
                Some(row) => row.generation,
            };
            let validated_by = match &current {
                Some(row) if row.spec == write.spec => row.validated_by.clone(),
                _ => write.validated_by.clone(),
            };
            tx.execute(
                "INSERT INTO app_object (app, kind, namespace, name, spec, labels, generation, \
                 version, created_by, created_ms, updated_by, updated_ms, validated_by) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?9, ?10, ?11) \
                 ON CONFLICT(app, kind, namespace, name) DO UPDATE SET spec = ?5, labels = ?6, \
                 generation = ?7, version = ?8, updated_by = ?9, updated_ms = ?10, \
                 validated_by = ?11;",
                params![
                    write.key.app,
                    write.key.kind,
                    write.key.namespace,
                    write.key.name,
                    write.spec,
                    labels,
                    generation as i64,
                    version as i64,
                    write.principal,
                    write.now_ms,
                    validated_by,
                ],
            )?;
            let row = Self::app_object_tx(&tx, &write.key)?
                .ok_or_else(|| anyhow!("the object vanished inside its own write"))?;
            tx.commit()?;
            Ok(PutObject::Written {
                row,
                previous: current,
            })
        })
    }

    /// Delete an object, compare-and-set on `expected` (a version).
    #[instrument(err, skip(self))]
    pub fn delete_app_object(&self, key: &ObjectKey, expected: Option<u64>) -> R<DeleteObject> {
        let key = key.clone();
        self.replicated(Lane::Control, "delete_app_object", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let Some(row) = Self::app_object_tx(&tx, &key)? else {
                return Ok(DeleteObject::Missing);
            };
            if expected.is_some_and(|version| version != row.version) {
                return Ok(DeleteObject::Conflict(row));
            }
            tx.execute(
                "DELETE FROM app_object WHERE app = ?1 AND kind = ?2 AND namespace = ?3 \
                 AND name = ?4;",
                params![key.app, key.kind, key.namespace, key.name],
            )?;
            tx.commit()?;
            Ok(DeleteObject::Deleted(row))
        })
    }

    /// Write an object's status and observed generation (the owning app's
    /// report), compare-and-set on `cas`. Never touches the spec or the
    /// generation; the version moves.
    #[allow(clippy::too_many_arguments)]
    #[instrument(err, skip(self, status))]
    pub fn update_app_object_status(
        &self,
        key: &ObjectKey,
        status: &str,
        observed_generation: u64,
        cas: StatusCas,
        principal: &str,
        now_ms: i64,
    ) -> R<PutStatus> {
        let (key, status, principal) = (key.clone(), status.to_owned(), principal.to_owned());
        self.replicated(Lane::Control, "update_app_object_status", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let Some(row) = Self::app_object_tx(&tx, &key)? else {
                return Ok(PutStatus::Missing);
            };
            let holds = match cas {
                StatusCas::Any => true,
                StatusCas::Version(version) => row.version == version,
                StatusCas::Generation(generation) => row.generation == generation,
            };
            if !holds {
                return Ok(PutStatus::Conflict(row));
            }
            let changed = row.status != status || row.observed_generation != observed_generation;
            let version = Self::next_scaling_counter_tx(&tx, APP_OBJECT_COUNTER)?;
            tx.execute(
                "UPDATE app_object SET status = ?5, observed_generation = ?6, version = ?7, \
                 status_updated_by = ?8, status_updated_ms = ?9 \
                 WHERE app = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4;",
                params![
                    key.app,
                    key.kind,
                    key.namespace,
                    key.name,
                    status,
                    observed_generation as i64,
                    version as i64,
                    principal,
                    now_ms,
                ],
            )?;
            let row = Self::app_object_tx(&tx, &key)?
                .ok_or_else(|| anyhow!("the object vanished inside its own write"))?;
            tx.commit()?;
            Ok(PutStatus::Written { row, changed })
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

    fn key(name: &str) -> ObjectKey {
        ObjectKey {
            app: "gitops".into(),
            kind: "Repo".into(),
            namespace: "team-a".into(),
            name: name.into(),
        }
    }

    fn write(name: &str, spec: &str, cas: VersionCas) -> ObjectWrite {
        ObjectWrite {
            key: key(name),
            spec: spec.into(),
            labels: BTreeMap::new(),
            principal: "alice".into(),
            now_ms: 1,
            validated_by: "gitops@d1".into(),
            cas,
        }
    }

    #[test]
    fn generations_versions_and_compare_and_set_test() -> R<()> {
        let (_dir, store) = store()?;
        let PutObject::Written { row: first, previous } =
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Missing))?
        else {
            panic!("created")
        };
        assert!(previous.is_none());
        assert_eq!((first.generation, first.created_by.as_str()), (1, "alice"));

        // Create-only refuses a second create; the same spec writes nothing.
        assert!(matches!(
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Missing))?,
            PutObject::Conflict { current: Some(_) }
        ));
        let PutObject::Unchanged { row } =
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Any))?
        else {
            panic!("unchanged")
        };
        assert_eq!(row.version, first.version);

        // A stale version refuses; the current one writes and bumps both.
        assert!(matches!(
            store.put_app_object(write("web", "{\"a\":2}", VersionCas::Version(first.version + 99)))?,
            PutObject::Conflict { .. }
        ));
        let PutObject::Written { row: second, .. } =
            store.put_app_object(write("web", "{\"a\":2}", VersionCas::Version(first.version)))?
        else {
            panic!("updated")
        };
        assert_eq!(second.generation, 2);
        assert!(second.version > first.version);

        // Labels alone move the version, not the generation.
        let mut labels = write("web", "{\"a\":2}", VersionCas::Any);
        labels.labels.insert("team".into(), "a".into());
        let PutObject::Written { row: third, .. } = store.put_app_object(labels)? else {
            panic!("labels")
        };
        assert_eq!(third.generation, 2);
        assert!(third.version > second.version);
        assert_eq!(third.labels.get("team").map(String::as_str), Some("a"));

        // Status: CAS on generation; it never touches the spec.
        assert!(matches!(
            store.update_app_object_status(&key("web"), "{\"ok\":true}", 2, StatusCas::Generation(1), "gitops", 2)?,
            PutStatus::Conflict(_)
        ));
        let PutStatus::Written { row: status, changed } = store.update_app_object_status(
            &key("web"),
            "{\"ok\":true}",
            2,
            StatusCas::Generation(2),
            "gitops",
            2,
        )?
        else {
            panic!("status")
        };
        assert!(changed);
        assert_eq!(
            (status.spec.as_str(), status.generation, status.observed_generation),
            ("{\"a\":2}", 2, 2)
        );
        assert!(status.version > third.version);
        let PutStatus::Written { changed, .. } = store.update_app_object_status(
            &key("web"),
            "{\"ok\":true}",
            2,
            StatusCas::Version(status.version),
            "gitops",
            3,
        )?
        else {
            panic!("same status")
        };
        assert!(!changed);
        assert_eq!(
            store.update_app_object_status(&key("gone"), "{}", 1, StatusCas::Any, "gitops", 3)?,
            PutStatus::Missing
        );

        // Delete with CAS; a recreate gets a new, higher version.
        let latest = store.app_object(&key("web"))?.context("stored")?;
        assert!(matches!(
            store.delete_app_object(&key("web"), Some(latest.version + 1))?,
            DeleteObject::Conflict(_)
        ));
        assert!(matches!(
            store.delete_app_object(&key("web"), Some(latest.version))?,
            DeleteObject::Deleted(_)
        ));
        assert_eq!(store.delete_app_object(&key("web"), None)?, DeleteObject::Missing);
        let PutObject::Written { row: again, .. } =
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Any))?
        else {
            panic!("recreated")
        };
        assert!(again.version > latest.version);
        assert_eq!(again.generation, 1);
        assert_eq!(store.app_object_fingerprint()?, (1, again.version as i64));
        Ok(())
    }

    #[test]
    fn listings_and_counts_test() -> R<()> {
        let (_dir, store) = store()?;
        store.put_app_object(write("a", "{}", VersionCas::Any))?;
        store.put_app_object(write("b", "{}", VersionCas::Any))?;
        let mut other = write("c", "{}", VersionCas::Any);
        other.key.app = "certs".into();
        other.key.kind = "Certificate".into();
        other.key.namespace = String::new();
        store.put_app_object(other)?;
        assert_eq!(store.app_objects(&ObjectFilter::default())?.len(), 3);
        let gitops = store.app_objects(&ObjectFilter {
            app: Some("gitops".into()),
            kind: Some("Repo".into()),
        })?;
        assert_eq!(
            gitops.iter().map(|row| row.key.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        let counts = store.app_object_counts()?;
        assert_eq!(counts.get(&("gitops".into(), "Repo".into())), Some(&2));
        assert_eq!(counts.get(&("certs".into(), "Certificate".into())), Some(&1));
        Ok(())
    }
}
