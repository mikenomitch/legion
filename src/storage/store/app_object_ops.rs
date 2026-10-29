//! App-owned object kinds in the store (`legion object`;
//! docs/app-objects.md; app-object-kinds plan §6.2-§6.9).
//!
//! - **`object_kind`** (Control lane): the kinds installed apps declare,
//!   synced from the app records ([`Store::sync_object_kinds`]); a kind
//!   whose app is gone is kept `orphaned` while it has objects.
//! - **`app_object`** (Control lane): one row per object. `uid` never
//!   repeats; `spec` holds the defaults of its write-time schema;
//!   `generation` bumps when the spec changes; `spec_version`, the CAS
//!   token, is new on every write of the row and never on a status write.
//!   Both come from the `app_object` counter in `scaling_counter`.
//! - **`app_object_status`** (Status lane): the owning app's report, keyed
//!   by `uid`. Identical reports are dropped before they reach raft, and an
//!   object's observed generation never goes backwards.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, OptionalExtension, Row};

use super::*;

/// The counter object uids and spec versions come from.
const APP_OBJECT_COUNTER: &str = "app_object";
/// The counter kind revisions come from.
const OBJECT_KIND_COUNTER: &str = "object_kind";
/// How long a status row whose object is gone waits before the sweep
/// removes it.
const STATUS_ORPHAN_GRACE_MS: i64 = 5 * 60 * 1000;

/// Which object: its app, kind (as declared), namespace (empty for a
/// cluster kind), and name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub app: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

/// One `object_kind` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectKindRow {
    pub app: String,
    pub kind: String,
    /// The declaration (JSON).
    pub decl: String,
    pub digest: String,
    /// `active` or `orphaned`.
    pub state: String,
    pub revision: u64,
    pub created_ms: i64,
    pub updated_ms: i64,
}

/// A kind an app's record declares, as the sync writes it.
#[derive(Debug, Clone, PartialEq)]
pub struct KindWrite {
    pub kind: String,
    pub decl: String,
    pub digest: String,
}

/// One `app_object` row.
#[derive(Debug, Clone, PartialEq)]
pub struct AppObjectRow {
    pub key: ObjectKey,
    pub uid: u64,
    /// The spec, defaults of its write-time schema filled (JSON).
    pub spec: String,
    pub labels: BTreeMap<String, String>,
    pub generation: u64,
    pub spec_version: u64,
    /// The permissions its writer was checked for (JSON).
    pub authority: String,
    /// The app whose own identity last wrote it, or empty.
    pub managed_by: String,
    /// When a bounded cleanup started (Unix ms), 0 for none; and its
    /// deadline.
    pub deleting_since: i64,
    pub cleanup_deadline: i64,
    /// Its watch shard among [`crate::structs::app_object::STORED_SHARD_COUNT`].
    pub shard: u32,
    pub schema_digest: String,
    pub created_by: String,
    pub created_ms: i64,
    pub updated_by: String,
    pub updated_ms: i64,
}

impl AppObjectRow {
    pub fn is_deleting(&self) -> bool {
        self.deleting_since > 0
    }
}

/// One `app_object_status` row.
#[derive(Debug, Clone, PartialEq)]
pub struct AppObjectStatusRow {
    pub uid: u64,
    pub app: String,
    pub observed_generation: u64,
    /// JSON list of conditions.
    pub conditions: String,
    /// JSON object.
    pub fields: String,
    pub updated_by: String,
    pub updated_ms: i64,
}

/// A write's compare-and-set condition on the stored spec version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCas {
    /// Write whatever is stored.
    Any,
    /// Only when nothing is stored (`expected_version = 0`).
    Missing,
    /// Only when the stored spec version is this one.
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

    pub fn holds(&self, current: Option<&AppObjectRow>) -> bool {
        match (self, current) {
            (VersionCas::Any, _) => true,
            (VersionCas::Missing, current) => current.is_none(),
            (VersionCas::Version(version), Some(row)) => row.spec_version == *version,
            (VersionCas::Version(_), None) => false,
        }
    }
}

/// The spec, labels, and authority an apply writes.
#[derive(Debug, Clone)]
pub struct ObjectWrite {
    pub key: ObjectKey,
    pub spec: String,
    pub labels: BTreeMap<String, String>,
    pub authority: String,
    pub managed_by: String,
    pub schema_digest: String,
    pub shard: u32,
    pub principal: String,
    pub now_ms: i64,
    pub cas: VersionCas,
    /// The kind's `maxObjects`, checked on create.
    pub max_objects: u64,
}

/// What an apply did.
#[derive(Debug, Clone, PartialEq)]
pub enum PutObject {
    /// Written: created (`created`), or a changed spec, labels, or
    /// authority.
    Written { row: AppObjectRow, created: bool },
    /// Already stored: nothing written.
    Unchanged { row: AppObjectRow },
    /// The compare-and-set failed; the stored row, if any.
    Conflict { current: Option<AppObjectRow> },
    /// The object is being deleted: its spec is frozen.
    Deleting { row: AppObjectRow },
    /// Creating it would pass the kind's `maxObjects`.
    TooMany { count: u64 },
}

/// What a delete did.
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteObject {
    Deleted(AppObjectRow),
    /// A bounded cleanup started: the row stays until the app releases it
    /// or the deadline passes.
    Deleting(AppObjectRow),
    /// A cleanup was already running.
    AlreadyDeleting(AppObjectRow),
    Missing,
    Conflict(AppObjectRow),
}

/// What a release did.
#[derive(Debug, Clone, PartialEq)]
pub enum ReleaseObject {
    Released(AppObjectRow),
    /// The object is not being deleted.
    NotDeleting(AppObjectRow),
    /// No object of that name, or one with another uid (deleted and
    /// recreated).
    Missing,
}

/// A status report, as `update_app_object_status` writes it.
#[derive(Debug, Clone)]
pub struct StatusWrite {
    pub key: ObjectKey,
    pub uid: u64,
    pub observed_generation: u64,
    pub conditions: String,
    pub fields: String,
    pub principal: String,
    pub now_ms: i64,
}

/// What a status report did.
#[derive(Debug, Clone, PartialEq)]
pub enum PutStatus {
    Written {
        row: AppObjectStatusRow,
        previous: Option<AppObjectStatusRow>,
    },
    /// Identical to what is stored: dropped without a raft proposal.
    Unchanged(AppObjectStatusRow),
    /// No object of that name with that uid on this replica.
    Missing,
    /// The report is for a generation this replica has not seen.
    Ahead { generation: u64 },
    /// The stored report is for a later generation.
    Behind { stored: u64 },
}

/// Which rows a listing reads.
#[derive(Debug, Clone, Default)]
pub struct ObjectFilter {
    pub app: Option<String>,
    /// The kind as declared; needs `app`.
    pub kind: Option<String>,
    /// Only these stored shards (among 64); needs `app`.
    pub shards: Option<BTreeSet<u32>>,
}

const KIND_COLUMNS: &str = "app, kind, decl, digest, state, revision, created_ms, updated_ms";

fn kind_from_row(row: &Row<'_>) -> Result<ObjectKindRow, rusqlite::Error> {
    Ok(ObjectKindRow {
        app: row.get(0)?,
        kind: row.get(1)?,
        decl: row.get(2)?,
        digest: row.get(3)?,
        state: row.get(4)?,
        revision: row.get::<_, i64>(5)?.max(0) as u64,
        created_ms: row.get(6)?,
        updated_ms: row.get(7)?,
    })
}

const COLUMNS: &str = "app, kind, namespace, name, uid, spec, labels, generation, spec_version, \
                       authority, managed_by, deleting_since, cleanup_deadline, shard, \
                       schema_digest, created_by, created_ms, updated_by, updated_ms";

fn from_row(row: &Row<'_>) -> Result<AppObjectRow, rusqlite::Error> {
    let labels: String = row.get(6)?;
    Ok(AppObjectRow {
        key: ObjectKey {
            app: row.get(0)?,
            kind: row.get(1)?,
            namespace: row.get(2)?,
            name: row.get(3)?,
        },
        uid: row.get::<_, i64>(4)?.max(0) as u64,
        spec: row.get(5)?,
        labels: serde_json::from_str(&labels).unwrap_or_default(),
        generation: row.get::<_, i64>(7)?.max(0) as u64,
        spec_version: row.get::<_, i64>(8)?.max(0) as u64,
        authority: row.get(9)?,
        managed_by: row.get(10)?,
        deleting_since: row.get(11)?,
        cleanup_deadline: row.get(12)?,
        shard: row.get::<_, i64>(13)?.max(0) as u32,
        schema_digest: row.get(14)?,
        created_by: row.get(15)?,
        created_ms: row.get(16)?,
        updated_by: row.get(17)?,
        updated_ms: row.get(18)?,
    })
}

const STATUS_COLUMNS: &str =
    "uid, app, observed_generation, conditions, fields, updated_by, updated_ms";

fn status_from_row(row: &Row<'_>) -> Result<AppObjectStatusRow, rusqlite::Error> {
    Ok(AppObjectStatusRow {
        uid: row.get::<_, i64>(0)?.max(0) as u64,
        app: row.get(1)?,
        observed_generation: row.get::<_, i64>(2)?.max(0) as u64,
        conditions: row.get(3)?,
        fields: row.get(4)?,
        updated_by: row.get(5)?,
        updated_ms: row.get(6)?,
    })
}

impl Store {
    // ── kinds ─────────────────────────────────────────────────────────────

    /// Every kind, active and orphaned, by app and kind.
    pub fn object_kinds(&self) -> R<Vec<ObjectKindRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {KIND_COLUMNS} FROM object_kind ORDER BY app, kind;"
        ))?;
        let rows = stmt.query_map([], kind_from_row)?;
        Self::collect_rows(rows)
    }

    /// A cheap fingerprint of the kinds: their count and highest revision
    /// (revisions only grow).
    pub fn object_kind_fingerprint(&self) -> R<(i64, i64)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(revision), 0) FROM object_kind;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    /// Make `app`'s `object_kind` rows match the kinds its record declares
    /// (`kinds`; empty when the app is uninstalled): each declared kind
    /// `active` with its declaration; every other kind of the app
    /// `orphaned`, keeping its declaration while it has objects and dropped
    /// once it has none. Idempotent, and proposes nothing when the rows
    /// already match. Returns whether it wrote.
    #[instrument(err, skip(self, kinds))]
    pub fn sync_object_kinds(&self, app: &str, kinds: Vec<KindWrite>, now_ms: i64) -> R<bool> {
        if !self.object_kinds_out_of_sync(app, &kinds)? {
            return Ok(false);
        }
        let app = app.to_owned();
        self.replicated(Lane::Control, "sync_object_kinds", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::app_kinds_tx(&tx, &app)?;
            let mut wrote = false;
            for kind in &kinds {
                let same = current.iter().any(|row| {
                    row.kind == kind.kind
                        && row.decl == kind.decl
                        && row.digest == kind.digest
                        && row.state == "active"
                });
                if same {
                    continue;
                }
                let revision = Self::next_scaling_counter_tx(&tx, OBJECT_KIND_COUNTER)?;
                tx.execute(
                    "INSERT INTO object_kind (app, kind, decl, digest, state, revision, \
                     created_ms, updated_ms) VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?6, ?6) \
                     ON CONFLICT(app, kind) DO UPDATE SET decl = ?3, digest = ?4, \
                     state = 'active', revision = ?5, updated_ms = ?6;",
                    params![
                        app,
                        kind.kind,
                        kind.decl,
                        kind.digest,
                        revision as i64,
                        now_ms
                    ],
                )?;
                wrote = true;
            }
            for row in current
                .iter()
                .filter(|row| !kinds.iter().any(|kind| kind.kind == row.kind))
            {
                let objects: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM app_object WHERE app = ?1 AND kind = ?2;",
                    params![app, row.kind],
                    |r| r.get(0),
                )?;
                if objects == 0 {
                    tx.execute(
                        "DELETE FROM object_kind WHERE app = ?1 AND kind = ?2;",
                        params![app, row.kind],
                    )?;
                    wrote = true;
                } else if row.state != "orphaned" {
                    let revision = Self::next_scaling_counter_tx(&tx, OBJECT_KIND_COUNTER)?;
                    tx.execute(
                        "UPDATE object_kind SET state = 'orphaned', revision = ?3, \
                         updated_ms = ?4 WHERE app = ?1 AND kind = ?2;",
                        params![app, row.kind, revision as i64, now_ms],
                    )?;
                    wrote = true;
                }
            }
            tx.commit()?;
            Ok(wrote)
        })
    }

    fn app_kinds_tx(conn: &Connection, app: &str) -> R<Vec<ObjectKindRow>> {
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {KIND_COLUMNS} FROM object_kind WHERE app = ?1 ORDER BY kind;"
        ))?;
        let rows = stmt.query_map([app], kind_from_row)?;
        Self::collect_rows(rows)
    }

    /// Whether [`Self::sync_object_kinds`] has anything to write.
    fn object_kinds_out_of_sync(&self, app: &str, kinds: &[KindWrite]) -> R<bool> {
        let conn = self.read_connection()?;
        let current = Self::app_kinds_tx(&conn, app)?;
        let declared_match = kinds.iter().all(|kind| {
            current.iter().any(|row| {
                row.kind == kind.kind
                    && row.decl == kind.decl
                    && row.digest == kind.digest
                    && row.state == "active"
            })
        });
        if !declared_match {
            return Ok(true);
        }
        for row in current
            .iter()
            .filter(|row| !kinds.iter().any(|kind| kind.kind == row.kind))
        {
            if row.state != "orphaned" {
                return Ok(true);
            }
            let objects: i64 = conn.query_row(
                "SELECT COUNT(*) FROM app_object WHERE app = ?1 AND kind = ?2;",
                params![app, row.kind],
                |r| r.get(0),
            )?;
            if objects == 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The apps that have `object_kind` rows.
    pub fn object_kind_apps(&self) -> R<BTreeSet<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached("SELECT DISTINCT app FROM object_kind;")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        Ok(Self::collect_rows(rows)?.into_iter().collect())
    }

    // ── objects ───────────────────────────────────────────────────────────

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

    /// Objects, by app, kind, namespace, and name: every one, an app's
    /// (optionally in some stored shards), or one kind's.
    pub fn app_objects(&self, filter: &ObjectFilter) -> R<Vec<AppObjectRow>> {
        let conn = self.read_connection()?;
        let mut clauses = Vec::new();
        let mut args: Vec<String> = Vec::new();
        if let Some(app) = &filter.app {
            args.push(app.clone());
            clauses.push(format!("app = ?{}", args.len()));
            if let Some(kind) = &filter.kind {
                args.push(kind.clone());
                clauses.push(format!("kind = ?{}", args.len()));
            }
            if let Some(shards) = &filter.shards {
                let list: Vec<String> = shards.iter().map(u32::to_string).collect();
                clauses.push(format!("shard IN ({})", list.join(", ")));
            }
        }
        let filter_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let mut stmt = conn.prepare(&format!(
            "SELECT {COLUMNS} FROM app_object {filter_sql} ORDER BY app, kind, namespace, name;"
        ))?;
        let rows = stmt.query_map(rusqlite::params_from_iter(args.iter()), from_row)?;
        Self::collect_rows(rows)
    }

    /// The objects in `namespace` (deleting ones included).
    pub fn app_objects_in_namespace(&self, namespace: &str) -> R<Vec<AppObjectRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {COLUMNS} FROM app_object WHERE namespace = ?1 \
             ORDER BY app, kind, name;"
        ))?;
        let rows = stmt.query_map([namespace], from_row)?;
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

    /// A cheap fingerprint of one app's objects: their count and highest
    /// spec version. Status writes never move it.
    pub fn app_object_fingerprint(&self, app: &str) -> R<(i64, i64)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(spec_version), 0) FROM app_object WHERE app = ?1;",
            [app],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    /// Create or update an object, compare-and-set on `write.cas`. Writing
    /// what is stored writes nothing; a deleting object's spec is frozen;
    /// a create past the kind's `maxObjects` is refused, counted inside the
    /// write.
    #[instrument(err, skip_all, fields(app = %write.key.app, kind = %write.key.kind, namespace = %write.key.namespace, name = %write.key.name))]
    pub fn put_app_object(&self, write: ObjectWrite) -> R<PutObject> {
        self.replicated(Lane::Control, "put_app_object", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let current = Self::app_object_tx(&tx, &write.key)?;
            if let Some(row) = current.as_ref().filter(|row| row.is_deleting()) {
                return Ok(PutObject::Deleting { row: row.clone() });
            }
            if !write.cas.holds(current.as_ref()) {
                return Ok(PutObject::Conflict { current });
            }
            if let Some(row) = &current {
                if row.spec == write.spec
                    && row.labels == write.labels
                    && row.authority == write.authority
                    && row.managed_by == write.managed_by
                {
                    return Ok(PutObject::Unchanged { row: row.clone() });
                }
            }
            if current.is_none() {
                let count: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM app_object WHERE app = ?1 AND kind = ?2;",
                    params![write.key.app, write.key.kind],
                    |row| row.get(0),
                )?;
                if count.max(0) as u64 >= write.max_objects {
                    return Ok(PutObject::TooMany {
                        count: count.max(0) as u64,
                    });
                }
            }
            let spec_version = Self::next_scaling_counter_tx(&tx, APP_OBJECT_COUNTER)?;
            let labels = serde_json::to_string(&write.labels)?;
            match &current {
                None => {
                    let uid = Self::next_scaling_counter_tx(&tx, APP_OBJECT_COUNTER)?;
                    tx.execute(
                        "INSERT INTO app_object (app, kind, namespace, name, uid, spec, labels, \
                         generation, spec_version, authority, managed_by, shard, schema_digest, \
                         created_by, created_ms, updated_by, updated_ms) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                         ?13, ?14);",
                        params![
                            write.key.app,
                            write.key.kind,
                            write.key.namespace,
                            write.key.name,
                            uid as i64,
                            write.spec,
                            labels,
                            spec_version as i64,
                            write.authority,
                            write.managed_by,
                            write.shard as i64,
                            write.schema_digest,
                            write.principal,
                            write.now_ms,
                        ],
                    )?;
                }
                Some(row) => {
                    let generation = if row.spec != write.spec {
                        row.generation + 1
                    } else {
                        row.generation
                    };
                    tx.execute(
                        "UPDATE app_object SET spec = ?5, labels = ?6, generation = ?7, \
                         spec_version = ?8, authority = ?9, managed_by = ?10, \
                         schema_digest = ?11, updated_by = ?12, updated_ms = ?13 \
                         WHERE app = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4;",
                        params![
                            write.key.app,
                            write.key.kind,
                            write.key.namespace,
                            write.key.name,
                            write.spec,
                            labels,
                            generation as i64,
                            spec_version as i64,
                            write.authority,
                            write.managed_by,
                            write.schema_digest,
                            write.principal,
                            write.now_ms,
                        ],
                    )?;
                }
            }
            let row = Self::app_object_tx(&tx, &write.key)?
                .ok_or_else(|| anyhow!("the object vanished inside its own write"))?;
            tx.commit()?;
            Ok(PutObject::Written {
                row,
                created: current.is_none(),
            })
        })
    }

    /// Delete an object, compare-and-set on `expected` (a spec version).
    /// With `cleanup_deadline` (Unix ms), start a bounded cleanup instead:
    /// mark it deleting (its spec freezes, its spec version moves) and keep
    /// the row for the app to release. Without one, a deleting object is
    /// removed at once (`--now`).
    #[instrument(err, skip(self))]
    pub fn delete_app_object(
        &self,
        key: &ObjectKey,
        expected: Option<u64>,
        cleanup_deadline: Option<i64>,
        now_ms: i64,
    ) -> R<DeleteObject> {
        let key = key.clone();
        self.replicated(Lane::Control, "delete_app_object", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let Some(row) = Self::app_object_tx(&tx, &key)? else {
                return Ok(DeleteObject::Missing);
            };
            if expected.is_some_and(|version| version != row.spec_version) {
                return Ok(DeleteObject::Conflict(row));
            }
            if let Some(deadline) = cleanup_deadline {
                if row.is_deleting() {
                    return Ok(DeleteObject::AlreadyDeleting(row));
                }
                let spec_version = Self::next_scaling_counter_tx(&tx, APP_OBJECT_COUNTER)?;
                tx.execute(
                    "UPDATE app_object SET deleting_since = ?5, cleanup_deadline = ?6, \
                     spec_version = ?7 \
                     WHERE app = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4;",
                    params![
                        key.app,
                        key.kind,
                        key.namespace,
                        key.name,
                        now_ms.max(1),
                        deadline,
                        spec_version as i64
                    ],
                )?;
                let marked = Self::app_object_tx(&tx, &key)?
                    .ok_or_else(|| anyhow!("the object vanished inside its own write"))?;
                tx.commit()?;
                return Ok(DeleteObject::Deleting(marked));
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

    /// End a deleting object's cleanup (the owning app's `ReleaseObject`):
    /// remove the row when it is deleting and still has `uid`.
    #[instrument(err, skip(self))]
    pub fn release_app_object(&self, key: &ObjectKey, uid: u64) -> R<ReleaseObject> {
        let key = key.clone();
        self.replicated(Lane::Control, "release_app_object", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let Some(row) = Self::app_object_tx(&tx, &key)?.filter(|row| row.uid == uid) else {
                return Ok(ReleaseObject::Missing);
            };
            if !row.is_deleting() {
                return Ok(ReleaseObject::NotDeleting(row));
            }
            tx.execute(
                "DELETE FROM app_object WHERE app = ?1 AND kind = ?2 AND namespace = ?3 \
                 AND name = ?4 AND uid = ?5;",
                params![key.app, key.kind, key.namespace, key.name, uid as i64],
            )?;
            tx.commit()?;
            Ok(ReleaseObject::Released(row))
        })
    }

    /// Remove deleting objects whose cleanup deadline passed, and deleting
    /// objects of `orphaned` kinds (no app is left to release them).
    /// Returns what it removed; proposes nothing when nothing is due.
    #[instrument(err, skip(self, orphaned))]
    pub fn expire_app_object_cleanups(
        &self,
        now_ms: i64,
        orphaned: BTreeSet<(String, String)>,
    ) -> R<Vec<AppObjectRow>> {
        {
            let conn = self.read_connection()?;
            if Self::due_cleanups(&conn, now_ms, &orphaned)?.is_empty() {
                return Ok(Vec::new());
            }
        }
        self.replicated(Lane::Control, "expire_app_object_cleanups", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let rows = Self::due_cleanups(&tx, now_ms, &orphaned)?;
            for row in &rows {
                tx.execute(
                    "DELETE FROM app_object WHERE uid = ?1 AND deleting_since > 0;",
                    [row.uid as i64],
                )?;
            }
            tx.commit()?;
            Ok(rows)
        })
    }

    /// Deleting objects past their deadline, or of an orphaned kind.
    fn due_cleanups(
        conn: &Connection,
        now_ms: i64,
        orphaned: &BTreeSet<(String, String)>,
    ) -> R<Vec<AppObjectRow>> {
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {COLUMNS} FROM app_object WHERE deleting_since > 0;"
        ))?;
        let rows = Self::collect_rows(stmt.query_map([], from_row)?)?;
        Ok(rows
            .into_iter()
            .filter(|row| {
                row.cleanup_deadline <= now_ms
                    || orphaned.contains(&(row.key.app.clone(), row.key.kind.clone()))
            })
            .collect())
    }

    // ── status (Status lane) ──────────────────────────────────────────────

    fn app_object_status_tx(conn: &Connection, uid: u64) -> R<Option<AppObjectStatusRow>> {
        Ok(conn
            .prepare_cached(&format!(
                "SELECT {STATUS_COLUMNS} FROM app_object_status WHERE uid = ?1;"
            ))?
            .query_row([uid as i64], status_from_row)
            .optional()?)
    }

    /// One object's status.
    pub fn app_object_status(&self, uid: u64) -> R<Option<AppObjectStatusRow>> {
        let conn = self.read_connection()?;
        Self::app_object_status_tx(&conn, uid)
    }

    /// The statuses of `app`'s objects (every app's when `None`), by uid.
    pub fn app_object_statuses(&self, app: Option<&str>) -> R<BTreeMap<u64, AppObjectStatusRow>> {
        let conn = self.read_connection()?;
        let rows = match app {
            Some(app) => {
                let mut stmt = conn.prepare_cached(&format!(
                    "SELECT {STATUS_COLUMNS} FROM app_object_status WHERE app = ?1;"
                ))?;
                let rows = Self::collect_rows(stmt.query_map([app], status_from_row)?)?;
                rows
            }
            None => {
                let mut stmt = conn
                    .prepare_cached(&format!("SELECT {STATUS_COLUMNS} FROM app_object_status;"))?;
                let rows = Self::collect_rows(stmt.query_map([], status_from_row)?)?;
                rows
            }
        };
        Ok(rows.into_iter().map(|row| (row.uid, row)).collect())
    }

    /// The checks a status report must pass on this replica, before and
    /// inside its write: the object exists with `uid`, the report's
    /// generation is not ahead of it, and not behind what is stored.
    fn status_precheck(
        conn: &Connection,
        write: &StatusWrite,
    ) -> R<Result<Option<AppObjectStatusRow>, PutStatus>> {
        let Some(object) =
            Self::app_object_tx(conn, &write.key)?.filter(|row| row.uid == write.uid)
        else {
            return Ok(Err(PutStatus::Missing));
        };
        if write.observed_generation > object.generation {
            return Ok(Err(PutStatus::Ahead {
                generation: object.generation,
            }));
        }
        let stored = Self::app_object_status_tx(conn, write.uid)?;
        if let Some(stored) = &stored {
            if write.observed_generation < stored.observed_generation {
                return Ok(Err(PutStatus::Behind {
                    stored: stored.observed_generation,
                }));
            }
        }
        Ok(Ok(stored))
    }

    /// Write an object's status (the owning app's report), on the Status
    /// lane. An identical report is dropped without a raft proposal.
    #[instrument(err, skip_all, fields(app = %write.key.app, kind = %write.key.kind, uid = write.uid))]
    pub fn update_app_object_status(&self, write: StatusWrite) -> R<PutStatus> {
        {
            let conn = self.read_connection()?;
            match Self::status_precheck(&conn, &write)? {
                Err(refusal) => return Ok(refusal),
                Ok(Some(stored))
                    if stored.observed_generation == write.observed_generation
                        && stored.conditions == write.conditions
                        && stored.fields == write.fields =>
                {
                    return Ok(PutStatus::Unchanged(stored));
                }
                Ok(_) => {}
            }
        }
        self.replicated(Lane::Status, "update_app_object_status", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let previous = match Self::status_precheck(&tx, &write)? {
                Err(refusal) => return Ok(refusal),
                Ok(previous) => previous,
            };
            tx.execute(
                "INSERT INTO app_object_status (uid, app, observed_generation, conditions, \
                 fields, updated_by, updated_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(uid) DO UPDATE SET observed_generation = ?3, conditions = ?4, \
                 fields = ?5, updated_by = ?6, updated_ms = ?7;",
                params![
                    write.uid as i64,
                    write.key.app,
                    write.observed_generation as i64,
                    write.conditions,
                    write.fields,
                    write.principal,
                    write.now_ms,
                ],
            )?;
            let row = Self::app_object_status_tx(&tx, write.uid)?
                .ok_or_else(|| anyhow!("the status vanished inside its own write"))?;
            tx.commit()?;
            Ok(PutStatus::Written { row, previous })
        })
    }

    /// Sweep status rows whose object is gone (deleted, released, or
    /// expired), once they are older than a grace period: the object's
    /// delete is a Control-lane write and cannot remove a Status-lane row
    /// itself. Runs on the status-lane leader.
    pub fn prune_app_object_status_orphans(&self, now_ms: i64) -> R<usize> {
        let cutoff = now_ms - STATUS_ORPHAN_GRACE_MS;
        let orphans: Vec<i64> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT s.uid FROM app_object_status s \
                 LEFT JOIN app_object o ON o.uid = s.uid \
                 WHERE o.uid IS NULL AND s.updated_ms < ?1;",
            )?;
            let rows = Self::collect_rows(stmt.query_map([cutoff], |row| row.get(0))?)?;
            rows
        };
        if orphans.is_empty() {
            return Ok(0);
        }
        self.replicated(Lane::Status, "prune_app_object_status_orphans", move |s| {
            let conn = s.write_connection()?;
            let mut removed = 0;
            for uid in &orphans {
                removed += conn.execute(
                    "DELETE FROM app_object_status WHERE uid = ?1 AND updated_ms < ?2 AND \
                     NOT EXISTS (SELECT 1 FROM app_object WHERE uid = ?1);",
                    params![uid, cutoff],
                )?;
            }
            Ok(removed)
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
            authority: "{}".into(),
            managed_by: String::new(),
            schema_digest: "d1".into(),
            shard: 3,
            principal: "alice".into(),
            now_ms: 1,
            cas,
            max_objects: 3,
        }
    }

    fn status(name: &str, uid: u64, observed: u64, fields: &str) -> StatusWrite {
        StatusWrite {
            key: key(name),
            uid,
            observed_generation: observed,
            conditions: "[]".into(),
            fields: fields.into(),
            principal: "gitops".into(),
            now_ms: 2,
        }
    }

    #[test]
    fn uids_generations_spec_versions_and_cas_test() -> R<()> {
        let (_dir, store) = store()?;
        let PutObject::Written { row: first, .. } =
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Missing))?
        else {
            panic!("created")
        };
        assert_eq!(first.generation, 1);
        assert!(first.uid > 0);
        assert!(matches!(
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Missing))?,
            PutObject::Conflict { current: Some(_) }
        ));
        assert!(matches!(
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Any))?,
            PutObject::Unchanged { .. }
        ));
        assert!(matches!(
            store.put_app_object(write(
                "web",
                "{\"a\":2}",
                VersionCas::Version(first.spec_version + 99)
            ))?,
            PutObject::Conflict { .. }
        ));
        let PutObject::Written { row: second, .. } = store.put_app_object(write(
            "web",
            "{\"a\":2}",
            VersionCas::Version(first.spec_version),
        ))?
        else {
            panic!("updated")
        };
        assert_eq!((second.generation, second.uid), (2, first.uid));
        assert!(second.spec_version > first.spec_version);

        // Labels move the spec version, not the generation.
        let mut labels = write("web", "{\"a\":2}", VersionCas::Any);
        labels.labels.insert("team".into(), "a".into());
        let PutObject::Written { row: third, .. } = store.put_app_object(labels)? else {
            panic!("labels")
        };
        assert_eq!(third.generation, 2);
        assert!(third.spec_version > second.spec_version);

        // A status write never moves the spec version.
        let PutStatus::Written { row, previous } =
            store.update_app_object_status(status("web", third.uid, 2, "{\"ok\":true}"))?
        else {
            panic!("status")
        };
        assert!(previous.is_none());
        assert_eq!(row.observed_generation, 2);
        assert_eq!(
            store
                .app_object(&key("web"))?
                .context("stored")?
                .spec_version,
            third.spec_version
        );
        // Identical: dropped. Ahead, behind, another uid: refused.
        assert!(matches!(
            store.update_app_object_status(status("web", third.uid, 2, "{\"ok\":true}"))?,
            PutStatus::Unchanged(_)
        ));
        assert_eq!(
            store.update_app_object_status(status("web", third.uid, 3, "{}"))?,
            PutStatus::Ahead { generation: 2 }
        );
        assert_eq!(
            store.update_app_object_status(status("web", third.uid, 1, "{}"))?,
            PutStatus::Behind { stored: 2 }
        );
        assert_eq!(
            store.update_app_object_status(status("web", third.uid + 1000, 1, "{}"))?,
            PutStatus::Missing
        );

        // maxObjects counts inside the write.
        store.put_app_object(write("b", "{}", VersionCas::Any))?;
        store.put_app_object(write("c", "{}", VersionCas::Any))?;
        assert_eq!(
            store.put_app_object(write("d", "{}", VersionCas::Any))?,
            PutObject::TooMany { count: 3 }
        );

        // Delete with CAS; a recreate gets a new uid, and the old status is
        // swept once it is past the grace period.
        let latest = store.app_object(&key("web"))?.context("stored")?;
        assert!(matches!(
            store.delete_app_object(&key("web"), Some(latest.spec_version + 1), None, 3)?,
            DeleteObject::Conflict(_)
        ));
        assert!(matches!(
            store.delete_app_object(&key("web"), Some(latest.spec_version), None, 3)?,
            DeleteObject::Deleted(_)
        ));
        let PutObject::Written { row: again, .. } =
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Any))?
        else {
            panic!("recreated")
        };
        assert!(again.uid > latest.uid);
        assert!(store.app_object_status(latest.uid)?.is_some());
        assert_eq!(store.prune_app_object_status_orphans(3)?, 0);
        assert_eq!(
            store.prune_app_object_status_orphans(3 + STATUS_ORPHAN_GRACE_MS)?,
            1
        );
        assert!(store.app_object_status(latest.uid)?.is_none());
        Ok(())
    }

    #[test]
    fn bounded_cleanup_and_release_test() -> R<()> {
        let (_dir, store) = store()?;
        let PutObject::Written { row, .. } =
            store.put_app_object(write("web", "{}", VersionCas::Any))?
        else {
            panic!("created")
        };
        let DeleteObject::Deleting(marked) =
            store.delete_app_object(&key("web"), None, Some(100), 10)?
        else {
            panic!("deleting")
        };
        assert!(marked.is_deleting() && marked.spec_version > row.spec_version);
        assert!(matches!(
            store.put_app_object(write("web", "{\"a\":1}", VersionCas::Any))?,
            PutObject::Deleting { .. }
        ));
        assert!(matches!(
            store.delete_app_object(&key("web"), None, Some(100), 11)?,
            DeleteObject::AlreadyDeleting(_)
        ));
        assert_eq!(
            store.release_app_object(&key("web"), row.uid + 1)?,
            ReleaseObject::Missing
        );
        assert!(store
            .expire_app_object_cleanups(50, BTreeSet::new())?
            .is_empty());
        assert!(matches!(
            store.release_app_object(&key("web"), row.uid)?,
            ReleaseObject::Released(_)
        ));
        assert!(store.app_object(&key("web"))?.is_none());

        // A cleanup past its deadline, or of an orphaned kind, expires.
        store.put_app_object(write("a", "{}", VersionCas::Any))?;
        store.put_app_object(write("b", "{}", VersionCas::Any))?;
        store.delete_app_object(&key("a"), None, Some(100), 10)?;
        store.delete_app_object(&key("b"), None, Some(1_000), 10)?;
        let expired = store.expire_app_object_cleanups(200, BTreeSet::new())?;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key.name, "a");
        let orphaned: BTreeSet<(String, String)> =
            [("gitops".to_string(), "Repo".to_string())].into();
        assert_eq!(store.expire_app_object_cleanups(200, orphaned)?.len(), 1);
        Ok(())
    }

    #[test]
    fn kinds_sync_from_records_and_orphan_test() -> R<()> {
        let (_dir, store) = store()?;
        let kind = |name: &str, decl: &str| KindWrite {
            kind: name.into(),
            decl: decl.into(),
            digest: "d1".into(),
        };
        assert!(store.sync_object_kinds(
            "gitops",
            vec![kind("Repo", "{}"), kind("Hook", "{}")],
            1
        )?);
        assert!(!store.sync_object_kinds(
            "gitops",
            vec![kind("Repo", "{}"), kind("Hook", "{}")],
            2
        )?);
        let (_, revision) = store.object_kind_fingerprint()?;
        store.put_app_object(write("web", "{}", VersionCas::Any))?;
        // Dropping Repo (which has an object) orphans it; Hook (none) goes.
        assert!(store.sync_object_kinds("gitops", Vec::new(), 3)?);
        let kinds = store.object_kinds()?;
        assert_eq!(kinds.len(), 1);
        assert_eq!(
            (kinds[0].kind.as_str(), kinds[0].state.as_str()),
            ("Repo", "orphaned")
        );
        assert!(store.object_kind_fingerprint()?.1 > revision);
        assert!(!store.sync_object_kinds("gitops", Vec::new(), 4)?);
        // Reinstalling adopts it.
        assert!(store.sync_object_kinds("gitops", vec![kind("Repo", "{\"v\":2}")], 5)?);
        assert_eq!(store.object_kinds()?[0].state, "active");
        // Once its last object is gone, an orphaned kind is dropped.
        store.sync_object_kinds("gitops", Vec::new(), 6)?;
        store.delete_app_object(&key("web"), None, None, 7)?;
        assert!(store.sync_object_kinds("gitops", Vec::new(), 8)?);
        assert!(store.object_kinds()?.is_empty());
        Ok(())
    }

    #[test]
    fn listings_counts_and_shards_test() -> R<()> {
        let (_dir, store) = store()?;
        store.put_app_object(write("a", "{}", VersionCas::Any))?;
        let mut other_shard = write("b", "{}", VersionCas::Any);
        other_shard.shard = 9;
        store.put_app_object(other_shard)?;
        let mut cluster = write("c", "{}", VersionCas::Any);
        cluster.key.app = "certs".into();
        cluster.key.kind = "Issuer".into();
        cluster.key.namespace = String::new();
        store.put_app_object(cluster)?;
        assert_eq!(store.app_objects(&ObjectFilter::default())?.len(), 3);
        let shard3 = store.app_objects(&ObjectFilter {
            app: Some("gitops".into()),
            kind: None,
            shards: Some([3].into()),
        })?;
        assert_eq!(shard3.len(), 1);
        assert_eq!(shard3[0].key.name, "a");
        assert_eq!(store.app_objects_in_namespace("team-a")?.len(), 2);
        let counts = store.app_object_counts()?;
        assert_eq!(counts.get(&("gitops".into(), "Repo".into())), Some(&2));
        let (count, _) = store.app_object_fingerprint("gitops")?;
        assert_eq!(count, 2);
        Ok(())
    }
}
