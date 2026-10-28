//! Storage for app records (`legion app`, docs/apps.md; apps plan §6.3,
//! "Records").
//!
//! One row per installed app on the Control lane, next to the jobs the app
//! reconciler submits. The manifest, settings, config and grant are JSON
//! columns; the reconciler's bookkeeping (last digest, last action and
//! error, the build marker it holds and why) are their own columns, written
//! by [`Store::set_app_state`] without touching `updated_at`, so the
//! reconciler never races an operator's compare-and-set write.
//!
//! Also here: the reads the reconciler and `legion app status` need about an
//! app's job (its latest version's `managedBy` stamp and submitter, and its
//! allocations), which live in `job_config.meta` and `allocation`.

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::app::{
    AppGrant, AppManifest, AppReconcileState, AppRecord, AppSettings, AppSource, BuildMarker,
    ManagedBy,
};
use rusqlite::{params, OptionalExtension, Row};

const APP_COLUMNS: &str = "name, source, source_ref, manifest, manifest_digest, enabled, \
     settings, config, grant_rules, granted_by, granted_at, installed_by, installed_at, \
     updated_at, last_digest, last_action, last_error, last_at, build_marker, marker_reason, \
     pending_change";

/// An app job's latest version, as the reconciler compares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppJobVersion {
    /// The job's status (`active`, `stopped`, `failed`, ...).
    pub status: String,
    pub version: i64,
    pub job_config_id: String,
    /// The stamp the reconciler put on it, if it still carries one.
    pub managed_by: Option<ManagedBy>,
    /// Who submitted it (absent for versions Legion stored itself).
    pub submitted_by: Option<String>,
}

impl AppJobVersion {
    pub fn is_stopped(&self) -> bool {
        self.status == "stopped"
    }
}

/// An app job's allocations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppJobAllocations {
    /// Allocations reported running.
    pub running: u32,
    /// Allocations not yet complete or failed (pending and running).
    pub live: u32,
    /// The nodes running them, sorted.
    pub nodes: Vec<String>,
}

fn invalid(message: String) -> anyhow::Error {
    anyhow::Error::new(StorageError::InvalidInput { message })
}

fn json_column<T: serde::de::DeserializeOwned>(raw: String, column: &str) -> rusqlite::Result<T> {
    serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("app.{column}: {err}").into(),
        )
    })
}

impl Store {
    fn app_from_row(row: &Row<'_>) -> rusqlite::Result<AppRecord> {
        let source: String = row.get(1)?;
        let marker: String = row.get(18)?;
        Ok(AppRecord {
            name: row.get(0)?,
            source: AppSource::parse(&source).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    err.into(),
                )
            })?,
            source_ref: row.get(2)?,
            manifest: json_column::<AppManifest>(row.get(3)?, "manifest")?,
            manifest_digest: row.get(4)?,
            enabled: row.get::<_, i64>(5)? != 0,
            settings: json_column::<AppSettings>(row.get(6)?, "settings")?,
            config: json_column::<Value>(row.get(7)?, "config")?,
            grant: AppGrant {
                rules: json_column(row.get(8)?, "grant_rules")?,
                granted_by: row.get(9)?,
                granted_at: row.get(10)?,
            },
            installed_by: row.get(11)?,
            installed_at: row.get(12)?,
            updated_at: row.get(13)?,
            state: AppReconcileState {
                last_digest: row.get(14)?,
                last_action: row.get(15)?,
                last_error: row.get(16)?,
                last_at: row.get(17)?,
                marker: if marker.is_empty() {
                    None
                } else {
                    Some(json_column::<BuildMarker>(marker, "build_marker")?)
                },
                marker_reason: row.get(19)?,
                pending_change: row.get(20)?,
            },
        })
    }

    /// Every installed app, by name.
    #[instrument(err, skip_all)]
    pub fn list_apps(&self) -> R<Vec<AppRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!("SELECT {APP_COLUMNS} FROM app ORDER BY name ASC;"))?;
        let rows = stmt.query_map([], Self::app_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(app = name))]
    pub fn get_app(&self, name: &str) -> R<Option<AppRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!("SELECT {APP_COLUMNS} FROM app WHERE name = ?;"),
            [name],
            Self::app_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Install an app: refused when one of that name exists.
    #[instrument(err, skip_all, fields(app = %record.name))]
    pub fn insert_app(&self, record: AppRecord) -> R<()> {
        self.replicated(Lane::Control, "insert_app", move |s| {
            s.insert_app_apply(&record, false).map(|_| ())
        })
    }

    /// Seed a bundled app: inserted only when no app of that name exists, so
    /// every later leader's seed pass changes nothing. Returns whether it
    /// inserted.
    #[instrument(err, skip_all, fields(app = %record.name))]
    pub fn seed_app(&self, record: AppRecord) -> R<bool> {
        if self.get_app(&record.name)?.is_some() {
            return Ok(false);
        }
        self.replicated(Lane::Control, "seed_app", move |s| {
            s.insert_app_apply(&record, true)
        })
    }

    fn insert_app_apply(&self, record: &AppRecord, if_missing: bool) -> R<bool> {
        let conn = self.write_connection()?;
        let exists = conn
            .query_row("SELECT 1 FROM app WHERE name = ?;", [&record.name], |_| {
                Ok(())
            })
            .optional()?
            .is_some();
        if exists {
            if if_missing {
                return Ok(false);
            }
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: format!(
                    "app {} is already installed; use `legion app upgrade` to change its manifest",
                    record.name
                ),
            }));
        }
        let marker = match &record.state.marker {
            Some(marker) => serde_json::to_string(marker)?,
            None => String::new(),
        };
        conn.execute(
            &format!(
                "INSERT INTO app ({APP_COLUMNS}) VALUES \
                 (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
                  ?18, ?19, ?20, ?21);"
            ),
            params![
                record.name,
                record.source.as_str(),
                record.source_ref,
                serde_json::to_string(&record.manifest)?,
                record.manifest_digest,
                record.enabled as i64,
                serde_json::to_string(&record.settings)?,
                serde_json::to_string(&record.config)?,
                serde_json::to_string(&record.grant.rules)?,
                record.grant.granted_by,
                record.grant.granted_at,
                record.installed_by,
                record.installed_at,
                record.updated_at,
                record.state.last_digest,
                record.state.last_action,
                record.state.last_error,
                record.state.last_at,
                marker,
                record.state.marker_reason,
                record.state.pending_change,
            ],
        )?;
        Ok(true)
    }

    /// Replace an app's manifest, settings, config, grant and enabled flag,
    /// if it is still at `expected_updated_at` (compare-and-set: two writers
    /// that read the same record cannot both win). `record.updated_at` is
    /// the new value; `record.state.pending_change` says what changed, for
    /// the reconciler's next event.
    #[instrument(err, skip_all, fields(app = %record.name))]
    pub fn update_app(&self, record: AppRecord, expected_updated_at: &str) -> R<()> {
        let expected = expected_updated_at.to_owned();
        self.replicated(Lane::Control, "update_app", move |s| {
            s.update_app_apply(&record, &expected)
        })
    }

    fn update_app_apply(&self, record: &AppRecord, expected_updated_at: &str) -> R<()> {
        let conn = self.write_connection()?;
        let changed = conn.execute(
            "UPDATE app SET source = ?2, source_ref = ?3, manifest = ?4, manifest_digest = ?5,
                 enabled = ?6, settings = ?7, config = ?8, grant_rules = ?9, granted_by = ?10,
                 granted_at = ?11, updated_at = ?12, pending_change = ?13
             WHERE name = ?1 AND updated_at = ?14;",
            params![
                record.name,
                record.source.as_str(),
                record.source_ref,
                serde_json::to_string(&record.manifest)?,
                record.manifest_digest,
                record.enabled as i64,
                serde_json::to_string(&record.settings)?,
                serde_json::to_string(&record.config)?,
                serde_json::to_string(&record.grant.rules)?,
                record.grant.granted_by,
                record.grant.granted_at,
                record.updated_at,
                record.state.pending_change,
                expected_updated_at,
            ],
        )?;
        if changed == 1 {
            return Ok(());
        }
        let exists = conn
            .query_row("SELECT 1 FROM app WHERE name = ?;", [&record.name], |_| {
                Ok(())
            })
            .optional()?
            .is_some();
        if !exists {
            return Err(StorageError::not_found("app", record.name.clone()).into());
        }
        Err(anyhow::Error::new(StorageError::failed_precondition(format!(
            "app {} changed while this request ran; read it again and retry",
            record.name
        ))))
    }

    /// Record the reconciler's bookkeeping for an app. Leaves `updated_at`
    /// alone. A missing app (uninstalled mid-pass) is not an error.
    #[instrument(err, skip_all, fields(app = name))]
    pub fn set_app_state(&self, name: &str, state: AppReconcileState) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "set_app_state", move |s| {
            s.set_app_state_apply(&name, &state)
        })
    }

    fn set_app_state_apply(&self, name: &str, state: &AppReconcileState) -> R<()> {
        let conn = self.write_connection()?;
        let marker = match &state.marker {
            Some(marker) => serde_json::to_string(marker)?,
            None => String::new(),
        };
        conn.execute(
            "UPDATE app SET last_digest = ?2, last_action = ?3, last_error = ?4, last_at = ?5,
                 build_marker = ?6, marker_reason = ?7, pending_change = ?8
             WHERE name = ?1;",
            params![
                name,
                state.last_digest,
                state.last_action,
                state.last_error,
                state.last_at,
                marker,
                state.marker_reason,
                state.pending_change,
            ],
        )?;
        Ok(())
    }

    /// Delete an app record. Returns whether one was deleted.
    #[instrument(err, skip_all, fields(app = name))]
    pub fn delete_app(&self, name: &str) -> R<bool> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_app", move |s| {
            let conn = s.write_connection()?;
            Ok(conn.execute("DELETE FROM app WHERE name = ?;", [&name])? > 0)
        })
    }

    /// The latest version of job `job_id`, with the stamp and submitter its
    /// meta carries; `None` when the job does not exist.
    #[instrument(err, skip_all, fields(job_id))]
    pub fn app_job_version(&self, job_id: &str) -> R<Option<AppJobVersion>> {
        let conn = self.read_connection()?;
        let row: Option<(String, Option<String>, Option<i64>, Option<Value>)> = conn
            .query_row(
                "SELECT j.status, jc.id, jc.version, jc.meta
                 FROM job j
                 LEFT JOIN job_config jc ON jc.id = (
                     SELECT latest.id FROM job_config latest
                     WHERE latest.job_id = j.id
                     ORDER BY latest.version DESC LIMIT 1
                 )
                 WHERE j.name = ?1;",
                [job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((status, Some(job_config_id), Some(version), meta)) = row else {
            return Ok(None);
        };
        let meta = meta.unwrap_or(Value::Null);
        Ok(Some(AppJobVersion {
            status,
            version,
            job_config_id,
            managed_by: meta
                .get("managedBy")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            submitted_by: meta
                .get("submittedBy")
                .and_then(Value::as_str)
                .map(str::to_string),
        }))
    }

    /// The `managedBy` stamp on job `job_id`'s latest version.
    pub fn job_managed_by(&self, job_id: &str) -> R<Option<ManagedBy>> {
        Ok(self
            .app_job_version(job_id)?
            .and_then(|version| version.managed_by))
    }

    /// Job `job_id`'s allocations across its versions: how many run, how
    /// many are live, and where they run.
    #[instrument(err, skip_all, fields(job_id))]
    pub fn app_job_allocations(&self, job_id: &str) -> R<AppJobAllocations> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT a.status, a.node_id FROM allocation a
             INNER JOIN job_config jc ON jc.id = a.job_config_id
             INNER JOIN job j ON j.id = jc.job_id
             WHERE j.name = ?1 AND a.status NOT IN ('complete', 'failed');",
        )?;
        let rows = stmt.query_map([job_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut counts = AppJobAllocations::default();
        for row in rows {
            let (status, node) = row?;
            counts.live += 1;
            if status == "running" {
                counts.running += 1;
                if !counts.nodes.contains(&node) {
                    counts.nodes.push(node);
                }
            }
        }
        counts.nodes.sort();
        Ok(counts)
    }
}

/// Reject an app record that could not have come from a valid manifest
/// (defense in depth for the write paths; the RPC layer validates first).
pub fn check_record(record: &AppRecord) -> R<()> {
    record
        .manifest
        .validate(record.is_bundled())
        .map_err(invalid)?;
    record.settings.validate().map_err(invalid)?;
    if record.name != record.manifest.name {
        return Err(invalid(format!(
            "app record {} holds the manifest of {}",
            record.name, record.manifest.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod app_store_tests {
    use super::*;
    use crate::structs::app::{AppReconcileState, Placement};

    fn store() -> R<Store> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?.keep();
        Store::start_storage(&crate::structs::config::ManagerConfigFull {
            store_path: dir.to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })
    }

    fn record(name: &str) -> AppRecord {
        let manifest: AppManifest = serde_json::from_value(json!({
            "name": name,
            "version": "1.0.0",
            "job": { "task": { "name": "main", "runtime": "noop", "config": {} } },
            "permissions": [{ "resources": ["node"], "actions": ["read"] }],
        }))
        .expect("manifest");
        AppRecord {
            name: name.into(),
            source: AppSource::File,
            source_ref: "./app.ncl".into(),
            manifest_digest: manifest.digest(),
            enabled: true,
            settings: AppSettings::from_manifest(&manifest),
            config: json!({}),
            grant: AppGrant {
                rules: manifest.permissions.clone(),
                granted_by: "userpass:alice".into(),
                granted_at: "t0".into(),
            },
            installed_by: "userpass:alice".into(),
            installed_at: "t0".into(),
            updated_at: "t0".into(),
            state: AppReconcileState::default(),
            manifest,
        }
    }

    #[test]
    fn records_round_trip_and_writes_compare_and_set() -> R<()> {
        let store = store()?;
        let app = record("demo");
        check_record(&app)?;
        store.insert_app(app.clone())?;
        assert_eq!(store.get_app("demo")?, Some(app.clone()));
        assert!(store.insert_app(app.clone()).is_err(), "names are unique");
        assert!(!store.seed_app(app.clone())?, "a seed never overwrites");

        let mut changed = app.clone();
        changed.settings.placement = Placement::Any;
        changed.updated_at = "t1".into();
        changed.state.pending_change = "settings changed".into();
        store.update_app(changed.clone(), "t0")?;
        let stored = store.get_app("demo")?.context("stored")?;
        assert_eq!(stored.settings.placement, Placement::Any);
        assert_eq!(stored.state.pending_change, "settings changed");

        // A writer that read the old record loses.
        let err = store
            .update_app(app.clone(), "t0")
            .expect_err("stale write");
        assert!(format!("{err:#}").contains("changed while"), "{err:#}");

        // The reconciler's bookkeeping leaves updated_at alone.
        store.set_app_state(
            "demo",
            AppReconcileState {
                last_digest: "abc".into(),
                marker: Some(BuildMarker {
                    version: "0.1.0".into(),
                    build_id: "x".into(),
                }),
                ..Default::default()
            },
        )?;
        let stored = store.get_app("demo")?.context("stored")?;
        assert_eq!(stored.updated_at, "t1");
        assert_eq!(stored.state.last_digest, "abc");
        assert_eq!(stored.state.marker.as_ref().map(|m| m.version.as_str()), Some("0.1.0"));

        assert!(store.delete_app("demo")?);
        assert!(store.get_app("demo")?.is_none());
        assert!(store.list_apps()?.is_empty());
        Ok(())
    }
}
