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
    ManagedBy, APP_UID_BASE, APP_UID_COUNT,
};
use rusqlite::{params, OptionalExtension, Row};

const APP_COLUMNS: &str = "name, source, source_ref, manifest, manifest_digest, enabled, \
     settings, config, grant_rules, granted_by, granted_at, installed_by, installed_at, \
     updated_at, last_digest, last_action, last_error, last_at, build_marker, marker_reason, \
     pending_change, uid";

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

    /// The job gave up: a reschedule policy's `moveCount`, a KV
    /// `onDelete = "fail"`, a gang that lost its quorum. The reconciler
    /// restarts it with backoff.
    pub fn is_failed(&self) -> bool {
        self.status == "failed"
    }
}

/// An app job's allocations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppJobAllocations {
    /// Allocations of the job's latest version reported running and
    /// healthy: what the app runs as its record says.
    pub running: u32,
    /// Healthy allocations of older versions: what still serves while an
    /// upgrade rolls, or after a rollout rolled back.
    pub previous: u32,
    /// Allocations not yet complete or failed (pending and running).
    pub live: u32,
    /// The nodes running them, sorted.
    pub nodes: Vec<String>,
}

/// One live allocation of an app's job: where a view or action request can
/// be delivered (docs/apps.md, "Views and actions").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppUiAllocation {
    pub id: i64,
    pub node_id: String,
    /// Its latest health report says healthy.
    pub healthy: bool,
}

fn invalid(message: String) -> anyhow::Error {
    anyhow::Error::new(StorageError::InvalidInput { message })
}

fn json_column<T: serde::de::DeserializeOwned>(raw: &str, column: &str) -> Result<T, String> {
    serde_json::from_str(raw).map_err(|err| format!("app.{column}: {err}"))
}

/// An app row that does not decode: its name, the columns that are plain
/// text, and why. `legion app list` and `status` show it as `corrupt`, the
/// reconciler and the registry of jobspec keys skip it, and `legion app
/// uninstall` removes it by name (docs/apps.md, "Upgrading Legion").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptApp {
    pub name: String,
    pub source: String,
    pub source_ref: String,
    pub enabled: bool,
    pub installed_by: String,
    pub installed_at: String,
    pub updated_at: String,
    pub uid: u32,
    pub error: String,
}

/// One `app` row's columns, before the JSON ones are decoded.
struct RawAppRow {
    name: String,
    source: String,
    source_ref: String,
    manifest: String,
    manifest_digest: String,
    enabled: bool,
    settings: String,
    config: String,
    grant_rules: String,
    granted_by: String,
    granted_at: String,
    installed_by: String,
    installed_at: String,
    updated_at: String,
    last_digest: String,
    last_action: String,
    last_error: String,
    last_at: String,
    build_marker: String,
    marker_reason: String,
    pending_change: String,
    uid: u32,
}

impl RawAppRow {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<RawAppRow> {
        Ok(RawAppRow {
            name: row.get(0)?,
            source: row.get(1)?,
            source_ref: row.get(2)?,
            manifest: row.get(3)?,
            manifest_digest: row.get(4)?,
            enabled: row.get::<_, i64>(5)? != 0,
            settings: row.get(6)?,
            config: row.get(7)?,
            grant_rules: row.get(8)?,
            granted_by: row.get(9)?,
            granted_at: row.get(10)?,
            installed_by: row.get(11)?,
            installed_at: row.get(12)?,
            updated_at: row.get(13)?,
            last_digest: row.get(14)?,
            last_action: row.get(15)?,
            last_error: row.get(16)?,
            last_at: row.get(17)?,
            build_marker: row.get(18)?,
            marker_reason: row.get(19)?,
            pending_change: row.get(20)?,
            uid: row.get(21)?,
        })
    }

    /// The record, or why it does not decode. Stored manifests and grants
    /// decode leniently ([`AppManifest::from_stored`]): a field another
    /// release wrote or dropped is not an error, a changed type is.
    fn decode(self) -> Result<AppRecord, CorruptApp> {
        let decoded = (|| -> Result<AppRecord, String> {
            let manifest = AppManifest::from_stored(&self.manifest)
                .map_err(|err| format!("app.manifest: {err}"))?;
            let marker = if self.build_marker.is_empty() {
                None
            } else {
                Some(json_column::<BuildMarker>(
                    &self.build_marker,
                    "build_marker",
                )?)
            };
            Ok(AppRecord {
                name: self.name.clone(),
                source: AppSource::parse(&self.source)?,
                source_ref: self.source_ref.clone(),
                manifest,
                manifest_digest: self.manifest_digest.clone(),
                enabled: self.enabled,
                settings: json_column::<AppSettings>(&self.settings, "settings")?,
                config: json_column::<Value>(&self.config, "config")?,
                grant: AppGrant {
                    rules: crate::structs::app::rules_from_stored(&self.grant_rules)
                        .map_err(|err| format!("app.grant_rules: {err}"))?,
                    granted_by: self.granted_by.clone(),
                    granted_at: self.granted_at.clone(),
                },
                installed_by: self.installed_by.clone(),
                installed_at: self.installed_at.clone(),
                updated_at: self.updated_at.clone(),
                state: AppReconcileState {
                    last_digest: self.last_digest.clone(),
                    last_action: self.last_action.clone(),
                    last_error: self.last_error.clone(),
                    last_at: self.last_at.clone(),
                    marker,
                    marker_reason: self.marker_reason.clone(),
                    pending_change: self.pending_change.clone(),
                },
                uid: self.uid,
            })
        })();
        decoded.map_err(|error| CorruptApp {
            name: self.name,
            source: self.source,
            source_ref: self.source_ref,
            enabled: self.enabled,
            installed_by: self.installed_by,
            installed_at: self.installed_at,
            updated_at: self.updated_at,
            uid: self.uid,
            error,
        })
    }
}

/// Corrupt rows already logged, by (name, `updated_at`, error): each is
/// logged once per manager, not on every read.
static REPORTED_CORRUPT: std::sync::Mutex<Option<std::collections::BTreeSet<String>>> =
    std::sync::Mutex::new(None);

fn report_corrupt(corrupt: &CorruptApp) {
    let key = format!(
        "{}\u{1f}{}\u{1f}{}",
        corrupt.name, corrupt.updated_at, corrupt.error
    );
    let mut reported = REPORTED_CORRUPT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if reported.get_or_insert_with(Default::default).insert(key) {
        error!(
            app = %corrupt.name,
            error = %corrupt.error,
            "App record does not decode; skipping it (`legion app uninstall {}` removes it)",
            corrupt.name
        );
    }
}

impl Store {
    /// Every app row, by name: each decoded on its own, so one row that does
    /// not decode is that row's problem, never every other app's (or every
    /// job submit's, which reads the installed jobspec keys).
    #[instrument(err, skip_all)]
    pub fn list_app_rows(&self) -> R<Vec<Result<AppRecord, CorruptApp>>> {
        let conn = self.read_connection()?;
        let mut stmt =
            conn.prepare(&format!("SELECT {APP_COLUMNS} FROM app ORDER BY name ASC;"))?;
        let rows = stmt
            .query_map([], RawAppRow::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows.into_iter().map(RawAppRow::decode).collect())
    }

    /// Every installed app that decodes, by name. A row that does not is
    /// logged (once) and left out; [`Self::list_app_rows`] has it.
    pub fn list_apps(&self) -> R<Vec<AppRecord>> {
        Ok(self
            .list_app_rows()?
            .into_iter()
            .filter_map(|row| match row {
                Ok(record) => Some(record),
                Err(corrupt) => {
                    report_corrupt(&corrupt);
                    None
                }
            })
            .collect())
    }

    /// App `name`'s row: `Some(Err)` when it does not decode.
    #[instrument(err, skip_all, fields(app = name))]
    pub fn get_app_row(&self, name: &str) -> R<Option<Result<AppRecord, CorruptApp>>> {
        let conn = self.read_connection()?;
        let raw = conn
            .query_row(
                &format!("SELECT {APP_COLUMNS} FROM app WHERE name = ?;"),
                [name],
                RawAppRow::from_row,
            )
            .optional()?;
        Ok(raw.map(RawAppRow::decode))
    }

    /// App `name`, or `None`. A row that does not decode is an error that
    /// names it and says how to remove it.
    pub fn get_app(&self, name: &str) -> R<Option<AppRecord>> {
        match self.get_app_row(name)? {
            None => Ok(None),
            Some(Ok(record)) => Ok(Some(record)),
            Some(Err(corrupt)) => {
                report_corrupt(&corrupt);
                Err(anyhow::Error::new(StorageError::failed_precondition(
                    format!(
                        "app {name}'s record does not decode ({}); `legion app uninstall {name}` \
                         removes it",
                        corrupt.error
                    ),
                )))
            }
        }
    }

    /// Whether an app named `name` is installed, decoded or not.
    pub fn app_exists(&self, name: &str) -> R<bool> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row("SELECT 1 FROM app WHERE name = ?;", [name], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Every installed app's name, decoded or not.
    pub fn app_names(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare("SELECT name FROM app ORDER BY name ASC;")?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        Ok(names)
    }

    /// The jobs in `legion-system` whose latest version the reconciler
    /// stamped: (job id, the stamp's app, the job's status). With
    /// [`Self::app_names`], the jobs no installed app manages any more.
    #[instrument(err, skip_all)]
    pub fn stamped_app_jobs(&self) -> R<Vec<(String, String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT j.name, json_extract(jc.meta, '$.managedBy.app'), j.status
             FROM job j
             INNER JOIN job_config jc ON jc.id = (
                 SELECT latest.id FROM job_config latest
                 WHERE latest.job_id = j.id
                 ORDER BY latest.version DESC LIMIT 1
             )
             WHERE j.name LIKE 'legion-system/%'
               AND json_extract(jc.meta, '$.managedBy.app') IS NOT NULL
               AND json_extract(jc.meta, '$.managedBy.object') IS NULL;",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<rusqlite::Result<Vec<(String, String, String)>>>()?;
        Ok(rows)
    }

    /// Install an app: refused when one of that name exists.
    #[instrument(err, skip_all, fields(app = %record.name))]
    pub fn insert_app(&self, record: AppRecord) -> R<()> {
        self.replicated(Lane::Control, "insert_app", move |s| {
            s.insert_app_apply(&record, false).map(|_| ())
        })
    }

    /// Install an app and record the install's idempotency key in the same
    /// replicated entry: a retry after a crash finds the key (and is
    /// `deduplicated`) exactly when it finds the record.
    #[instrument(err, skip_all, fields(app = %record.name))]
    pub fn insert_app_recorded(&self, record: AppRecord, key: &str, reply: &str) -> R<()> {
        let key = key.to_owned();
        let reply = reply.to_owned();
        self.replicated(Lane::Control, "insert_app", move |s| {
            s.insert_app_apply(&record, false)?;
            s.idempotency_record(&key, "install_app", &reply)?;
            Ok(())
        })
    }

    /// Seed a bundled app: inserted only when no app of that name exists, so
    /// every later leader's seed pass changes nothing. Returns whether it
    /// inserted.
    #[instrument(err, skip_all, fields(app = %record.name))]
    pub fn seed_app(&self, record: AppRecord) -> R<bool> {
        if self.app_exists(&record.name)? {
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
        let uid = Self::free_app_uid(&conn)?;
        conn.execute(
            &format!(
                "INSERT INTO app ({APP_COLUMNS}) VALUES \
                 (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
                  ?18, ?19, ?20, ?21, ?22);"
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
                uid,
            ],
        )?;
        Ok(true)
    }

    /// The lowest app uid no app holds ([`APP_UID_BASE`] up): the uid an
    /// app being inserted gets, whatever its record says. Chosen inside the
    /// replicated write, so two installs can never pick the same one.
    fn free_app_uid(conn: &rusqlite::Connection) -> R<u32> {
        let mut stmt = conn.prepare("SELECT uid FROM app WHERE uid >= ?1 ORDER BY uid ASC;")?;
        let taken = stmt
            .query_map([APP_UID_BASE], |row| row.get::<_, u32>(0))?
            .collect::<rusqlite::Result<Vec<u32>>>()?;
        let mut next = APP_UID_BASE;
        for uid in taken {
            if uid > next {
                break;
            }
            next = uid + 1;
        }
        if next >= APP_UID_BASE + APP_UID_COUNT {
            return Err(invalid(format!(
                "every app uid ({APP_UID_BASE} to {}) is taken; uninstall an app first",
                APP_UID_BASE + APP_UID_COUNT - 1
            )));
        }
        Ok(next)
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
        Err(anyhow::Error::new(StorageError::failed_precondition(
            format!(
                "app {} changed while this request ran; read it again and retry",
                record.name
            ),
        )))
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

    /// Delete an app record, and with `purge` every KV key under that
    /// prefix (and its locks), and record the uninstall's idempotency key
    /// with `reply`, all in one replicated entry. Returns the KV keys it
    /// deleted.
    #[instrument(err, skip_all, fields(app = name))]
    pub fn delete_app_recorded(
        &self,
        name: &str,
        purge: Option<&str>,
        key: &str,
        reply: &str,
    ) -> R<Vec<String>> {
        let name = name.to_owned();
        let purge = purge.map(str::to_owned);
        let key = key.to_owned();
        let reply = reply.to_owned();
        self.replicated(Lane::Control, "delete_app", move |s| {
            let purged = {
                let conn = s.write_connection()?;
                conn.execute("DELETE FROM app WHERE name = ?;", [&name])?;
                match &purge {
                    Some(prefix) => {
                        let mut stmt = conn.prepare(
                            "SELECT key FROM kv_entry WHERE substr(key, 1, length(?1)) = ?1;",
                        )?;
                        let keys = stmt
                            .query_map([prefix], |row| row.get::<_, String>(0))?
                            .collect::<rusqlite::Result<Vec<String>>>()?;
                        drop(stmt);
                        conn.execute(
                            "DELETE FROM kv_entry WHERE substr(key, 1, length(?1)) = ?1;",
                            [prefix],
                        )?;
                        conn.execute(
                            "DELETE FROM kv_lock WHERE substr(key, 1, length(?1)) = ?1;",
                            [prefix],
                        )?;
                        keys
                    }
                    None => Vec::new(),
                }
            };
            s.idempotency_record(&key, "uninstall_app", &reply)?;
            Ok(purged)
        })
    }

    /// The latest version of job `job_id`, with the stamp and submitter its
    /// meta carries; `None` when the job does not exist.
    #[instrument(err, skip_all, fields(job_id))]
    pub fn app_job_version(&self, job_id: &str) -> R<Option<AppJobVersion>> {
        /// The job's status, and its latest version's id, number, and meta.
        type LatestVersionRow = (String, Option<String>, Option<i64>, Option<Value>);
        let conn = self.read_connection()?;
        let row: Option<LatestVersionRow> = conn
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

    /// The app allocation `allocation_id` runs for: its job is
    /// `legion-system/<app>` **and** the version it runs carries the
    /// reconciler's `managedBy` stamp for that same app. A version someone
    /// submitted straight to the job carries none, so its allocations are
    /// nobody's app. `None` for every other allocation, and for one that does
    /// not exist.
    #[instrument(err, skip_all, fields(allocation_id))]
    pub fn allocation_app(&self, allocation_id: i64) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let row: Option<(String, Option<Value>)> = conn
            .prepare_cached(
                "SELECT tc.job_id, jc.meta FROM allocation a
                 INNER JOIN task_config tc ON tc.id = a.task_config_id
                 LEFT JOIN job_config jc ON jc.id = a.job_config_id
                 WHERE a.id = ?1;",
            )?
            .query_row([allocation_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        let Some((job, meta)) = row else {
            return Ok(None);
        };
        let Some(app) = crate::structs::app::app_name_of_job(&job) else {
            return Ok(None);
        };
        let stamp: Option<ManagedBy> = meta
            .and_then(|meta| meta.get("managedBy").cloned())
            .and_then(|value| serde_json::from_value(value).ok());
        // Only the reconciler's own stamp: a version an app wrote for one
        // of its objects (`managedBy.object`) never makes a job the app's.
        Ok(stamp
            .filter(|stamp| stamp.app == app && stamp.object.is_none())
            .map(|_| app.to_string()))
    }

    /// The `managedBy` stamp on job `job_id`'s latest version.
    pub fn job_managed_by(&self, job_id: &str) -> R<Option<ManagedBy>> {
        Ok(self
            .app_job_version(job_id)?
            .and_then(|version| version.managed_by))
    }

    /// Job `job_id`'s allocations across its versions: how many of the
    /// latest version run (their latest health report says healthy, as the
    /// scaling inputs' `ready` counts them), how many of older versions do,
    /// how many are live, and where the healthy ones are.
    #[instrument(err, skip_all, fields(job_id))]
    pub fn app_job_allocations(&self, job_id: &str) -> R<AppJobAllocations> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT COALESCE(ah.healthy, 0), a.node_id,
                    jc.id = (SELECT latest.id FROM job_config latest
                             WHERE latest.job_id = j.id
                             ORDER BY latest.version DESC LIMIT 1)
             FROM allocation a
             INNER JOIN job_config jc ON jc.id = a.job_config_id
             INNER JOIN job j ON j.id = jc.job_id
             LEFT JOIN allocation_health ah ON ah.allocation_id = a.id
             WHERE j.name = ?1 AND a.status NOT IN ('complete', 'failed');",
        )?;
        let rows = stmt.query_map([job_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut counts = AppJobAllocations::default();
        for row in rows {
            let (healthy, node, latest) = row?;
            counts.live += 1;
            if healthy != 0 {
                if latest != 0 {
                    counts.running += 1;
                } else {
                    counts.previous += 1;
                }
                if !counts.nodes.contains(&node) {
                    counts.nodes.push(node);
                }
            }
        }
        counts.nodes.sort();
        Ok(counts)
    }

    /// The newest rollout to job version `job_config_id`: (status,
    /// message). `failed` is an upgrade that did not take (rolled back, or
    /// stopped at a deadline).
    pub fn rollout_to_version(&self, job_config_id: &str) -> R<Option<(String, String)>> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row(
                "SELECT status, message FROM rollout WHERE to_job_config_id = ?1
                 ORDER BY id DESC LIMIT 1;",
                [job_config_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    }
}

impl Store {
    /// Job `job_id`'s live allocations (not complete, failed, or lost),
    /// healthy ones first, then newest first: the order a view request
    /// tries them in.
    #[instrument(err, skip_all, fields(job_id))]
    pub fn app_ui_allocations(&self, job_id: &str) -> R<Vec<AppUiAllocation>> {
        // Only allocations of versions the reconciler stamped for this app
        // ([`Self::allocation_app`]): a viewer assertion is never sent to a
        // version someone submitted straight to the job.
        let Some(app) = crate::structs::app::app_name_of_job(job_id) else {
            return Ok(Vec::new());
        };
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT a.id, a.node_id, COALESCE(ah.healthy, 0) FROM allocation a
             INNER JOIN job_config jc ON jc.id = a.job_config_id
             INNER JOIN job j ON j.id = jc.job_id
             LEFT JOIN allocation_health ah ON ah.allocation_id = a.id
             WHERE j.name = ?1 AND a.status NOT IN ('complete', 'failed', 'lost')
               AND json_extract(jc.meta, '$.managedBy.app') = ?2
               AND json_extract(jc.meta, '$.managedBy.object') IS NULL
             ORDER BY COALESCE(ah.healthy, 0) DESC, a.id DESC;",
        )?;
        let rows = stmt.query_map(params![job_id, app], |row| {
            Ok(AppUiAllocation {
                id: row.get(0)?,
                node_id: row.get(1)?,
                healthy: row.get::<_, i64>(2)? != 0,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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
            uid: 0,
        }
    }

    #[test]
    fn records_round_trip_and_writes_compare_and_set() -> R<()> {
        let store = store()?;
        let mut app = record("demo");
        check_record(&app)?;
        store.insert_app(app.clone())?;
        app.uid = APP_UID_BASE;
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
        assert_eq!(
            stored.state.marker.as_ref().map(|m| m.version.as_str()),
            Some("0.1.0")
        );

        assert!(store.delete_app("demo")?);
        assert!(store.get_app("demo")?.is_none());
        assert!(store.list_apps()?.is_empty());
        Ok(())
    }

    /// Each app gets its own uid, the lowest free one, whatever its record
    /// says; a freed uid is the next one handed out, and an update never
    /// moves it (hardening after the adversarial review).
    #[test]
    fn every_app_gets_its_own_uid() -> R<()> {
        let store = store()?;
        let uid = |name: &str| -> R<u32> { Ok(store.get_app(name)?.context("stored")?.uid) };
        for name in ["one", "two", "three"] {
            let mut app = record(name);
            app.uid = APP_UID_BASE; // what a record says is ignored
            store.insert_app(app)?;
        }
        assert_eq!(
            (uid("one")?, uid("two")?, uid("three")?),
            (APP_UID_BASE, APP_UID_BASE + 1, APP_UID_BASE + 2)
        );
        assert!(store.delete_app("two")?);
        store.insert_app(record("four"))?;
        assert_eq!(uid("four")?, APP_UID_BASE + 1);
        let mut moved = store.get_app("one")?.context("one")?;
        moved.uid = APP_UID_BASE + 9;
        moved.updated_at = "t1".into();
        store.update_app(moved, "t0")?;
        assert_eq!(uid("one")?, APP_UID_BASE);
        Ok(())
    }
}
