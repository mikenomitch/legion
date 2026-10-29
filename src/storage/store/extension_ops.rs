//! Reads for jobspec extensions (apps plan §6.6; docs/apps.md, "Jobspec
//! extensions"): which jobs' latest versions hold extension values or
//! pending keys, and cheap fingerprints the extension registry cache and
//! `WatchAppExtensions` poll before doing a full read.

use super::*;

/// One job whose latest config holds extension values or pending keys.
#[derive(Debug, Clone)]
pub struct ExtensionJob {
    pub job_id: String,
    pub status: String,
    pub config: JobConfig,
}

impl Store {
    /// The installed apps, as a fingerprint that changes whenever any app
    /// record is written (install, upgrade, uninstall, enable, settings).
    pub fn app_fingerprint(&self) -> R<String> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached("SELECT name, updated_at FROM app ORDER BY name;")?;
        let rows = stmt.query_map([], |row| {
            Ok(format!(
                "{}@{}",
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?
            ))
        })?;
        Ok(Self::collect_rows(rows)?.join(","))
    }

    /// A cheap fingerprint of every job's versions and status: changes when
    /// a version is stored or deleted, or a job stops, fails, or goes.
    pub fn job_versions_fingerprint(&self) -> R<String> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT (SELECT COUNT(*) FROM job_config), \
                    (SELECT COALESCE(MAX(rowid), 0) FROM job_config), \
                    (SELECT COUNT(*) FROM job), \
                    (SELECT COUNT(*) FROM job WHERE status IN ('stopped', 'failed'));",
            [],
            |row| {
                Ok(format!(
                    "{}:{}:{}:{}",
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?
                ))
            },
        )?)
    }

    /// Every job whose latest version stores extension values or pending
    /// keys at any point, with that version, by job id.
    pub fn extension_jobs(&self) -> R<Vec<ExtensionJob>> {
        let rows: Vec<(String, String, String)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare_cached(&format!(
                "SELECT j.name, j.status, jc.id FROM job j \
                 INNER JOIN job_config jc ON jc.id = {LATEST_CONFIG_FOR_JOB} \
                 WHERE json_extract(jc.meta, '$.extensions') IS NOT NULL \
                    OR json_extract(jc.meta, '$.pendingExtensions') IS NOT NULL \
                    OR EXISTS (SELECT 1 FROM task_config tc WHERE tc.job_config_id = jc.id AND \
                         (tc.pending_extensions IS NOT NULL \
                          OR instr(COALESCE(tc.volumes, ''), 'pendingExtensions') > 0 \
                          OR instr(COALESCE(tc.network, ''), 'pendingExtensions') > 0 \
                          OR instr(COALESCE(tc.service, ''), 'pendingExtensions') > 0)) \
                 ORDER BY j.name;"
            ))?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            Self::collect_rows(rows)?
        };
        let mut jobs = Vec::new();
        for (job_id, status, config_id) in rows {
            if let Some(config) = self.get_job_config_by_id_optional(&config_id)? {
                jobs.push(ExtensionJob {
                    job_id,
                    status,
                    config,
                });
            }
        }
        Ok(jobs)
    }
}
