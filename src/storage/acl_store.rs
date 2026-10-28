//! Storage for ACL policies, tokens, and users.
//!
//! Secrets never touch this layer: tokens are stored as SHA-256 hashes and
//! users as argon2id PHC strings. Minting/verification lives in
//! `crate::manager::acl`.

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::acl::parse_rules;
use crate::structs::grant::{parse_grant_policy_ref, TaskGrant, GRANT_POLICY_PREFIX};
use crate::structs::{AclPolicy, AclTokenInfo, AclUser, ExecKey};

use rusqlite::{named_params, params, Connection, OptionalExtension, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Full ACL token row, including the secret hash. Never leaves the manager.
#[derive(Debug, Clone)]
pub struct AclTokenRecord {
    pub accessor_id: String,
    pub secret_hash: String,
    pub name: String,
    pub token_type: String,
    pub policies: Vec<String>,
    pub expires_at: Option<String>,
    pub created_at: String,
}

impl AclTokenRecord {
    pub fn to_info(&self) -> AclTokenInfo {
        AclTokenInfo {
            accessor_id: self.accessor_id.clone(),
            name: self.name.clone(),
            token_type: self.token_type.clone(),
            policies: self.policies.clone(),
            created_at: self.created_at.clone(),
            expires_at: self.expires_at.clone(),
        }
    }

    pub fn is_management(&self) -> bool {
        self.token_type == "management"
    }

    /// True when the token has an expiry in the past.
    pub fn is_expired(&self, now: OffsetDateTime) -> bool {
        match &self.expires_at {
            None => false,
            Some(raw) => match OffsetDateTime::parse(raw, &Rfc3339) {
                Ok(expiry) => expiry <= now,
                // Unparseable expiry fails closed.
                Err(_) => true,
            },
        }
    }
}

/// Full join token row, including the secret hash. Never leaves the
/// managers (a follower forwards a manager's own in-process worker's row to
/// the control leader, `ForwardableLaneWrite::InsertJoinToken`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JoinTokenRecord {
    pub id: String,
    pub secret_hash: String,
    pub worker_type: String,
    pub description: String,
    pub expires_at: Option<String>,
    pub created_at: String,
    /// The node config attributes a registration through this token gets
    /// whatever the worker reports (`namespaces`, `legion.role`), as a JSON
    /// object. Set for the token a manager mints for its own in-process
    /// worker; `None` takes them from the worker type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_attrs: Option<String>,
    /// The most new nodes that may register with the token (`None`: no
    /// limit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u32>,
    /// New nodes that have registered with it.
    #[serde(default)]
    pub uses: u32,
    /// Node attributes stamped on every node that registers with it.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// What [`Store::consume_join_token_use`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinTokenUse {
    /// The use was counted; the token has now been used this many times.
    Counted(u32),
    /// The token is spent: it has been used `uses` of `max_uses` times.
    Exhausted {
        /// Uses so far.
        uses: u32,
        /// The limit.
        max_uses: u32,
    },
    /// The token no longer exists (revoked or pruned meanwhile).
    Missing,
}

impl JoinTokenRecord {
    pub fn to_info(&self) -> crate::structs::JoinTokenInfo {
        crate::structs::JoinTokenInfo {
            id: self.id.clone(),
            worker_type: self.worker_type.clone(),
            description: self.description.clone(),
            expires_at: self.expires_at.clone(),
            created_at: self.created_at.clone(),
            max_uses: self.max_uses,
            uses: self.uses,
            attributes: self.attributes.clone().into_iter().collect(),
        }
    }

    /// True when the token has an expiry in the past.
    pub fn is_expired(&self, now: OffsetDateTime) -> bool {
        match &self.expires_at {
            None => false,
            Some(raw) => match OffsetDateTime::parse(raw, &Rfc3339) {
                Ok(expiry) => expiry <= now,
                // Unparseable expiry fails closed.
                Err(_) => true,
            },
        }
    }
}

/// Full ACL user row, including the password hash. Never leaves the manager.
#[derive(Debug, Clone)]
pub struct AclUserRecord {
    pub username: String,
    pub password_hash: String,
    pub policies: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl AclUserRecord {
    pub fn to_user(&self) -> AclUser {
        AclUser {
            username: self.username.clone(),
            policies: self.policies.clone(),
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
        }
    }
}

/// One job config version's grant: what its tasks' `legion` identities
/// were granted at submit (docs/workload-identity.md, "Permissions").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobGrantRecord {
    pub job_config_id: String,
    /// The canonical job id (`namespace/name`, or the bare name in
    /// `default`).
    pub job_id: String,
    pub version: i64,
    pub tasks: Vec<TaskGrant>,
    /// The principal whose submit was checked (`userpass:alice`), or the
    /// version's own grant carried forward by a KV re-render.
    pub granted_by: String,
    pub granted_at: String,
    pub revoked_at: Option<String>,
    pub revoked_by: Option<String>,
}

impl JobGrantRecord {
    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }
}

/// How a submit records the grant of the version it stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantSource {
    /// A principal submitted the job and the submit-time check passed: the
    /// version's grant is theirs. Recorded on every submit, an unchanged one
    /// included, so a resubmit by someone who holds the access restores a
    /// revoked grant.
    Submitter(String),
    /// Legion stored the version itself (a KV re-render): the previous
    /// version's grant carries forward when the new version asks for exactly
    /// the same and it is not revoked; otherwise the version has none.
    CarryForward,
    /// The app reconciler submitted an app's job, checked against the grant
    /// recorded on the app record (apps plan §6.4): recorded like a
    /// submitter's, in `granted_by`'s name, and the version is stamped
    /// `managedBy` (§6.3).
    App {
        granted_by: String,
        managed_by: crate::structs::app::ManagedBy,
    },
}

impl GrantSource {
    /// The principal a submit records the grant in the name of; `None` for
    /// a version Legion stored itself.
    pub fn granter(&self) -> Option<&str> {
        match self {
            GrantSource::Submitter(principal) => Some(principal),
            GrantSource::App { granted_by, .. } => Some(granted_by),
            GrantSource::CarryForward => None,
        }
    }
}

const JOB_GRANT_COLUMNS: &str =
    "job_config_id, job_id, version, tasks, granted_by, granted_at, revoked_at, revoked_by";

fn parse_policy_names(raw: String) -> Vec<String> {
    serde_json::from_str(&raw).unwrap_or_default()
}

fn invalid_input(message: String) -> anyhow::Error {
    anyhow::Error::new(StorageError::InvalidInput { message })
}

impl Store {
    fn acl_policy_from_row(row: &Row<'_>) -> rusqlite::Result<AclPolicy> {
        Ok(AclPolicy {
            name: row.get(0)?,
            description: row.get(1)?,
            rules_json: row.get(2)?,
            created_at: row.get(3)?,
            updated_at: row.get(4)?,
        })
    }

    #[instrument(err, skip_all, fields(policy = name))]
    pub fn upsert_acl_policy(
        &self,
        name: &str,
        description: &str,
        rules_json: &str,
    ) -> R<AclPolicy> {
        let name = name.to_owned();
        let description = description.to_owned();
        let rules_json = rules_json.to_owned();
        self.replicated(Lane::Control, "upsert_acl_policy", move |s| {
            s.upsert_acl_policy_apply(&name, &description, &rules_json)
        })
    }

    fn upsert_acl_policy_apply(
        &self,
        name: &str,
        description: &str,
        rules_json: &str,
    ) -> R<AclPolicy> {
        let name = name.trim();
        if name.is_empty() {
            return Err(invalid_input("policy name must not be empty".into()));
        }
        if name.starts_with(GRANT_POLICY_PREFIX) {
            return Err(invalid_input(format!(
                "policy names starting with `{GRANT_POLICY_PREFIX}` are reserved: a token carries \
                 `{GRANT_POLICY_PREFIX}alloc/<id>` for the grant of the job an allocation runs"
            )));
        }
        parse_rules(rules_json).map_err(|e| invalid_input(format!("invalid policy rules: {e}")))?;

        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT INTO acl_policy (name, description, rules, created_at, updated_at)
             VALUES (:name, :description, :rules, :now, :now)
             ON CONFLICT(name) DO UPDATE SET
                description = excluded.description,
                rules = excluded.rules,
                updated_at = excluded.updated_at;",
            named_params! {
                ":name": name,
                ":description": description,
                ":rules": rules_json,
                ":now": now,
            },
        )?;
        conn.query_row(
            "SELECT name, description, rules, created_at, updated_at FROM acl_policy WHERE name = ?;",
            [name],
            Self::acl_policy_from_row,
        )
        .map_err(Into::into)
    }

    #[instrument(err, skip_all, fields(policy = name))]
    pub fn get_acl_policy(&self, name: &str) -> R<Option<AclPolicy>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT name, description, rules, created_at, updated_at FROM acl_policy WHERE name = ?;",
            [name],
            Self::acl_policy_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    #[instrument(err, skip_all)]
    pub fn list_acl_policies(&self) -> R<Vec<AclPolicy>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT name, description, rules, created_at, updated_at FROM acl_policy ORDER BY name ASC;",
        )?;
        let rows = stmt.query_map([], Self::acl_policy_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(policy = name))]
    pub fn delete_acl_policy(&self, name: &str) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_acl_policy", move |s| {
            s.delete_acl_policy_apply(&name)
        })
    }

    fn delete_acl_policy_apply(&self, name: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM acl_policy WHERE name = ?;", [name])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "acl_policy",
                name,
            )));
        }
        Ok(())
    }

    /// Validates that every named policy exists; returns the missing names.
    pub fn missing_acl_policies(&self, names: &[String]) -> R<Vec<String>> {
        let mut missing = Vec::new();
        for name in names {
            if self.get_acl_policy(name)?.is_none() {
                missing.push(name.clone());
            }
        }
        Ok(missing)
    }

    /// Flattens the rule sets of the named policies, skipping unknown names
    /// (a deleted policy simply stops granting access).
    pub fn resolve_acl_rules(
        &self,
        policy_names: &[String],
    ) -> R<Vec<crate::structs::acl::AclRule>> {
        let mut rules = Vec::new();
        for name in policy_names {
            // A grant reference stands for the grant of the job an
            // allocation runs, read now, so a revoke applies at the next
            // request: its inline permissions, then its named policies.
            if let Some(allocation_id) = parse_grant_policy_ref(name) {
                if let Some(grant) = self.task_grant_for_allocation(allocation_id)? {
                    rules.extend(grant.permissions);
                    let named: Vec<String> = grant
                        .policies
                        .into_iter()
                        .filter(|policy| parse_grant_policy_ref(policy).is_none())
                        .collect();
                    rules.extend(self.resolve_acl_rules(&named)?);
                }
                continue;
            }
            if let Some(policy) = self.get_acl_policy(name)? {
                match parse_rules(&policy.rules_json) {
                    Ok(parsed) => rules.extend(parsed),
                    Err(e) => warn!(policy = %name, "Skipping unparseable ACL policy rules: {e}"),
                }
            }
        }
        Ok(rules)
    }

    /// The live grant of the task an allocation runs: its job config
    /// version's grant, unrevoked, narrowed to the allocation's own task.
    /// `None` when the allocation is gone, its version has no grant, or the
    /// grant was revoked. One indexed read; runs on every verification of a
    /// workload's `legion` token.
    pub fn task_grant_for_allocation(&self, allocation_id: i64) -> R<Option<TaskGrant>> {
        let conn = self.read_connection()?;
        let row: Option<(String, String, String)> = conn
            .prepare_cached(
                "SELECT g.tasks, tc.group_name, tc.name
                 FROM allocation a
                 JOIN job_grant g ON g.job_config_id = a.job_config_id
                 JOIN task_config tc ON tc.id = a.task_config_id
                 WHERE a.id = ?1 AND g.revoked_at IS NULL;",
            )?
            .query_row([allocation_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;
        let Some((tasks, group, task)) = row else {
            return Ok(None);
        };
        let tasks: Vec<TaskGrant> = serde_json::from_str(&tasks).unwrap_or_else(|err| {
            warn!(allocation_id, error = %err, "Unreadable job grant; granting nothing");
            Vec::new()
        });
        Ok(tasks
            .into_iter()
            .find(|grant| grant.group == group && grant.task == task))
    }

    /// Grants, newest version first: one job's (by canonical id) or every
    /// job's. Revoked grants are left out unless `include_revoked`.
    #[instrument(err, skip_all)]
    pub fn list_job_grants(
        &self,
        job_id: Option<&str>,
        include_revoked: bool,
    ) -> R<Vec<JobGrantRecord>> {
        let conn = self.read_connection()?;
        let mut sql = format!("SELECT {JOB_GRANT_COLUMNS} FROM job_grant WHERE 1 = 1");
        if job_id.is_some() {
            sql.push_str(" AND job_id = ?1");
        }
        if !include_revoked {
            sql.push_str(" AND revoked_at IS NULL");
        }
        sql.push_str(" ORDER BY job_id ASC, version DESC;");
        let mut stmt = conn.prepare(&sql)?;
        let rows = match job_id {
            Some(job_id) => stmt.query_map([job_id], Self::job_grant_from_row)?,
            None => stmt.query_map([], Self::job_grant_from_row)?,
        };
        Self::collect_rows(rows)
    }

    fn job_grant_from_row(row: &Row<'_>) -> rusqlite::Result<JobGrantRecord> {
        let tasks: String = row.get(3)?;
        Ok(JobGrantRecord {
            job_config_id: row.get(0)?,
            job_id: row.get(1)?,
            version: row.get(2)?,
            tasks: serde_json::from_str(&tasks).unwrap_or_default(),
            granted_by: row.get(4)?,
            granted_at: row.get(5)?,
            revoked_at: row.get(6)?,
            revoked_by: row.get(7)?,
        })
    }

    /// Revoke a job's grants: every version's, or one version's. A token
    /// resolves its grant on every verification, so the job's tokens lose
    /// the access at their next request. Returns how many grants this
    /// revoked (already-revoked ones are left as they are).
    #[instrument(err, skip_all, fields(job_id = job_id))]
    pub fn revoke_job_grants(
        &self,
        job_id: &str,
        version: Option<i64>,
        revoked_by: &str,
    ) -> R<u64> {
        let job_id = job_id.to_owned();
        let revoked_by = revoked_by.to_owned();
        self.replicated(Lane::Control, "revoke_job_grants", move |s| {
            s.revoke_job_grants_apply(&job_id, version, &revoked_by)
        })
    }

    fn revoke_job_grants_apply(
        &self,
        job_id: &str,
        version: Option<i64>,
        revoked_by: &str,
    ) -> R<u64> {
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        let revoked = conn.execute(
            "UPDATE job_grant SET revoked_at = ?1, revoked_by = ?2
             WHERE job_id = ?3 AND revoked_at IS NULL
               AND (?4 IS NULL OR version = ?4);",
            params![now, revoked_by, job_id, version],
        )?;
        Ok(revoked as u64)
    }

    /// Record the grant of the version a submit just stored (or found
    /// unchanged), inside the submit's own transaction. `tasks` is what the
    /// version asks for (`crate::structs::grant::requested_grants`); a
    /// version that asks for nothing gets no row.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_job_grant_tx(
        tx: &Connection,
        job_id: &str,
        job_config_id: &str,
        version: i64,
        previous_job_config_id: Option<&str>,
        tasks: &[TaskGrant],
        source: &GrantSource,
    ) -> R<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let tasks_json = serde_json::to_string(tasks)?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        match source {
            GrantSource::Submitter(principal)
            | GrantSource::App {
                granted_by: principal,
                ..
            } => {
                // An unchanged row stays as it is (no write, so a steady
                // resubmit adds nothing to the log); a revoked one, or one
                // another principal granted, is granted again by this one.
                tx.execute(
                    "INSERT INTO job_grant
                         (job_config_id, job_id, version, tasks, granted_by, granted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(job_config_id) DO UPDATE SET
                         tasks = excluded.tasks,
                         granted_by = excluded.granted_by,
                         granted_at = excluded.granted_at,
                         revoked_at = NULL,
                         revoked_by = NULL
                     WHERE job_grant.tasks IS NOT excluded.tasks
                        OR job_grant.granted_by IS NOT excluded.granted_by
                        OR job_grant.revoked_at IS NOT NULL;",
                    params![job_config_id, job_id, version, tasks_json, principal, now],
                )?;
            }
            GrantSource::CarryForward => {
                let Some(previous) = previous_job_config_id else {
                    return Ok(());
                };
                let carried: Option<(String, String)> = tx
                    .query_row(
                        "SELECT tasks, granted_by FROM job_grant
                         WHERE job_config_id = ?1 AND revoked_at IS NULL;",
                        [previous],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((previous_tasks, granted_by)) = carried else {
                    return Ok(());
                };
                let previous_tasks: Vec<TaskGrant> =
                    serde_json::from_str(&previous_tasks).unwrap_or_default();
                if previous_tasks != tasks {
                    warn!(
                        job_id,
                        job_config_id,
                        "A re-rendered job version asks for different permissions than the \
                         version it replaces; it carries no grant until someone who holds them \
                         resubmits it"
                    );
                    return Ok(());
                }
                tx.execute(
                    "INSERT OR IGNORE INTO job_grant
                         (job_config_id, job_id, version, tasks, granted_by, granted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                    params![job_config_id, job_id, version, tasks_json, granted_by, now],
                )?;
            }
        }
        Ok(())
    }

    fn acl_token_from_row(row: &Row<'_>) -> rusqlite::Result<AclTokenRecord> {
        Ok(AclTokenRecord {
            accessor_id: row.get(0)?,
            secret_hash: row.get(1)?,
            name: row.get(2)?,
            token_type: row.get(3)?,
            policies: parse_policy_names(row.get(4)?),
            expires_at: row.get(5)?,
            created_at: row.get(6)?,
        })
    }

    const ACL_TOKEN_COLUMNS: &'static str =
        "accessor_id, secret_hash, name, token_type, policies, expires_at, created_at";

    #[instrument(err, skip_all, fields(accessor_id = record.accessor_id, name = record.name))]
    pub fn insert_acl_token(&self, record: &AclTokenRecord) -> R<()> {
        let record = record.clone();
        self.replicated(Lane::Control, "insert_acl_token", move |s| {
            s.insert_acl_token_apply(&record)
        })
    }

    fn insert_acl_token_apply(&self, record: &AclTokenRecord) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT INTO acl_token (accessor_id, secret_hash, name, token_type, policies, expires_at, created_at)
             VALUES (:accessor_id, :secret_hash, :name, :token_type, :policies, :expires_at, :created_at);",
            named_params! {
                ":accessor_id": record.accessor_id,
                ":secret_hash": record.secret_hash,
                ":name": record.name,
                ":token_type": record.token_type,
                ":policies": serde_json::to_string(&record.policies)?,
                ":expires_at": record.expires_at,
                ":created_at": record.created_at,
            },
        )?;
        Ok(())
    }

    /// True when at least one management token exists. A cheap read the
    /// manager uses at startup to skip minting the initial token.
    pub fn has_management_token(&self) -> R<bool> {
        let conn = self.read_connection()?;
        let existing: i64 = conn.query_row(
            "SELECT COUNT(*) FROM acl_token WHERE token_type = 'management';",
            [],
            |row| row.get(0),
        )?;
        Ok(existing > 0)
    }

    /// Inserts the initial management token if and only if no management
    /// token exists yet. Returns false (without inserting) when one does.
    /// The check and insert share one transaction so concurrent bootstrap
    /// calls cannot both succeed.
    #[instrument(err, skip_all)]
    pub fn bootstrap_acl_token(&self, record: &AclTokenRecord) -> R<bool> {
        let record = record.clone();
        self.replicated(Lane::Control, "bootstrap_acl_token", move |s| {
            s.bootstrap_acl_token_apply(&record)
        })
    }

    fn bootstrap_acl_token_apply(&self, record: &AclTokenRecord) -> R<bool> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let existing: i64 = tx.query_row(
            "SELECT COUNT(*) FROM acl_token WHERE token_type = 'management';",
            [],
            |row| row.get(0),
        )?;
        if existing > 0 {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO acl_token (accessor_id, secret_hash, name, token_type, policies, expires_at, created_at)
             VALUES (:accessor_id, :secret_hash, :name, :token_type, :policies, :expires_at, :created_at);",
            named_params! {
                ":accessor_id": record.accessor_id,
                ":secret_hash": record.secret_hash,
                ":name": record.name,
                ":token_type": record.token_type,
                ":policies": serde_json::to_string(&record.policies)?,
                ":expires_at": record.expires_at,
                ":created_at": record.created_at,
            },
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Resolves a presented secret to its token record via SHA-256 hash.
    pub fn get_acl_token_by_hash(&self, secret_hash: &str) -> R<Option<AclTokenRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!(
                "SELECT {} FROM acl_token WHERE secret_hash = ?;",
                Self::ACL_TOKEN_COLUMNS
            ),
            [secret_hash],
            Self::acl_token_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    #[instrument(err, skip_all)]
    pub fn get_acl_token_by_accessor(&self, accessor_id: &str) -> R<Option<AclTokenRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!(
                "SELECT {} FROM acl_token WHERE accessor_id = ?;",
                Self::ACL_TOKEN_COLUMNS
            ),
            [accessor_id],
            Self::acl_token_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    #[instrument(err, skip_all)]
    pub fn list_acl_tokens(&self) -> R<Vec<AclTokenRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM acl_token ORDER BY created_at DESC, accessor_id ASC;",
            Self::ACL_TOKEN_COLUMNS
        ))?;
        let rows = stmt.query_map([], Self::acl_token_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(accessor_id))]
    pub fn delete_acl_token(&self, accessor_id: &str) -> R<()> {
        let accessor_id = accessor_id.to_owned();
        self.replicated(Lane::Control, "delete_acl_token", move |s| {
            s.delete_acl_token_apply(&accessor_id)
        })
    }

    fn delete_acl_token_apply(&self, accessor_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute(
            "DELETE FROM acl_token WHERE accessor_id = ?;",
            [accessor_id],
        )?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "acl_token",
                accessor_id,
            )));
        }
        Ok(())
    }

    /// Resolves a job config ID to its job name so config-addressed endpoints
    /// can be scope-checked against job-name policies.
    pub fn job_name_for_config(&self, config_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT name FROM job_config WHERE id = ?;",
            [config_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    fn join_token_from_row(row: &Row<'_>) -> rusqlite::Result<JoinTokenRecord> {
        Ok(JoinTokenRecord {
            id: row.get(0)?,
            secret_hash: row.get(1)?,
            worker_type: row.get(2)?,
            description: row.get(3)?,
            expires_at: row.get(4)?,
            created_at: row.get(5)?,
            pinned_attrs: row
                .get::<_, String>(6)
                .ok()
                .filter(|attrs| !attrs.is_empty()),
            max_uses: row
                .get::<_, Option<i64>>(7)?
                .map(|max| u32::try_from(max).unwrap_or(u32::MAX)),
            uses: u32::try_from(row.get::<_, i64>(8)?).unwrap_or(u32::MAX),
            attributes: {
                let raw: String = row.get(9)?;
                if raw.is_empty() {
                    Default::default()
                } else {
                    // An unreadable row stamps nothing rather than failing
                    // every lookup of the token.
                    serde_json::from_str(&raw).unwrap_or_default()
                }
            },
        })
    }

    const JOIN_TOKEN_COLUMNS: &'static str = "id, secret_hash, worker_type, description, \
         expires_at, created_at, pinned_attrs, max_uses, uses, attributes";

    #[instrument(err, skip_all, fields(id = record.id, worker_type = record.worker_type))]
    pub fn insert_join_token(&self, record: &JoinTokenRecord) -> R<()> {
        let record = record.clone();
        self.replicated(Lane::Control, "insert_join_token", move |s| {
            s.insert_join_token_apply(&record)
        })
    }

    /// [`Self::insert_join_token`], forwarded to the control lane's leader
    /// when this manager does not lead it. Only for the token a manager
    /// mints for its own in-process worker: a follower must be able to start
    /// its system worker, and nothing else would ever write that row.
    pub fn insert_join_token_forwarding(&self, record: &JoinTokenRecord) -> R<()> {
        match self.insert_join_token(record) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection::<()>(
                    crate::storage::raft::forward::ForwardableLaneWrite::InsertJoinToken {
                        record: record.clone(),
                    },
                    err,
                ),
            other => other,
        }
    }

    fn insert_join_token_apply(&self, record: &JoinTokenRecord) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT INTO join_token (id, secret_hash, worker_type, description, expires_at, created_at,
                                     pinned_attrs, max_uses, uses, attributes)
             VALUES (:id, :secret_hash, :worker_type, :description, :expires_at, :created_at,
                     :pinned_attrs, :max_uses, :uses, :attributes);",
            named_params! {
                ":id": record.id,
                ":secret_hash": record.secret_hash,
                ":worker_type": record.worker_type,
                ":description": record.description,
                ":expires_at": record.expires_at,
                ":created_at": record.created_at,
                ":pinned_attrs": record.pinned_attrs.clone().unwrap_or_default(),
                ":max_uses": record.max_uses.map(i64::from),
                ":uses": i64::from(record.uses),
                ":attributes": if record.attributes.is_empty() {
                    String::new()
                } else {
                    serde_json::to_string(&record.attributes)?
                },
            },
        )?;
        Ok(())
    }

    /// Count one new node's registration against a join token, refusing
    /// it when the token's `max_uses` is spent. Check and increment are one
    /// replicated write, so two registrations racing for a token's last use
    /// cannot both get it.
    #[instrument(err, skip_all, fields(id))]
    pub fn consume_join_token_use(&self, id: &str) -> R<JoinTokenUse> {
        let id = id.to_owned();
        self.replicated(Lane::Control, "consume_join_token_use", move |s| {
            s.consume_join_token_use_apply(&id)
        })
    }

    fn consume_join_token_use_apply(&self, id: &str) -> R<JoinTokenUse> {
        let conn = self.write_connection()?;
        let counted = conn.execute(
            "UPDATE join_token SET uses = uses + 1
             WHERE id = ?1 AND (max_uses IS NULL OR uses < max_uses);",
            [id],
        )?;
        let row: Option<(i64, Option<i64>)> = conn
            .query_row(
                "SELECT uses, max_uses FROM join_token WHERE id = ?1;",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let as_u32 = |value: i64| u32::try_from(value).unwrap_or(u32::MAX);
        Ok(match (counted, row) {
            (_, None) => JoinTokenUse::Missing,
            (0, Some((uses, max))) => JoinTokenUse::Exhausted {
                uses: as_u32(uses),
                max_uses: max.map(as_u32).unwrap_or(0),
            },
            (_, Some((uses, _))) => JoinTokenUse::Counted(as_u32(uses)),
        })
    }

    /// Resolves a presented join secret to its record via SHA-256 hash.
    pub fn get_join_token_by_hash(&self, secret_hash: &str) -> R<Option<JoinTokenRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!(
                "SELECT {} FROM join_token WHERE secret_hash = ?;",
                Self::JOIN_TOKEN_COLUMNS
            ),
            [secret_hash],
            Self::join_token_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    #[instrument(err, skip_all)]
    pub fn list_join_tokens(&self) -> R<Vec<JoinTokenRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM join_token ORDER BY created_at DESC, id ASC;",
            Self::JOIN_TOKEN_COLUMNS
        ))?;
        let rows = stmt.query_map([], Self::join_token_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(id))]
    pub fn delete_join_token(&self, id: &str) -> R<()> {
        let id = id.to_owned();
        self.replicated(Lane::Control, "delete_join_token", move |s| {
            s.delete_join_token_apply(&id)
        })
    }

    fn delete_join_token_apply(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM join_token WHERE id = ?;", [id])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "join_token",
                id,
            )));
        }
        Ok(())
    }

    /// Deletes expired ACL tokens and join tokens; returns counts removed.
    ///
    /// Expired credentials are already rejected at authentication time; this
    /// keeps the tables (and `legion acl token list`) tidy.
    #[instrument(err, skip_all)]
    pub fn prune_expired_credentials(&self) -> R<(usize, usize)> {
        self.replicated(Lane::Control, "prune_expired_credentials", move |s| {
            s.prune_expired_credentials_apply()
        })
    }

    fn prune_expired_credentials_apply(&self) -> R<(usize, usize)> {
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        let acl_tokens = conn.execute(
            "DELETE FROM acl_token WHERE expires_at IS NOT NULL AND expires_at <= ?;",
            [&now],
        )?;
        let join_tokens = conn.execute(
            "DELETE FROM join_token WHERE expires_at IS NOT NULL AND expires_at <= ?;",
            [&now],
        )?;
        Ok((acl_tokens, join_tokens))
    }

    /// Revokes any node tokens previously minted for `node_id` (used to
    /// rotate the credential when a worker re-registers).
    pub fn delete_node_tokens(&self, node_id: &str) -> R<usize> {
        let node_id = node_id.to_owned();
        self.replicated(Lane::Control, "delete_node_tokens", move |s| {
            s.delete_node_tokens_apply(&node_id)
        })
    }

    fn delete_node_tokens_apply(&self, node_id: &str) -> R<usize> {
        let conn = self.write_connection()?;
        let deleted = conn.execute(
            "DELETE FROM acl_token WHERE token_type = 'worker' AND name = ?;",
            [&format!("node:{node_id}")],
        )?;
        Ok(deleted)
    }

    fn acl_user_from_row(row: &Row<'_>) -> rusqlite::Result<AclUserRecord> {
        Ok(AclUserRecord {
            username: row.get(0)?,
            password_hash: row.get(1)?,
            policies: parse_policy_names(row.get(2)?),
            created_at: row.get(3)?,
            updated_at: row.get(4)?,
        })
    }

    /// Creates or updates a user. `password_hash = None` keeps the existing
    /// hash (and is an error for a new user).
    #[instrument(err, skip_all, fields(username))]
    pub fn upsert_acl_user(
        &self,
        username: &str,
        password_hash: Option<&str>,
        policies: &[String],
    ) -> R<AclUserRecord> {
        let username = username.to_owned();
        let password_hash = password_hash.map(str::to_owned);
        let policies = policies.to_vec();
        self.replicated(Lane::Control, "upsert_acl_user", move |s| {
            s.upsert_acl_user_apply(&username, password_hash.as_deref(), &policies)
        })
    }

    fn upsert_acl_user_apply(
        &self,
        username: &str,
        password_hash: Option<&str>,
        policies: &[String],
    ) -> R<AclUserRecord> {
        let username = username.trim();
        if username.is_empty() {
            return Err(invalid_input("username must not be empty".into()));
        }

        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let policies_json = serde_json::to_string(policies)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        let existing_hash: Option<String> = tx
            .query_row(
                "SELECT password_hash FROM acl_user WHERE username = ?;",
                [username],
                |row| row.get(0),
            )
            .optional()?;
        let effective_hash = match (password_hash, existing_hash) {
            (Some(hash), _) => hash.to_string(),
            (None, Some(existing)) => existing,
            (None, None) => {
                return Err(invalid_input(
                    "password is required when creating a new user".into(),
                ))
            }
        };

        tx.execute(
            "INSERT INTO acl_user (username, password_hash, policies, created_at, updated_at)
             VALUES (:username, :hash, :policies, :now, :now)
             ON CONFLICT(username) DO UPDATE SET
                password_hash = excluded.password_hash,
                policies = excluded.policies,
                updated_at = excluded.updated_at;",
            named_params! {
                ":username": username,
                ":hash": effective_hash,
                ":policies": policies_json,
                ":now": now,
            },
        )?;
        let user = tx.query_row(
            "SELECT username, password_hash, policies, created_at, updated_at FROM acl_user WHERE username = ?;",
            [username],
            Self::acl_user_from_row,
        )?;
        tx.commit()?;
        Ok(user)
    }

    pub fn get_acl_user(&self, username: &str) -> R<Option<AclUserRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT username, password_hash, policies, created_at, updated_at FROM acl_user WHERE username = ?;",
            [username],
            Self::acl_user_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    #[instrument(err, skip_all)]
    pub fn list_acl_users(&self) -> R<Vec<AclUserRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT username, password_hash, policies, created_at, updated_at FROM acl_user ORDER BY username ASC;",
        )?;
        let rows = stmt.query_map([], Self::acl_user_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(username))]
    pub fn delete_acl_user(&self, username: &str) -> R<()> {
        let username = username.to_owned();
        self.replicated(Lane::Control, "delete_acl_user", move |s| {
            s.delete_acl_user_apply(&username)
        })
    }

    fn delete_acl_user_apply(&self, username: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM acl_user WHERE username = ?;", [username])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "acl_user", username,
            )));
        }
        Ok(())
    }
}

// --- Delegated exec minting keys (sandbox-exec plan §6.5;
// docs/exec-and-actions.md, "Delegated minting keys"). One control-lane row
// per key: a platform's public key, the job glob its tickets may reach, and
// their ceilings. Nothing here is secret.

impl Store {
    const EXEC_KEY_COLUMNS: &'static str = "id, name, jobs, public_key_pem, max_ttl_seconds, \
         max_session_seconds, created_by, created_at";

    fn exec_key_from_row(row: &Row) -> rusqlite::Result<ExecKey> {
        Ok(ExecKey {
            id: row.get(0)?,
            name: row.get(1)?,
            jobs: row.get(2)?,
            public_key_pem: row.get(3)?,
            max_ttl_seconds: row.get::<_, i64>(4)?.max(0) as u64,
            max_session_seconds: row.get::<_, i64>(5)?.max(0) as u64,
            created_by: row.get(6)?,
            created_at: row.get(7)?,
        })
    }

    #[instrument(err, skip_all, fields(id = key.id))]
    pub fn insert_exec_key(&self, key: &ExecKey) -> R<()> {
        let key = key.clone();
        self.replicated(Lane::Control, "insert_exec_key", move |s| {
            s.insert_exec_key_apply(&key)
        })
    }

    fn insert_exec_key_apply(&self, key: &ExecKey) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT INTO exec_key (id, name, jobs, public_key_pem, max_ttl_seconds,
                 max_session_seconds, created_by, created_at)
             VALUES (:id, :name, :jobs, :public_key_pem, :max_ttl_seconds,
                 :max_session_seconds, :created_by, :created_at);",
            named_params! {
                ":id": key.id,
                ":name": key.name,
                ":jobs": key.jobs,
                ":public_key_pem": key.public_key_pem,
                ":max_ttl_seconds": key.max_ttl_seconds as i64,
                ":max_session_seconds": key.max_session_seconds as i64,
                ":created_by": key.created_by,
                ":created_at": key.created_at,
            },
        )?;
        Ok(())
    }

    pub fn get_exec_key(&self, id: &str) -> R<Option<ExecKey>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!(
                "SELECT {} FROM exec_key WHERE id = ?;",
                Self::EXEC_KEY_COLUMNS
            ),
            [id],
            Self::exec_key_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Every key, newest first.
    pub fn list_exec_keys(&self) -> R<Vec<ExecKey>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM exec_key ORDER BY created_at DESC, id ASC;",
            Self::EXEC_KEY_COLUMNS
        ))?;
        let rows = stmt.query_map([], Self::exec_key_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(id))]
    pub fn delete_exec_key(&self, id: &str) -> R<()> {
        let id = id.to_owned();
        self.replicated(Lane::Control, "delete_exec_key", move |s| {
            s.delete_exec_key_apply(&id)
        })
    }

    fn delete_exec_key_apply(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM exec_key WHERE id = ?;", [id])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found("exec_key", id)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod acl_store_test {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-acl-store-")
            .tempdir_in(crate::test_scratch::root())
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

    fn token_record(accessor: &str, token_type: &str) -> AclTokenRecord {
        AclTokenRecord {
            accessor_id: accessor.to_string(),
            secret_hash: format!("hash-{accessor}"),
            name: format!("token-{accessor}"),
            token_type: token_type.to_string(),
            policies: vec!["readers".to_string()],
            expires_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn policy_crud_roundtrip_and_validation() -> R<()> {
        let store = setup_store()?;

        let rules = r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#;
        let policy = store.upsert_acl_policy("readers", "read jobs", rules)?;
        assert_eq!(policy.name, "readers");
        assert_eq!(policy.rules_json, rules);

        // Invalid rules are rejected.
        assert!(store
            .upsert_acl_policy("bad", "", r#"[{"resources": [], "actions": ["read"]}]"#)
            .is_err());

        // Update keeps created_at, bumps content.
        let updated = store.upsert_acl_policy("readers", "v2", rules)?;
        assert_eq!(updated.description, "v2");
        assert_eq!(updated.created_at, policy.created_at);

        assert_eq!(store.list_acl_policies()?.len(), 1);
        assert!(store.get_acl_policy("readers")?.is_some());
        assert_eq!(
            store.missing_acl_policies(&["readers".into(), "nope".into()])?,
            vec!["nope".to_string()]
        );

        store.delete_acl_policy("readers")?;
        assert!(store.get_acl_policy("readers")?.is_none());
        assert!(store.delete_acl_policy("readers").is_err());
        Ok(())
    }

    #[test]
    fn token_insert_lookup_and_delete() -> R<()> {
        let store = setup_store()?;
        let record = token_record("acc-1", "client");
        store.insert_acl_token(&record)?;

        let found = store
            .get_acl_token_by_hash("hash-acc-1")?
            .expect("token should resolve by hash");
        assert_eq!(found.accessor_id, "acc-1");
        assert_eq!(found.policies, vec!["readers".to_string()]);
        assert!(!found.is_management());
        assert!(!found.is_expired(OffsetDateTime::now_utc()));

        assert!(store.get_acl_token_by_hash("missing")?.is_none());
        assert_eq!(store.list_acl_tokens()?.len(), 1);

        store.delete_acl_token("acc-1")?;
        assert!(store.get_acl_token_by_hash("hash-acc-1")?.is_none());
        Ok(())
    }

    #[test]
    fn token_expiry_checks() {
        let mut record = token_record("acc-2", "client");
        let now = OffsetDateTime::now_utc();
        record.expires_at = Some("2020-01-01T00:00:00Z".to_string());
        assert!(record.is_expired(now));
        record.expires_at = Some("2099-01-01T00:00:00Z".to_string());
        assert!(!record.is_expired(now));
        record.expires_at = Some("garbage".to_string());
        assert!(
            record.is_expired(now),
            "unparseable expiry must fail closed"
        );
    }

    #[test]
    fn bootstrap_only_succeeds_once() -> R<()> {
        let store = setup_store()?;
        assert!(store.bootstrap_acl_token(&token_record("boot-1", "management"))?);
        assert!(!store.bootstrap_acl_token(&token_record("boot-2", "management"))?);
        assert_eq!(store.list_acl_tokens()?.len(), 1);

        // Deleting the management token lets the next start mint one (documented
        // recovery path when the initial token is lost).
        store.delete_acl_token("boot-1")?;
        assert!(store.bootstrap_acl_token(&token_record("boot-3", "management"))?);
        Ok(())
    }

    #[test]
    fn user_upsert_keeps_password_when_omitted() -> R<()> {
        let store = setup_store()?;

        // New user without a password is rejected.
        assert!(store.upsert_acl_user("alice", None, &[]).is_err());

        let created = store.upsert_acl_user("alice", Some("hash-1"), &["readers".into()])?;
        assert_eq!(created.password_hash, "hash-1");

        // Update policies without touching the password.
        let updated = store.upsert_acl_user("alice", None, &["writers".into()])?;
        assert_eq!(updated.password_hash, "hash-1");
        assert_eq!(updated.policies, vec!["writers".to_string()]);

        // Update the password.
        let rehashed = store.upsert_acl_user("alice", Some("hash-2"), &["writers".into()])?;
        assert_eq!(rehashed.password_hash, "hash-2");

        assert_eq!(store.list_acl_users()?.len(), 1);
        store.delete_acl_user("alice")?;
        assert!(store.get_acl_user("alice")?.is_none());
        assert!(store.delete_acl_user("alice").is_err());
        Ok(())
    }

    #[test]
    fn join_token_crud_and_pruning() -> R<()> {
        let store = setup_store()?;
        let record = JoinTokenRecord {
            id: "jt-1".into(),
            secret_hash: "hash-jt-1".into(),
            worker_type: "gpu".into(),
            description: "rack 12".into(),
            expires_at: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            pinned_attrs: None,
            max_uses: None,
            uses: 0,
            attributes: Default::default(),
        };
        store.insert_join_token(&record)?;

        let found = store
            .get_join_token_by_hash("hash-jt-1")?
            .expect("join token resolves by hash");
        assert_eq!(found.worker_type, "gpu");
        assert!(!found.is_expired(OffsetDateTime::now_utc()));
        assert_eq!(store.list_join_tokens()?.len(), 1);

        // An expired token is pruned; unexpired ones survive.
        store.insert_join_token(&JoinTokenRecord {
            id: "jt-2".into(),
            secret_hash: "hash-jt-2".into(),
            worker_type: "default".into(),
            description: String::new(),
            expires_at: Some("2020-01-01T00:00:00Z".into()),
            created_at: "2020-01-01T00:00:00Z".into(),
            pinned_attrs: None,
            max_uses: None,
            uses: 0,
            attributes: Default::default(),
        })?;
        let (acl_pruned, join_pruned) = store.prune_expired_credentials()?;
        assert_eq!((acl_pruned, join_pruned), (0, 1));
        assert!(store.get_join_token_by_hash("hash-jt-2")?.is_none());

        store.delete_join_token("jt-1")?;
        assert!(store.delete_join_token("jt-1").is_err());
        Ok(())
    }

    #[test]
    fn prune_removes_expired_acl_tokens() -> R<()> {
        let store = setup_store()?;
        let mut expired = token_record("expired", "client");
        expired.expires_at = Some("2020-01-01T00:00:00Z".into());
        store.insert_acl_token(&expired)?;
        store.insert_acl_token(&token_record("fresh", "client"))?;

        let (acl_pruned, _) = store.prune_expired_credentials()?;
        assert_eq!(acl_pruned, 1);
        assert_eq!(store.list_acl_tokens()?.len(), 1);
        Ok(())
    }

    #[test]
    fn node_token_rotation_deletes_by_node_name() -> R<()> {
        let store = setup_store()?;
        let mut node_token = token_record("nt-1", "worker");
        node_token.name = "node:worker-a".into();
        store.insert_acl_token(&node_token)?;
        let mut other = token_record("nt-2", "worker");
        other.name = "node:worker-b".into();
        store.insert_acl_token(&other)?;

        assert_eq!(store.delete_node_tokens("worker-a")?, 1);
        assert_eq!(store.list_acl_tokens()?.len(), 1);
        assert_eq!(store.delete_node_tokens("worker-a")?, 0);
        Ok(())
    }

    #[test]
    fn resolve_acl_rules_flattens_and_skips_missing() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy(
            "a",
            "",
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
        )?;
        store.upsert_acl_policy(
            "b",
            "",
            r#"[{"resources": ["kv"], "actions": ["*"], "paths": ["x/*"]}]"#,
        )?;

        let rules = store.resolve_acl_rules(&["a".into(), "missing".into(), "b".into()])?;
        assert_eq!(rules.len(), 2);
        Ok(())
    }
}
