//! Storage for agent governance (internal_docs/plans/agent-governance-plan.md):
//! the audit journal, approval requests, ACL token lineage and limits, and
//! hourly usage rollups.
//!
//! - `audit_event` is a commutative table (random 128-bit ids, any lane
//!   inserts, pruned locally like `scheduler_event`), so every manager can
//!   serve the journal and a batch is one raft entry on whichever lane the
//!   writing manager leads.
//! - `approval_request` and `acl_token_meta` are control-lane rows: operator
//!   intent, written by the gate, the approval RPCs, and token minting.
//! - `usage_rollup` is a status-lane table written by the status-lane
//!   leader's accumulator (`crate::manager::governance::usage`).

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::store::Store;
use crate::storage::Lane;

use rusqlite::{params, params_from_iter, OptionalExtension, Row};
use serde::{Deserialize, Serialize};

/// One audit journal record. Never carries a request body or a secret.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRecord {
    pub id: String,
    pub ts_ms: i64,
    pub manager: String,
    pub operation: String,
    /// `write`, `denied`, or `audited` (an always-audited RPC that checked
    /// no write permission, such as `Login`).
    pub kind: String,
    pub accessor_id: String,
    pub principal: String,
    pub source: String,
    pub on_behalf_of: String,
    pub parent_accessor_id: String,
    pub resource: String,
    pub action: String,
    pub target: String,
    pub namespace: String,
    pub request_digest: String,
    pub idempotency_key: String,
    pub approval_id: String,
    pub outcome: String,
    pub message: String,
    pub detail: String,
    pub duration_ms: i64,
}

/// Filters for [`Store::list_audit_events`]. Empty strings match anything.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub accessor_id: String,
    pub principal: String,
    pub operation: String,
    pub target_prefix: String,
    pub namespace: String,
    pub outcome: String,
    pub kind: String,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// Newest-first paging: only records strictly older than this cursor.
    pub before: Option<(i64, String)>,
    /// Oldest-first following: only records strictly newer than this cursor.
    pub after: Option<(i64, String)>,
    pub limit: usize,
}

/// A cursor for [`AuditFilter::before`]/[`AuditFilter::after`]: `<ts_ms>:<id>`.
pub fn audit_cursor(record: &AuditRecord) -> String {
    format!("{}:{}", record.ts_ms, record.id)
}

/// Parses a cursor written by [`audit_cursor`].
pub fn parse_audit_cursor(raw: &str) -> Option<(i64, String)> {
    let (ts, id) = raw.split_once(':')?;
    Some((ts.parse().ok()?, id.to_string()))
}

/// One approval request (docs/approvals-and-rate-limits.md).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRecord {
    pub id: String,
    pub created_ms: i64,
    pub expires_ms: i64,
    pub ttl_secs: i64,
    pub rule: String,
    pub operation: String,
    pub permission: String,
    pub target: String,
    pub namespace: String,
    pub request_digest: String,
    pub requester_principal: String,
    pub requester_accessor: String,
    pub requester_root: String,
    pub requester_source: String,
    pub on_behalf_of: String,
    /// `pending`, `approved`, `denied`, or `consumed`. `expired` is computed
    /// from `expires_ms` by [`ApprovalRecord::effective_status`].
    pub status: String,
    pub decided_by: String,
    pub decided_ms: i64,
    pub reason: String,
    pub consumed_ms: i64,
}

impl ApprovalRecord {
    /// The status a reader sees: `expired` once a pending or approved
    /// request is past its expiry.
    pub fn effective_status(&self, now_ms: i64) -> &str {
        match self.status.as_str() {
            "pending" | "approved" if now_ms >= self.expires_ms => "expired",
            other => other,
        }
    }
}

/// Why [`Store::consume_approval`] refused an approval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalUse {
    Consumed,
    Refused(String),
}

/// A token's lineage and limits (`acl_token_meta`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TokenMeta {
    pub accessor_id: String,
    /// Empty for a token minted by `CreateAclToken` or `Login`.
    pub parent_accessor_id: String,
    pub root_accessor_id: String,
    pub depth: i64,
    /// Writes per second; 0 = use the configured default.
    pub write_rate: f64,
    pub write_burst: i64,
    pub created_by: String,
}

/// One usage rollup row: `amount` of `dimension` (unit-seconds) in the hour
/// starting at `bucket_ms`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRow {
    pub bucket_ms: i64,
    pub namespace: String,
    pub job_id: String,
    pub submitter: String,
    pub dimension: String,
    pub amount: f64,
}

/// An allocation holding capacity, as the usage accumulator reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct HeldAllocation {
    pub job_id: String,
    pub resources: serde_json::Value,
}

const AUDIT_COLUMNS: &str = "id, ts_ms, manager, operation, kind, accessor_id, principal, \
     source, on_behalf_of, parent_accessor_id, resource, action, target, namespace, \
     request_digest, idempotency_key, approval_id, outcome, message, detail, duration_ms";

const APPROVAL_COLUMNS: &str = "id, created_ms, expires_ms, ttl_secs, rule, operation, \
     permission, target, namespace, request_digest, requester_principal, requester_accessor, \
     requester_root, requester_source, on_behalf_of, status, decided_by, decided_ms, reason, \
     consumed_ms";

fn audit_from_row(row: &Row<'_>) -> rusqlite::Result<AuditRecord> {
    Ok(AuditRecord {
        id: row.get(0)?,
        ts_ms: row.get(1)?,
        manager: row.get(2)?,
        operation: row.get(3)?,
        kind: row.get(4)?,
        accessor_id: row.get(5)?,
        principal: row.get(6)?,
        source: row.get(7)?,
        on_behalf_of: row.get(8)?,
        parent_accessor_id: row.get(9)?,
        resource: row.get(10)?,
        action: row.get(11)?,
        target: row.get(12)?,
        namespace: row.get(13)?,
        request_digest: row.get(14)?,
        idempotency_key: row.get(15)?,
        approval_id: row.get(16)?,
        outcome: row.get(17)?,
        message: row.get(18)?,
        detail: row.get(19)?,
        duration_ms: row.get(20)?,
    })
}

fn approval_from_row(row: &Row<'_>) -> rusqlite::Result<ApprovalRecord> {
    Ok(ApprovalRecord {
        id: row.get(0)?,
        created_ms: row.get(1)?,
        expires_ms: row.get(2)?,
        ttl_secs: row.get(3)?,
        rule: row.get(4)?,
        operation: row.get(5)?,
        permission: row.get(6)?,
        target: row.get(7)?,
        namespace: row.get(8)?,
        request_digest: row.get(9)?,
        requester_principal: row.get(10)?,
        requester_accessor: row.get(11)?,
        requester_root: row.get(12)?,
        requester_source: row.get(13)?,
        on_behalf_of: row.get(14)?,
        status: row.get(15)?,
        decided_by: row.get(16)?,
        decided_ms: row.get(17)?,
        reason: row.get(18)?,
        consumed_ms: row.get(19)?,
    })
}

/// Escapes `%`, `_` and `\` for a `LIKE … ESCAPE '\'` prefix match.
fn like_prefix(prefix: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + 1);
    for ch in prefix.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('%');
    out
}

impl Store {
    // ── audit journal ────────────────────────────────────────────────────

    /// The lane an audit batch rides: one this manager leads (the table is
    /// commutative, so any lane may carry the insert), or the control lane
    /// outside raft mode. `None` in raft mode on a manager leading nothing.
    fn audit_lane(&self) -> Option<Lane> {
        if !self.raft_mode() {
            return Some(Lane::Control);
        }
        [Lane::Status, Lane::Control, Lane::Scheduler]
            .into_iter()
            .find(|lane| self.is_raft_leader(*lane))
    }

    /// Whether this manager can write audit batches right now (see
    /// [`Self::append_audit_events`]).
    pub fn can_write_audit(&self) -> bool {
        self.audit_lane().is_some()
    }

    /// Appends a batch of audit records as one replicated entry. Fails with
    /// the typed not-the-leader error in raft mode when this manager leads no
    /// lane; the governance writer then logs the batch instead.
    pub fn append_audit_events(&self, records: Vec<AuditRecord>) -> R<()> {
        if records.is_empty() {
            return Ok(());
        }
        let Some(lane) = self.audit_lane() else {
            bail!("this manager leads no raft lane, so it cannot journal audit records");
        };
        self.replicated(lane, "append_audit_events", move |s| {
            let conn = s.write_connection()?;
            let mut stmt = conn.prepare_cached(&format!(
                "INSERT OR IGNORE INTO audit_event ({AUDIT_COLUMNS}) VALUES (?1, ?2, ?3, ?4, \
                 ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21);"
            ))?;
            for r in &records {
                stmt.execute(params![
                    r.id,
                    r.ts_ms,
                    r.manager,
                    r.operation,
                    r.kind,
                    r.accessor_id,
                    r.principal,
                    r.source,
                    r.on_behalf_of,
                    r.parent_accessor_id,
                    r.resource,
                    r.action,
                    r.target,
                    r.namespace,
                    r.request_digest,
                    r.idempotency_key,
                    r.approval_id,
                    r.outcome,
                    r.message,
                    r.detail,
                    r.duration_ms,
                ])?;
            }
            Ok(())
        })
    }

    /// Audit records matching `filter`: newest first, or oldest first when
    /// following (`filter.after`).
    pub fn list_audit_events(&self, filter: &AuditFilter) -> R<Vec<AuditRecord>> {
        let mut clauses: Vec<String> = Vec::new();
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        let mut push = |clause: &str, value: rusqlite::types::Value| {
            values.push(value);
            clauses.push(clause.replace('?', &format!("?{}", values.len())));
        };
        for (column, value) in [
            ("accessor_id", &filter.accessor_id),
            ("principal", &filter.principal),
            ("operation", &filter.operation),
            ("namespace", &filter.namespace),
            ("outcome", &filter.outcome),
            ("kind", &filter.kind),
        ] {
            if !value.is_empty() {
                push(&format!("{column} = ?"), value.clone().into());
            }
        }
        if !filter.target_prefix.is_empty() {
            push(
                "target LIKE ? ESCAPE '\\'",
                like_prefix(&filter.target_prefix).into(),
            );
        }
        if let Some(since) = filter.since_ms {
            push("ts_ms >= ?", since.into());
        }
        if let Some(until) = filter.until_ms {
            push("ts_ms < ?", until.into());
        }
        let ascending = filter.after.is_some();
        if let Some((ts, id)) = &filter.before {
            values.push((*ts).into());
            values.push(id.clone().into());
            let (a, b) = (values.len() - 1, values.len());
            clauses.push(format!("(ts_ms < ?{a} OR (ts_ms = ?{a} AND id < ?{b}))"));
        }
        if let Some((ts, id)) = &filter.after {
            values.push((*ts).into());
            values.push(id.clone().into());
            let (a, b) = (values.len() - 1, values.len());
            clauses.push(format!("(ts_ms > ?{a} OR (ts_ms = ?{a} AND id > ?{b}))"));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let order = if ascending { "ASC" } else { "DESC" };
        let limit = filter.limit.clamp(1, 1000);
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {AUDIT_COLUMNS} FROM audit_event {where_clause} \
             ORDER BY ts_ms {order}, id {order} LIMIT {limit};"
        ))?;
        let rows = stmt.query_map(params_from_iter(values), audit_from_row)?;
        Self::collect_rows(rows)
    }

    /// Local retention pruning of the journal (JC-28: commutative tables are
    /// pruned per node, outside raft): rows older than `before_ms`, then
    /// everything beyond the newest `retain_rows`. Returns rows deleted.
    pub fn prune_audit_events_local(&self, retain_rows: u64, before_ms: i64) -> R<usize> {
        self.local_write("prune_audit_events", move |s| {
            let conn = s.write_connection()?;
            let mut deleted =
                conn.execute("DELETE FROM audit_event WHERE ts_ms < ?1;", [before_ms])?;
            deleted += conn.execute(
                "DELETE FROM audit_event WHERE rowid IN (
                     SELECT rowid FROM audit_event ORDER BY ts_ms DESC, id DESC
                     LIMIT -1 OFFSET ?1
                 );",
                [retain_rows as i64],
            )?;
            Ok(deleted)
        })
    }

    // ── approvals ────────────────────────────────────────────────────────

    /// Records a new pending approval request.
    pub fn insert_approval(&self, record: &ApprovalRecord) -> R<()> {
        let r = record.clone();
        self.replicated(Lane::Control, "insert_approval", move |s| {
            let conn = s.write_connection()?;
            conn.execute(
                &format!(
                    "INSERT INTO approval_request ({APPROVAL_COLUMNS}) VALUES (?1, ?2, ?3, ?4, \
                     ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20);"
                ),
                params![
                    r.id,
                    r.created_ms,
                    r.expires_ms,
                    r.ttl_secs,
                    r.rule,
                    r.operation,
                    r.permission,
                    r.target,
                    r.namespace,
                    r.request_digest,
                    r.requester_principal,
                    r.requester_accessor,
                    r.requester_root,
                    r.requester_source,
                    r.on_behalf_of,
                    r.status,
                    r.decided_by,
                    r.decided_ms,
                    r.reason,
                    r.consumed_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_approval(&self, id: &str) -> R<Option<ApprovalRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!("SELECT {APPROVAL_COLUMNS} FROM approval_request WHERE id = ?1;"),
            [id],
            approval_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// A still-pending, unexpired request for the same caller, operation and
    /// digest: a retry without an approval id reuses it instead of filing a
    /// second one.
    pub fn find_pending_approval(
        &self,
        requester_principal: &str,
        requester_accessor: &str,
        operation: &str,
        request_digest: &str,
        now_ms: i64,
    ) -> R<Option<ApprovalRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            &format!(
                "SELECT {APPROVAL_COLUMNS} FROM approval_request
                 WHERE status = 'pending' AND expires_ms > ?1 AND operation = ?2
                   AND request_digest = ?3 AND requester_principal = ?4
                   AND requester_accessor = ?5
                 ORDER BY created_ms DESC LIMIT 1;"
            ),
            params![
                now_ms,
                operation,
                request_digest,
                requester_principal,
                requester_accessor
            ],
            approval_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Approval requests, newest first. `status` filters on the stored
    /// status (`expired` selects pending/approved rows past their expiry);
    /// `requester` (a principal or accessor) restricts to one caller's.
    pub fn list_approvals(
        &self,
        status: &str,
        requester: Option<(&str, &str)>,
        now_ms: i64,
        limit: usize,
    ) -> R<Vec<ApprovalRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {APPROVAL_COLUMNS} FROM approval_request ORDER BY created_ms DESC, id DESC;"
        ))?;
        let rows = Self::collect_rows(stmt.query_map([], approval_from_row)?)?;
        Ok(rows
            .into_iter()
            .filter(|r| status.is_empty() || r.effective_status(now_ms) == status)
            .filter(|r| match requester {
                Some((principal, accessor)) => {
                    r.requester_principal == principal
                        && (accessor.is_empty() || r.requester_accessor == accessor)
                }
                None => true,
            })
            .take(limit.clamp(1, 1000))
            .collect())
    }

    /// Records an approver's decision on a pending, unexpired request.
    /// Returns the updated record, or a typed failed-precondition when the
    /// request is not pending any more.
    pub fn decide_approval(
        &self,
        id: &str,
        approve: bool,
        decided_by: &str,
        reason: &str,
        now_ms: i64,
    ) -> R<ApprovalRecord> {
        let (id, decided_by, reason) = (id.to_string(), decided_by.to_string(), reason.to_string());
        self.replicated(Lane::Control, "decide_approval", move |s| {
            let conn = s.write_connection()?;
            let current = conn
                .query_row(
                    &format!("SELECT {APPROVAL_COLUMNS} FROM approval_request WHERE id = ?1;"),
                    [&id],
                    approval_from_row,
                )
                .optional()?
                .ok_or_else(|| anyhow::Error::new(StorageError::not_found("approval", &id)))?;
            let status = current.effective_status(now_ms).to_string();
            if status != "pending" {
                bail!(StorageError::failed_precondition(format!(
                    "approval {id} is {status}, not pending; only a pending request can be \
                     decided"
                )));
            }
            let (new_status, expires_ms) = if approve {
                // An approval is usable for one more TTL from the decision.
                ("approved", now_ms + current.ttl_secs.max(1) * 1000)
            } else {
                ("denied", current.expires_ms)
            };
            conn.execute(
                "UPDATE approval_request SET status = ?2, decided_by = ?3, decided_ms = ?4,
                     reason = ?5, expires_ms = ?6 WHERE id = ?1;",
                params![id, new_status, decided_by, now_ms, reason, expires_ms],
            )?;
            Ok(ApprovalRecord {
                status: new_status.to_string(),
                decided_by,
                decided_ms: now_ms,
                reason,
                expires_ms,
                ..current
            })
        })
    }

    /// Consumes an approved request for one retry: a compare-and-set that
    /// succeeds once, only for the same operation, digest and requester.
    pub fn consume_approval(
        &self,
        id: &str,
        operation: &str,
        request_digest: &str,
        requester_principal: &str,
        requester_accessor: &str,
        now_ms: i64,
    ) -> R<ApprovalUse> {
        let (id, operation, digest, principal, accessor) = (
            id.to_string(),
            operation.to_string(),
            request_digest.to_string(),
            requester_principal.to_string(),
            requester_accessor.to_string(),
        );
        self.replicated(Lane::Control, "consume_approval", move |s| {
            let conn = s.write_connection()?;
            let Some(current) = conn
                .query_row(
                    &format!("SELECT {APPROVAL_COLUMNS} FROM approval_request WHERE id = ?1;"),
                    [&id],
                    approval_from_row,
                )
                .optional()?
            else {
                return Ok(ApprovalUse::Refused(format!(
                    "approval {id} does not exist"
                )));
            };
            let refuse = |why: String| Ok(ApprovalUse::Refused(why));
            match current.effective_status(now_ms) {
                "approved" => {}
                "pending" => return refuse(format!("approval {id} has not been approved yet")),
                "consumed" => return refuse(format!("approval {id} was already used")),
                other => return refuse(format!("approval {id} is {other}")),
            }
            if current.operation != operation {
                return refuse(format!(
                    "approval {id} was granted for {}, not {operation}",
                    current.operation
                ));
            }
            if current.request_digest != digest {
                return refuse(format!(
                    "approval {id} was granted for a different request (digest {} != {}); \
                     retry with exactly the arguments that were approved",
                    short(&current.request_digest),
                    short(&digest)
                ));
            }
            if current.requester_principal != principal || current.requester_accessor != accessor {
                return refuse(format!(
                    "approval {id} was requested by another caller; only its requester may use it"
                ));
            }
            let updated = conn.execute(
                "UPDATE approval_request SET status = 'consumed', consumed_ms = ?2
                 WHERE id = ?1 AND status = 'approved';",
                params![id, now_ms],
            )?;
            Ok(if updated == 1 {
                ApprovalUse::Consumed
            } else {
                ApprovalUse::Refused(format!("approval {id} was already used"))
            })
        })
    }

    /// Deletes approval requests created before `before_ms` (control lane).
    pub fn prune_approvals(&self, before_ms: i64) -> R<usize> {
        self.replicated(Lane::Control, "prune_approvals", move |s| {
            let conn = s.write_connection()?;
            Ok(conn.execute(
                "DELETE FROM approval_request WHERE expires_ms < ?1;",
                [before_ms],
            )?)
        })
    }

    // ── token lineage and limits ─────────────────────────────────────────

    /// Records a token's lineage and limits (written beside the token row).
    pub fn upsert_token_meta(&self, meta: &TokenMeta) -> R<()> {
        let m = meta.clone();
        self.replicated(Lane::Control, "upsert_token_meta", move |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO acl_token_meta (accessor_id, parent_accessor_id, root_accessor_id,
                     depth, write_rate, write_burst, created_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(accessor_id) DO UPDATE SET parent_accessor_id = excluded.parent_accessor_id,
                     root_accessor_id = excluded.root_accessor_id, depth = excluded.depth,
                     write_rate = excluded.write_rate, write_burst = excluded.write_burst,
                     created_by = excluded.created_by;",
                params![
                    m.accessor_id,
                    m.parent_accessor_id,
                    m.root_accessor_id,
                    m.depth,
                    m.write_rate,
                    m.write_burst,
                    m.created_by
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_token_meta(&self, accessor_id: &str) -> R<Option<TokenMeta>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT accessor_id, parent_accessor_id, root_accessor_id, depth, write_rate,
                    write_burst, created_by
             FROM acl_token_meta WHERE accessor_id = ?1;",
            [accessor_id],
            |row| {
                Ok(TokenMeta {
                    accessor_id: row.get(0)?,
                    parent_accessor_id: row.get(1)?,
                    root_accessor_id: row.get(2)?,
                    depth: row.get(3)?,
                    write_rate: row.get(4)?,
                    write_burst: row.get(5)?,
                    created_by: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    /// Every token's parent accessor, for listings (`accessor -> parent`).
    pub fn token_parents(&self) -> R<std::collections::HashMap<String, String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT accessor_id, parent_accessor_id FROM acl_token_meta
             WHERE parent_accessor_id != '';",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(Self::collect_rows(rows)?.into_iter().collect())
    }

    /// Deletes every descendant of `accessor_id` (children, their children,
    /// ...) and their lineage rows, inside the caller's write. Called from
    /// the token delete so revoking a parent revokes its children.
    pub(crate) fn delete_token_descendants_on(
        conn: &rusqlite::Connection,
        accessor_id: &str,
    ) -> R<usize> {
        let descendants: Vec<String> = {
            let mut stmt = conn.prepare(
                "WITH RECURSIVE tree(id) AS (
                     SELECT accessor_id FROM acl_token_meta WHERE parent_accessor_id = ?1
                     UNION
                     SELECT m.accessor_id FROM acl_token_meta m JOIN tree t
                         ON m.parent_accessor_id = t.id
                 )
                 SELECT id FROM tree;",
            )?;
            let rows = stmt.query_map([accessor_id], |row| row.get(0))?;
            Self::collect_rows(rows)?
        };
        let mut deleted = 0;
        for id in descendants
            .iter()
            .chain(std::iter::once(&accessor_id.to_string()))
        {
            if id != accessor_id {
                deleted += conn.execute("DELETE FROM acl_token WHERE accessor_id = ?1;", [id])?;
            }
            conn.execute("DELETE FROM acl_token_meta WHERE accessor_id = ?1;", [id])?;
        }
        Ok(deleted)
    }

    // ── usage rollups ────────────────────────────────────────────────────

    /// Allocations holding node capacity (the set namespace quotas charge),
    /// with their reserved resources.
    pub fn held_allocations(&self) -> R<Vec<HeldAllocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT tc.job_id, a.resources FROM allocation a
             INNER JOIN task_config tc ON tc.id = a.task_config_id
             WHERE a.status NOT IN ('{}', '{}') AND json_valid(a.resources);",
            crate::constants::ALLOCATION_STATUS_COMPLETE,
            crate::constants::ALLOCATION_STATUS_FAILED,
        ))?;
        let rows = stmt.query_map([], |row| {
            let raw: String = row.get(1)?;
            Ok(HeldAllocation {
                job_id: row.get(0)?,
                resources: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
            })
        })?;
        Self::collect_rows(rows)
    }

    /// The accessor (or principal) of each job's latest successful submit in
    /// the audit journal, for usage attribution. Best effort: a job whose
    /// submit was pruned from the journal maps to nothing.
    pub fn job_submitters(&self) -> R<std::collections::HashMap<String, String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT target, accessor_id, principal FROM audit_event
             WHERE outcome = 'ok' AND operation IN ('RunJob', 'RunJobTemplate')
               AND target LIKE 'job:%'
             ORDER BY ts_ms ASC;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut out = std::collections::HashMap::new();
        for (target, accessor, principal) in Self::collect_rows(rows)? {
            let job = target.trim_start_matches("job:").to_string();
            let who = if accessor.is_empty() {
                principal
            } else {
                accessor
            };
            out.insert(job, who);
        }
        Ok(out)
    }

    /// Adds `rows` (unit-seconds) into their hourly buckets, and drops
    /// buckets older than `retain_before_ms`, as one status-lane entry.
    pub fn accumulate_usage(&self, rows: Vec<UsageRow>, retain_before_ms: i64) -> R<()> {
        self.replicated(Lane::Status, "accumulate_usage", move |s| {
            let conn = s.write_connection()?;
            let mut stmt = conn.prepare_cached(
                "INSERT INTO usage_rollup (bucket_ms, namespace, job_id, submitter, dimension, amount)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(bucket_ms, namespace, job_id, submitter, dimension)
                 DO UPDATE SET amount = amount + excluded.amount;",
            )?;
            for r in &rows {
                stmt.execute(params![
                    r.bucket_ms,
                    r.namespace,
                    r.job_id,
                    r.submitter,
                    r.dimension,
                    r.amount
                ])?;
            }
            conn.execute(
                "DELETE FROM usage_rollup WHERE bucket_ms < ?1;",
                [retain_before_ms],
            )?;
            Ok(())
        })
    }

    /// Usage rows with `since_ms <= bucket_ms < until_ms`.
    pub fn list_usage(&self, since_ms: i64, until_ms: i64) -> R<Vec<UsageRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT bucket_ms, namespace, job_id, submitter, dimension, amount FROM usage_rollup
             WHERE bucket_ms >= ?1 AND bucket_ms < ?2
             ORDER BY bucket_ms ASC, namespace ASC, job_id ASC;",
        )?;
        let rows = stmt.query_map(params![since_ms, until_ms], |row| {
            Ok(UsageRow {
                bucket_ms: row.get(0)?,
                namespace: row.get(1)?,
                job_id: row.get(2)?,
                submitter: row.get(3)?,
                dimension: row.get(4)?,
                amount: row.get(5)?,
            })
        })?;
        Self::collect_rows(rows)
    }
}

fn short(digest: &str) -> &str {
    &digest[..digest.len().min(12)]
}

#[cfg(test)]
mod governance_store_test {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use tempfile::Builder;

    fn store() -> R<Store> {
        let dir = Builder::new()
            .prefix("legion-governance-store-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("path")?.to_string(),
            ..Default::default()
        })
    }

    fn record(id: &str, ts_ms: i64, operation: &str, target: &str) -> AuditRecord {
        AuditRecord {
            id: id.into(),
            ts_ms,
            operation: operation.into(),
            kind: "write".into(),
            principal: "alice".into(),
            accessor_id: "acc-1".into(),
            target: target.into(),
            outcome: "ok".into(),
            ..Default::default()
        }
    }

    #[test]
    fn audit_journal_filters_pages_and_prunes_test() -> R<()> {
        let store = store()?;
        store.append_audit_events(vec![
            record("a", 1_000, "RunJob", "job:default/web"),
            record("b", 2_000, "DestroyJob", "job:prod/web"),
            record("c", 3_000, "PutKv", "kv:team/db"),
            record("d", 3_000, "RunJob", "job:prod/api"),
        ])?;
        let all = store.list_audit_events(&AuditFilter {
            limit: 10,
            ..Default::default()
        })?;
        assert_eq!(
            all.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["d", "c", "b", "a"]
        );
        let prod = store.list_audit_events(&AuditFilter {
            target_prefix: "job:prod/".into(),
            limit: 10,
            ..Default::default()
        })?;
        assert_eq!(prod.len(), 2);
        // Paging newest-first with a cursor.
        let page = store.list_audit_events(&AuditFilter {
            before: parse_audit_cursor(&audit_cursor(&all[1])),
            limit: 10,
            ..Default::default()
        })?;
        assert_eq!(
            page.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["b", "a"]
        );
        // Following oldest-first.
        let follow = store.list_audit_events(&AuditFilter {
            after: Some((2_000, "b".into())),
            limit: 10,
            ..Default::default()
        })?;
        assert_eq!(
            follow.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["c", "d"]
        );
        // `%` in a prefix is literal.
        let none = store.list_audit_events(&AuditFilter {
            target_prefix: "job:%".into(),
            limit: 10,
            ..Default::default()
        })?;
        assert!(none.is_empty());
        // Age prune, then count prune.
        assert_eq!(store.prune_audit_events_local(10, 1_500)?, 1);
        assert_eq!(store.prune_audit_events_local(2, 0)?, 1);
        let left = store.list_audit_events(&AuditFilter {
            limit: 10,
            ..Default::default()
        })?;
        assert_eq!(left.len(), 2);
        Ok(())
    }

    fn approval(id: &str, now: i64) -> ApprovalRecord {
        ApprovalRecord {
            id: id.into(),
            created_ms: now,
            expires_ms: now + 60_000,
            ttl_secs: 60,
            rule: "r".into(),
            operation: "DestroyJob".into(),
            request_digest: "d1".into(),
            requester_principal: "agent".into(),
            requester_accessor: "acc-agent".into(),
            status: "pending".into(),
            ..Default::default()
        }
    }

    #[test]
    fn approvals_decide_once_and_consume_once_test() -> R<()> {
        let store = store()?;
        let now = 1_000_000;
        store.insert_approval(&approval("apr_1", now))?;
        let refused =
            store.consume_approval("apr_1", "DestroyJob", "d1", "agent", "acc-agent", now)?;
        assert!(matches!(refused, ApprovalUse::Refused(why) if why.contains("not been approved")));
        let decided = store.decide_approval("apr_1", true, "boss", "ok", now + 10)?;
        assert_eq!(decided.status, "approved");
        assert!(store
            .decide_approval("apr_1", false, "boss", "", now + 20)
            .is_err());
        let wrong_digest =
            store.consume_approval("apr_1", "DestroyJob", "d2", "agent", "acc-agent", now + 30)?;
        assert!(
            matches!(wrong_digest, ApprovalUse::Refused(why) if why.contains("different request"))
        );
        let other =
            store.consume_approval("apr_1", "DestroyJob", "d1", "eve", "acc-eve", now + 30)?;
        assert!(matches!(other, ApprovalUse::Refused(_)));
        assert_eq!(
            store.consume_approval("apr_1", "DestroyJob", "d1", "agent", "acc-agent", now + 40)?,
            ApprovalUse::Consumed
        );
        let again =
            store.consume_approval("apr_1", "DestroyJob", "d1", "agent", "acc-agent", now + 50)?;
        assert!(matches!(again, ApprovalUse::Refused(why) if why.contains("already used")));
        // Expiry is computed.
        store.insert_approval(&approval("apr_2", now))?;
        let listed = store.list_approvals("expired", None, now + 120_000, 10)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, "apr_2");
        Ok(())
    }

    #[test]
    fn usage_accumulates_into_buckets_test() -> R<()> {
        let store = store()?;
        let row = |amount| UsageRow {
            bucket_ms: 3_600_000,
            namespace: "default".into(),
            job_id: "web".into(),
            submitter: String::new(),
            dimension: "cpu_mhz".into(),
            amount,
        };
        store.accumulate_usage(vec![row(10.0)], 0)?;
        store.accumulate_usage(vec![row(5.0)], 0)?;
        let rows = store.list_usage(0, i64::MAX)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].amount, 15.0);
        store.accumulate_usage(Vec::new(), 7_200_000)?;
        assert!(store.list_usage(0, i64::MAX)?.is_empty());
        Ok(())
    }
}
