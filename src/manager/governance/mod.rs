//! Agent governance on the manager's request path
//! (internal_docs/plans/agent-governance-plan.md): the audit journal, the
//! approval gate, and per-token write rate limits.
//!
//! [`layer::GovernanceLayer`] wraps every Manager RPC. It computes the
//! request digest, installs a per-request [`RequestAudit`] in a tokio
//! task-local, and journals the request when it completes. The handler's
//! existing ACL calls feed the context through three hooks:
//!
//! - [`note_identity`] from `authenticate_acl` (who is calling, for whom);
//! - [`on_enforce`] from `acl::enforce` (what was checked, and the decision)
//!   — once a `write` is allowed it also runs the gates, which can still
//!   refuse the request with a typed status;
//! - [`note_idempotency_key`] from `scoped_idempotency_key`.
//!
//! Coverage is therefore structural: a handler that checks a write
//! permission is journaled and gated without any per-handler code
//! (`tests/integration/agent_governance.rs` walks the ACL endpoint table to
//! prove it). Calls that bypass gRPC — the manager's own reconcilers, tests
//! calling the service directly — run without a context and are neither
//! journaled nor gated.

// tonic::Status is ~176 bytes; returning it directly matches the gRPC surface.
#![allow(clippy::result_large_err)]

pub mod digest;
pub mod journal;
pub mod layer;
pub mod rate_limit;
pub mod usage;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};
use time::OffsetDateTime;
use tonic::{Code, Status};

use crate::storage::governance_store::{ApprovalRecord, ApprovalUse, AuditRecord};
use crate::storage::store::Store;
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope, AclSource};
use crate::structs::governance::{GateCaller, GovernanceConfig};

/// Request metadata carrying an approval id for a gated retry.
pub const APPROVAL_HEADER: &str = "legion-approval-id";
/// Status metadata carrying a typed error's JSON details
/// (`{"approvalId", ...}`, `{"retryAfterMs", ...}`).
pub const ERROR_DETAILS_METADATA: &str = "legion-error-details";
/// Typed code: a gate requires an approval.
pub const APPROVAL_REQUIRED: &str = "APPROVAL_REQUIRED";
/// Typed code: the presented approval cannot be used.
pub const APPROVAL_INVALID: &str = "APPROVAL_INVALID";
/// Typed code: the caller's write rate limit is exhausted.
pub const RATE_LIMITED: &str = "RATE_LIMITED";

/// RPCs journaled even when they check no write permission: credential
/// minting and approval decisions.
pub const ALWAYS_AUDITED: &[&str] = &["Login", "CreateChildToken", "DecideApproval"];

/// RPCs the approval gate never applies to (deciding an approval must not
/// itself need one).
const GATE_EXEMPT: &[&str] = &["DecideApproval"];

/// The manager's governance state, shared by the layer and the gates.
pub struct Governance {
    pub store: Store,
    pub config: GovernanceConfig,
    pub journal: Option<journal::JournalHandle>,
    pub limiter: rate_limit::RateLimiter,
    /// This manager's raft node id (empty outside raft mode).
    pub manager_id: String,
}

impl Governance {
    /// Governance for `store` with `config`. The journal writer is started
    /// separately ([`journal::start`]) so tests can run without one.
    pub fn new(
        store: Store,
        config: GovernanceConfig,
        journal: Option<journal::JournalHandle>,
    ) -> Self {
        let manager_id = store.raft_node_id().unwrap_or_default();
        Governance {
            store,
            config,
            journal,
            limiter: rate_limit::RateLimiter::default(),
            manager_id,
        }
    }
}

/// The caller, as recorded in the journal.
#[derive(Debug, Clone, Default)]
pub struct Caller {
    pub principal: String,
    pub accessor_id: Option<String>,
    pub source: String,
    pub on_behalf_of: String,
    pub policies: Vec<String>,
    pub is_management: bool,
    pub disabled: bool,
}

impl Caller {
    fn of(identity: &AclIdentity) -> Self {
        let on_behalf_of = match identity.source {
            AclSource::Delegated | AclSource::ForObject => identity.principal.clone(),
            _ => String::new(),
        };
        Caller {
            principal: identity.principal.clone(),
            accessor_id: identity.accessor_id.clone(),
            source: identity.source.as_str().to_string(),
            on_behalf_of,
            policies: identity.policies.clone(),
            is_management: identity.is_management,
            disabled: identity.source == AclSource::Disabled,
        }
    }

    fn gate(&self) -> GateCaller<'_> {
        GateCaller {
            principal: &self.principal,
            accessor_id: self.accessor_id.as_deref(),
            policies: &self.policies,
            is_management: self.is_management,
        }
    }
}

/// The check that decided a request.
#[derive(Debug, Clone)]
pub struct Check {
    pub resource: AclResource,
    pub action: AclAction,
    pub scope: AclScope,
    pub allowed: bool,
}

/// What a request's handler told the context.
#[derive(Debug, Default)]
pub struct AuditState {
    pub caller: Option<Caller>,
    /// True once any `write` check ran.
    pub write: bool,
    /// The decisive check: the latest write check, else the latest check.
    pub check: Option<Check>,
    pub denied: bool,
    pub idempotency_key: String,
    pub approval_used: String,
    pub rate_checked: bool,
    pub detail: serde_json::Map<String, Value>,
}

/// One in-flight Manager RPC, installed by the governance layer.
pub struct RequestAudit {
    pub operation: String,
    pub digest: String,
    pub approval_id: Option<String>,
    pub governance: Arc<Governance>,
    pub started: Instant,
    pub state: Mutex<AuditState>,
}

tokio::task_local! {
    static REQUEST: Arc<RequestAudit>;
}

impl RequestAudit {
    pub fn new(
        governance: Arc<Governance>,
        operation: &str,
        digest: String,
        approval_id: Option<String>,
    ) -> Self {
        RequestAudit {
            operation: operation.to_string(),
            digest,
            approval_id,
            governance,
            started: Instant::now(),
            state: Mutex::new(AuditState::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, AuditState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Runs `fut` with this request as the current context.
    pub async fn scope<F: std::future::Future>(self: Arc<Self>, fut: F) -> F::Output {
        REQUEST.scope(self, fut).await
    }

    /// The journal record for this request with `outcome`, or `None` when it
    /// is not journaled (a read that passed its checks).
    pub fn record(&self, outcome: &str, message: &str) -> Option<AuditRecord> {
        let state = self.state();
        let auth_failure = matches!(outcome, "PermissionDenied" | "Unauthenticated");
        let journaled =
            state.write || auth_failure || ALWAYS_AUDITED.contains(&self.operation.as_str());
        if !journaled {
            return None;
        }
        let kind = if auth_failure || state.denied {
            "denied"
        } else if state.write {
            "write"
        } else {
            "audited"
        };
        let caller = state.caller.clone().unwrap_or_default();
        let (resource, action, target, namespace) = match &state.check {
            Some(check) => (
                check.resource.as_str().to_string(),
                check.action.as_str().to_string(),
                target_of(check.resource, &check.scope),
                check
                    .scope
                    .namespace
                    .clone()
                    .filter(|ns| ns != "*")
                    .unwrap_or_default(),
            ),
            None => Default::default(),
        };
        let now = OffsetDateTime::now_utc();
        Some(AuditRecord {
            id: random_id(),
            ts_ms: (now.unix_timestamp_nanos() / 1_000_000) as i64,
            manager: self.governance.manager_id.clone(),
            operation: self.operation.clone(),
            kind: kind.to_string(),
            accessor_id: caller.accessor_id.clone().unwrap_or_default(),
            principal: caller.principal.clone(),
            source: caller.source.clone(),
            on_behalf_of: caller.on_behalf_of.clone(),
            parent_accessor_id: String::new(),
            resource,
            action,
            target,
            namespace,
            request_digest: self.digest.clone(),
            idempotency_key: state.idempotency_key.clone(),
            approval_id: state.approval_used.clone(),
            outcome: outcome.to_string(),
            message: truncate(message, 512),
            detail: if state.detail.is_empty() {
                String::new()
            } else {
                Value::Object(state.detail.clone()).to_string()
            },
            duration_ms: self.started.elapsed().as_millis() as i64,
        })
    }
}

/// The request being handled on this task, when the governance layer is in
/// front of it.
pub fn current() -> Option<Arc<RequestAudit>> {
    REQUEST.try_with(Arc::clone).ok()
}

/// Wraps a closure bound for `spawn_blocking` so the ACL checks it makes
/// still report to (and are gated by) the request's governance context.
pub fn carry<F, T>(f: F) -> impl FnOnce() -> T + Send + 'static
where
    F: FnOnce() -> T + Send + 'static,
{
    let request = current();
    move || match request {
        Some(request) => REQUEST.sync_scope(request, f),
        None => f(),
    }
}

/// Hook: the handler authenticated `identity` (`authenticate_acl`).
pub fn note_identity(identity: &AclIdentity) {
    if let Some(request) = current() {
        request.state().caller = Some(Caller::of(identity));
    }
}

/// Hook: the handler honoured idempotency key `key`.
pub fn note_idempotency_key(key: &str) {
    if key.trim().is_empty() {
        return;
    }
    if let Some(request) = current() {
        request.state().idempotency_key = truncate(key.trim(), 256);
    }
}

/// Hook: operation-specific detail for the journal record (never secrets).
pub fn note_detail(key: &str, value: Value) {
    if let Some(request) = current() {
        request.state().detail.insert(key.to_string(), value);
    }
}

/// Hook from `acl::enforce`: records the check, and once a write is
/// allowed runs the rate limit and the approval gate, which may refuse the
/// request with a typed status.
pub fn on_enforce(
    identity: &AclIdentity,
    resource: AclResource,
    action: AclAction,
    scope: &AclScope,
    allowed: bool,
) -> Result<(), Status> {
    let Some(request) = current() else {
        return Ok(());
    };
    let caller = Caller::of(identity);
    {
        let mut state = request.state();
        let is_write = action == AclAction::Write;
        if is_write || !state.write {
            state.check = Some(Check {
                resource,
                action,
                scope: scope.clone(),
                allowed,
            });
        }
        state.write |= is_write;
        state.denied |= !allowed;
        state.caller = Some(caller.clone());
    }
    if !allowed || action != AclAction::Write || caller.disabled {
        return Ok(());
    }
    rate_gate(&request, &caller)?;
    approval_gate(&request, &caller, resource, action, scope)
}

fn rate_gate(request: &RequestAudit, caller: &Caller) -> Result<(), Status> {
    {
        let mut state = request.state();
        if state.rate_checked {
            return Ok(());
        }
        state.rate_checked = true;
    }
    // An installed app acting for a viewer or for one of its objects is the
    // operator's own automation (gitops applying a Repo, the cert manager
    // renewing): it is journaled and gated, but not throttled per token.
    if !caller.on_behalf_of.is_empty() {
        return Ok(());
    }
    let governance = &request.governance;
    let token_limit = caller
        .accessor_id
        .as_deref()
        .and_then(|accessor| governance.limiter.token_limit(&governance.store, accessor));
    let Some((rate, burst)) = governance
        .config
        .rate_limit_for(&caller.gate(), token_limit)
    else {
        return Ok(());
    };
    let key = match &caller.accessor_id {
        Some(accessor) => format!("accessor:{accessor}"),
        None => format!("principal:{}", caller.principal),
    };
    match governance.limiter.take(&key, rate, burst, Instant::now()) {
        Ok(()) => Ok(()),
        Err(retry_after) => {
            governance_event("rate_limited");
            let retry_after_ms = retry_after.as_millis().max(1) as u64;
            Err(typed_status(
                Code::ResourceExhausted,
                RATE_LIMITED,
                format!(
                    "rate limited: {} exceeded its write limit of {rate}/s (burst {burst})\n\
                     Hint: retry after {retry_after_ms} ms; the limit is per token \
                     (governance.rateLimit; a child token's own --write-rate)",
                    caller.principal
                ),
                json!({
                    "retryAfterMs": retry_after_ms,
                    "limitPerSecond": rate,
                    "burst": burst,
                }),
            ))
        }
    }
}

fn approval_gate(
    request: &RequestAudit,
    caller: &Caller,
    resource: AclResource,
    action: AclAction,
    scope: &AclScope,
) -> Result<(), Status> {
    if GATE_EXEMPT.contains(&request.operation.as_str())
        || !request.state().approval_used.is_empty()
    {
        return Ok(());
    }
    let governance = &request.governance;
    let Some(rule) = governance
        .config
        .approval_rules()
        .iter()
        .find(|rule| rule.matches(&request.operation, resource, action, scope, &caller.gate()))
    else {
        return Ok(());
    };
    let store = &governance.store;
    let now_ms = now_ms();
    let accessor = caller.accessor_id.clone().unwrap_or_default();
    if let Some(id) = &request.approval_id {
        return match store.consume_approval(
            id,
            &request.operation,
            &request.digest,
            &caller.principal,
            &accessor,
            now_ms,
        ) {
            Ok(ApprovalUse::Consumed) => {
                governance_event("approval_used");
                request.state().approval_used = id.clone();
                Ok(())
            }
            Ok(ApprovalUse::Refused(why)) => {
                governance_event("approval_invalid");
                Err(typed_status(
                    Code::FailedPrecondition,
                    APPROVAL_INVALID,
                    format!(
                        "approval required: {} on {} matches approval rule `{}`, and the \
                         presented approval cannot be used: {why}\nHint: `legion approval get \
                         {id}` shows it; retry without --approval to file a new request",
                        request.operation,
                        target_of(resource, scope),
                        rule.name
                    ),
                    json!({ "approvalId": id, "rule": rule.name, "requestDigest": request.digest }),
                ))
            }
            Err(err) => Err(crate::manager::server::map_store_error(
                "Error consuming approval",
                err,
            )),
        };
    }
    let existing = store
        .find_pending_approval(
            &caller.principal,
            &accessor,
            &request.operation,
            &request.digest,
            now_ms,
        )
        .map_err(|err| crate::manager::server::map_store_error("Error reading approvals", err))?;
    let record = match existing {
        Some(record) => record,
        None => {
            let ttl_secs = governance.config.approval_ttl().as_secs().max(1) as i64;
            let record = ApprovalRecord {
                id: format!("apr_{}", random_id()),
                created_ms: now_ms,
                expires_ms: now_ms + ttl_secs * 1000,
                ttl_secs,
                rule: rule.name.clone(),
                operation: request.operation.clone(),
                permission: format!("{}:{}", resource.as_str(), action.as_str()),
                target: target_of(resource, scope),
                namespace: scope
                    .namespace
                    .clone()
                    .filter(|ns| ns != "*")
                    .unwrap_or_default(),
                request_digest: request.digest.clone(),
                requester_principal: caller.principal.clone(),
                requester_accessor: accessor.clone(),
                requester_root: token_root(store, &accessor),
                requester_source: caller.source.clone(),
                on_behalf_of: caller.on_behalf_of.clone(),
                status: "pending".into(),
                ..Default::default()
            };
            store.insert_approval(&record).map_err(|err| {
                crate::manager::server::map_store_error("Error filing approval request", err)
            })?;
            record
        }
    };
    governance_event("approval_required");
    note_detail("approvalRequested", Value::String(record.id.clone()));
    Err(typed_status(
        Code::FailedPrecondition,
        APPROVAL_REQUIRED,
        format!(
            "approval required: {} on {} matches approval rule `{}` (request {})\nHint: ask an \
             approver to run `legion approval approve {}`, then retry the same command with \
             --approval {}",
            request.operation, record.target, rule.name, record.id, record.id, record.id
        ),
        json!({
            "approvalId": record.id,
            "requestDigest": record.request_digest,
            "rule": rule.name,
            "expiresAt": rfc3339_ms(record.expires_ms),
        }),
    ))
}

/// The root of a token's lineage (itself when it has no parent).
pub fn token_root(store: &Store, accessor: &str) -> String {
    if accessor.is_empty() {
        return String::new();
    }
    match store.get_token_meta(accessor) {
        Ok(Some(meta)) if !meta.root_accessor_id.is_empty() => meta.root_accessor_id,
        _ => accessor.to_string(),
    }
}

/// A status carrying a typed Legion code and JSON details in metadata, which
/// the CLI's `--json` envelope and the MCP server surface as
/// `error.code`/`error.details`.
pub fn typed_status(code: Code, legion_code: &str, message: String, details: Value) -> Status {
    let mut status = Status::new(code, message);
    if let Ok(value) = legion_code.parse() {
        status
            .metadata_mut()
            .insert(crate::manager::worker_plane::ERROR_CODE_METADATA, value);
    }
    if let Ok(value) = details.to_string().parse() {
        status.metadata_mut().insert(ERROR_DETAILS_METADATA, value);
    }
    status
}

/// The audit `target` for a check: `job:prod/web`, `kv:team/db`,
/// `node:pool/gpu`, `acl`, ...
pub fn target_of(resource: AclResource, scope: &AclScope) -> String {
    let resource = resource.as_str();
    if let Some(path) = &scope.path {
        return format!("{resource}:{path}");
    }
    if let Some(kind) = &scope.kind {
        let ns = scope.namespace.as_deref().unwrap_or("*");
        let name = scope.name.as_deref().unwrap_or("*");
        return format!("{resource}:{kind}/{ns}/{name}");
    }
    match (&scope.namespace, &scope.name) {
        (Some(ns), Some(name)) => return format!("{resource}:{ns}/{name}"),
        (Some(ns), None) => return format!("{resource}:{ns}/*"),
        (None, Some(name)) => return format!("{resource}:{name}"),
        (None, None) => {}
    }
    if let Some(pool) = &scope.pool {
        return format!("{resource}:pool/{pool}");
    }
    if let Some(template) = &scope.template {
        return format!("{resource}:template/{template}");
    }
    resource.to_string()
}

/// Counts a governance event on `legion_governance_events_total`.
pub fn governance_event(event: &str) {
    crate::manager::telemetry::metrics()
        .governance_events_total
        .inc(&[event]);
}

pub fn now_ms() -> i64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

/// RFC3339 for unix milliseconds (empty for 0).
pub fn rfc3339_ms(ms: i64) -> String {
    if ms == 0 {
        return String::new();
    }
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default()
}

/// Parses a time bound: RFC3339, or a duration back from now (`1h`, `7d`).
pub fn parse_time_bound(raw: &str, now_ms: i64) -> Result<Option<i64>, Status> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    if let Ok(t) = OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339) {
        return Ok(Some((t.unix_timestamp_nanos() / 1_000_000) as i64));
    }
    let ago: std::time::Duration =
        raw.parse::<humantime::Duration>()
            .map(Into::into)
            .map_err(|_| {
                Status::invalid_argument(format!(
                    "invalid time `{raw}`: use RFC3339 (2026-11-08T10:00:00Z) or a duration back \
                 from now (1h, 7d)"
                ))
            })?;
    Ok(Some(now_ms - ago.as_millis() as i64))
}

/// 128 random bits, hex.
pub fn random_id() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod governance_test {
    use super::*;

    #[test]
    fn targets_name_what_was_checked_test() {
        assert_eq!(
            target_of(AclResource::Job, &AclScope::job("prod/web")),
            "job:prod/web"
        );
        assert_eq!(
            target_of(
                AclResource::Kv,
                &AclScope {
                    path: Some("team/db".into()),
                    ..Default::default()
                }
            ),
            "kv:team/db"
        );
        assert_eq!(
            target_of(
                AclResource::Node,
                &AclScope {
                    pool: Some("gpu".into()),
                    ..Default::default()
                }
            ),
            "node:pool/gpu"
        );
        assert_eq!(target_of(AclResource::Acl, &AclScope::none()), "acl");
    }

    fn store() -> Store {
        let dir = tempfile::Builder::new()
            .prefix("legion-governance-")
            .tempdir_in(crate::test_scratch::root())
            .unwrap()
            .keep();
        Store::start_storage(&crate::structs::ManagerConfigFull {
            store_path: dir.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .unwrap()
    }

    fn identity(principal: &str, source: AclSource) -> AclIdentity {
        AclIdentity {
            principal: principal.into(),
            accessor_id: Some("acc-app".into()),
            is_management: false,
            is_worker: false,
            policies: vec!["p".into()],
            rules: Vec::new(),
            source,
            expires_at: None,
            audit_only: false,
            acting_for: None,
        }
    }

    #[tokio::test]
    async fn records_name_the_on_behalf_of_chain_and_target_test() {
        let governance = Arc::new(Governance::new(store(), GovernanceConfig::default(), None));
        let delegated = identity("alice via app autoscaler", AclSource::Delegated);
        let request = Arc::new(RequestAudit::new(
            governance,
            "ScaleTask",
            "d1".into(),
            None,
        ));
        request
            .clone()
            .scope(async {
                note_identity(&delegated);
                on_enforce(
                    &delegated,
                    AclResource::Scale,
                    AclAction::Write,
                    &AclScope::job("prod/web"),
                    true,
                )
                .unwrap();
                note_idempotency_key("k-1");
            })
            .await;
        let record = request.record("ok", "").expect("a write is journaled");
        assert_eq!(record.on_behalf_of, "alice via app autoscaler");
        assert_eq!(record.source, "delegated");
        assert_eq!(record.accessor_id, "acc-app");
        assert_eq!(record.target, "scale:prod/web");
        assert_eq!(record.namespace, "prod");
        assert_eq!(record.kind, "write");
        assert_eq!(record.idempotency_key, "k-1");
        assert_eq!(record.request_digest, "d1");
    }

    #[tokio::test]
    async fn reads_are_not_journaled_but_denials_are_test() {
        let governance = Arc::new(Governance::new(store(), GovernanceConfig::default(), None));
        let caller = identity("reader", AclSource::Token);
        let request = Arc::new(RequestAudit::new(
            governance.clone(),
            "GetKv",
            String::new(),
            None,
        ));
        request
            .clone()
            .scope(async {
                on_enforce(
                    &caller,
                    AclResource::Kv,
                    AclAction::Read,
                    &AclScope::none(),
                    true,
                )
                .unwrap();
            })
            .await;
        assert!(request.record("ok", "").is_none());
        let denied = request
            .record("PermissionDenied", "no")
            .expect("denials are journaled");
        assert_eq!(denied.kind, "denied");
        // Outside a governance context the hooks are inert.
        assert!(current().is_none());
        assert!(on_enforce(
            &caller,
            AclResource::Kv,
            AclAction::Write,
            &AclScope::none(),
            true
        )
        .is_ok());
    }

    #[test]
    fn time_bounds_accept_rfc3339_and_durations_test() {
        let now = 10_000_000;
        assert_eq!(parse_time_bound("", now).unwrap(), None);
        assert_eq!(parse_time_bound("1s", now).unwrap(), Some(now - 1000));
        assert_eq!(
            parse_time_bound("1970-01-01T00:00:01Z", now).unwrap(),
            Some(1000)
        );
        assert!(parse_time_bound("yesterday-ish", now).is_err());
        assert_eq!(rfc3339_ms(1000), "1970-01-01T00:00:01Z");
    }
}
