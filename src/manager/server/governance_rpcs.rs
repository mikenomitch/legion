//! Agent-governance RPCs (internal_docs/plans/agent-governance-plan.md):
//! the audit journal, child tokens, approvals, the caller-aware catalog, and
//! usage. The gates themselves run in `crate::manager::governance`.

use super::*;
use crate::manager::governance::{parse_time_bound, rfc3339_ms, token_root, usage::BUCKET_MS};
use crate::manager::rpc_access::{Access, RPC_ACCESS};
use crate::storage::governance_store::{
    audit_cursor, parse_audit_cursor, ApprovalRecord, AuditFilter, AuditRecord, UsageRow,
};
use crate::structs::{
    Approval, AuditEvent, CreateChildTokenReply, CreateChildTokenRequest, DecideApprovalReply,
    DecideApprovalRequest, GetApprovalReply, GetApprovalRequest, GetCallerCapabilitiesReply,
    GetCallerCapabilitiesRequest, GetUsageReply, GetUsageRequest, ListApprovalsReply,
    ListApprovalsRequest, ListAuditEventsReply, ListAuditEventsRequest, RpcAccess, UsageSummary,
};
use std::collections::BTreeMap;

fn audit_to_proto(record: AuditRecord) -> AuditEvent {
    AuditEvent {
        time: rfc3339_ms(record.ts_ms),
        id: record.id,
        ts_ms: record.ts_ms,
        manager: record.manager,
        operation: record.operation,
        kind: record.kind,
        accessor_id: record.accessor_id,
        principal: record.principal,
        source: record.source,
        on_behalf_of: record.on_behalf_of,
        parent_accessor_id: record.parent_accessor_id,
        resource: record.resource,
        action: record.action,
        target: record.target,
        namespace: record.namespace,
        request_digest: record.request_digest,
        idempotency_key: record.idempotency_key,
        approval_id: record.approval_id,
        outcome: record.outcome,
        message: record.message,
        detail_json: record.detail,
        duration_ms: record.duration_ms,
    }
}

pub(super) fn approval_to_proto(record: &ApprovalRecord, now_ms: i64) -> Approval {
    Approval {
        id: record.id.clone(),
        status: record.effective_status(now_ms).to_string(),
        operation: record.operation.clone(),
        permission: record.permission.clone(),
        target: record.target.clone(),
        namespace: record.namespace.clone(),
        rule: record.rule.clone(),
        request_digest: record.request_digest.clone(),
        requester_principal: record.requester_principal.clone(),
        requester_accessor: record.requester_accessor.clone(),
        on_behalf_of: record.on_behalf_of.clone(),
        created_at: rfc3339_ms(record.created_ms),
        expires_at: rfc3339_ms(record.expires_ms),
        decided_by: record.decided_by.clone(),
        decided_at: rfc3339_ms(record.decided_ms),
        reason: record.reason.clone(),
        consumed_at: rfc3339_ms(record.consumed_ms),
    }
}

/// Whether `identity` filed `record`.
fn is_requester(identity: &AclIdentity, record: &ApprovalRecord) -> bool {
    record.requester_principal == identity.principal
        && record.requester_accessor == identity.accessor_id.clone().unwrap_or_default()
}

impl ManagerDaemonService {
    pub(super) async fn list_audit_events_rpc(
        &self,
        request: Request<ListAuditEventsRequest>,
    ) -> Result<Response<ListAuditEventsReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Audit,
            AclAction::Read,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let now = crate::manager::governance::now_ms();
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000) as usize
        };
        let cursor = |raw: &str, field: &str| -> Result<Option<(i64, String)>, Status> {
            if raw.trim().is_empty() {
                return Ok(None);
            }
            parse_audit_cursor(raw.trim())
                .map(Some)
                .ok_or_else(|| Status::invalid_argument(format!("invalid {field} `{raw}`")))
        };
        let filter = AuditFilter {
            accessor_id: req.accessor_id.trim().to_string(),
            principal: req.principal.trim().to_string(),
            operation: req.operation.trim().to_string(),
            target_prefix: req.target_prefix.trim().to_string(),
            namespace: req.namespace.trim().to_string(),
            outcome: req.outcome.trim().to_string(),
            kind: req.kind.trim().to_string(),
            since_ms: parse_time_bound(&req.since, now)?,
            until_ms: parse_time_bound(&req.until, now)?,
            before: cursor(&req.page_token, "page_token")?,
            after: cursor(&req.after_cursor, "after_cursor")?,
            limit,
        };
        let following = filter.after.is_some();
        let records = self
            .store
            .list_audit_events(&filter)
            .map_err(|e| map_store_error("Error listing audit events", e))?;
        let next_page_token = match records.last() {
            Some(last) if !following && records.len() == limit => audit_cursor(last),
            _ => String::new(),
        };
        let newest = if following {
            records.last()
        } else {
            records.first()
        };
        let last_cursor = newest
            .map(audit_cursor)
            .unwrap_or_else(|| req.after_cursor.trim().to_string());
        Ok(Response::new(ListAuditEventsReply {
            events: records.into_iter().map(audit_to_proto).collect(),
            next_page_token,
            last_cursor,
        }))
    }

    pub(super) async fn create_child_token_rpc(
        &self,
        request: Request<CreateChildTokenRequest>,
    ) -> Result<Response<CreateChildTokenReply>, Status> {
        // ACL: self-service. Any caller presenting a Legion ACL token may
        // mint a child that is never wider than itself; the checks live in
        // `Authenticator::mint_child_token`. A caller with no credential is
        // unauthenticated.
        let identity = self.authenticate_acl(&request)?;
        if identity.source == crate::structs::acl::AclSource::Anonymous {
            let message = "CreateChildToken mints a child of the presented token, and no \
                           token was provided\nHint: set LEGION_TOKEN or run `legion login`";
            // Audit-only mode refuses nothing for want of a credential; there
            // is still nothing to mint a child of.
            return Err(if identity.audit_only {
                Status::failed_precondition(message)
            } else {
                Status::unauthenticated(message)
            });
        }
        let req = request.into_inner();
        let name = req.name.trim();
        if name.is_empty() {
            return Err(Status::invalid_argument("a child token needs a name"));
        }
        let write_rate = req
            .write_rate
            .as_deref()
            .map(crate::manager::governance::rate_limit::parse_write_rate)
            .transpose()
            .map_err(Status::invalid_argument)?;
        let (token, secret) = self.auth.mint_child_token(
            &self.store,
            &identity,
            name,
            &req.policies,
            req.ttl.as_deref(),
            write_rate,
        )?;
        crate::manager::governance::note_detail(
            "mintedAccessorId",
            Value::String(token.accessor_id.clone()),
        );
        info!(
            accessor_id = %token.accessor_id,
            parent = identity.accessor_id.as_deref().unwrap_or("-"),
            "child ACL token created"
        );
        Ok(Response::new(CreateChildTokenReply {
            token: Some(token),
            secret,
        }))
    }

    pub(super) async fn list_approvals_rpc(
        &self,
        request: Request<ListApprovalsRequest>,
    ) -> Result<Response<ListApprovalsReply>, Status> {
        // ACL: `approval:read` lists everyone's requests; any other
        // authenticated caller lists only its own.
        let identity = self.authenticate_acl(&request)?;
        let sees_all = identity.allows(AclResource::Approval, AclAction::Read, &AclScope::none());
        if !sees_all && identity.source == crate::structs::acl::AclSource::Anonymous {
            acl::enforce(
                &identity,
                AclResource::Approval,
                AclAction::Read,
                &AclScope::none(),
            )?;
        }
        let req = request.into_inner();
        let now = crate::manager::governance::now_ms();
        let accessor = identity.accessor_id.clone().unwrap_or_default();
        let requester =
            (req.mine || !sees_all).then_some((identity.principal.as_str(), accessor.as_str()));
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };
        let records = self
            .store
            .list_approvals(req.status.trim(), requester, now, limit)
            .map_err(|e| map_store_error("Error listing approvals", e))?;
        Ok(Response::new(ListApprovalsReply {
            approvals: records
                .iter()
                .map(|record| approval_to_proto(record, now))
                .collect(),
        }))
    }

    pub(super) async fn get_approval_rpc(
        &self,
        request: Request<GetApprovalRequest>,
    ) -> Result<Response<GetApprovalReply>, Status> {
        // ACL: `approval:read`, or being the requester. Check before fetch:
        // a caller without `approval:read` learns only about its own.
        let identity = self.authenticate_acl(&request)?;
        if identity.source == crate::structs::acl::AclSource::Anonymous {
            acl::enforce(
                &identity,
                AclResource::Approval,
                AclAction::Read,
                &AclScope::none(),
            )?;
        }
        let sees_all = identity.allows(AclResource::Approval, AclAction::Read, &AclScope::none());
        let id = request.into_inner().id;
        let now = crate::manager::governance::now_ms();
        let record = self
            .store
            .get_approval(id.trim())
            .map_err(|e| map_store_error("Error reading approval", e))?
            .filter(|record| sees_all || is_requester(&identity, record))
            .ok_or_else(|| Status::not_found(format!("approval {id} not found")))?;
        Ok(Response::new(GetApprovalReply {
            approval: Some(approval_to_proto(&record, now)),
        }))
    }

    pub(super) async fn decide_approval_rpc(
        &self,
        request: Request<DecideApprovalRequest>,
    ) -> Result<Response<DecideApprovalReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Approval,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "decide_approval",
            req.idempotency_key.as_deref(),
            &req,
        );
        let now = crate::manager::governance::now_ms();
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let record = self
                .store
                .get_approval(&message)
                .map_err(|e| map_store_error("Error reading approval", e))?
                .ok_or_else(|| Status::not_found(format!("approval {message} not found")))?;
            return Ok(Response::new(DecideApprovalReply {
                approval: Some(approval_to_proto(&record, now)),
                deduplicated: true,
            }));
        }
        let id = req.id.trim();
        let record = self
            .store
            .get_approval(id)
            .map_err(|e| map_store_error("Error reading approval", e))?
            .ok_or_else(|| Status::not_found(format!("approval {id} not found")))?;
        // Nobody approves their own request: not the requesting token, not a
        // token sharing its principal, and not a token in its lineage.
        let accessor = identity.accessor_id.clone().unwrap_or_default();
        let same_lineage = !accessor.is_empty()
            && !record.requester_root.is_empty()
            && token_root(&self.store, &accessor) == record.requester_root;
        if is_requester(&identity, &record)
            || record.requester_principal == identity.principal
            || (!accessor.is_empty() && accessor == record.requester_accessor)
            || same_lineage
        {
            return Err(Status::permission_denied(format!(
                "approval {id} was requested by {} and cannot be decided by the same caller or \
                 a token in its lineage\nHint: ask another approver (approval:write)",
                record.requester_principal
            )));
        }
        crate::manager::governance::note_detail("approvalId", Value::String(id.to_string()));
        crate::manager::governance::note_detail("approve", Value::Bool(req.approve));
        let decided = self
            .store
            .decide_approval(id, req.approve, &identity.principal, req.reason.trim(), now)
            .map_err(|e| map_store_error("Error deciding approval", e))?;
        self.store
            .idempotency_record(&idempotency_key, "decide_approval", id)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        info!(approval = %id, approve = req.approve, "approval decided");
        Ok(Response::new(DecideApprovalReply {
            approval: Some(approval_to_proto(&decided, now)),
            deduplicated: false,
        }))
    }

    pub(super) async fn get_caller_capabilities_rpc(
        &self,
        request: Request<GetCallerCapabilitiesRequest>,
    ) -> Result<Response<GetCallerCapabilitiesReply>, Status> {
        // ACL: open endpoint. It reports on the presented credential only.
        let identity = self.authenticate_acl(&request)?;
        let mut rpcs: Vec<RpcAccess> = RPC_ACCESS
            .iter()
            .map(|(rpc, access)| caller_access(&identity, rpc, *access))
            .collect();
        rpcs.sort_by(|a, b| a.rpc.cmp(&b.rpc));
        Ok(Response::new(GetCallerCapabilitiesReply {
            principal: identity.principal.clone(),
            accessor_id: identity.accessor_id.clone(),
            source: identity.source.as_str().to_string(),
            is_management: identity.is_management,
            acl_enabled: self.auth.enabled,
            rpcs,
        }))
    }

    pub(super) async fn get_usage_rpc(
        &self,
        request: Request<GetUsageRequest>,
    ) -> Result<Response<GetUsageReply>, Status> {
        // ACL: per item. Coarse gate first so a caller with no namespace
        // reach gets the ordinary ACL error; rows are then filtered by
        // `namespace:read` (and `job:read` for per-job grouping).
        let identity = self.check_acl(
            &request,
            AclResource::Namespace,
            AclAction::Read,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        let now = crate::manager::governance::now_ms();
        let since = parse_time_bound(&req.since, now)?.unwrap_or(now - 24 * BUCKET_MS);
        let until = parse_time_bound(&req.until, now)?.unwrap_or(now + 1);
        if since >= until {
            return Err(Status::invalid_argument("`since` must be before `until`"));
        }
        let group_by = match req.group_by.trim() {
            "" | "namespace" => "namespace",
            "job" => "job",
            "submitter" => "submitter",
            "hour" => "hour",
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown group_by `{other}` (namespace, job, submitter, hour)"
                )))
            }
        };
        let job_filter = req
            .job
            .trim()
            .split_once('/')
            .map(|(ns, name)| crate::structs::namespace::job_id(ns, name))
            .unwrap_or_else(|| req.job.trim().to_string());
        let rows = self
            .store
            .list_usage(crate::manager::governance::usage::bucket_of(since), until)
            .map_err(|e| map_store_error("Error reading usage", e))?;
        let mut grouped: BTreeMap<(String, String, String, i64), UsageSummary> = BTreeMap::new();
        for row in rows {
            if !usage_row_visible(
                &identity,
                &row,
                &req.namespaces,
                &job_filter,
                &req.submitter,
                group_by,
            ) {
                continue;
            }
            let key = match group_by {
                "job" => (row.namespace.clone(), row.job_id.clone(), String::new(), 0),
                "submitter" => (String::new(), String::new(), row.submitter.clone(), 0),
                "hour" => (
                    row.namespace.clone(),
                    String::new(),
                    String::new(),
                    row.bucket_ms,
                ),
                _ => (row.namespace.clone(), String::new(), String::new(), 0),
            };
            let entry = grouped.entry(key.clone()).or_insert_with(|| UsageSummary {
                namespace: key.0.clone(),
                job: crate::structs::namespace::job_name(&key.1).to_string(),
                submitter: key.2.clone(),
                bucket_start: if group_by == "hour" {
                    rfc3339_ms(key.3)
                } else {
                    String::new()
                },
                ..Default::default()
            });
            let hours = row.amount / 3600.0;
            match row.dimension.as_str() {
                "cpu_mhz" => {
                    entry.cpu_mhz_hours += hours;
                    entry.cpu_core_hours += hours / 1000.0;
                }
                "memory_mib" => entry.memory_gib_hours += hours / 1024.0,
                "cores" => entry.cores_hours += hours,
                "allocations" => entry.allocation_hours += hours,
                other => {
                    let name = other.strip_prefix("resource:").unwrap_or(other).to_string();
                    *entry.other_hours.entry(name).or_default() += hours;
                }
            }
        }
        Ok(Response::new(GetUsageReply {
            rows: grouped.into_values().collect(),
            since: rfc3339_ms(since),
            until: rfc3339_ms(until),
        }))
    }
}

/// Whether `row` is in the caller's view and matches the request filters.
fn usage_row_visible(
    identity: &AclIdentity,
    row: &UsageRow,
    namespaces: &[String],
    job: &str,
    submitter: &str,
    group_by: &str,
) -> bool {
    if !namespaces.is_empty()
        && !namespaces
            .iter()
            .any(|glob| crate::structs::acl::glob_match(glob, &row.namespace))
    {
        return false;
    }
    if !job.is_empty() && row.job_id != job {
        return false;
    }
    if !submitter.trim().is_empty() && row.submitter != submitter.trim() {
        return false;
    }
    if !identity.allows(
        AclResource::Namespace,
        AclAction::Read,
        &AclScope::namespace(&row.namespace),
    ) {
        return false;
    }
    // Naming jobs (per-job and per-submitter views) needs job:read on them.
    if matches!(group_by, "job" | "submitter") || !job.is_empty() {
        return identity.allows(
            AclResource::Job,
            AclAction::Read,
            &AclScope::job(&row.job_id),
        );
    }
    true
}

/// One RPC as `identity` can use it (see `GetCallerCapabilities`).
fn caller_access(identity: &AclIdentity, rpc: &str, access: Access) -> RpcAccess {
    let (verdict, namespaces) = match access {
        Access::Open => ("open", Vec::new()),
        Access::Worker => ("worker", Vec::new()),
        Access::App => ("app", Vec::new()),
        Access::SelfService => {
            let has_token = identity.source == crate::structs::acl::AclSource::Token
                && identity.accessor_id.is_some()
                && !identity.is_worker;
            (
                if has_token || identity.source == crate::structs::acl::AclSource::Disabled {
                    "allowed"
                } else {
                    "denied"
                },
                Vec::new(),
            )
        }
        Access::Protected(resource, action) => {
            match (AclResource::parse(resource), AclAction::parse(action)) {
                (Some(resource), Some(action)) => protected_access(identity, resource, action),
                _ => ("denied", Vec::new()),
            }
        }
    };
    RpcAccess {
        rpc: rpc.to_string(),
        permission: access.permission(),
        access: verdict.to_string(),
        namespaces,
        mutates: access.mutates(),
    }
}

fn protected_access(
    identity: &AclIdentity,
    resource: AclResource,
    action: AclAction,
) -> (&'static str, Vec<String>) {
    if identity.is_management {
        return ("allowed", Vec::new());
    }
    if !identity.would_allow(resource, action, &AclScope::any()) {
        return ("denied", Vec::new());
    }
    if resource.is_namespaced() {
        let namespaces = identity.namespaces_granting(resource, action);
        if namespaces.iter().any(|ns| ns == "*") {
            ("allowed", Vec::new())
        } else {
            ("partial", namespaces)
        }
    } else if identity.would_allow(resource, action, &AclScope::none()) {
        ("allowed", Vec::new())
    } else {
        ("partial", Vec::new())
    }
}
