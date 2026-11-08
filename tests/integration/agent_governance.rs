//! Agent governance end to end (internal_docs/plans/agent-governance-plan.md):
//! the audit journal, token lifetimes, child tokens, approval gates, rate
//! limits, the caller-aware catalog, and usage accounting.
//!
//! The gates and the journal live in the gRPC layer, so the request-path
//! tests serve a real manager with `GovernanceLayer` on a loopback port and
//! call it through the CLI's client.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::cmd::helpers::{
    connect_manager_client_with_token, AuthedManagerClient, ClientTimeouts,
};
use legion::manager::acl::Authenticator;
use legion::manager::governance::layer::GovernanceLayer;
use legion::manager::governance::{Governance, APPROVAL_HEADER};
use legion::manager::server::ManagerDaemonService;
use legion::storage::governance_store::{AuditFilter, AuditRecord};
use legion::storage::store::Store;
use legion::structs::governance::{
    ApprovalRule, ApprovalsConfig, GovernanceConfig, RateLimitConfig,
};
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::*;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Request, Status};

type R<T> = anyhow::Result<T>;

/// A served manager with the governance layer, and its management token.
struct Served {
    store: Store,
    svc: Arc<ManagerDaemonService>,
    addr: String,
    management: String,
    _server: tokio::task::JoinHandle<()>,
}

impl Served {
    async fn start(prefix: &str, config: GovernanceConfig) -> R<Self> {
        let store = crate::support::temp_store(prefix)?;
        let auth = Arc::new(Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )?);
        let (_, management) = auth
            .mint_initial_management_token(&store, None)
            .map_err(|status| anyhow!(status.to_string()))?
            .context("a fresh store has no management token")?;
        let svc = Arc::new(ManagerDaemonService::with_acl(store.clone(), auth));
        // No journal writer: records are written synchronously, so a test
        // reads them right after the call.
        let governance = Arc::new(Governance::new(store.clone(), config, None));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = format!("http://{}", listener.local_addr()?);
        let served = svc.clone();
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .layer(GovernanceLayer::new(governance))
                .add_service(ManagerServer::from_arc(served))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        Ok(Served {
            store,
            svc,
            addr,
            management,
            _server: server,
        })
    }

    fn client(&self, token: &str) -> R<AuthedManagerClient> {
        connect_manager_client_with_token(
            &self.addr,
            token,
            ClientTimeouts {
                connect: Some(Duration::from_secs(5)),
                request: Some(Duration::from_secs(30)),
            },
        )
    }

    /// A client token holding policy `name` with `rules`.
    async fn token(&self, name: &str, rules: &str) -> R<(String, String)> {
        self.policy(name, rules).await?;
        let reply = self
            .client(&self.management)?
            .create_acl_token(CreateAclTokenRequest {
                name: name.into(),
                token_type: "client".into(),
                policies: vec![name.into()],
                ttl: None,
            })
            .await?
            .into_inner();
        Ok((reply.token.unwrap_or_default().accessor_id, reply.secret))
    }

    async fn policy(&self, name: &str, rules: &str) -> R<()> {
        self.client(&self.management)?
            .upsert_acl_policy(UpsertAclPolicyRequest {
                name: name.into(),
                description: String::new(),
                rules_json: rules.into(),
            })
            .await?;
        Ok(())
    }

    fn journal(&self, operation: &str) -> R<Vec<AuditRecord>> {
        self.store.list_audit_events(&AuditFilter {
            operation: operation.into(),
            limit: 100,
            ..Default::default()
        })
    }
}

fn put(key: &str, value: &str) -> PutKvRequest {
    PutKvRequest {
        key: key.into(),
        value: value.into(),
        value_type: KvValueType::String as i32,
        ..Default::default()
    }
}

fn with_approval<T>(inner: T, approval: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request
        .metadata_mut()
        .insert(APPROVAL_HEADER, approval.parse().expect("ascii id"));
    request
}

fn legion_code(status: &Status) -> String {
    status
        .metadata()
        .get("legion-error-code")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn details(status: &Status) -> serde_json::Value {
    status
        .metadata()
        .get("legion-error-details")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default()
}

const KV_TEAM_WRITE: &str =
    r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["team/*"]}]"#;

#[tokio::test]
async fn journal_records_writes_denials_and_their_callers_test() -> R<()> {
    let served = Served::start("legion-gov-journal-", GovernanceConfig::default()).await?;
    let (accessor, secret) = served.token("kv-team", KV_TEAM_WRITE).await?;
    let mut client = served.client(&secret)?;
    client.put_kv(put("team/a", "1")).await?;
    let denied = client
        .put_kv(put("other/b", "2"))
        .await
        .expect_err("outside the policy");
    assert_eq!(denied.code(), Code::PermissionDenied);
    // A read is not journaled.
    client
        .get_kv(GetKvRequest {
            key: "team/a".into(),
            metadata_only: false,
        })
        .await?;
    // Idempotency keys are recorded.
    let _ = served
        .client(&served.management)?
        .destroy_job(DestroyJobRequest {
            name: "no-such-job".into(),
            idempotency_key: Some("retry-key-1".into()),
            ..Default::default()
        })
        .await;

    let puts = served.journal("PutKv")?;
    let ok = puts
        .iter()
        .find(|r| r.target == "kv:team/a")
        .context("the successful put is journaled")?;
    assert_eq!(ok.outcome, "ok");
    assert_eq!(ok.kind, "write");
    assert_eq!(ok.accessor_id, accessor);
    assert_eq!(ok.principal, "kv-team");
    assert_eq!(ok.source, "token");
    assert_eq!(ok.resource, "kv");
    assert_eq!(ok.action, "write");
    assert_eq!(ok.request_digest.len(), 64, "a SHA-256 digest");
    let no = puts
        .iter()
        .find(|r| r.target == "kv:other/b")
        .context("the denial is journaled")?;
    assert_eq!(no.outcome, "PermissionDenied");
    assert_eq!(no.kind, "denied");
    assert!(
        served.journal("GetKv")?.is_empty(),
        "reads are not journaled"
    );
    let destroy = served.journal("DestroyJob")?;
    assert_eq!(destroy.len(), 1);
    assert_eq!(destroy[0].idempotency_key, "retry-key-1");
    assert_eq!(destroy[0].outcome, "NotFound");

    // The same digest for the same request, a different one otherwise.
    client.put_kv(put("team/a", "1")).await?;
    let again: Vec<_> = served
        .journal("PutKv")?
        .into_iter()
        .filter(|r| r.target == "kv:team/a")
        .collect();
    assert_eq!(again.len(), 2);
    assert_eq!(again[0].request_digest, again[1].request_digest);
    assert_ne!(ok.request_digest, no.request_digest);

    // ListAuditEvents needs audit:read, and filters.
    let refused = client
        .list_audit_events(ListAuditEventsRequest::default())
        .await
        .expect_err("kv-team holds no audit:read");
    assert_eq!(refused.code(), Code::PermissionDenied);
    let listed = served
        .client(&served.management)?
        .list_audit_events(ListAuditEventsRequest {
            accessor_id: accessor.clone(),
            outcome: "ok".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(listed.events.len(), 2);
    assert!(listed.events.iter().all(|e| e.operation == "PutKv"));
    assert!(!listed.last_cursor.is_empty());
    // Following from the newest cursor returns nothing new.
    let follow = served
        .client(&served.management)?
        .list_audit_events(ListAuditEventsRequest {
            after_cursor: listed.last_cursor.clone(),
            ..Default::default()
        })
        .await?
        .into_inner();
    // Only kv-team's refused ListAuditEvents came after it: denials of reads
    // are journaled too.
    assert!(
        follow.events.len() == 1
            && follow.events[0].operation == "ListAuditEvents"
            && follow.events[0].kind == "denied"
            && follow.events[0].accessor_id == accessor,
        "newer records: {:?}",
        follow.events
    );
    Ok(())
}

#[tokio::test]
async fn journal_retention_keeps_the_newest_records_test() -> R<()> {
    let store = crate::support::temp_store("legion-gov-retention-")?;
    let records = (0..10)
        .map(|i| AuditRecord {
            id: format!("{i:032x}"),
            ts_ms: 1_000 + i,
            operation: "PutKv".into(),
            kind: "write".into(),
            outcome: "ok".into(),
            ..Default::default()
        })
        .collect();
    store.append_audit_events(records)?;
    assert_eq!(store.prune_audit_events_local(3, 0)?, 7);
    let left = store.list_audit_events(&AuditFilter {
        limit: 100,
        ..Default::default()
    })?;
    assert_eq!(
        left.iter().map(|r| r.ts_ms).collect::<Vec<_>>(),
        [1_009, 1_008, 1_007]
    );
    assert_eq!(store.prune_audit_events_local(100, 1_009)?, 2);
    Ok(())
}

#[tokio::test]
async fn tokens_expire_by_default_test() -> R<()> {
    let served = Served::start("legion-gov-ttl-", GovernanceConfig::default()).await?;
    served.policy("kv-team", KV_TEAM_WRITE).await?;
    let mut admin = served.client(&served.management)?;
    let token = admin
        .create_acl_token(CreateAclTokenRequest {
            name: "default-ttl".into(),
            token_type: "client".into(),
            policies: vec!["kv-team".into()],
            ttl: None,
        })
        .await?
        .into_inner()
        .token
        .unwrap_or_default();
    let expires = time::OffsetDateTime::parse(
        token
            .expires_at
            .as_deref()
            .context("client tokens expire by default")?,
        &time::format_description::well_known::Rfc3339,
    )?;
    let days = (expires - time::OffsetDateTime::now_utc()).whole_days();
    assert!((29..=30).contains(&days), "30-day default, got {days}");
    // Beyond the 90-day bound, and `never`, are refused for client tokens.
    for ttl in ["2400h", "never"] {
        let refused = admin
            .create_acl_token(CreateAclTokenRequest {
                name: "too-long".into(),
                token_type: "client".into(),
                policies: vec!["kv-team".into()],
                ttl: Some(ttl.into()),
            })
            .await
            .expect_err(ttl);
        assert_eq!(refused.code(), Code::InvalidArgument, "{ttl}");
    }
    // A management token may ask for `never`; by default it expires too.
    let never = admin
        .create_acl_token(CreateAclTokenRequest {
            name: "break-glass".into(),
            token_type: "management".into(),
            policies: Vec::new(),
            ttl: Some("never".into()),
        })
        .await?
        .into_inner();
    assert!(never.token.unwrap_or_default().expires_at.is_none());
    let defaulted = admin
        .create_acl_token(CreateAclTokenRequest {
            name: "ops".into(),
            token_type: "management".into(),
            policies: Vec::new(),
            ttl: None,
        })
        .await?
        .into_inner();
    assert!(defaulted.token.unwrap_or_default().expires_at.is_some());

    // An expired token is refused.
    let short = admin
        .create_acl_token(CreateAclTokenRequest {
            name: "short".into(),
            token_type: "client".into(),
            policies: vec!["kv-team".into()],
            ttl: Some("1s".into()),
        })
        .await?
        .into_inner();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let expired = served
        .client(&short.secret)?
        .put_kv(put("team/x", "1"))
        .await
        .expect_err("expired");
    assert_eq!(expired.code(), Code::Unauthenticated);
    assert!(
        expired.message().contains("expired"),
        "{}",
        expired.message()
    );
    Ok(())
}

#[tokio::test]
async fn child_tokens_are_never_wider_than_their_parent_test() -> R<()> {
    let served = Served::start("legion-gov-child-", GovernanceConfig::default()).await?;
    served.policy("kv-team", KV_TEAM_WRITE).await?;
    served
        .policy(
            "kv-other",
            r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["other/*"]}]"#,
        )
        .await?;
    let mut admin = served.client(&served.management)?;
    let parent = admin
        .create_acl_token(CreateAclTokenRequest {
            name: "agent".into(),
            token_type: "client".into(),
            policies: vec!["kv-team".into(), "kv-other".into()],
            ttl: Some("2h".into()),
        })
        .await?
        .into_inner();
    let parent_info = parent.token.clone().unwrap_or_default();
    let mut agent = served.client(&parent.secret)?;

    // A policy the parent does not hold is refused; so is a long TTL.
    let wider = agent
        .create_child_token(CreateChildTokenRequest {
            name: "wider".into(),
            policies: vec!["admin".into()],
            ..Default::default()
        })
        .await
        .expect_err("not the parent's policy");
    assert_eq!(wider.code(), Code::PermissionDenied);
    let long = agent
        .create_child_token(CreateChildTokenRequest {
            name: "long".into(),
            policies: vec!["kv-team".into()],
            ttl: Some("48h".into()),
            ..Default::default()
        })
        .await
        .expect_err("beyond acl.childTokenMaxTtl");
    assert_eq!(long.code(), Code::InvalidArgument);

    let child = agent
        .create_child_token(CreateChildTokenRequest {
            name: "sub-agent".into(),
            policies: vec!["kv-team".into()],
            ttl: Some("30m".into()),
            ..Default::default()
        })
        .await?
        .into_inner();
    let child_info = child.token.clone().unwrap_or_default();
    assert_eq!(
        child_info.parent_accessor_id.as_deref(),
        Some(parent_info.accessor_id.as_str())
    );
    assert!(child_info.expires_at <= parent_info.expires_at);
    let mut sub = served.client(&child.secret)?;
    sub.put_kv(put("team/a", "1")).await?;
    let narrower = sub
        .put_kv(put("other/a", "1"))
        .await
        .expect_err("the child holds only kv-team");
    assert_eq!(narrower.code(), Code::PermissionDenied);
    agent.put_kv(put("other/a", "1")).await?;

    // A deny added to a root policy the child does not hold still binds it.
    served
        .policy(
            "kv-other",
            r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["other/*"]},
                {"resources": ["kv"], "actions": ["write"], "effect": "deny", "paths": ["team/secret"]}]"#,
        )
        .await?;
    let denied = sub
        .put_kv(put("team/secret", "1"))
        .await
        .expect_err("the root's deny applies to the child");
    assert_eq!(denied.code(), Code::PermissionDenied);
    sub.put_kv(put("team/b", "1")).await?;

    // A grandchild, then revoking the root revokes the whole lineage.
    let grandchild = sub
        .create_child_token(CreateChildTokenRequest {
            name: "sub-sub".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    let tokens = admin
        .list_acl_tokens(ListAclTokensRequest {})
        .await?
        .into_inner()
        .tokens;
    assert!(tokens.iter().any(
        |t| t.accessor_id == grandchild.token.as_ref().unwrap().accessor_id
            && t.parent_accessor_id.as_deref() == Some(child_info.accessor_id.as_str())
    ));
    admin
        .delete_acl_token(DeleteAclTokenRequest {
            accessor_id: parent_info.accessor_id.clone(),
        })
        .await?;
    for secret in [&child.secret, &grandchild.secret] {
        let gone = served
            .client(secret)?
            .put_kv(put("team/c", "1"))
            .await
            .expect_err("revoked with its parent");
        assert_eq!(gone.code(), Code::Unauthenticated);
    }

    // Anonymous callers cannot mint children; the journal names the minting.
    let anonymous = served
        .svc
        .create_child_token(Request::new(CreateChildTokenRequest {
            name: "nobody".into(),
            ..Default::default()
        }))
        .await
        .expect_err("no token");
    assert_eq!(anonymous.code(), Code::Unauthenticated);
    let minted = served.journal("CreateChildToken")?;
    assert!(minted
        .iter()
        .any(|r| r.outcome == "ok" && r.detail.contains(&child_info.accessor_id)));
    Ok(())
}

#[tokio::test]
async fn child_tokens_expire_with_their_parent_test() -> R<()> {
    let served = Served::start("legion-gov-child-expiry-", GovernanceConfig::default()).await?;
    served.policy("kv-team", KV_TEAM_WRITE).await?;
    let parent = served
        .client(&served.management)?
        .create_acl_token(CreateAclTokenRequest {
            name: "brief".into(),
            token_type: "client".into(),
            policies: vec!["kv-team".into()],
            ttl: Some("2s".into()),
        })
        .await?
        .into_inner();
    let child = served
        .client(&parent.secret)?
        .create_child_token(CreateChildTokenRequest {
            name: "brief-child".into(),
            ttl: Some("1h".into()),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(
        child.token.as_ref().unwrap().expires_at <= parent.token.as_ref().unwrap().expires_at,
        "a child expires no later than its parent"
    );
    served
        .client(&child.secret)?
        .put_kv(put("team/a", "1"))
        .await?;
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let expired = served
        .client(&child.secret)?
        .put_kv(put("team/a", "2"))
        .await
        .expect_err("expired with its parent");
    assert_eq!(expired.code(), Code::Unauthenticated);
    Ok(())
}

fn prod_kv_gate(ttl: &str) -> GovernanceConfig {
    GovernanceConfig {
        approvals: Some(ApprovalsConfig {
            ttl: Some(ttl.into()),
            rules: vec![ApprovalRule {
                name: "prod-kv".into(),
                operations: vec!["PutKv".into(), "DeleteKv".into()],
                paths: vec!["prod/*".into()],
                ..Default::default()
            }],
        }),
        ..Default::default()
    }
}

const AGENT_RULES: &str = r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["*"]},
    {"resources": ["approval"], "actions": ["read", "write"]}]"#;
const APPROVER_RULES: &str = r#"[{"resources": ["approval"], "actions": ["read", "write"]}]"#;

#[tokio::test]
async fn approval_gate_flow_end_to_end_test() -> R<()> {
    let served = Served::start("legion-gov-approval-", prod_kv_gate("1h")).await?;
    let (agent_accessor, agent_secret) = served.token("agent", AGENT_RULES).await?;
    let (_, approver_secret) = served.token("approver", APPROVER_RULES).await?;
    let mut agent = served.client(&agent_secret)?;
    let mut approver = served.client(&approver_secret)?;

    // Ungated writes pass.
    agent.put_kv(put("dev/a", "1")).await?;
    // A gated write is refused with a typed error and an approval id.
    let gated = agent.put_kv(put("prod/db", "v1")).await.expect_err("gated");
    assert_eq!(gated.code(), Code::FailedPrecondition);
    assert_eq!(legion_code(&gated), "APPROVAL_REQUIRED");
    let approval_id = details(&gated)["approvalId"]
        .as_str()
        .context("approvalId")?
        .to_string();
    assert!(approval_id.starts_with("apr_"));
    assert_eq!(details(&gated)["rule"], "prod-kv");
    // Retrying without an approval reuses the pending request.
    let again = agent.put_kv(put("prod/db", "v1")).await.expect_err("gated");
    assert_eq!(details(&again)["approvalId"], approval_id.as_str());
    // Nothing was written.
    let absent = agent
        .get_kv(GetKvRequest {
            key: "prod/db".into(),
            metadata_only: false,
        })
        .await?
        .into_inner();
    assert!(absent.entry.is_none(), "the gated write did not happen");

    // The agent cannot approve its own request, even with approval:write.
    let own = agent
        .decide_approval(DecideApprovalRequest {
            id: approval_id.clone(),
            approve: true,
            ..Default::default()
        })
        .await
        .expect_err("self-approval");
    assert_eq!(own.code(), Code::PermissionDenied);
    // Retrying before the decision says so.
    let early = agent
        .put_kv(with_approval(put("prod/db", "v1"), &approval_id))
        .await
        .expect_err("not approved yet");
    assert_eq!(legion_code(&early), "APPROVAL_INVALID");
    assert!(early.message().contains("not been approved yet"));

    // The approver sees it pending and approves it.
    let pending = approver
        .list_approvals(ListApprovalsRequest {
            status: "pending".into(),
            ..Default::default()
        })
        .await?
        .into_inner()
        .approvals;
    assert!(pending.iter().any(|a| a.id == approval_id
        && a.requester_accessor == agent_accessor
        && a.target == "kv:prod/db"));
    let decided = approver
        .decide_approval(DecideApprovalRequest {
            id: approval_id.clone(),
            approve: true,
            reason: "change ticket 42".into(),
            ..Default::default()
        })
        .await?
        .into_inner()
        .approval
        .unwrap_or_default();
    assert_eq!(decided.status, "approved");

    // A different request (different digest) cannot use it.
    let mismatch = agent
        .put_kv(with_approval(put("prod/db", "v2"), &approval_id))
        .await
        .expect_err("digest mismatch");
    assert_eq!(legion_code(&mismatch), "APPROVAL_INVALID");
    assert!(
        mismatch.message().contains("different request"),
        "{}",
        mismatch.message()
    );
    // The approved request succeeds once.
    agent
        .put_kv(with_approval(put("prod/db", "v1"), &approval_id))
        .await?;
    let reused = agent
        .put_kv(with_approval(put("prod/db", "v1"), &approval_id))
        .await
        .expect_err("single use");
    assert!(
        reused.message().contains("already used"),
        "{}",
        reused.message()
    );

    // Denied requests stay denied.
    let gated = agent
        .put_kv(put("prod/cache", "x"))
        .await
        .expect_err("gated");
    let denied_id = details(&gated)["approvalId"].as_str().unwrap().to_string();
    approver
        .decide_approval(DecideApprovalRequest {
            id: denied_id.clone(),
            approve: false,
            reason: "no".into(),
            ..Default::default()
        })
        .await?;
    let refused = agent
        .put_kv(with_approval(put("prod/cache", "x"), &denied_id))
        .await
        .expect_err("denied");
    assert!(
        refused.message().contains("denied"),
        "{}",
        refused.message()
    );

    // Every step is journaled.
    let puts = served.journal("PutKv")?;
    assert!(puts.iter().any(|r| r.outcome == "APPROVAL_REQUIRED"));
    assert!(puts.iter().any(|r| r.outcome == "APPROVAL_INVALID"));
    let used = puts
        .iter()
        .find(|r| r.approval_id == approval_id)
        .context("the approved write names its approval")?;
    assert_eq!(used.outcome, "ok");
    let decisions = served.journal("DecideApproval")?;
    assert!(decisions
        .iter()
        .any(|r| r.outcome == "ok" && r.principal == "approver"));
    assert!(decisions
        .iter()
        .any(|r| r.outcome == "PermissionDenied" && r.principal == "agent"));
    Ok(())
}

#[tokio::test]
async fn approvals_expire_test() -> R<()> {
    let served = Served::start("legion-gov-approval-expiry-", prod_kv_gate("1s")).await?;
    let (_, agent_secret) = served.token("agent", AGENT_RULES).await?;
    let (_, approver_secret) = served.token("approver", APPROVER_RULES).await?;
    let mut agent = served.client(&agent_secret)?;
    let gated = agent.put_kv(put("prod/db", "v1")).await.expect_err("gated");
    let id = details(&gated)["approvalId"].as_str().unwrap().to_string();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let late = served
        .client(&approver_secret)?
        .decide_approval(DecideApprovalRequest {
            id: id.clone(),
            approve: true,
            ..Default::default()
        })
        .await
        .expect_err("expired before the decision");
    assert_eq!(late.code(), Code::FailedPrecondition);
    assert!(late.message().contains("expired"), "{}", late.message());
    let status = agent
        .get_approval(GetApprovalRequest { id })
        .await?
        .into_inner()
        .approval
        .unwrap_or_default()
        .status;
    assert_eq!(status, "expired");
    Ok(())
}

#[tokio::test]
async fn rate_limit_trips_and_recovers_test() -> R<()> {
    let config = GovernanceConfig {
        rate_limit: Some(RateLimitConfig {
            writes_per_second: Some(1.0),
            burst: Some(2),
            ..Default::default()
        }),
        ..Default::default()
    };
    let served = Served::start("legion-gov-rate-", config).await?;
    let (_, secret) = served.token("kv-team", KV_TEAM_WRITE).await?;
    let mut client = served.client(&secret)?;
    client.put_kv(put("team/a", "1")).await?;
    client.put_kv(put("team/a", "2")).await?;
    let limited = client
        .put_kv(put("team/a", "3"))
        .await
        .expect_err("burst exhausted");
    assert_eq!(limited.code(), Code::ResourceExhausted);
    assert_eq!(legion_code(&limited), "RATE_LIMITED");
    let retry_after = details(&limited)["retryAfterMs"]
        .as_u64()
        .context("retryAfterMs")?;
    assert!(retry_after > 0 && retry_after <= 1_000);
    // Reads are not limited; management tokens are exempt by default.
    client
        .get_kv(GetKvRequest {
            key: "team/a".into(),
            metadata_only: true,
        })
        .await?;
    for i in 0..5 {
        served
            .client(&served.management)?
            .put_kv(put("team/m", &i.to_string()))
            .await?;
    }
    tokio::time::sleep(Duration::from_millis(retry_after + 50)).await;
    client.put_kv(put("team/a", "4")).await?;
    assert!(served
        .journal("PutKv")?
        .iter()
        .any(|r| r.outcome == "RATE_LIMITED"));
    Ok(())
}

#[tokio::test]
async fn caller_aware_catalog_marks_what_a_token_may_do_test() -> R<()> {
    let served = Served::start("legion-gov-catalog-", GovernanceConfig::default()).await?;
    let (_, secret) = served
        .token(
            "mixed",
            r#"[{"resources": ["kv"], "actions": ["read"], "paths": ["team/*"]},
                {"resources": ["job"], "actions": ["read"], "namespaces": ["default"]}]"#,
        )
        .await?;
    let reply = served
        .client(&secret)?
        .get_caller_capabilities(GetCallerCapabilitiesRequest {})
        .await?
        .into_inner();
    let access = |rpc: &str| {
        reply
            .rpcs
            .iter()
            .find(|r| r.rpc == rpc)
            .unwrap_or_else(|| panic!("{rpc} listed"))
            .clone()
    };
    assert_eq!(access("GetKv").access, "partial");
    assert_eq!(access("PutKv").access, "denied");
    assert!(access("PutKv").mutates);
    assert_eq!(access("ListJobs").access, "partial");
    assert_eq!(access("ListJobs").namespaces, vec!["default".to_string()]);
    assert_eq!(access("WhoAmI").access, "open");
    assert_eq!(access("Heartbeat").access, "worker");
    assert_eq!(access("CreateChildToken").access, "allowed");
    assert_eq!(access("ListAuditEvents").access, "denied");
    let management = served
        .client(&served.management)?
        .get_caller_capabilities(GetCallerCapabilitiesRequest {})
        .await?
        .into_inner();
    assert!(management.is_management);
    assert!(management
        .rpcs
        .iter()
        .filter(|r| !r.permission.is_empty())
        .all(|r| r.access == "allowed"));
    Ok(())
}

#[tokio::test]
async fn usage_accumulates_over_simulated_time_test() -> R<()> {
    use legion::manager::governance::usage::{accumulate_once, BUCKET_MS};
    let served = Served::start("legion-gov-usage-", GovernanceConfig::default()).await?;
    let store = &served.store;
    store.insert_node(NodeMsg {
        id: "usage-node".into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "[]".into(),
        last_heartbeat: "2026-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;
    for (name, cpu) in [("team/web", 1000), ("api", 500)] {
        let spec = serde_json::json!({
            "name": name,
            "tasks": [{
                "runtime": "noop", "count": 1, "config": {},
                "resources": { "cpu": cpu, "memory": 512 }, "constraints": {},
            }],
        })
        .to_string();
        let config: JobConfig = serde_json::from_str(&spec)?;
        store.handle_job_submit(
            &config,
            &[Submission {
                path: format!("{}.json", name.replace('/', "-")),
                content: spec.clone(),
                is_primary: true,
            }],
        )?;
        let config_id = store.get_job_config(name)?.id();
        for (task_id, _) in store.get_task_configs_for_job_config(&config_id)? {
            store.place_alloc("usage-node", task_id)?;
        }
    }
    // Two hours of simulated ticks: one hour in bucket 0, one in bucket 1.
    let hour = Duration::from_secs(90 * 24 * 3600);
    let base = (legion::manager::governance::now_ms() / BUCKET_MS - 2) * BUCKET_MS;
    accumulate_once(store, 3600.0, base + 10, hour)?;
    accumulate_once(store, 3600.0, base + BUCKET_MS + 10, hour)?;

    let mut admin = served.client(&served.management)?;
    let by_namespace = admin
        .get_usage(GetUsageRequest {
            since: "4h".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    let team = by_namespace
        .rows
        .iter()
        .find(|r| r.namespace == "team")
        .context("team usage")?;
    // 1000 MHz for two hours = 2 core-hours; 512 MiB for two hours = 1 GiB-hour.
    assert!((team.cpu_core_hours - 2.0).abs() < 1e-6, "{team:?}");
    assert!((team.memory_gib_hours - 1.0).abs() < 1e-6, "{team:?}");
    assert!((team.allocation_hours - 2.0).abs() < 1e-6, "{team:?}");
    let default = by_namespace
        .rows
        .iter()
        .find(|r| r.namespace == "default")
        .context("default usage")?;
    assert!((default.cpu_core_hours - 1.0).abs() < 1e-6, "{default:?}");

    let by_hour = admin
        .get_usage(GetUsageRequest {
            since: "4h".into(),
            group_by: "hour".into(),
            namespaces: vec!["team".into()],
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(by_hour.rows.len(), 2);
    assert!(by_hour
        .rows
        .iter()
        .all(|r| (r.cpu_core_hours - 1.0).abs() < 1e-6));
    let by_job = admin
        .get_usage(GetUsageRequest {
            since: "4h".into(),
            group_by: "job".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(by_job
        .rows
        .iter()
        .any(|r| r.namespace == "team" && r.job == "web"));

    // A token that reads only `team` sees only `team`.
    let (_, secret) = served
        .token(
            "team-reader",
            r#"[{"resources": ["namespace", "job"], "actions": ["read"], "namespaces": ["team"]}]"#,
        )
        .await?;
    let scoped = served
        .client(&secret)?
        .get_usage(GetUsageRequest {
            since: "4h".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(scoped.rows.iter().all(|r| r.namespace == "team"));
    assert_eq!(scoped.rows.len(), 1);
    Ok(())
}
