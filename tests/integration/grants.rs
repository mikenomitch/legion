//! Job grants (apps plan §6.4 and §9 A2; docs/workload-identity.md,
//! "Permissions"): a job's `legion` identity asks for `permissions` and
//! `policies`, and gets them only when the token that submits it holds them.
//!
//! In process, against a manager service with ACLs on, workload identity on,
//! and no `controlPlaneBindings` at all:
//!
//! - escalation is refused through RunJob and RunJobTemplate, with the
//!   documented error, on every submit, an unchanged one included;
//! - a passing submit records the grant in the submitter's name;
//! - the job's `legion` token carries the grant of its allocation's version,
//!   as a bearer credential and through `legion login --method legion`;
//! - revoking it takes effect at the next verification, and a resubmit by
//!   someone who holds the access restores it;
//! - grants are keyed by the canonical job id, so a same-named job in
//!   another namespace gets nothing;
//! - the reserved prefix: a held `*` does not cover a requested `legion-*`;
//! - named policies are checked by their rules;
//! - a KV re-render carries an unchanged grant forward, and drops a changed
//!   one;
//! - the shipped cert renewer submits with no manager-config change, and
//!   fails with the documented error for a token lacking `kv:write certs/*`.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::manager::acl::Authenticator;
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::sync::Arc;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://manager.test:4747";
const NODE: &str = "grants-node";

struct Fixture {
    svc: ManagerDaemonService,
    store: Store,
    state: Arc<WorkloadIdentityState>,
    admin: String,
}

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-grants-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..Default::default()
    })?;
    store.insert_node(NodeMsg::from(Node {
        id: NODE.into(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": NODE,
            "maxAllocations": 20,
            "resources": { "memory": 65536, "cpu": 40000 },
        }),
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
        }),
        dynamic_attrs: json!({ "capabilities": [] }),
        health_checks: json!({}),
    }))?;
    let state = Arc::new(WorkloadIdentityState::initialize(
        &store,
        &WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some(ISSUER.into()),
            ..Default::default()
        },
        ISSUER,
        std::time::SystemTime::now(),
    )?);
    let mut auth = Authenticator::from_config(
        &AclConfig {
            enabled: Some(true),
            ..Default::default()
        }
        .with_defaults(),
    )?;
    // No bindings: what the job's token may do comes from its grant alone.
    auth.register_method(Box::new(ControlPlaneIdentityMethod::new(
        state.clone(),
        Vec::new(),
        std::time::Duration::from_secs(3600),
        store.clone(),
    )))?;
    let auth = Arc::new(auth);
    let (_, admin) = auth
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .context("a fresh store has no management token")?;
    let svc = ManagerDaemonService::with_acl(store.clone(), auth)
        .with_workload_identity(Some(state.clone()));
    Ok(Fixture {
        svc,
        store,
        state,
        admin,
    })
}

impl Fixture {
    /// A token whose one policy holds `rules`.
    async fn token(&self, name: &str, rules: Value) -> R<String> {
        self.svc
            .upsert_acl_policy(req(
                UpsertAclPolicyRequest {
                    name: name.into(),
                    description: String::new(),
                    rules_json: rules.to_string(),
                },
                &self.admin,
            ))
            .await?;
        Ok(self
            .svc
            .create_acl_token(req(
                CreateAclTokenRequest {
                    name: name.into(),
                    token_type: "client".into(),
                    policies: vec![name.into()],
                    ttl: None,
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .secret)
    }

    async fn run(&self, spec: &Value, token: &str) -> Result<RunJobReply, tonic::Status> {
        self.svc
            .run_job(req(
                RunJobRequest {
                    parsed_jobspec: spec.to_string(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn grants(&self, job: &str) -> R<Vec<JobGrant>> {
        Ok(self
            .svc
            .list_grants(req(
                ListGrantsRequest {
                    job: Some(job.into()),
                    include_revoked: true,
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .grants)
    }

    /// Place `job` and return its allocation.
    fn place(&self, job: &str) -> R<i64> {
        let config_id = self.store.get_job_config(job)?.id();
        self.store
            .enqueue_eval("job_reschedule", Some(job), Some(&config_id), None, None)?;
        while let Some(eval) = self.store.get_next_eval()? {
            let outcome = eval.handle(&self.store);
            self.store.set_eval_status(
                &eval.id,
                if outcome.is_ok() {
                    "complete"
                } else {
                    "failed"
                },
            )?;
        }
        self.store
            .allocs_for_job(job)?
            .into_iter()
            .map(|allocation| allocation.id)
            .max()
            .with_context(|| format!("{job} should be placed"))
    }

    /// The `legion` identity token the manager issues `allocation_id`.
    fn legion_token(&self, allocation_id: i64) -> R<String> {
        let allocation = self
            .store
            .get_allocation_by_id(allocation_id)?
            .context("allocation")?;
        let task = self
            .store
            .get_task_configs_for_job_config(&allocation.job_config_id)?
            .into_iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .map(|(_, task)| task)
            .context("task config")?;
        let claims = claims_for_allocation(&self.store, &allocation, &task, "auto")?;
        let identity = WorkloadIdentity {
            name: CONTROL_PLANE_IDENTITY_NAME.into(),
            ..Default::default()
        };
        Ok(self
            .state
            .issue(&claims, &identity, std::time::SystemTime::now())?
            .token)
    }

    async fn put_kv(&self, key: &str, token: &str) -> Result<(), tonic::Status> {
        self.svc
            .put_kv(req(
                PutKvRequest {
                    key: key.into(),
                    value: "value".into(),
                    value_type: KvValueType::String as i32,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ())
    }

    async fn revoke(&self, job: &str, key: Option<&str>) -> R<RevokeGrantReply> {
        Ok(self
            .svc
            .revoke_grant(req(
                RevokeGrantRequest {
                    job: job.into(),
                    version: None,
                    idempotency_key: key.map(str::to_string),
                },
                &self.admin,
            ))
            .await?
            .into_inner())
    }
}

/// A one-task job whose `legion` identity asks for `permissions`. `exec`,
/// the built-in runtime, so no plugin needs building.
fn job(name: &str, permissions: Value) -> Value {
    json!({
        "name": name,
        "task": {
            "name": "main",
            "runtime": "exec",
            "config": { "command": ["true"] },
            "resources": { "cpu": 100, "memory": 64 },
            "identities": [{ "name": "legion", "permissions": permissions }],
        },
    })
}

fn certs_writer() -> Value {
    json!([{ "resources": ["kv"], "actions": ["read", "write"], "paths": ["certs/*"] }])
}

const DEPLOYER: &str = r#"[
    { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["*"], "host": true },
    { "resources": ["template"], "actions": ["read"] }
]"#;

fn deployer_rules() -> Value {
    serde_json::from_str(DEPLOYER).expect("deployer rules")
}

/// The deployer's rules plus `kv` on `certs/*`.
fn holder_rules() -> Value {
    let mut rules = deployer_rules();
    rules
        .as_array_mut()
        .expect("array")
        .extend(certs_writer().as_array().expect("array").iter().cloned());
    rules
}

fn denied(result: Result<impl std::fmt::Debug, tonic::Status>) -> String {
    let status = result.expect_err("the submit should be refused");
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    status.message().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_cannot_be_granted_more_than_its_submitter_holds_test() -> R<()> {
    let fx = fixture()?;
    let deployer = fx.token("deployer", deployer_rules()).await?;
    let holder = fx.token("holder", holder_rules()).await?;
    let spec = job("renewer", certs_writer());

    // RunJob: refused, with the documented error, and nothing stored.
    let message = denied(fx.run(&spec, &deployer).await);
    assert!(
        message.starts_with(
            "renewer: identity 'legion' asks for kv:write on certs/*, which your token does not \
             hold"
        ),
        "{message}"
    );
    assert!(
        message.contains(
            "Hint: have someone with that access submit this job, or ask for it to be granted \
             to you"
        ),
        "{message}"
    );
    assert!(fx.store.get_job_config_optional("renewer")?.is_none());

    // RunJobTemplate renders the same job, and is refused the same way.
    fx.svc
        .upsert_job_template(req(
            UpsertJobTemplateRequest {
                name: "renewer-template".into(),
                source_nickel: format!(
                    "{{ description = \"grants\", variables = [], job | not_exported = fun vars \
                     => std.deserialize 'Json {} }}",
                    serde_json::to_string(&spec.to_string())?
                ),
            },
            &fx.admin,
        ))
        .await?;
    let through_template = fx
        .svc
        .run_job_template(req(
            RunJobTemplateRequest {
                name: "renewer-template".into(),
                variables_json: "{}".into(),
                ..Default::default()
            },
            &deployer,
        ))
        .await;
    let message = denied(through_template);
    assert!(
        message.contains("identity 'legion' asks for kv:write on certs/*"),
        "{message}"
    );

    // A token that holds it submits, and the grant is recorded in its name.
    fx.run(&spec, &holder).await?;
    let grants = fx.grants("renewer").await?;
    assert_eq!(grants.len(), 1, "{grants:?}");
    assert_eq!(grants[0].granted_by, "holder");
    assert_eq!(grants[0].version, 1);
    assert_eq!(grants[0].tasks[0].task, "main");
    assert!(grants[0].tasks[0].permissions_json.contains("certs/*"));

    // Every submit is checked, an unchanged one included: write access to
    // the job does not let the deployer redeploy what it could not grant.
    let message = denied(fx.run(&spec, &deployer).await);
    assert!(
        message.contains("asks for kv:write on certs/*"),
        "{message}"
    );

    // A job that asks for nothing is not checked, and has no grant.
    fx.run(&job("plain", json!([])), &deployer).await?;
    assert!(fx.grants("plain").await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workload_token_carries_its_grant_until_it_is_revoked_test() -> R<()> {
    let fx = fixture()?;
    let holder = fx.token("holder", holder_rules()).await?;
    fx.run(&job("renewer", certs_writer()), &holder).await?;
    let allocation = fx.place("renewer")?;
    let token = fx.legion_token(allocation)?;

    // The grant, as a bearer credential: exactly what was asked for.
    fx.put_kv("certs/app.example.com", &token).await?;
    let outside = fx
        .put_kv("other/key", &token)
        .await
        .expect_err("not granted");
    assert_eq!(outside.code(), Code::PermissionDenied, "{outside:?}");
    let whoami = fx
        .svc
        .who_am_i(req(WhoAmIRequest {}, &token))
        .await?
        .into_inner();
    assert_eq!(
        whoami.policies,
        vec![format!("grant:alloc/{allocation}")],
        "{whoami:?}"
    );

    // And through `legion login --method legion`.
    let login = fx
        .svc
        .login(Request::new(LoginRequest {
            method: "legion".into(),
            jwt: Some(token.clone()),
            ..Default::default()
        }))
        .await?
        .into_inner()
        .secret;
    fx.put_kv("certs/login.example.com", &login).await?;

    // Revoked: both credentials lose it at their next request.
    let reply = fx.revoke("renewer", Some("revoke-1")).await?;
    assert_eq!(reply.revoked, 1, "{reply:?}");
    let again = fx.revoke("renewer", Some("revoke-1")).await?;
    assert!(again.deduplicated, "{again:?}");
    assert_eq!(again.revoked, 1, "the first reply, replayed");
    for credential in [&token, &login] {
        let refused = fx
            .put_kv("certs/app.example.com", credential)
            .await
            .expect_err("revoked");
        assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    }
    let grants = fx.grants("renewer").await?;
    assert!(!grants[0].revoked_at.is_empty(), "{grants:?}");
    assert!(!grants[0].revoked_by.is_empty(), "{grants:?}");

    // A resubmit by someone who holds the access grants it again.
    fx.run(&job("renewer", certs_writer()), &holder).await?;
    fx.put_kv("certs/app.example.com", &token).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grants_are_keyed_by_the_canonical_job_id_test() -> R<()> {
    let fx = fixture()?;
    fx.svc
        .apply_namespace(req(
            ApplyNamespaceRequest {
                namespace: Some(Namespace {
                    name: "team-a".into(),
                    ..Default::default()
                }),
                dry_run: false,
            },
            &fx.admin,
        ))
        .await?;
    let holder = fx.token("holder", holder_rules()).await?;
    let mut granted = job("web", certs_writer());
    granted["namespace"] = json!("team-a");
    fx.run(&granted, &holder).await?;
    // The same bare name in `default`, asking for nothing.
    fx.run(&job("web", json!([])), &holder).await?;

    let in_default = fx.legion_token(fx.place("web")?)?;
    let refused = fx
        .put_kv("certs/app.example.com", &in_default)
        .await
        .expect_err("default/web holds no grant");
    assert!(
        matches!(
            refused.code(),
            Code::PermissionDenied | Code::Unauthenticated
        ),
        "{refused:?}"
    );
    let in_team = fx.legion_token(fx.place("team-a/web")?)?;
    fx.put_kv("certs/app.example.com", &in_team).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reserved_names_and_named_policies_are_checked_test() -> R<()> {
    let fx = fixture()?;
    // A tenant admin: everything in every tenant namespace, every KV key
    // `*` reaches.
    let tenant_admin = fx
        .token(
            "tenant-admin",
            json!([
                { "resources": ["*"], "actions": ["*"], "namespaces": ["*"], "host": true },
                { "resources": ["kv"], "actions": ["*"], "paths": ["*"] },
            ]),
        )
        .await?;
    let message = denied(
        fx.run(
            &job(
                "side-door",
                json!([{ "resources": ["exec"], "actions": ["write"], "namespaces": ["legion-system"] }]),
            ),
            &tenant_admin,
        )
        .await,
    );
    assert!(
        message.contains("asks for exec:write in legion-system")
            && message.contains("does not reach reserved"),
        "{message}"
    );
    let message = denied(
        fx.run(
            &job(
                "leases",
                json!([{ "resources": ["kv"], "actions": ["write"], "paths": ["legion/leases/*"] }]),
            ),
            &tenant_admin,
        )
        .await,
    );
    assert!(message.contains("kv:write on legion/leases/*"), "{message}");

    // A named policy must exist, and is checked by its rules.
    let deployer = fx.token("deployer", deployer_rules()).await?;
    let holder = fx.token("holder", holder_rules()).await?;
    let mut named = job("named", json!([]));
    named["task"]["identities"][0]["policies"] = json!(["cert-writer"]);
    let missing = fx.run(&named, &holder).await.expect_err("no such policy");
    assert_eq!(missing.code(), Code::InvalidArgument, "{missing:?}");
    assert!(missing.message().contains("cert-writer"), "{missing:?}");
    fx.token("cert-writer", certs_writer()).await?;
    let message = denied(fx.run(&named, &deployer).await);
    assert!(
        message.contains("asks for policy cert-writer, which grants kv:write on certs/*"),
        "{message}"
    );
    fx.run(&named, &holder).await?;
    let grants = fx.grants("named").await?;
    assert_eq!(grants[0].tasks[0].policies, vec!["cert-writer".to_string()]);

    // Only the `legion` identity may ask, and a grant reference is not a
    // policy name.
    let mut elsewhere = job("elsewhere", json!([]));
    elsewhere["task"]["identities"] = json!([{
        "name": "aws", "audience": ["sts.amazonaws.com"], "file": { "path": "aws/token" },
        "permissions": certs_writer(),
    }]);
    let refused = fx.run(&elsewhere, &holder).await.expect_err("not legion");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    let upsert = fx
        .svc
        .upsert_acl_policy(req(
            UpsertAclPolicyRequest {
                name: "grant:alloc/1".into(),
                description: String::new(),
                rules_json: "[]".into(),
            },
            &fx.admin,
        ))
        .await
        .expect_err("reserved policy name");
    assert_eq!(upsert.code(), Code::InvalidArgument, "{upsert:?}");
    Ok(())
}

/// Legion's own re-render of a job (after a KV change) stores a new version
/// with no submitter to check: it carries the previous version's grant when
/// it asks for exactly the same, and none when it asks for something else.
#[test]
fn a_rerender_carries_an_unchanged_grant_forward_test() -> R<()> {
    let fx = fixture()?;
    let submission = |n: &str| Submission {
        path: format!("{n}.json"),
        content: "{}".into(),
        is_primary: true,
    };
    let parse = |spec: Value| -> R<JobConfig> { Ok(serde_json::from_value(spec)?) };
    let v1 = parse(job("rerendered", certs_writer()))?;
    fx.store
        .handle_job_submit_as(&v1, &[submission("v1")], None, "holder")?;
    let mut v2 = v1.clone();
    v2.tasks[0].count = TaskCount::new(2);
    fx.store.handle_job_submit(&v2, &[submission("v2")])?;
    let grants = fx.store.list_job_grants(Some("rerendered"), true)?;
    assert_eq!(grants.len(), 2, "{grants:?}");
    assert_eq!(grants[0].version, 2);
    assert_eq!(grants[0].granted_by, "holder");

    let mut v3 = v2.clone();
    v3.tasks[0].identities[0].permissions[0].paths = Some(vec!["*".into()]);
    fx.store.handle_job_submit(&v3, &[submission("v3")])?;
    let versions: Vec<i64> = fx
        .store
        .list_job_grants(Some("rerendered"), true)?
        .iter()
        .map(|grant| grant.version)
        .collect();
    assert_eq!(versions, vec![2, 1], "a widened re-render carries nothing");
    Ok(())
}

/// The rewritten `configs/jobs/cert-renewer.ncl` (apps plan §12 A2): it
/// submits against a manager with no binding and no policy for it, and a
/// token lacking `kv:write certs/*` gets the documented error.
#[tokio::test(flavor = "multi_thread")]
async fn the_cert_renewer_example_submits_with_no_manager_config_test() -> R<()> {
    let fx = fixture()?;
    // The DNS provider token its env template reads.
    fx.svc
        .put_kv(req(
            PutKvRequest {
                key: "secrets/cloudflare-dns-token".into(),
                value: "dns-token".into(),
                value_type: KvValueType::String as i32,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/configs/jobs/cert-renewer.ncl");
    let spec: Value = serde_json::from_str(&legion::config_lang::eval_file_with_vars(
        path,
        Some(&serde_json::Map::new()),
    )?)?;
    let dns_token = json!({ "resources": ["kv"], "actions": ["read"], "paths": ["secrets/cloudflare-dns-token"] });
    let mut lacking = deployer_rules();
    lacking
        .as_array_mut()
        .expect("array")
        .push(dns_token.clone());
    let lacking = fx.token("lacking", lacking).await?;
    let message = denied(fx.run(&spec, &lacking).await);
    assert!(
        message.starts_with(
            "cert-renewer-app-example-com: identity 'legion' asks for kv:write on certs/*, which \
             your token does not hold"
        ),
        "{message}"
    );

    let mut holding = holder_rules();
    holding.as_array_mut().expect("array").push(dns_token);
    let holding = fx.token("holding", holding).await?;
    fx.run(&spec, &holding).await?;
    let grants = fx.grants("cert-renewer-app-example-com").await?;
    assert_eq!(grants.len(), 1, "{grants:?}");
    assert_eq!(grants[0].granted_by, "holding");
    Ok(())
}
