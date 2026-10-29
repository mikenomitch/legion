//! App actions run as the viewer (app-actions-as-viewer plan §9;
//! docs/auth-and-acls.md, "Delegated identities"; docs/apps.md, "Acting as
//! the viewer").
//!
//! In process, against a manager service with ACLs and workload identity on,
//! served on a port: two installed apps placed on a manager node (`deleg`,
//! whose grant is `scale` in `default` only, and `other`), `deleg`'s UI
//! served by the app framework on loopback behind a forwarder that delivers
//! as a worker does, and its `pause` action calling back into the manager
//! through `ActionRequest::client_as_viewer_over`, over a connection that
//! presents the app's own `legion` token, as its API socket would.
//!
//! 1. A press runs as the viewer: the pause is recorded by `alice via app
//!    deleg`, and `WhoAmI` through the viewer client says `delegated`.
//! 2. The same assertion on another job, or for another resource or action
//!    (`job:write`), is refused; `read` on the context is delegated.
//! 3. Presented by another app's identity, a management token, the viewer's
//!    own token, or no credential, it is refused.
//! 4. An expired assertion is refused, also inside the JWT library's leeway.
//! 5. Revoking the viewer's token, or the permission, ends the delegation at
//!    once.
//! 6. A delegation never exceeds the app's own grant.
//! 7. Streams and the app plane refuse a delegated identity.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::apps::ui::{
    ActionOutcome, ActionRequest, AssertionVerifier, ClusterKeys, Refused, StaticKeys, UiRegistry,
    UiServer, ViewRequest,
};
use legion::cmd::helpers::{manager_connection_with_token, ClientTimeouts, ManagerConnection};
use legion::manager::acl::Authenticator;
use legion::manager::app_forward::AppForwarder;
use legion::manager::apps::{AppReconciler, PassOutcome, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::acl::{AclAction, AclResource, AclScope};
use legion::structs::app::BuildMarker;
use legion::structs::app_ui::{
    AppView, AssertionKind, AssertionScope, Delegation, ViewContext, ViewerAssertion,
    ASSERTION_CLAIM, ON_BEHALF_OF_HEADER,
};
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
const APP: &str = "deleg";
const OTHER_APP: &str = "other";
const NODE: &str = "mgr-1";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

/// A request with `token` as the credential and `assertion` in
/// `legion-on-behalf-of`, as a viewer client sends it.
fn on_behalf<T>(inner: T, token: &str, assertion: &str) -> Request<T> {
    let mut request = if token.is_empty() {
        Request::new(inner)
    } else {
        req(inner, token)
    };
    request
        .metadata_mut()
        .insert(ON_BEHALF_OF_HEADER, assertion.parse().expect("ascii"));
    request
}

fn timeouts() -> ClientTimeouts {
    ClientTimeouts {
        connect: Some(Duration::from_secs(5)),
        request: Some(Duration::from_secs(30)),
    }
}

/// An app whose grant is `scale` in `default`, with a job panel and a
/// `pause` action declaring `scale:write`.
fn manifest(name: &str) -> Value {
    json!({
        "name": name,
        "version": "1.0.0",
        "description": "Pauses autoscaling as the viewer.",
        "job": { "task": { "name": "main", "runtime": "exec",
                            "config": { "command": ["sleep", "3600"] },
                            "network": { "ports": [{ "name": "api" }] } } },
        "permissions": [
            { "resources": ["scale"], "actions": ["read", "write"], "namespaces": ["default"] },
        ],
        "ui": {
            "job": [{ "title": "Pauses", "view": "job",
                      "permission": { "resource": "scale", "action": "read" } }],
            "actions": [{ "name": "pause", "title": "Pause",
                          "permission": { "resource": "scale", "action": "write" } }],
        },
    })
}

/// Delivers what the manager forwards to the app's port, the way a worker
/// does.
#[derive(Debug)]
struct LoopbackForwarder {
    addr: std::net::SocketAddr,
}

#[tonic::async_trait]
impl AppForwarder for LoopbackForwarder {
    async fn forward(
        &self,
        _store: &Store,
        node_id: &str,
        request: ForwardAppRequestRequest,
    ) -> Result<ForwardAppRequestReply, tonic::Status> {
        assert_eq!(node_id, NODE);
        legion::worker::app_forward::check_request(&request)?;
        let stream = tokio::net::TcpStream::connect(self.addr)
            .await
            .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
        legion::worker::app_forward::exchange(stream, &request).await
    }
}

struct Fixture {
    svc: Arc<ManagerDaemonService>,
    store: Store,
    state: Arc<WorkloadIdentityState>,
    admin: String,
    /// The assertion of the last action the app ran.
    captured: Arc<Mutex<Option<String>>>,
    _server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self._server.abort();
    }
}

/// A token's secret and accessor.
struct Token {
    secret: String,
    accessor: String,
}

impl Fixture {
    async fn token(&self, name: &str, rules: Value) -> R<Token> {
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
        let reply = self
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
            .into_inner();
        Ok(Token {
            secret: reply.secret,
            accessor: reply.token.context("token info")?.accessor_id,
        })
    }

    /// Alice: `app:read` on `deleg`, and `scale` in `default`.
    async fn alice(&self) -> R<Token> {
        self.token(
            "alice",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": [APP] },
                { "resources": ["scale"], "actions": ["read", "write"], "namespaces": ["default"] },
            ]),
        )
        .await
    }

    async fn press(&self, token: &str, job: &str) -> Result<InvokeAppActionReply, tonic::Status> {
        self.svc
            .invoke_app_action(req(
                InvokeAppActionRequest {
                    app: APP.into(),
                    action: "pause".into(),
                    context: Some(AppViewContext {
                        slot: "job".into(),
                        job: job.into(),
                        ..Default::default()
                    }),
                    inputs_json: String::new(),
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    fn captured(&self) -> R<String> {
        self.captured
            .lock()
            .unwrap()
            .clone()
            .context("the app ran no action")
    }

    /// The `legion` token the manager would issue `app`'s allocation: what
    /// its API socket attaches.
    fn app_token(&self, app: &str) -> R<String> {
        let job = format!("legion-system/{app}");
        let allocation = self
            .store
            .allocs_for_job(&job)?
            .into_iter()
            .next()
            .with_context(|| format!("{job} should be placed"))?;
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

    /// An action assertion for `deleg` signed with the cluster's key, as
    /// the manager signs one, living `iat..exp`.
    fn sign(&self, accessor: Option<&str>, iat: u64, exp: u64) -> R<String> {
        let claim = ViewerAssertion {
            kind: AssertionKind::Action,
            app: APP.into(),
            name: "pause".into(),
            principal: "alice".into(),
            accessor: accessor.map(str::to_string),
            permission: Some("scale:write".into()),
            scope: AssertionScope::Context,
            namespaces: Vec::new(),
            context: ViewContext {
                slot: "job".into(),
                namespace: "default".into(),
                job: "web".into(),
                ..Default::default()
            },
            inputs: Some(String::new()),
            delegation: Some(
                Delegation::new(
                    AclResource::Scale,
                    &[AclAction::Write],
                    &AclScope::job("web"),
                )
                .map_err(|err| anyhow!(err))?,
            ),
        };
        let mut claims = serde_json::Map::new();
        claims.insert(ASSERTION_CLAIM.into(), serde_json::to_value(&claim)?);
        Ok(self
            .state
            .issue_viewer_assertion(APP, "va_test", "principal:alice", claims, iat, exp)?
            .token)
    }

    fn pause_request(job: &str) -> PauseScalingRequest {
        PauseScalingRequest {
            job: job.into(),
            reason: "by hand".into(),
            ..Default::default()
        }
    }

    fn events(&self, kind: &str) -> R<Vec<String>> {
        Ok(self
            .store
            .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
                kind: Some(kind.into()),
                limit: Some(50),
                ..Default::default()
            })?
            .into_iter()
            .map(|event| event.message)
            .collect())
    }
}

/// The app's UI: a job panel, and `pause`, which records the assertion it
/// ran on, asks `WhoAmI` and pauses the context's job through the viewer
/// client, and answers who it acted as. A refusal from the manager becomes
/// the app's refusal, carrying the manager's code and message.
fn app_ui(
    connection: Arc<OnceLock<ManagerConnection>>,
    captured: Arc<Mutex<Option<String>>>,
) -> Arc<UiRegistry> {
    let registry = Arc::new(UiRegistry::default());
    registry.view("job", |_request: ViewRequest| async move {
        Ok(AppView::hidden())
    });
    registry.action("pause", move |request: ActionRequest| {
        let connection = connection.clone();
        let captured = captured.clone();
        async move {
            *captured.lock().unwrap() = Some(request.assertion().to_string());
            let refused = |status: tonic::Status| {
                anyhow::Error::new(Refused(format!(
                    "{:?}: {}",
                    status.code(),
                    status.message()
                )))
            };
            let connection = connection.get().context("the app is not connected")?;
            let mut legion = request.client_as_viewer_over(connection)?;
            let me = legion
                .who_am_i(WhoAmIRequest {})
                .await
                .map_err(refused)?
                .into_inner();
            let job = request.context.job_id().unwrap_or_default();
            legion
                .pause_scaling(PauseScalingRequest {
                    job: job.clone(),
                    reason: "paused by the fake".into(),
                    ..Default::default()
                })
                .await
                .map_err(refused)?;
            Ok(ActionOutcome::done(format!(
                "{} ({}) paused {job} holding {}",
                me.principal,
                me.source,
                me.rules_json.unwrap_or_default()
            )))
        }
    });
    registry
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-app-delegation-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    store.ensure_system_namespace()?;
    store.insert_worker_type(&WorkerConfig {
        name: "legion-system".into(),
        pool: "legion-system".into(),
        namespaces: vec!["legion-system".into()],
        ..Default::default()
    })?;
    store.insert_node(NodeMsg::from(Node {
        id: NODE.into(),
        pool: "legion-system".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": NODE,
            "legion.role": "manager",
            "namespaces": ["legion-system"],
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
    auth.register_method(Box::new(ControlPlaneIdentityMethod::new(
        state.clone(),
        Vec::new(),
        Duration::from_secs(3600),
        store.clone(),
    )))?;
    let auth = Arc::new(auth);
    let (_, admin) = auth
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .context("a fresh store has no management token")?;

    // The app's UI port.
    let connection = Arc::new(OnceLock::new());
    let captured = Arc::new(Mutex::new(None));
    let keys = ClusterKeys {
        issuer: state.issuer_url()?,
        jwks: state.jwks()?,
    };
    let ui = Arc::new(UiServer::new(
        app_ui(connection.clone(), captured.clone()),
        Some(Arc::new(AssertionVerifier::new(
            APP,
            Arc::new(StaticKeys(keys)),
        ))),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let app_addr = listener.local_addr()?;
    tokio::spawn(legion::apps::ui::serve(listener, ui));

    // The manager, also on a port for the app's calls back.
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth)
            .with_workload_identity(Some(state.clone()))
            .with_app_forwarder(Arc::new(LoopbackForwarder { addr: app_addr })),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let served = svc.clone();
    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ManagerServer::from_arc(served))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    let fx = Fixture {
        svc,
        store,
        state,
        admin,
        captured,
        _server: server,
    };

    // Install both apps, let the reconciler submit their jobs, and place
    // them on the manager.
    let mut reconciler = AppReconciler::new(
        fx.svc.clone(),
        ReconcilerSettings {
            seed: BTreeMap::new(),
            own_build: BuildMarker {
                version: "0.1.0".into(),
                build_id: "a".into(),
            },
        },
    );
    for app in [APP, OTHER_APP] {
        fx.svc
            .install_app(req(
                InstallAppRequest {
                    manifest_json: manifest(app).to_string(),
                    source: "file".into(),
                    source_ref: format!("./{app}.json"),
                    ..Default::default()
                },
                &fx.admin,
            ))
            .await?;
        let outcome = reconciler.reconcile_app(app).await;
        assert!(
            matches!(outcome, PassOutcome::Submitted { .. }),
            "{outcome:?}"
        );
    }
    while let Some(eval) = fx.store.claim_eval()? {
        let ok = eval.handle(&fx.store).is_ok();
        fx.store.complete_eval(&eval.id, ok)?;
    }
    for app in [APP, OTHER_APP] {
        anyhow::ensure!(
            !fx.store
                .app_ui_allocations(&format!("legion-system/{app}"))?
                .is_empty(),
            "{app}'s job should be placed"
        );
    }
    let _ = connection.set(manager_connection_with_token(
        &addr,
        &fx.app_token(APP)?,
        timeouts(),
    )?);

    // Jobs to act on: two in `default`, one in `team-b`.
    fx.svc
        .apply_namespace(req(
            ApplyNamespaceRequest {
                namespace: Some(Namespace {
                    name: "team-b".into(),
                    ..Default::default()
                }),
                dry_run: false,
            },
            &fx.admin,
        ))
        .await?;
    for (namespace, name) in [("default", "web"), ("default", "api"), ("team-b", "other")] {
        fx.svc
            .run_job(req(
                RunJobRequest {
                    parsed_jobspec: json!({
                        "name": name,
                        "namespace": namespace,
                        "task": { "name": name, "runtime": "exec", "count": 1,
                                  "config": { "command": ["sleep", "3600"] },
                                  "resources": { "cpu": 100, "memory": 32 } },
                    })
                    .to_string(),
                    ..Default::default()
                },
                &fx.admin,
            ))
            .await
            .map_err(|status| anyhow!("RunJob {namespace}/{name}: {}", status.message()))?;
    }
    Ok(fx)
}

/// Scenarios 1-3: a press runs as the viewer on its context only, and only
/// the app it was signed for may present it.
#[tokio::test(flavor = "multi_thread")]
async fn an_action_runs_as_the_viewer_on_its_context_only_test() -> R<()> {
    let fx = fixture().await?;
    let alice = fx.alice().await?;

    // 1. The press: the app's call is `alice via app deleg`, holding one
    // rule, and the pause is recorded by her through the app.
    let reply = fx.press(&alice.secret, "web").await?;
    assert!(
        reply
            .message
            .starts_with("alice via app deleg (delegated) paused web holding"),
        "{}",
        reply.message
    );
    let rules: Value =
        serde_json::from_str(reply.message.split_once("holding ").context("rules")?.1)?;
    assert_eq!(
        rules,
        json!([{ "resources": ["scale"], "actions": ["write", "read"],
                 "namespaces": ["default"], "names": ["web"] }])
    );
    let paused = fx.events("scale.paused")?;
    assert!(
        paused
            .iter()
            .any(|message| message.ends_with("by alice via app deleg")),
        "{paused:?}"
    );
    let pressed = fx.events("app.action")?;
    assert!(
        pressed
            .iter()
            .any(|message| message.contains(": alice ran pause on web: ok")),
        "{pressed:?}"
    );

    // 2. The same assertion, beside the app's own token: read on the
    // context is delegated; another job, or `job:write`, is not.
    let assertion = fx.captured()?;
    let app = fx.app_token(APP)?;
    fx.svc
        .get_scale_history(on_behalf(
            GetScaleHistoryRequest {
                job: "web".into(),
                ..Default::default()
            },
            &app,
            &assertion,
        ))
        .await?;
    let elsewhere = fx
        .svc
        .pause_scaling(on_behalf(Fixture::pause_request("api"), &app, &assertion))
        .await
        .unwrap_err();
    assert_eq!(elsewhere.code(), Code::PermissionDenied, "{elsewhere}");
    assert!(
        elsewhere
            .message()
            .contains("scale:write is outside what alice via app deleg delegates"),
        "{elsewhere}"
    );
    let stop = fx
        .svc
        .stop_job(on_behalf(
            StopJobRequest {
                name: "web".into(),
                ..Default::default()
            },
            &app,
            &assertion,
        ))
        .await
        .unwrap_err();
    assert_eq!(stop.code(), Code::PermissionDenied, "{stop}");
    assert!(stop.message().contains("job:write"), "{stop}");
    // The app's own identity could pause api; the delegation cannot widen
    // into it, and without the header the app is itself again.
    fx.svc
        .pause_scaling(req(Fixture::pause_request("api"), &app))
        .await?;

    // 3. Only `deleg` may present it: not the other app, a management
    // token, alice's own token, or nobody.
    let other = fx
        .svc
        .pause_scaling(on_behalf(
            Fixture::pause_request("web"),
            &fx.app_token(OTHER_APP)?,
            &assertion,
        ))
        .await
        .unwrap_err();
    assert_eq!(other.code(), Code::Unauthenticated, "{other}");
    assert!(other.message().contains("for app other"), "{other}");
    for (who, token) in [("management", fx.admin.as_str()), ("alice", &alice.secret)] {
        let refused = fx
            .svc
            .pause_scaling(on_behalf(Fixture::pause_request("web"), token, &assertion))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::PermissionDenied, "{who}: {refused}");
        assert!(
            refused.message().contains("only an app's own identity"),
            "{who}: {refused}"
        );
    }
    let anonymous = fx
        .svc
        .pause_scaling(on_behalf(Fixture::pause_request("web"), "", &assertion))
        .await
        .unwrap_err();
    assert_eq!(anonymous.code(), Code::Unauthenticated, "{anonymous}");
    // Garbage in the header never falls back to the app's own identity.
    let garbage = fx
        .svc
        .pause_scaling(on_behalf(Fixture::pause_request("web"), &app, "not.a.jwt"))
        .await
        .unwrap_err();
    assert_eq!(garbage.code(), Code::Unauthenticated, "{garbage}");

    // 7. Streams take the caller's own credential.
    let stream = fx
        .svc
        .watch_scaling_policies(on_behalf(
            WatchScalingPoliciesRequest::default(),
            &app,
            &assertion,
        ))
        .await
        .err()
        .context("a delegated stream is refused")?;
    assert_eq!(stream.code(), Code::PermissionDenied, "{stream}");
    Ok(())
}

/// Scenarios 4 and 5: an expired assertion, and a revoked viewer, end the
/// delegation at once.
#[tokio::test(flavor = "multi_thread")]
async fn an_expired_or_revoked_delegation_is_refused_test() -> R<()> {
    let fx = fixture().await?;
    let alice = fx.alice().await?;
    let app = fx.app_token(APP)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    // A fresh one works, then the same claim expired: long ago, and five
    // seconds ago (inside the JWT library's default leeway).
    let fresh = fx.sign(Some(&alice.accessor), now, now + 60)?;
    fx.svc
        .pause_scaling(on_behalf(Fixture::pause_request("web"), &app, &fresh))
        .await?;
    for (iat, exp) in [(now - 300, now - 240), (now - 65, now - 5)] {
        let expired = fx.sign(Some(&alice.accessor), iat, exp)?;
        let refused = fx
            .svc
            .pause_scaling(on_behalf(Fixture::pause_request("web"), &app, &expired))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::Unauthenticated, "{refused}");
        assert!(
            refused.message().to_lowercase().contains("expired"),
            "{refused}"
        );
    }

    // Alice loses `scale:write`: the delegation she pressed is refused,
    // though the assertion has most of its minute left.
    fx.svc
        .upsert_acl_policy(req(
            UpsertAclPolicyRequest {
                name: "alice".into(),
                description: String::new(),
                rules_json: json!([
                    { "resources": ["app"], "actions": ["read"], "names": [APP] },
                    { "resources": ["scale"], "actions": ["read"], "namespaces": ["default"] },
                ])
                .to_string(),
            },
            &fx.admin,
        ))
        .await?;
    let lost = fx
        .svc
        .pause_scaling(on_behalf(Fixture::pause_request("web"), &app, &fresh))
        .await
        .unwrap_err();
    assert_eq!(lost.code(), Code::PermissionDenied, "{lost}");
    assert!(
        lost.message().contains("no longer holds scale:write"),
        "{lost}"
    );

    // Her token revoked: refused at once.
    fx.svc
        .delete_acl_token(req(
            DeleteAclTokenRequest {
                accessor_id: alice.accessor.clone(),
            },
            &fx.admin,
        ))
        .await?;
    let revoked = fx
        .svc
        .pause_scaling(on_behalf(Fixture::pause_request("web"), &app, &fresh))
        .await
        .unwrap_err();
    assert_eq!(revoked.code(), Code::Unauthenticated, "{revoked}");
    assert!(revoked.message().contains("was revoked"), "{revoked}");
    Ok(())
}

/// Scenario 6: a delegation never exceeds the app's own grant. Bob may
/// pause in `team-b`, and the manager lets him press there; the app's grant
/// covers only `default`, so its call as Bob is refused, and nothing is
/// paused.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegation_never_exceeds_the_apps_grant_test() -> R<()> {
    let fx = fixture().await?;
    let bob = fx
        .token(
            "bob",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": [APP] },
                { "resources": ["scale"], "actions": ["read", "write"],
                  "namespaces": ["default", "team-b"] },
            ]),
        )
        .await?;
    let refused = fx.press(&bob.secret, "team-b/other").await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert!(
        refused
            .message()
            .contains("app deleg's own grant does not hold scale:write on team-b/other"),
        "{refused}"
    );
    assert!(
        !fx.events("scale.paused")?
            .iter()
            .any(|message| message.contains("other")),
        "nothing was paused in team-b"
    );
    // In `default`, both hold it: the press works.
    let reply = fx.press(&bob.secret, "web").await?;
    assert!(
        reply.message.starts_with("bob via app deleg"),
        "{}",
        reply.message
    );
    Ok(())
}
