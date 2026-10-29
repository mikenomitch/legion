//! App UI extensions, tier 1 (apps plan §6.5, §9 A4, §12 A4; docs/apps.md,
//! "Views and actions").
//!
//! In process, against a manager service with ACLs and workload identity on,
//! a fake app built on the app framework (`legion::apps::ui`) serving its UI
//! port on loopback, and a forwarder that delivers to it the way a worker
//! does (the worker's request checks and HTTP exchange, minus the
//! worker-plane hop, which `worker_plane_classes` and the forwarder's own
//! tests cover):
//!
//! - scenario C: one view and one action, through `ListUiExtensions`,
//!   `GetAppView`, and `InvokeAppAction`, then `legion app view` and the
//!   `legion_app_view` MCP tool against the same service on a port;
//! - a viewer without the declared permission gets no forward: the app sees
//!   zero requests, and a denied action is still audited;
//! - the app refuses requests the manager did not sign for it, when called
//!   directly on its UI port: no assertion, an expired one, another app's,
//!   another context's;
//! - actions are audited and retry-safe; a view that fails the schema is
//!   refused by the manager.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::apps::ui::{
    ActionOutcome, ActionRequest, AssertionVerifier, ClusterKeys, Refused, StaticKeys, UiRegistry,
    UiServer, ViewRequest,
};
use legion::manager::acl::Authenticator;
use legion::manager::app_forward::AppForwarder;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::WorkloadIdentityState;
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::app_ui::{
    AppView, AssertionKind, AssertionScope, Block, NoticeBlock, Stat, StatsBlock, Tone,
    ViewContext, ViewerAssertion, ASSERTION_CLAIM,
};
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
const APP: &str = "fake";
const NODE: &str = "mgr-1";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

/// The fake app's manifest: a nav page, a job panel needing `job:read`, and
/// one action needing `job:write`.
fn manifest() -> Value {
    json!({
        "name": APP,
        "version": "1.0.0",
        "description": "A fake app with one view and one action.",
        "job": { "task": { "name": "main", "runtime": "exec",
                            "config": { "command": ["sleep", "3600"] },
                            "network": { "ports": [{ "name": "api" }] } } },
        "ui": {
            "nav": [{ "title": "Fake", "icon": "gauge", "view": "overview" }],
            "job": [{ "title": "Fake panel", "view": "job",
                      "permission": { "resource": "job", "action": "read" } }],
            "actions": [{ "name": "bump", "title": "Bump", "confirm": "Bump {job}?",
                          "inputs": { "by": { "type": "number", "default": 1 } },
                          "permission": { "resource": "job", "action": "write" } }],
            "refresh": "10s",
        },
    })
}

/// The fake app: its UI server and how many actions ran.
struct FakeApp {
    server: Arc<UiServer>,
    addr: std::net::SocketAddr,
    actions_run: Arc<AtomicU64>,
}

/// Serve the fake app on loopback, verifying assertions against `state`'s
/// keys. `job` answers a stats block and an actions block; `bump` counts.
async fn fake_app(state: &WorkloadIdentityState) -> R<FakeApp> {
    let registry = Arc::new(UiRegistry::default());
    registry.view("overview", |_request: ViewRequest| async move {
        Ok(AppView::new(vec![Block::Notice(NoticeBlock {
            tone: Tone::Info,
            title: None,
            text: "Fake runs.".into(),
        })]))
    });
    registry.view("job", |request: ViewRequest| async move {
        if request.context.job == "hidden" {
            return Ok(AppView::hidden());
        }
        let mut view = AppView::new(vec![
            Block::Stats(StatsBlock {
                title: None,
                items: vec![
                    Stat {
                        label: "Job".into(),
                        value: request.context.job_id().unwrap_or_default(),
                        tone: None,
                        hint: None,
                    },
                    Stat {
                        label: "Viewer".into(),
                        value: request.viewer.principal.clone(),
                        tone: Some(Tone::Ok),
                        hint: None,
                    },
                ],
            }),
            serde_json::from_value(json!({ "type": "actions",
                "items": [{ "action": "bump", "label": "Bump by 2", "inputs": { "by": 2 } }] }))?,
        ]);
        view.title = Some("Fake panel".into());
        Ok(view)
    });
    let actions_run = Arc::new(AtomicU64::new(0));
    let counter = actions_run.clone();
    registry.action("bump", move |request: ActionRequest| {
        let counter = counter.clone();
        async move {
            if request.context.job == "refuse" {
                return Err(anyhow::Error::new(Refused("nothing to bump here".into())));
            }
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(ActionOutcome::done(format!(
                "bumped {} by {}",
                request.context.job_id().unwrap_or_default(),
                request.inputs["by"]
            )))
        }
    });
    let keys = ClusterKeys {
        issuer: state.issuer_url()?,
        jwks: state.jwks()?,
    };
    let verifier = AssertionVerifier::new(APP, Arc::new(StaticKeys(keys)));
    let server = Arc::new(UiServer::new(registry, Some(Arc::new(verifier))));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(legion::apps::ui::serve(listener, server.clone()));
    Ok(FakeApp {
        server,
        addr,
        actions_run,
    })
}

/// Delivers what the manager forwards to the fake app's port, the way a
/// worker does: its request checks, then one HTTP/1.1 exchange. Or, when
/// `canned` is set, answers it without asking the app.
#[derive(Debug)]
struct LoopbackForwarder {
    addr: std::net::SocketAddr,
    forwarded: Arc<AtomicU64>,
    canned: std::sync::Mutex<Option<(u32, String)>>,
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
        self.forwarded.fetch_add(1, Ordering::SeqCst);
        if let Some((status, body)) = self.canned.lock().unwrap().clone() {
            return Ok(ForwardAppRequestReply {
                status,
                content_type: "application/json".into(),
                body: body.into_bytes(),
            });
        }
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
    app: FakeApp,
    forwarder: Arc<LoopbackForwarder>,
}

impl Fixture {
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

    async fn view(
        &self,
        token: &str,
        slot: &str,
        job: &str,
        view: &str,
    ) -> Result<GetAppViewReply, tonic::Status> {
        self.svc
            .get_app_view(req(
                GetAppViewRequest {
                    app: APP.into(),
                    view: view.into(),
                    context: Some(AppViewContext {
                        slot: slot.into(),
                        job: job.into(),
                        ..Default::default()
                    }),
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn bump(
        &self,
        token: &str,
        job: &str,
        inputs: Value,
        key: Option<&str>,
    ) -> Result<InvokeAppActionReply, tonic::Status> {
        self.svc
            .invoke_app_action(req(
                InvokeAppActionRequest {
                    app: APP.into(),
                    action: "bump".into(),
                    context: Some(AppViewContext {
                        slot: "job".into(),
                        job: job.into(),
                        ..Default::default()
                    }),
                    inputs_json: inputs.to_string(),
                    idempotency_key: key.map(str::to_string),
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn extensions(
        &self,
        token: &str,
        context: Option<AppViewContext>,
    ) -> Result<Vec<UiExtension>, tonic::Status> {
        self.svc
            .list_ui_extensions(req(
                ListUiExtensionsRequest {
                    slot: context
                        .as_ref()
                        .map(|context| context.slot.clone())
                        .unwrap_or_default(),
                    context,
                    app: String::new(),
                },
                token,
            ))
            .await
            .map(|reply| reply.into_inner().extensions)
    }

    fn action_events(&self) -> R<Vec<legion::storage::events::SchedulerEventRecord>> {
        self.store
            .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
                kind: Some("app.action".into()),
                limit: Some(100),
                ..Default::default()
            })
    }

    /// Sign an assertion with the cluster's key, as the manager would (for
    /// calling the app's port directly).
    fn sign(&self, audience_app: &str, claim: &ViewerAssertion, iat: u64, exp: u64) -> R<String> {
        let mut claims = serde_json::Map::new();
        claims.insert(ASSERTION_CLAIM.into(), serde_json::to_value(claim)?);
        Ok(self
            .state
            .issue_viewer_assertion(audience_app, "va_test", "accessor:x", claims, iat, exp)?
            .token)
    }
}

fn job_context(job: &str) -> AppViewContext {
    AppViewContext {
        slot: "job".into(),
        job: job.into(),
        ..Default::default()
    }
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-app-views-")
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
    let auth = Arc::new(Authenticator::from_config(
        &AclConfig {
            enabled: Some(true),
            ..Default::default()
        }
        .with_defaults(),
    )?);
    let (_, admin) = auth
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .context("a fresh store has no management token")?;
    let app = fake_app(&state).await?;
    let forwarder = Arc::new(LoopbackForwarder {
        addr: app.addr,
        forwarded: Arc::new(AtomicU64::new(0)),
        canned: std::sync::Mutex::new(None),
    });
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth)
            .with_workload_identity(Some(state.clone()))
            .with_app_forwarder(forwarder.clone()),
    );
    let fx = Fixture {
        svc,
        store,
        state,
        admin,
        app,
        forwarder,
    };
    // Install the app, let the reconciler submit its job, and place it.
    fx.svc
        .install_app(req(
            InstallAppRequest {
                manifest_json: manifest().to_string(),
                source: "file".into(),
                source_ref: "./fake.json".into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let mut reconciler = AppReconciler::new(
        fx.svc.clone(),
        ReconcilerSettings {
            app_user: "nobody".into(),
            seed: BTreeMap::new(),
            own_build: BuildMarker {
                version: "0.1.0".into(),
                build_id: "a".into(),
            },
        },
    );
    let outcome = reconciler.reconcile_app(APP).await;
    assert!(
        matches!(
            outcome,
            legion::manager::apps::PassOutcome::Submitted { .. }
        ),
        "{outcome:?}"
    );
    let config_id = fx.store.get_job_config("legion-system/fake")?.id();
    fx.store.enqueue_eval(
        "job_reschedule",
        Some("legion-system/fake"),
        Some(&config_id),
        None,
        None,
    )?;
    while let Some(eval) = fx.store.get_next_eval()? {
        let outcome = eval.handle(&fx.store);
        fx.store.set_eval_status(
            &eval.id,
            if outcome.is_ok() {
                "complete"
            } else {
                "failed"
            },
        )?;
    }
    let placed = fx.store.app_ui_allocations("legion-system/fake")?;
    assert!(!placed.is_empty(), "the fake app's job should be placed");
    Ok(fx)
}

/// Scenario C through the RPCs: the registry, the view with its action, the
/// action run once for two calls with one key, and its audit event.
#[tokio::test(flavor = "multi_thread")]
async fn scenario_c_one_view_and_one_action_through_the_rpcs_test() -> R<()> {
    let fx = fixture().await?;

    // The registry at login: the nav page and the job panel.
    let all = fx.extensions(&fx.admin, None).await?;
    let slots: Vec<(&str, &str)> = all
        .iter()
        .map(|extension| (extension.slot.as_str(), extension.view.as_str()))
        .collect();
    assert_eq!(slots, vec![("nav", "overview"), ("job", "job")]);
    assert_eq!(all[0].refresh_seconds, 10);
    assert_eq!(all[0].icon, "gauge");

    // The job panel, for default/web.
    let reply = fx.view(&fx.admin, "job", "web", "").await?;
    assert_eq!(reply.view, "job");
    assert_eq!(reply.node_id, NODE);
    let view: Value = serde_json::from_str(&reply.view_json)?;
    assert_eq!(view["view"], "legion-view/1");
    assert_eq!(view["title"], "Fake panel");
    assert_eq!(view["blocks"][0]["items"][0]["value"], "web");
    assert_eq!(view["blocks"][1]["type"], "actions");
    assert_eq!(reply.actions.len(), 1);
    let action = &reply.actions[0];
    assert_eq!(action.name, "bump");
    assert!(action.allowed && action.mutating);
    assert_eq!(action.confirm, "Bump web?");
    let context = reply.context.clone().context("context")?;
    assert_eq!(
        (context.namespace.as_str(), context.job.as_str()),
        ("default", "web")
    );

    // `{ "hidden": true }` passes through.
    let hidden = fx.view(&fx.admin, "job", "hidden", "job").await?;
    assert_eq!(
        serde_json::from_str::<Value>(&hidden.view_json)?["hidden"],
        true
    );

    // The action: run once for two calls with one key.
    let ran = fx
        .bump(&fx.admin, "web", json!({ "by": 2 }), Some("bump-1"))
        .await?;
    assert_eq!(ran.message, "bumped web by 2");
    assert!(!ran.deduplicated);
    assert_eq!(
        serde_json::from_str::<Value>(&ran.inputs_json)?,
        json!({ "by": 2 })
    );
    let again = fx
        .bump(&fx.admin, "web", json!({ "by": 2 }), Some("bump-1"))
        .await?;
    assert!(again.deduplicated);
    assert_eq!(again.message, ran.message);
    assert_eq!(fx.app.actions_run.load(Ordering::SeqCst), 1);
    // Defaults fill in; unknown inputs are refused before the app is asked.
    assert_eq!(
        fx.bump(&fx.admin, "web", json!({}), None).await?.message,
        "bumped web by 1"
    );
    let seen = fx.app.server.requests_seen();
    let unknown = fx
        .bump(&fx.admin, "web", json!({ "job": "other" }), None)
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::InvalidArgument, "{unknown}");
    assert_eq!(fx.app.server.requests_seen(), seen);
    // The app refusing an action is FailedPrecondition with its message.
    let refused = fx
        .bump(&fx.admin, "refuse", json!({}), None)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert!(
        refused.message().contains("nothing to bump here"),
        "{refused}"
    );

    // Audited: who, what, where, and the outcome.
    let events = fx.action_events()?;
    let outcomes: Vec<String> = events
        .iter()
        .map(|event| {
            let meta: Value = serde_json::from_str(&event.metadata_json).unwrap_or_default();
            format!(
                "{} {} {}",
                meta["action"].as_str().unwrap_or_default(),
                meta["context"]["job"].as_str().unwrap_or_default(),
                meta["outcome"].as_str().unwrap_or_default()
            )
        })
        .collect();
    assert!(
        outcomes.contains(&"bump web ok".to_string()),
        "{outcomes:?}"
    );
    assert!(
        outcomes.contains(&"bump refuse failed".to_string()),
        "{outcomes:?}"
    );
    // Two runs with the key are one event; plus the default run.
    assert_eq!(
        outcomes.iter().filter(|o| *o == "bump web ok").count(),
        2,
        "{outcomes:?}"
    );
    assert!(events
        .iter()
        .all(|event| event.job_id.as_deref() == Some("legion-system/fake")));
    Ok(())
}

/// A viewer without the declared permission gets no forward: the app sees
/// zero requests, the panel is not listed for them, and a denied action is
/// audited (apps plan §9 A4, §12 A4).
#[tokio::test(flavor = "multi_thread")]
async fn a_viewer_without_the_permission_gets_no_forward_test() -> R<()> {
    let fx = fixture().await?;
    let team = fx
        .token(
            "team-a",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": [APP] },
                { "resources": ["job"], "actions": ["read"], "namespaces": ["team-a"] },
            ]),
        )
        .await?;
    let outsider = fx
        .token(
            "outsider",
            json!([{ "resources": ["job"], "actions": ["read", "write"], "namespaces": ["*"] }]),
        )
        .await?;
    let before = fx.app.server.requests_seen();
    let forwarded = fx.forwarder.forwarded.load(Ordering::SeqCst);

    // Listed only where it may be seen.
    let on_default = fx.extensions(&team, Some(job_context("web"))).await?;
    assert!(on_default.is_empty(), "{on_default:?}");
    let on_team = fx
        .extensions(&team, Some(job_context("team-a/web")))
        .await?;
    assert_eq!(on_team.len(), 1);
    // At login (no context) the job panel is listed: the token holds
    // job:read somewhere.
    assert_eq!(fx.extensions(&team, None).await?.len(), 2);

    // Not rendered for a job outside its namespaces, not run without
    // job:write, and nothing at all without app:read.
    let denied = fx.view(&team, "job", "web", "job").await.unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    let denied = fx
        .bump(&team, "team-a/web", json!({}), None)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    for status in [
        fx.view(&outsider, "job", "web", "job").await.unwrap_err(),
        fx.bump(&outsider, "web", json!({}), None)
            .await
            .unwrap_err(),
    ] {
        assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    }
    assert_eq!(
        fx.extensions(&outsider, None).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        fx.app.server.requests_seen(),
        before,
        "the app saw a request"
    );
    assert_eq!(fx.forwarder.forwarded.load(Ordering::SeqCst), forwarded);

    // The denied action is on the record; the one by a caller without
    // app:read is not (it never got past the app check).
    let events = fx.action_events()?;
    assert_eq!(events.len(), 1, "{events:?}");
    let meta: Value = serde_json::from_str(&events[0].metadata_json)?;
    assert_eq!(meta["outcome"], "denied");
    assert_eq!(meta["by"], "team-a");

    // The same viewer, on its own namespace's job, is forwarded.
    let reply = fx.view(&team, "job", "team-a/web", "").await?;
    assert_eq!(reply.view, "job");
    assert!(!reply.actions[0].allowed, "team-a may not write jobs");
    assert_eq!(fx.app.server.requests_seen(), before + 1);
    Ok(())
}

/// The app refuses what the manager did not sign for it, called directly
/// on its UI port: no assertion, a garbage one, an expired one, another
/// app's, another context's, a view assertion on an action (apps plan §9
/// A4).
#[tokio::test(flavor = "multi_thread")]
async fn the_app_refuses_direct_calls_without_a_matching_assertion_test() -> R<()> {
    let fx = fixture().await?;
    let get = |path: String, assertion: Option<String>| {
        let addr = fx.app.addr;
        async move {
            let stream = tokio::net::TcpStream::connect(addr).await?;
            let mut headers = Vec::new();
            if let Some(assertion) = assertion {
                headers.push(AppHttpHeader {
                    name: "legion-viewer".into(),
                    value: assertion,
                });
            }
            let reply = legion::worker::app_forward::exchange(
                stream,
                &ForwardAppRequestRequest {
                    allocation_id: 1,
                    job_id: "legion-system/fake".into(),
                    port: "api".into(),
                    method: if path.contains("/actions/") {
                        "POST"
                    } else {
                        "GET"
                    }
                    .into(),
                    path,
                    headers,
                    body:
                        json!({ "context": { "slot": "job", "namespace": "default", "job": "web" },
                                  "inputs": { "by": 1 } })
                        .to_string()
                        .into_bytes(),
                },
            )
            .await
            .map_err(|status| anyhow!("{status}"))?;
            Ok::<_, anyhow::Error>((
                reply.status,
                String::from_utf8_lossy(&reply.body).to_string(),
            ))
        }
    };
    let path = "/legion/v1/views/job?slot=job&namespace=default&job=web".to_string();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let claim = ViewerAssertion {
        kind: AssertionKind::View,
        app: APP.into(),
        name: "job".into(),
        principal: "mallory".into(),
        accessor: None,
        permission: Some("job:read".into()),
        scope: AssertionScope::Context,
        namespaces: Vec::new(),
        context: ViewContext {
            slot: "job".into(),
            namespace: "default".into(),
            job: "web".into(),
            ..Default::default()
        },
        inputs: None,
        delegation: None,
    };

    let (status, body) = get(path.clone(), None).await?;
    assert_eq!(status, 401, "{body}");
    assert!(body.contains("through the manager"), "{body}");
    assert_eq!(get(path.clone(), Some("not-a-jwt".into())).await?.0, 401);
    let expired = fx.sign(APP, &claim, now - 600, now - 540)?;
    assert_eq!(get(path.clone(), Some(expired)).await?.0, 401);
    let other_app = fx.sign("gitops", &claim, now, now + 60)?;
    assert_eq!(get(path.clone(), Some(other_app)).await?.0, 401);
    let valid = fx.sign(APP, &claim, now, now + 60)?;
    let (status, body) = get(
        "/legion/v1/views/job?slot=job&namespace=default&job=db".into(),
        Some(valid.clone()),
    )
    .await?;
    assert_eq!(status, 403, "{body}");
    let (status, body) = get("/legion/v1/actions/bump".into(), Some(valid.clone())).await?;
    assert_eq!(status, 403, "{body}");
    assert_eq!(fx.app.actions_run.load(Ordering::SeqCst), 0);
    // The valid one, on its own context, renders.
    let (status, body) = get(path, Some(valid)).await?;
    assert_eq!(status, 200, "{body}");
    Ok(())
}

/// The manager validates what an app answers: unknown blocks and raw HTML
/// never reach a viewer, and an app that refuses the assertion is reported
/// as the app's problem, not the viewer's.
#[tokio::test(flavor = "multi_thread")]
async fn the_manager_refuses_views_outside_the_schema_test() -> R<()> {
    let fx = fixture().await?;
    let answer = |status: u32, body: Value| {
        *fx.forwarder.canned.lock().unwrap() = Some((status, body.to_string()));
    };
    answer(
        200,
        json!({ "view": "legion-view/1", "blocks": [{ "type": "html", "html": "<script>" }] }),
    );
    let err = fx.view(&fx.admin, "job", "web", "job").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("unknown variant"), "{err}");
    answer(
        200,
        json!({ "view": "legion-view/1", "blocks": [{ "type": "markdown", "text": "<img src=x onerror=alert(1)>" }] }),
    );
    let err = fx.view(&fx.admin, "job", "web", "job").await.unwrap_err();
    assert!(err.message().contains("raw HTML"), "{err}");
    answer(
        200,
        json!({ "view": "legion-view/1", "blocks": [{ "type": "actions", "items": [{ "action": "drain" }] }] }),
    );
    let err = fx.view(&fx.admin, "job", "web", "job").await.unwrap_err();
    assert!(err.message().contains("does not declare"), "{err}");
    answer(401, json!({ "error": "bad assertion" }));
    let err = fx.view(&fx.admin, "job", "web", "job").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(
        err.message().contains("refused the viewer assertion"),
        "{err}"
    );
    // A view that is not registered is NotFound, before any forward.
    let forwarded = fx.forwarder.forwarded.load(Ordering::SeqCst);
    let err = fx
        .view(&fx.admin, "job", "web", "secret")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    // Actions are never pressed in the nav slot.
    let err = fx
        .svc
        .invoke_app_action(req(
            InvokeAppActionRequest {
                app: APP.into(),
                action: "bump".into(),
                context: Some(AppViewContext {
                    slot: "nav".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert_eq!(fx.forwarder.forwarded.load(Ordering::SeqCst), forwarded);
    Ok(())
}

/// Scenario C, from the outside: the same view through `legion app view`
/// (text and `-j`) and the `legion_app_view` MCP tool, against the service
/// on a port.
#[tokio::test(flavor = "multi_thread")]
async fn scenario_c_renders_in_the_terminal_and_over_mcp_test() -> R<()> {
    let fx = fixture().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let served = fx.svc.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ManagerServer::from_arc(served))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    let home = tempfile::tempdir_in(crate::support::scratch_root())?;
    let run = |args: Vec<&'static str>, stdin: Option<String>| {
        let addr = addr.clone();
        let admin = fx.admin.clone();
        let home = home.path().to_path_buf();
        tokio::task::spawn_blocking(move || -> R<(String, String)> {
            use std::io::Write;
            let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_legion"))
                .args(&args)
                .env("LEGION_ADDR", &addr)
                .env("LEGION_TOKEN", &admin)
                .env("HOME", &home)
                .env("LEGION_TOKEN_FILE", home.join("token"))
                .env_remove("LEGION_NAMESPACE")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()?;
            if let Some(input) = stdin {
                child
                    .stdin
                    .take()
                    .context("stdin")?
                    .write_all(input.as_bytes())?;
            }
            drop(child.stdin.take());
            let output = child.wait_with_output()?;
            Ok((
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        })
    };

    let (text, stderr) = run(vec!["app", "view", APP, "job", "web"], None).await??;
    assert!(text.contains("Fake panel"), "{text} / {stderr}");
    assert!(text.contains("Job: web"), "{text}");
    assert!(text.contains("Actions: Bump by 2"), "{text}");
    assert!(text.contains("bump (Bump): needs job:write"), "{text}");

    let (json_out, stderr) = run(vec!["app", "view", APP, "job", "web", "-j"], None).await??;
    let envelope: Value =
        serde_json::from_str(json_out.trim()).with_context(|| format!("{json_out} / {stderr}"))?;
    assert_eq!(envelope["ok"], true, "{envelope}");
    assert_eq!(envelope["data"]["document"]["view"], "legion-view/1");
    assert_eq!(envelope["data"]["actions"][0]["name"], "bump");

    let (listing, _) = run(vec!["app", "view", APP], None).await??;
    assert!(listing.contains("a Fake page in the sidebar"), "{listing}");
    assert!(listing.contains("needs job:read"), "{listing}");

    let call = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "legion_app_view",
                    "arguments": { "app": APP, "slot": "job", "job": "web" } },
    });
    let (mcp, stderr) = run(vec!["mcp", "serve"], Some(format!("{call}\n"))).await??;
    let line = mcp
        .lines()
        .find(|line| line.starts_with('{'))
        .with_context(|| format!("{mcp} / {stderr}"))?;
    let response: Value = serde_json::from_str(line)?;
    assert_ne!(response["result"]["isError"], true, "{response}");
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .context("text")?;
    let data: Value = serde_json::from_str(text)?;
    assert_eq!(data["document"], envelope["data"]["document"]);
    assert_eq!(data["view"], "job");
    Ok(())
}
