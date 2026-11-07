//! The autoscaler's UI, built as app views (autoscaler-v2 plan §6.14,
//! Phase 6, §12 P6; docs/autoscaling.md, "In the UI").
//!
//! In process, against a manager service with ACLs and workload identity on:
//! the bundled autoscaler app seeded and placed on a manager node, its real
//! views (`legion::apps::autoscaler_ui`) served by the app framework on
//! loopback, and a forwarder that delivers to them as a worker does. The
//! views call Legion with the app's own `legion` token, as its API socket
//! would attach it, which holds exactly the autoscaler's grant (its
//! manifest's `permissions`), so a view that reads beyond it fails here.
//! The engine runs a tick against the same service, so the job panel draws
//! real status and the evaluation log.
//!
//! - P6 acceptance: the job page shows the Autoscaling panel for an
//!   autoscaled job; it renders nothing for a viewer without `scale:read` on
//!   that job (the app sees no request); the same view renders in the
//!   terminal (`legion app view autoscaler job web`) and over MCP.
//! - The task badge, the sidebar page filtered by the viewer's namespaces,
//!   and the node-group panel (hidden for a node in no group).
//! - The Pause, Resume, and Release actions act on the job in their context,
//!   as the viewer: recorded by `alice via app autoscaler`
//!   (app-actions-as-viewer plan); the app plane refuses a delegated call.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::apps::autoscaler::{Engine, EngineOptions};
use legion::apps::autoscaler_ui::AutoscalerUi;
use legion::apps::ui::{AssertionVerifier, ClusterKeys, StaticKeys, UiRegistry, UiServer};
use legion::cmd::helpers::{
    connect_manager_client_with_token, manager_connection_with_token, ClientTimeouts,
};
use legion::manager::acl::Authenticator;
use legion::manager::app_forward::AppForwarder;
use legion::manager::apps::{AppReconciler, PassOutcome, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::app_ui::ON_BEHALF_OF_HEADER;
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
const APP: &str = "autoscaler";
const MANAGER_NODE: &str = "mgr-1";
const T0: i64 = 1_800_000_000_000;

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

fn timeouts() -> ClientTimeouts {
    ClientTimeouts {
        connect: Some(Duration::from_secs(5)),
        request: Some(Duration::from_secs(30)),
    }
}

/// A worker with four 2400 MHz cores in pool `default`, with `attrs`.
fn worker(id: &str, attrs: Value) -> NodeMsg {
    let mut config = json!({ "maxAllocations": 20 });
    if let (Some(config), Some(attrs)) = (config.as_object_mut(), attrs.as_object()) {
        for (key, value) in attrs {
            config.insert(key.clone(), value.clone());
        }
    }
    NodeMsg {
        id: id.into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: config.to_string(),
        fingerprint_attrs: json!({
            "memory_total": (16384u64 * 1024 * 1024).to_string(),
            "cpus": {
                "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" },
                "cpu-3": { "frequency": "2400" }, "cpu-4": { "frequency": "2400" }
            }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

/// The manager's system worker, on the build the reconciler runs.
fn manager_node() -> NodeMsg {
    NodeMsg {
        id: MANAGER_NODE.into(),
        pool: "legion-system".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({
            "legion.role": "manager",
            "namespaces": ["legion-system"],
            "maxAllocations": 10,
            "resources": { "memory": 65536, "cpu": 40000 },
        })
        .to_string(),
        fingerprint_attrs: json!({
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
        })
        .to_string(),
        dynamic_attrs: json!({ "buildVersion": "0.1.0", "buildId": "a" }).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

/// Delivers what the manager forwards to the autoscaler's UI port, the way
/// a worker does.
#[derive(Debug)]
struct LoopbackForwarder {
    addr: std::net::SocketAddr,
    forwarded: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl AppForwarder for LoopbackForwarder {
    async fn forward(
        &self,
        _store: &Store,
        node_id: &str,
        request: ForwardAppRequestRequest,
    ) -> Result<ForwardAppRequestReply, tonic::Status> {
        assert_eq!(node_id, MANAGER_NODE);
        legion::worker::app_forward::check_request(&request)?;
        self.forwarded.fetch_add(1, Ordering::SeqCst);
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
    addr: String,
    app: Arc<UiServer>,
    forwarded: Arc<AtomicU64>,
    _server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self._server.abort();
    }
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

    fn drain_evals(&self) -> R<()> {
        while let Some(eval) = self.store.claim_eval()? {
            let ok = eval.handle(&self.store).is_ok();
            self.store.complete_eval(&eval.id, ok)?;
        }
        Ok(())
    }

    async fn run(&self, spec: Value) -> R<()> {
        self.svc
            .run_job(req(
                RunJobRequest {
                    parsed_jobspec: spec.to_string(),
                    ..Default::default()
                },
                &self.admin,
            ))
            .await
            .map_err(|status| anyhow!("RunJob: {}", status.message()))?;
        self.drain_evals()
    }

    async fn view(
        &self,
        token: &str,
        view: &str,
        context: AppViewContext,
    ) -> Result<GetAppViewReply, tonic::Status> {
        self.svc
            .get_app_view(req(
                GetAppViewRequest {
                    app: APP.into(),
                    view: view.into(),
                    context: Some(context),
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn job_panel(&self, token: &str, job: &str) -> Result<Value, tonic::Status> {
        let reply = self.view(token, "job", job_context(job)).await?;
        Ok(serde_json::from_str(&reply.view_json).expect("view json"))
    }

    async fn act(
        &self,
        action: &str,
        inputs: Value,
    ) -> Result<InvokeAppActionReply, tonic::Status> {
        self.act_as(&self.admin, action, inputs).await
    }

    async fn act_as(
        &self,
        token: &str,
        action: &str,
        inputs: Value,
    ) -> Result<InvokeAppActionReply, tonic::Status> {
        self.svc
            .invoke_app_action(req(
                InvokeAppActionRequest {
                    app: APP.into(),
                    action: action.into(),
                    context: Some(job_context("web")),
                    inputs_json: inputs.to_string(),
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    /// The `legion` token the manager would issue the autoscaler's
    /// allocation: what its API socket attaches.
    fn app_token(&self) -> R<String> {
        let job = format!("legion-system/{APP}");
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

    async fn status(&self) -> R<ScalingTargetStatus> {
        self.svc
            .get_scaling_status(req(
                GetScalingStatusRequest {
                    job: "web".into(),
                    ..Default::default()
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .targets
            .into_iter()
            .next()
            .context("web is autoscaled")
    }
}

fn job_context(job: &str) -> AppViewContext {
    AppViewContext {
        slot: "job".into(),
        job: job.into(),
        ..Default::default()
    }
}

fn texts(view: &Value) -> String {
    view.to_string()
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-autoscaler-ui-")
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
    store.insert_node(manager_node())?;
    store.insert_node(worker("n0", json!({})))?;
    store.insert_node(worker(
        "n1",
        json!({
            "legion.nodegroup": "general",
            "legion.provider-id": "i-0abc",
            "legion.join": { "token": "jt-general", "attributes": { "legion.nodegroup": "general" } },
        }),
    ))?;
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
    // The app's `legion` token authenticates through this method, as on a
    // manager with workload identity on.
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

    // The autoscaler's UI port: its real views, verifying assertions
    // against the cluster's keys.
    let registry = Arc::new(UiRegistry::default());
    let ui = AutoscalerUi::new(T0);
    legion::apps::autoscaler_ui::register(&registry, ui.clone());
    let keys = ClusterKeys {
        issuer: state.issuer_url()?,
        jwks: state.jwks()?,
    };
    let app = Arc::new(UiServer::new(
        registry,
        Some(Arc::new(AssertionVerifier::new(
            APP,
            Arc::new(StaticKeys(keys)),
        ))),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let app_addr = listener.local_addr()?;
    tokio::spawn(legion::apps::ui::serve(listener, app.clone()));
    let forwarded = Arc::new(AtomicU64::new(0));
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth)
            .with_workload_identity(Some(state.clone()))
            .with_app_forwarder(Arc::new(LoopbackForwarder {
                addr: app_addr,
                forwarded: forwarded.clone(),
            })),
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
        addr,
        app,
        forwarded,
        _server: server,
    };

    // Seed the bundled autoscaler, as the first Control leader does, and
    // place its job on the manager.
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
    assert_eq!(reconciler.seed()?, vec![APP.to_string()]);
    let outcome = reconciler.reconcile_app(APP).await;
    assert!(
        matches!(outcome, PassOutcome::Submitted { .. }),
        "{outcome:?}"
    );
    fx.drain_evals()?;
    anyhow::ensure!(
        !fx.store
            .app_ui_allocations("legion-system/autoscaler")?
            .is_empty(),
        "the autoscaler's job should be placed"
    );

    // An autoscaled job and a plain one.
    fx.run(json!({
        "name": "web",
        "task": { "name": "web", "runtime": "exec", "count": 2,
                  "config": { "command": ["sleep", "3600"] },
                  "resources": { "cpu": 500, "memory": 64 },
                  "autoscale": true },
    }))
    .await?;
    fx.run(json!({
        "name": "db",
        "task": { "name": "db", "runtime": "exec", "count": 1,
                  "config": { "command": ["sleep", "3600"] },
                  "resources": { "cpu": 500, "memory": 64 } },
    }))
    .await?;

    // The app's two clients: the engine reports as a management caller (a
    // report is app-plane), and the views and actions call with the app's
    // own `legion` token, which holds exactly its grant; the actions' viewer
    // clients ride on the same connection.
    let connection = manager_connection_with_token(&fx.addr, &fx.app_token()?, timeouts())?;
    let mut engine_client = connect_manager_client_with_token(&fx.addr, &fx.admin, timeouts())?;
    let engine = Engine::new(
        engine_client.clone(),
        EngineOptions {
            instance: "test".into(),
            ..Default::default()
        },
        T0,
    );
    let mut watch = engine_client
        .watch_scaling_policies(WatchScalingPoliciesRequest::default())
        .await?
        .into_inner();
    let set = watch.message().await?.context("a policy set")?;
    engine.set_policies(set.policies);
    ui.connect(connection, engine.evaluations());
    let mut engine = engine;
    for tick in 0..3 {
        engine.tick(T0 + tick * 15_000).await;
    }
    Ok(fx)
}

/// P6 acceptance: the job page shows the Autoscaling panel for an
/// autoscaled job, with its bounds, the hour of evaluations, the decision
/// timeline, and Pause; a job without autoscaling shows nothing; a viewer
/// without `scale:read` on the job is not shown the panel, and the app sees
/// no request for them (plan §12 P6).
#[tokio::test(flavor = "multi_thread")]
async fn the_job_page_shows_the_autoscaling_panel_test() -> R<()> {
    let fx = fixture().await?;

    // Listed on web's page for the admin.
    let listed = fx
        .svc
        .list_ui_extensions(req(
            ListUiExtensionsRequest {
                slot: "job".into(),
                context: Some(job_context("web")),
                app: String::new(),
            },
            &fx.admin,
        ))
        .await?
        .into_inner()
        .extensions;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        (listed[0].app.as_str(), listed[0].title.as_str()),
        (APP, "Autoscaling")
    );
    assert_eq!(listed[0].refresh_seconds, 10);

    let panel = fx.job_panel(&fx.admin, "web").await?;
    assert_eq!(panel["title"], "Autoscaling", "{panel}");
    let blocks = panel["blocks"].as_array().context("blocks")?;
    let stats = &blocks
        .iter()
        .find(|block| block["type"] == "stats")
        .context("stats")?["items"];
    let stat = |label: &str| {
        stats
            .as_array()
            .and_then(|items| items.iter().find(|item| item["label"] == label))
            .map(|item| item["value"].as_str().unwrap_or_default().to_string())
            .unwrap_or_default()
    };
    assert_eq!(stat("Now"), "2", "{panel}");
    assert_eq!(stat("Bounds"), "2..8", "{panel}");
    assert_eq!(stat("Mode"), "enforce", "{panel}");
    let series = blocks
        .iter()
        .find(|block| block["type"] == "series")
        .context("the hour of evaluations")?;
    assert_eq!(
        series["series"][0]["points"].as_array().map(Vec::len),
        Some(3),
        "{series}"
    );
    assert!(blocks.iter().any(|block| block["type"] == "timeline"));
    let actions = blocks
        .iter()
        .find(|block| block["type"] == "actions")
        .context("actions")?;
    assert_eq!(actions["items"][0]["action"], "pause", "{actions}");

    // Not autoscaled: the panel draws nothing.
    let plain = fx.job_panel(&fx.admin, "db").await?;
    assert_eq!(plain["hidden"], true, "{plain}");

    // A viewer who may read jobs and the app, but not scaling: no panel,
    // refused, and the app saw nothing.
    let reader = fx
        .token(
            "reader",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": [APP] },
                { "resources": ["job"], "actions": ["read"], "namespaces": ["*"] },
            ]),
        )
        .await?;
    let seen = fx.app.requests_seen();
    let forwarded = fx.forwarded.load(Ordering::SeqCst);
    let listed = fx
        .svc
        .list_ui_extensions(req(
            ListUiExtensionsRequest {
                slot: "job".into(),
                context: Some(job_context("web")),
                app: String::new(),
            },
            &reader,
        ))
        .await?
        .into_inner()
        .extensions;
    assert!(listed.is_empty(), "{listed:?}");
    let denied = fx.job_panel(&reader, "web").await.unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    let denied = fx
        .view(
            &reader,
            "task-badge",
            AppViewContext {
                slot: "task".into(),
                job: "web".into(),
                task: "web".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    assert_eq!(fx.app.requests_seen(), seen, "the app saw a request");
    assert_eq!(fx.forwarded.load(Ordering::SeqCst), forwarded);

    // The task badge.
    let badge = fx
        .view(
            &fx.admin,
            "task-badge",
            AppViewContext {
                slot: "task".into(),
                job: "web".into(),
                task: "web".into(),
                ..Default::default()
            },
        )
        .await?;
    let badge: Value = serde_json::from_str(&badge.view_json)?;
    assert_eq!(badge["badge"]["text"], "auto 2 · 2..8", "{badge}");
    Ok(())
}

/// The sidebar page lists the policies the viewer may read, and the
/// node-group panel shows a node's group (and nothing for a node in none).
#[tokio::test(flavor = "multi_thread")]
async fn the_sidebar_page_and_the_node_group_panel_test() -> R<()> {
    let fx = fixture().await?;
    let nav = AppViewContext {
        slot: "nav".into(),
        ..Default::default()
    };
    let page = fx.view(&fx.admin, "policies", nav.clone()).await?;
    let page: Value = serde_json::from_str(&page.view_json)?;
    assert!(texts(&page).contains("/jobs/default/web"), "{page}");
    assert!(
        !texts(&page).contains("db/db"),
        "db is not autoscaled: {page}"
    );

    // A viewer with scale:read only in team-* namespaces sees the page, but
    // no row from `default`.
    let team = fx
        .token(
            "team",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": [APP] },
                { "resources": ["scale"], "actions": ["read"], "namespaces": ["team-*"] },
            ]),
        )
        .await?;
    let theirs = fx.view(&team, "policies", nav).await?;
    let theirs: Value = serde_json::from_str(&theirs.view_json)?;
    let table = theirs["blocks"]
        .as_array()
        .and_then(|blocks| blocks.iter().find(|block| block["type"] == "table"))
        .context("table")?;
    assert_eq!(table["rows"].as_array().map(Vec::len), Some(0), "{theirs}");

    // The node-group panel.
    let node = |id: &str| AppViewContext {
        slot: "node".into(),
        node: id.into(),
        ..Default::default()
    };
    let panel = fx.view(&fx.admin, "node", node("n1")).await?;
    let panel: Value = serde_json::from_str(&panel.view_json)?;
    assert_eq!(panel["title"], "Node group general", "{panel}");
    for needle in [
        "jt-general",
        "i-0abc",
        "/nodes/n1",
        "No NodeGroup runs general",
    ] {
        assert!(texts(&panel).contains(needle), "{needle}: {panel}");
    }
    let none = fx.view(&fx.admin, "node", node("n0")).await?;
    assert_eq!(
        serde_json::from_str::<Value>(&none.view_json)?["hidden"],
        true
    );
    // node:read on another pool only: refused before the app is asked.
    let gpu = fx
        .token(
            "gpu",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": [APP] },
                { "resources": ["node"], "actions": ["read"], "pools": ["gpu"] },
            ]),
        )
        .await?;
    let denied = fx.view(&gpu, "node", node("n1")).await.unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    Ok(())
}

/// Pause, Resume, and Release act on the job the panel is for, as the
/// viewer: the pause and the released pin are recorded by `<viewer> via app
/// autoscaler`, not the app; Release with nothing pinned is refused with
/// the app's message.
#[tokio::test(flavor = "multi_thread")]
async fn pause_resume_and_release_act_on_the_panels_job_test() -> R<()> {
    let fx = fixture().await?;
    let paused = fx.act("pause", json!({ "for": "30m" })).await?;
    assert!(paused.message.contains("web"), "{}", paused.message);
    let target = fx.status().await?;
    let pause = target.pause.context("paused")?;
    assert_eq!(pause.by, "bootstrap via app autoscaler", "{pause:?}");
    assert_eq!(pause.reason, "paused from the UI", "{pause:?}");
    // The refreshed panel offers Resume, not Pause.
    let view: Value = serde_json::from_str(&paused.view_json)?;
    assert!(texts(&view).contains("\"resume\""), "{view}");
    assert!(!texts(&view).contains("\"pause\""), "{view}");

    fx.act("resume", json!({})).await?;
    assert!(fx.status().await?.pause.is_none());

    let refused = fx.act("release", json!({})).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert!(
        refused.message().contains("nothing to release"),
        "{refused}"
    );

    fx.svc
        .scale_task(req(
            ScaleTaskRequest {
                name: "web".into(),
                count: Some(3),
                pin_for_seconds: Some(600),
                reason: Some("launch".into()),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    assert!(fx.status().await?.pin.is_some());
    fx.act("release", json!({})).await?;
    assert!(fx.status().await?.pin.is_none());
    Ok(())
}

/// A viewer's press runs as the viewer (app-actions-as-viewer plan, §12):
/// Alice, who holds `scale` on `default` and `app:read` on the autoscaler,
/// pauses web from its panel, and the pause, its `scale.paused` event, and
/// a pin she releases are recorded by `alice via app autoscaler`, while the
/// `app.action` event names her. The app plane refuses the autoscaler's own
/// identity when it presents a viewer's assertion, and takes it without.
#[tokio::test(flavor = "multi_thread")]
async fn a_viewers_press_is_recorded_as_the_viewer_through_the_app_test() -> R<()> {
    let fx = fixture().await?;
    let alice = fx
        .token(
            "alice",
            json!([
                { "resources": ["app"], "actions": ["read"], "names": ["autoscaler"] },
                { "resources": ["scale"], "actions": ["read", "write"], "namespaces": ["default"] },
            ]),
        )
        .await?;
    fx.act_as(&alice, "pause", json!({ "for": "10m" })).await?;
    let pause = fx.status().await?.pause.context("paused")?;
    assert_eq!(pause.by, "alice via app autoscaler", "{pause:?}");
    let events = |kind: &str| {
        fx.store
            .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
                kind: Some(kind.into()),
                limit: Some(20),
                ..Default::default()
            })
            .map(|events| {
                events
                    .into_iter()
                    .map(|event| event.message)
                    .collect::<Vec<_>>()
            })
    };
    let paused = events("scale.paused")?;
    assert!(
        paused
            .iter()
            .any(|message| message.ends_with("by alice via app autoscaler")),
        "{paused:?}"
    );
    let pressed = events("app.action")?;
    assert!(
        pressed
            .iter()
            .any(|message| message.contains(": alice ran pause on web: ok")),
        "{pressed:?}"
    );
    fx.act_as(&alice, "resume", json!({})).await?;
    assert!(fx.status().await?.pause.is_none());

    fx.svc
        .scale_task(req(
            ScaleTaskRequest {
                name: "web".into(),
                count: Some(3),
                pin_for_seconds: Some(600),
                reason: Some("launch".into()),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    fx.act_as(&alice, "release", json!({})).await?;
    let history = fx
        .svc
        .get_scale_history(req(
            GetScaleHistoryRequest {
                job: "web".into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?
        .into_inner()
        .entries;
    let released = history
        .iter()
        .find(|entry| entry.kind == "released")
        .context("the release is in history")?;
    assert_eq!(
        released.principal, "alice via app autoscaler",
        "{released:?}"
    );
    assert!(!released.summary.contains("by alice"), "{released:?}");

    // The app plane: the autoscaler's own identity reports; the same
    // identity presenting a viewer's assertion is refused.
    let app = fx.app_token()?;
    let report = ReportScalingStatusRequest {
        instance: "probe".into(),
        ..Default::default()
    };
    fx.svc
        .report_scaling_status(req(report.clone(), &app))
        .await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let claim = legion::structs::app_ui::ViewerAssertion {
        kind: legion::structs::app_ui::AssertionKind::Action,
        app: APP.into(),
        name: "pause".into(),
        principal: "alice".into(),
        accessor: None,
        permission: Some("scale:write".into()),
        scope: legion::structs::app_ui::AssertionScope::Context,
        namespaces: Vec::new(),
        context: legion::structs::app_ui::ViewContext {
            slot: "job".into(),
            namespace: "default".into(),
            job: "web".into(),
            ..Default::default()
        },
        inputs: Some(String::new()),
        delegation: Some(
            legion::structs::app_ui::Delegation::new(
                legion::structs::acl::AclResource::Scale,
                &[legion::structs::acl::AclAction::Write],
                &legion::structs::acl::AclScope::job("web"),
            )
            .map_err(|err| anyhow!(err))?,
        ),
    };
    let mut claims = serde_json::Map::new();
    claims.insert(
        legion::structs::app_ui::ASSERTION_CLAIM.into(),
        serde_json::to_value(&claim)?,
    );
    let assertion = fx
        .state
        .issue_viewer_assertion(APP, "va_probe", "principal:alice", claims, now, now + 60)?
        .token;
    let mut delegated = req(report, &app);
    delegated
        .metadata_mut()
        .insert(ON_BEHALF_OF_HEADER, assertion.parse()?);
    let refused = fx.svc.report_scaling_status(delegated).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert!(
        refused.message().contains("legion-on-behalf-of"),
        "{refused}"
    );
    Ok(())
}

/// The same panel in the terminal (`legion app view autoscaler job web`)
/// and over MCP (`legion_app_view`), against the service on a port (plan
/// §12 P6).
#[tokio::test(flavor = "multi_thread")]
async fn the_panel_renders_in_the_terminal_and_over_mcp_test() -> R<()> {
    let fx = fixture().await?;
    let home = tempfile::tempdir_in(crate::support::scratch_root())?;
    let run = |args: Vec<&'static str>, stdin: Option<String>| {
        let addr = fx.addr.clone();
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
    assert!(text.contains("Autoscaling"), "{text} / {stderr}");
    assert!(text.contains("Bounds: 2..8"), "{text}");
    assert!(text.contains("Now: 2"), "{text}");
    assert!(text.contains("pause (Pause autoscaling)"), "{text}");

    let (json_out, stderr) = run(vec!["app", "view", APP, "job", "web", "-j"], None).await??;
    let envelope: Value =
        serde_json::from_str(json_out.trim()).with_context(|| format!("{json_out} / {stderr}"))?;
    assert_eq!(envelope["ok"], true, "{envelope}");
    assert_eq!(envelope["data"]["document"]["title"], "Autoscaling");

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
    assert_eq!(data["view"], "job");
    assert_eq!(data["document"]["title"], "Autoscaling");
    Ok(())
}
