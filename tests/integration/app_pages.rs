//! App UI extensions, tier 2: sandboxed pages (apps plan §6.5, §9 A5;
//! docs/apps.md, "Sandboxed pages").
//!
//! In process, against a manager service with ACLs and workload identity on,
//! a fake app built on the app framework (`legion::apps::ui`) serving its
//! pages on loopback, and a forwarder that delivers to it the way a worker
//! does:
//!
//! - the page bundle is fetched through `GetAppPage` as the viewer: `app:read`,
//!   then the page's declared permission, before the app is asked; the reply
//!   carries the page's RPC allowlist, each marked read or write;
//! - the manager refuses an answer that is not UTF-8 HTML within 1 MiB, and
//!   the app refuses a page request without a page assertion;
//! - the server-side half of the allowlist: a call stamped
//!   `legion-app-page: <app>/<page>` is refused unless the page lists the
//!   RPC, against the app's current manifest (an upgrade that narrows `rpcs`
//!   takes effect at once).
//!
//! The browser half (the sandbox, the CSP, the bridge's own allowlist and
//! its confirmations) is covered by the SPA's Playwright smoke tests
//! (`ui/tests/smoke`).

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::apps::ui::{
    AssertionVerifier, ClusterKeys, PageRequest, StaticKeys, UiRegistry, UiServer,
};
use legion::manager::acl::Authenticator;
use legion::manager::app_forward::AppForwarder;
use legion::manager::app_page_guard::AppPageGuardLayer;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::WorkloadIdentityState;
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::app_ui::{
    page_context, AssertionKind, AssertionScope, ViewerAssertion, APP_PAGE_HEADER, ASSERTION_CLAIM,
    MAX_PAGE_BYTES,
};
use legion::structs::manager_client::ManagerClient;
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

/// The fake app's manifest at `version`: a console page that may read a
/// job's info and stop jobs (needing `job:read` to be seen), an open page,
/// and a page served from its own bundle path.
fn manifest(version: &str, console_rpcs: Value) -> Value {
    json!({
        "name": APP,
        "version": version,
        "description": "A fake app with sandboxed pages.",
        "job": { "task": { "name": "main", "runtime": "exec",
                            "config": { "command": ["sleep", "3600"] },
                            "network": { "ports": [{ "name": "api" }] } } },
        "ui": {
            "pages": [
                { "name": "console", "title": "Console", "icon": "terminal",
                  "rpcs": console_rpcs,
                  "permission": { "resource": "job", "action": "read" } },
                { "name": "open", "title": "Open page" },
                { "name": "graph", "title": "Graph", "bundle": "/legion/v1/graph.html" },
            ],
        },
    })
}

/// The fake app: its UI server and its address.
struct FakeApp {
    server: Arc<UiServer>,
}

async fn fake_app(state: &WorkloadIdentityState) -> R<(FakeApp, std::net::SocketAddr)> {
    let registry = Arc::new(UiRegistry::default());
    registry.page("console", |request: PageRequest| async move {
        Ok(format!(
            "<!doctype html><title>Console</title><p id=who>{}</p>\
             <script>legion.call('GetJobInfo', {{ name: 'web' }})</script>",
            request.viewer.principal
        ))
    });
    registry.page("open", |_request: PageRequest| async move {
        Ok("<p>open</p>".to_string())
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
    Ok((FakeApp { server }, addr))
}

/// Delivers what the manager forwards to the fake app's port, the way a
/// worker does, or answers with `canned` when set.
#[derive(Debug)]
struct LoopbackForwarder {
    addr: std::net::SocketAddr,
    forwarded: Arc<AtomicU64>,
    canned: std::sync::Mutex<Option<(u32, String, Vec<u8>)>>,
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
        if let Some((status, content_type, body)) = self.canned.lock().unwrap().clone() {
            return Ok(ForwardAppRequestReply {
                status,
                content_type,
                body,
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
    app_addr: std::net::SocketAddr,
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

    async fn page(&self, token: &str, page: &str) -> Result<GetAppPageReply, tonic::Status> {
        self.svc
            .get_app_page(req(
                GetAppPageRequest {
                    app: APP.into(),
                    page: page.into(),
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn nav(&self, token: &str) -> Result<Vec<UiExtension>, tonic::Status> {
        self.svc
            .list_ui_extensions(req(
                ListUiExtensionsRequest {
                    slot: "nav".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|reply| reply.into_inner().extensions)
    }

    fn canned(&self, status: u32, content_type: &str, body: Vec<u8>) {
        *self.forwarder.canned.lock().unwrap() = Some((status, content_type.into(), body));
    }
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-app-pages-")
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
    let (app, app_addr) = fake_app(&state).await?;
    let forwarder = Arc::new(LoopbackForwarder {
        addr: app_addr,
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
        app_addr,
        forwarder,
    };
    fx.svc
        .install_app(req(
            InstallAppRequest {
                manifest_json: manifest("1.0.0", json!(["GetJobInfo", "StopJob"])).to_string(),
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
    assert!(
        !fx.store
            .app_ui_allocations("legion-system/fake")?
            .is_empty(),
        "the fake app's job should be placed"
    );
    Ok(fx)
}

/// The page is listed in the sidebar and fetched through `GetAppPage` as
/// the viewer; the reply carries its allowlist, each RPC marked read or
/// write (apps plan §12 A5).
#[tokio::test(flavor = "multi_thread")]
async fn a_page_is_listed_and_fetched_as_the_viewer_test() -> R<()> {
    let fx = fixture().await?;
    let nav = fx.nav(&fx.admin).await?;
    let pages: Vec<(&str, &str, &str)> = nav
        .iter()
        .map(|entry| {
            (
                entry.page.as_str(),
                entry.title.as_str(),
                entry.view.as_str(),
            )
        })
        .collect();
    assert_eq!(
        pages,
        vec![
            ("console", "Console", ""),
            ("open", "Open page", ""),
            ("graph", "Graph", "")
        ]
    );
    assert_eq!(nav[0].icon, "terminal");
    assert_eq!(nav[0].slot, "nav");

    let reply = fx.page(&fx.admin, "console").await?;
    assert_eq!((reply.app.as_str(), reply.page.as_str()), (APP, "console"));
    assert_eq!(reply.title, "Console");
    assert_eq!(reply.node_id, NODE);
    let who = fx
        .svc
        .who_am_i(req(WhoAmIRequest::default(), &fx.admin))
        .await?
        .into_inner();
    assert!(
        reply
            .html
            .contains(&format!("<p id=who>{}</p>", who.principal)),
        "the app saw the viewer: {}",
        reply.html
    );
    let rpcs: Vec<(&str, bool)> = reply
        .rpcs
        .iter()
        .map(|rpc| (rpc.method.as_str(), rpc.mutating))
        .collect();
    assert_eq!(rpcs, vec![("GetJobInfo", false), ("StopJob", true)]);
    assert!(fx.page(&fx.admin, "open").await?.rpcs.is_empty());
    Ok(())
}

/// A viewer without the page's permission, or without `app:read`, is
/// refused before the app is asked; an undeclared page is NotFound before
/// any forward.
#[tokio::test(flavor = "multi_thread")]
async fn a_viewer_without_the_page_permission_gets_no_forward_test() -> R<()> {
    let fx = fixture().await?;
    let app_only = fx
        .token(
            "app-only",
            json!([{ "resources": ["app"], "actions": ["read"], "names": [APP] }]),
        )
        .await?;
    let outsider = fx
        .token(
            "outsider",
            json!([{ "resources": ["job"], "actions": ["read"], "namespaces": ["*"] }]),
        )
        .await?;
    let seen = fx.app.server.requests_seen();
    let forwarded = fx.forwarder.forwarded.load(Ordering::SeqCst);

    // Listed only where it may be seen: the console needs job:read.
    let listed: Vec<String> = fx
        .nav(&app_only)
        .await?
        .into_iter()
        .map(|entry| entry.page)
        .collect();
    assert_eq!(listed, vec!["open".to_string(), "graph".to_string()]);
    let denied = fx.page(&app_only, "console").await.unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    let denied = fx.page(&outsider, "open").await.unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    let missing = fx.page(&fx.admin, "secret").await.unwrap_err();
    assert_eq!(missing.code(), Code::NotFound, "{missing}");
    assert_eq!(fx.app.server.requests_seen(), seen, "the app saw a request");
    assert_eq!(fx.forwarder.forwarded.load(Ordering::SeqCst), forwarded);

    // The open page needs app:read only.
    assert_eq!(fx.page(&app_only, "open").await?.html, "<p>open</p>");
    Ok(())
}

/// The manager refuses an answer that is not a page: another content type,
/// past 1 MiB, not UTF-8, or the app refusing the assertion.
#[tokio::test(flavor = "multi_thread")]
async fn the_manager_refuses_answers_that_are_not_pages_test() -> R<()> {
    let fx = fixture().await?;
    fx.canned(
        200,
        "application/json",
        b"{\"view\":\"legion-view/1\"}".to_vec(),
    );
    let err = fx.page(&fx.admin, "graph").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("text/html"), "{err}");
    fx.canned(200, "text/html", vec![b'a'; MAX_PAGE_BYTES + 1]);
    let err = fx.page(&fx.admin, "graph").await.unwrap_err();
    assert!(err.message().contains("at most"), "{err}");
    fx.canned(200, "text/html; charset=utf-8", vec![0xff, 0xfe, 0x00]);
    let err = fx.page(&fx.admin, "graph").await.unwrap_err();
    assert!(err.message().contains("UTF-8"), "{err}");
    fx.canned(
        403,
        "application/json",
        b"{\"error\":\"bad assertion\"}".to_vec(),
    );
    let err = fx.page(&fx.admin, "graph").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(
        err.message().contains("refused the viewer assertion"),
        "{err}"
    );
    fx.canned(200, "text/html; charset=utf-8", b"<p>graph</p>".to_vec());
    assert_eq!(fx.page(&fx.admin, "graph").await?.html, "<p>graph</p>");
    Ok(())
}

/// The app refuses a page request the manager did not sign for that page,
/// called directly on its UI port.
#[tokio::test(flavor = "multi_thread")]
async fn the_app_refuses_page_requests_without_a_page_assertion_test() -> R<()> {
    let fx = fixture().await?;
    let get = |path: &'static str, assertion: Option<String>| {
        let addr = fx.app_addr;
        async move {
            let stream = tokio::net::TcpStream::connect(addr).await?;
            let headers = assertion
                .map(|value| {
                    vec![AppHttpHeader {
                        name: "legion-viewer".into(),
                        value,
                    }]
                })
                .unwrap_or_default();
            let reply = legion::worker::app_forward::exchange(
                stream,
                &ForwardAppRequestRequest {
                    allocation_id: 1,
                    job_id: "legion-system/fake".into(),
                    port: "api".into(),
                    method: "GET".into(),
                    path: path.into(),
                    headers,
                    body: Vec::new(),
                },
            )
            .await
            .map_err(|status| anyhow!("{status}"))?;
            Ok::<_, anyhow::Error>(reply.status)
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let sign = |kind: AssertionKind, name: &str| -> R<String> {
        let claim = ViewerAssertion {
            kind,
            app: APP.into(),
            name: name.into(),
            principal: "mallory".into(),
            accessor: None,
            permission: None,
            scope: AssertionScope::None,
            namespaces: Vec::new(),
            context: page_context(),
            inputs: None,
        };
        let mut claims = serde_json::Map::new();
        claims.insert(ASSERTION_CLAIM.into(), serde_json::to_value(&claim)?);
        Ok(fx
            .state
            .issue_viewer_assertion(APP, "va_test", "accessor:x", claims, now, now + 60)?
            .token)
    };
    assert_eq!(get("/legion/v1/pages/console", None).await?, 401);
    let view = sign(AssertionKind::View, "console")?;
    assert_eq!(get("/legion/v1/pages/console", Some(view)).await?, 403);
    let open = sign(AssertionKind::Page, "open")?;
    assert_eq!(
        get("/legion/v1/pages/console", Some(open.clone())).await?,
        403
    );
    assert_eq!(get("/legion/v1/pages/open", Some(open)).await?, 200);
    Ok(())
}

/// The server-side half of the allowlist: a call stamped
/// `legion-app-page` passes only for an RPC the page lists, checked against
/// the app's current manifest; an unstamped call is untouched.
#[tokio::test(flavor = "multi_thread")]
async fn bridged_calls_are_held_to_the_page_allowlist_test() -> R<()> {
    let fx = fixture().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let served = fx.svc.clone();
    let guard = AppPageGuardLayer::new(fx.store.clone());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(guard)
            .add_service(ManagerServer::from_arc(served))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    let mut client = ManagerClient::connect(addr).await?;
    let stamped = |page: Option<&str>, job: &str| {
        let mut request = req(
            GetJobInfoRequest {
                name: job.into(),
                ..Default::default()
            },
            &fx.admin,
        );
        if let Some(page) = page {
            request
                .metadata_mut()
                .insert(APP_PAGE_HEADER, page.parse().expect("ascii"));
        }
        request
    };
    let list = |page: Option<&str>| {
        let mut request = req(ListJobsRequest::default(), &fx.admin);
        if let Some(page) = page {
            request
                .metadata_mut()
                .insert(APP_PAGE_HEADER, page.parse().expect("ascii"));
        }
        request
    };

    // Listed: the guard passes it to the handler (no such job here).
    let passed = client
        .get_job_info(stamped(Some("fake/console"), "web"))
        .await
        .unwrap_err();
    assert_eq!(passed.code(), Code::NotFound, "{passed}");
    // Not listed, another page's, an unknown page or app: refused.
    for page in ["fake/console", "fake/open", "fake/nope", "other/console"] {
        let refused = client.list_jobs(list(Some(page))).await.unwrap_err();
        assert_eq!(refused.code(), Code::PermissionDenied, "{page}: {refused}");
    }
    let refused = client
        .list_jobs(list(Some("fake/console")))
        .await
        .unwrap_err();
    assert!(
        refused.message().contains("not in the page's `rpcs`"),
        "{refused}"
    );
    // Unstamped: the SPA's own calls are untouched.
    client.list_jobs(list(None)).await?;

    // An upgrade that narrows the page's `rpcs` takes effect at once.
    fx.svc
        .upgrade_app(req(
            UpgradeAppRequest {
                name: APP.into(),
                manifest_json: manifest("1.0.1", json!(["StopJob"])).to_string(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let refused = client
        .get_job_info(stamped(Some("fake/console"), "web"))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    // And a disabled app's pages may call nothing.
    fx.svc
        .set_app_enabled(req(
            SetAppEnabledRequest {
                name: APP.into(),
                enabled: false,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let refused = client
        .stop_job({
            let mut request = req(
                StopJobRequest {
                    name: "web".into(),
                    ..Default::default()
                },
                &fx.admin,
            );
            request
                .metadata_mut()
                .insert(APP_PAGE_HEADER, "fake/console".parse().expect("ascii"));
            request
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert!(refused.message().contains("disabled"), "{refused}");
    Ok(())
}
