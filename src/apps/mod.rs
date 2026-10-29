//! Apps from the inside (docs/apps.md, "Bundled apps" and "Writing an
//! app"): the apps compiled into the binary, and `legion app run <name>`,
//! the entrypoint their tasks run.
//!
//! A bundled app's job runs `command = ["legion:self", "app", "run",
//! "<name>"]`: the exec runtime resolves `legion:self` to the worker's own
//! executable (apps plan calls entry 12), so the app is always the same
//! build as the node that runs it. [`run_app`] is what that command does:
//!
//! - read the app's config from `$LEGION_APP_CONFIG` (the file the app
//!   reconciler renders into the job, resolved by the worker to a path the
//!   task can open);
//! - serve the app's HTTP port (`$LEGION_PORT_API`, the manifest's `api`
//!   port) with `/healthz`, answering from the health the app body sets, and
//!   the views and actions the body registers ([`AppContext::view`],
//!   [`AppContext::action`]; [`ui`]), each request's viewer assertion
//!   verified before a handler runs;
//! - hand the body an [`AppContext`] that connects to Legion through
//!   `LEGION_ADDR` (the allocation's API socket, authenticated as the app's
//!   `legion` identity);
//! - stop the body on SIGTERM or SIGINT, giving it a grace period.
//!
//! **Adding a bundled app** is one entry in [`BUNDLED`]: its manifest (a
//! Nickel file under `src/apps/bundled/`) and its body, an async function
//! from [`AppContext`] to a result. The manifest is validated by a unit test
//! below, and seeded on clusters by the first Control leader.

pub mod autoscaler;
pub mod autoscaler_ui;
pub mod ui;

use crate::prelude::*;
use crate::structs::app::{AppManifest, APP_CONFIG_ENV, APP_NAME_ENV};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

/// How long a body has to finish after a stop signal before the process
/// exits anyway. Under the worker's default kill timeout, so the app exits
/// on its own terms.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// The port name an app's HTTP listener binds (`$LEGION_PORT_API`).
pub const APP_PORT_NAME: &str = "api";

/// What an app body returns.
pub type AppFuture = Pin<Box<dyn Future<Output = R<()>> + Send>>;

/// An app body: started once, after config and the HTTP port are up. It
/// should return when [`AppContext::stopped`] resolves.
pub type AppBody = fn(AppContext) -> AppFuture;

/// One app compiled into the binary.
pub struct BundledApp {
    pub name: &'static str,
    /// The manifest, in Nickel, evaluated once per process with imports
    /// refused.
    pub manifest_source: &'static str,
    pub body: AppBody,
}

/// Every bundled app. The first Control leader seeds a record for each
/// (enabled unless the manager config's `apps` block says otherwise), with
/// its manifest's permissions granted by `legion:bootstrap`.
pub const BUNDLED: &[BundledApp] = &[BundledApp {
    name: "autoscaler",
    manifest_source: include_str!("bundled/autoscaler.ncl"),
    body: autoscaler::run,
}];

/// The bundled manifests, evaluated and validated once.
fn evaluated() -> &'static Result<Vec<AppManifest>, String> {
    static MANIFESTS: OnceLock<Result<Vec<AppManifest>, String>> = OnceLock::new();
    MANIFESTS.get_or_init(|| {
        BUNDLED
            .iter()
            .map(|app| {
                let json = crate::config_lang::eval_sandboxed(
                    &format!("bundled/{}.ncl", app.name),
                    app.manifest_source,
                    None,
                )
                .map_err(|err| format!("bundled app {}: {err:#}", app.name))?;
                let manifest = AppManifest::from_json(&json)
                    .map_err(|err| format!("bundled app {}: {err}", app.name))?;
                manifest.validate(true)?;
                if manifest.name != app.name {
                    return Err(format!(
                        "bundled app {} has a manifest named {}",
                        app.name, manifest.name
                    ));
                }
                Ok(manifest)
            })
            .collect()
    })
}

/// The manifests compiled into this binary. An error is a build defect,
/// which the unit tests catch; callers log it and carry on without apps.
pub fn bundled_manifests() -> R<&'static [AppManifest]> {
    evaluated()
        .as_ref()
        .map(Vec::as_slice)
        .map_err(|err| anyhow!("{err}"))
}

/// The manifest of bundled app `name`, if this binary has one.
pub fn bundled_manifest(name: &str) -> Option<&'static AppManifest> {
    bundled_manifests()
        .ok()?
        .iter()
        .find(|manifest| manifest.name == name)
}

/// Whether `name` is a bundled app's (installed apps may not take it).
pub fn is_bundled(name: &str) -> bool {
    BUNDLED.iter().any(|app| app.name == name)
}

/// The health an app reports on `/healthz`.
#[derive(Debug, Clone)]
pub(crate) struct Health {
    healthy: bool,
    detail: String,
}

/// What an app body gets: its name, config, health, its UI registry, a way
/// to reach Legion, and the stop signal.
#[derive(Clone)]
pub struct AppContext {
    name: String,
    config: Value,
    config_path: Option<PathBuf>,
    listen: SocketAddr,
    health: Arc<std::sync::RwLock<Health>>,
    ui: Arc<ui::UiRegistry>,
    stop: tokio::sync::watch::Receiver<bool>,
}

impl AppContext {
    /// Serve view `name` (docs/apps.md, "Writing views"): the manifest's
    /// `ui` block registers it in a slot, the manager authorizes each
    /// viewer, and the framework verifies the request's viewer assertion
    /// before `handler` runs. `handler` gets the slot's context and the
    /// viewer, and returns a `legion-view/1` document
    /// ([`crate::structs::app_ui::AppView`]), validated before it leaves.
    /// Register at any time; a request for a view not registered yet is
    /// answered 404.
    ///
    /// ```ignore
    /// context.view("job", |request| async move {
    ///     let job = request.context.job_id().unwrap_or_default();
    ///     Ok(AppView::new(vec![Block::Notice(NoticeBlock {
    ///         tone: Tone::Info,
    ///         title: None,
    ///         text: format!("{job} is not autoscaled"),
    ///     })]))
    /// });
    /// ```
    pub fn view<F, Fut>(&self, name: &str, handler: F)
    where
        F: Fn(ui::ViewRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<crate::structs::app_ui::AppView>> + Send + 'static,
    {
        self.ui.view(name, handler);
    }

    /// Serve action `name`, declared in the manifest's `ui.actions`. The
    /// manager checked the action's permission as the viewer on the context,
    /// and the inputs against their declared types; the framework checks the
    /// assertion binds this context and these inputs, and runs each
    /// assertion once. Act only on `request.context`. Return
    /// [`ui::Refused`] as the error to refuse with a message the viewer sees.
    pub fn action<F, Fut>(&self, name: &str, handler: F)
    where
        F: Fn(ui::ActionRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<ui::ActionOutcome>> + Send + 'static,
    {
        self.ui.action(name, handler);
    }

    /// Serve sandboxed page `name`, declared in the manifest's `ui.pages`
    /// (docs/apps.md, "Sandboxed pages"): `handler` returns the page's
    /// single-file HTML, which the SPA mounts in a sandboxed iframe. The
    /// manager checked the page's permission as the viewer, and the
    /// framework checks the assertion names this page.
    pub fn page<F, Fut>(&self, name: &str, handler: F)
    where
        F: Fn(ui::PageRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<String>> + Send + 'static,
    {
        self.ui.page(name, handler);
    }

    /// The registry the app's views, actions, and pages are served from.
    pub fn ui(&self) -> &ui::UiRegistry {
        &self.ui
    }

    /// The app's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The app's config: what `legion app config set` stored, validated
    /// against the manifest's schema.
    pub fn config(&self) -> &Value {
        &self.config
    }

    /// Where the config was read from (`None` when the task had no
    /// `$LEGION_APP_CONFIG` and the manifest's default was used).
    pub fn config_path(&self) -> Option<&std::path::Path> {
        self.config_path.as_deref()
    }

    /// The address the app's HTTP port listens on.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen
    }

    /// The manager address the client uses: `LEGION_ADDR`, which the worker
    /// sets to the allocation's API socket when workload identity is on.
    /// `None` when it is unset, and the app has no Legion credentials.
    pub fn legion_addr(&self) -> Option<String> {
        std::env::var("LEGION_ADDR")
            .ok()
            .filter(|addr| !addr.trim().is_empty())
    }

    /// A Legion client, authenticated as the app's `legion` identity by the
    /// worker behind the API socket. Errors when the task has no
    /// `LEGION_ADDR`. To act as the viewer in an action, use
    /// [`ui::ActionRequest::client_as_viewer`] instead.
    pub async fn client(&self) -> R<crate::cmd::helpers::AuthedManagerClient> {
        Ok(self.connection()?.client())
    }

    /// The connection [`Self::client`] makes its client from: the app's own
    /// clients and its viewer clients
    /// ([`ui::ActionRequest::client_as_viewer_over`]) share it.
    pub fn connection(&self) -> R<crate::cmd::helpers::ManagerConnection> {
        let Some(address) = self.legion_addr() else {
            bail!(
                "LEGION_ADDR is not set: the app has no way to reach Legion (workload identity \
                 is turned off on the managers, `workloadIdentity.enabled = false`, so its \
                 `legion` identity gets no API socket)"
            );
        };
        crate::cmd::helpers::manager_connection_at(
            address.trim(),
            crate::cmd::helpers::ClientTimeouts::from_env()?,
        )
    }

    /// Report health on `/healthz`: 200 when healthy, 503 otherwise, with
    /// `detail` as the body.
    pub fn set_health(&self, healthy: bool, detail: impl Into<String>) {
        if let Ok(mut health) = self.health.write() {
            *health = Health {
                healthy,
                detail: detail.into(),
            };
        }
    }

    /// Whether a stop was requested.
    pub fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    /// Resolves when the app is asked to stop (SIGTERM or SIGINT).
    pub async fn stopped(&mut self) {
        while !*self.stop.borrow() {
            if self.stop.changed().await.is_err() {
                return;
            }
        }
    }
}

/// `legion app run` arguments.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Listen here instead of `$LEGION_PORT_API`.
    pub listen: Option<String>,
    /// Read the config here instead of `$LEGION_APP_CONFIG`.
    pub config: Option<PathBuf>,
}

/// The config file path: the option, else `$LEGION_APP_CONFIG`, resolved
/// against `$LEGION_TASK_DIR` when relative (the worker resolves it already;
/// this keeps a hand-run app working too).
fn config_path(options: &RunOptions) -> Option<PathBuf> {
    if let Some(path) = &options.config {
        return Some(path.clone());
    }
    let raw = std::env::var(APP_CONFIG_ENV).ok()?;
    let path = PathBuf::from(raw.trim());
    if path.as_os_str().is_empty() {
        return None;
    }
    if path.is_absolute() {
        return Some(path);
    }
    match std::env::var("LEGION_TASK_DIR") {
        Ok(dir) if !dir.trim().is_empty() => Some(PathBuf::from(dir).join(path)),
        _ => Some(path),
    }
}

/// The listen address: the option, else every interface on
/// `$LEGION_PORT_API` (the port to bind, as for any task's port: in a gated
/// task it is inside the task's own namespace), else an ephemeral loopback
/// port.
fn listen_addr(options: &RunOptions) -> R<SocketAddr> {
    let port_env = APP_PORT_NAME.to_ascii_uppercase();
    let raw = options
        .listen
        .clone()
        .or_else(|| {
            std::env::var(format!("LEGION_PORT_{port_env}"))
                .ok()
                .map(|port| format!("0.0.0.0:{}", port.trim()))
        })
        .unwrap_or_else(|| "127.0.0.1:0".to_string());
    raw.trim()
        .parse()
        .with_context(|| format!("invalid listen address {raw:?}"))
}

/// `legion app run <name>`: run bundled app `name` in this process until it
/// returns or is told to stop.
pub async fn run_app(name: &str, options: RunOptions) -> R<()> {
    let Some(app) = BUNDLED.iter().find(|app| app.name == name) else {
        let known: Vec<&str> = BUNDLED.iter().map(|app| app.name).collect();
        bail!(
            "`legion app run` runs the apps bundled with Legion ({}); {name:?} is not one",
            known.join(", ")
        );
    };
    let manifest = bundled_manifest(name)
        .with_context(|| format!("bundled app {name} has no valid manifest in this build"))?;

    let path = config_path(&options);
    let config = match &path {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read the app config {}", path.display()))?;
            serde_json::from_str(&text)
                .with_context(|| format!("the app config {} is not JSON", path.display()))?
        }
        None => {
            warn!(
                app = name,
                "{APP_CONFIG_ENV} is not set; running with the manifest's default config"
            );
            manifest.default_config()
        }
    };
    crate::structs::app::validate_app_config(manifest, &config)
        .map_err(|err| anyhow!("the app config does not match {name}'s schema: {err}"))?;

    let listener = tokio::net::TcpListener::bind(listen_addr(&options)?)
        .await
        .context("failed to bind the app's HTTP port")?;
    let listen = listener.local_addr()?;
    let health = Arc::new(std::sync::RwLock::new(Health {
        healthy: true,
        detail: "starting".to_string(),
    }));
    let app_name = std::env::var(APP_NAME_ENV).unwrap_or_else(|_| name.to_string());
    let registry = Arc::new(ui::UiRegistry::default());
    let verifier = ui::AssertionVerifier::from_env(&app_name).map(Arc::new);
    if verifier.is_none() {
        warn!(
            app = name,
            "LEGION_ADDR is not set, so this app cannot verify viewer assertions and serves no \
             views (turn on workloadIdentity.enabled on the managers)"
        );
    }
    let ui_server = Arc::new(ui::UiServer::new(registry.clone(), verifier));
    let server_health = health.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_port(listener, Some(server_health), Some(ui_server)).await {
            error!(error = ?err, "the app's HTTP listener stopped");
        }
    });

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let context = AppContext {
        name: app_name,
        config,
        config_path: path,
        listen,
        health,
        ui: registry,
        stop: stop_rx,
    };
    info!(
        app = name,
        %listen,
        build = %std::env::var(crate::structs::app::APP_BUILD_ENV)
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
        legion = context.legion_addr().is_some(),
        "App started"
    );

    let mut body = tokio::spawn((app.body)(context));
    let outcome = tokio::select! {
        joined = &mut body => Some(joined),
        signal = stop_signal() => {
            info!(app = name, signal, "Stopping the app");
            let _ = stop_tx.send(true);
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut body).await {
                Ok(joined) => Some(joined),
                Err(_) => {
                    warn!(app = name, "The app did not stop within its grace period; exiting");
                    body.abort();
                    None
                }
            }
        }
    };
    match outcome {
        Some(Ok(Ok(()))) | None => {
            info!(app = name, "App stopped");
            Ok(())
        }
        Some(Ok(Err(err))) => Err(err.context(format!("app {name} failed"))),
        Some(Err(join)) => Err(anyhow!("app {name} panicked: {join}")),
    }
}

/// The first of SIGTERM and SIGINT.
async fn stop_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let (Ok(mut term), Ok(mut int)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) else {
            let _ = tokio::signal::ctrl_c().await;
            return "SIGINT";
        };
        tokio::select! {
            _ = term.recv() => "SIGTERM",
            _ = int.recv() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

/// The app's HTTP port: `/healthz` from the body's health (always 200 when
/// `health` is `None`), and the UI views and actions under `/legion/v1/`
/// (apps plan §6.5; [`ui::UiServer`]).
pub(crate) async fn serve_port(
    listener: tokio::net::TcpListener,
    health: Option<Arc<std::sync::RwLock<Health>>>,
    ui: Option<Arc<ui::UiServer>>,
) -> R<()> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full, Limited};
    use hyper::{Response, StatusCode};
    use hyper_util::rt::TokioIo;
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                debug!(error = ?err, "app listener accept failed");
                continue;
            }
        };
        let health = health.clone();
        let ui = ui.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request: hyper::Request<_>| {
                let health = health.clone();
                let ui = ui.clone();
                async move {
                    let path = request.uri().path().to_string();
                    let (status, content_type, body) = if path == "/healthz" {
                        let health =
                            match &health {
                                Some(health) => health
                                    .read()
                                    .map(|health| health.clone())
                                    .unwrap_or(Health {
                                        healthy: false,
                                        detail: "health state poisoned".into(),
                                    }),
                                None => Health {
                                    healthy: true,
                                    detail: "ok".into(),
                                },
                            };
                        let status = if health.healthy {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        };
                        (status, "text/plain", format!("{}\n", health.detail))
                    } else if let (Some(ui), true) = (&ui, path.starts_with("/legion/v1/")) {
                        let method = request.method().as_str().to_string();
                        let path_and_query = request
                            .uri()
                            .path_and_query()
                            .map(|value| value.as_str().to_string())
                            .unwrap_or(path.clone());
                        let assertion = request
                            .headers()
                            .get(crate::structs::app_ui::VIEWER_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string);
                        let body = Limited::new(request.into_body(), ui::MAX_ACTION_BODY)
                            .collect()
                            .await
                            .map(|collected| collected.to_bytes());
                        let answer = match body {
                            Ok(body) => {
                                ui.handle(&method, &path_and_query, assertion.as_deref(), &body)
                                    .await
                            }
                            Err(_) => ui::UiAnswer {
                                status: 413,
                                body: json!({ "error": "the request body is too large" }),
                                html: None,
                            },
                        };
                        let status = StatusCode::from_u16(answer.status)
                            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                        match answer.html {
                            Some(html) => (status, "text/html; charset=utf-8", html),
                            None => (status, "application/json", answer.body.to_string()),
                        }
                    } else {
                        (
                            StatusCode::NOT_FOUND,
                            "text/plain",
                            "not found\n".to_string(),
                        )
                    };
                    let response = Response::builder()
                        .status(status)
                        .header("content-type", content_type)
                        .body(Full::new(Bytes::from(body)))
                        .expect("static response parts");
                    Ok::<_, std::convert::Infallible>(response)
                }
            });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!(error = ?err, "app connection closed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every bundled manifest evaluates, validates, and renders a job
    /// RunJob takes.
    #[test]
    fn bundled_manifests_are_valid() {
        let manifests = bundled_manifests().expect("bundled manifests evaluate");
        assert_eq!(manifests.len(), BUNDLED.len());
        for manifest in manifests {
            assert!(manifest.version.is_none(), "{}", manifest.name);
            assert!(manifest.runs_self(), "{} runs legion:self", manifest.name);
            let record = crate::structs::app::AppRecord {
                name: manifest.name.clone(),
                source: crate::structs::app::AppSource::Bundled,
                source_ref: String::new(),
                manifest_digest: manifest.digest(),
                enabled: true,
                settings: crate::structs::app::AppSettings::from_manifest(manifest),
                config: manifest.default_config(),
                grant: crate::structs::app::AppGrant {
                    rules: manifest.permissions.clone(),
                    granted_by: crate::structs::app::BOOTSTRAP_PRINCIPAL.into(),
                    granted_at: String::new(),
                },
                installed_by: crate::structs::app::BOOTSTRAP_PRINCIPAL.into(),
                installed_at: String::new(),
                updated_at: String::new(),
                state: Default::default(),
                manifest: manifest.clone(),
                uid: crate::structs::app::APP_UID_BASE,
            };
            let marker = crate::structs::app::BuildMarker {
                version: env!("CARGO_PKG_VERSION").to_string(),
                build_id: "test".to_string(),
            };
            let rendered = crate::structs::app::render_app_job(
                &record,
                crate::structs::app::RenderContext {
                    marker: Some(&marker),
                },
            )
            .expect("renders");
            let config: crate::structs::JobConfig =
                serde_json::from_value(rendered.jobspec).expect("a valid jobspec");
            assert_eq!(config.name, format!("legion-system/{}", manifest.name));
        }
    }

    /// The autoscaler asks for exactly what it calls: policies, inputs,
    /// status, and history (`scale:read`), count writes and pauses
    /// (`scale:write`), the jobs on a node group's members (`job:read`),
    /// and nodes, their capacity demand, join tokens, drains, and forgets
    /// for node groups (`node:read`, `node:write`), and its replicas' shard
    /// leases under the reserved `legion/apps/autoscaler/` (`kv`). Its
    /// reports and action intents are app-plane and need no rule
    /// (autoscaler-v2 calls entry 37).
    #[test]
    fn the_autoscaler_manifest_asks_for_what_it_calls() {
        let manifest = bundled_manifest("autoscaler").expect("bundled");
        let rules = serde_json::to_value(&manifest.permissions).expect("rules");
        assert_eq!(
            rules,
            json!([
                { "resources": ["scale"], "actions": ["read", "write"], "namespaces": ["*"] },
                { "resources": ["job"], "actions": ["read"], "namespaces": ["*"] },
                { "resources": ["node"], "actions": ["read", "write"] },
                { "resources": ["kv"], "actions": ["read", "write"], "paths": ["legion/apps/autoscaler/*"] },
            ])
        );
        // Its UI: a sidebar page, a job panel, task badges, a node panel,
        // and three actions, each behind the permission it reads or writes.
        let ui = manifest.ui_spec().expect("the ui block parses");
        assert_eq!(
            ui.slots(),
            vec![
                crate::structs::app_ui::Slot::Nav,
                crate::structs::app_ui::Slot::Job,
                crate::structs::app_ui::Slot::Task,
                crate::structs::app_ui::Slot::Node
            ]
        );
        let actions: Vec<(&str, bool)> = ui
            .actions
            .iter()
            .map(|action| (action.name.as_str(), action.mutating()))
            .collect();
        assert_eq!(
            actions,
            vec![("pause", true), ("resume", true), ("release", true)]
        );
        assert!(ui
            .entries(crate::structs::app_ui::Slot::Nav)
            .iter()
            .all(|entry| entry.permission.is_some()));
        assert_eq!(manifest.placement, crate::structs::app::Placement::Managers);
        assert_eq!(manifest.job["priority"], "critical");
        assert!(is_bundled("autoscaler"));
        assert!(!is_bundled("gitops"));
    }

    #[test]
    fn the_listen_address_and_config_path_follow_the_options() {
        let options = RunOptions {
            listen: Some("127.0.0.1:9999".into()),
            config: Some(PathBuf::from("/tmp/x.json")),
        };
        assert_eq!(listen_addr(&options).unwrap().port(), 9999);
        assert_eq!(config_path(&options), Some(PathBuf::from("/tmp/x.json")));
    }

    /// The framework serves /healthz from the body's health, and stops the
    /// body when asked.
    #[tokio::test]
    async fn healthz_follows_the_body() -> R<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let health = Arc::new(std::sync::RwLock::new(Health {
            healthy: true,
            detail: "ok".into(),
        }));
        tokio::spawn(serve_port(listener, Some(health.clone()), None));
        let get = |path: &'static str| async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = tokio::net::TcpStream::connect(addr).await?;
            stream
                .write_all(
                    format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await?;
            let mut response = String::new();
            stream.read_to_string(&mut response).await?;
            Ok::<_, anyhow::Error>(response)
        };
        assert!(get("/healthz").await?.starts_with("HTTP/1.1 200"));
        health.write().unwrap().healthy = false;
        assert!(get("/healthz").await?.starts_with("HTTP/1.1 503"));
        assert!(get("/other").await?.starts_with("HTTP/1.1 404"));
        Ok(())
    }
}
