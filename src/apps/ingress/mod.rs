//! The bundled ingress (docs/ingress.md; ingress plan
//! `internal_docs/plans/ingress-app-plan.md`).
//!
//! A job routes a public hostname to one of its services by adding
//! `ingress = { host = "shop.example.com" }` to the service block. This app
//! serves every such route on ports 80 and 443 of the nodes it runs on:
//!
//! - [`routes`]: the values, from `WatchAppExtensions`, compiled into a
//!   table every request reads;
//! - [`backend`]: each routed service's endpoints, refreshed from the
//!   catalog every [`REFRESH_EVERY`], picked by power of two choices, with
//!   pooled connections;
//! - [`proxy`]: the per-request HTTP proxy (HTTP/1.1 and HTTP/2 to clients,
//!   HTTP/1.1 to services, upgrades, `X-Forwarded-*`);
//! - [`certs`]: the bundles port 443 serves by SNI, followed from the app's
//!   KV;
//! - [`certificates`]: automatic certificates, from the bundled cert
//!   manager: one `certs/Certificate` per routed host, and HTTP-01 answered
//!   statelessly from the Issuer's public thumbprint;
//! - [`edge`]: where the replicas run (the ingress's own catalog service)
//!   and whether each routed host's DNS points there;
//! - [`views`]: the Ingress page and the job panel.
//!
//! [`Ingress::start`] runs all of it against any [`control::Control`], so
//! the integration tests drive the real proxy without a cluster; [`run`] is
//! the bundled app's body.

pub mod backend;
pub mod certificates;
pub mod certs;
pub mod control;
pub mod edge;
pub mod proxy;
pub mod routes;
pub mod views;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;

use futures::StreamExt;

use self::backend::{Backend, CatalogEndpoint};
use self::certificates::{CertState, IssuerView, Kinds};
use self::certs::{CertStore, KV_PREFIX};
use self::control::Control;
use self::routes::{RouteSpec, RouteTable, Target, ValueKey};
use crate::apps::{AppContext, AppFuture};
use crate::prelude::*;
use crate::structs::AppExtensionValue;

/// How often every routed service's endpoints are read from the catalog.
pub const REFRESH_EVERY: Duration = Duration::from_secs(2);
/// How many services are resolved at once.
pub const REFRESH_CONCURRENCY: usize = 16;
/// How often the app's KV (certificate bundles) and its own replicas are
/// read.
pub const SYNC_EVERY: Duration = Duration::from_secs(10);
/// How often every routed host's DNS is checked.
pub const DNS_EVERY: Duration = Duration::from_secs(60);
/// How many routed hosts are looked up at once.
pub const DNS_CONCURRENCY: usize = 16;
/// How long open connections get to finish when the app stops.
pub const STOP_GRACE: Duration = Duration::from_secs(8);

/// The app's config (`legion app config get ingress`), with defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressConfig {
    /// Ask the cert manager for a certificate for every routed host its
    /// Issuer can prove. Default true.
    pub certificates: bool,
    /// The `certs/Issuer` that issues them. Default `ingress`, which the app
    /// keeps itself from `acme`.
    pub issuer: String,
    /// The default Issuer's ACME server (`letsencrypt`,
    /// `letsencrypt-staging`, or a directory URL) and contact.
    pub acme_server: String,
    pub acme_email: Option<String>,
    pub redirect_https: bool,
    /// The app that owns the certificate kinds (`certs`; tests install the
    /// cert manager under another name).
    pub kinds: Kinds,
}

impl IngressConfig {
    /// Read the config the manifest's schema already checked.
    pub fn from_value(value: &Value) -> R<IngressConfig> {
        let acme = value.get("acme").cloned().unwrap_or_else(|| json!({}));
        let text = |value: &Value, field: &str| {
            value
                .get(field)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let issuer =
            text(value, "issuer").unwrap_or_else(|| certificates::DEFAULT_ISSUER.to_string());
        crate::structs::app_object::validate_object_name(&issuer)
            .map_err(|err| anyhow!("issuer: {err}"))?;
        let acme_server = text(&acme, "server").unwrap_or_else(|| "letsencrypt".to_string());
        if !matches!(acme_server.as_str(), "letsencrypt" | "letsencrypt-staging")
            && !acme_server.starts_with("https://")
        {
            bail!(
                "acme.server {acme_server:?} is not `letsencrypt`, `letsencrypt-staging`, or an https:// directory URL"
            );
        }
        Ok(IngressConfig {
            certificates: value
                .get("certificates")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            issuer,
            acme_server,
            acme_email: text(&acme, "email"),
            redirect_https: value
                .get("redirectHttps")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            kinds: Kinds::default(),
        })
    }
}

/// How the route watch is doing.
#[derive(Debug, Clone, Default)]
pub struct WatchStatus {
    pub connected: bool,
    /// Any snapshot received yet.
    pub synced: bool,
    pub error: Option<String>,
}

/// What every part of the app shares.
pub struct State {
    pub config: IngressConfig,
    table: RwLock<Arc<RouteTable>>,
    pub certs: Arc<CertStore>,
    watch: Mutex<WatchStatus>,
    /// The certs/Issuer as last read: its thumbprint answers HTTP-01.
    issuer: Mutex<Option<IssuerView>>,
    /// Each routed host's certificate, as the views show it.
    cert_states: Mutex<BTreeMap<String, CertState>>,
    /// Where the replicas run, from the ingress's own catalog service.
    edge: Mutex<Vec<edge::EdgeAddress>>,
    /// Each routed host's DNS, as this node resolves it.
    dns: Mutex<BTreeMap<String, edge::DnsCheck>>,
    /// Bumped when the routes change, so endpoints and certificates follow
    /// at once instead of on their next tick. A watch, not a `Notify`: a
    /// loop busy when the routes change still sees it on its next wait.
    routes_changed: tokio::sync::watch::Sender<u64>,
    stop: tokio::sync::watch::Sender<bool>,
}

impl State {
    pub fn new(config: IngressConfig) -> Arc<State> {
        Arc::new(State {
            config,
            table: RwLock::new(Arc::new(RouteTable::default())),
            certs: Arc::new(CertStore::default()),
            watch: Mutex::new(WatchStatus::default()),
            issuer: Mutex::new(None),
            cert_states: Mutex::new(BTreeMap::new()),
            edge: Mutex::new(Vec::new()),
            dns: Mutex::new(BTreeMap::new()),
            routes_changed: tokio::sync::watch::channel(0).0,
            stop: tokio::sync::watch::channel(false).0,
        })
    }

    /// The routes now.
    pub fn table(&self) -> Arc<RouteTable> {
        self.table
            .read()
            .map(|table| table.clone())
            .unwrap_or_default()
    }

    fn set_table(&self, table: RouteTable) {
        if let Ok(mut current) = self.table.write() {
            *current = Arc::new(table);
        }
        self.routes_changed
            .send_modify(|generation| *generation += 1);
    }

    pub fn redirect_https(&self) -> bool {
        self.config.redirect_https
    }

    pub fn watch_status(&self) -> WatchStatus {
        self.watch
            .lock()
            .map(|watch| watch.clone())
            .unwrap_or_default()
    }

    fn set_watch(&self, update: impl FnOnce(&mut WatchStatus)) {
        if let Ok(mut watch) = self.watch.lock() {
            update(&mut watch);
        }
    }

    /// The certs/Issuer as last read.
    pub fn issuer(&self) -> Option<IssuerView> {
        self.issuer.lock().ok().and_then(|issuer| issuer.clone())
    }

    pub(crate) fn set_issuer(&self, issuer: IssuerView) {
        if let Ok(mut current) = self.issuer.lock() {
            *current = Some(issuer);
        }
    }

    /// The thumbprint HTTP-01 answers carry, once the Issuer reports one.
    pub fn thumbprint(&self) -> Option<String> {
        self.issuer().and_then(|issuer| issuer.thumbprint)
    }

    /// Each routed host's certificate state.
    pub fn cert_states(&self) -> BTreeMap<String, CertState> {
        self.cert_states
            .lock()
            .map(|states| states.clone())
            .unwrap_or_default()
    }

    pub(crate) fn set_cert_states(&self, states: BTreeMap<String, CertState>) {
        if let Ok(mut current) = self.cert_states.lock() {
            *current = states;
        }
    }

    /// Where the replicas run: what DNS should point at.
    pub fn edge(&self) -> Vec<edge::EdgeAddress> {
        self.edge
            .lock()
            .map(|edge| edge.clone())
            .unwrap_or_default()
    }

    /// Each routed host's DNS check.
    pub fn dns(&self) -> BTreeMap<String, edge::DnsCheck> {
        self.dns.lock().map(|dns| dns.clone()).unwrap_or_default()
    }

    /// Whether the app is stopping.
    pub fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    fn stopped(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stop.subscribe()
    }

    /// A one-line summary for `/healthz`.
    pub fn summary(&self) -> String {
        let table = self.table();
        let (certs, _) = self.certs.snapshot();
        let watch = self.watch_status();
        let routes = table.served();
        let mut summary = format!("serving {routes} route(s), {} certificate(s)", certs.len());
        if !watch.connected {
            summary.push_str("; not watching routes (");
            summary.push_str(watch.error.as_deref().unwrap_or("connecting"));
            summary.push(')');
        }
        summary
    }
}

/// Turn one watched value into a route spec.
pub fn spec_of(value: &AppExtensionValue) -> RouteSpec {
    let path = value.path.clone().unwrap_or_default();
    let target = Target {
        job_id: value.job_id.clone(),
        namespace: if value.namespace.is_empty() {
            crate::structs::namespace::job_namespace(&value.job_id).to_string()
        } else {
            value.namespace.clone()
        },
        task: path.task.clone(),
        service: if path.service.is_empty() {
            path.task.clone()
        } else {
            path.service.clone()
        },
    };
    let effective: Value = serde_json::from_str(&value.effective_json)
        .or_else(|_| serde_json::from_str(&value.value_json))
        .unwrap_or(Value::Null);
    let mut spec = match RouteSpec::from_value(&effective, target.clone()) {
        Ok(spec) => spec,
        Err(problem) => RouteSpec {
            hosts: Vec::new(),
            path: "/".into(),
            strip_path: false,
            target,
            active: false,
            problem: Some(problem),
        },
    };
    if value.state == "invalid" {
        spec.active = false;
        spec.problem = Some(format!(
            "the value fails the ingress's schema: {}",
            value.detail
        ));
    } else if value.job_status == "stopped" {
        spec.active = false;
        spec.problem = Some("the job is stopped".into());
    }
    spec
}

fn value_key(job_id: &str, path: Option<&crate::structs::ExtensionPath>) -> ValueKey {
    let path = path.cloned().unwrap_or_default();
    ValueKey {
        job_id: job_id.to_string(),
        group: path.group,
        task: path.task,
        service: path.service,
    }
}

/// Follow the routes until stopped, reconnecting with backoff and keeping
/// the last table while disconnected.
async fn watch_routes(state: Arc<State>, control: Arc<dyn Control>) {
    let mut stop = state.stopped();
    let mut specs: BTreeMap<ValueKey, RouteSpec> = BTreeMap::new();
    let mut backends: HashMap<Target, Arc<Backend>> = HashMap::new();
    let mut wait = Duration::from_millis(500);
    while !state.stopping() {
        match control.watch_routes().await {
            Ok(mut stream) => {
                state.set_watch(|watch| {
                    watch.connected = true;
                    watch.error = None;
                });
                loop {
                    let event = tokio::select! {
                        event = stream.next() => event,
                        _ = stop.changed() => return,
                    };
                    match event {
                        Some(Ok(event)) => {
                            wait = Duration::from_millis(500);
                            if event.snapshot {
                                specs.clear();
                            }
                            for value in &event.upserts {
                                if value.point != "service" {
                                    continue;
                                }
                                specs.insert(
                                    value_key(&value.job_id, value.path.as_ref()),
                                    spec_of(value),
                                );
                            }
                            for gone in &event.deletes {
                                specs.remove(&value_key(&gone.job_id, gone.path.as_ref()));
                            }
                            let table = RouteTable::compile(specs.values(), &mut backends);
                            debug!(routes = table.served(), "Ingress routes changed");
                            state.set_table(table);
                            state.set_watch(|watch| watch.synced = true);
                        }
                        Some(Err(err)) => {
                            warn!("The ingress's route watch broke: {err:#}");
                            state.set_watch(|watch| watch.error = Some(format!("{err:#}")));
                            break;
                        }
                        None => {
                            state.set_watch(|watch| {
                                watch.error = Some("the route watch ended".into())
                            });
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                debug!("Opening the route watch: {err:#}");
                state.set_watch(|watch| watch.error = Some(format!("{err:#}")));
            }
        }
        state.set_watch(|watch| watch.connected = false);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = stop.changed() => return,
        }
        wait = (wait * 2).min(Duration::from_secs(30));
    }
}

/// Every backend the table routes to.
fn routed_backends(table: &RouteTable, backends: &mut HashMap<Target, Arc<Backend>>) {
    backends.clear();
    for backend in table.backends() {
        backends.insert(backend.target.clone(), backend.clone());
    }
}

/// Refresh every routed service's endpoints, on a timer and whenever the
/// routes change.
async fn refresh_endpoints(state: Arc<State>, control: Arc<dyn Control>) {
    let mut stop = state.stopped();
    let mut routes = state.routes_changed.subscribe();
    let mut backends: HashMap<Target, Arc<Backend>> = HashMap::new();
    while !state.stopping() {
        routes.mark_unchanged();
        let table = state.table();
        routed_backends(&table, &mut backends);
        drop(table);
        // One resolve per service name; each backend keeps its own job's
        // and task's endpoints.
        let mut by_service: BTreeMap<String, Vec<Arc<Backend>>> = BTreeMap::new();
        for backend in backends.values() {
            by_service
                .entry(backend.target.service_ref())
                .or_default()
                .push(backend.clone());
        }
        let control = control.clone();
        futures::stream::iter(by_service)
            .for_each_concurrent(REFRESH_CONCURRENCY, |(service, backends)| {
                let control = control.clone();
                async move {
                    match control.resolve(&service).await {
                        Ok(endpoints) => {
                            for backend in backends {
                                let owner = format!(
                                    "{}\u{0}{}",
                                    backend.target.job_id, backend.target.task
                                );
                                let mine: Vec<CatalogEndpoint> = endpoints
                                    .iter()
                                    .filter(|(key, _)| *key == owner)
                                    .map(|(_, endpoint)| endpoint.clone())
                                    .collect();
                                backend.update(&mine);
                            }
                        }
                        Err(err) => {
                            for backend in backends {
                                backend.refresh_failed(format!("{err:#}"));
                            }
                        }
                    }
                }
            })
            .await;
        tokio::select! {
            _ = tokio::time::sleep(REFRESH_EVERY) => {}
            _ = routes.changed() => {}
            _ = stop.changed() => return,
        }
    }
}

/// Follow the app's KV (certificate bundles: the operator's, and the ones
/// the cert manager writes for the ingress), and, with `edge_service`, where
/// the replicas run and whether each routed host's DNS points there: every
/// [`DNS_EVERY`], and at once for a host not checked yet or when the
/// replicas' addresses change.
async fn follow_kv_and_edge(
    state: Arc<State>,
    control: Arc<dyn Control>,
    edge_service: Option<String>,
) {
    let mut stop = state.stopped();
    let mut routes = state.routes_changed.subscribe();
    let mut last_dns: Option<std::time::Instant> = None;
    while !state.stopping() {
        let now = chrono::Utc::now().timestamp();
        match control.kv_list(KV_PREFIX).await {
            Ok(listing) => {
                let fetch = |key: String| {
                    let control = control.clone();
                    async move { control.kv_get(&key).await }
                };
                let changed = state.certs.sync(&listing, now, fetch).await;
                if changed > 0 {
                    info!(changed, "Ingress certificates changed");
                }
            }
            Err(err) => debug!("Listing the ingress's KV: {err:#}"),
        }
        if let Some(service) = &edge_service {
            let mut edge_moved = false;
            match control.resolve(service).await {
                Ok(endpoints) => {
                    let edge = edge::edge_addresses(&endpoints);
                    if let Ok(mut current) = state.edge.lock() {
                        edge_moved = *current != edge;
                        *current = edge;
                    }
                }
                Err(err) => debug!("Resolving the ingress's own replicas: {err:#}"),
            }
            let hosts: Vec<String> = state
                .table()
                .served_hosts()
                .into_iter()
                .filter(|host| !host.starts_with("*."))
                .collect();
            let unchecked = {
                let dns = state.dns();
                hosts.iter().any(|host| !dns.contains_key(host))
            };
            if edge_moved || unchecked || last_dns.is_none_or(|at| at.elapsed() >= DNS_EVERY) {
                last_dns = Some(std::time::Instant::now());
                let edge_ips = edge::edge_ips(&state.edge()).await;
                let checks: BTreeMap<String, edge::DnsCheck> = futures::stream::iter(hosts)
                    .map(|host| async move {
                        let resolved = edge::resolve(&host).await;
                        (host, resolved)
                    })
                    .buffer_unordered(DNS_CONCURRENCY)
                    .map(|(host, resolved)| {
                        let check = edge::judge(&resolved, &edge_ips);
                        (host, check)
                    })
                    .collect()
                    .await;
                if let Ok(mut current) = state.dns.lock() {
                    *current = checks;
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(SYNC_EVERY) => {}
            _ = routes.changed() => {}
            _ = stop.changed() => break,
        }
    }
}

/// Keep the ingress's certificates until stopped, then give the lock back:
/// a pass every [`certificates::PASS_EVERY`] and whenever the routes change,
/// and the Issuer re-read every [`certificates::THUMBPRINT_EVERY`] until
/// its thumbprint is known, so HTTP-01 challenges are answered as soon as
/// the cert manager registers its account.
async fn run_certificates(state: Arc<State>, control: Arc<dyn Control>, owner: String) {
    let mut stop = state.stopped();
    let mut routes = state.routes_changed.subscribe();
    let mut keeper = certificates::Certificates::new(control, state.clone(), owner);
    let mut last_pass: Option<std::time::Instant> = None;
    let mut routes_moved = false;
    while !state.stopping() {
        if routes_moved || last_pass.is_none_or(|at| at.elapsed() >= certificates::PASS_EVERY) {
            routes_moved = false;
            keeper.pass().await;
            last_pass = Some(std::time::Instant::now());
        } else if state.config.certificates && state.thumbprint().is_none() {
            keeper.read_issuer().await;
            if state.thumbprint().is_some() {
                // The account is registered: ask for the Certificates that
                // waited for it now, not on the next pass.
                routes_moved = true;
                continue;
            }
        }
        let wait = if state.config.certificates && state.thumbprint().is_none() {
            certificates::THUMBPRINT_EVERY
        } else {
            certificates::PASS_EVERY
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = routes.changed() => {
                // Let a burst of route changes settle into one pass.
                tokio::time::sleep(Duration::from_secs(2)).await;
                routes.mark_unchanged();
                routes_moved = true;
            }
            _ = stop.changed() => break,
        }
    }
    keeper.release().await;
}

/// Answer the validate call: a value whose host and path another job's
/// service already serves is an error; a host no public CA can issue for is
/// a warning while ACME is on.
pub fn validate_values(
    state: &State,
    call: &crate::structs::extensions::ValidateCall,
) -> Vec<crate::structs::extensions::ValidateResult> {
    let table = state.table();
    let mut claimed: HashMap<(String, String), String> = HashMap::new();
    call.values
        .iter()
        .map(|value| {
            let mut result = crate::structs::extensions::ValidateResult::default();
            let target = Target {
                job_id: call.job.clone(),
                namespace: call.namespace.clone(),
                task: value.path.task.clone(),
                service: value.path.service.clone(),
            };
            let spec = match RouteSpec::from_value(&value.effective, target) {
                Ok(spec) => spec,
                Err(problem) => {
                    result.errors.push(problem);
                    return result;
                }
            };
            for host in &spec.hosts {
                if let Some(owner) = table.owner(host, &spec.path) {
                    if owner.target.job_id != call.job {
                        result.errors.push(format!(
                            "{host}{} is already routed to {}",
                            spec.path,
                            owner.target.display()
                        ));
                    }
                }
                let here = format!("task {} service {}", value.path.task, value.path.service);
                if let Some(other) = claimed.insert((host.clone(), spec.path.clone()), here.clone()) {
                    if other != here {
                        result
                            .errors
                            .push(format!("{host}{} is also routed by {other} in this job", spec.path));
                    }
                }
                if state.config.certificates {
                    if let Some(issuer) = state.issuer() {
                        if let Err(why) = issuer.proves(host) {
                            result.warnings.push(format!(
                                "{host} gets no automatic certificate ({why}); it is served over HTTP unless an operator bundle under legion/apps/ingress/certs/ covers it"
                            ));
                        }
                    }
                }
            }
            result
        })
        .collect()
}

/// Where the proxy listens.
#[derive(Debug, Clone)]
pub struct Listen {
    pub http: SocketAddr,
    /// `None` serves no HTTPS.
    pub https: Option<SocketAddr>,
}

/// A running ingress.
pub struct Ingress {
    pub state: Arc<State>,
    pub http: SocketAddr,
    pub https: Option<SocketAddr>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Ingress {
    /// Bind the listeners and start everything: the proxy at once (it serves
    /// `404` until routes arrive), and the loops once `control` is given.
    pub async fn start(state: Arc<State>, listen: Listen) -> R<Ingress> {
        let http_listener = tokio::net::TcpListener::bind(listen.http)
            .await
            .with_context(|| format!("binding the ingress's HTTP port {}", listen.http))?;
        let https_listener = match listen.https {
            Some(address) => Some(
                tokio::net::TcpListener::bind(address)
                    .await
                    .with_context(|| format!("binding the ingress's HTTPS port {address}"))?,
            ),
            None => None,
        };
        let http = http_listener.local_addr()?;
        let https = match &https_listener {
            Some(listener) => Some(listener.local_addr()?),
            None => None,
        };
        let mut tasks = vec![tokio::spawn(proxy::serve(
            state.clone(),
            http_listener,
            proxy::Listener::Http,
            None,
            state.stopped(),
            STOP_GRACE,
        ))];
        if let Some(listener) = https_listener {
            let acceptor =
                tokio_rustls::TlsAcceptor::from(certs::server_config(state.certs.clone()));
            tasks.push(tokio::spawn(proxy::serve(
                state.clone(),
                listener,
                proxy::Listener::Https,
                Some(acceptor),
                state.stopped(),
                STOP_GRACE,
            )));
        }
        Ok(Ingress {
            state,
            http,
            https,
            tasks,
        })
    }

    /// Start following Legion through `control`: routes, endpoints, KV,
    /// (with `edge_service`) the ingress's own replicas and its routes' DNS,
    /// and (with `issue`) the certificates the cert manager keeps for the
    /// routes. `owner` names this replica on the certificates lock.
    pub fn connect(
        &mut self,
        control: Arc<dyn Control>,
        owner: String,
        edge_service: Option<String>,
        issue: bool,
    ) {
        self.tasks.push(tokio::spawn(watch_routes(
            self.state.clone(),
            control.clone(),
        )));
        self.tasks.push(tokio::spawn(refresh_endpoints(
            self.state.clone(),
            control.clone(),
        )));
        self.tasks.push(tokio::spawn(follow_kv_and_edge(
            self.state.clone(),
            control.clone(),
            edge_service,
        )));
        if issue {
            self.tasks.push(tokio::spawn(run_certificates(
                self.state.clone(),
                control,
                owner,
            )));
        }
    }

    /// Stop: the listeners close, open connections get [`STOP_GRACE`], and
    /// the certificates lock is given back.
    pub async fn stop(self) {
        let _ = self.state.stop.send(true);
        for task in self.tasks {
            let _ = tokio::time::timeout(STOP_GRACE + Duration::from_secs(1), task).await;
        }
    }
}

/// The listen addresses from the task's ports: `$LEGION_PORT_HTTP` and
/// `$LEGION_PORT_HTTPS` on every interface (80 and 443 when unset).
fn listen_from_env() -> R<Listen> {
    let port = |name: &str, default: u16| -> R<u16> {
        match std::env::var(format!("LEGION_PORT_{name}")) {
            Ok(value) => value
                .trim()
                .parse()
                .with_context(|| format!("LEGION_PORT_{name} is not a port: {value:?}")),
            Err(_) => Ok(default),
        }
    };
    Ok(Listen {
        http: SocketAddr::from(([0, 0, 0, 0], port("HTTP", 80)?)),
        https: Some(SocketAddr::from(([0, 0, 0, 0], port("HTTPS", 443)?))),
    })
}

/// The bundled app's body.
pub fn run(context: AppContext) -> AppFuture {
    Box::pin(run_body(context))
}

async fn run_body(mut context: AppContext) -> R<()> {
    let config = IngressConfig::from_value(context.config())?;
    let state = State::new(config);
    views::register(context.ui(), state.clone());
    {
        let state = state.clone();
        context.ui().validate(move |request| {
            let state = state.clone();
            async move { Ok(validate_values(&state, &request.call)) }
        });
    }
    let listen = listen_from_env()?;
    let mut ingress = match Ingress::start(state.clone(), listen.clone()).await {
        Ok(ingress) => ingress,
        Err(err) => {
            // Restarted by the reconciler; the error names the port.
            context.set_health(false, format!("{err:#}"));
            return Err(err);
        }
    };
    info!(
        app = context.name(),
        http = %ingress.http,
        https = ?ingress.https,
        certificates = state.config.certificates,
        issuer = %state.config.issuer,
        "The ingress is listening"
    );
    let connection = loop {
        match context.connection() {
            Ok(connection) => break Some(connection),
            Err(err) => {
                warn!("The ingress cannot reach Legion: {err:#}");
                context.set_health(
                    true,
                    format!("serving no routes: cannot reach Legion: {err:#}"),
                );
                tokio::select! {
                    _ = context.stopped() => break None,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        }
    };
    if let Some(connection) = connection {
        let control: Arc<dyn Control> = Arc::new(control::GrpcControl::new(connection.client()));
        let id = std::env::var("LEGION_ALLOC_ID")
            .ok()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| format!("{:016x}", rand::random::<u64>()));
        ingress.connect(
            control,
            format!("alloc-{id}"),
            Some(edge::SERVICE.to_string()),
            true,
        );
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = context.stopped() => break,
                _ = ticker.tick() => context.set_health(true, state.summary()),
            }
        }
    }
    info!(app = context.name(), "The ingress is stopping");
    ingress.stop().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_reads_with_defaults() {
        let config = IngressConfig::from_value(&json!({})).unwrap();
        assert!(config.certificates);
        assert_eq!(config.issuer, certificates::DEFAULT_ISSUER);
        assert_eq!(config.acme_server, "letsencrypt");
        assert!(config.redirect_https);
        assert_eq!(config.kinds.certificate(), "certs/Certificate");
        let config = IngressConfig::from_value(&json!({
            "certificates": false,
            "issuer": "letsencrypt-dns",
            "acme": { "server": "letsencrypt-staging", "email": " ops@example.com " },
            "redirectHttps": false,
        }))
        .unwrap();
        assert!(!config.certificates);
        assert_eq!(config.issuer, "letsencrypt-dns");
        assert_eq!(config.acme_server, "letsencrypt-staging");
        assert_eq!(config.acme_email.as_deref(), Some("ops@example.com"));
        assert!(!config.redirect_https);
        assert!(IngressConfig::from_value(&json!({ "acme": { "server": "ftp://x" } })).is_err());
        assert!(IngressConfig::from_value(&json!({ "issuer": "Not A Name" })).is_err());
    }

    fn value(job: &str, service: &str, effective: Value) -> AppExtensionValue {
        AppExtensionValue {
            job_id: job.into(),
            namespace: crate::structs::namespace::job_namespace(job).into(),
            config_version: 1,
            job_status: "running".into(),
            point: "service".into(),
            path: Some(crate::structs::ExtensionPath {
                task: "web".into(),
                service: service.into(),
                ..Default::default()
            }),
            key: "ingress".into(),
            value_json: effective.to_string(),
            effective_json: effective.to_string(),
            state: "valid".into(),
            ..Default::default()
        }
    }

    #[test]
    fn watched_values_become_specs() {
        let spec = spec_of(&value(
            "team-a/shop",
            "storefront",
            json!({ "host": "a.example.com", "path": "/" }),
        ));
        assert!(spec.active);
        assert_eq!(spec.target.service_ref(), "team-a/storefront");
        assert_eq!(spec.target.task, "web");

        let mut stopped = value("shop", "web", json!({ "host": "a.example.com" }));
        stopped.job_status = "stopped".into();
        assert_eq!(
            spec_of(&stopped).problem.as_deref(),
            Some("the job is stopped")
        );

        let mut invalid = value("shop", "web", json!({ "host": "a.example.com" }));
        invalid.state = "invalid".into();
        invalid.detail = "path: bad".into();
        assert!(!spec_of(&invalid).active);

        let broken = spec_of(&value("shop", "web", json!({ "nope": 1 })));
        assert!(!broken.active);
        assert!(broken.problem.unwrap().contains("host is required"));
    }

    #[test]
    fn validate_refuses_a_taken_host_and_warns_about_names_without_a_ca() {
        let state = State::new(IngressConfig::from_value(&json!({})).unwrap());
        let mut backends = HashMap::new();
        let taken = vec![RouteSpec::from_value(
            &json!({ "host": "shop.example.com" }),
            Target {
                job_id: "storefront".into(),
                namespace: "default".into(),
                task: "web".into(),
                service: "web".into(),
            },
        )
        .unwrap()];
        state.set_table(RouteTable::compile(&taken, &mut backends));
        // An ACME Issuer over HTTP-01: it cannot prove `.internal` names.
        state.set_issuer(certificates::IssuerView::from_object(
            "ingress",
            &control::ObjectView {
                spec: json!({ "namespaces": ["legion-system"], "acme": { "http01": {} } }),
                ..Default::default()
            },
        ));
        let call_value =
            |effective: Value, service: &str| crate::structs::extensions::ValidateCallValue {
                point: crate::structs::spec_fields::ExtensionPoint::Service,
                path: crate::structs::extensions::ExtensionPath {
                    task: "web".into(),
                    service: service.into(),
                    ..Default::default()
                },
                key: "ingress".into(),
                value: effective.clone(),
                effective,
            };
        let call = crate::structs::extensions::ValidateCall {
            job: "shop".into(),
            namespace: "default".into(),
            values: vec![
                call_value(json!({ "host": "shop.example.com", "path": "/" }), "web"),
                call_value(json!({ "host": "shop.internal", "path": "/" }), "admin"),
                call_value(json!({ "host": "shop.internal", "path": "/" }), "other"),
            ],
        };
        let results = validate_values(&state, &call);
        assert!(
            results[0].errors[0].contains("already routed to storefront"),
            "{results:?}"
        );
        assert!(results[1].errors.is_empty());
        assert!(results[1].warnings[0].contains("no automatic certificate"));
        assert!(results[2].errors[0].contains("also routed by task web service admin"));

        // The job that holds the host may resubmit it.
        let resubmit = crate::structs::extensions::ValidateCall {
            job: "storefront".into(),
            namespace: "default".into(),
            values: vec![call_value(json!({ "host": "shop.example.com" }), "web")],
        };
        assert!(validate_values(&state, &resubmit)[0].errors.is_empty());
    }
}
