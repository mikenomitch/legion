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
//! - [`issuer`] and [`acme`]: automatic certificates from an ACME CA, one
//!   issuer per cluster by KV lock;
//! - [`views`]: the Ingress page and the job panel.
//!
//! [`Ingress::start`] runs all of it against any [`control::Control`], so
//! the integration tests drive the real proxy without a cluster; [`run`] is
//! the bundled app's body.

pub mod acme;
pub mod backend;
pub mod certs;
pub mod control;
pub mod issuer;
pub mod proxy;
pub mod routes;
pub mod views;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;

use futures::StreamExt;

use self::backend::{Backend, CatalogEndpoint};
use self::certs::{CertStore, KvVersion, KV_PREFIX};
use self::control::Control;
use self::issuer::IssuerStatus;
use self::routes::{RouteSpec, RouteTable, Target, ValueKey};
use crate::apps::{AppContext, AppFuture};
use crate::prelude::*;
use crate::structs::AppExtensionValue;

/// How often every routed service's endpoints are read from the catalog.
pub const REFRESH_EVERY: Duration = Duration::from_secs(2);
/// How many services are resolved at once.
pub const REFRESH_CONCURRENCY: usize = 16;
/// How often the app's KV (certificates, replicas, issuer status) is read.
pub const SYNC_EVERY: Duration = Duration::from_secs(10);
/// How often the issuer runs when nothing changes.
pub const ISSUE_EVERY: Duration = Duration::from_secs(60);
/// A replica record older than this is not shown.
pub const REPLICA_STALE: Duration = Duration::from_secs(120);
/// Replica records, by allocation.
pub const REPLICA_PREFIX: &str = "legion/apps/ingress/replicas/";
/// HTTP-01 answers looked up in KV at once by one replica, so a flood of
/// unknown tokens cannot become a flood of reads.
pub const CHALLENGE_LOOKUPS: usize = 4;
/// How long open connections get to finish when the app stops.
pub const STOP_GRACE: Duration = Duration::from_secs(8);

/// The ACME part of the app's config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcmeConfig {
    pub enabled: bool,
    /// The directory URL (aliases resolved).
    pub directory: String,
    pub email: Option<String>,
    pub trust_roots: Option<String>,
}

/// The app's config (`legion app config get ingress`), with defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressConfig {
    pub acme: AcmeConfig,
    pub redirect_https: bool,
}

impl IngressConfig {
    /// Read the config the manifest's schema already checked.
    pub fn from_value(value: &Value) -> R<IngressConfig> {
        let acme = value.get("acme").cloned().unwrap_or_else(|| json!({}));
        let text = |field: &str| {
            acme.get(field)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        Ok(IngressConfig {
            acme: AcmeConfig {
                enabled: acme.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                directory: acme::directory_url(text("directory").as_deref())?,
                email: text("email"),
                trust_roots: text("trustRoots"),
            },
            redirect_https: value
                .get("redirectHttps")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        })
    }
}

/// One replica's record, for the "Listening on" list.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecord {
    /// The node address serving ports 80 and 443.
    pub address: String,
    pub http_port: u16,
    pub https_port: u16,
    /// Unix seconds.
    pub seen_at: i64,
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
    control: RwLock<Option<Arc<dyn Control>>>,
    /// HTTP-01 answers this replica published.
    challenges: Mutex<HashMap<String, String>>,
    challenge_lookups: tokio::sync::Semaphore,
    watch: Mutex<WatchStatus>,
    issuer: Mutex<Option<IssuerStatus>>,
    replicas: Mutex<BTreeMap<String, ReplicaRecord>>,
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
            control: RwLock::new(None),
            challenges: Mutex::new(HashMap::new()),
            challenge_lookups: tokio::sync::Semaphore::new(CHALLENGE_LOOKUPS),
            watch: Mutex::new(WatchStatus::default()),
            issuer: Mutex::new(None),
            replicas: Mutex::new(BTreeMap::new()),
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

    pub fn issuer_status(&self) -> Option<IssuerStatus> {
        self.issuer.lock().ok().and_then(|issuer| issuer.clone())
    }

    pub(crate) fn set_issuer_status(&self, status: IssuerStatus) {
        if let Ok(mut issuer) = self.issuer.lock() {
            *issuer = Some(status);
        }
    }

    /// Live replicas, by allocation.
    pub fn replicas(&self) -> BTreeMap<String, ReplicaRecord> {
        let now = chrono::Utc::now().timestamp();
        self.replicas
            .lock()
            .map(|replicas| {
                replicas
                    .iter()
                    .filter(|(_, record)| now - record.seen_at <= REPLICA_STALE.as_secs() as i64)
                    .map(|(id, record)| (id.clone(), record.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn control(&self) -> Option<Arc<dyn Control>> {
        self.control.read().ok().and_then(|control| control.clone())
    }

    /// Whether the app is stopping.
    pub fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    fn stopped(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stop.subscribe()
    }

    pub(crate) fn publish_challenge(&self, token: &str, answer: &str) {
        if let Ok(mut challenges) = self.challenges.lock() {
            challenges.insert(token.to_string(), answer.to_string());
        }
    }

    pub(crate) fn withdraw_challenge(&self, token: &str) {
        if let Ok(mut challenges) = self.challenges.lock() {
            challenges.remove(token);
        }
    }

    /// The HTTP-01 answer for `token`: this replica's own, else the shared
    /// board in KV.
    pub async fn challenge(&self, token: &str) -> Option<String> {
        if !acme::valid_token(token) {
            return None;
        }
        if let Some(answer) = self
            .challenges
            .lock()
            .ok()
            .and_then(|challenges| challenges.get(token).cloned())
        {
            return Some(answer);
        }
        let control = self.control()?;
        let _permit = self.challenge_lookups.try_acquire().ok()?;
        control
            .kv_get(&format!("{}{token}", issuer::CHALLENGE_PREFIX))
            .await
            .ok()
            .flatten()
            .map(|(answer, _)| answer)
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

/// Follow the app's KV: certificate bundles, the issuer's status, and the
/// replicas' records; and keep this replica's record fresh.
async fn sync_kv(
    state: Arc<State>,
    control: Arc<dyn Control>,
    me: Option<(String, ReplicaRecord)>,
) {
    let mut stop = state.stopped();
    let mut versions: HashMap<String, i64> = HashMap::new();
    let mut last_record = 0i64;
    while !state.stopping() {
        let now = chrono::Utc::now().timestamp();
        if let Some((id, record)) = &me {
            if now - last_record >= (REPLICA_STALE.as_secs() / 4) as i64 {
                let record = ReplicaRecord {
                    seen_at: now,
                    ..record.clone()
                };
                if let Ok(text) = serde_json::to_string(&record) {
                    match control
                        .kv_put(&format!("{REPLICA_PREFIX}{id}"), &text)
                        .await
                    {
                        Ok(()) => last_record = now,
                        Err(err) => debug!("Writing the replica record: {err:#}"),
                    }
                }
            }
        }
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
                follow_records(&state, &control, &listing, &mut versions).await;
            }
            Err(err) => debug!("Listing the ingress's KV: {err:#}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(SYNC_EVERY) => {}
            _ = stop.changed() => break,
        }
    }
    if let Some((id, _)) = me {
        let _ = control.kv_delete(&format!("{REPLICA_PREFIX}{id}")).await;
    }
}

/// Read the issuer status and replica records whose versions changed.
async fn follow_records(
    state: &State,
    control: &Arc<dyn Control>,
    listing: &[KvVersion],
    versions: &mut HashMap<String, i64>,
) {
    let mut present = std::collections::HashSet::new();
    for entry in listing {
        let wanted = entry.key == issuer::STATUS_KEY || entry.key.starts_with(REPLICA_PREFIX);
        if !wanted {
            continue;
        }
        present.insert(entry.key.clone());
        if versions.get(&entry.key) == Some(&entry.version) {
            continue;
        }
        let Ok(Some((value, version))) = control.kv_get(&entry.key).await else {
            continue;
        };
        versions.insert(entry.key.clone(), version);
        if entry.key == issuer::STATUS_KEY {
            if let Ok(status) = serde_json::from_str::<IssuerStatus>(&value) {
                state.set_issuer_status(status);
            }
        } else if let Ok(record) = serde_json::from_str::<ReplicaRecord>(&value) {
            let id = entry.key[REPLICA_PREFIX.len()..].to_string();
            if let Ok(mut replicas) = state.replicas.lock() {
                replicas.insert(id, record);
            }
        }
    }
    versions.retain(|key, _| present.contains(key));
    if let Ok(mut replicas) = state.replicas.lock() {
        replicas.retain(|id, _| present.contains(&format!("{REPLICA_PREFIX}{id}")));
    }
}

/// Run the issuer until stopped, then give its lock back.
async fn run_issuer(state: Arc<State>, control: Arc<dyn Control>, owner: String) {
    let mut stop = state.stopped();
    let mut routes = state.routes_changed.subscribe();
    let mut issuer = issuer::Issuer::new(control, state.clone(), owner);
    // Let the first route snapshot and certificate sync land first.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(3)) => {}
        _ = stop.changed() => return,
    }
    while !state.stopping() {
        routes.mark_unchanged();
        issuer.pass().await;
        tokio::select! {
            _ = tokio::time::sleep(ISSUE_EVERY) => {}
            _ = routes.changed() => {
                // Let a burst of route changes settle into one pass.
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            _ = stop.changed() => break,
        }
    }
    issuer.release().await;
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
                if state.config.acme.enabled
                    && !host.starts_with("*.")
                    && !routes::publicly_issuable(host)
                {
                    result.warnings.push(format!(
                        "{host} gets no automatic certificate (no public CA issues for it); it is served over HTTP unless an operator bundle under legion/apps/ingress/certs/ covers it"
                    ));
                }
                if host.starts_with("*.") {
                    result.warnings.push(format!(
                        "{host} needs an operator bundle for HTTPS: automatic certificates cover exact names only"
                    ));
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

    /// Start following Legion through `control`: routes, endpoints, KV, and
    /// (with `issue`) the certificate issuer. `me` is this replica's id and
    /// record for the views.
    pub fn connect(
        &mut self,
        control: Arc<dyn Control>,
        owner: String,
        me: Option<(String, ReplicaRecord)>,
        issue: bool,
    ) {
        if let Ok(mut slot) = self.state.control.write() {
            *slot = Some(control.clone());
        }
        self.tasks.push(tokio::spawn(watch_routes(
            self.state.clone(),
            control.clone(),
        )));
        self.tasks.push(tokio::spawn(refresh_endpoints(
            self.state.clone(),
            control.clone(),
        )));
        self.tasks.push(tokio::spawn(sync_kv(
            self.state.clone(),
            control.clone(),
            me,
        )));
        if issue {
            self.tasks
                .push(tokio::spawn(run_issuer(self.state.clone(), control, owner)));
        }
    }

    /// Stop: the listeners close, open connections get [`STOP_GRACE`], and
    /// the issuer gives its lock back.
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
        acme = state.config.acme.enabled,
        directory = %state.config.acme.directory,
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
        let address = std::env::var("LEGION_IP_HTTP")
            .ok()
            .filter(|address| !address.trim().is_empty())
            .unwrap_or_else(|| ingress.http.ip().to_string());
        let record = ReplicaRecord {
            address,
            http_port: ingress.http.port(),
            https_port: ingress.https.map(|address| address.port()).unwrap_or(0),
            seen_at: 0,
        };
        ingress.connect(control, format!("alloc-{id}"), Some((id, record)), true);
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
        assert!(config.acme.enabled);
        assert_eq!(config.acme.directory, acme::LETS_ENCRYPT);
        assert!(config.redirect_https);
        let config = IngressConfig::from_value(&json!({
            "acme": { "enabled": false, "directory": "letsencrypt-staging", "email": " ops@example.com " },
            "redirectHttps": false,
        }))
        .unwrap();
        assert!(!config.acme.enabled);
        assert_eq!(config.acme.directory, acme::LETS_ENCRYPT_STAGING);
        assert_eq!(config.acme.email.as_deref(), Some("ops@example.com"));
        assert!(!config.redirect_https);
        assert!(IngressConfig::from_value(&json!({ "acme": { "directory": "ftp://x" } })).is_err());
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
