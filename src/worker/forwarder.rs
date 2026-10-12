//! Node-local upstream forwarders: the connect datapath.
//!
//! A task that declares `connect.upstreams` is handed a loopback address per
//! upstream (`LEGION_UPSTREAM_<SERVICE>` = `127.0.0.1:<local_port>`). This
//! module owns the other end of that address: one TCP listener per
//! `(allocation, upstream service)` pair, which balances every accepted
//! connection across the service's currently healthy endpoints and splices the
//! two sockets together.
//!
//! Deliberately boring, because it runs on every node:
//!
//! - **Loopback TCP only.** No netns, no bridge, no iptables, no
//!   `SO_REUSEPORT` — the behavior is identical on macOS and Linux, and
//!   nothing here is platform-conditional.
//! - **Driven entirely by heartbeat replies.** [`UpstreamForwarder::sync`]
//!   reconciles listeners against the desired allocation set;
//!   [`UpstreamForwarder::set_endpoints`] swaps in the endpoint snapshot the
//!   manager just sent. Both are no-ops for jobs that declare no upstreams.
//! - **Fail-static.** Endpoints are only replaced on a *successful* heartbeat
//!   (the daemon enforces that by calling `set_endpoints` only after one), so a
//!   manager outage keeps forwarding to the last known good set. An explicit
//!   empty list is authoritative and stops traffic.
//! - **Never fatal.** A port that will not bind, an upstream that refuses
//!   connections, or a poisoned lock degrades this listener only; it can never
//!   take down the heartbeat loop or the worker.
//!
//! Choosing *which* endpoint a connection goes to is [`crate::worker::balance`].

use crate::prelude::*;
use crate::structs::netpolicy::HandlerConfig;
use crate::structs::netpolicy::{ConnectFacts, Verdict, PROTOCOL_TCP};
use crate::structs::UpstreamHttpConfig;
use crate::structs::{parse_upstream_target, UpstreamTarget, UPSTREAM_LOOPBACK_ADDR};
use crate::worker::balance::{balancer, default_balancer, Balancer, Endpoint};
use crate::worker::netd::handlers::{self, HopFacts, HopOutcome, ProxyTarget};
use crate::worker::netd::http::HttpSettings;
use crate::worker::netd::policy::{egress_connect_verdict, AllocIdentity};
use crate::worker::netd::tls::MeshIdentity;
use crate::worker::netd::{metrics as net_metrics, Denials, PolicyLookup};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// How long a single upstream dial may take before the forwarder gives up on
/// that endpoint and tries the next candidate. Short on purpose: a wedged
/// backend must not stall a client for longer than a heartbeat or two.
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// How many endpoints one client connection may try before it is closed.
/// Bounded so a service whose endpoints are all dead fails fast instead of
/// walking the whole list.
const MAX_DIAL_ATTEMPTS: usize = 3;

/// Pause after a failed `accept()` so a persistent failure (descriptor
/// exhaustion, most likely) cannot spin the accept loop hot.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Identifies one forwarder listener: an allocation and the upstream service
/// it dials. A task may declare many upstreams; each gets its own listener.
type UpstreamKey = (i64, String);

/// One listener the forwarder should be running, as derived from a desired
/// allocation: the loopback port assigned at placement time
/// (`AllocNetwork.upstreams`) plus the strategy to balance with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamSpec {
    pub allocation_id: i64,
    /// The upstream key: a catalog service name, or the `name` of an external
    /// upstream (whose destination is `to`).
    pub service: String,
    pub local_port: u16,
    pub balance: String,
    /// External destination URL (`tcp://`, `tls://`, `http://`, `https://`);
    /// `None` for a catalog service, which is dialed from the endpoint feed.
    pub to: Option<String>,
    /// `connect.upstreams[].http` — turns on HTTP mode with traffic management.
    pub http: Option<UpstreamHttpConfig>,
    /// `connect.upstreams[].handlers` — run in HTTP mode.
    pub handlers: Vec<HandlerConfig>,
    /// Name-is-config: the VIP (`127.42.b.n`) and canonical port this
    /// upstream is also served on, so `<key>.svc.legion:<port>` reaches the
    /// same listener state. `None` on platforms without 127/8 routing.
    pub vip: Option<String>,
    pub canonical_port: Option<u16>,
    /// The gated namespace to bind inside (`network.mode = "gated"`); `None`
    /// binds in the host namespace. A gated listener fails closed while its
    /// policy is pending or its edge-boot gate is shut.
    pub netns: Option<PathBuf>,
}

impl UpstreamSpec {
    fn key(&self) -> UpstreamKey {
        (self.allocation_id, self.service.clone())
    }
}

/// State shared between a listener's accept loop and the heartbeat thread that
/// refreshes its endpoints. Held behind an `Arc` so `set_endpoints` never has
/// to restart a listener to change where it sends traffic.
struct UpstreamShared {
    allocation_id: i64,
    service: String,
    balancer: Arc<dyn Balancer>,
    /// Latest endpoint snapshot from the manager. Replaced wholesale.
    endpoints: Arc<Mutex<Vec<Endpoint>>>,
    /// Live connection count per endpoint address, for load-aware strategies.
    /// Entries are removed at zero so the map stays bounded by live endpoints.
    inflight: Arc<Mutex<HashMap<String, usize>>>,
    /// Parsed external destination, when this upstream has one.
    target: Option<UpstreamTarget>,
    /// Policy, denial ledger, and TLS shared by every listener.
    context: Arc<ForwarderContext>,
    /// The upstream's `http` block and handlers, as declared.
    http_config: Option<UpstreamHttpConfig>,
    handlers: Vec<HandlerConfig>,
    /// HTTP-mode state, built on first use (an upstream can enter HTTP mode
    /// through its own declaration or through the effective policy's
    /// `protocol = "http"`, which is only known once policy arrives).
    http: OnceLock<Arc<HttpSettings>>,
    /// Whether this listener lives in a gated namespace (fail-closed rules).
    gated: bool,
}

/// `proxy` handler targets by (allocation, service).
type HandlerEndpoints = HashMap<(i64, String), Vec<Endpoint>>;
/// Ingress TLS configs by (allocation, port label): the fingerprint of what
/// built each, and the result.
type IngressTlsConfigs =
    HashMap<(i64, String), (String, Result<Arc<rustls::ServerConfig>, String>)>;
/// An allocation's KV-held ingress certificate: the last good one (kept when
/// a later fetch fails, so a bad `kv put` never takes a listener down) and
/// the latest fetch problem.
#[derive(Debug, Default, Clone)]
pub struct KvCertState {
    pub cert: Option<Arc<crate::worker::netd::tls::KvCertificate>>,
    pub error: Option<String>,
}

/// `authz` verdicts by cache key: (allowed, reason, expires).
type AuthzCache = HashMap<String, (bool, Option<String>, Instant)>;

/// What every listener consults: the allocation's effective policy (egress
/// rules are evaluated before any dial), the denial ledger the heartbeat
/// drains, and a lazily built TLS client for `tls://` / `https://` upstreams.
/// A forwarder built without policy (tests, the pre-policy code path) allows
/// everything, exactly as before.
#[derive(Default)]

pub struct ForwarderContext {
    policy: Option<Arc<dyn PolicyLookup>>,
    cache: Option<Arc<crate::worker::netd::NetPolicyCache>>,
    denials: Option<Arc<Denials>>,
    tls: OnceLock<Arc<rustls::ClientConfig>>,
    /// Where allocation logs live (`<logs>/<allocation id>/`); access logs
    /// are written next to the task's own logs.
    logs_base_dir: Option<PathBuf>,
    /// This node's id (`node` proxy targets, the mesh certificate).
    node_id: OnceLock<String>,
    /// `proxy` handler targets, from the heartbeat feed (`for_handler`).
    handler_endpoints: Mutex<HandlerEndpoints>,
    /// What every external hop has done lately, keyed (allocation, source).
    handler_health: Mutex<HashMap<(i64, String), HandlerHealth>>,
    handler_pick: AtomicUsize,
    /// `authz` verdicts: key → (allowed, reason, expires).
    authz_cache: Mutex<AuthzCache>,
    http_client: OnceLock<reqwest::Client>,
    /// The node's mesh credential and the client config built from it.
    mesh: RwLock<Option<(Arc<MeshIdentity>, Arc<rustls::ClientConfig>)>>,
    /// Whether anything on this node needs a mesh certificate.
    mesh_wanted: AtomicBool,
    /// Ingress TLS configs per (allocation, port label): the fingerprint of
    /// what built it, and the result (an error is reported as `tls_error`).
    ingress_tls: Mutex<IngressTlsConfigs>,
    /// `certFrom = { kv }` material per allocation, from
    /// `GetIngressCertificates`. In memory only.
    kv_certs: Mutex<HashMap<i64, KvCertState>>,
    /// Service-graph samples since the last heartbeat.
    edges: Arc<crate::worker::netd::graph::EdgeLedger>,
    /// Ingress connections and bytes per allocation, for the idle policy.
    activity: Arc<crate::worker::netd::activity::ActivityLedger>,
    /// The host guard (worker-plane-auth plan §6.11). Every worker builds its
    /// context with [`ForwarderContext::new`], which installs the default
    /// guard; `None` only for a context built without policy (`Default`, the
    /// unit tests that predate it).
    host_guard: Option<Arc<crate::structs::host_guard::HostGuard>>,
    /// Name resolution for destinations the host guard judges; the system
    /// resolver when unset.
    resolver: Option<Arc<dyn crate::worker::netd::guard::Resolver>>,
}

/// The default guard: no operator overrides, the node's current interface
/// addresses.
fn default_host_guard() -> crate::structs::host_guard::HostGuard {
    let guard = crate::structs::host_guard::HostGuard::default();
    guard.set_node_addrs(crate::structs::host_guard::interface_addresses());
    guard
}

/// One external handler hop as the datapath has seen it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandlerHealth {
    pub handler: String,
    pub target: String,
    pub healthy: bool,
    pub last_error: Option<String>,
    pub calls: u64,
    pub errors: u64,
}

/// [`HandlerHealth`] with its source, for the heartbeat report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerStatus {
    pub source: String,
    pub handler: String,
    pub target: String,
    pub healthy: bool,
    pub last_error: Option<String>,
    pub calls: u64,
    pub errors: u64,
}

impl ForwarderContext {
    pub fn new(cache: Arc<crate::worker::netd::NetPolicyCache>, denials: Arc<Denials>) -> Self {
        ForwarderContext {
            policy: Some(cache.clone() as Arc<dyn PolicyLookup>),
            cache: Some(cache),
            denials: Some(denials),
            tls: OnceLock::new(),
            logs_base_dir: None,
            node_id: OnceLock::new(),
            handler_endpoints: Mutex::new(HashMap::new()),
            handler_health: Mutex::new(HashMap::new()),
            handler_pick: AtomicUsize::new(0),
            authz_cache: Mutex::new(HashMap::new()),
            http_client: OnceLock::new(),
            mesh: RwLock::new(None),
            mesh_wanted: AtomicBool::new(false),
            ingress_tls: Mutex::new(HashMap::new()),
            kv_certs: Mutex::new(HashMap::new()),
            edges: Arc::new(crate::worker::netd::graph::EdgeLedger::new()),
            activity: Arc::new(crate::worker::netd::activity::ActivityLedger::new()),
            host_guard: Some(Arc::new(default_host_guard())),
            resolver: None,
        }
    }

    /// Replaces the host guard (the worker installs the one its config
    /// describes; tests open their loopback origins with `allow` entries).
    pub fn with_host_guard(mut self, guard: Arc<crate::structs::host_guard::HostGuard>) -> Self {
        self.host_guard = Some(guard);
        self
    }

    /// Replaces the resolver (tests answer names without DNS).
    pub fn with_resolver(
        mut self,
        resolver: Arc<dyn crate::worker::netd::guard::Resolver>,
    ) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// The host guard every destination an allocation names is judged by.
    pub fn host_guard(&self) -> Option<&crate::structs::host_guard::HostGuard> {
        self.host_guard.as_deref()
    }

    /// The resolver the guard's resolve-then-judge step uses.
    pub fn resolver(&self) -> &dyn crate::worker::netd::guard::Resolver {
        static SYSTEM: crate::worker::netd::guard::SystemResolver =
            crate::worker::netd::guard::SystemResolver;
        match &self.resolver {
            Some(resolver) => resolver.as_ref(),
            None => &SYSTEM,
        }
    }

    /// Re-reads the node's interface addresses into the guard, so an
    /// address added after start is guarded too. Cheap; the heartbeat loop
    /// calls it every tick.
    pub fn refresh_node_addresses(&self) {
        if let Some(guard) = &self.host_guard {
            guard.set_node_addrs(crate::structs::host_guard::interface_addresses());
        }
    }

    /// Ingress activity per allocation (open connections, last bytes), read
    /// by the usage sampler for the idle policy.
    pub fn activity(&self) -> &Arc<crate::worker::netd::activity::ActivityLedger> {
        &self.activity
    }

    /// The service-graph ledger the heartbeat drains.
    pub fn edges(&self) -> &Arc<crate::worker::netd::graph::EdgeLedger> {
        &self.edges
    }

    /// This node's id, for `node`-local proxy targets and the mesh.
    pub fn with_node_id(self, node_id: String) -> Self {
        let _ = self.node_id.set(node_id);
        self
    }

    /// Same, after construction (the first heartbeat knows the id).
    pub fn set_node_id(&self, node_id: &str) {
        let _ = self.node_id.set(node_id.to_string());
    }

    pub fn node_id(&self) -> Option<String> {
        self.node_id.get().cloned()
    }

    // ----- external handlers -------------------------------------------

    /// Endpoints a `proxy` handler may hand a flow to, from the feed.
    pub fn set_handler_endpoints(
        &self,
        allocation_id: i64,
        service: &str,
        endpoints: Vec<Endpoint>,
    ) {
        lock(&self.handler_endpoints).insert((allocation_id, service.to_string()), endpoints);
    }

    pub fn handler_endpoints_for(&self, allocation_id: i64, service: &str) -> Vec<Endpoint> {
        lock(&self.handler_endpoints)
            .get(&(allocation_id, service.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Forget feeds and health for allocations that left the node.
    pub fn retain_allocations(&self, live: &std::collections::HashSet<i64>) {
        lock(&self.handler_endpoints).retain(|(id, _), _| live.contains(id));
        lock(&self.handler_health).retain(|(id, _), _| live.contains(id));
        lock(&self.ingress_tls).retain(|(id, _), _| live.contains(id));
        lock(&self.kv_certs).retain(|id, _| live.contains(id));
    }

    /// The KV version of the certificate held for an allocation (0 for none),
    /// what `GetIngressCertificates` compares against.
    pub fn kv_cert_version(&self, allocation_id: i64) -> i64 {
        lock(&self.kv_certs)
            .get(&allocation_id)
            .and_then(|state| state.cert.as_ref())
            .map(|cert| cert.version)
            .unwrap_or(0)
    }

    /// Install a freshly fetched certificate. New connections pick it up via
    /// the version in the server-config fingerprint; open ones keep theirs.
    pub fn set_kv_cert(&self, allocation_id: i64, cert: crate::worker::netd::tls::KvCertificate) {
        lock(&self.kv_certs).insert(
            allocation_id,
            KvCertState {
                cert: Some(Arc::new(cert)),
                error: None,
            },
        );
    }

    /// Record a fetch problem, keeping whatever certificate is held.
    pub fn set_kv_cert_error(&self, allocation_id: i64, error: String) {
        lock(&self.kv_certs).entry(allocation_id).or_default().error = Some(error);
    }

    /// Clear a fetch problem after an `unchanged` reply.
    pub fn clear_kv_cert_error(&self, allocation_id: i64) {
        if let Some(state) = lock(&self.kv_certs).get_mut(&allocation_id) {
            state.error = None;
        }
    }

    pub fn next_handler_pick(&self) -> usize {
        self.handler_pick.fetch_add(1, Ordering::Relaxed)
    }

    /// Record what a hop just did.
    pub fn note_handler(
        &self,
        allocation_id: i64,
        source: &str,
        handler: &str,
        target: &str,
        result: Result<(), String>,
    ) {
        let mut health = lock(&self.handler_health);
        let entry = health
            .entry((allocation_id, source.to_string()))
            .or_default();
        entry.handler = handler.to_string();
        entry.target = target.to_string();
        entry.calls += 1;
        match result {
            Ok(()) => {
                entry.healthy = true;
                entry.last_error = None;
            }
            Err(err) => {
                entry.healthy = false;
                entry.errors += 1;
                entry.last_error = Some(err);
            }
        }
    }

    /// Every hop the datapath has run for an allocation, by source.
    pub fn handler_statuses(&self, allocation_id: i64) -> Vec<HandlerStatus> {
        let health = lock(&self.handler_health);
        let mut out: Vec<HandlerStatus> = health
            .iter()
            .filter(|((id, _), _)| *id == allocation_id)
            .map(|((_, source), h)| HandlerStatus {
                source: source.clone(),
                handler: h.handler.clone(),
                target: h.target.clone(),
                healthy: h.healthy,
                last_error: h.last_error.clone(),
                calls: h.calls,
                errors: h.errors,
            })
            .collect();
        out.sort_by(|a, b| a.source.cmp(&b.source));
        out
    }

    pub fn authz_cached(&self, key: &str) -> Option<(bool, Option<String>)> {
        let cache = lock(&self.authz_cache);
        cache
            .get(key)
            .filter(|(_, _, expires)| *expires > Instant::now())
            .map(|(allowed, reason, _)| (*allowed, reason.clone()))
    }

    pub fn authz_store(&self, key: String, allowed: bool, reason: Option<String>, ttl: Duration) {
        if ttl.is_zero() {
            return;
        }
        let mut cache = lock(&self.authz_cache);
        if cache.len() >= crate::worker::netd::handlers::AUTHZ_CACHE_MAX {
            let now = Instant::now();
            cache.retain(|_, (_, _, expires)| *expires > now);
            if cache.len() >= crate::worker::netd::handlers::AUTHZ_CACHE_MAX {
                cache.clear();
            }
        }
        cache.insert(key, (allowed, reason, Instant::now() + ttl));
    }

    /// The HTTP client webhooks are called with (no redirects, rustls).
    pub fn http_client(&self) -> &reqwest::Client {
        self.http_client.get_or_init(|| {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .user_agent("legion-netd")
                .build()
                .unwrap_or_default()
        })
    }

    /// The workload identity subject (`<region>:<job>:<group>:<task>`) of
    /// an allocation — what a webhook, a PROXY v2 TLV, and an `identity`
    /// rule on the receiving side all see.
    pub fn identity_string_for(&self, allocation_id: i64) -> Option<String> {
        let who = self.identity_for(allocation_id)?;
        Some(format!(
            "{}:{}:{}:{}",
            crate::constants::DEF_CLUSTER_REGION,
            who.job_id,
            who.group,
            who.task_name
        ))
    }

    /// A sidecar's advertised port on this node: another allocation of the
    /// same job with a port of that label.
    pub fn sidecar_port(&self, allocation_id: i64, label: &str) -> Option<u16> {
        let cache = self.cache.as_ref()?;
        let identities = cache.identities();
        let job = identities.get(&allocation_id)?.job_id.clone();
        let mut candidates: Vec<(i64, u16)> = identities
            .iter()
            .filter(|(id, who)| **id != allocation_id && who.job_id == job)
            .filter_map(|(id, who)| who.ports.get(label).map(|port| (*id, *port)))
            .collect();
        candidates.sort();
        candidates.first().map(|(_, port)| *port)
    }

    // ----- the mesh ----------------------------------------------------

    pub fn set_mesh_wanted(&self, wanted: bool) {
        self.mesh_wanted.store(wanted, Ordering::Relaxed);
    }

    pub fn mesh_wanted(&self) -> bool {
        self.mesh_wanted.load(Ordering::Relaxed)
    }

    /// Whether the worker should (re)issue its node certificate now.
    pub fn mesh_needs_refresh(&self) -> bool {
        if !self.mesh_wanted() {
            return false;
        }
        match self.mesh.read().unwrap_or_else(|p| p.into_inner()).as_ref() {
            Some((identity, _)) => identity.needs_refresh(),
            None => true,
        }
    }

    pub fn set_mesh_identity(&self, identity: MeshIdentity) -> R<()> {
        let client = identity.client_config()?;
        *self.mesh.write().unwrap_or_else(|p| p.into_inner()) = Some((Arc::new(identity), client));
        // Ingress configs built from the previous certificate are stale.
        lock(&self.ingress_tls).clear();
        Ok(())
    }

    pub fn mesh_identity(&self) -> Option<Arc<MeshIdentity>> {
        self.mesh
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|(identity, _)| identity.clone())
    }

    /// Dial a mesh endpoint: trust the mesh CA, present the node certificate.
    pub fn mesh_connector(&self) -> Option<tokio_rustls::TlsConnector> {
        self.mesh
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|(_, client)| tokio_rustls::TlsConnector::from(client.clone()))
    }

    /// The server config for an ingress listener's chain, built once per
    /// (allocation, label, material) and rebuilt when the material changes.
    pub fn ingress_server_config(
        &self,
        allocation_id: i64,
        label: &str,
        tls: &crate::structs::netpolicy::TlsConfig,
    ) -> Result<Arc<rustls::ServerConfig>, String> {
        let mesh = self.mesh_identity();
        let kv_cert = tls.cert_kv_path().map(|_| {
            lock(&self.kv_certs)
                .get(&allocation_id)
                .cloned()
                .unwrap_or_default()
        });
        let fingerprint = format!(
            "{}|{}|{}",
            serde_json::to_string(tls).unwrap_or_default(),
            mesh.as_ref().map(|m| m.expires_at).unwrap_or(0),
            kv_cert
                .as_ref()
                .and_then(|state| state.cert.as_ref())
                .map(|cert| cert.version)
                .unwrap_or(0)
        );
        let key = (allocation_id, label.to_string());
        {
            let cache = lock(&self.ingress_tls);
            if let Some((fp, result)) = cache.get(&key) {
                if *fp == fingerprint {
                    return result.clone();
                }
            }
        }
        let mutual = tls.mode == crate::structs::netpolicy::TLS_MUTUAL;
        let result: Result<Arc<rustls::ServerConfig>, String> =
            if let Some(path) = tls.cert_kv_path() {
                let state = kv_cert.unwrap_or_default();
                match state.cert {
                    Some(cert) => cert
                        .server_config(tls, mesh.as_deref())
                        .map_err(|err| format!("kv {path}: {err:#}")),
                    None => Err(state
                        .error
                        .unwrap_or_else(|| format!("kv {path}: certificate not fetched yet"))),
                }
            } else if tls.cert_from_mesh() {
                match mesh {
                    Some(identity) => identity
                        .server_config(mutual)
                        .map_err(|err| format!("mesh server config: {err:#}")),
                    None => Err("mesh certificate not issued yet".to_string()),
                }
            } else if mutual && tls.client_ca.as_deref() == Some("legion") {
                // BYO server cert, mesh clients.
                match mesh {
                    Some(identity) => crate::worker::netd::tls::byo_server_config(
                        &crate::structs::netpolicy::TlsConfig {
                            client_ca: None,
                            mode: crate::structs::netpolicy::TLS_SERVER.to_string(),
                            ..tls.clone()
                        },
                    )
                    .and_then(|_| identity.server_config(true))
                    .map_err(|err| format!("{err:#}")),
                    None => Err("mesh certificate not issued yet".to_string()),
                }
            } else {
                crate::worker::netd::tls::byo_server_config(tls).map_err(|err| format!("{err:#}"))
            };
        if let Err(err) = &result {
            debug!(allocation_id, label, error = %err, "Ingress TLS material is not usable");
        }
        lock(&self.ingress_tls).insert(key, (fingerprint, result.clone()));
        result
    }

    /// (ready, error) for the allocation's ingress TLS material.
    pub fn ingress_tls_status(&self, allocation_id: i64) -> (bool, String) {
        // KV certificates are reported eagerly, from the fetch, rather than
        // waiting for the first connection to build a config: a missing key
        // is not ready, and a failed renewal while an older certificate
        // still serves is ready with the error attached.
        if let Some(state) = lock(&self.kv_certs).get(&allocation_id) {
            if let Some(error) = &state.error {
                return (state.cert.is_some(), error.clone());
            }
        }
        let cache = lock(&self.ingress_tls);
        for ((id, _), (_, result)) in cache.iter() {
            if *id == allocation_id {
                if let Err(err) = result {
                    return (false, err.clone());
                }
            }
        }
        (true, String::new())
    }

    /// Where to write per-allocation access logs.
    pub fn with_logs_base_dir(mut self, dir: PathBuf) -> Self {
        self.logs_base_dir = Some(dir);
        self
    }

    /// The access-log directory for one allocation, when logging is set up.
    pub fn log_dir_for(&self, allocation_id: i64) -> Option<PathBuf> {
        self.logs_base_dir
            .as_ref()
            .map(|base| base.join(allocation_id.to_string()))
    }

    /// Record a denial for the next heartbeat (no-op without a ledger).
    #[allow(clippy::too_many_arguments)]
    pub fn record_denial(
        &self,
        allocation_id: i64,
        direction: &str,
        protocol: &str,
        destination: &str,
        source: &str,
        reason: Option<&str>,
        method: &str,
        path: &str,
    ) {
        self.record_denial_for(
            allocation_id,
            None,
            direction,
            protocol,
            destination,
            source,
            reason,
            method,
            path,
        );
    }

    /// [`Self::record_denial`] with the verified peer identity (ingress).
    #[allow(clippy::too_many_arguments)]
    pub fn record_denial_for(
        &self,
        allocation_id: i64,
        identity: Option<&str>,
        direction: &str,
        protocol: &str,
        destination: &str,
        source: &str,
        reason: Option<&str>,
        method: &str,
        path: &str,
    ) {
        let Some(denials) = self.denials.as_ref() else {
            return;
        };
        let who = self.identity_for(allocation_id);
        denials.record(
            allocation_id,
            who.as_ref(),
            direction,
            protocol,
            destination,
            source,
            reason,
            identity,
            method,
            path,
        );
    }

    /// The policy cache the heartbeat loop feeds; `None` on a forwarder built
    /// without policy.
    pub fn cache(&self) -> Option<&Arc<crate::worker::netd::NetPolicyCache>> {
        self.cache.as_ref()
    }

    /// The denial ledger the heartbeat loop drains.
    pub fn denials(&self) -> Option<&Arc<Denials>> {
        self.denials.as_ref()
    }

    pub fn policy_for(
        &self,
        allocation_id: i64,
    ) -> Option<Arc<crate::structs::netpolicy::EffectivePolicy>> {
        self.policy.as_ref()?.policy_for(allocation_id)
    }

    pub fn identity_for(&self, allocation_id: i64) -> Option<AllocIdentity> {
        self.policy.as_ref()?.identity_for(allocation_id)
    }

    /// Whether the allocation's policy state is *known* — a body is cached
    /// for the hash the manager named — as opposed to still pending. A
    /// forwarder without a cache treats everything as known (and allowed).
    pub fn policy_known(&self, allocation_id: i64) -> bool {
        match self.cache.as_ref() {
            Some(cache) => cache.has_assignment(allocation_id) && cache.is_ready(allocation_id),
            None => true,
        }
    }

    /// A rustls client trusting the Mozilla roots (webpki-roots). Built once
    /// per process; external upstreams that pin a CA are a later phase.
    pub fn tls_connector(&self) -> tokio_rustls::TlsConnector {
        let config = self.tls.get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        });
        tokio_rustls::TlsConnector::from(config.clone())
    }
}

impl UpstreamShared {
    fn new(spec: &UpstreamSpec, context: Arc<ForwarderContext>) -> Self {
        // A stored allocation can name a strategy this build no longer has
        // (config validation only ran against the submitting build). Falling
        // back keeps the upstream serving instead of dropping it silently.
        let balancer = balancer(&spec.balance).unwrap_or_else(|err| {
            warn!(
                allocation_id = spec.allocation_id,
                service = %spec.service,
                error = ?err,
                "Unknown balance strategy for upstream; using the default"
            );
            default_balancer()
        });
        // A destination that fails to parse was validated at submit; a stored
        // allocation carrying one this build cannot parse is treated as a
        // service upstream with no endpoints, which closes every connection
        // rather than dialing something unintended.
        let target = spec.to.as_deref().and_then(|to| match parse_upstream_target(to) {
            Ok(target) => Some(target),
            Err(err) => {
                warn!(
                    allocation_id = spec.allocation_id,
                    upstream = %spec.service,
                    error = ?err,
                    "External upstream destination is invalid; the upstream will refuse connections"
                );
                None
            }
        });
        UpstreamShared {
            allocation_id: spec.allocation_id,
            service: spec.service.clone(),
            balancer,
            endpoints: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            target,
            context,
            http_config: spec.http.clone(),
            handlers: spec.handlers.clone(),
            http: OnceLock::new(),
            gated: spec.netns.is_some(),
        }
    }

    /// Whether this upstream declares HTTP itself (an `http` block, an
    /// `http://`/`https://` target, or handlers that need bodies).
    fn declares_http(&self) -> bool {
        self.http_config.is_some()
            || self.target.as_ref().is_some_and(|t| t.http)
            || self
                .handlers
                .iter()
                .any(|h| h.handler == crate::structs::netpolicy::HANDLER_DLP)
    }

    /// HTTP-mode settings when this connection must be parsed: the upstream
    /// declares HTTP, or the allocation's effective egress policy is an HTTP
    /// chain. `None` means splice bytes.
    fn http_settings(self: &Arc<Self>) -> Option<Arc<HttpSettings>> {
        let policy_http = self
            .context
            .policy_for(self.allocation_id)
            .and_then(|p| p.egress.as_ref().map(|c| c.is_http()))
            .unwrap_or(false);
        if !(self.declares_http() || policy_http) {
            return None;
        }
        Some(
            self.http
                .get_or_init(|| {
                    Arc::new(HttpSettings::new(
                        self.allocation_id,
                        self.service.clone(),
                        self.target.clone(),
                        self.http_config.as_ref(),
                        &self.handlers,
                        self.context.clone(),
                        self.balancer.clone(),
                        self.endpoints.clone(),
                        self.inflight.clone(),
                        self.context.log_dir_for(self.allocation_id),
                    ))
                })
                .clone(),
        )
    }

    fn endpoints(&self) -> Vec<Endpoint> {
        lock(&self.endpoints).clone()
    }

    fn inflight(&self) -> HashMap<String, usize> {
        lock(&self.inflight).clone()
    }
}

/// A bound (or not-yet-bindable) listener plus the state its accept loop uses.
struct Listener {
    spec: UpstreamSpec,
    shared: Arc<UpstreamShared>,
    /// The accept loop. `None` while the port cannot be bound; `sync` retries
    /// every heartbeat, and the endpoint list survives the retry.
    task: Option<JoinHandle<()>>,
    /// The VIP accept loop (`<vip>:<canonical port>`), sharing the same
    /// state; `None` when no VIP was assigned or its bind failed (which only
    /// costs the DNS name — the loopback port keeps working).
    vip_task: Option<JoinHandle<()>>,
    /// Whether the current bind failure was already warned about, so a
    /// permanently occupied port warns once and then stays at debug.
    bind_warned: bool,
}

impl Listener {
    /// Whether this upstream is actually being served. A finished accept loop
    /// counts as stopped, so the next `sync` rebuilds it — the loop is written
    /// not to exit, and silently forwarding nothing would be worse than a
    /// restart.
    fn is_running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }
}

/// The worker's node-local connect datapath. One instance per worker, built in
/// `run_worker_with_manager` and driven by the heartbeat loop.
#[derive(Default)]
pub struct UpstreamForwarder {
    listeners: Mutex<HashMap<UpstreamKey, Listener>>,
    context: Arc<ForwarderContext>,
    /// Per-allocation resolvers (docs/network-handlers.md, name-is-config);
    /// `None` on a forwarder built without one.
    dns: Option<Arc<crate::worker::netd::dns::NetDns>>,
    /// Per-allocation egress proxies inside gated namespaces.
    proxies: Option<Arc<crate::worker::netd::proxy::EgressProxies>>,
    /// Ingress listeners in front of advertised ports.
    ingress: Option<Arc<crate::worker::netd::ingress::IngressListeners>>,
}

impl UpstreamForwarder {
    /// A forwarder with no policy: every connection is allowed, as before
    /// network policy existed. Used by tests and by the pre-policy paths.
    pub fn new() -> Self {
        UpstreamForwarder::default()
    }

    /// A forwarder that consults `policy` before every dial and records
    /// denials for the heartbeat to report.
    pub fn with_context(context: Arc<ForwarderContext>) -> Self {
        UpstreamForwarder {
            listeners: Mutex::new(HashMap::new()),
            context,
            dns: None,
            proxies: None,
            ingress: None,
        }
    }

    /// Attach the ingress listeners.
    pub fn with_ingress(
        mut self,
        ingress: Arc<crate::worker::netd::ingress::IngressListeners>,
    ) -> Self {
        self.ingress = Some(ingress);
        self
    }

    /// The ingress listeners, when attached.
    pub fn ingress(&self) -> Option<&Arc<crate::worker::netd::ingress::IngressListeners>> {
        self.ingress.as_ref()
    }

    /// Attach the gated-namespace egress proxies.
    pub fn with_proxies(mut self, proxies: Arc<crate::worker::netd::proxy::EgressProxies>) -> Self {
        self.proxies = Some(proxies);
        self
    }

    /// The egress proxies, when attached.
    pub fn proxies(&self) -> Option<&Arc<crate::worker::netd::proxy::EgressProxies>> {
        self.proxies.as_ref()
    }

    /// Attach the per-allocation resolvers, reconciled by `sync_dns`.
    pub fn with_dns(mut self, dns: Arc<crate::worker::netd::dns::NetDns>) -> Self {
        self.dns = Some(dns);
        self
    }

    /// The resolvers, when attached.
    pub fn dns(&self) -> Option<&Arc<crate::worker::netd::dns::NetDns>> {
        self.dns.as_ref()
    }

    /// The shared policy / denial / TLS context.
    pub fn context(&self) -> &Arc<ForwarderContext> {
        &self.context
    }

    /// Reconcile the running listeners against `desired`.
    ///
    /// Starts a listener for every new `(allocation, service)` pair, retries
    /// binds that previously failed, and stops listeners that are no longer
    /// desired (their allocation stopped, or its upstreams changed). A changed
    /// port or strategy is a stop plus a start, since both are baked into the
    /// bound socket and the balancer instance.
    ///
    /// Stopping aborts the accept loop and waits for the socket to be dropped,
    /// so a replacement listener can bind the same port immediately.
    /// Connections already being forwarded drain on their own — only the
    /// accept path stops.
    #[instrument(skip_all, fields(desired = desired.len()))]
    pub async fn sync(&self, desired: &[UpstreamSpec]) {
        // One listener per key even if a malformed payload repeats a service.
        let mut wanted: HashMap<UpstreamKey, UpstreamSpec> = HashMap::new();
        for spec in desired {
            wanted.insert(spec.key(), spec.clone());
        }

        let mut stopped: Vec<JoinHandle<()>> = Vec::new();
        let mut pending: Vec<(UpstreamSpec, Arc<UpstreamShared>)> = Vec::new();
        {
            let mut listeners = lock(&self.listeners);
            listeners.retain(|key, listener| {
                if wanted.get(key).is_some_and(|spec| *spec == listener.spec) {
                    return true;
                }
                if let Some(task) = listener.task.take() {
                    stopped.push(task);
                }
                if let Some(task) = listener.vip_task.take() {
                    stopped.push(task);
                }
                debug!(
                    allocation_id = key.0,
                    service = %key.1,
                    "Stopping upstream forwarder listener"
                );
                false
            });
            for (key, spec) in wanted {
                match listeners.get(&key) {
                    // Serving and unchanged: leave it (and its endpoints) alone.
                    Some(listener) if listener.is_running() => {}
                    // Bind failed earlier (or the accept loop died); retry it,
                    // keeping any endpoints already delivered for this upstream.
                    Some(listener) => pending.push((spec, listener.shared.clone())),
                    None => {
                        let shared = Arc::new(UpstreamShared::new(&spec, self.context.clone()));
                        pending.push((spec, shared));
                    }
                }
            }
        }

        // Sockets are bound and released outside the lock: a std Mutex must
        // never be held across an await.
        for task in stopped {
            task.abort();
            // The accept loop parks in `accept()`, so cancellation lands (and
            // the port is released) before this returns.
            let _ = task.await;
        }
        for (spec, shared) in pending {
            self.start_listener(spec, shared).await;
        }
    }

    /// Replace the endpoint set one listener balances across.
    ///
    /// Called once per `HeartbeatReply.upstream_endpoints` entry, after a
    /// heartbeat that actually succeeded — that is what makes the datapath
    /// fail-static. An empty list is authoritative: the listener keeps
    /// accepting but closes each connection immediately, which is how a
    /// scaled-to-zero, unhealthy, or intention-denied service stops receiving
    /// traffic. Updates for an unknown listener are ignored.
    pub fn set_endpoints(&self, allocation_id: i64, service: &str, endpoints: Vec<Endpoint>) {
        let shared = {
            let listeners = lock(&self.listeners);
            listeners
                .get(&(allocation_id, service.to_string()))
                .map(|listener| listener.shared.clone())
        };
        let Some(shared) = shared else {
            debug!(
                allocation_id,
                service, "Ignoring endpoints for an upstream with no listener"
            );
            return;
        };
        debug!(
            allocation_id,
            service,
            endpoints = endpoints.len(),
            "Updated upstream endpoints"
        );
        *lock(&shared.endpoints) = endpoints;
    }

    /// The upstreams whose listener is currently bound. Diagnostics: an
    /// upstream whose port could not be bound is absent until a later `sync`
    /// succeeds.
    pub fn active(&self) -> Vec<UpstreamSpec> {
        let listeners = lock(&self.listeners);
        listeners
            .values()
            .filter(|listener| listener.is_running())
            .map(|listener| listener.spec.clone())
            .collect()
    }

    /// Live connection counts per endpoint address for one upstream — exactly
    /// what `least_conn` sees. Empty for an unknown upstream.
    pub fn inflight(&self, allocation_id: i64, service: &str) -> HashMap<String, usize> {
        let shared = {
            let listeners = lock(&self.listeners);
            listeners
                .get(&(allocation_id, service.to_string()))
                .map(|listener| listener.shared.clone())
        };
        shared.map(|shared| shared.inflight()).unwrap_or_default()
    }

    async fn start_listener(&self, spec: UpstreamSpec, shared: Arc<UpstreamShared>) {
        let key = spec.key();
        let addr: SocketAddr = format!("{UPSTREAM_LOOPBACK_ADDR}:{}", spec.local_port)
            .parse()
            .expect("loopback socket address");
        match crate::worker::netd::netns::bind_tcp(spec.netns.clone(), addr).await {
            Ok(socket) => {
                debug!(
                    allocation_id = spec.allocation_id,
                    service = %spec.service,
                    local_port = spec.local_port,
                    balance = %shared.balancer.name(),
                    "Started upstream forwarder listener"
                );
                let task = tokio::spawn(run_listener(socket, shared.clone()));
                let vip_task = bind_vip(&spec, &shared).await;
                lock(&self.listeners).insert(
                    key,
                    Listener {
                        spec,
                        shared,
                        task: Some(task),
                        vip_task,
                        bind_warned: false,
                    },
                );
            }
            Err(err) => {
                // Something else on the node holds the port. Keep the entry so
                // the next heartbeat retries; the task keeps failing to
                // connect meanwhile, which is the honest outcome.
                let mut listeners = lock(&self.listeners);
                let warned = listeners
                    .get(&key)
                    .is_some_and(|listener| listener.bind_warned);
                if warned {
                    debug!(
                        allocation_id = spec.allocation_id,
                        service = %spec.service,
                        local_port = spec.local_port,
                        "Upstream forwarder port still unavailable"
                    );
                } else {
                    warn!(
                        allocation_id = spec.allocation_id,
                        service = %spec.service,
                        local_port = spec.local_port,
                        error = ?err,
                        "Failed to bind upstream forwarder port; retrying on the next heartbeat"
                    );
                }
                listeners.insert(
                    key,
                    Listener {
                        spec,
                        shared,
                        task: None,
                        vip_task: None,
                        bind_warned: true,
                    },
                );
            }
        }
    }
}

/// Bind the upstream's VIP listener (`<vip>:<canonical port>`), when it has
/// one and the platform routes 127/8. Shares the listener state, so policy,
/// endpoints, and HTTP mode are identical whichever address the task dials.
async fn bind_vip(spec: &UpstreamSpec, shared: &Arc<UpstreamShared>) -> Option<JoinHandle<()>> {
    let (Some(vip), Some(port)) = (spec.vip.as_deref(), spec.canonical_port) else {
        return None;
    };
    if !crate::structs::platform_supports_vip_blocks() {
        return None;
    }
    let addr: SocketAddr = match format!("{vip}:{port}").parse() {
        Ok(addr) => addr,
        Err(_) => return None,
    };
    match crate::worker::netd::netns::bind_tcp(spec.netns.clone(), addr).await {
        Ok(socket) => {
            debug!(
                allocation_id = spec.allocation_id,
                upstream = %spec.service,
                vip = %vip,
                port,
                "Started upstream VIP listener"
            );
            Some(tokio::spawn(run_listener(socket, shared.clone())))
        }
        Err(err) => {
            debug!(
                allocation_id = spec.allocation_id,
                upstream = %spec.service,
                vip = %vip,
                port,
                error = ?err,
                "Failed to bind upstream VIP listener; the loopback port still serves"
            );
            None
        }
    }
}

impl Drop for UpstreamForwarder {
    fn drop(&mut self) {
        // Accept loops hold their socket; without this they would outlive the
        // forwarder and keep the ports bound.
        for listener in lock(&self.listeners).values_mut() {
            if let Some(task) = listener.task.take() {
                task.abort();
            }
            if let Some(task) = listener.vip_task.take() {
                task.abort();
            }
        }
    }
}

/// Lock a forwarder mutex, recovering from poisoning.
///
/// Every critical section here is a clone or a counter bump, so a panic
/// elsewhere cannot leave the state inconsistent — and a forwarder must never
/// panic the worker. Nothing in this module locks across an `.await`.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Accept loop for one upstream listener. Runs until aborted by `sync` (or by
/// the forwarder being dropped); every accepted connection is forwarded on its
/// own task so a slow dial never blocks the next client.
#[instrument(skip_all, fields(allocation_id = shared.allocation_id, service = %shared.service))]
async fn run_listener(socket: TcpListener, shared: Arc<UpstreamShared>) {
    loop {
        let client = match socket.accept().await {
            Ok((client, _peer)) => client,
            Err(err) => {
                debug!(error = ?err, "Upstream forwarder accept failed");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            forward_connection(client, shared).await;
        });
    }
}

/// Forward one client connection.
///
/// Policy first: the allocation's effective egress rules are evaluated with
/// what is known at connect time — the upstream's host (an external
/// destination's host, or the service name), the port, and for service
/// upstreams the picked endpoint's address — and a denial closes the
/// connection before anything is dialed. Then the dial: an external upstream
/// goes straight to its destination (with TLS originated for `tls://` and
/// `https://`); a service upstream picks an endpoint, dials it with a timeout,
/// and fails over to another candidate on error. Finally the two sockets are
/// spliced.
///
/// Per-connection logging stays at `debug` — this is the hot path.
async fn forward_connection(mut client: TcpStream, shared: Arc<UpstreamShared>) {
    // HTTP mode parses requests instead of splicing; see `netd::http`.
    if let Some(settings) = shared.http_settings() {
        crate::worker::netd::http::serve(client, settings).await;
        return;
    }
    if let Some(target) = shared.target.clone() {
        forward_external(client, shared, target).await;
        return;
    }

    let mut candidates = shared.endpoints();
    if candidates.is_empty() {
        debug!(
            allocation_id = shared.allocation_id,
            service = %shared.service,
            "No endpoints for upstream; closing connection"
        );
        net_metrics::connection("egress", PROTOCOL_TCP, "error");
        shared.context.edges().record_connection(
            shared.allocation_id,
            &shared.service,
            None,
            false,
        );
        let _ = client.shutdown().await;
        return;
    }

    for _ in 0..MAX_DIAL_ATTEMPTS {
        // Re-picking against the shrinking candidate list is what makes
        // failover work: a refused endpoint is out of the running for this
        // connection, and load-aware strategies see fresh in-flight counts.
        let inflight = shared.inflight();
        let Some(index) = shared.balancer.pick(&candidates, &inflight) else {
            break;
        };
        if index >= candidates.len() {
            break;
        }
        let endpoint = candidates.remove(index);

        // Policy, against the endpoint about to be dialed. A rule naming the
        // service (`host = "db"`) or the endpoint's address/port decides here.
        let addr: Option<SocketAddr> = endpoint.addr.parse().ok();
        let facts = ConnectFacts {
            host: Some(shared.service.as_str()),
            ip: addr.map(|a| a.ip()),
            port: addr.map(|a| a.port()).unwrap_or(0),
            identity: None,
        };
        if deny_if_policy_says_so(&shared, &facts, &endpoint.addr).await {
            let _ = client.shutdown().await;
            return;
        }
        // External handlers: a webhook may deny, a proxy may take the flow.
        let hop_facts = HopFacts {
            allocation_id: shared.allocation_id,
            direction: "egress",
            protocol: PROTOCOL_TCP,
            destination: &endpoint.addr,
            host: Some(shared.service.as_str()),
            ip: addr.map(|a| a.ip()),
            port: addr.map(|a| a.port()).unwrap_or(0),
            ..Default::default()
        };
        match run_hops(&shared, &hop_facts).await {
            HopOutcome::Allow => {}
            HopOutcome::Deny { source, reason } => {
                deny_connection(&shared, &endpoint.addr, &source, reason.as_deref());
                let _ = client.shutdown().await;
                return;
            }
            HopOutcome::Proxy(target) => {
                hand_off(&shared, client, &hop_facts, target, addr).await;
                return;
            }
        }

        // Counted from the dial (not from a successful connect) so
        // `least_conn` also sees connections that are still connecting.
        // Released on every exit path, including errors, by `Drop`.
        let _inflight_guard = InflightGuard::acquire(&shared, &endpoint.addr);

        let upstream =
            match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(&endpoint.addr)).await {
                Ok(Ok(upstream)) => upstream,
                Ok(Err(err)) => {
                    debug!(
                        allocation_id = shared.allocation_id,
                        service = %shared.service,
                        endpoint = %endpoint.addr,
                        error = ?err,
                        "Upstream dial failed; trying the next endpoint"
                    );
                    continue;
                }
                Err(_) => {
                    debug!(
                        allocation_id = shared.allocation_id,
                        service = %shared.service,
                        endpoint = %endpoint.addr,
                        "Upstream dial timed out; trying the next endpoint"
                    );
                    continue;
                }
            };

        shared.context.edges().record_connection(
            shared.allocation_id,
            &shared.service,
            endpoint.tags.first().map(String::as_str),
            true,
        );
        if endpoint.mesh_tls {
            mesh_splice(&shared, client, upstream, &endpoint, &hop_facts).await;
            return;
        }
        net_metrics::connection("egress", PROTOCOL_TCP, "allow");
        splice(&shared, &endpoint.addr, client, upstream).await;
        return;
    }

    debug!(
        allocation_id = shared.allocation_id,
        service = %shared.service,
        attempts = MAX_DIAL_ATTEMPTS,
        "No reachable endpoint for upstream; closing connection"
    );
    net_metrics::connection("egress", PROTOCOL_TCP, "error");
    shared
        .context
        .edges()
        .record_connection(shared.allocation_id, &shared.service, None, false);
    let _ = client.shutdown().await;
}

/// Dial an external upstream's fixed destination, originating TLS when the
/// scheme asks for it.
async fn forward_external(
    mut client: TcpStream,
    shared: Arc<UpstreamShared>,
    target: UpstreamTarget,
) {
    let destination = target.addr();
    // The boot gate before anything else, as everywhere in the datapath.
    if shared.gated && crate::worker::netd::netns::gate_is_shut(shared.allocation_id) {
        let facts = ConnectFacts {
            host: Some(target.host.as_str()),
            ip: target.host.parse().ok(),
            port: target.port,
            identity: None,
        };
        deny_if_policy_says_so(&shared, &facts, &destination).await;
        let _ = client.shutdown().await;
        return;
    }
    // Resolve once, judge every address against the host guard, and dial
    // only what passed (worker-plane-auth plan §6.11): `to = "tcp://localhost:4747"`
    // and a name that resolves to the metadata service are refused here.
    let addrs = match crate::worker::netd::guard::resolve_and_guard(
        shared.context.resolver(),
        shared.context.host_guard(),
        &target.host,
        target.port,
    )
    .await
    {
        Ok(addrs) => addrs,
        Err(crate::worker::netd::guard::GuardRefusal::Denied(reason)) => {
            deny_connection(
                &shared,
                &destination,
                crate::structs::host_guard::HOST_GUARD_SOURCE,
                Some(&reason),
            );
            let _ = client.shutdown().await;
            return;
        }
        Err(crate::worker::netd::guard::GuardRefusal::Unresolved(reason)) => {
            debug!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                %reason,
                "External upstream did not resolve"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    // Policy per resolved address: a name rule matches the name, a CIDR
    // rule the address. Only addresses every check passed are dialed.
    let mut allowed = Vec::with_capacity(addrs.len());
    for addr in &addrs {
        let facts = ConnectFacts {
            host: Some(target.host.as_str()),
            ip: Some(addr.ip()),
            port: target.port,
            identity: None,
        };
        if !policy_denies(&shared, &facts) {
            allowed.push(*addr);
        }
    }
    if allowed.is_empty() {
        let facts = ConnectFacts {
            host: Some(target.host.as_str()),
            ip: addrs.first().map(|addr| addr.ip()),
            port: target.port,
            identity: None,
        };
        deny_if_policy_says_so(&shared, &facts, &destination).await;
        let _ = client.shutdown().await;
        return;
    }
    let hop_facts = HopFacts {
        allocation_id: shared.allocation_id,
        direction: "egress",
        protocol: PROTOCOL_TCP,
        destination: &destination,
        host: Some(target.host.as_str()),
        ip: allowed.first().map(|addr| addr.ip()),
        port: target.port,
        ..Default::default()
    };
    match run_hops(&shared, &hop_facts).await {
        HopOutcome::Allow => {}
        HopOutcome::Deny { source, reason } => {
            deny_connection(&shared, &destination, &source, reason.as_deref());
            let _ = client.shutdown().await;
            return;
        }
        HopOutcome::Proxy(proxy_target) => {
            let resolved = handlers::resolve_addr(&destination).await;
            hand_off(&shared, client, &hop_facts, proxy_target, resolved).await;
            return;
        }
    }
    let _inflight_guard = InflightGuard::acquire(&shared, &destination);
    let upstream = match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(&allowed[..])).await
    {
        Ok(Ok(upstream)) => {
            shared.context.edges().record_connection(
                shared.allocation_id,
                &shared.service,
                None,
                true,
            );
            upstream
        }
        Ok(Err(err)) => {
            debug!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                destination = %destination,
                error = ?err,
                "External upstream dial failed"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
        Err(_) => {
            debug!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                destination = %destination,
                "External upstream dial timed out"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    if !target.tls {
        net_metrics::connection("egress", PROTOCOL_TCP, "allow");
        splice(&shared, &destination, client, upstream).await;
        return;
    }
    let server_name = match rustls_pki_types::ServerName::try_from(target.host.clone()) {
        Ok(name) => name,
        Err(err) => {
            warn!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                host = %target.host,
                error = ?err,
                "External upstream host is not a valid TLS server name"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    let connector = shared.context.tls_connector();
    match tokio::time::timeout(DIAL_TIMEOUT, connector.connect(server_name, upstream)).await {
        Ok(Ok(tls)) => {
            net_metrics::connection("egress", PROTOCOL_TCP, "allow");
            splice(&shared, &destination, client, tls).await;
        }
        Ok(Err(err)) => {
            debug!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                destination = %destination,
                error = ?err,
                "External upstream TLS handshake failed"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
        }
        Err(_) => {
            debug!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                destination = %destination,
                "External upstream TLS handshake timed out"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
        }
    }
}

/// Evaluate the allocation's egress policy for one destination. Returns true
/// when the connection must be closed, having recorded the denial and the
/// metric. No policy (or an allow / defer verdict) returns false.
/// Whether the allocation's egress verdict (the gate, pending policy, then
/// the chain) would refuse `facts`, without recording anything.
fn policy_denies(shared: &Arc<UpstreamShared>, facts: &ConnectFacts<'_>) -> bool {
    if shared.gated {
        if crate::worker::netd::netns::gate_is_shut(shared.allocation_id) {
            return true;
        }
        let policy = shared.context.policy_for(shared.allocation_id);
        if policy.is_none() && !shared.context.policy_known(shared.allocation_id) {
            return true;
        }
    }
    let policy = shared.context.policy_for(shared.allocation_id);
    matches!(
        egress_connect_verdict(policy.as_deref(), facts),
        Verdict::Deny { .. }
    )
}

async fn deny_if_policy_says_so(
    shared: &Arc<UpstreamShared>,
    facts: &ConnectFacts<'_>,
    destination: &str,
) -> bool {
    let policy = shared.context.policy_for(shared.allocation_id);
    // A gated allocation fails closed: while its policy is still pending, or
    // while its edge-boot gate is shut, nothing leaves.
    let gated_block = if shared.gated {
        if crate::worker::netd::netns::gate_is_shut(shared.allocation_id) {
            Some((
                "gate".to_string(),
                Some("network gate is shut until the placement is ratified".to_string()),
            ))
        } else if policy.is_none() && !shared.context.policy_known(shared.allocation_id) {
            Some((
                "policy-pending".to_string(),
                Some("network policy has not arrived yet".to_string()),
            ))
        } else {
            None
        }
    } else {
        None
    };
    let verdict = match gated_block {
        Some((source, reason)) => Verdict::Deny { source, reason },
        None => egress_connect_verdict(policy.as_deref(), facts),
    };
    match verdict {
        Verdict::Allow | Verdict::Defer => false,
        Verdict::Deny { source, reason } => {
            debug!(
                allocation_id = shared.allocation_id,
                upstream = %shared.service,
                destination = %destination,
                source = %source,
                "Egress connection denied by policy"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "deny");
            shared.context.edges().record_denied(
                shared.allocation_id,
                &shared.service,
                PROTOCOL_TCP,
            );
            shared.context.record_denial(
                shared.allocation_id,
                "egress",
                PROTOCOL_TCP,
                destination,
                &source,
                reason.as_deref(),
                "",
                "",
            );
            true
        }
    }
}

/// Splice the two halves and account the bytes.
/// The external hops that apply to this listener's flows: the effective
/// egress chain's, then the upstream's own.
async fn run_hops(shared: &Arc<UpstreamShared>, facts: &HopFacts<'_>) -> HopOutcome {
    let hops = handlers::hops_for(
        &shared.context,
        shared.allocation_id,
        "egress",
        Some(&shared.service),
        &shared.handlers,
    );
    if hops.is_empty() {
        return HopOutcome::Allow;
    }
    handlers::run(&shared.context, &hops, facts).await
}

fn deny_connection(
    shared: &Arc<UpstreamShared>,
    destination: &str,
    source: &str,
    reason: Option<&str>,
) {
    debug!(
        allocation_id = shared.allocation_id,
        upstream = %shared.service,
        destination = %destination,
        source = %source,
        "Egress connection denied by a handler"
    );
    net_metrics::connection("egress", PROTOCOL_TCP, "deny");
    shared
        .context
        .edges()
        .record_denied(shared.allocation_id, &shared.service, PROTOCOL_TCP);
    shared.context.record_denial(
        shared.allocation_id,
        "egress",
        PROTOCOL_TCP,
        destination,
        source,
        reason,
        "",
        "",
    );
}

/// Hand a flow to a `proxy` handler: dial it, write the PROXY v2 header
/// naming the workload and the original destination, then splice.
async fn hand_off(
    shared: &Arc<UpstreamShared>,
    mut client: TcpStream,
    facts: &HopFacts<'_>,
    target: ProxyTarget,
    destination: Option<SocketAddr>,
) {
    let mut proxy =
        match crate::worker::netd::guard::dial_guarded(&shared.context, &target.addr, DIAL_TIMEOUT)
            .await
        {
            Ok(stream) => stream,
            Err(err) => {
                debug!(
                    allocation_id = shared.allocation_id,
                    upstream = %shared.service,
                    proxy = %target.addr,
                    error = %err,
                    "Proxy handler dial failed"
                );
                shared.context.note_handler(
                    shared.allocation_id,
                    &target.source,
                    crate::structs::netpolicy::HANDLER_PROXY,
                    &target.addr,
                    Err(format!("dial {}: {err}", target.addr)),
                );
                net_metrics::connection("egress", PROTOCOL_TCP, "error");
                let _ = client.shutdown().await;
                return;
            }
        };
    if target.proxy_protocol {
        let header =
            handlers::proxy_header(&shared.context, facts, client.peer_addr().ok(), destination);
        if proxy.write_all(&header).await.is_err() {
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    }
    net_metrics::connection("egress", PROTOCOL_TCP, "allow");
    splice(shared, &target.addr, client, proxy).await;
}

/// Dial a mesh endpoint: TLS with this node's certificate, verify the peer
/// node, name the workload in a PROXY v2 header, then splice.
async fn mesh_splice(
    shared: &Arc<UpstreamShared>,
    mut client: TcpStream,
    upstream: TcpStream,
    endpoint: &Endpoint,
    facts: &HopFacts<'_>,
) {
    let Some(connector) = shared.context.mesh_connector() else {
        debug!(
            allocation_id = shared.allocation_id,
            service = %shared.service,
            endpoint = %endpoint.addr,
            "Endpoint needs mesh TLS but this node holds no mesh certificate yet"
        );
        net_metrics::connection("egress", PROTOCOL_TCP, "error");
        let _ = client.shutdown().await;
        return;
    };
    let server_name = match crate::worker::netd::tls::server_name(
        &crate::manager::mesh_ca::node_dns_name(&endpoint.node_id),
    ) {
        Ok(name) => name,
        Err(err) => {
            debug!(error = %err, "Mesh endpoint node name is not a valid server name");
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    match tokio::time::timeout(DIAL_TIMEOUT, connector.connect(server_name, upstream)).await {
        Ok(Ok(mut tls)) => {
            let header = handlers::proxy_header(
                &shared.context,
                facts,
                client.peer_addr().ok(),
                endpoint.addr.parse().ok(),
            );
            if tls.write_all(&header).await.is_err() {
                net_metrics::connection("egress", PROTOCOL_TCP, "error");
                let _ = client.shutdown().await;
                return;
            }
            net_metrics::connection("egress", PROTOCOL_TCP, "allow");
            splice(shared, &endpoint.addr, client, tls).await;
        }
        Ok(Err(err)) => {
            debug!(
                allocation_id = shared.allocation_id,
                service = %shared.service,
                endpoint = %endpoint.addr,
                error = ?err,
                "Mesh TLS handshake failed"
            );
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
        }
        Err(_) => {
            net_metrics::connection("egress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
        }
    }
}

async fn splice<U>(
    shared: &Arc<UpstreamShared>,
    destination: &str,
    mut client: TcpStream,
    mut upstream: U,
) where
    U: AsyncRead + AsyncWrite + Unpin,
{
    let tag = shared
        .endpoints()
        .iter()
        .find(|e| e.addr == destination)
        .and_then(|e| e.tags.first().cloned());
    match tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
        Ok((to_upstream, to_client)) => {
            net_metrics::bytes("egress", to_upstream);
            net_metrics::bytes("ingress", to_client);
            shared.context.edges().record_bytes(
                shared.allocation_id,
                &shared.service,
                tag.as_deref(),
                PROTOCOL_TCP,
                to_upstream,
                to_client,
            );
            debug!(
                service = %shared.service,
                endpoint = %destination,
                to_upstream,
                to_client,
                "Upstream connection closed"
            )
        }
        Err(err) => debug!(
            service = %shared.service,
            endpoint = %destination,
            error = ?err,
            "Upstream connection ended with an error"
        ),
    }
}

/// Keeps one endpoint's in-flight count accurate for the lifetime of a
/// connection attempt, however it ends (dial error, timeout, copy error, or a
/// clean close).
struct InflightGuard {
    shared: Arc<UpstreamShared>,
    addr: String,
}

impl InflightGuard {
    fn acquire(shared: &Arc<UpstreamShared>, addr: &str) -> Self {
        *lock(&shared.inflight).entry(addr.to_string()).or_insert(0) += 1;
        InflightGuard {
            shared: shared.clone(),
            addr: addr.to_string(),
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let mut inflight = lock(&self.shared.inflight);
        if let Some(count) = inflight.get_mut(&self.addr) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inflight.remove(&self.addr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Every wait in these tests is bounded by this and polls at
    /// [`POLL_INTERVAL`]; nothing sleeps for a fixed duration.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    fn spec(port: u16, balance: &str) -> UpstreamSpec {
        UpstreamSpec {
            allocation_id: 7,
            service: "db".into(),
            local_port: port,
            balance: balance.into(),
            to: None,
            http: None,
            handlers: Vec::new(),
            vip: None,
            canonical_port: None,
            netns: None,
        }
    }

    fn endpoint(addr: &str) -> Endpoint {
        Endpoint {
            addr: addr.to_string(),
            allocation_id: 1,
            node_id: "node-a".into(),
            tags: Vec::new(),
            mesh_tls: false,
        }
    }

    /// Reserve a loopback port by binding an ephemeral listener and dropping
    /// it. The forwarder needs a concrete port, and asking the OS for a free
    /// one behaves the same on macOS and Linux.
    async fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("ephemeral addr").port();
        drop(listener);
        port
    }

    /// A loopback echo server that announces itself: it writes `tag` on accept
    /// and then echoes. Tests read the tag to learn which endpoint served a
    /// connection, and hold the connection open to hold an in-flight slot.
    async fn spawn_echo(tag: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind echo server");
        let addr = listener.local_addr().expect("echo server addr").to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _peer)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    if stream.write_all(tag.as_bytes()).await.is_err() {
                        return;
                    }
                    let (mut reader, mut writer) = stream.split();
                    let _ = tokio::io::copy(&mut reader, &mut writer).await;
                });
            }
        });
        addr
    }

    async fn connect(port: u16) -> TcpStream {
        tokio::time::timeout(TEST_TIMEOUT, TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("forwarder did not accept in time")
            .expect("connect to forwarder")
    }

    /// Which echo server served this connection.
    async fn served_by(stream: &mut TcpStream) -> String {
        let mut tag = [0u8; 1];
        tokio::time::timeout(TEST_TIMEOUT, stream.read_exact(&mut tag))
            .await
            .expect("no endpoint served the connection in time")
            .expect("read endpoint tag");
        String::from_utf8_lossy(&tag).to_string()
    }

    /// Poll `condition` until it holds, bounded by [`TEST_TIMEOUT`].
    async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + TEST_TIMEOUT;
        while std::time::Instant::now() < deadline {
            if condition() {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// An external upstream on a context with the default host guard and a
    /// resolver that answers `metadata.test` with the metadata address.
    fn guarded_external(
        port: u16,
        to: &str,
    ) -> (
        UpstreamForwarder,
        Arc<crate::worker::netd::policy::Denials>,
        UpstreamSpec,
    ) {
        let cache = Arc::new(crate::worker::netd::NetPolicyCache::new());
        let denials = Arc::new(crate::worker::netd::policy::Denials::new());
        let resolver = crate::worker::netd::guard::test_support::FakeResolver::new();
        resolver.answer(
            "metadata.test",
            vec![vec!["169.254.169.254".parse().unwrap()]],
        );
        let context = ForwarderContext::new(cache, denials.clone()).with_resolver(resolver);
        let forwarder = UpstreamForwarder::with_context(Arc::new(context));
        let mut upstream = spec(port, "round_robin");
        upstream.service = "ext".into();
        upstream.to = Some(to.to_string());
        (forwarder, denials, upstream)
    }

    #[tokio::test]
    async fn external_upstreams_to_the_host_or_metadata_are_refused_by_the_host_guard_test() {
        // A live origin on loopback: without the guard the dial would succeed.
        let origin = spawn_echo("x").await;
        let origin_port: u16 = origin.rsplit(':').next().unwrap().parse().unwrap();
        for (to, refused) in [
            (format!("tcp://localhost:{origin_port}"), "localhost"),
            (format!("tcp://127.0.0.1:{origin_port}"), "127.0.0.1"),
            ("tcp://metadata.test:80".to_string(), "169.254.169.254"),
        ] {
            let port = free_port().await;
            let (forwarder, denials, upstream) = guarded_external(port, &to);
            forwarder.sync(&[upstream]).await;
            let mut client = connect(port).await;
            let mut buf = [0u8; 1];
            let read = tokio::time::timeout(TEST_TIMEOUT, client.read(&mut buf))
                .await
                .expect("the forwarder closed the refused connection");
            assert!(
                matches!(read, Ok(0) | Err(_)),
                "{to}: nothing may be served, read {read:?}"
            );
            let mut recorded = Vec::new();
            wait_until("the host-guard denial", || {
                recorded.extend(denials.drain());
                !recorded.is_empty()
            })
            .await;
            assert_eq!(recorded[0].source, "host-guard", "{to}: {recorded:?}");
            assert!(
                recorded[0].reason.contains(refused),
                "{to}: reason names {refused}: {}",
                recorded[0].reason
            );
        }
    }

    #[tokio::test]
    async fn round_robin_spreads_connections_across_live_endpoints() {
        let first = spawn_echo("a").await;
        let second = spawn_echo("b").await;
        let port = free_port().await;

        let forwarder = UpstreamForwarder::new();
        forwarder.sync(&[spec(port, "round_robin")]).await;
        forwarder.set_endpoints(7, "db", vec![endpoint(&first), endpoint(&second)]);

        // Sequential connections, so each pick happens before the next dial:
        // round robin must hand them out in order and then wrap.
        let mut tags = Vec::new();
        for _ in 0..3 {
            let mut client = connect(port).await;
            tags.push(served_by(&mut client).await);
        }
        assert_eq!(tags, vec!["a", "b", "a"]);
    }

    #[tokio::test]
    async fn failover_skips_a_dead_endpoint_and_still_serves() {
        // Nothing listens here, so the dial is refused immediately.
        let dead = format!("127.0.0.1:{}", free_port().await);
        let live = spawn_echo("b").await;
        let port = free_port().await;

        let forwarder = UpstreamForwarder::new();
        forwarder.sync(&[spec(port, "round_robin")]).await;
        forwarder.set_endpoints(7, "db", vec![endpoint(&dead), endpoint(&live)]);

        // The first pick lands on the dead endpoint; the connection must still
        // be served by failing over rather than being dropped.
        let mut client = connect(port).await;
        assert_eq!(served_by(&mut client).await, "b");

        // And the failed dial released its in-flight slot.
        wait_until("the failed dial to release its in-flight slot", || {
            !forwarder.inflight(7, "db").contains_key(&dead)
        })
        .await;
    }

    #[tokio::test]
    async fn empty_endpoint_list_closes_the_connection() {
        let live = spawn_echo("a").await;
        let port = free_port().await;

        let forwarder = UpstreamForwarder::new();
        forwarder.sync(&[spec(port, "round_robin")]).await;
        forwarder.set_endpoints(7, "db", vec![endpoint(&live)]);
        let mut served = connect(port).await;
        assert_eq!(served_by(&mut served).await, "a");

        // An explicit empty list is authoritative: stop sending traffic.
        forwarder.set_endpoints(7, "db", Vec::new());
        let mut client = connect(port).await;
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(TEST_TIMEOUT, client.read(&mut buf))
            .await
            .expect("forwarder must close the connection promptly, not hang");
        assert_eq!(
            read.expect("read from forwarder"),
            0,
            "forwarder must close connections while it has no endpoints"
        );
    }

    #[tokio::test]
    async fn sync_stops_a_listener_that_is_no_longer_desired() {
        let port = free_port().await;
        let forwarder = UpstreamForwarder::new();

        forwarder.sync(&[spec(port, "round_robin")]).await;
        assert_eq!(forwarder.active(), vec![spec(port, "round_robin")]);
        assert!(
            TcpListener::bind(("127.0.0.1", port)).await.is_err(),
            "the forwarder must hold the upstream port while it is desired"
        );

        forwarder.sync(&[]).await;
        assert!(
            forwarder.active().is_empty(),
            "an undesired upstream must have no listener"
        );
        // `sync` awaits the aborted accept loop, so the socket is already gone.
        assert!(
            TcpListener::bind(("127.0.0.1", port)).await.is_ok(),
            "the upstream port must be bindable again once the listener stops"
        );
    }

    #[tokio::test]
    async fn sync_retries_a_bind_that_failed_and_keeps_serving_afterwards() {
        let port = free_port().await;
        let live = spawn_echo("a").await;
        let squatter = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("occupy the upstream port");

        let forwarder = UpstreamForwarder::new();
        forwarder.sync(&[spec(port, "round_robin")]).await;
        assert!(
            forwarder.active().is_empty(),
            "a bind failure must not register a listener"
        );
        // Endpoints delivered while the bind is failing must survive the retry.
        forwarder.set_endpoints(7, "db", vec![endpoint(&live)]);

        drop(squatter);
        forwarder.sync(&[spec(port, "round_robin")]).await;
        assert_eq!(forwarder.active(), vec![spec(port, "round_robin")]);

        let mut client = connect(port).await;
        assert_eq!(served_by(&mut client).await, "a");
    }

    #[tokio::test]
    async fn least_conn_prefers_the_endpoint_with_fewer_in_flight_connections() {
        let first = spawn_echo("a").await;
        let second = spawn_echo("b").await;
        let port = free_port().await;

        let forwarder = UpstreamForwarder::new();
        forwarder.sync(&[spec(port, "least_conn")]).await;
        forwarder.set_endpoints(7, "db", vec![endpoint(&first), endpoint(&second)]);

        // Both endpoints idle: the tie breaks round robin onto the first.
        let mut held = connect(port).await;
        assert_eq!(served_by(&mut held).await, "a");
        assert_eq!(
            forwarder.inflight(7, "db").get(&first).copied(),
            Some(1),
            "a live connection must count against its endpoint"
        );

        // `held` is still open, so the next connection must avoid that endpoint.
        let mut next = connect(port).await;
        assert_eq!(served_by(&mut next).await, "b");

        // Closing a connection releases its slot again.
        drop(held);
        wait_until(
            "the closed connection to release its in-flight slot",
            || !forwarder.inflight(7, "db").contains_key(&first),
        )
        .await;
    }
}
