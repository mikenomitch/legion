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
use crate::structs::netpolicy::{ConnectFacts, Verdict, PROTOCOL_TCP};
use crate::structs::{parse_upstream_target, UpstreamTarget, UPSTREAM_LOOPBACK_ADDR};
use crate::worker::balance::{balancer, default_balancer, Balancer, Endpoint};
use crate::worker::netd::policy::{egress_connect_verdict, AllocIdentity};
use crate::worker::netd::{metrics as net_metrics, Denials, PolicyLookup};

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;
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
    endpoints: Mutex<Vec<Endpoint>>,
    /// Live connection count per endpoint address, for load-aware strategies.
    /// Entries are removed at zero so the map stays bounded by live endpoints.
    inflight: Mutex<HashMap<String, usize>>,
    /// Parsed external destination, when this upstream has one.
    target: Option<UpstreamTarget>,
    /// Policy, denial ledger, and TLS shared by every listener.
    context: Arc<ForwarderContext>,
}

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
}

impl ForwarderContext {
    pub fn new(cache: Arc<crate::worker::netd::NetPolicyCache>, denials: Arc<Denials>) -> Self {
        ForwarderContext {
            policy: Some(cache.clone() as Arc<dyn PolicyLookup>),
            cache: Some(cache),
            denials: Some(denials),
            tls: OnceLock::new(),
        }
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

    fn policy_for(
        &self,
        allocation_id: i64,
    ) -> Option<Arc<crate::structs::netpolicy::EffectivePolicy>> {
        self.policy.as_ref()?.policy_for(allocation_id)
    }

    fn identity_for(&self, allocation_id: i64) -> Option<AllocIdentity> {
        self.policy.as_ref()?.identity_for(allocation_id)
    }

    /// A rustls client trusting the Mozilla roots (webpki-roots). Built once
    /// per process; external upstreams that pin a CA are a later phase.
    fn tls_connector(&self) -> tokio_rustls::TlsConnector {
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
            endpoints: Mutex::new(Vec::new()),
            inflight: Mutex::new(HashMap::new()),
            target,
            context,
        }
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
        }
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
        match TcpListener::bind((UPSTREAM_LOOPBACK_ADDR, spec.local_port)).await {
            Ok(socket) => {
                debug!(
                    allocation_id = spec.allocation_id,
                    service = %spec.service,
                    local_port = spec.local_port,
                    balance = %shared.balancer.name(),
                    "Started upstream forwarder listener"
                );
                let task = tokio::spawn(run_listener(socket, shared.clone()));
                lock(&self.listeners).insert(
                    key,
                    Listener {
                        spec,
                        shared,
                        task: Some(task),
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
                        bind_warned: true,
                    },
                );
            }
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
    let _ = client.shutdown().await;
}

/// Dial an external upstream's fixed destination, originating TLS when the
/// scheme asks for it.
async fn forward_external(
    mut client: TcpStream,
    shared: Arc<UpstreamShared>,
    target: UpstreamTarget,
) {
    let facts = ConnectFacts {
        host: Some(target.host.as_str()),
        ip: target.host.parse().ok(),
        port: target.port,
        identity: None,
    };
    let destination = target.addr();
    if deny_if_policy_says_so(&shared, &facts, &destination).await {
        let _ = client.shutdown().await;
        return;
    }
    let _inflight_guard = InflightGuard::acquire(&shared, &destination);
    let upstream = match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(&destination)).await
    {
        Ok(Ok(upstream)) => upstream,
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
async fn deny_if_policy_says_so(
    shared: &Arc<UpstreamShared>,
    facts: &ConnectFacts<'_>,
    destination: &str,
) -> bool {
    let policy = shared.context.policy_for(shared.allocation_id);
    match egress_connect_verdict(policy.as_deref(), facts) {
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
            if let Some(denials) = shared.context.denials.as_ref() {
                let who = shared.context.identity_for(shared.allocation_id);
                denials.record(
                    shared.allocation_id,
                    who.as_ref(),
                    "egress",
                    PROTOCOL_TCP,
                    destination,
                    &source,
                    reason.as_deref(),
                    None,
                    "",
                    "",
                );
            }
            true
        }
    }
}

/// Splice the two halves and account the bytes.
async fn splice<U>(
    shared: &Arc<UpstreamShared>,
    destination: &str,
    mut client: TcpStream,
    mut upstream: U,
) where
    U: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
        Ok((to_upstream, to_client)) => {
            net_metrics::bytes("egress", to_upstream);
            net_metrics::bytes("ingress", to_client);
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
        }
    }

    fn endpoint(addr: &str) -> Endpoint {
        Endpoint {
            addr: addr.to_string(),
            allocation_id: 1,
            node_id: "node-a".into(),
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
