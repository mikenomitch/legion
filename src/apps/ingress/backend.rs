//! Backends: one routed service's endpoints, how one is picked for a
//! request, and the pooled connections to each (docs/ingress.md,
//! "Balancing").
//!
//! The endpoint set comes from the catalog every refresh
//! ([`Backend::update`]). An [`Endpoint`] is kept by address across
//! refreshes, so its in-flight count, ejection, and idle connections
//! outlive the poll that saw it. A pick is power of two choices on
//! in-flight requests; an endpoint whose dial fails is ejected for
//! [`EJECT_FOR`] unless nothing else is left.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;

use super::routes::Target;

/// How long an endpoint whose dial failed is skipped.
pub const EJECT_FOR: Duration = Duration::from_secs(10);
/// Most idle connections kept per endpoint.
pub const MAX_IDLE: usize = 32;
/// An idle connection older than this is closed rather than reused (under
/// the usual 60-75 s keep-alive of the servers behind it).
pub const IDLE_FOR: Duration = Duration::from_secs(50);

/// The request body type the pool sends: the client's own, streamed.
pub type UpstreamBody = Incoming;

/// Process start, the zero for the atomics' millisecond clocks.
fn epoch() -> Instant {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn now_ms() -> u64 {
    epoch().elapsed().as_millis() as u64
}

/// One endpoint of a backend.
#[derive(Debug)]
pub struct Endpoint {
    /// The host to dial (the node's address) and the port.
    pub host: String,
    pub port: u16,
    /// The allocation behind it, for the views.
    pub allocation_id: i64,
    pub node_id: String,
    in_flight: AtomicUsize,
    /// Skipped until this many ms after [`epoch`]; 0 when not ejected.
    ejected_until: AtomicU64,
    /// Idle HTTP/1.1 connections, newest last, with when each went idle.
    idle: Mutex<Vec<(SendRequest<UpstreamBody>, Instant)>>,
}

impl Endpoint {
    pub fn new(host: String, port: u16, allocation_id: i64, node_id: String) -> Endpoint {
        Endpoint {
            host,
            port,
            allocation_id,
            node_id,
            in_flight: AtomicUsize::new(0),
            ejected_until: AtomicU64::new(0),
            idle: Mutex::new(Vec::new()),
        }
    }

    /// `host:port`, bracketing an IPv6 literal.
    pub fn authority(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn ejected(&self) -> bool {
        self.ejected_until.load(Ordering::Relaxed) > now_ms()
    }

    /// Skip this endpoint for [`EJECT_FOR`].
    pub fn eject(&self) {
        let until = now_ms() + EJECT_FOR.as_millis() as u64;
        self.ejected_until.store(until, Ordering::Relaxed);
        // Its pooled connections go with it.
        if let Ok(mut idle) = self.idle.lock() {
            idle.clear();
        }
    }

    /// Count a request against this endpoint until the guard drops.
    pub fn begin(self: &Arc<Self>) -> InFlight {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlight(self.clone())
    }

    /// An idle connection that is still open, newest first.
    pub fn checkout(&self) -> Option<SendRequest<UpstreamBody>> {
        let mut idle = self.idle.lock().ok()?;
        while let Some((sender, since)) = idle.pop() {
            if !sender.is_closed() && since.elapsed() < IDLE_FOR {
                return Some(sender);
            }
        }
        None
    }

    /// Return a connection whose response has been read in full.
    pub fn checkin(&self, sender: SendRequest<UpstreamBody>) {
        if sender.is_closed() || self.ejected() {
            return;
        }
        if let Ok(mut idle) = self.idle.lock() {
            if idle.len() < MAX_IDLE {
                idle.push((sender, Instant::now()));
            }
        }
    }

    /// Idle connections held, for tests and the views.
    pub fn idle_count(&self) -> usize {
        self.idle.lock().map(|idle| idle.len()).unwrap_or(0)
    }
}

/// A request in flight on an endpoint.
#[derive(Debug)]
pub struct InFlight(Arc<Endpoint>);

impl InFlight {
    pub fn endpoint(&self) -> &Arc<Endpoint> {
        &self.0
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// What the last catalog refresh said about a backend, for the views.
#[derive(Debug, Clone, Default)]
pub struct BackendHealth {
    /// Endpoints routed to (healthy, and reachable without the mesh).
    pub routed: usize,
    /// Every endpoint the catalog lists for the target, healthy or not.
    pub total: usize,
    /// Healthy endpoints skipped because their port takes only mesh mTLS.
    pub mesh_only: usize,
    /// The last refresh's error, while the last good set is kept.
    pub error: Option<String>,
    /// Whether any refresh has answered yet.
    pub refreshed: bool,
}

/// An endpoint as the catalog lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEndpoint {
    pub host: String,
    pub port: u16,
    pub allocation_id: i64,
    pub node_id: String,
    pub healthy: bool,
    pub mesh_tls: bool,
}

/// One routed service.
#[derive(Debug)]
pub struct Backend {
    pub target: Target,
    endpoints: RwLock<Arc<Vec<Arc<Endpoint>>>>,
    health: Mutex<BackendHealth>,
    /// Round-robin cursor for the tie-break-free first choice.
    cursor: AtomicUsize,
}

impl Backend {
    pub fn new(target: Target) -> Backend {
        Backend {
            target,
            endpoints: RwLock::new(Arc::new(Vec::new())),
            health: Mutex::new(BackendHealth::default()),
            cursor: AtomicUsize::new(0),
        }
    }

    /// The endpoints routed to now.
    pub fn endpoints(&self) -> Arc<Vec<Arc<Endpoint>>> {
        self.endpoints
            .read()
            .map(|endpoints| endpoints.clone())
            .unwrap_or_default()
    }

    pub fn health(&self) -> BackendHealth {
        self.health
            .lock()
            .map(|health| health.clone())
            .unwrap_or_default()
    }

    /// Replace the endpoint set with the healthy, non-mesh ones in
    /// `catalog`, keeping each endpoint already known by address.
    pub fn update(&self, catalog: &[CatalogEndpoint]) {
        let current = self.endpoints();
        let mut next = Vec::with_capacity(catalog.len());
        let mut mesh_only = 0;
        for entry in catalog {
            if !entry.healthy || entry.host.is_empty() || entry.port == 0 {
                continue;
            }
            if entry.mesh_tls {
                mesh_only += 1;
                continue;
            }
            let known = current.iter().find(|endpoint| {
                endpoint.host == entry.host
                    && endpoint.port == entry.port
                    && endpoint.allocation_id == entry.allocation_id
            });
            next.push(match known {
                Some(endpoint) => endpoint.clone(),
                None => Arc::new(Endpoint::new(
                    entry.host.clone(),
                    entry.port,
                    entry.allocation_id,
                    entry.node_id.clone(),
                )),
            });
        }
        let routed = next.len();
        if let Ok(mut endpoints) = self.endpoints.write() {
            *endpoints = Arc::new(next);
        }
        if let Ok(mut health) = self.health.lock() {
            *health = BackendHealth {
                routed,
                total: catalog.len(),
                mesh_only,
                error: None,
                refreshed: true,
            };
        }
    }

    /// Record a failed refresh; the last good endpoints stay.
    pub fn refresh_failed(&self, error: String) {
        if let Ok(mut health) = self.health.lock() {
            health.error = Some(error);
        }
    }

    /// Pick an endpoint for a request, skipping `tried`: power of two
    /// choices on in-flight requests among the endpoints not ejected, or
    /// among all of them when every one is.
    pub fn pick(&self, tried: &[Arc<Endpoint>]) -> Option<Arc<Endpoint>> {
        let endpoints = self.endpoints();
        let untried =
            |endpoint: &&Arc<Endpoint>| !tried.iter().any(|seen| Arc::ptr_eq(seen, endpoint));
        let mut candidates: Vec<&Arc<Endpoint>> = endpoints
            .iter()
            .filter(untried)
            .filter(|endpoint| !endpoint.ejected())
            .collect();
        if candidates.is_empty() {
            candidates = endpoints.iter().filter(untried).collect();
        }
        match candidates.len() {
            0 => None,
            1 => Some(candidates[0].clone()),
            len => {
                // Two distinct candidates: one from a rotating cursor, one at
                // random, so even an idle backend spreads its requests.
                use rand::Rng;
                let first = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
                let offset = rand::thread_rng().gen_range(1..len);
                let second = (first + offset) % len;
                let (a, b) = (candidates[first], candidates[second]);
                Some(if b.in_flight() < a.in_flight() { b } else { a }.clone())
            }
        }
    }
}

/// A response body from an upstream connection that goes back to its
/// endpoint's pool when it has been read to the end, and holds the request's
/// in-flight count until then.
pub struct PooledBody {
    inner: Incoming,
    sender: Option<SendRequest<UpstreamBody>>,
    guard: Option<InFlight>,
}

impl PooledBody {
    /// `sender` is `None` when the connection must not be reused (the
    /// response said `Connection: close`).
    pub fn new(
        inner: Incoming,
        sender: Option<SendRequest<UpstreamBody>>,
        guard: InFlight,
    ) -> Self {
        PooledBody {
            inner,
            sender,
            guard: Some(guard),
        }
    }

    fn finish(&mut self) {
        if let (Some(sender), Some(guard)) = (self.sender.take(), self.guard.as_ref()) {
            guard.endpoint().checkin(sender);
        }
        self.guard = None;
    }
}

impl Drop for PooledBody {
    /// A body the server never polled to its end (HEAD, `204`, `304`: hyper
    /// sees `is_end_stream` and writes no body) is still a finished
    /// exchange: its connection goes back to the pool too. One dropped
    /// mid-stream (the client went away) is not, and closes.
    fn drop(&mut self) {
        if self.sender.is_some() && hyper::body::Body::is_end_stream(&self.inner) {
            self.finish();
        }
    }
}

impl hyper::body::Body for PooledBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        let this = self.get_mut();
        let polled = std::pin::Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            std::task::Poll::Ready(None) => this.finish(),
            std::task::Poll::Ready(Some(Err(_))) => {
                // A broken body breaks the connection: never reuse it.
                this.sender = None;
                this.guard = None;
            }
            _ => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend_with(n: usize) -> Backend {
        let backend = Backend::new(Target {
            job_id: "shop".into(),
            namespace: "default".into(),
            task: "web".into(),
            service: "web".into(),
        });
        let catalog: Vec<CatalogEndpoint> = (0..n)
            .map(|i| CatalogEndpoint {
                host: "10.0.0.1".into(),
                port: 20000 + i as u16,
                allocation_id: i as i64,
                node_id: "n1".into(),
                healthy: true,
                mesh_tls: false,
            })
            .collect();
        backend.update(&catalog);
        backend
    }

    #[test]
    fn updates_keep_known_endpoints_and_drop_unroutable_ones() {
        let backend = backend_with(2);
        let before = backend.endpoints();
        let mut catalog = vec![
            CatalogEndpoint {
                host: "10.0.0.1".into(),
                port: 20000,
                allocation_id: 0,
                node_id: "n1".into(),
                healthy: true,
                mesh_tls: false,
            },
            CatalogEndpoint {
                host: "10.0.0.2".into(),
                port: 21000,
                allocation_id: 7,
                node_id: "n2".into(),
                healthy: false,
                mesh_tls: false,
            },
        ];
        catalog.push(CatalogEndpoint {
            host: "10.0.0.3".into(),
            port: 22000,
            allocation_id: 8,
            node_id: "n3".into(),
            healthy: true,
            mesh_tls: true,
        });
        backend.update(&catalog);
        let after = backend.endpoints();
        assert_eq!(after.len(), 1);
        assert!(Arc::ptr_eq(&before[0], &after[0]), "kept by address");
        let health = backend.health();
        assert_eq!((health.routed, health.total, health.mesh_only), (1, 3, 1));
    }

    #[test]
    fn picks_prefer_fewer_requests_and_skip_ejected_and_tried_endpoints() {
        let backend = backend_with(2);
        let endpoints = backend.endpoints();
        let _busy: Vec<InFlight> = (0..5).map(|_| endpoints[0].begin()).collect();
        for _ in 0..20 {
            let picked = backend.pick(&[]).unwrap();
            assert!(Arc::ptr_eq(&picked, &endpoints[1]), "the idle one wins");
        }
        endpoints[1].eject();
        assert!(Arc::ptr_eq(&backend.pick(&[]).unwrap(), &endpoints[0]));
        // Every endpoint ejected: still pick one rather than fail.
        endpoints[0].eject();
        assert!(backend.pick(&[]).is_some());
        assert!(backend.pick(&endpoints).is_none(), "all tried");
        assert_eq!(endpoints[0].in_flight(), 5);
    }

    #[test]
    fn an_idle_backend_spreads_requests() {
        let backend = backend_with(3);
        let endpoints = backend.endpoints();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..30 {
            let picked = backend.pick(&[]).unwrap();
            seen.insert(picked.port);
        }
        assert_eq!(seen.len(), endpoints.len());
    }

    #[test]
    fn in_flight_counts_follow_the_guard() {
        let backend = backend_with(1);
        let endpoint = backend.endpoints()[0].clone();
        let guard = endpoint.begin();
        assert_eq!(endpoint.in_flight(), 1);
        drop(guard);
        assert_eq!(endpoint.in_flight(), 0);
        assert_eq!(endpoint.authority(), "10.0.0.1:20000");
        let v6 = Endpoint::new("fd00::1".into(), 80, 1, "n".into());
        assert_eq!(v6.authority(), "[fd00::1]:80");
    }
}
