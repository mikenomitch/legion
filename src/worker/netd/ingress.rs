//! Ingress listeners: the datapath in front of a task's advertised ports.
//!
//! When a task (or a policy matching it) declares an `ingress` chain, the
//! advertised host port belongs to the datapath and the task binds a
//! private twin (`AllocPort::private_port`, what `LEGION_PORT_<label>`
//! carries). This module binds the advertised port in the *host* namespace
//! — consumers reach it from other nodes — and, per connection:
//!
//! 1. judges the peer against the effective ingress chain with what is
//!    known at connect time (peer address, the advertised port; identity
//!    arrives with mTLS);
//! 2. for a gated task, fails closed while policy is pending or the edge-boot
//!    gate is shut;
//! 3. forwards to the task — spliced (L4), or parsed as HTTP/1.1 when the
//!    chain is `protocol = "http"` so request-time rules apply — the way
//!    [`TaskDial`] says: the private port on loopback (inside the task's
//!    namespace when it has one), or a sandbox guest over host-initiated
//!    vsock or at its address on the bridge.
//!
//! A sandbox guest has its own network stack, so the datapath fronts every
//! port a gated or TAP-wired sandbox advertises, with or without an ingress
//! chain: nothing else on the node can reach the guest's ports.
//!
//! Health checks and the catalog keep targeting the advertised port, so a
//! check exercises the same path a consumer does. A deny-everything ingress
//! chain therefore fails the task's own health check; `legion net doctor`
//! names that case.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, Verdict, PROTOCOL_TCP, TLS_MUTUAL, TLS_SERVER};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::handlers::{self, HopFacts, HopOutcome};
use crate::worker::netd::http::{self, HttpSettings};
use crate::worker::netd::listen::{dial_tcp, spawn_accept_loop, ListenAddr, Stream};
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::netns;
use crate::worker::netd::policy::ingress_connect_verdict;
use crate::worker::netd::proxyproto::{self, tlv, Prefixed};
use crate::worker::netd::splice::{self as flow, SpliceStream};
use crate::worker::netd::tls::{self as net_tls, PeerIdentity};
use crate::worker::netd::vsock;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// How long a TCP dial to a task may take.
const TASK_DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// How an allowed ingress connection reaches the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskDial {
    /// `127.0.0.1:<port>`, inside the task's gated namespace when it has
    /// one: `exec` and `container` tasks.
    Loopback { netns: Option<PathBuf> },
    /// A gated sandbox guest, which has no NIC: host-initiated vsock
    /// through the VMM's base socket in the guest's sandbox dir
    /// (`<alloc>/.sandbox/vsock.sock`, `CONNECT <port>`), to the guest
    /// agent, which relays to the workload's loopback port.
    Vsock { scratch: PathBuf },
    /// A TAP-wired sandbox guest: TCP to the address the worker leased it
    /// on its bridge (read from the sandbox dir).
    Bridge { scratch: PathBuf },
}

impl TaskDial {
    /// A gated listener fails closed while policy is pending or the boot
    /// gate is shut: the task's namespace, or a gated sandbox's vsock.
    pub fn gated(&self) -> bool {
        matches!(
            self,
            TaskDial::Loopback { netns: Some(_) } | TaskDial::Vsock { .. }
        )
    }
}

/// One advertised port's way to the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTarget {
    pub dial: TaskDial,
    /// The port label (`network.ports[].name`).
    pub label: String,
    /// The port the allocation's task binds (`AllocPort::bind_port`).
    pub port: u16,
}

impl TaskTarget {
    /// The port the workload listens on now. A sandbox guest restored from
    /// a memory checkpoint keeps the ports it was captured with, which its
    /// sandbox dir records; everything else binds the allocation's own.
    /// Read per connection, because a restore writes the record after this
    /// listener is already bound.
    pub fn port(&self) -> u16 {
        match &self.dial {
            TaskDial::Loopback { .. } => self.port,
            TaskDial::Vsock { scratch } | TaskDial::Bridge { scratch } => {
                crate::task_plugins::sandbox_guest_port(scratch, &self.label).unwrap_or(self.port)
            }
        }
    }

    /// The TCP address a loopback or bridge dial goes to. A TAP guest's
    /// address is read from its lease per connection, since a restore or a
    /// fork leases a new one after this listener is bound.
    pub fn tcp_addr(&self) -> R<SocketAddr> {
        let port = self.port();
        match &self.dial {
            TaskDial::Loopback { .. } => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
            TaskDial::Bridge { scratch } => {
                let address =
                    crate::task_plugins::sandbox_guest_address(scratch).with_context(|| {
                        format!(
                            "the sandbox guest has no address leased in {} yet",
                            scratch.display()
                        )
                    })?;
                Ok(SocketAddr::new(IpAddr::V4(address), port))
            }
            TaskDial::Vsock { .. } => bail!("a gated sandbox guest is reached over vsock, not TCP"),
        }
    }

    /// Open a connection to the task.
    pub async fn connect(&self) -> R<TaskStream> {
        match &self.dial {
            TaskDial::Loopback { netns } => {
                let stream = netns::connect_tcp(netns.clone(), self.tcp_addr()?).await?;
                let _ = stream.set_nodelay(true);
                Ok(TaskStream::Tcp(stream))
            }
            TaskDial::Bridge { .. } => {
                let addr = self.tcp_addr()?;
                let stream = dial_tcp(addr, TASK_DIAL_TIMEOUT)
                    .await
                    .with_context(|| format!("connect {addr}"))?;
                let _ = stream.set_nodelay(true);
                Ok(TaskStream::Tcp(stream))
            }
            TaskDial::Vsock { scratch } => {
                let base = crate::task_plugins::vsock_socket_base(scratch);
                Ok(TaskStream::Unix(
                    vsock::dial(&base, u32::from(self.port())).await?,
                ))
            }
        }
    }
}

/// A connection to the task: TCP (loopback, a namespace, a bridge), or a
/// vsock stream through a sandbox's VMM.
pub type TaskStream = Stream;

/// One advertised port the datapath fronts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressSpec {
    pub allocation_id: i64,
    /// The port label (`network.ports[].name`).
    pub label: String,
    /// The advertised host port the datapath binds.
    pub host_port: u16,
    /// The port the task binds: the private twin with an ingress chain,
    /// else (a sandbox guest, whose port space is its own) the advertised
    /// port's own number.
    pub private_port: u16,
    /// How a connection reaches the task.
    pub dial: TaskDial,
    /// Bind address; `None` is every interface.
    pub bind: Option<IpAddr>,
}

impl IngressSpec {
    fn target(&self) -> TaskTarget {
        TaskTarget {
            dial: self.dial.clone(),
            label: self.label.clone(),
            port: self.private_port,
        }
    }
}

type IngressKey = (i64, String);

struct Shared {
    spec: IngressSpec,
    target: TaskTarget,
    context: Arc<ForwarderContext>,
    http: OnceLock<Arc<HttpSettings>>,
}

impl Shared {
    fn gated(&self) -> bool {
        self.spec.dial.gated()
    }

    fn http_settings(self: &Arc<Self>) -> Option<Arc<HttpSettings>> {
        let policy_http = self
            .context
            .policy_for(self.spec.allocation_id)
            .and_then(|p| p.ingress.as_ref().map(|c| c.is_http()))
            .unwrap_or(false);
        if !policy_http {
            return None;
        }
        Some(
            self.http
                .get_or_init(|| {
                    Arc::new(HttpSettings::for_ingress(
                        self.spec.allocation_id,
                        self.target.clone(),
                        self.context.clone(),
                        self.context.log_dir_for(self.spec.allocation_id),
                    ))
                })
                .clone(),
        )
    }
}

struct Listener {
    spec: IngressSpec,
    task: Option<JoinHandle<()>>,
    problem: Option<String>,
}

/// The node's ingress listeners, reconciled from the desired set.
pub struct IngressListeners {
    context: Arc<ForwarderContext>,
    listeners: Mutex<HashMap<IngressKey, Listener>>,
}

impl IngressListeners {
    pub fn new(context: Arc<ForwarderContext>) -> Self {
        IngressListeners {
            context,
            listeners: Mutex::new(HashMap::new()),
        }
    }

    pub async fn sync(&self, desired: &[IngressSpec]) {
        let mut wanted: HashMap<IngressKey, IngressSpec> = HashMap::new();
        for spec in desired {
            wanted.insert((spec.allocation_id, spec.label.clone()), spec.clone());
        }
        let mut stopped = Vec::new();
        let mut pending = Vec::new();
        {
            let mut listeners = lock(&self.listeners);
            listeners.retain(|key, listener| {
                if wanted.get(key).is_some_and(|spec| *spec == listener.spec) {
                    return true;
                }
                if let Some(task) = listener.task.take() {
                    stopped.push(task);
                }
                false
            });
            for (key, spec) in wanted {
                match listeners.get(&key) {
                    Some(listener) if listener.task.as_ref().is_some_and(|t| !t.is_finished()) => {}
                    _ => pending.push(spec),
                }
            }
        }
        for task in stopped {
            task.abort();
            let _ = task.await;
        }
        for spec in pending {
            self.start(spec).await;
        }
    }

    /// (allocation, label) pairs whose listener is bound.
    pub fn active(&self) -> Vec<(i64, String)> {
        let listeners = lock(&self.listeners);
        let mut out: Vec<(i64, String)> = listeners
            .values()
            .filter(|l| l.task.as_ref().is_some_and(|t| !t.is_finished()))
            .map(|l| (l.spec.allocation_id, l.spec.label.clone()))
            .collect();
        out.sort();
        out
    }

    /// Bind problems per allocation, for the status report.
    pub fn problems(&self, allocation_id: i64) -> Vec<String> {
        lock(&self.listeners)
            .values()
            .filter(|l| l.spec.allocation_id == allocation_id)
            .filter_map(|l| l.problem.clone())
            .collect()
    }

    async fn start(&self, spec: IngressSpec) {
        let listen = ListenAddr::host(SocketAddr::new(
            spec.bind.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            spec.host_port,
        ));
        let key = (spec.allocation_id, spec.label.clone());
        match listen.bind().await {
            Ok(listener) => {
                debug!(
                    allocation_id = spec.allocation_id,
                    label = %spec.label,
                    host_port = spec.host_port,
                    private_port = spec.private_port,
                    "Started ingress listener"
                );
                let shared = Arc::new(Shared {
                    target: spec.target(),
                    spec: spec.clone(),
                    context: self.context.clone(),
                    http: OnceLock::new(),
                });
                let task = spawn_accept_loop(listener, "ingress", move |client, peer| {
                    let shared = shared.clone();
                    async move {
                        // Ingress listens on TCP in the host namespace only.
                        if let (Stream::Tcp(client), Some(peer)) = (client, peer) {
                            handle(client, peer, shared).await;
                        }
                    }
                });
                lock(&self.listeners).insert(
                    key,
                    Listener {
                        spec,
                        task: Some(task),
                        problem: None,
                    },
                );
            }
            Err(err) => {
                let problem = format!(
                    "port {} ({}) ingress listener: {err:#}",
                    spec.host_port, spec.label
                );
                warn!(
                    allocation_id = spec.allocation_id,
                    label = %spec.label,
                    host_port = spec.host_port,
                    error = ?err,
                    "Failed to bind ingress listener; retrying on the next heartbeat"
                );
                lock(&self.listeners).insert(
                    key,
                    Listener {
                        spec,
                        task: None,
                        problem: Some(problem),
                    },
                );
            }
        }
    }
}

impl Drop for IngressListeners {
    fn drop(&mut self) {
        for listener in lock(&self.listeners).values_mut() {
            if let Some(task) = listener.task.take() {
                task.abort();
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

/// How long a TLS handshake may take before the connection is dropped.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

async fn handle(mut client: TcpStream, peer: SocketAddr, shared: Arc<Shared>) {
    let allocation_id = shared.spec.allocation_id;
    let policy = shared.context.policy_for(allocation_id);
    let block = if shared.gated() {
        if netns::gate_is_shut(allocation_id) {
            Some((
                "gate".to_string(),
                Some("network gate is shut until the placement is ratified".to_string()),
            ))
        } else if policy.is_none() && !shared.context.policy_known(allocation_id) {
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

    // TLS termination, when the effective ingress chain asks for it.
    let tls = policy
        .as_ref()
        .and_then(|p| p.ingress.as_ref())
        .and_then(|chain| chain.tls.clone())
        .filter(|tls| matches!(tls.mode.as_str(), TLS_SERVER | TLS_MUTUAL));
    let Some(tls) = tls else {
        serve_accepted(client, Vec::new(), peer, None, shared, block, policy).await;
        return;
    };
    let config = match shared
        .context
        .ingress_server_config(allocation_id, &shared.spec.label, &tls)
    {
        Ok(config) => config,
        Err(err) => {
            debug!(
                allocation_id,
                label = %shared.spec.label,
                error = %err,
                "Ingress TLS material is not usable; refusing the connection"
            );
            net_metrics::connection("ingress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let mut stream =
        match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(client)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                debug!(allocation_id, peer = %peer, error = ?err, "Ingress TLS handshake failed");
                net_metrics::connection("ingress", PROTOCOL_TCP, "error");
                return;
            }
            Err(_) => {
                net_metrics::connection("ingress", PROTOCOL_TCP, "error");
                return;
            }
        };
    // A `legion://node/` URI makes a peer a node, trusted to name the
    // workload it forwards for, only on a chain verified against the
    // cluster's CA. An operator's `clientCa` may issue whatever URIs its
    // clients ask for, so there it is just that client's identity.
    let cluster_verified = tls.client_ca.as_deref() == Some("legion")
        || (tls.cert_from_mesh() && tls.cert_kv_path().is_none());
    let peer_identity = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(net_tls::peer_identity)
        .map(|identity| match identity {
            PeerIdentity::Node(node) if !cluster_verified => {
                PeerIdentity::Other(format!("legion://node/{node}"))
            }
            other => other,
        });
    let (identity, leftover) = match peer_identity {
        // A node peer names the workload it forwards for in a PROXY v2
        // header — trusted because the peer proved it is a node.
        Some(PeerIdentity::Node(node)) => match proxyproto::read_header(&mut stream).await {
            Ok((Some(header), leftover)) => (
                header
                    .text(tlv::IDENTITY)
                    .filter(|s| !s.is_empty())
                    .or_else(|| Some(format!("node:{node}"))),
                leftover,
            ),
            Ok((None, leftover)) => (Some(format!("node:{node}")), leftover),
            Err(_) => return,
        },
        Some(PeerIdentity::Other(identity)) => (Some(identity), Vec::new()),
        None => (None, Vec::new()),
    };
    if !net_tls::identity_allowed(&tls.require_identity, identity.as_deref()) {
        let reason = format!(
            "peer identity {} is not in tls.requireIdentity",
            identity.as_deref().unwrap_or("(none)")
        );
        net_metrics::connection("ingress", PROTOCOL_TCP, "deny");
        shared.context.record_denial_for(
            allocation_id,
            identity.as_deref(),
            "ingress",
            PROTOCOL_TCP,
            &peer.to_string(),
            "tls/requireIdentity",
            Some(&reason),
            "",
            "",
        );
        let _ = stream.shutdown().await;
        return;
    }
    serve_accepted(stream, leftover, peer, identity, shared, block, policy).await;
}

/// The rest of the ingress path once the transport is settled: the chain
/// at connect time, external handlers, then HTTP mode or a splice to the
/// task's private port.
async fn serve_accepted<S>(
    client: S,
    leftover: Vec<u8>,
    peer: SocketAddr,
    identity: Option<String>,
    shared: Arc<Shared>,
    block: Option<(String, Option<String>)>,
    policy: Option<Arc<crate::structs::netpolicy::EffectivePolicy>>,
) where
    S: SpliceStream + Send + 'static,
{
    let allocation_id = shared.spec.allocation_id;
    // Counted as open (and its bytes as activity) for the idle policy's
    // `connections` and `network` signals, from here until it closes.
    let mut client = shared
        .context
        .activity()
        .track(allocation_id, Prefixed::new(leftover, client));
    let peer_text = peer.to_string();
    let facts = ConnectFacts {
        host: None,
        ip: Some(peer.ip()),
        port: shared.spec.host_port,
        identity: identity.as_deref(),
    };
    let verdict = match block {
        Some((source, reason)) => Verdict::Deny { source, reason },
        None => ingress_connect_verdict(policy.as_deref(), &facts),
    };
    if let Verdict::Deny { source, reason } = verdict {
        net_metrics::connection("ingress", PROTOCOL_TCP, "deny");
        shared.context.record_denial_for(
            allocation_id,
            identity.as_deref(),
            "ingress",
            PROTOCOL_TCP,
            &peer_text,
            &source,
            reason.as_deref(),
            "",
            "",
        );
        let _ = client.shutdown().await;
        return;
    }

    // External handlers at connect time (HTTP mode runs them with the
    // request head instead).
    let hop_facts = HopFacts {
        allocation_id,
        direction: "ingress",
        protocol: PROTOCOL_TCP,
        destination: &peer_text,
        host: None,
        ip: Some(peer.ip()),
        port: shared.spec.host_port,
        identity: identity.as_deref(),
        ..Default::default()
    };
    let hops = handlers::hops_for(&shared.context, allocation_id, "ingress", None, &[]);
    if !hops.is_empty() && shared.http_settings().is_none() {
        match handlers::run(&shared.context, &hops, &hop_facts).await {
            HopOutcome::Allow => {}
            HopOutcome::Deny { source, reason } => {
                net_metrics::connection("ingress", PROTOCOL_TCP, "deny");
                shared.context.record_denial_for(
                    allocation_id,
                    identity.as_deref(),
                    "ingress",
                    PROTOCOL_TCP,
                    &peer_text,
                    &source,
                    reason.as_deref(),
                    "",
                    "",
                );
                let _ = client.shutdown().await;
                return;
            }
            HopOutcome::Proxy(target) => {
                // The proxy reaches the task on the private port named in
                // the header's DESTINATION TLV.
                let private = format!("127.0.0.1:{}", shared.spec.private_port);
                // The handler's target comes from policy a job may append
                // to, so the host guard judges it: an ingress hop can't be
                // used to dial the node's own services either.
                let mut proxy = match crate::worker::netd::guard::dial_guarded(
                    &shared.context,
                    &target.addr,
                    Duration::from_secs(2),
                )
                .await
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        shared.context.note_handler(
                            allocation_id,
                            &target.source,
                            crate::structs::netpolicy::HANDLER_PROXY,
                            &target.addr,
                            Err(format!("dial {}: {err}", target.addr)),
                        );
                        net_metrics::connection("ingress", PROTOCOL_TCP, "error");
                        let _ = client.shutdown().await;
                        return;
                    }
                };
                if target.proxy_protocol {
                    let header_facts = HopFacts {
                        destination: &private,
                        ..hop_facts.clone()
                    };
                    let header = handlers::proxy_header(
                        &shared.context,
                        &header_facts,
                        Some(peer),
                        private.parse().ok(),
                    );
                    if proxy.write_all(&header).await.is_err() {
                        return;
                    }
                }
                net_metrics::connection("ingress", PROTOCOL_TCP, "allow");
                let transfer = flow::bidirectional(&mut client, &mut proxy).await;
                net_metrics::bytes("ingress", transfer.sent);
                net_metrics::bytes("egress", transfer.received);
                if let Some(err) = transfer.error {
                    debug!(error = ?err, "Ingress hand-off ended with an error");
                }
                return;
            }
        }
    }

    if let Some(settings) = shared.http_settings() {
        http::serve_with(
            client,
            settings,
            http::ConnInfo {
                identity,
                peer: Some(peer),
            },
        )
        .await;
        return;
    }

    let mut task = match shared.target.connect().await {
        Ok(stream) => stream,
        Err(err) => {
            debug!(
                allocation_id,
                label = %shared.spec.label,
                error = ?err,
                "Ingress could not reach the task"
            );
            net_metrics::connection("ingress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    net_metrics::connection("ingress", PROTOCOL_TCP, "allow");
    let transfer = flow::bidirectional(&mut client, &mut task).await;
    net_metrics::bytes("ingress", transfer.sent);
    net_metrics::bytes("egress", transfer.received);
    if let Some(err) = transfer.error {
        debug!(error = ?err, "Ingress connection ended with an error");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::AllocNetPolicy;
    use crate::worker::netd::{Denials, NetPolicyCache};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UnixStream};

    async fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// A "task" on a private port that answers HTTP with the path it saw.
    async fn task_on(port: u16) {
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let first = String::from_utf8_lossy(&buf[..n])
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    let body = format!("task saw {first}");
                    let _ = stream
                        .write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes())
                        .await;
                });
            }
        });
    }

    fn policy(ingress: serde_json::Value) -> crate::structs::netpolicy::EffectivePolicy {
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "in", "enforcement": "require", "ingress": ingress
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        merge_policies(&[object], InlinePolicy::default())
    }

    async fn request(port: u16, raw: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        String::from_utf8_lossy(&out).to_string()
    }

    #[tokio::test]
    async fn ingress_fronts_the_private_port_with_l4_and_http_rules() {
        let private = free_port().await;
        task_on(private).await;
        let host_port = free_port().await;

        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        // HTTP ingress: /public allowed from anywhere, /admin denied.
        let effective = policy(json!({
            "protocol": "http", "policy": "deny",
            "rules": [
                { "match": { "path": "/admin/**" }, "action": "deny", "reason": "not externally reachable" },
                { "match": { "cidr": "127.0.0.0/8", "path": "/public/**" }, "action": "allow" }
            ]
        }));
        let hash = effective.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 11,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, effective).unwrap();
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));
        let ingress = IngressListeners::new(context);
        ingress
            .sync(&[IngressSpec {
                allocation_id: 11,
                label: "http".into(),
                host_port,
                private_port: private,
                dial: TaskDial::Loopback { netns: None },
                bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            }])
            .await;
        assert_eq!(ingress.active(), vec![(11, "http".to_string())]);

        let ok = request(
            host_port,
            "GET /public/x HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(ok.contains("task saw GET /public/x"), "{ok}");
        let denied = request(
            host_port,
            "GET /admin/users HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(denied.starts_with("HTTP/1.1 403"), "{denied}");
        assert!(
            denied
                .to_ascii_lowercase()
                .contains("legion-denied-by: policy/in#0"),
            "{denied}"
        );
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.direction == "ingress" && d.path == "/admin/users"),
            "{drained:?}"
        );

        // Re-sync with a changed spec stops and restarts the listener.
        ingress.sync(&[]).await;
        assert!(ingress.active().is_empty());
        assert!(TcpStream::connect(("127.0.0.1", host_port)).await.is_err());
    }

    /// Mesh mTLS end to end: node B fronts a task with a mutual, mesh-issued
    /// ingress chain; node A dials with its node certificate and names the
    /// workload in a PROXY v2 header. The chain sees that identity, so an
    /// `identity` rule and `requireIdentity` decide; a plain client and a
    /// workload outside `requireIdentity` are refused.
    #[tokio::test]
    async fn mesh_mtls_ingress_attributes_the_peer_workload() {
        use crate::manager::mesh_ca::{generate_ca, issue_node_cert, node_dns_name};
        use crate::worker::netd::proxyproto::{self, tlv};
        use crate::worker::netd::tls::MeshIdentity;

        let private = free_port().await;
        task_on(private).await;
        let host_port = free_port().await;

        let (ca_pem, ca_key) = generate_ca().unwrap();
        let (a_cert, a_key, a_exp) = issue_node_cert(&ca_pem, &ca_key, "node-a").unwrap();
        let (b_cert, b_key, b_exp) = issue_node_cert(&ca_pem, &ca_key, "node-b").unwrap();

        // Node B: the ingress side.
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let effective = policy(json!({
            "policy": "deny",
            "tls": { "mode": "mutual", "certFrom": "legion", "clientCa": "legion",
                     "requireIdentity": ["auto:web:*", "auto:ops:*"] },
            "rules": [ { "when": { "identity": "auto:web:*" }, "action": "allow" } ]
        }));
        let hash = effective.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 21,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, effective).unwrap();
        let context_b =
            Arc::new(ForwarderContext::new(cache, denials.clone()).with_node_id("node-b".into()));
        context_b
            .set_mesh_identity(
                MeshIdentity::from_pem("node-b", &b_cert, &b_key, &ca_pem, b_exp).unwrap(),
            )
            .unwrap();
        let ingress = IngressListeners::new(context_b.clone());
        ingress
            .sync(&[IngressSpec {
                allocation_id: 21,
                label: "http".into(),
                host_port,
                private_port: private,
                dial: TaskDial::Loopback { netns: None },
                bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            }])
            .await;
        assert_eq!(ingress.active(), vec![(21, "http".to_string())]);
        assert_eq!(context_b.ingress_tls_status(21), (true, String::new()));

        // Node A: a consumer's datapath dialing the advertised port.
        let context_a = Arc::new(
            ForwarderContext::new(Arc::new(NetPolicyCache::new()), Arc::new(Denials::new()))
                .with_node_id("node-a".into()),
        );
        context_a
            .set_mesh_identity(
                MeshIdentity::from_pem("node-a", &a_cert, &a_key, &ca_pem, a_exp).unwrap(),
            )
            .unwrap();
        let dial = |identity: &'static str| {
            let context_a = context_a.clone();
            async move {
                let connector = context_a.mesh_connector().unwrap();
                let stream = TcpStream::connect(("127.0.0.1", host_port)).await.unwrap();
                let name = crate::worker::netd::tls::server_name(&node_dns_name("node-b")).unwrap();
                let mut tls = connector.connect(name, stream).await.unwrap();
                let header =
                    proxyproto::encode(None, None, &[(tlv::IDENTITY, identity.as_bytes())]);
                tls.write_all(&header).await.unwrap();
                tls.write_all(b"GET /hello HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ =
                    tokio::time::timeout(Duration::from_secs(3), tls.read_to_end(&mut out)).await;
                String::from_utf8_lossy(&out).to_string()
            }
        };

        let ok = dial("auto:web:default:server").await;
        assert!(ok.contains("task saw GET /hello"), "{ok}");

        // Allowed by requireIdentity, but no rule allows `ops`: the chain's
        // deny fallthrough, attributed to the workload.
        let denied = dial("auto:ops:default:cli").await;
        assert!(denied.is_empty(), "{denied}");
        // Outside requireIdentity entirely.
        let refused = dial("auto:db:default:pg").await;
        assert!(refused.is_empty(), "{refused}");
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.identity == "auto:ops:default:cli" && d.source == "fallthrough"),
            "{drained:?}"
        );
        assert!(
            drained
                .iter()
                .any(|d| d.identity == "auto:db:default:pg" && d.source == "tls/requireIdentity"),
            "{drained:?}"
        );

        // A plain TCP client never gets past the handshake.
        let plain = request(host_port, "GET /hello HTTP/1.1\r\nHost: svc\r\n\r\n").await;
        assert!(!plain.contains("task saw"), "{plain}");
    }

    /// `certFrom = { kv }`: the listener serves the certificate the node
    /// fetched from KV, a newer version is presented to the next connection
    /// with no listener restart, and a failed renewal keeps serving the last
    /// good certificate while reporting the problem.
    #[tokio::test]
    async fn kv_backed_ingress_serves_and_hot_swaps_the_certificate_test() {
        use crate::manager::mesh_ca::{generate_ca, issue_node_cert, node_dns_name};
        use crate::worker::netd::tls::{certs_from_pem, server_name, KvCertificate};

        let private = free_port().await;
        task_on(private).await;
        let host_port = free_port().await;
        let (ca_pem, ca_key) = generate_ca().unwrap();
        let (cert_1, key_1, _) = issue_node_cert(&ca_pem, &ca_key, "edge").unwrap();
        let (cert_2, key_2, _) = issue_node_cert(&ca_pem, &ca_key, "edge").unwrap();

        let cache = Arc::new(NetPolicyCache::new());
        let effective = policy(json!({
            "tls": { "mode": "server", "certFrom": { "kv": "certs/edge" } }
        }));
        let hash = effective.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 31,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, effective).unwrap();
        let context = Arc::new(ForwarderContext::new(cache, Arc::new(Denials::new())));
        let ingress = IngressListeners::new(context.clone());
        ingress
            .sync(&[IngressSpec {
                allocation_id: 31,
                label: "https".into(),
                host_port,
                private_port: private,
                dial: TaskDial::Loopback { netns: None },
                bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            }])
            .await;

        // A client that trusts the CA: returns the leaf it was shown and
        // what the task answered.
        let dial = || {
            let ca_pem = ca_pem.clone();
            async move {
                let mut roots = rustls::RootCertStore::empty();
                for cert in certs_from_pem(ca_pem.as_bytes()).unwrap() {
                    roots.add(cert).unwrap();
                }
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
                let stream = TcpStream::connect(("127.0.0.1", host_port)).await.unwrap();
                let mut tls = connector
                    .connect(server_name(&node_dns_name("edge")).unwrap(), stream)
                    .await
                    .unwrap();
                let leaf = tls.get_ref().1.peer_certificates().unwrap()[0].clone();
                tls.write_all(b"GET /hello HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ =
                    tokio::time::timeout(Duration::from_secs(3), tls.read_to_end(&mut out)).await;
                (leaf, String::from_utf8_lossy(&out).to_string())
            }
        };
        let leaf_of = |pem: &str| certs_from_pem(pem.as_bytes()).unwrap()[0].clone();

        context.set_kv_cert(
            31,
            KvCertificate::from_bundle("certs/edge", 1, &format!("{cert_1}{key_1}")).unwrap(),
        );
        let (leaf, body) = dial().await;
        assert_eq!(leaf, leaf_of(&cert_1));
        assert!(body.contains("task saw GET /hello"), "{body}");

        context.set_kv_cert(
            31,
            KvCertificate::from_bundle("certs/edge", 2, &format!("{cert_2}{key_2}")).unwrap(),
        );
        assert_eq!(context.kv_cert_version(31), 2);
        let (leaf, _) = dial().await;
        assert_eq!(
            leaf,
            leaf_of(&cert_2),
            "the next connection gets the new version"
        );

        context.set_kv_cert_error(31, "kv certs/edge holds no private key".into());
        assert_eq!(
            context.ingress_tls_status(31),
            (true, "kv certs/edge holds no private key".to_string())
        );
        let (leaf, _) = dial().await;
        assert_eq!(
            leaf,
            leaf_of(&cert_2),
            "a failed renewal keeps the last good cert"
        );
    }

    #[test]
    fn kv_bundles_are_validated_test() {
        use crate::manager::mesh_ca::{generate_ca, issue_node_cert};
        use crate::worker::netd::tls::validate_bundle;
        let (ca_pem, ca_key) = generate_ca().unwrap();
        let (cert, key, _) = issue_node_cert(&ca_pem, &ca_key, "edge").unwrap();
        let (_, other_key, _) = issue_node_cert(&ca_pem, &ca_key, "edge").unwrap();

        validate_bundle(&format!("{cert}{key}")).unwrap();
        let no_key = validate_bundle(&cert).unwrap_err().to_string();
        assert!(no_key.contains("no private key"), "{no_key}");
        let no_cert = validate_bundle(&key).unwrap_err().to_string();
        assert!(no_cert.contains("no certificate"), "{no_cert}");
        let mismatch = validate_bundle(&format!("{cert}{other_key}"))
            .unwrap_err()
            .to_string();
        assert!(mismatch.contains("do not match"), "{mismatch}");
    }

    #[tokio::test]
    async fn l4_ingress_denies_by_peer_cidr() {
        let private = free_port().await;
        task_on(private).await;
        let host_port = free_port().await;
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        // TCP ingress: only 10.0.0.0/8 may connect — a loopback peer is denied.
        let effective = policy(json!({ "allow": ["10.0.0.0/8"] }));
        let hash = effective.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 12,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, effective).unwrap();
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));
        let ingress = IngressListeners::new(context);
        ingress
            .sync(&[IngressSpec {
                allocation_id: 12,
                label: "tcp".into(),
                host_port,
                private_port: private,
                dial: TaskDial::Loopback { netns: None },
                bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            }])
            .await;
        let out = request(host_port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(
            out.is_empty(),
            "denied peers are closed without a byte: {out:?}"
        );
        assert!(denials
            .drain()
            .iter()
            .any(|d| d.direction == "ingress" && d.source == "fallthrough"));
    }

    /// A fake VMM on a gated sandbox's base socket: it answers the
    /// host-initiated handshake (`CONNECT <port>` → `OK 1073741824`) by
    /// connecting to the guest's listener for that port, here a Unix socket
    /// `<guest>_<port>` standing in for vsock, and splicing. Nothing
    /// listening on the port closes the stream, as both VMMs do.
    fn fake_vmm(base: PathBuf, guest: PathBuf) {
        let listener = tokio::net::UnixListener::bind(&base).unwrap();
        tokio::spawn(async move {
            while let Ok((mut host, _)) = listener.accept().await {
                let guest = guest.clone();
                tokio::spawn(async move {
                    let mut line = Vec::new();
                    let mut byte = [0u8; 1];
                    while host.read(&mut byte).await.unwrap_or(0) == 1 && byte[0] != b'\n' {
                        line.push(byte[0]);
                    }
                    let line = String::from_utf8_lossy(&line).to_string();
                    let Some(port) = line
                        .strip_prefix("CONNECT ")
                        .and_then(|port| port.parse::<u32>().ok())
                    else {
                        return;
                    };
                    let path = crate::worker::netd::guest_agent::host_socket_path(&guest, port);
                    let Ok(mut inside) = UnixStream::connect(path).await else {
                        return;
                    };
                    if host.write_all(b"OK 1073741824\n").await.is_err() {
                        return;
                    }
                    let _ = tokio::io::copy_bidirectional(&mut host, &mut inside).await;
                });
            }
        });
    }

    /// Assign each allocation its ingress policy, in one sync (the cache
    /// replaces its whole assignment table on every sync).
    fn assign(cache: &NetPolicyCache, policies: &[(i64, serde_json::Value)]) {
        let effective: Vec<_> = policies
            .iter()
            .map(|(id, ingress)| (*id, policy(ingress.clone())))
            .collect();
        let assignments: Vec<AllocNetPolicy> = effective
            .iter()
            .map(|(allocation_id, body)| AllocNetPolicy {
                allocation_id: *allocation_id,
                policy_hash: body.hash.clone(),
            })
            .collect();
        cache.apply_assignments(&assignments);
        for (_, body) in effective {
            cache.insert_body(&body.hash.clone(), body).unwrap();
        }
    }

    /// Gated plan §6.10: an advertised port of a gated sandbox, end to end
    /// against fakes: the ingress listener judges the peer, dials the VMM's
    /// base socket with `CONNECT <port>`, and the guest agent's ingress
    /// relays to the workload's loopback port. L4 and HTTP chains both
    /// apply, the activity ledger sees the connection, a gated guest fails
    /// closed while its policy is pending, and a restored guest is dialed
    /// on the port its boot record says it still listens on.
    #[tokio::test]
    async fn gated_sandbox_ingress_splices_through_the_vmm_vsock_socket_test() {
        use crate::worker::netd::guest_agent::{Ingress, Relays, UnixListen};

        let workload = free_port().await;
        task_on(workload).await;
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join(".sandbox");
        std::fs::create_dir_all(&scratch).unwrap();
        let guest = dir.path().join("guest.sock");
        fake_vmm(
            crate::task_plugins::vsock_socket_base(&scratch),
            guest.clone(),
        );
        // The guest agent's ingress side, listening on vsock port ==
        // the workload's port.
        let agent = Ingress::new(
            Arc::new(UnixListen {
                base: guest.clone(),
            }),
            Arc::new(Relays::default()),
        );
        assert!(agent.ensure(&[workload]).is_empty());

        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        assign(
            &cache,
            &[
                (41, json!({ "allow": ["127.0.0.0/8"] })),
                (
                    43,
                    json!({
                        "protocol": "http", "policy": "deny",
                        "rules": [{ "match": { "path": "/public/**" }, "action": "allow" }]
                    }),
                ),
            ],
        );
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));
        let ingress = IngressListeners::new(context.clone());
        let (l4, http_port, pending) = (free_port().await, free_port().await, free_port().await);
        let spec = |allocation_id: i64, host_port: u16, private_port: u16| IngressSpec {
            allocation_id,
            label: "http".into(),
            host_port,
            private_port,
            dial: TaskDial::Vsock {
                scratch: scratch.clone(),
            },
            bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        };
        ingress
            .sync(&[
                spec(41, l4, workload),
                spec(43, http_port, workload),
                spec(44, pending, workload),
            ])
            .await;

        let ok = request(
            l4,
            "GET /hello HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(ok.contains("task saw GET /hello"), "{ok}");
        assert!(
            context
                .activity()
                .snapshot()
                .get(&41)
                .is_some_and(|activity| activity.last_bytes_unix > 0),
            "the activity ledger saw the connection"
        );

        // HTTP mode: the request rules apply, then the request goes over
        // the same vsock dial.
        let public = request(
            http_port,
            "GET /public/x HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(public.contains("task saw GET /public/x"), "{public}");
        let admin = request(
            http_port,
            "GET /admin HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(admin.starts_with("HTTP/1.1 403"), "{admin}");

        // No policy has arrived for allocation 44: a gated guest denies.
        let out = request(pending, "GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(out.is_empty(), "{out:?}");
        assert!(denials
            .drain()
            .iter()
            .any(|d| d.allocation_id == 44 && d.source == "policy-pending"));

        // A restored guest: the allocation was assigned another port, but
        // the workload in the guest's memory still holds the old one, which
        // the boot record names. The dial follows the record.
        let elsewhere = free_port().await;
        ingress.sync(&[spec(41, l4, elsewhere)]).await;
        let missed = request(l4, "GET /a HTTP/1.1\r\nHost: svc\r\n\r\n").await;
        assert!(
            missed.is_empty(),
            "nothing listens on {elsewhere}: {missed}"
        );
        crate::task_plugins::write_guest_ports_record(&scratch, &[("http", workload)]).unwrap();
        let restored = request(
            l4,
            "GET /after-restore HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            restored.contains("task saw GET /after-restore"),
            "{restored}"
        );
    }

    /// A TAP-wired sandbox: the datapath dials the address the worker
    /// leased the guest (loopback stands in for the bridge here) on the
    /// port the guest listens on, read per connection so a restore's new
    /// lease and recorded ports apply at once.
    #[tokio::test]
    async fn tap_sandbox_ingress_dials_the_leased_guest_address_test() {
        use crate::task_plugins::sandbox_addr::{GuestLease, LEASE_FILE};

        let workload = free_port().await;
        task_on(workload).await;
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join(".sandbox");
        std::fs::create_dir_all(&scratch).unwrap();
        let lease = |address: &str| {
            let lease = GuestLease {
                bridge: "br0".into(),
                address: address.parse().unwrap(),
                prefix: 16,
                gateway: None,
                dns: Vec::new(),
            };
            std::fs::write(
                scratch.join(LEASE_FILE),
                serde_json::to_vec(&lease).unwrap(),
            )
            .unwrap();
        };

        // The dial target, before and after the worker leased an address.
        let target = TaskTarget {
            dial: TaskDial::Bridge {
                scratch: scratch.clone(),
            },
            label: "http".into(),
            port: workload,
        };
        let err = target.tcp_addr().unwrap_err().to_string();
        assert!(err.contains("no address leased"), "{err}");
        lease("10.88.128.7");
        assert_eq!(
            target.tcp_addr().unwrap(),
            format!("10.88.128.7:{workload}").parse().unwrap()
        );
        assert!(!target.dial.gated(), "a TAP guest is not gated");
        crate::task_plugins::write_guest_ports_record(&scratch, &[("http", 20001)]).unwrap();
        assert_eq!(
            target.tcp_addr().unwrap(),
            "10.88.128.7:20001".parse().unwrap(),
            "a restored guest keeps the port it was captured with"
        );
        crate::task_plugins::write_guest_ports_record(&scratch, &[("http", workload)]).unwrap();

        // End to end, with the guest "on" loopback.
        lease("127.0.0.1");
        let cache = Arc::new(NetPolicyCache::new());
        let context = Arc::new(ForwarderContext::new(cache, Arc::new(Denials::new())));
        let ingress = IngressListeners::new(context);
        let host_port = free_port().await;
        ingress
            .sync(&[IngressSpec {
                allocation_id: 51,
                label: "http".into(),
                host_port,
                private_port: workload,
                dial: TaskDial::Bridge {
                    scratch: scratch.clone(),
                },
                bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            }])
            .await;
        let ok = request(
            host_port,
            "GET /preview HTTP/1.1\r\nHost: svc\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(ok.contains("task saw GET /preview"), "{ok}");
    }
}
