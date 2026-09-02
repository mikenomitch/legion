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
//! 3. forwards to the private port — spliced (L4), or parsed as HTTP/1.1
//!    when the chain is `protocol = "http"` so request-time rules apply —
//!    dialing inside the task's namespace when it has one.
//!
//! Health checks and the catalog keep targeting the advertised port, so a
//! check exercises the same path a consumer does. A deny-everything ingress
//! chain therefore fails the task's own health check; `legion net doctor`
//! names that case.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, Verdict, PROTOCOL_TCP};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::http::{self, HttpSettings};
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::netns;
use crate::worker::netd::policy::ingress_connect_verdict;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// One advertised port the datapath fronts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressSpec {
    pub allocation_id: i64,
    /// The port label (`network.ports[].name`).
    pub label: String,
    /// The advertised host port the datapath binds.
    pub host_port: u16,
    /// The private port the task bound.
    pub private_port: u16,
    /// The task's gated namespace, when it has one (dial inside it).
    pub netns: Option<PathBuf>,
    /// Bind address; `None` is every interface.
    pub bind: Option<IpAddr>,
}

type IngressKey = (i64, String);

struct Shared {
    spec: IngressSpec,
    context: Arc<ForwarderContext>,
    http: OnceLock<Arc<HttpSettings>>,
}

impl Shared {
    fn gated(&self) -> bool {
        self.spec.netns.is_some()
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
                        self.spec.label.clone(),
                        self.spec.private_port,
                        self.spec.netns.clone(),
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
        let addr = SocketAddr::new(
            spec.bind.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            spec.host_port,
        );
        let key = (spec.allocation_id, spec.label.clone());
        match netns::bind_tcp(None, addr).await {
            Ok(listener) => {
                debug!(
                    allocation_id = spec.allocation_id,
                    label = %spec.label,
                    host_port = spec.host_port,
                    private_port = spec.private_port,
                    "Started ingress listener"
                );
                let shared = Arc::new(Shared {
                    spec: spec.clone(),
                    context: self.context.clone(),
                    http: OnceLock::new(),
                });
                let task = tokio::spawn(run(listener, shared));
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

async fn run(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        let (client, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                debug!(error = ?err, "Ingress accept failed");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            handle(client, peer, shared).await;
        });
    }
}

async fn handle(mut client: TcpStream, peer: SocketAddr, shared: Arc<Shared>) {
    let policy = shared.context.policy_for(shared.spec.allocation_id);
    let facts = ConnectFacts {
        host: None,
        ip: Some(peer.ip()),
        port: shared.spec.host_port,
        identity: None,
    };
    let block = if shared.gated() {
        if netns::gate_is_shut(shared.spec.allocation_id) {
            Some((
                "gate".to_string(),
                Some("network gate is shut until the placement is ratified".to_string()),
            ))
        } else if policy.is_none() && !shared.context.policy_known(shared.spec.allocation_id) {
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
    let verdict = match block {
        Some((source, reason)) => Verdict::Deny { source, reason },
        None => ingress_connect_verdict(policy.as_deref(), &facts),
    };
    if let Verdict::Deny { source, reason } = verdict {
        net_metrics::connection("ingress", PROTOCOL_TCP, "deny");
        shared.context.record_denial(
            shared.spec.allocation_id,
            "ingress",
            PROTOCOL_TCP,
            &peer.to_string(),
            &source,
            reason.as_deref(),
            "",
            "",
        );
        let _ = client.shutdown().await;
        return;
    }

    if let Some(settings) = shared.http_settings() {
        http::serve(client, settings).await;
        return;
    }

    let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), shared.spec.private_port);
    let mut task = match netns::connect_tcp(shared.spec.netns.clone(), target).await {
        Ok(stream) => stream,
        Err(err) => {
            debug!(
                allocation_id = shared.spec.allocation_id,
                label = %shared.spec.label,
                error = ?err,
                "Ingress could not reach the task's private port"
            );
            net_metrics::connection("ingress", PROTOCOL_TCP, "error");
            let _ = client.shutdown().await;
            return;
        }
    };
    net_metrics::connection("ingress", PROTOCOL_TCP, "allow");
    match tokio::io::copy_bidirectional(&mut client, &mut task).await {
        Ok((in_bytes, out_bytes)) => {
            net_metrics::bytes("ingress", in_bytes);
            net_metrics::bytes("egress", out_bytes);
        }
        Err(err) => debug!(error = ?err, "Ingress connection ended with an error"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::AllocNetPolicy;
    use crate::worker::netd::{Denials, NetPolicyCache};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
                netns: None,
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
                netns: None,
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
}
