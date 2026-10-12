//! Per-allocation DNS: name-is-config for upstreams, and egress policy at
//! resolution time.
//!
//! Every allocation with upstreams owns a loopback /24 (`127.42.b`, see
//! `AllocNetwork::vip_block`): `.1` is its resolver, `.2` onward are its
//! upstream VIPs. Because the resolver is bound to an address only that
//! allocation was told about (`LEGION_DNS`, and `resolv.conf` where a
//! namespace lets Legion write one), the server knows which allocation is
//! asking without any packet marking — and can answer `<key>.svc.legion`
//! with *that* allocation's VIP.
//!
//! Anything else is forwarded to the host's resolvers over UDP, after the
//! allocation's egress policy has judged the name: a host the policy would
//! deny at connect time is `NXDOMAIN` here, one round trip instead of a
//! hanging connect, and recorded as a denial exactly like a refused dial.
//! Names are never judged when the chain matches on CIDRs (the address is
//! what the rule needs, and it is not known yet) — resolution proceeds and
//! the connect-time check decides.
//!
//! Wire handling is deliberately minimal: A / AAAA / SRV questions for the
//! zone are answered locally, everything else is proxied byte-for-byte. No
//! caching, no recursion, no DNSSEC — the host resolver does all of that.
//! Linux only, because only Linux routes all of 127/8 to loopback.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, Verdict};
use crate::structs::SVC_ZONE;
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::policy::{egress_connect_verdict, gated_egress_connect_verdict};

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

/// TTL on local answers. Short: a VIP never changes for a running
/// allocation, but a restarted allocation gets a new block.
const LOCAL_TTL: u32 = 5;
/// How long to wait for a host resolver before trying the next.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(2);
/// Largest DNS message accepted over UDP.
const MAX_UDP: usize = 4096;
/// Port the per-allocation resolver listens on.
pub const DNS_PORT: u16 = 53;

/// What one allocation's resolver serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsSpec {
    pub allocation_id: i64,
    /// `127.42.b.1`, the address a namespace or host resolver binds. `None`
    /// for a gated sandbox without a VIP block, which serves only on
    /// [`Self::unix`] and never binds an address.
    pub resolver: Option<Ipv4Addr>,
    /// `<key>` → (VIP, canonical port).
    pub names: BTreeMap<String, (Ipv4Addr, u16)>,
    /// The gated namespace to bind inside, if any.
    pub netns: Option<std::path::PathBuf>,
    /// A gated *sandbox*: serve DNS-over-TCP framing on this Unix socket —
    /// the host end of the guest's vsock port 53 — instead of binding the
    /// resolver address (the guest agent relays UDP queries to it).
    pub unix: Option<std::path::PathBuf>,
}

struct Server {
    spec: DnsSpec,
    udp: Option<JoinHandle<()>>,
    tcp: Option<JoinHandle<()>>,
    bind_warned: bool,
}

impl Server {
    fn is_running(&self) -> bool {
        self.udp.as_ref().is_some_and(|t| !t.is_finished())
            || self.tcp.as_ref().is_some_and(|t| !t.is_finished())
    }
}

/// The node's per-allocation resolvers, reconciled from heartbeat replies
/// like the upstream forwarder's listeners.
pub struct NetDns {
    context: Arc<ForwarderContext>,
    forwarders: Vec<SocketAddr>,
    servers: Mutex<HashMap<i64, Server>>,
}

impl NetDns {
    /// `forwarders` are the host's resolvers (`/etc/resolv.conf`); external
    /// names go there.
    pub fn new(context: Arc<ForwarderContext>, forwarders: Vec<SocketAddr>) -> Self {
        NetDns {
            context,
            forwarders,
            servers: Mutex::new(HashMap::new()),
        }
    }

    /// The host's resolvers from `/etc/resolv.conf`, or a public one when it
    /// names none.
    pub fn host_forwarders() -> Vec<SocketAddr> {
        let mut out = Vec::new();
        if let Ok(text) = std::fs::read_to_string("/etc/resolv.conf") {
            for line in text.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("nameserver") {
                    if let Ok(ip) = rest.trim().parse::<IpAddr>() {
                        // A host resolver on loopback (systemd-resolved) is
                        // fine: the allocation's own resolver is on a
                        // different 127 address.
                        out.push(SocketAddr::new(ip, 53));
                    }
                }
            }
        }
        if out.is_empty() {
            out.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53));
        }
        out
    }

    /// Reconcile resolvers against the desired set.
    pub async fn sync(&self, desired: &[DnsSpec]) {
        if !crate::structs::platform_supports_vip_blocks() {
            return;
        }
        let mut wanted: HashMap<i64, DnsSpec> = HashMap::new();
        for spec in desired {
            wanted.insert(spec.allocation_id, spec.clone());
        }
        let mut stopped: Vec<JoinHandle<()>> = Vec::new();
        let mut pending: Vec<DnsSpec> = Vec::new();
        {
            let mut servers = lock(&self.servers);
            servers.retain(|id, server| {
                if wanted.get(id).is_some_and(|spec| *spec == server.spec) {
                    return true;
                }
                if let Some(task) = server.udp.take() {
                    stopped.push(task);
                }
                if let Some(task) = server.tcp.take() {
                    stopped.push(task);
                }
                false
            });
            for (id, spec) in wanted {
                match servers.get(&id) {
                    Some(server) if server.is_running() => {}
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

    /// Allocations whose resolver is bound.
    pub fn active(&self) -> Vec<i64> {
        let servers = lock(&self.servers);
        let mut ids: Vec<i64> = servers
            .values()
            .filter(|s| s.is_running())
            .map(|s| s.spec.allocation_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    async fn start(&self, spec: DnsSpec) {
        if let Some(path) = spec.unix.clone() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::remove_file(&path);
            match tokio::net::UnixListener::bind(&path) {
                Ok(listener) => {
                    debug!(allocation_id = spec.allocation_id, path = %path.display(), "Started allocation resolver on vsock host socket");
                    let shared = Arc::new(Resolver {
                        spec: spec.clone(),
                        context: self.context.clone(),
                        forwarders: self.forwarders.clone(),
                    });
                    let task = tokio::spawn(run_unix(listener, shared));
                    lock(&self.servers).insert(
                        spec.allocation_id,
                        Server {
                            spec,
                            udp: None,
                            tcp: Some(task),
                            bind_warned: false,
                        },
                    );
                }
                Err(err) => {
                    warn!(allocation_id = spec.allocation_id, path = %path.display(), error = ?err, "Failed to bind resolver socket; retrying on the next heartbeat");
                    lock(&self.servers).insert(
                        spec.allocation_id,
                        Server {
                            spec,
                            udp: None,
                            tcp: None,
                            bind_warned: true,
                        },
                    );
                }
            }
            return;
        }
        let Some(resolver) = spec.resolver else {
            // Nothing to bind: a spec with neither an address nor a socket
            // is never produced, but a resolver that cannot exist must not
            // count as running.
            return;
        };
        let addr = SocketAddr::new(IpAddr::V4(resolver), DNS_PORT);
        let udp = crate::worker::netd::netns::bind_udp(spec.netns.clone(), addr).await;
        let tcp = crate::worker::netd::netns::bind_tcp(spec.netns.clone(), addr).await;
        match (udp, tcp) {
            (Ok(udp), Ok(tcp)) => {
                debug!(
                    allocation_id = spec.allocation_id,
                    resolver = %addr,
                    names = spec.names.len(),
                    "Started allocation resolver"
                );
                let shared = Arc::new(Resolver {
                    spec: spec.clone(),
                    context: self.context.clone(),
                    forwarders: self.forwarders.clone(),
                });
                let udp_task = tokio::spawn(run_udp(udp, shared.clone()));
                let tcp_task = tokio::spawn(run_tcp(tcp, shared));
                lock(&self.servers).insert(
                    spec.allocation_id,
                    Server {
                        spec,
                        udp: Some(udp_task),
                        tcp: Some(tcp_task),
                        bind_warned: false,
                    },
                );
            }
            (udp, tcp) => {
                let err = udp
                    .err()
                    .map(|e| e.to_string())
                    .or_else(|| tcp.err().map(|e| e.to_string()))
                    .unwrap_or_default();
                let mut servers = lock(&self.servers);
                let warned = servers
                    .get(&spec.allocation_id)
                    .is_some_and(|s| s.bind_warned);
                if !warned {
                    warn!(
                        allocation_id = spec.allocation_id,
                        resolver = %addr,
                        error = %err,
                        "Failed to bind allocation resolver (port 53 needs CAP_NET_BIND_SERVICE); retrying on the next heartbeat"
                    );
                }
                servers.insert(
                    spec.allocation_id,
                    Server {
                        spec,
                        udp: None,
                        tcp: None,
                        bind_warned: true,
                    },
                );
            }
        }
    }
}

impl Drop for NetDns {
    fn drop(&mut self) {
        for server in lock(&self.servers).values_mut() {
            if let Some(task) = server.udp.take() {
                task.abort();
            }
            if let Some(task) = server.tcp.take() {
                task.abort();
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

struct Resolver {
    spec: DnsSpec,
    context: Arc<ForwarderContext>,
    forwarders: Vec<SocketAddr>,
}

async fn run_udp(socket: UdpSocket, resolver: Arc<Resolver>) {
    let socket = Arc::new(socket);
    let mut buf = vec![0u8; MAX_UDP];
    loop {
        let (len, peer) = match socket.recv_from(&mut buf).await {
            Ok(received) => received,
            Err(err) => {
                debug!(error = ?err, "Resolver recv failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let query = buf[..len].to_vec();
        let resolver = resolver.clone();
        let socket = socket.clone();
        tokio::spawn(async move {
            if let Some(response) = answer(&resolver, &query).await {
                let _ = socket.send_to(&response, peer).await;
            }
        });
    }
}

async fn run_tcp(listener: TcpListener, resolver: Arc<Resolver>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                debug!(error = ?err, "Resolver accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let resolver = resolver.clone();
        tokio::spawn(serve_stream(stream, resolver));
    }
}

async fn run_unix(listener: tokio::net::UnixListener, resolver: Arc<Resolver>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                debug!(error = ?err, "Resolver accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let resolver = resolver.clone();
        tokio::spawn(serve_stream(stream, resolver));
    }
}

/// One DNS-over-TCP exchange (RFC 1035 §4.2.2 framing: two-byte length
/// prefix), on a TCP or a vsock-backed stream.
async fn serve_stream<S>(mut stream: S, resolver: Arc<Resolver>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut len_buf = [0u8; 2];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let len = usize::from(u16::from_be_bytes(len_buf));
    let mut query = vec![0u8; len];
    if stream.read_exact(&mut query).await.is_err() {
        return;
    }
    if let Some(response) = answer(&resolver, &query).await {
        let mut framed = Vec::with_capacity(response.len() + 2);
        framed.extend_from_slice(&(response.len() as u16).to_be_bytes());
        framed.extend_from_slice(&response);
        let _ = stream.write_all(&framed).await;
    }
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

const TYPE_A: u16 = 1;
const TYPE_AAAA: u16 = 28;
const TYPE_SRV: u16 = 33;
const TYPE_ANY: u16 = 255;
const CLASS_IN: u16 = 1;
const RCODE_NXDOMAIN: u8 = 3;

/// The one question in a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Question {
    pub name: String,
    pub qtype: u16,
    pub qclass: u16,
    /// Byte length of the question section (for echoing it).
    pub end: usize,
}

/// Parse the header and first question of a DNS message.
pub fn parse_question(message: &[u8]) -> Option<(u16, Question)> {
    if message.len() < 12 {
        return None;
    }
    let id = u16::from_be_bytes([message[0], message[1]]);
    let qdcount = u16::from_be_bytes([message[4], message[5]]);
    if qdcount == 0 {
        return None;
    }
    let (name, mut offset) = read_name(message, 12, 0)?;
    if offset + 4 > message.len() {
        return None;
    }
    let qtype = u16::from_be_bytes([message[offset], message[offset + 1]]);
    let qclass = u16::from_be_bytes([message[offset + 2], message[offset + 3]]);
    offset += 4;
    Some((
        id,
        Question {
            name,
            qtype,
            qclass,
            end: offset,
        },
    ))
}

/// Read a (possibly compressed) name; returns the name and the offset after
/// it in the *uncompressed* stream position.
fn read_name(message: &[u8], mut offset: usize, depth: u8) -> Option<(String, usize)> {
    if depth > 8 {
        return None;
    }
    let mut labels: Vec<String> = Vec::new();
    loop {
        let len = *message.get(offset)? as usize;
        if len == 0 {
            offset += 1;
            break;
        }
        if len & 0xC0 == 0xC0 {
            let pointer = ((len & 0x3F) << 8) | *message.get(offset + 1)? as usize;
            let (rest, _) = read_name(message, pointer, depth + 1)?;
            if !rest.is_empty() {
                labels.push(rest);
            }
            offset += 2;
            return Some((labels.join("."), offset));
        }
        offset += 1;
        let label = message.get(offset..offset + len)?;
        labels.push(String::from_utf8_lossy(label).to_ascii_lowercase());
        offset += len;
    }
    Some((labels.join("."), offset))
}

fn encode_name(name: &str, out: &mut Vec<u8>) {
    for label in name
        .trim_end_matches('.')
        .split('.')
        .filter(|l| !l.is_empty())
    {
        let bytes = label.as_bytes();
        out.push(bytes.len().min(63) as u8);
        out.extend_from_slice(&bytes[..bytes.len().min(63)]);
    }
    out.push(0);
}

/// Build a response: the query's header (id, RD echoed, QR/AA set), the
/// question echoed, and `answers` as (type, rdata) records for the question
/// name (a compression pointer to offset 12).
pub fn build_response(
    query: &[u8],
    question: &Question,
    rcode: u8,
    answers: &[(u16, Vec<u8>)],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(query.len() + 64);
    out.extend_from_slice(&query[0..2]);
    let rd = query[2] & 0x01;
    out.push(0x80 | 0x04 | rd); // QR, AA, RD
    out.push(0x80 | (rcode & 0x0F)); // RA
    out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    out.extend_from_slice(&(answers.len() as u16).to_be_bytes()); // ANCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    out.extend_from_slice(&query[12..question.end]);
    for (rtype, rdata) in answers {
        out.extend_from_slice(&[0xC0, 0x0C]);
        out.extend_from_slice(&rtype.to_be_bytes());
        out.extend_from_slice(&CLASS_IN.to_be_bytes());
        out.extend_from_slice(&LOCAL_TTL.to_be_bytes());
        out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        out.extend_from_slice(rdata);
    }
    out
}

/// Strip the zone from a name in it: `db.svc.legion` → `db`, bare `db` →
/// `db`; anything else → `None`.
fn zone_key(name: &str) -> Option<String> {
    let name = name.trim_end_matches('.');
    if let Some(key) = name.strip_suffix(&format!(".{SVC_ZONE}")) {
        if !key.is_empty() && !key.contains('.') {
            return Some(key.to_string());
        }
        // `_<key>._tcp.svc.legion` (SRV convention).
        if let Some(rest) = key.strip_suffix("._tcp") {
            if let Some(key) = rest.strip_prefix('_') {
                return Some(key.to_string());
            }
        }
        return None;
    }
    if !name.contains('.') && !name.is_empty() {
        return Some(name.to_string());
    }
    None
}

/// Answer one query for one allocation.
async fn answer(resolver: &Arc<Resolver>, query: &[u8]) -> Option<Vec<u8>> {
    let (_, question) = parse_question(query)?;
    if question.qclass != CLASS_IN {
        return Some(build_response(query, &question, RCODE_NXDOMAIN, &[]));
    }

    // Local zone.
    if let Some(key) = zone_key(&question.name) {
        return Some(match resolver.spec.names.get(&key) {
            Some((vip, port)) => {
                net_metrics::connection("egress", "dns", "allow");
                let mut answers = Vec::new();
                match question.qtype {
                    TYPE_A | TYPE_ANY => answers.push((TYPE_A, vip.octets().to_vec())),
                    TYPE_SRV => {
                        let mut rdata = Vec::new();
                        rdata.extend_from_slice(&0u16.to_be_bytes()); // priority
                        rdata.extend_from_slice(&0u16.to_be_bytes()); // weight
                        rdata.extend_from_slice(&port.to_be_bytes());
                        encode_name(&format!("{key}.{SVC_ZONE}"), &mut rdata);
                        answers.push((TYPE_SRV, rdata));
                    }
                    // AAAA: the zone is v4-only; NOERROR with no data tells
                    // a dual-stack resolver to use the A record.
                    TYPE_AAAA => {}
                    _ => {}
                }
                build_response(query, &question, 0, &answers)
            }
            None if question.name.ends_with(SVC_ZONE) || !question.name.contains('.') => {
                // In the zone (or a bare label) but not one of this
                // allocation's upstreams.
                build_response(query, &question, RCODE_NXDOMAIN, &[])
            }
            None => build_response(query, &question, RCODE_NXDOMAIN, &[]),
        });
    }

    // External: policy at resolution time, then forward. A gated
    // allocation's resolver (inside its namespace, or a sandbox's vsock
    // socket) denies by default when no egress chain covers it, so a name
    // nothing allows is NXDOMAIN in one round trip (`gated-default`).
    let allocation_id = resolver.spec.allocation_id;
    let policy = resolver.context.policy_for(allocation_id);
    let gated = resolver.spec.netns.is_some() || resolver.spec.unix.is_some();
    let chain = policy.as_ref().and_then(|p| p.egress.as_ref());
    let judge = match chain {
        Some(chain) => !chain.uses_cidr(),
        None => gated && resolver.context.policy_known(allocation_id),
    };
    if judge {
        let facts = ConnectFacts {
            host: Some(question.name.trim_end_matches('.')),
            ip: None,
            port: 0,
            identity: None,
        };
        let verdict = if gated {
            gated_egress_connect_verdict(policy.as_deref(), &facts)
        } else {
            egress_connect_verdict(policy.as_deref(), &facts)
        };
        if let Verdict::Deny { source, reason } = verdict {
            net_metrics::connection("egress", "dns", "deny");
            resolver.context.record_denial(
                resolver.spec.allocation_id,
                "egress",
                "dns",
                question.name.trim_end_matches('.'),
                &source,
                reason.as_deref(),
                "",
                "",
            );
            return Some(build_response(query, &question, RCODE_NXDOMAIN, &[]));
        }
    }
    net_metrics::connection("egress", "dns", "allow");
    forward(resolver, query).await
}

/// Proxy a query to the host resolvers, first answer wins.
async fn forward(resolver: &Arc<Resolver>, query: &[u8]) -> Option<Vec<u8>> {
    for upstream in &resolver.forwarders {
        let bind: SocketAddr = if upstream.is_ipv4() {
            "0.0.0.0:0".parse().ok()?
        } else {
            "[::]:0".parse().ok()?
        };
        let Ok(socket) = UdpSocket::bind(bind).await else {
            continue;
        };
        if socket.send_to(query, upstream).await.is_err() {
            continue;
        }
        let mut buf = vec![0u8; MAX_UDP];
        match tokio::time::timeout(FORWARD_TIMEOUT, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) => return Some(buf[..len].to_vec()),
            _ => continue,
        }
    }
    debug!(
        allocation_id = resolver.spec.allocation_id,
        "No host resolver answered; dropping query"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(name: &str, qtype: u16) -> Vec<u8> {
        let mut out = vec![0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
        encode_name(name, &mut out);
        out.extend_from_slice(&qtype.to_be_bytes());
        out.extend_from_slice(&CLASS_IN.to_be_bytes());
        out
    }

    #[test]
    fn parses_and_answers_the_zone() {
        let q = query("db.svc.legion", TYPE_A);
        let (id, question) = parse_question(&q).unwrap();
        assert_eq!(id, 0x1234);
        assert_eq!(question.name, "db.svc.legion");
        assert_eq!(zone_key(&question.name).as_deref(), Some("db"));
        assert_eq!(zone_key("db").as_deref(), Some("db"));
        assert_eq!(zone_key("_db._tcp.svc.legion").as_deref(), Some("db"));
        assert_eq!(zone_key("api.github.com"), None);
        assert_eq!(zone_key("a.b.svc.legion"), None);

        let response = build_response(&q, &question, 0, &[(TYPE_A, vec![127, 42, 7, 2])]);
        assert_eq!(&response[0..2], &[0x12, 0x34]);
        assert_eq!(response[2] & 0x80, 0x80, "QR set");
        assert_eq!(response[3] & 0x0F, 0, "NOERROR");
        assert_eq!(
            u16::from_be_bytes([response[6], response[7]]),
            1,
            "one answer"
        );
        assert_eq!(&response[response.len() - 4..], &[127, 42, 7, 2]);

        let nx = build_response(&q, &question, RCODE_NXDOMAIN, &[]);
        assert_eq!(nx[3] & 0x0F, RCODE_NXDOMAIN);
    }

    #[test]
    fn reads_compressed_names() {
        // "svc.legion" at 12, then a name "db" + pointer to 12.
        let mut msg = vec![0u8; 12];
        encode_name("svc.legion", &mut msg);
        let pointer_at = msg.len();
        msg.extend_from_slice(&[2, b'd', b'b', 0xC0, 12]);
        let (name, end) = read_name(&msg, pointer_at, 0).unwrap();
        assert_eq!(name, "db.svc.legion");
        assert_eq!(end, msg.len());
    }

    #[test]
    fn host_forwarders_never_empty() {
        assert!(!NetDns::host_forwarders().is_empty());
    }

    #[tokio::test]
    async fn resolver_serves_names_and_applies_policy() {
        use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
        use crate::structs::AllocNetPolicy;
        use crate::worker::netd::{Denials, NetPolicyCache};
        if !crate::structs::platform_supports_vip_blocks() {
            return;
        }
        // A stub "host resolver" that answers every query with 9.9.9.9.
        let stub = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stub_addr = stub.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            loop {
                let Ok((len, peer)) = stub.recv_from(&mut buf).await else {
                    break;
                };
                let q = buf[..len].to_vec();
                if let Some((_, question)) = parse_question(&q) {
                    let response = build_response(&q, &question, 0, &[(TYPE_A, vec![9, 9, 9, 9])]);
                    let _ = stub.send_to(&response, peer).await;
                }
            }
        });

        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut object: NetPolicyObject = serde_json::from_value(serde_json::json!({
            "name": "p", "enforcement": "require",
            "egress": { "allow": ["allowed.example", "api.github.com:443"] }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 3,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));

        // Bind on an unprivileged port for the test by talking to the
        // resolver internals directly.
        let resolver = Arc::new(Resolver {
            spec: DnsSpec {
                allocation_id: 3,
                resolver: Some(Ipv4Addr::new(127, 42, 9, 1)),
                names: [("db".to_string(), (Ipv4Addr::new(127, 42, 9, 2), 5432))]
                    .into_iter()
                    .collect(),
                netns: None,
                unix: None,
            },
            context,
            forwarders: vec![stub_addr],
        });

        // Zone name → VIP, SRV carries the canonical port, unknown → NXDOMAIN.
        let r = answer(&resolver, &query("db.svc.legion", TYPE_A))
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[127, 42, 9, 2]);
        let r = answer(&resolver, &query("db", TYPE_SRV)).await.unwrap();
        assert!(r.windows(2).any(|w| w == 5432u16.to_be_bytes()));
        let r = answer(&resolver, &query("nope.svc.legion", TYPE_A))
            .await
            .unwrap();
        assert_eq!(r[3] & 0x0F, RCODE_NXDOMAIN);

        // External: allowed names forward (9.9.9.9 from the stub), a
        // port-constrained allow still resolves (port unknown at DNS time),
        // and a name the policy would deny is NXDOMAIN plus a denial.
        let r = answer(&resolver, &query("allowed.example", TYPE_A))
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[9, 9, 9, 9]);
        let r = answer(&resolver, &query("api.github.com", TYPE_A))
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[9, 9, 9, 9]);
        let r = answer(&resolver, &query("evil.example", TYPE_A))
            .await
            .unwrap();
        assert_eq!(r[3] & 0x0F, RCODE_NXDOMAIN);
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.protocol == "dns" && d.destination == "evil.example"),
            "{drained:?}"
        );
    }

    /// Gated plan §6.5: a gated sandbox's resolver with nothing covering its
    /// egress answers every outside name NXDOMAIN in one round trip and
    /// records a `gated-default` denial, while its own `*.svc.legion` zone
    /// still resolves. A non-gated resolver with no policy still forwards.
    #[tokio::test]
    async fn a_gated_resolver_with_no_policy_denies_by_default() {
        use crate::structs::netpolicy::{merge_policies, InlinePolicy};
        use crate::structs::AllocNetPolicy;
        use crate::worker::netd::{Denials, NetPolicyCache};
        let stub = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stub_addr = stub.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            loop {
                let Ok((len, peer)) = stub.recv_from(&mut buf).await else {
                    break;
                };
                let q = buf[..len].to_vec();
                if let Some((_, question)) = parse_question(&q) {
                    let response = build_response(&q, &question, 0, &[(TYPE_A, vec![9, 9, 9, 9])]);
                    let _ = stub.send_to(&response, peer).await;
                }
            }
        });
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        // Known and empty: the manager names the empty policy's hash.
        let empty = merge_policies(&[], InlinePolicy::default());
        let hash = empty.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 3,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, empty).unwrap();
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));
        let resolver = |unix: Option<std::path::PathBuf>| {
            Arc::new(Resolver {
                spec: DnsSpec {
                    allocation_id: 3,
                    resolver: None,
                    names: [("db".to_string(), (Ipv4Addr::new(127, 42, 9, 2), 5432))]
                        .into_iter()
                        .collect(),
                    netns: None,
                    unix,
                },
                context: context.clone(),
                forwarders: vec![stub_addr],
            })
        };

        let gated = resolver(Some("/allocs/3/.sandbox/vsock.sock_53".into()));
        let r = answer(&gated, &query("pypi.org", TYPE_A)).await.unwrap();
        assert_eq!(r[3] & 0x0F, RCODE_NXDOMAIN);
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.destination == "pypi.org" && d.source == "gated-default"),
            "{drained:?}"
        );
        let r = answer(&gated, &query("db.svc.legion", TYPE_A))
            .await
            .unwrap();
        assert_eq!(
            &r[r.len() - 4..],
            &[127, 42, 9, 2],
            "the zone still resolves"
        );

        let cooperative = resolver(None);
        let r = answer(&cooperative, &query("pypi.org", TYPE_A))
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[9, 9, 9, 9]);
    }
}
