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
//! Anything else is forwarded to the host's resolvers (the `nameserver`
//! lines of `/etc/resolv.conf`) over UDP, after the allocation's egress
//! policy has judged the name: a host the policy would deny at connect time
//! is `NXDOMAIN` here, one round trip instead of a hanging connect, and
//! recorded as a denial exactly like a refused dial. Names are never judged
//! when the chain matches on CIDRs (the address is what the rule needs, and
//! it is not known yet) — resolution proceeds and the connect-time check
//! decides. A client that asked over TCP gets a truncated UDP answer fetched
//! again over TCP. A node whose `resolv.conf` names no resolver answers
//! `SERVFAIL`: nothing is sent to a resolver the operator did not name.
//!
//! Messages are parsed and built with `hickory-proto`. A / AAAA / SRV
//! questions for the zone are answered locally, everything else is proxied
//! byte-for-byte. No caching, no recursion, no DNSSEC — the host resolver
//! does all of that. Linux only, because only Linux routes all of 127/8 to
//! loopback.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, Verdict};
use crate::structs::SVC_ZONE;
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::attach::Attachment;
use crate::worker::netd::listen::{spawn_accept_loop, ListenAddr, Stream};

use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::policy::{egress_connect_verdict, gated_egress_connect_verdict};
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::rdata::{A, SRV};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
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
    /// `127.42.b.1` (or the gated default), the address the resolver binds
    /// in the host namespace or the allocation's namespace. `None` for a
    /// gated sandbox without a VIP block, which serves only on its vsock
    /// host socket.
    pub resolver: Option<Ipv4Addr>,
    /// `<key>` → (VIP, canonical port).
    pub names: BTreeMap<String, (Ipv4Addr, u16)>,
    /// How the allocation is attached. A gated sandbox's resolver serves
    /// DNS-over-TCP framing on the host end of the guest's vsock port 53
    /// (the guest agent relays UDP queries to it) instead of binding the
    /// resolver address.
    pub attachment: Attachment,
}

impl DnsSpec {
    /// Where the resolver binds, when it can bind anywhere.
    pub fn listen(&self) -> Option<ListenAddr> {
        match (&self.attachment, self.resolver) {
            (Attachment::GuestVsock { .. }, _) => Some(
                self.attachment
                    .listen(IpAddr::V4(Ipv4Addr::LOCALHOST), DNS_PORT),
            ),
            (_, Some(resolver)) => Some(self.attachment.listen(IpAddr::V4(resolver), DNS_PORT)),
            (_, None) => None,
        }
    }
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

/// The `nameserver` lines of a `resolv.conf`. A host resolver on loopback
/// (systemd-resolved) is fine: the allocation's own resolver is on a
/// different 127 address, and queries are forwarded from the host namespace.
pub fn forwarders_in(resolv_conf: &str) -> Vec<SocketAddr> {
    resolv_conf
        .lines()
        .filter_map(|line| line.trim().strip_prefix("nameserver"))
        .filter_map(|rest| rest.trim().parse::<IpAddr>().ok())
        .map(|ip| SocketAddr::new(ip, 53))
        .collect()
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

    /// The host's resolvers, from `/etc/resolv.conf`. Empty when it names
    /// none: external names then get `SERVFAIL`, and no query goes to a
    /// resolver the operator did not name.
    pub fn host_forwarders() -> Vec<SocketAddr> {
        let forwarders = std::fs::read_to_string("/etc/resolv.conf")
            .map(|text| forwarders_in(&text))
            .unwrap_or_default();
        if forwarders.is_empty() {
            warn!(
                "/etc/resolv.conf names no nameserver; allocation resolvers will answer \
                 SERVFAIL for every name outside svc.legion"
            );
        }
        forwarders
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
        let Some(listen) = spec.listen() else {
            // Nothing to bind: a spec with neither an address nor a socket
            // is never produced, but a resolver that cannot exist must not
            // count as running.
            return;
        };
        let udp = listen.bind_udp().await;
        let tcp = listen.bind().await;
        match (udp, tcp) {
            (Ok(udp), Ok(tcp)) => {
                debug!(
                    allocation_id = spec.allocation_id,
                    listen = %listen,
                    names = spec.names.len(),
                    "Started allocation resolver"
                );
                let shared = Arc::new(Resolver {
                    spec: spec.clone(),
                    context: self.context.clone(),
                    forwarders: self.forwarders.clone(),
                });
                let udp_task = udp.map(|udp| tokio::spawn(run_udp(udp, shared.clone())));
                let tcp_task = spawn_accept_loop(tcp, "resolver", move |stream, _| {
                    serve_stream(stream, shared.clone())
                });
                lock(&self.servers).insert(
                    spec.allocation_id,
                    Server {
                        spec,
                        udp: udp_task,
                        tcp: Some(tcp_task),
                        bind_warned: false,
                    },
                );
            }
            (udp, tcp) => {
                let err = udp
                    .err()
                    .map(|e| format!("{e:#}"))
                    .or_else(|| tcp.err().map(|e| format!("{e:#}")))
                    .unwrap_or_default();
                let mut servers = lock(&self.servers);
                let warned = servers
                    .get(&spec.allocation_id)
                    .is_some_and(|s| s.bind_warned);
                if !warned {
                    warn!(
                        allocation_id = spec.allocation_id,
                        listen = %listen,
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
            if let Some(response) = answer(&resolver, &query, false).await {
                let _ = socket.send_to(&response, peer).await;
            }
        });
    }
}

/// One DNS-over-TCP exchange (RFC 1035 §4.2.2 framing: two-byte length
/// prefix), on a TCP or a vsock-backed stream. A query that arrived over TCP
/// was asked over TCP, so a truncated upstream answer is fetched again in
/// full; one on a guest's vsock socket is a UDP query the guest agent
/// relayed, so it gets what a UDP client would.
async fn serve_stream(mut stream: Stream, resolver: Arc<Resolver>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let over_tcp = matches!(stream, Stream::Tcp(_));
    let mut len_buf = [0u8; 2];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let len = usize::from(u16::from_be_bytes(len_buf));
    let mut query = vec![0u8; len];
    if stream.read_exact(&mut query).await.is_err() {
        return;
    }
    if let Some(response) = answer(&resolver, &query, over_tcp).await {
        let Ok(response_len) = u16::try_from(response.len()) else {
            return;
        };
        let mut framed = Vec::with_capacity(response.len() + 2);
        framed.extend_from_slice(&response_len.to_be_bytes());
        framed.extend_from_slice(&response);
        let _ = stream.write_all(&framed).await;
    }
}

// ---------------------------------------------------------------------------
// Wire format (hickory-proto)
// ---------------------------------------------------------------------------

/// The one question in a query, parsed.
struct Asked {
    message: Message,
    query: Query,
    /// The question name, lower-cased, without the trailing dot.
    name: String,
}

/// Parse a query and its first question. `None` for anything that is not a
/// well-formed query with a question: no answer is better than a wrong one.
fn parse_query(bytes: &[u8]) -> Option<Asked> {
    let message = Message::from_vec(bytes).ok()?;
    if message.message_type() != MessageType::Query {
        return None;
    }
    let query = message.queries().first()?.clone();
    let name = query
        .name()
        .to_ascii()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    Some(Asked {
        message,
        query,
        name,
    })
}

/// A response to `asked`: its id, opcode and RD echoed, AA and RA set, the
/// question echoed, and `answers` for the question name.
fn respond(asked: &Asked, code: ResponseCode, answers: Vec<RData>) -> Option<Vec<u8>> {
    let mut response = Message::new();
    response
        .set_id(asked.message.id())
        .set_message_type(MessageType::Response)
        .set_op_code(asked.message.op_code())
        .set_authoritative(true)
        .set_recursion_desired(asked.message.recursion_desired())
        .set_recursion_available(true)
        .set_response_code(code)
        .add_query(asked.query.clone());
    for rdata in answers {
        response.add_answer(Record::from_rdata(
            asked.query.name().clone(),
            LOCAL_TTL,
            rdata,
        ));
    }
    response.to_vec().ok()
}

/// Whether a response says it was truncated (`TC`).
fn truncated(response: &[u8]) -> bool {
    Message::from_vec(response).is_ok_and(|message| message.truncated())
}

/// Strip the zone from a name in it: `db.svc.legion` → `db`, bare `db` →
/// `db`, and the cross-namespace form `auth.shared.svc.legion` →
/// `auth.shared` (an upstream's key when it names another namespace);
/// anything else → `None`.
fn zone_key(name: &str) -> Option<String> {
    let name = name.trim_end_matches('.');
    if let Some(key) = name.strip_suffix(&format!(".{SVC_ZONE}")) {
        // `_<key>._tcp[.<namespace>].svc.legion` (SRV convention).
        if let Some((service, rest)) = key
            .strip_prefix('_')
            .and_then(|rest| rest.split_once("._tcp"))
        {
            return match rest.strip_prefix('.') {
                _ if service.is_empty() || service.contains('.') => None,
                None if rest.is_empty() => Some(service.to_string()),
                Some(namespace) if !namespace.is_empty() && !namespace.contains('.') => {
                    Some(format!("{service}.{namespace}"))
                }
                _ => None,
            };
        }
        let labels: Vec<&str> = key.split('.').collect();
        if labels.len() <= 2 && labels.iter().all(|label| !label.is_empty()) {
            return Some(key.to_string());
        }
        return None;
    }
    if !name.contains('.') && !name.is_empty() {
        return Some(name.to_string());
    }
    None
}

/// Answer one query for one allocation. `over_stream` says the client asked
/// over TCP, so an upstream answer truncated over UDP is fetched again over
/// TCP instead of being handed back for the client to retry.
async fn answer(resolver: &Arc<Resolver>, query: &[u8], over_stream: bool) -> Option<Vec<u8>> {
    let asked = parse_query(query)?;
    if asked.query.query_class() != DNSClass::IN {
        return respond(&asked, ResponseCode::NXDomain, Vec::new());
    }

    // Local zone.
    if let Some(key) = zone_key(&asked.name) {
        return match resolver.spec.names.get(&key) {
            Some((vip, port)) => {
                net_metrics::connection("egress", "dns", "allow");
                let answers = match asked.query.query_type() {
                    RecordType::A | RecordType::ANY => vec![RData::A(A(*vip))],
                    RecordType::SRV => {
                        let target = Name::from_ascii(format!("{key}.{SVC_ZONE}.")).ok()?;
                        vec![RData::SRV(SRV::new(0, 0, *port, target))]
                    }
                    // AAAA: the zone is v4-only; NOERROR with no data tells
                    // a dual-stack resolver to use the A record.
                    _ => Vec::new(),
                };
                respond(&asked, ResponseCode::NoError, answers)
            }
            // In the zone (or a bare label) but not one of this
            // allocation's upstreams.
            None => respond(&asked, ResponseCode::NXDomain, Vec::new()),
        };
    }

    // External: policy at resolution time, then forward. A gated
    // allocation's resolver (inside its namespace, or a sandbox's vsock
    // socket) denies by default when no egress chain covers it, so a name
    // nothing allows is NXDOMAIN in one round trip (`gated-default`).
    let allocation_id = resolver.spec.allocation_id;
    let policy = resolver.context.policy_for(allocation_id);
    let gated = resolver.spec.attachment.is_gated();
    let chain = policy.as_ref().and_then(|p| p.egress.as_ref());
    let judge = match chain {
        Some(chain) => !chain.uses_cidr(),
        None => gated && resolver.context.policy_known(allocation_id),
    };
    if judge {
        let facts = ConnectFacts {
            host: Some(&asked.name),
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
                &asked.name,
                &source,
                reason.as_deref(),
                "",
                "",
            );
            return respond(&asked, ResponseCode::NXDomain, Vec::new());
        }
    }
    net_metrics::connection("egress", "dns", "allow");
    if resolver.forwarders.is_empty() {
        // The node names no resolver (`/etc/resolv.conf` has no
        // `nameserver`): say so at once rather than leave the client to
        // time out. Nothing is sent to a resolver the operator did not name.
        return respond(&asked, ResponseCode::ServFail, Vec::new());
    }
    forward(resolver, query, over_stream).await
}

/// Proxy a query to the host resolvers over UDP, first answer wins. An
/// answer truncated over UDP goes back to a UDP client as it is (the client
/// asks again over TCP, which this resolver serves too); for a client that
/// asked over TCP, it is fetched again from the same resolver over TCP.
async fn forward(resolver: &Arc<Resolver>, query: &[u8], over_stream: bool) -> Option<Vec<u8>> {
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
        let response = match tokio::time::timeout(FORWARD_TIMEOUT, socket.recv_from(&mut buf)).await
        {
            Ok(Ok((len, _))) => buf[..len].to_vec(),
            _ => continue,
        };
        if over_stream && truncated(&response) {
            if let Some(full) = forward_tcp(*upstream, query).await {
                return Some(full);
            }
        }
        return Some(response);
    }
    debug!(
        allocation_id = resolver.spec.allocation_id,
        "No host resolver answered; dropping query"
    );
    None
}

/// One DNS-over-TCP exchange with a host resolver.
async fn forward_tcp(upstream: SocketAddr, query: &[u8]) -> Option<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let exchange = async {
        let mut stream = crate::worker::netd::listen::dial_tcp(upstream, FORWARD_TIMEOUT)
            .await
            .ok()?;
        let mut framed = Vec::with_capacity(query.len() + 2);
        framed.extend_from_slice(&u16::try_from(query.len()).ok()?.to_be_bytes());
        framed.extend_from_slice(query);
        stream.write_all(&framed).await.ok()?;
        let mut len = [0u8; 2];
        stream.read_exact(&mut len).await.ok()?;
        let mut response = vec![0u8; usize::from(u16::from_be_bytes(len))];
        stream.read_exact(&mut response).await.ok()?;
        Some(response)
    };
    tokio::time::timeout(FORWARD_TIMEOUT, exchange)
        .await
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TYPE_A: RecordType = RecordType::A;
    const TYPE_SRV: RecordType = RecordType::SRV;
    const RCODE_NXDOMAIN: u8 = 3;
    const RCODE_SERVFAIL: u8 = 2;

    fn query(name: &str, qtype: RecordType) -> Vec<u8> {
        let mut message = Message::new();
        message
            .set_id(0x1234)
            .set_message_type(MessageType::Query)
            .set_recursion_desired(true)
            .add_query(Query::query(Name::from_ascii(name).unwrap(), qtype));
        message.to_vec().unwrap()
    }

    /// A stub "host resolver" on UDP that answers every query with
    /// `answer`, truncated (`TC`, no records) when `truncate` is set.
    async fn stub_resolver(answer: Ipv4Addr, truncate: bool) -> SocketAddr {
        let stub = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stub_addr = stub.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            loop {
                let Ok((len, peer)) = stub.recv_from(&mut buf).await else {
                    break;
                };
                let Some(asked) = parse_query(&buf[..len]) else {
                    continue;
                };
                let response = if truncate {
                    let mut message = Message::from_vec(
                        &respond(&asked, ResponseCode::NoError, Vec::new()).unwrap(),
                    )
                    .unwrap();
                    message.set_truncated(true);
                    message.to_vec().unwrap()
                } else {
                    respond(&asked, ResponseCode::NoError, vec![RData::A(A(answer))]).unwrap()
                };
                let _ = stub.send_to(&response, peer).await;
            }
        });
        stub_addr
    }

    #[test]
    fn parses_and_answers_the_zone() {
        let q = query("DB.svc.legion.", TYPE_A);
        let asked = parse_query(&q).unwrap();
        assert_eq!(asked.message.id(), 0x1234);
        assert_eq!(asked.name, "db.svc.legion", "lower-cased, no trailing dot");
        assert_eq!(zone_key(&asked.name).as_deref(), Some("db"));
        assert_eq!(zone_key("db").as_deref(), Some("db"));
        assert_eq!(zone_key("_db._tcp.svc.legion").as_deref(), Some("db"));
        assert_eq!(zone_key("api.github.com"), None);
        assert_eq!(
            zone_key("auth.shared.svc.legion").as_deref(),
            Some("auth.shared")
        );
        assert_eq!(
            zone_key("_auth._tcp.shared.svc.legion").as_deref(),
            Some("auth.shared")
        );
        assert_eq!(zone_key("a.b.c.svc.legion"), None);
        assert_eq!(zone_key("a..svc.legion"), None);

        let response = respond(
            &asked,
            ResponseCode::NoError,
            vec![RData::A(A(Ipv4Addr::new(127, 42, 7, 2)))],
        )
        .unwrap();
        assert_eq!(&response[0..2], &[0x12, 0x34]);
        assert_eq!(response[2] & 0x80, 0x80, "QR set");
        assert_eq!(response[2] & 0x01, 0x01, "RD echoed");
        assert_eq!(response[3] & 0x0F, 0, "NOERROR");
        assert_eq!(
            u16::from_be_bytes([response[6], response[7]]),
            1,
            "one answer"
        );
        assert_eq!(&response[response.len() - 4..], &[127, 42, 7, 2]);

        let nx = respond(&asked, ResponseCode::NXDomain, Vec::new()).unwrap();
        assert_eq!(nx[3] & 0x0F, RCODE_NXDOMAIN);
        // Garbage and responses are not queries.
        assert!(parse_query(&[0x12, 0x34]).is_none());
        assert!(parse_query(&nx).is_none());
    }

    #[test]
    fn forwarders_come_only_from_the_nameserver_lines() {
        assert_eq!(
            forwarders_in("# comment\nsearch example.com\nnameserver 127.0.0.53\nnameserver ::1\n"),
            vec![
                "127.0.0.53:53".parse::<SocketAddr>().unwrap(),
                "[::1]:53".parse::<SocketAddr>().unwrap(),
            ]
        );
        // No public resolver is invented for a file that names none.
        assert!(forwarders_in("search example.com\n").is_empty());
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
        let stub_addr = stub_resolver(Ipv4Addr::new(9, 9, 9, 9), false).await;

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
                attachment: Attachment::Host,
            },
            context,
            forwarders: vec![stub_addr],
        });

        // Zone name → VIP, SRV carries the canonical port, unknown → NXDOMAIN.
        let r = answer(&resolver, &query("db.svc.legion", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[127, 42, 9, 2]);
        let r = answer(&resolver, &query("db", TYPE_SRV), false)
            .await
            .unwrap();
        assert!(r.windows(2).any(|w| w == 5432u16.to_be_bytes()));
        let r = answer(&resolver, &query("nope.svc.legion", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(r[3] & 0x0F, RCODE_NXDOMAIN);

        // External: allowed names forward (9.9.9.9 from the stub), a
        // port-constrained allow still resolves (port unknown at DNS time),
        // and a name the policy would deny is NXDOMAIN plus a denial.
        let r = answer(&resolver, &query("allowed.example", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[9, 9, 9, 9]);
        let r = answer(&resolver, &query("api.github.com", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[9, 9, 9, 9]);
        let r = answer(&resolver, &query("evil.example", TYPE_A), false)
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
        let stub_addr = stub_resolver(Ipv4Addr::new(9, 9, 9, 9), false).await;
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
        let resolver = |attachment: Attachment| {
            Arc::new(Resolver {
                spec: DnsSpec {
                    allocation_id: 3,
                    resolver: None,
                    names: [("db".to_string(), (Ipv4Addr::new(127, 42, 9, 2), 5432))]
                        .into_iter()
                        .collect(),
                    attachment,
                },
                context: context.clone(),
                forwarders: vec![stub_addr],
            })
        };

        let gated = resolver(Attachment::GuestVsock {
            scratch: "/allocs/3/.sandbox".into(),
        });
        let r = answer(&gated, &query("pypi.org", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(r[3] & 0x0F, RCODE_NXDOMAIN);
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.destination == "pypi.org" && d.source == "gated-default"),
            "{drained:?}"
        );
        let r = answer(&gated, &query("db.svc.legion", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(
            &r[r.len() - 4..],
            &[127, 42, 9, 2],
            "the zone still resolves"
        );

        let cooperative = resolver(Attachment::Host);
        let r = answer(&cooperative, &query("pypi.org", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(&r[r.len() - 4..], &[9, 9, 9, 9]);
    }

    fn plain_resolver(forwarders: Vec<SocketAddr>) -> Arc<Resolver> {
        use crate::worker::netd::{Denials, NetPolicyCache};
        Arc::new(Resolver {
            spec: DnsSpec {
                allocation_id: 4,
                resolver: None,
                names: BTreeMap::new(),
                attachment: Attachment::Host,
            },
            context: Arc::new(ForwarderContext::new(
                Arc::new(NetPolicyCache::new()),
                Arc::new(Denials::new()),
            )),
            forwarders,
        })
    }

    /// A node that names no resolver answers outside names `SERVFAIL` at
    /// once, and asks nobody it was not told about.
    #[tokio::test]
    async fn no_host_resolver_is_servfail_not_a_public_one_test() {
        let resolver = plain_resolver(Vec::new());
        let r = answer(&resolver, &query("pypi.org", TYPE_A), false)
            .await
            .unwrap();
        assert_eq!(r[3] & 0x0F, RCODE_SERVFAIL);
    }

    /// A truncated UDP answer goes back as it is to a UDP client (it asks
    /// again over TCP), and is fetched again over TCP for a TCP client.
    #[tokio::test]
    async fn a_truncated_answer_is_refetched_over_tcp_for_a_tcp_client_test() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // One resolver address serving a truncated UDP answer and the full
        // answer over TCP, as a real resolver does for a large response.
        let udp_addr = stub_resolver(Ipv4Addr::new(9, 9, 9, 9), true).await;
        let tcp = tokio::net::TcpListener::bind(udp_addr).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = tcp.accept().await {
                let mut len = [0u8; 2];
                if stream.read_exact(&mut len).await.is_err() {
                    continue;
                }
                let mut q = vec![0u8; usize::from(u16::from_be_bytes(len))];
                if stream.read_exact(&mut q).await.is_err() {
                    continue;
                }
                let asked = parse_query(&q).unwrap();
                let full = respond(
                    &asked,
                    ResponseCode::NoError,
                    vec![RData::A(A(Ipv4Addr::new(8, 8, 4, 4)))],
                )
                .unwrap();
                let mut framed = (full.len() as u16).to_be_bytes().to_vec();
                framed.extend_from_slice(&full);
                let _ = stream.write_all(&framed).await;
            }
        });
        let resolver = plain_resolver(vec![udp_addr]);

        let over_udp = answer(&resolver, &query("big.example", TYPE_A), false)
            .await
            .unwrap();
        assert!(truncated(&over_udp), "a UDP client gets the TC answer");

        let over_tcp = answer(&resolver, &query("big.example", TYPE_A), true)
            .await
            .unwrap();
        assert!(!truncated(&over_tcp));
        assert_eq!(&over_tcp[over_tcp.len() - 4..], &[8, 8, 4, 4]);
    }
}
