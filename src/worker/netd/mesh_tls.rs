//! Mesh TLS that survives certificate refreshes (mesh-control-path plan
//! §6.3, review NET3; docs/network-handlers.md, "The mesh").
//!
//! Every node-to-node mesh hop is mTLS on node certificates. A full
//! handshake costs two signatures and a chain verification on each side, so
//! the node keeps TLS 1.3 sessions and resumes them. Two rustls facts shape
//! how:
//!
//! - A client only resumes a session with the **same** verifier and client
//!   certificate resolver objects that made it. Rebuilding the client config
//!   on a certificate refresh therefore threw every session away, every 8
//!   hours on every node. Here one client config and one server config are
//!   built per **trust bundle**, around a [`NodeCertResolver`] whose key is
//!   swapped in place when the certificate is re-issued. A refresh keeps
//!   every session; a different trust bundle builds new configs with new,
//!   empty stores, so nothing established under a CA that left the bundle
//!   resumes on either end.
//! - A resumed session carries the identity of the handshake that started
//!   its chain, and each resumption issues new tickets with that same
//!   identity. Left alone, a chain could outlive the certificates it was
//!   established with. The client's [`MeshTicketStore`] tags every ticket
//!   with the oldest of its own certificates the chain may carry and drops
//!   tickets whose certificate expires within [`LEAF_MARGIN_SECS`]; both ends
//!   also refuse a resumed session whose peer certificate has expired
//!   ([`resumed_with_expired_peer`]).
//!
//! The server keeps sessions in one node-wide store (stateful tickets, which
//! rustls makes single-use), shared by every mesh port.

use crate::prelude::*;
use crate::worker::netd::tls::MeshIdentity;
use rustls::client::{ClientSessionStore, ResolvesClientCert, Resumption, Tls12Resumption};
use rustls::server::{ClientHello, ResolvesServerCert, ServerSessionMemoryCache};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, NamedGroup, ServerConfig, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName};
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Sessions the node-wide server store holds.
pub const SERVER_SESSION_CACHE: usize = 4096;

/// Tickets the client keeps per peer node.
pub const TICKETS_PER_PEER: usize = 8;

/// Peers the client keeps tickets for. Cleared wholesale past it.
pub const MAX_PEERS: usize = 4096;

/// A ticket whose chain may carry a certificate of this node that expires
/// within this long is not offered: the next dial does a full handshake with
/// the current certificate instead.
pub const LEAF_MARGIN_SECS: i64 = 30 * 60;

/// A ticket inserted within this long of a ticket taken for the same peer
/// is assumed to continue that ticket's chain.
const TAKE_WINDOW: Duration = Duration::from_secs(60);

/// Certificates the resolver remembers, newest last.
const LEAVES_KEPT: usize = 16;

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// The `notAfter` of a chain's leaf, as Unix seconds.
pub fn leaf_not_after(chain: &[CertificateDer<'_>]) -> Option<i64> {
    let leaf = chain.first()?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref()).ok()?;
    Some(cert.validity().not_after.timestamp())
}

/// Whether a handshake resumed a session whose peer certificate has expired.
/// Both ends refuse such a connection: the identity a resumed session
/// carries is only as good as the certificate that established it.
pub fn resumed_with_expired_peer(connection: &rustls::CommonState) -> bool {
    connection.handshake_kind() == Some(rustls::HandshakeKind::Resumed)
        && connection
            .peer_certificates()
            .and_then(leaf_not_after)
            .is_some_and(|not_after| not_after <= now_unix())
}

fn certified_key(identity: &MeshIdentity) -> R<CertifiedKey> {
    let key = rustls::crypto::ring::sign::any_supported_type(&identity.private_key()?)
        .map_err(|err| anyhow::anyhow!("mesh key: {err}"))?;
    Ok(CertifiedKey::new(identity.cert_chain.clone(), key))
}

#[derive(Debug)]
struct ResolverState {
    current: Arc<CertifiedKey>,
    generation: u64,
    /// (generation, notAfter) of the certificates this resolver served.
    leaves: VecDeque<(u64, i64)>,
}

/// This node's certificate, swappable while configs built on it keep their
/// session stores. Serves both ends of a mesh hop.
#[derive(Debug)]
pub struct NodeCertResolver {
    state: RwLock<ResolverState>,
}

impl NodeCertResolver {
    fn new(key: CertifiedKey, not_after: i64) -> Self {
        NodeCertResolver {
            state: RwLock::new(ResolverState {
                current: Arc::new(key),
                generation: 1,
                leaves: VecDeque::from([(1, not_after)]),
            }),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, ResolverState> {
        self.state.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Serve `key` from now on. Sessions made under the previous one stay
    /// resumable while that certificate is valid.
    fn swap(&self, key: CertifiedKey, not_after: i64) {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.generation += 1;
        let generation = state.generation;
        state.current = Arc::new(key);
        state.leaves.push_back((generation, not_after));
        while state.leaves.len() > LEAVES_KEPT {
            state.leaves.pop_front();
        }
    }

    /// The generation of the certificate served now.
    pub fn generation(&self) -> u64 {
        self.read().generation
    }

    /// Whether a session whose chain may carry the certificate of
    /// `generation` may still be offered at `now`.
    pub fn usable(&self, generation: u64, now: i64) -> bool {
        self.read()
            .leaves
            .iter()
            .find(|(held, _)| *held == generation)
            .is_some_and(|(_, not_after)| not_after - now > LEAF_MARGIN_SECS)
    }

    fn current(&self) -> Arc<CertifiedKey> {
        Arc::clone(&self.read().current)
    }
}

impl ResolvesClientCert for NodeCertResolver {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl ResolvesServerCert for NodeCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

/// Per-peer tickets with the certificate generation their chain may carry.
/// Generic over the ticket so the bookkeeping is testable without rustls
/// session values, which only rustls can make.
#[derive(Debug)]
struct Shelf<T> {
    tickets: HashMap<String, VecDeque<(u64, T)>>,
    /// Recently taken tickets per peer: (when, generation).
    taken: HashMap<String, Vec<(Instant, u64)>>,
}

impl<T> Default for Shelf<T> {
    fn default() -> Self {
        Shelf {
            tickets: HashMap::new(),
            taken: HashMap::new(),
        }
    }
}

impl<T> Shelf<T> {
    /// Store a ticket for `peer`. A ticket that arrives soon after one was
    /// taken for the same peer continues that ticket's chain, so it inherits
    /// the oldest such generation; otherwise it came from a full handshake
    /// with `current`.
    fn insert(&mut self, peer: &str, ticket: T, current: u64, now: Instant) {
        let taken = self.taken.entry(peer.to_string()).or_default();
        taken.retain(|(at, _)| now.duration_since(*at) < TAKE_WINDOW);
        let generation = taken
            .iter()
            .map(|(_, generation)| *generation)
            .min()
            .unwrap_or(current);
        if taken.is_empty() {
            self.taken.remove(peer);
        }
        if self.tickets.len() >= MAX_PEERS && !self.tickets.contains_key(peer) {
            self.tickets.clear();
            self.taken.clear();
        }
        let tickets = self.tickets.entry(peer.to_string()).or_default();
        tickets.push_back((generation, ticket));
        while tickets.len() > TICKETS_PER_PEER {
            tickets.pop_front();
        }
    }

    /// The newest ticket for `peer` that `usable` accepts; older unusable
    /// ones are dropped on the way.
    fn take(&mut self, peer: &str, now: Instant, usable: impl Fn(u64) -> bool) -> Option<T> {
        let tickets = self.tickets.get_mut(peer)?;
        while let Some((generation, ticket)) = tickets.pop_back() {
            if usable(generation) {
                self.taken
                    .entry(peer.to_string())
                    .or_default()
                    .push((now, generation));
                return Some(ticket);
            }
        }
        self.tickets.remove(peer);
        None
    }
}

/// The client's TLS 1.3 tickets, bounded by the age of the certificates
/// their chains may carry. TLS 1.2 is never used on the mesh.
#[derive(Debug)]
pub struct MeshTicketStore {
    resolver: Arc<NodeCertResolver>,
    shelf: Mutex<Shelf<rustls::client::Tls13ClientSessionValue>>,
    kx_hints: Mutex<HashMap<String, NamedGroup>>,
}

impl MeshTicketStore {
    fn new(resolver: Arc<NodeCertResolver>) -> Self {
        MeshTicketStore {
            resolver,
            shelf: Mutex::new(Shelf::default()),
            kx_hints: Mutex::new(HashMap::new()),
        }
    }
}

fn peer_key(name: &ServerName<'_>) -> String {
    name.to_str().into_owned()
}

impl ClientSessionStore for MeshTicketStore {
    fn set_kx_hint(&self, server_name: ServerName<'static>, group: NamedGroup) {
        let mut hints = self.kx_hints.lock().unwrap_or_else(|p| p.into_inner());
        if hints.len() >= MAX_PEERS {
            hints.clear();
        }
        hints.insert(peer_key(&server_name), group);
    }

    fn kx_hint(&self, server_name: &ServerName<'_>) -> Option<NamedGroup> {
        self.kx_hints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&peer_key(server_name))
            .copied()
    }

    fn set_tls12_session(
        &self,
        _server_name: ServerName<'static>,
        _value: rustls::client::Tls12ClientSessionValue,
    ) {
    }

    fn tls12_session(
        &self,
        _server_name: &ServerName<'_>,
    ) -> Option<rustls::client::Tls12ClientSessionValue> {
        None
    }

    fn remove_tls12_session(&self, _server_name: &ServerName<'static>) {}

    fn insert_tls13_ticket(
        &self,
        server_name: ServerName<'static>,
        value: rustls::client::Tls13ClientSessionValue,
    ) {
        let current = self.resolver.generation();
        self.shelf.lock().unwrap_or_else(|p| p.into_inner()).insert(
            &peer_key(&server_name),
            value,
            current,
            Instant::now(),
        );
    }

    fn take_tls13_ticket(
        &self,
        server_name: &ServerName<'static>,
    ) -> Option<rustls::client::Tls13ClientSessionValue> {
        let now = now_unix();
        self.shelf.lock().unwrap_or_else(|p| p.into_inner()).take(
            &peer_key(server_name),
            Instant::now(),
            |generation| self.resolver.usable(generation, now),
        )
    }
}

/// The node's mesh TLS for one trust bundle: a client config and a server
/// config sharing one swappable certificate, each with its session store.
pub struct MeshTls {
    bundle: Vec<CertificateDer<'static>>,
    resolver: Arc<NodeCertResolver>,
    client: Arc<ClientConfig>,
    server: Arc<ServerConfig>,
}

impl std::fmt::Debug for MeshTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshTls")
            .field("bundle", &self.bundle.len())
            .field("generation", &self.resolver.generation())
            .finish_non_exhaustive()
    }
}

impl MeshTls {
    /// Configs for `identity`'s trust bundle, serving its certificate.
    pub fn new(identity: &MeshIdentity) -> R<Self> {
        let resolver = Arc::new(NodeCertResolver::new(
            certified_key(identity)?,
            identity.expires_at,
        ));
        let mut client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(identity.roots())
            .with_client_cert_resolver(resolver.clone());
        client.resumption =
            Resumption::store(Arc::new(MeshTicketStore::new(Arc::clone(&resolver))))
                .tls12_resumption(Tls12Resumption::Disabled);
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(identity.roots()))
            .build()
            .map_err(|err| anyhow::anyhow!("mesh client verifier: {err}"))?;
        let mut server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(resolver.clone());
        server.session_storage = ServerSessionMemoryCache::new(SERVER_SESSION_CACHE);
        Ok(MeshTls {
            bundle: identity.ca.clone(),
            resolver,
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }

    /// Whether `identity` is under the same trust bundle, so a refresh can
    /// keep these configs and their sessions.
    pub fn same_bundle(&self, identity: &MeshIdentity) -> bool {
        self.bundle == identity.ca
    }

    /// Serve `identity`'s re-issued certificate under the same bundle.
    pub fn refresh(&self, identity: &MeshIdentity) -> R<()> {
        self.resolver
            .swap(certified_key(identity)?, identity.expires_at);
        Ok(())
    }

    /// Dial another node.
    pub fn client_config(&self) -> Arc<ClientConfig> {
        Arc::clone(&self.client)
    }

    /// Terminate mesh TLS on an advertised port (clients must present a
    /// certificate from the mesh CA).
    pub fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server)
    }
}

#[cfg(test)]
mod mesh_tls_test {
    use super::*;
    use crate::mesh_ca::{generate_ca, issue_node_cert, node_dns_name};
    use crate::worker::netd::tls::server_name;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn identity(ca: &(String, String), node: &str) -> R<MeshIdentity> {
        let (cert, key, expires) = issue_node_cert(&ca.0, &ca.1, node)?;
        MeshIdentity::from_pem(node, &cert, &key, &ca.0, expires)
    }

    /// One mesh hop: the server echoes, so the client reads the session
    /// tickets the server sends after the handshake. Returns how each end
    /// saw the handshake.
    async fn hop(
        client: &MeshTls,
        server: &MeshTls,
    ) -> R<(rustls::HandshakeKind, rustls::HandshakeKind)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let acceptor = tokio_rustls::TlsAcceptor::from(server.server_config());
        let served = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut tls = acceptor.accept(stream).await?;
            let kind = tls.get_ref().1.handshake_kind().expect("handshaken");
            let mut buf = [0u8; 4];
            tls.read_exact(&mut buf).await?;
            tls.write_all(&buf).await?;
            tls.shutdown().await?;
            Ok::<_, anyhow::Error>(kind)
        });
        let connector = tokio_rustls::TlsConnector::from(client.client_config());
        let stream = TcpStream::connect(addr).await?;
        let mut tls = connector
            .connect(server_name(&node_dns_name("node-b"))?, stream)
            .await?;
        let kind = tls.get_ref().1.handshake_kind().expect("handshaken");
        tls.write_all(b"ping").await?;
        let mut buf = Vec::new();
        tls.read_to_end(&mut buf).await?;
        assert_eq!(buf, b"ping");
        Ok((kind, served.await??))
    }

    #[tokio::test]
    async fn a_certificate_refresh_keeps_sessions_and_a_new_bundle_does_not() -> R<()> {
        use rustls::HandshakeKind::{Full, Resumed};
        let ca = generate_ca()?;
        let a = MeshTls::new(&identity(&ca, "node-a")?)?;
        let b = MeshTls::new(&identity(&ca, "node-b")?)?;
        assert_eq!(hop(&a, &b).await?, (Full, Full));
        assert_eq!(
            hop(&a, &b).await?,
            (Resumed, Resumed),
            "the second hop resumes"
        );

        // Both nodes re-issue under the same CA: the configs stay, and so do
        // the sessions.
        let refreshed = identity(&ca, "node-a")?;
        assert!(a.same_bundle(&refreshed));
        a.refresh(&refreshed)?;
        b.refresh(&identity(&ca, "node-b")?)?;
        assert_eq!(hop(&a, &b).await?, (Resumed, Resumed));

        // A rotation to a new trust bundle builds new configs: nothing made
        // under the old one resumes.
        let rotated = generate_ca()?;
        let a2 = identity(&rotated, "node-a")?;
        assert!(!a.same_bundle(&a2));
        let a2 = MeshTls::new(&a2)?;
        let b2 = MeshTls::new(&identity(&rotated, "node-b")?)?;
        assert_eq!(hop(&a2, &b2).await?, (Full, Full));
        Ok(())
    }

    #[test]
    fn tickets_inherit_their_chains_generation_and_expire_with_it() {
        let mut shelf: Shelf<u32> = Shelf::default();
        let start = Instant::now();
        // A full handshake under generation 1.
        shelf.insert("b", 10, 1, start);
        shelf.insert("b", 11, 1, start);
        // The certificate is re-issued (generation 2). A ticket from a
        // session resumed off generation 1 keeps generation 1.
        let taken = shelf.take("b", start, |_| true);
        assert_eq!(taken, Some(11), "newest first");
        shelf.insert("b", 12, 2, start + Duration::from_secs(1));
        // Once generation 1 is about to expire, its tickets are not offered,
        // and the stale ones are dropped.
        let later = start + TAKE_WINDOW * 2;
        assert_eq!(shelf.take("b", later, |generation| generation >= 2), None);
        assert!(!shelf.tickets.contains_key("b"));
        // A full handshake under generation 2, long after any take, is
        // generation 2.
        shelf.insert("b", 13, 2, later);
        assert_eq!(
            shelf.take("b", later, |generation| generation >= 2),
            Some(13)
        );
    }

    #[test]
    fn the_resolver_refuses_sessions_on_a_certificate_about_to_expire() -> R<()> {
        let ca = generate_ca()?;
        let id = identity(&ca, "node-a")?;
        let resolver = NodeCertResolver::new(certified_key(&id)?, 10_000);
        assert!(resolver.usable(1, 10_000 - LEAF_MARGIN_SECS - 1));
        assert!(!resolver.usable(1, 10_000 - LEAF_MARGIN_SECS));
        assert!(
            !resolver.usable(7, 0),
            "an unknown generation is not usable"
        );
        resolver.swap(certified_key(&id)?, 20_000);
        assert_eq!(resolver.generation(), 2);
        assert!(resolver.usable(1, 0) && resolver.usable(2, 0));
        Ok(())
    }
}
