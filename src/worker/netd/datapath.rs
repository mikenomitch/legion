//! The worker ↔ datapath boundary.
//!
//! The worker owns the manager connection; the datapath owns listeners,
//! namespaces, vsock sockets, policy, and the ledgers its listeners write.
//! Everything that crosses between them is one of three types:
//!
//! - [`NetdInput`] — what the worker tells the datapath: one acknowledged
//!   heartbeat's desired set, endpoint feed, and policy assignments
//!   ([`DesiredNetwork`]), and the answers to the fetches the datapath asked
//!   for (policy bodies, ingress certificates, upstream credentials, the
//!   node's mesh certificate), plus the node facts only the worker sees.
//! - [`NetdWants`] — what the datapath needs fetched from the manager after
//!   a desired set lands. The worker makes the calls and sends the answers
//!   back as [`NetdInput`]s.
//! - [`NetdReport`] — what the datapath reports on the next heartbeat:
//!   denials, service-graph samples, and per-allocation status. A heartbeat
//!   that fails hands the report back ([`Netd::redrive`]).
//!
//! [`Netd`] is the in-process implementation. A `legion netd` process would
//! carry the same three types over a socket; nothing on the worker's side
//! reaches past them.

use crate::prelude::*;
use crate::structs::netpolicy::EffectivePolicy;
use crate::structs::{
    AllocNetPolicy, IngressCertificate, IngressCertificateKnown, NetAllocStatus, NetDenial,
    NetEdgeSample, TaskAllocation, UpstreamCredentialKnown, UpstreamCredentialValue,
    UpstreamEndpoints,
};
use crate::worker::forwarder::{SecretValue, UpstreamForwarder};
use crate::worker::netd::activity::ActivityLedger;
use crate::worker::netd::kernel_guard::GuardedAllocation;
use crate::worker::netd::specs;
use crate::worker::netd::state::{NetdState, NetdStateStore};
use crate::worker::netd::tls::MeshIdentity;
use crate::worker::netd::PolicyLookup;
use serde::{Deserialize, Serialize};

use std::collections::HashSet;
use std::path::PathBuf;

/// One acknowledged heartbeat's datapath inputs. Sent only after the manager
/// acknowledged the heartbeat, which is what makes the datapath fail-static:
/// while the manager is unreachable the listeners keep their last known good
/// endpoints and policy. Within one, an empty endpoint list is authoritative
/// (a killed or intention-denied backend must stop receiving traffic), while
/// an upstream missing from `upstream_endpoints` keeps its current list.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DesiredNetwork {
    /// The allocations the node runs.
    #[serde(default)]
    pub desired: Vec<TaskAllocation>,
    /// The endpoints each upstream (and each `proxy` handler) may dial.
    #[serde(default)]
    pub upstream_endpoints: Vec<UpstreamEndpoints>,
    /// The policy hash each allocation runs under.
    #[serde(default)]
    pub net_policies: Vec<AllocNetPolicy>,
}

/// What the worker tells the datapath.
#[derive(Debug)]
pub enum NetdInput {
    /// One acknowledged heartbeat's desired set, endpoint feed, and policy
    /// assignments. Creates the gated namespaces the set needs before
    /// anything else, so the reconcile that follows never starts a gated
    /// task without one.
    Desired(DesiredNetwork),
    /// The node's id (`node` proxy targets, the mesh certificate's name).
    NodeId(String),
    /// A policy body fetched by hash ([`NetdWants::policy_bodies`]).
    PolicyBody {
        hash: String,
        policy: Box<EffectivePolicy>,
    },
    /// The node's mesh certificate, issued or re-issued.
    MeshIdentity(MeshIdentity),
    /// `GetIngressCertificates`' answer to
    /// [`NetdWants::ingress_certificates`].
    IngressCertificates(Vec<IngressCertificate>),
    /// `GetUpstreamCredentials`' answer to
    /// [`NetdWants::upstream_credentials`]. Values stay in memory only.
    UpstreamCredentials(Vec<UpstreamCredentialValue>),
    /// The node's interface addresses may have changed: the host guard's
    /// node-address class follows them.
    RefreshNodeAddresses,
    /// The allocations the kernel host guard matches this beat (their cgroup
    /// leaves are the worker's to find).
    KernelGuard(Vec<GuardedAllocation>),
}

/// What the datapath needs fetched from the manager for the desired set it
/// holds. Empty lists cost the worker nothing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NetdWants {
    /// Policy hashes assigned without a body yet (`FetchNetPolicy`).
    pub policy_bodies: Vec<String>,
    /// Allocations whose ingress chain takes its certificate from KV, with
    /// the version held (`GetIngressCertificates`).
    pub ingress_certificates: Vec<IngressCertificateKnown>,
    /// Declared credential headers, with the version held
    /// (`GetUpstreamCredentials`).
    pub upstream_credentials: Vec<UpstreamCredentialKnown>,
}

/// What the datapath reports on the next heartbeat.
#[derive(Debug, Clone, Default)]
pub struct NetdReport {
    /// Denials recorded since the last report, folded.
    pub denials: Vec<NetDenial>,
    /// Service-graph samples since the last report.
    pub edges: Vec<NetEdgeSample>,
    /// Status of every allocation in the last desired set the datapath was
    /// told about.
    pub statuses: Vec<NetAllocStatus>,
}

/// The node datapath behind its boundary.
pub struct Netd {
    forwarder: Arc<UpstreamForwarder>,
    /// The worker's allocation directory root, where each sandbox keeps its
    /// VMM sockets.
    alloc_root: PathBuf,
    /// Where the datapath's inputs are persisted across worker restarts.
    state: Option<NetdStateStore>,
    /// Hash of the last persisted state, so an unchanged heartbeat costs no
    /// write.
    last_state_hash: Mutex<u64>,
    /// The last desired set, endpoint feed, and assignments: what the
    /// status report covers and what is persisted.
    last: Mutex<DesiredNetwork>,
    /// Allocations this datapath created a gated namespace for, so
    /// departures are torn down. A namespace that outlives a worker restart
    /// is found again by path.
    namespaces: Mutex<HashSet<i64>>,
}

impl Netd {
    /// A datapath around `forwarder` (with its context, resolvers, egress
    /// proxies, and ingress listeners attached).
    pub fn new(forwarder: Arc<UpstreamForwarder>, alloc_root: impl Into<PathBuf>) -> Self {
        Netd {
            forwarder,
            alloc_root: alloc_root.into(),
            state: None,
            last_state_hash: Mutex::new(0),
            last: Mutex::new(DesiredNetwork::default()),
            namespaces: Mutex::new(HashSet::new()),
        }
    }

    /// Persist inputs to `store` after every change, and restore from it
    /// ([`Self::restore`]).
    pub fn with_state_store(mut self, store: NetdStateStore) -> Self {
        self.state = Some(store);
        self
    }

    /// Hand the datapath one input.
    pub async fn send(&self, input: NetdInput) {
        let context = self.forwarder.context();
        match input {
            NetdInput::Desired(desired) => self.apply_desired(desired).await,
            NetdInput::NodeId(node_id) => context.set_node_id(&node_id),
            NetdInput::PolicyBody { hash, policy } => {
                let Some(cache) = context.cache() else {
                    return;
                };
                match cache.insert_body(&hash, *policy) {
                    Ok(()) => debug!(hash = %hash, "Fetched network policy body"),
                    Err(err) => warn!(hash = %hash, error = ?err, "Rejected network policy body"),
                }
            }
            NetdInput::MeshIdentity(mesh) => {
                if let Err(err) = context.set_mesh_identity(mesh) {
                    warn!(error = ?err, "Rejected the node certificate for the mesh");
                }
            }
            NetdInput::IngressCertificates(certificates) => {
                self.apply_ingress_certificates(certificates)
            }
            NetdInput::UpstreamCredentials(values) => self.apply_upstream_credentials(values),
            NetdInput::RefreshNodeAddresses => context.refresh_node_addresses(),
            NetdInput::KernelGuard(allocations) => {
                if !context.kernel_guard_active() {
                    return;
                }
                let context = context.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    context.reconcile_kernel_guard(allocations)
                })
                .await;
            }
        }
    }

    /// What the worker should fetch for the desired set the datapath holds.
    pub fn wants(&self) -> NetdWants {
        let context = self.forwarder.context();
        let last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        let desired = &last.desired;
        let Some(cache) = context.cache() else {
            return NetdWants::default();
        };
        let ingress_certificates = desired
            .iter()
            .filter(|allocation| {
                cache
                    .policy_for(allocation.allocation_id)
                    .and_then(|policy| policy.ingress.as_ref()?.tls.clone())
                    .is_some_and(|tls| tls.cert_kv_path().is_some())
            })
            .map(|allocation| IngressCertificateKnown {
                allocation_id: allocation.allocation_id,
                version: context.kv_cert_version(allocation.allocation_id),
            })
            .collect();
        let upstream_credentials = specs::declared_credentials(desired)
            .into_iter()
            .map(
                |(allocation_id, upstream, header)| UpstreamCredentialKnown {
                    version: context.credential_version(allocation_id, &upstream, &header),
                    allocation_id,
                    upstream,
                    header,
                },
            )
            .collect();
        NetdWants {
            policy_bodies: cache.pending_hashes(),
            ingress_certificates,
            upstream_credentials,
        }
    }

    /// Drain what the next heartbeat reports. Hand it back with
    /// [`Self::redrive`] when the heartbeat fails.
    pub fn report(&self) -> NetdReport {
        let context = self.forwarder.context();
        NetdReport {
            denials: context
                .denials()
                .map(|denials| denials.drain())
                .unwrap_or_default(),
            edges: context.edges().drain(),
            statuses: self.statuses(),
        }
    }

    /// Put a report's denials and samples back after a failed heartbeat, so
    /// the next one still carries them.
    pub fn redrive(&self, report: NetdReport) {
        let context = self.forwarder.context();
        if let Some(denials) = context.denials() {
            denials.restore(report.denials);
        }
        context.edges().restore(report.edges);
    }

    /// The status of every allocation in the last desired set.
    pub fn statuses(&self) -> Vec<NetAllocStatus> {
        let desired = self
            .last
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .desired
            .clone();
        specs::net_status(&self.forwarder, &desired, &self.alloc_root)
    }

    /// Ingress connections and bytes per allocation, for the idle policy.
    pub fn activity(&self) -> &Arc<ActivityLedger> {
        self.forwarder.context().activity()
    }

    /// Whether the kernel host guard runs on this node.
    pub fn kernel_guard_active(&self) -> bool {
        self.forwarder.context().kernel_guard_active()
    }

    /// Whether the node holds a mesh certificate.
    pub fn has_mesh_identity(&self) -> bool {
        self.forwarder.context().mesh_identity().is_some()
    }

    /// Save the datapath's inputs when they differ from the last save. Off
    /// the heartbeat path (a blocking task), owner-only, atomic. Secrets —
    /// credentials, KV-held certificates, the mesh key — are never saved.
    pub fn persist(&self) {
        let Some(store) = self.state.as_ref() else {
            return;
        };
        let state = NetdState {
            network: self.last.lock().unwrap_or_else(|p| p.into_inner()).clone(),
            policy_bodies: self
                .forwarder
                .context()
                .cache()
                .map(|cache| cache.bodies())
                .unwrap_or_default(),
        };
        let hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            serde_json::to_string(&state)
                .unwrap_or_default()
                .hash(&mut hasher);
            hasher.finish()
        };
        {
            let mut last = self
                .last_state_hash
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if *last == hash {
                return;
            }
            *last = hash;
        }
        let path = store.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let store = NetdStateStore::beside(&path);
            if let Err(err) = store.save(&state) {
                warn!(path = %path.display(), error = ?err, "Failed to persist datapath state");
            }
        });
    }

    /// Replay the persisted inputs for the allocations in `live` (the ones
    /// that survived the restart): listeners, VIPs, resolvers, egress
    /// proxies, ingress ports, identities and policy bodies come back from
    /// disk, so restored allocations have their network before the manager
    /// is even contacted. Returns how many allocations were restored.
    pub async fn restore(&self, live: &HashSet<i64>) -> usize {
        let Some(store) = self.state.as_ref() else {
            return 0;
        };
        let state = match store.load() {
            Ok(state) => state,
            Err(err) => {
                warn!(error = ?err, "Ignoring unreadable datapath state");
                return 0;
            }
        };
        if state.is_empty() {
            return 0;
        }
        let NetdState {
            network,
            policy_bodies,
        } = state;
        // Only allocations that survived the restart get their datapath
        // back; the rest is the manager's to re-dispatch or forget.
        let desired: Vec<TaskAllocation> = network
            .desired
            .into_iter()
            .filter(|a| live.contains(&a.allocation_id))
            .collect();
        if desired.is_empty() {
            return 0;
        }
        let restored = desired.len();
        if let Some(cache) = self.forwarder.context().cache() {
            for (hash, body) in policy_bodies {
                let _ = cache.insert_body(&hash, body);
            }
        }
        self.apply_desired(DesiredNetwork {
            desired,
            upstream_endpoints: network
                .upstream_endpoints
                .into_iter()
                .filter(|u| live.contains(&u.allocation_id))
                .collect(),
            net_policies: network
                .net_policies
                .into_iter()
                .filter(|p| live.contains(&p.allocation_id))
                .collect(),
        })
        .await;
        info!(
            allocations = restored,
            listeners = self.forwarder.active().len(),
            "Restored the datapath from disk ahead of the first heartbeat"
        );
        restored
    }

    async fn apply_desired(&self, input: DesiredNetwork) {
        let desired = &input.desired;
        let context = self.forwarder.context();
        // Policy first: attribution for every allocation and the hash each
        // runs under, so a listener bound below judges by it from its first
        // connection. Bodies not cached yet are in `wants()`; until one
        // arrives a gated allocation stays closed.
        if let Some(cache) = context.cache() {
            cache.set_identities(crate::worker::netd::policy::identities_from_desired(
                desired,
            ));
            cache.apply_assignments(&input.net_policies);
        }
        // Gated namespaces next: the egress proxy, the resolver, and the
        // upstream listeners of a gated allocation bind *inside* its
        // namespace, so binding them before it exists fails and leaves the
        // task with no network until the next beat.
        self.apply_capture(desired).await;
        let forwarder = &self.forwarder;
        let root = self.alloc_root.as_path();
        forwarder.sync(&specs::upstream_specs(desired, root)).await;
        if let Some(dns) = forwarder.dns() {
            dns.sync(&specs::dns_specs(desired, root)).await;
        }
        if let Some(proxies) = forwarder.proxies() {
            proxies.sync(&specs::proxy_specs(desired, root)).await;
        }
        if let Some(ingress) = forwarder.ingress() {
            ingress.sync(&specs::ingress_specs(desired, root)).await;
        }
        let mut mesh_wanted = false;
        for update in &input.upstream_endpoints {
            let endpoints = specs::dialable_endpoints(update);
            mesh_wanted |= endpoints.iter().any(|endpoint| endpoint.mesh_tls);
            if update.for_handler {
                // A `proxy` handler's target: dialed on hand-off, no listener.
                context.set_handler_endpoints(update.allocation_id, &update.service, endpoints);
            } else {
                forwarder.set_endpoints(update.allocation_id, &update.service, endpoints);
            }
        }
        let live: HashSet<i64> = desired
            .iter()
            .map(|allocation| allocation.allocation_id)
            .collect();
        context.retain_allocations(&live);
        // Credentials for allocations that left, or that no longer declare a
        // header, are forgotten before anything is fetched.
        context.retain_credentials(&specs::declared_credentials(desired).into_iter().collect());
        // The node needs a mesh certificate when it dials a mesh endpoint or
        // terminates mesh TLS on an advertised port.
        mesh_wanted |= desired.iter().any(|allocation| {
            !allocation.network.trim().is_empty()
                && serde_json::from_str::<crate::structs::AllocNetwork>(&allocation.network)
                    .map(|network| network.ingress_tls.as_deref() == Some("legion"))
                    .unwrap_or(false)
        });
        context.set_mesh_wanted(mesh_wanted);
        *self.last.lock().unwrap_or_else(|p| p.into_inner()) = input;
    }

    /// Gated namespaces: created for every gated allocation in the desired
    /// set *before* anything is started (a container joins the namespace at
    /// start; an exec task enters it before exec), and removed for
    /// allocations that have left it. Failures are logged and retried on the
    /// next desired set; a gated task whose namespace is missing refuses to
    /// start with a clear error rather than starting unconfined.
    async fn apply_capture(&self, desired: &[TaskAllocation]) {
        if !crate::worker::netd::netns::platform_supports_netns() {
            return;
        }
        let wanted: HashSet<i64> = specs::gated_namespace_ids(desired, &self.alloc_root)
            .into_iter()
            .collect();
        // The guard is never held across an await: take the set out,
        // reconcile, put it back.
        let previously =
            std::mem::take(&mut *self.namespaces.lock().unwrap_or_else(|p| p.into_inner()));
        for allocation_id in wanted.iter().copied() {
            if crate::worker::netd::netns::netns_exists(allocation_id) {
                continue;
            }
            let created = tokio::task::spawn_blocking(move || {
                crate::worker::netd::netns::create(allocation_id)
            })
            .await;
            match created {
                Ok(Ok(path)) => {
                    info!(allocation_id, path = %path.display(), "Created gated network namespace")
                }
                Ok(Err(err)) => {
                    warn!(allocation_id, error = ?err, "Failed to create gated network namespace; the task will not start until it exists")
                }
                Err(err) => warn!(allocation_id, error = ?err, "Namespace creation task panicked"),
            }
        }
        for allocation_id in previously.iter().copied() {
            if wanted.contains(&allocation_id) {
                continue;
            }
            let destroyed = tokio::task::spawn_blocking(move || {
                crate::worker::netd::netns::destroy(allocation_id)
            })
            .await;
            match destroyed {
                Ok(Ok(())) => debug!(allocation_id, "Removed gated network namespace"),
                Ok(Err(err)) => {
                    warn!(allocation_id, error = ?err, "Failed to remove gated network namespace")
                }
                Err(err) => warn!(allocation_id, error = ?err, "Namespace removal task panicked"),
            }
            let _ = crate::worker::netd::netns::set_gate(allocation_id, false);
        }
        *self.namespaces.lock().unwrap_or_else(|p| p.into_inner()) = wanted;
    }

    /// Keep `certFrom = { kv }` ingress certificates current
    /// (docs/network-handlers.md, "Certificates from KV"). Fail-static: a bad
    /// bundle keeps the certificate in hand and only records the problem.
    fn apply_ingress_certificates(&self, certificates: Vec<IngressCertificate>) {
        let context = self.forwarder.context();
        for certificate in certificates {
            let allocation_id = certificate.allocation_id;
            if !certificate.error.is_empty() {
                context.set_kv_cert_error(allocation_id, certificate.error);
                continue;
            }
            if certificate.unchanged {
                context.clear_kv_cert_error(allocation_id);
                continue;
            }
            match crate::worker::netd::tls::KvCertificate::from_bundle(
                &certificate.kv_path,
                certificate.version,
                &certificate.bundle_pem,
            ) {
                Ok(cert) => {
                    context.set_kv_cert(allocation_id, cert);
                    info!(
                        allocation_id,
                        kv_path = %certificate.kv_path,
                        version = certificate.version,
                        "Installed ingress certificate from KV"
                    );
                }
                Err(err) => context.set_kv_cert_error(
                    allocation_id,
                    format!("kv {} {err:#}", certificate.kv_path),
                ),
            }
        }
    }

    /// Keep `connect.upstreams[].credentials` current
    /// (docs/network-handlers.md, "Credential injection"). Values live in
    /// memory only — never in `netd_state.json`, never in a log line. An
    /// `absent` answer (the key was deleted, or the declaration is gone)
    /// drops the value, so requests on that upstream fail closed with a
    /// `503`; any other error keeps the value in hand.
    fn apply_upstream_credentials(&self, values: Vec<UpstreamCredentialValue>) {
        let context = self.forwarder.context();
        for value in values {
            let header = value.header.trim().to_ascii_lowercase();
            if !value.error.is_empty() {
                if value.absent {
                    warn!(
                        allocation_id = value.allocation_id,
                        upstream = %value.upstream,
                        header = %header,
                        source = %value.source,
                        error = %value.error,
                        "Upstream credential is gone; requests on this upstream fail closed"
                    );
                }
                context.set_credential_error(
                    value.allocation_id,
                    &value.upstream,
                    &header,
                    &value.source,
                    value.error,
                    value.absent,
                );
                continue;
            }
            if value.unchanged {
                context.clear_credential_error(value.allocation_id, &value.upstream, &header);
                continue;
            }
            context.set_credential(
                value.allocation_id,
                &value.upstream,
                &header,
                &value.source,
                value.version,
                SecretValue::new(value.value),
            );
            // The source and version only: never the value.
            info!(
                allocation_id = value.allocation_id,
                upstream = %value.upstream,
                header = %header,
                source = %value.source,
                version = value.version,
                "Installed upstream credential"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::ServiceEndpoint;
    use crate::worker::forwarder::{ForwarderContext, UpstreamSpec};
    use crate::worker::netd::attach::Attachment;
    use crate::worker::netd::{Denials, NetPolicyCache};
    use tempfile::TempDir;

    fn netd(root: &std::path::Path) -> Netd {
        let context = Arc::new(ForwarderContext::new(
            Arc::new(NetPolicyCache::new()),
            Arc::new(Denials::new()),
        ));
        Netd::new(
            Arc::new(UpstreamForwarder::with_context(context)),
            root.to_path_buf(),
        )
    }

    async fn free_port() -> u16 {
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        socket.local_addr().unwrap().port()
    }

    fn connect_allocation(allocation_id: i64, service: &str, local_port: u16) -> TaskAllocation {
        TaskAllocation {
            allocation_id,
            job_id: "web".into(),
            task_name: "server".into(),
            task_runtime: "exec".into(),
            task_connect: json!({ "upstreams": [{ "service": service }] }).to_string(),
            network: json!({
                "upstreams": [{ "service": service, "localPort": local_port, "balance": "least_conn" }]
            })
            .to_string(),
            ..Default::default()
        }
    }

    fn feed(allocation_id: i64, service: &str, mesh_tls: bool) -> UpstreamEndpoints {
        UpstreamEndpoints {
            allocation_id,
            service: service.into(),
            endpoints: vec![ServiceEndpoint {
                address: "10.0.0.5".into(),
                port: 20001,
                allocation_id: 42,
                node_id: "node-b".into(),
                mesh_tls,
                ..Default::default()
            }],
            for_handler: false,
        }
    }

    /// The datapath is driven entirely by desired sets: one must both start
    /// the listener and fill in where it may send traffic, and the next one
    /// without the allocation takes its listener away.
    #[tokio::test]
    async fn a_desired_set_starts_and_feeds_upstream_listeners_test() -> R<()> {
        let root = TempDir::new()?;
        let netd = netd(root.path());
        let local_port = free_port().await;
        netd.send(NetdInput::Desired(DesiredNetwork {
            desired: vec![connect_allocation(11, "db", local_port)],
            upstream_endpoints: vec![feed(11, "db", false)],
            net_policies: Vec::new(),
        }))
        .await;
        assert_eq!(
            netd.forwarder.active(),
            vec![UpstreamSpec {
                allocation_id: 11,
                service: "db".into(),
                local_port,
                balance: "least_conn".into(),
                to: None,
                http: None,
                handlers: Vec::new(),
                credentials: Vec::new(),
                vip: None,
                canonical_port: None,
                attachment: Attachment::Host,
            }]
        );
        // The listener is live on the loopback port the task will dial.
        assert!(
            tokio::net::TcpListener::bind(("127.0.0.1", local_port))
                .await
                .is_err(),
            "the datapath must hold the upstream's loopback port"
        );
        // A plain endpoint wants no mesh certificate.
        assert!(!netd.forwarder.context().mesh_wanted());

        netd.send(NetdInput::Desired(DesiredNetwork::default()))
            .await;
        assert!(netd.forwarder.active().is_empty());
        assert!(netd.statuses().is_empty());
        Ok(())
    }

    /// A mesh endpoint on the feed keeps its flag all the way to the
    /// listener, and makes the node want a mesh certificate: without one it
    /// would be dialed in plaintext and refused at the handshake.
    #[tokio::test]
    async fn a_mesh_endpoint_on_the_feed_wants_the_mesh_test() -> R<()> {
        let root = TempDir::new()?;
        let netd = netd(root.path());
        netd.send(NetdInput::Desired(DesiredNetwork {
            desired: vec![connect_allocation(12, "db", free_port().await)],
            upstream_endpoints: vec![feed(12, "db", true)],
            net_policies: Vec::new(),
        }))
        .await;
        assert!(netd.forwarder.context().mesh_wanted());
        Ok(())
    }

    /// What the worker must fetch follows the desired set: a policy hash
    /// with no body, and each declared credential header with the version
    /// held. Nothing is wanted for a set that needs nothing.
    #[tokio::test]
    async fn wants_follow_the_desired_set_test() -> R<()> {
        let root = TempDir::new()?;
        let netd = netd(root.path());
        assert_eq!(netd.wants(), NetdWants::default());
        let mut allocation = connect_allocation(21, "openai", free_port().await);
        allocation.task_connect = json!({ "upstreams": [
            { "name": "openai", "to": "https://api.openai.com", "credentials": [
                { "header": "Authorization", "from": { "kv": "secrets/openai" } },
            ] },
        ] })
        .to_string();
        netd.send(NetdInput::Desired(DesiredNetwork {
            desired: vec![allocation],
            upstream_endpoints: Vec::new(),
            net_policies: vec![AllocNetPolicy {
                allocation_id: 21,
                policy_hash: "abc".into(),
            }],
        }))
        .await;
        let wants = netd.wants();
        assert_eq!(wants.policy_bodies, vec!["abc".to_string()]);
        assert!(wants.ingress_certificates.is_empty());
        assert_eq!(
            wants.upstream_credentials,
            vec![UpstreamCredentialKnown {
                allocation_id: 21,
                upstream: "openai".into(),
                header: "authorization".into(),
                version: 0,
            }]
        );
        // The body arrives: nothing left to fetch for policy.
        let policy = EffectivePolicy {
            hash: "abc".into(),
            ..Default::default()
        };
        netd.send(NetdInput::PolicyBody {
            hash: "abc".into(),
            policy: Box::new(policy),
        })
        .await;
        assert!(netd.wants().policy_bodies.is_empty());
        Ok(())
    }

    /// A failed heartbeat hands the report back, and the next one carries
    /// the same denials.
    #[tokio::test]
    async fn a_redriven_report_is_reported_again_test() {
        let root = TempDir::new().unwrap();
        let netd = netd(root.path());
        netd.forwarder.context().record_denial(
            7,
            "egress",
            "tcp",
            "pypi.org:443",
            "gated-default",
            None,
            "",
            "",
        );
        let report = netd.report();
        assert_eq!(report.denials.len(), 1);
        assert!(netd.report().denials.is_empty(), "a report drains");
        netd.redrive(report);
        assert_eq!(netd.report().denials.len(), 1);
    }

    /// A restarted worker re-binds the datapath for the allocations it
    /// re-adopted, from disk, before any heartbeat; an allocation that did
    /// not survive gets nothing.
    #[tokio::test]
    async fn restore_rebinds_listeners_for_surviving_allocations_test() -> R<()> {
        let root = TempDir::new()?;
        let state_path = root.path().join("worker_state.json");
        let local_port = free_port().await;
        let store = NetdStateStore::beside(&state_path);
        store.save(&NetdState {
            network: DesiredNetwork {
                desired: vec![
                    connect_allocation(31, "db", local_port),
                    // Not running here any more: its datapath is not
                    // restored.
                    connect_allocation(32, "db", local_port + 1),
                ],
                upstream_endpoints: vec![feed(31, "db", false)],
                net_policies: Vec::new(),
            },
            policy_bodies: Vec::new(),
        })?;

        let netd = netd(root.path()).with_state_store(NetdStateStore::beside(&state_path));
        let restored = netd.restore(&[31].into_iter().collect()).await;
        assert_eq!(restored, 1);
        let active: Vec<(i64, String)> = netd
            .forwarder
            .active()
            .into_iter()
            .map(|spec| (spec.allocation_id, spec.service))
            .collect();
        assert_eq!(active, vec![(31, "db".to_string())]);
        assert_eq!(
            netd.last
                .lock()
                .unwrap()
                .desired
                .iter()
                .map(|a| a.allocation_id)
                .collect::<Vec<_>>(),
            vec![31]
        );
        // The listener is really bound, with the persisted endpoint feed.
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", local_port))
            .await
            .is_ok());
        Ok(())
    }
}
