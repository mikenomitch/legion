//! Versioned endpoint sets per service (mesh-control-path plan §6.1, review
//! NET1; docs/service-discovery.md, "How endpoints reach a node").
//!
//! **Soft state over the catalog.** The catalog is derived, never
//! registered: a service's endpoints are its task configs' allocations,
//! their health, and their nodes' liveness and addresses
//! ([`crate::storage::store::Store::service_endpoints`]). This index caches
//! that derivation once per service on each manager, so a heartbeat costs a
//! hash lookup per set instead of a catalog query per (service, consumer).
//!
//! - **Per service**: the catalog rows, sorted by allocation, a content
//!   digest, and a **version** drawn from one process-wide counter. The
//!   version moves only when the content does.
//! - **Per consumer**: the intention-filtered, healthy-only wire list and
//!   its digest. Every consumer that no `service.allow` restricts shares one
//!   list.
//! - **Invalidation from committed state**: the store's changeset
//!   projection, which runs on every replica, marks a service dirty when an
//!   allocation of it, that allocation's health, its node's liveness or
//!   address, or its task config changes ([`EndpointIndex::mark_allocations`]
//!   and friends). A dirty service is re-read the next time anything asks,
//!   and at once by the refresher when a node consumes it. Every set is also
//!   re-read when older than [`MAX_SET_AGE`], a backstop for an invalidation
//!   the projection could not name.
//! - **Interest**: each node registers the sets its listeners bind
//!   ([`EndpointIndex::register`]). When a service's version moves, the
//!   index queues the changed sets for every node that consumes it and wakes
//!   that node's work stream, which pushes them.
//!
//! The marks are taken against a dirty counter, not a flag: a re-read that
//! races a commit keeps the entry stale, so the next caller reads again.

use crate::prelude::*;
pub use crate::structs::worker_protocol::StableHash;
use crate::structs::{ServiceEndpoint, ServiceEndpointRow};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A set older than this is re-read even when nothing marked it dirty.
pub const MAX_SET_AGE: Duration = Duration::from_secs(30);

/// A service no node consumes and nobody asked for in this long is
/// forgotten, so the projection stops tracking it.
pub const IDLE_EVICT_AFTER: Duration = Duration::from_secs(300);

/// A node that has not registered its interest in this long (it moved to
/// another manager, or left) is forgotten.
pub const INTEREST_TTL: Duration = Duration::from_secs(300);

/// Task configs remembered by id. Cleared wholesale when full; each entry
/// is one indexed read to rebuild.
const TASK_CONFIG_MEMO_MAX: usize = 65_536;

/// The key of one endpoint set: a canonical service reference and the
/// consumer intentions are checked against.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SetKey {
    pub service: String,
    pub consumer: String,
}

impl SetKey {
    pub fn new(service: impl Into<String>, consumer: impl Into<String>) -> Self {
        SetKey {
            service: service.into(),
            consumer: consumer.into(),
        }
    }
}

/// One consumer's view of one service, as the wire carries it.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerView {
    /// The service's version on this manager.
    pub version: u64,
    /// Digest of `endpoints`.
    pub digest: u64,
    pub endpoints: Arc<Vec<ServiceEndpoint>>,
}

#[derive(Debug)]
struct Loaded {
    version: u64,
    /// Digest of every row, healthy or not: what decides a version bump.
    content: u64,
    at: Instant,
    rows: Arc<Vec<ServiceEndpointRow>>,
    /// The view of a consumer every row permits, shared.
    open: Option<Arc<ConsumerView>>,
    /// Every consumer's view, memoized (open consumers hold `open`).
    views: HashMap<String, Arc<ConsumerView>>,
}

#[derive(Debug)]
struct ServiceEntry {
    /// Bumped by every invalidation.
    dirty: u64,
    /// `dirty` as it stood when the held rows were read.
    loaded_dirty: u64,
    loaded: Option<Loaded>,
    last_used: Instant,
}

impl ServiceEntry {
    fn new(now: Instant) -> Self {
        ServiceEntry {
            dirty: 1,
            loaded_dirty: 0,
            loaded: None,
            last_used: now,
        }
    }

    fn fresh(&self, now: Instant) -> bool {
        self.loaded_dirty == self.dirty
            && self
                .loaded
                .as_ref()
                .is_some_and(|loaded| now.duration_since(loaded.at) < MAX_SET_AGE)
    }
}

#[derive(Debug)]
struct NodeInterest {
    keys: BTreeSet<SetKey>,
    pending: BTreeSet<SetKey>,
    wake: Arc<tokio::sync::Notify>,
    seen: Instant,
}

impl NodeInterest {
    fn new(now: Instant) -> Self {
        NodeInterest {
            keys: BTreeSet::new(),
            pending: BTreeSet::new(),
            wake: Arc::new(tokio::sync::Notify::new()),
            seen: now,
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    services: HashMap<String, ServiceEntry>,
    /// Allocation → service, for every allocation in a loaded service.
    by_allocation: HashMap<i64, String>,
    /// Node → services with an allocation on it, for loaded services.
    by_node: HashMap<String, HashSet<String>>,
    /// Task config id → the service it declares (`None`: none).
    task_configs: HashMap<i64, Option<String>>,
    nodes: HashMap<String, NodeInterest>,
    /// Service → nodes that consume it.
    subscribers: HashMap<String, HashSet<String>>,
}

/// Every service's endpoint set on this manager. See the module docs.
#[derive(Debug)]
pub struct EndpointIndex {
    epoch: u64,
    next_version: AtomicU64,
    /// Any service is loaded: the projection does nothing while false.
    active: AtomicBool,
    inner: Mutex<Inner>,
    /// Woken when a service some node consumes went dirty.
    refresh: Arc<tokio::sync::Notify>,
}

impl Default for EndpointIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl EndpointIndex {
    pub fn new() -> Self {
        let mut epoch = rand::random::<u64>();
        if epoch == 0 {
            epoch = 1;
        }
        EndpointIndex {
            epoch,
            next_version: AtomicU64::new(0),
            active: AtomicBool::new(false),
            inner: Mutex::new(Inner::default()),
            refresh: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// This manager process's endpoint epoch (`EndpointFeed.epoch`).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether any service is loaded. The projection's endpoint pass costs
    /// nothing while this is false (no node consumes a service).
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Woken when a consumed service needs a re-read: the refresher waits on
    /// it.
    pub fn refresh_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.refresh)
    }

    // ----- invalidation ---------------------------------------------------

    fn mark(inner: &mut Inner, service: &str) -> bool {
        match inner.services.get_mut(service) {
            Some(entry) => {
                entry.dirty += 1;
                inner.subscribers.contains_key(service)
            }
            None => false,
        }
    }

    fn after_marks(&self, consumed: bool) {
        if consumed {
            self.refresh.notify_one();
        }
    }

    /// Allocations whose row or health changed.
    pub fn mark_allocations(&self, ids: impl IntoIterator<Item = i64>) {
        if !self.is_active() {
            return;
        }
        let mut inner = self.lock();
        let mut consumed = false;
        for id in ids {
            if let Some(service) = inner.by_allocation.get(&id).cloned() {
                consumed |= Self::mark(&mut inner, &service);
            }
        }
        drop(inner);
        self.after_marks(consumed);
    }

    /// Nodes whose liveness or advertised address changed.
    pub fn mark_nodes<'a>(&self, nodes: impl IntoIterator<Item = &'a str>) {
        if !self.is_active() {
            return;
        }
        let mut inner = self.lock();
        let mut consumed = false;
        for node in nodes {
            let services: Vec<String> = inner
                .by_node
                .get(node)
                .map(|services| services.iter().cloned().collect())
                .unwrap_or_default();
            for service in services {
                consumed |= Self::mark(&mut inner, &service);
            }
        }
        drop(inner);
        self.after_marks(consumed);
    }

    /// One service by name (a new allocation of it was placed).
    pub fn mark_service(&self, service: &str) {
        if !self.is_active() {
            return;
        }
        let consumed = Self::mark(&mut self.lock(), service);
        self.after_marks(consumed);
    }

    /// Every loaded service: the projection could not name what changed, a
    /// task config was rewritten, or the read model was rebuilt.
    pub fn mark_all(&self) {
        let mut inner = self.lock();
        let consumed = !inner.subscribers.is_empty();
        for entry in inner.services.values_mut() {
            entry.dirty += 1;
        }
        inner.task_configs.clear();
        drop(inner);
        self.after_marks(consumed);
    }

    /// The service a task config declares, when remembered: `Some(None)` for
    /// a task config with no service.
    pub fn task_config_service(&self, task_config_id: i64) -> Option<Option<String>> {
        self.lock().task_configs.get(&task_config_id).cloned()
    }

    pub fn remember_task_config(&self, task_config_id: i64, service: Option<String>) {
        let mut inner = self.lock();
        if inner.task_configs.len() >= TASK_CONFIG_MEMO_MAX {
            inner.task_configs.clear();
        }
        inner.task_configs.insert(task_config_id, service);
    }

    // ----- reading --------------------------------------------------------

    /// One consumer's view of `key.service`, re-read through `load` when the
    /// held rows are dirty or old. `load` returns the service's catalog rows
    /// (`Store::service_endpoints(Some(service))`). An error leaves the held
    /// rows in place and is returned, so a caller can keep what it sent last
    /// (fail-static).
    pub fn view(
        &self,
        key: &SetKey,
        load: impl FnOnce(&str) -> R<Vec<ServiceEndpointRow>>,
    ) -> R<Arc<ConsumerView>> {
        let now = Instant::now();
        let target = {
            let mut inner = self.lock();
            let entry = inner
                .services
                .entry(key.service.clone())
                .or_insert_with(|| ServiceEntry::new(now));
            entry.last_used = now;
            if entry.fresh(now) {
                let loaded = entry.loaded.as_mut().expect("fresh means loaded");
                return Ok(Self::consumer_view(loaded, &key.consumer));
            }
            entry.dirty
        };
        self.active.store(true, Ordering::Release);
        let rows = load(&key.service)?;
        let mut inner = self.lock();
        self.install(&mut inner, &key.service, target, rows, now);
        let loaded = inner
            .services
            .get_mut(&key.service)
            .and_then(|entry| entry.loaded.as_mut())
            .expect("just installed");
        Ok(Self::consumer_view(loaded, &key.consumer))
    }

    /// Re-read every consumed service that is dirty or old, through `load`.
    /// Returns how many were re-read. The refresher calls this, so a change
    /// is pushed without waiting for any heartbeat.
    pub fn refresh(&self, mut load: impl FnMut(&str) -> R<Vec<ServiceEndpointRow>>) -> usize {
        let now = Instant::now();
        let stale: Vec<(String, u64)> = {
            let inner = self.lock();
            inner
                .subscribers
                .keys()
                .filter_map(|service| {
                    let entry = inner.services.get(service)?;
                    (!entry.fresh(now)).then(|| (service.clone(), entry.dirty))
                })
                .collect()
        };
        let mut read = 0;
        for (service, target) in stale {
            match load(&service) {
                Ok(rows) => {
                    let mut inner = self.lock();
                    if inner.services.contains_key(&service) {
                        self.install(&mut inner, &service, target, rows, now);
                    }
                    read += 1;
                }
                Err(err) => debug!(
                    service = %service,
                    error = ?err,
                    "Could not re-read a service's endpoints; keeping the last set"
                ),
            }
        }
        read
    }

    /// Store re-read rows for `service`, read when its dirty counter was
    /// `target`. Bumps the version and queues pushes only when the content
    /// changed.
    fn install(
        &self,
        inner: &mut Inner,
        service: &str,
        target: u64,
        mut rows: Vec<ServiceEndpointRow>,
        now: Instant,
    ) {
        rows.sort_by_key(|row| row.allocation_id);
        let content = rows_digest(&rows);
        let Some(entry) = inner.services.get_mut(service) else {
            return;
        };
        // Never move backwards: a concurrent reader may have installed rows
        // read after a later mark.
        entry.loaded_dirty = entry.loaded_dirty.max(target);
        let previous_rows = match entry.loaded.as_mut() {
            Some(loaded) if loaded.content == content => {
                loaded.at = now;
                return;
            }
            Some(loaded) => Some(Arc::clone(&loaded.rows)),
            None => None,
        };
        let version = self.next_version.fetch_add(1, Ordering::Relaxed) + 1;
        let rows = Arc::new(rows);
        entry.loaded = Some(Loaded {
            version,
            content,
            at: now,
            rows: Arc::clone(&rows),
            open: None,
            views: HashMap::new(),
        });
        // Reverse maps: what the projection uses to find this service.
        let replaced = previous_rows.is_some();
        if let Some(previous) = previous_rows {
            for row in previous.iter() {
                if inner
                    .by_allocation
                    .get(&row.allocation_id)
                    .map(String::as_str)
                    == Some(service)
                {
                    inner.by_allocation.remove(&row.allocation_id);
                }
                if let Some(services) = inner.by_node.get_mut(&row.node_id) {
                    services.remove(service);
                    if services.is_empty() {
                        inner.by_node.remove(&row.node_id);
                    }
                }
            }
        }
        for row in rows.iter() {
            inner
                .by_allocation
                .insert(row.allocation_id, service.to_string());
            inner
                .by_node
                .entry(row.node_id.clone())
                .or_default()
                .insert(service.to_string());
        }
        // Queue the changed sets for every node that consumes the service.
        // A first read changes nothing a node holds: the node that asked
        // gets the set in its reply.
        if let Some(nodes) = inner.subscribers.get(service).filter(|_| replaced).cloned() {
            for node in nodes {
                if let Some(interest) = inner.nodes.get_mut(&node) {
                    let keys: Vec<SetKey> = interest
                        .keys
                        .iter()
                        .filter(|key| key.service == service)
                        .cloned()
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    interest.pending.extend(keys);
                    interest.wake.notify_one();
                }
            }
        }
    }

    /// The view of `consumer`, memoized per consumer. A consumer every row
    /// permits sees every healthy row, so all such consumers share one list.
    fn consumer_view(loaded: &mut Loaded, consumer: &str) -> Arc<ConsumerView> {
        if let Some(view) = loaded.views.get(consumer) {
            return Arc::clone(view);
        }
        let open = loaded
            .rows
            .iter()
            .all(|row| crate::structs::allow_permits(&row.allow, &row.job_id, consumer));
        if let (true, Some(view)) = (open, loaded.open.as_ref()) {
            let view = Arc::clone(view);
            loaded.views.insert(consumer.to_string(), Arc::clone(&view));
            return view;
        }
        let endpoints: Vec<ServiceEndpoint> = loaded
            .rows
            .iter()
            .filter(|row| {
                row.healthy && crate::structs::allow_permits(&row.allow, &row.job_id, consumer)
            })
            .map(wire_endpoint)
            .collect();
        let view = Arc::new(ConsumerView {
            version: loaded.version,
            digest: endpoints_digest(&endpoints),
            endpoints: Arc::new(endpoints),
        });
        if open {
            loaded.open = Some(Arc::clone(&view));
        }
        loaded.views.insert(consumer.to_string(), Arc::clone(&view));
        view
    }

    // ----- interest -------------------------------------------------------

    /// Record the sets `node`'s listeners bind, so a change to one of them
    /// is pushed to the node. Cheap when nothing changed.
    pub fn register(&self, node: &str, keys: &BTreeSet<SetKey>) {
        let now = Instant::now();
        let mut inner = self.lock();
        if keys.is_empty() && !inner.nodes.contains_key(node) {
            // A node with no upstreams costs nothing here.
            return;
        }
        let interest = inner
            .nodes
            .entry(node.to_string())
            .or_insert_with(|| NodeInterest::new(now));
        interest.seen = now;
        if interest.keys == *keys {
            return;
        }
        let before: HashSet<String> = interest.keys.iter().map(|k| k.service.clone()).collect();
        let after: HashSet<String> = keys.iter().map(|k| k.service.clone()).collect();
        interest.keys = keys.clone();
        interest.pending.retain(|key| keys.contains(key));
        for service in before.difference(&after) {
            if let Some(nodes) = inner.subscribers.get_mut(service) {
                nodes.remove(node);
                if nodes.is_empty() {
                    inner.subscribers.remove(service);
                }
            }
        }
        for service in after.difference(&before) {
            inner
                .subscribers
                .entry(service.clone())
                .or_default()
                .insert(node.to_string());
        }
    }

    /// The wake `node`'s work stream waits on for pushes.
    pub fn node_wake(&self, node: &str) -> Arc<tokio::sync::Notify> {
        let now = Instant::now();
        let mut inner = self.lock();
        Arc::clone(
            &inner
                .nodes
                .entry(node.to_string())
                .or_insert_with(|| NodeInterest::new(now))
                .wake,
        )
    }

    /// Whether `node` has sets queued to push.
    pub fn has_pending(&self, node: &str) -> bool {
        self.lock()
            .nodes
            .get(node)
            .is_some_and(|interest| !interest.pending.is_empty())
    }

    /// Take the sets queued for `node`.
    pub fn take_pending(&self, node: &str) -> Vec<SetKey> {
        self.lock()
            .nodes
            .get_mut(node)
            .map(|interest| std::mem::take(&mut interest.pending).into_iter().collect())
            .unwrap_or_default()
    }

    /// Forget nodes that stopped registering and services nobody uses.
    pub fn sweep(&self, now: Instant) {
        let mut inner = self.lock();
        let gone: Vec<String> = inner
            .nodes
            .iter()
            .filter(|(_, interest)| now.duration_since(interest.seen) >= INTEREST_TTL)
            .map(|(node, _)| node.clone())
            .collect();
        for node in gone {
            inner.nodes.remove(&node);
            for nodes in inner.subscribers.values_mut() {
                nodes.remove(&node);
            }
        }
        inner.subscribers.retain(|_, nodes| !nodes.is_empty());
        let idle: Vec<String> = inner
            .services
            .iter()
            .filter(|(service, entry)| {
                !inner.subscribers.contains_key(*service)
                    && now.duration_since(entry.last_used) >= IDLE_EVICT_AFTER
            })
            .map(|(service, _)| service.clone())
            .collect();
        for service in idle {
            inner.services.remove(&service);
            inner.by_allocation.retain(|_, owner| *owner != service);
            for services in inner.by_node.values_mut() {
                services.remove(&service);
            }
        }
        inner.by_node.retain(|_, services| !services.is_empty());
        self.active
            .store(!inner.services.is_empty(), Ordering::Release);
    }

    /// How many services are loaded (tests and diagnostics).
    pub fn loaded_services(&self) -> usize {
        self.lock()
            .services
            .values()
            .filter(|entry| entry.loaded.is_some())
            .count()
    }
}

/// A catalog row as the wire carries it.
pub fn wire_endpoint(row: &ServiceEndpointRow) -> ServiceEndpoint {
    ServiceEndpoint {
        namespace: crate::structs::namespace::job_namespace(&row.job_id).to_string(),
        service: row.service.clone(),
        job_id: row.job_id.clone(),
        task_name: row.task_name.clone(),
        allocation_id: row.allocation_id,
        node_id: row.node_id.clone(),
        address: row.address.clone().unwrap_or_default(),
        public_address: row.public_address.clone().unwrap_or_default(),
        port: u32::from(row.port.unwrap_or(0)),
        healthy: row.healthy,
        health_status: row.health_status.clone(),
        tags: row.tags.clone(),
        meta: row
            .meta
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        port_label: row.port_label.clone().unwrap_or_default(),
        mesh_tls: row.mesh_tls,
    }
}

fn rows_digest(rows: &[ServiceEndpointRow]) -> u64 {
    let mut hash = StableHash::default();
    hash.u64(rows.len() as u64);
    for row in rows {
        hash.str(&row.service)
            .str(&row.job_id)
            .str(&row.task_name)
            .i64(row.allocation_id)
            .str(&row.node_id)
            .str(row.address.as_deref().unwrap_or(""))
            .str(row.public_address.as_deref().unwrap_or(""))
            .u64(u64::from(row.port.unwrap_or(0)))
            .bool(row.healthy)
            .str(&row.health_status)
            .str(row.port_label.as_deref().unwrap_or(""))
            .bool(row.mesh_tls);
        hash.u64(row.tags.len() as u64);
        for tag in &row.tags {
            hash.str(tag);
        }
        hash.u64(row.allow.len() as u64);
        for entry in &row.allow {
            hash.str(entry);
        }
        hash.u64(row.meta.len() as u64);
        for (key, value) in &row.meta {
            hash.str(key).str(value);
        }
    }
    hash.finish()
}

/// The digest of a wire endpoint list (`EndpointSet.digest`). Meta is
/// hashed in key order, so the map's iteration order does not matter.
pub fn endpoints_digest(endpoints: &[ServiceEndpoint]) -> u64 {
    let mut hash = StableHash::default();
    hash.u64(endpoints.len() as u64);
    for endpoint in endpoints {
        hash.str(&endpoint.namespace)
            .str(&endpoint.service)
            .str(&endpoint.job_id)
            .str(&endpoint.task_name)
            .i64(endpoint.allocation_id)
            .str(&endpoint.node_id)
            .str(&endpoint.address)
            .str(&endpoint.public_address)
            .u64(u64::from(endpoint.port))
            .bool(endpoint.healthy)
            .str(&endpoint.health_status)
            .str(&endpoint.port_label)
            .bool(endpoint.mesh_tls);
        hash.u64(endpoint.tags.len() as u64);
        for tag in &endpoint.tags {
            hash.str(tag);
        }
        let mut meta: Vec<(&String, &String)> = endpoint.meta.iter().collect();
        meta.sort();
        hash.u64(meta.len() as u64);
        for (key, value) in meta {
            hash.str(key).str(value);
        }
    }
    hash.finish()
}

#[cfg(test)]
mod endpoint_index_test {
    use super::*;
    use std::cell::Cell;

    fn row(allocation_id: i64, node: &str, healthy: bool) -> ServiceEndpointRow {
        ServiceEndpointRow {
            service: "db".into(),
            job_id: "db".into(),
            task_name: "server".into(),
            allocation_id,
            node_id: node.into(),
            address: Some("10.0.0.1".into()),
            public_address: None,
            port: Some(5432),
            healthy,
            health_status: if healthy { "healthy" } else { "failing" }.into(),
            tags: Vec::new(),
            allow: Vec::new(),
            meta: Default::default(),
            port_label: None,
            mesh_tls: false,
        }
    }

    fn key() -> SetKey {
        SetKey::new("db", "web")
    }

    #[test]
    fn a_service_is_read_once_until_something_marks_it() -> R<()> {
        let index = EndpointIndex::new();
        let reads = Cell::new(0);
        let load = |_: &str| {
            reads.set(reads.get() + 1);
            Ok(vec![row(2, "n1", true), row(1, "n2", false)])
        };
        let first = index.view(&key(), load)?;
        assert_eq!(first.endpoints.len(), 1, "healthy only");
        let again = index.view(&key(), |_| -> R<Vec<ServiceEndpointRow>> {
            panic!("a fresh set is not re-read")
        })?;
        assert_eq!(first, again);
        assert_eq!(reads.get(), 1);
        // An unrelated allocation changes nothing.
        index.mark_allocations([99]);
        index.view(&key(), |_| -> R<Vec<ServiceEndpointRow>> {
            panic!("an unrelated mark does not dirty the set")
        })?;
        // Allocation 1 is the service's: re-read.
        index.mark_allocations([1]);
        let after = index.view(&key(), |_| Ok(vec![row(2, "n1", true), row(1, "n2", true)]))?;
        assert_eq!(after.endpoints.len(), 2);
        assert!(
            after.version > first.version,
            "content changed: new version"
        );
        assert_ne!(after.digest, first.digest);
        Ok(())
    }

    #[test]
    fn an_unchanged_reread_keeps_the_version() -> R<()> {
        let index = EndpointIndex::new();
        let first = index.view(&key(), |_| Ok(vec![row(1, "n1", true)]))?;
        index.mark_nodes(["n1"]);
        let second = index.view(&key(), |_| Ok(vec![row(1, "n1", true)]))?;
        assert_eq!(first.version, second.version);
        assert_eq!(first.digest, second.digest);
        Ok(())
    }

    #[test]
    fn a_mark_that_races_a_reread_keeps_the_set_dirty() -> R<()> {
        let index = EndpointIndex::new();
        index.view(&key(), |_| Ok(vec![row(1, "n1", true)]))?;
        index.mark_allocations([1]);
        // The re-read is under way when another commit marks the service.
        let view = index.view(&key(), |_| {
            index.mark_allocations([1]);
            Ok(vec![row(1, "n1", false)])
        })?;
        assert!(view.endpoints.is_empty());
        let reread = Cell::new(false);
        index.view(&key(), |_| {
            reread.set(true);
            Ok(vec![row(1, "n1", false)])
        })?;
        assert!(reread.get(), "the mark during the read must not be lost");
        Ok(())
    }

    #[test]
    fn intentions_filter_per_consumer_and_open_consumers_share_one_list() -> R<()> {
        let index = EndpointIndex::new();
        let mut restricted = row(2, "n1", true);
        restricted.allow = vec!["billing".into()];
        let load = || Ok(vec![row(1, "n1", true), restricted.clone()]);
        let web = index.view(&SetKey::new("db", "web"), |_| load())?;
        let billing = index.view(&SetKey::new("db", "billing"), |_| load())?;
        assert_eq!(web.endpoints.len(), 1);
        assert_eq!(billing.endpoints.len(), 2);
        assert_eq!(web.version, billing.version, "one version per service");

        let open_index = EndpointIndex::new();
        let a = open_index.view(&SetKey::new("db", "a"), |_| Ok(vec![row(1, "n1", true)]))?;
        let b = open_index.view(&SetKey::new("db", "b"), |_| Ok(vec![row(1, "n1", true)]))?;
        assert!(
            Arc::ptr_eq(&a.endpoints, &b.endpoints),
            "shared, not copied"
        );
        Ok(())
    }

    #[test]
    fn a_version_change_queues_the_set_for_consuming_nodes_only() -> R<()> {
        let index = EndpointIndex::new();
        let db = SetKey::new("db", "web");
        let cache = SetKey::new("cache", "web");
        index.view(&db, |_| Ok(vec![row(1, "n9", true)]))?;
        index.view(&cache, |_| Ok(vec![]))?;
        index.register("n1", &[db.clone()].into_iter().collect());
        index.register("n2", &[cache.clone()].into_iter().collect());
        let wake = index.node_wake("n1");
        index.mark_allocations([1]);
        // The refresher re-reads consumed, dirty services.
        let read = index.refresh(|service| {
            assert_eq!(service, "db", "only the dirty, consumed service");
            Ok(vec![row(1, "n9", false)])
        });
        assert_eq!(read, 1);
        assert!(index.has_pending("n1"));
        assert!(!index.has_pending("n2"), "n2 does not consume db");
        assert_eq!(index.take_pending("n1"), vec![db]);
        assert!(!index.has_pending("n1"));
        // The wake holds a permit for the stream.
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(1), wake.notified())
                    .await
                    .expect("n1's stream was woken");
            });
        Ok(())
    }

    #[test]
    fn a_node_that_stops_consuming_is_not_pushed_and_idle_services_are_evicted() -> R<()> {
        let index = EndpointIndex::new();
        let db = SetKey::new("db", "web");
        index.view(&db, |_| Ok(vec![row(1, "n9", true)]))?;
        index.register("n1", &[db.clone()].into_iter().collect());
        index.register("n1", &BTreeSet::new());
        index.mark_allocations([1]);
        assert_eq!(index.refresh(|_| Ok(vec![])), 0, "nobody consumes db now");
        assert!(!index.has_pending("n1"));
        index.sweep(Instant::now() + IDLE_EVICT_AFTER + Duration::from_secs(1));
        assert_eq!(index.loaded_services(), 0);
        assert!(!index.is_active());
        Ok(())
    }

    #[test]
    fn digests_are_stable_and_ignore_meta_order() {
        let mut a = wire_endpoint(&row(1, "n1", true));
        a.meta.insert("protocol".into(), "http".into());
        a.meta.insert("zone".into(), "a".into());
        let mut b = wire_endpoint(&row(1, "n1", true));
        b.meta.insert("zone".into(), "a".into());
        b.meta.insert("protocol".into(), "http".into());
        assert_eq!(endpoints_digest(&[a.clone()]), endpoints_digest(&[b]));
        // Pinned, so a change to the digest is a deliberate wire change.
        assert_eq!(endpoints_digest(&[]), {
            let mut hash = StableHash::default();
            hash.u64(0);
            hash.finish()
        });
        a.port = 1;
        assert_ne!(
            endpoints_digest(&[a]),
            endpoints_digest(&[wire_endpoint(&row(1, "n1", true))])
        );
    }
}
