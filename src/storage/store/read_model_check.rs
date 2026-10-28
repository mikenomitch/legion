//! Read-model coherence checks: compare the in-memory
//! [`SchedulingReadModel`] with a fresh load from committed SQLite state
//! (docs/state-store.md, "Read model coherence").
//!
//! The read model is a cache. SQLite is the truth, and every write must
//! reach both. Two things here make a miss visible instead of silent:
//!
//! - [`Store::read_model_divergence`] lists every way the cache disagrees
//!   with a fresh load. The shadow check runs it after every local mutation
//!   in tests (and when `LEGION_READ_MODEL_SHADOW_CHECK=1` is set), so a
//!   write path that forgets the cache fails the suite.
//! - [`Store::check_external_writes`] notices commits made by another
//!   SQLite connection (an operator's `sqlite3`, a second process) through
//!   `PRAGMA data_version`, and rebuilds the read model from disk.

use super::*;

/// Node fields the read model deliberately lets lag: heartbeat-only
/// refreshes never reach it (the invariant on
/// `update_node_heartbeats_apply`), so comparing them would flag every
/// heartbeat.
const HEARTBEAT_ONLY_NODE_FIELDS: [&str; 2] = ["lastHeartbeat", "heartbeatSource"];

/// Env var that turns the shadow check on outside `cfg(test)` builds, for
/// integration tests and soak runs. Off by default: the check reloads the
/// whole read model after every mutation.
pub const READ_MODEL_SHADOW_CHECK_ENV: &str = "LEGION_READ_MODEL_SHADOW_CHECK";

/// Whether the shadow check runs: always in this crate's unit tests, and
/// wherever [`READ_MODEL_SHADOW_CHECK_ENV`] is `1`.
pub(crate) fn shadow_check_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        cfg!(test)
            || std::env::var(READ_MODEL_SHADOW_CHECK_ENV)
                .map(|value| value == "1")
                .unwrap_or(false)
    })
}

thread_local! {
    /// Set by tests that write SQLite behind the read model's back on
    /// purpose (to exercise drift repair), so the shadow check does not
    /// fail them for the drift they are creating.
    static SHADOW_CHECK_SUSPENDED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// RAII guard that suspends the shadow check on this thread.
#[cfg(test)]
pub(crate) struct ShadowCheckSuspended {
    prev: bool,
}

#[cfg(test)]
impl ShadowCheckSuspended {
    pub(crate) fn new() -> Self {
        let prev = SHADOW_CHECK_SUSPENDED.with(|flag| flag.replace(true));
        ShadowCheckSuspended { prev }
    }
}

#[cfg(test)]
impl Drop for ShadowCheckSuspended {
    fn drop(&mut self) {
        let prev = self.prev;
        SHADOW_CHECK_SUSPENDED.with(|flag| flag.set(prev));
    }
}

/// A node as the comparison sees it: every field except the heartbeat-only
/// ones.
fn comparable_node(node: &Node) -> R<Value> {
    let mut value = serde_json::to_value(node)?;
    if let Some(object) = value.as_object_mut() {
        for field in HEARTBEAT_ONLY_NODE_FIELDS {
            object.remove(field);
        }
    }
    Ok(value)
}

/// A pool's amounts in a stable order (`ResourcePool` holds a `HashMap`).
fn pool_amounts(pool: &ResourcePool) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = pool
        .values
        .keys()
        .map(|name| (name.clone(), format!("{:?}", pool.amount(name))))
        .collect();
    out.sort();
    out
}

/// Whether the `Arc` a scrub visit held before its read is still the one in
/// the model (see `Store::scrub_read_model_node`).
fn same_arc<T>(held: &Option<Arc<T>>, current: Option<&Arc<T>>) -> bool {
    match (held, current) {
        (Some(held), Some(current)) => Arc::ptr_eq(held, current),
        (None, None) => true,
        _ => false,
    }
}

fn sorted_allocations(allocations: Option<&Arc<Vec<Allocation>>>) -> Vec<Allocation> {
    let mut out: Vec<Allocation> = allocations
        .map(|list| list.as_ref().clone())
        .unwrap_or_default();
    out.sort_by_key(|allocation| allocation.id);
    out
}

fn sorted_tasks(tasks: Option<&Arc<Vec<(i64, TaskConfig)>>>) -> Vec<(i64, TaskConfig)> {
    let mut out = tasks.map(|t| t.to_vec()).unwrap_or_default();
    out.sort_by_key(|(id, _)| *id);
    out
}

impl Store {
    #[cfg(test)]
    fn cached_node_for_test(&self, id: &str) -> R<Node> {
        self.read_model_reader()?
            .nodes
            .get(id)
            .map(|node| node.as_ref().clone())
            .with_context(|| format!("node {id} is not cached"))
    }

    /// Every way the cached read model disagrees with a fresh load from
    /// committed state, one line per mismatch, or an empty list when they
    /// agree. Compares nodes (minus heartbeat timestamps), each node's
    /// allocation list, each job config's task list, the free pools, and
    /// the scope-epoch mirror.
    ///
    /// Only meaningful when no write is in flight: a mutation that has
    /// committed but not yet reached the cache shows up here as drift.
    pub(crate) fn read_model_divergence(&self) -> R<Vec<String>> {
        let fresh = self.load_read_model()?;
        let cached = self.read_model_reader()?.clone();
        let mut out = Vec::new();

        let node_ids: BTreeSet<&String> = cached.nodes.keys().chain(fresh.nodes.keys()).collect();
        for id in node_ids {
            match (cached.nodes.get(id), fresh.nodes.get(id)) {
                (Some(cached_node), Some(fresh_node)) => {
                    let (cached_value, fresh_value) =
                        (comparable_node(cached_node)?, comparable_node(fresh_node)?);
                    if cached_value != fresh_value {
                        out.push(format!(
                            "node {id}: cached {cached_value} != committed {fresh_value}"
                        ));
                    }
                }
                (Some(_), None) => out.push(format!("node {id}: cached but not committed")),
                (None, Some(_)) => out.push(format!("node {id}: committed but not cached")),
                (None, None) => {}
            }
        }

        // The available-node list is derived from the cached nodes; when it
        // is built it must match them exactly.
        if let Some(available) = cached.available_nodes_cache.as_ref() {
            let listed: Vec<&str> = available
                .as_slice()
                .iter()
                .map(|node| node.id.as_str())
                .collect();
            let mut expected: Vec<&str> = cached
                .nodes
                .values()
                .filter(|node| node.availability == "available")
                .map(|node| node.id.as_str())
                .collect();
            expected.sort_unstable();
            if listed != expected {
                out.push(format!(
                    "available-node list {listed:?} != available cached nodes {expected:?}"
                ));
            }
        }

        let alloc_nodes: BTreeSet<&String> = cached
            .allocations_by_node
            .keys()
            .chain(fresh.allocations_by_node.keys())
            .collect();
        for node_id in alloc_nodes {
            let cached_list = sorted_allocations(cached.allocations_by_node.get(node_id));
            let fresh_list = sorted_allocations(fresh.allocations_by_node.get(node_id));
            if cached_list != fresh_list {
                let ids = |list: &[Allocation]| -> Vec<i64> { list.iter().map(|a| a.id).collect() };
                out.push(format!(
                    "allocations on node {node_id}: cached ids {:?} != committed ids {:?}{}",
                    ids(&cached_list),
                    ids(&fresh_list),
                    if ids(&cached_list) == ids(&fresh_list) {
                        " (same ids, different row contents)"
                    } else {
                        ""
                    }
                ));
            }
        }

        let job_configs: BTreeSet<&String> = cached
            .task_configs
            .keys()
            .chain(fresh.task_configs.keys())
            .collect();
        for job_config_id in job_configs {
            let cached_tasks = sorted_tasks(cached.task_configs.get(job_config_id));
            let fresh_tasks = sorted_tasks(fresh.task_configs.get(job_config_id));
            if cached_tasks != fresh_tasks {
                let ids =
                    |list: &[(i64, TaskConfig)]| -> Vec<i64> { list.iter().map(|t| t.0).collect() };
                out.push(format!(
                    "task configs of {job_config_id}: cached ids {:?} != committed ids {:?}{}",
                    ids(&cached_tasks),
                    ids(&fresh_tasks),
                    if ids(&cached_tasks) == ids(&fresh_tasks) {
                        " (same ids, different contents)"
                    } else {
                        ""
                    }
                ));
            }
        }

        let pool_nodes: BTreeSet<&String> = cached
            .node_pools
            .keys()
            .chain(fresh.node_pools.keys())
            .collect();
        for node_id in pool_nodes {
            let cached_pool = cached.node_pools.get(node_id).map(|p| pool_amounts(p));
            let fresh_pool = fresh.node_pools.get(node_id).map(|p| pool_amounts(p));
            if cached_pool != fresh_pool {
                out.push(format!(
                    "free pool of node {node_id}: cached {cached_pool:?} != committed {fresh_pool:?}"
                ));
            }
        }

        let scopes: BTreeSet<&String> = cached
            .scope_epochs
            .keys()
            .chain(fresh.scope_epochs.keys())
            .collect();
        for scope in scopes {
            let cached_epoch = cached.scope_epochs.get(scope).copied().unwrap_or(0);
            let fresh_epoch = fresh.scope_epochs.get(scope).copied().unwrap_or(0);
            if cached_epoch != fresh_epoch {
                out.push(format!(
                    "epoch of scope {scope}: cached {cached_epoch} != committed {fresh_epoch}"
                ));
            }
        }

        Ok(out)
    }

    /// The shadow check: after a mutation has fully landed, fail loudly if
    /// the read model disagrees with SQLite. A no-op unless
    /// [`shadow_check_enabled`], inside a mutation body (only the outermost
    /// op has finished its writes), or while a test has suspended it.
    pub(crate) fn shadow_check_read_model(&self, op: &str) {
        if !shadow_check_enabled()
            || capture::in_mutation_body()
            || SHADOW_CHECK_SUSPENDED.with(std::cell::Cell::get)
        {
            return;
        }
        match self.read_model_divergence() {
            Ok(divergence) if divergence.is_empty() => {}
            Ok(divergence) => panic!(
                "read model diverged from SQLite after `{op}` ({} mismatches):\n  {}",
                divergence.len(),
                divergence.join("\n  ")
            ),
            // A store mid-teardown (dropped tables in a test) cannot be
            // compared; that is not drift.
            Err(err) => debug!(op, error = ?err, "read-model shadow check skipped"),
        }
    }
}

impl Store {
    /// Notice commits made through any connection other than this store's
    /// write connection (an operator's `sqlite3` against the live file, a
    /// second process) and rebuild the read model from disk when one
    /// happened. Returns whether it rebuilt.
    ///
    /// `PRAGMA data_version` on a connection changes only when ANOTHER
    /// connection commits, so the manager's own writes never trip this.
    /// Our read connections never write, and checkpoints do not count as
    /// commits.
    pub fn check_external_writes(&self) -> R<bool> {
        let version: i64 = {
            // A read pragma, so it bypasses the raft-mode write guard; it
            // still needs the connection's mutex.
            let conn = Self::lock_conn(&self.write_conn, "write")?;
            conn.query_row("PRAGMA data_version;", [], |row| row.get(0))?
        };
        let changed = {
            let mut baseline = self
                .external_write_baseline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let changed = baseline.is_some_and(|seen| seen != version);
            *baseline = Some(version);
            changed
        };
        if !changed {
            return Ok(false);
        }
        warn!(
            db_path = %self.db_path.display(),
            "Another process committed to the state database while the manager was running; \
             rebuilding the cached scheduling state from disk. Writing the state database \
             directly is unsupported (docs/state-store.md)"
        );
        crate::manager::telemetry::metrics()
            .read_model_external_writes_total
            .inc();
        // An outside write may have touched anything, the keyring included.
        self.refresh_read_model()?;
        self.keyring_rows_changed();
        Ok(true)
    }

    /// Reload up to `nodes` nodes from committed state, compare each with
    /// the cache, and repair any that differ. The rotation covers every
    /// committed and every cached node once per sweep, so drift on a node
    /// that no write ever touches again is still repaired. Returns how
    /// many nodes it repaired; each repair increments
    /// `legion_read_model_drift_repaired_total{source="scrub"}`.
    pub fn scrub_read_model_slice(&self, nodes: usize) -> R<usize> {
        let ids = self.next_read_model_scrub_nodes(nodes)?;
        let mut repaired = 0;
        for id in ids {
            if self.scrub_read_model_node(&id)? {
                repaired += 1;
            }
        }
        if repaired > 0 {
            crate::manager::telemetry::metrics()
                .read_model_drift_repaired_total
                .inc_by(&["scrub"], repaired as u64);
        }
        Ok(repaired)
    }

    fn next_read_model_scrub_nodes(&self, count: usize) -> R<Vec<String>> {
        let mut queue = self
            .read_model_scrub_queue
            .lock()
            .map_err(|_| anyhow!("read-model scrub queue lock poisoned"))?;
        if queue.is_empty() {
            let mut ids: BTreeSet<String> = {
                let conn = self.read_connection()?;
                let mut stmt = conn.prepare_cached("SELECT id FROM node;")?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                Self::collect_rows(rows)?.into_iter().collect()
            };
            ids.extend(self.read_model_reader()?.nodes.keys().cloned());
            queue.extend(ids);
        }
        let keep = queue.len().saturating_sub(count);
        Ok(queue.split_off(keep))
    }

    /// One node of [`Self::scrub_read_model_slice`]: returns whether the
    /// cache was wrong and has been repaired.
    ///
    /// The repair must never replace newer cached state with this older
    /// read. In `local_disk` mode the capture mutex excludes every mutation
    /// and its projection for the whole visit. In raft mode projection runs
    /// outside that mutex, so the visit is optimistic instead: it holds the
    /// node's cached `Arc`s from before its read and publishes only if both
    /// are still the ones in the model. Every writer replaces them (an
    /// insert, or `Arc::make_mut`, which copies because this visit holds a
    /// second reference), so pointer equality proves nothing moved;
    /// otherwise the node waits for the next sweep.
    fn scrub_read_model_node(&self, id: &str) -> R<bool> {
        let _exclusive = if self.raft_mode() {
            None
        } else {
            Some(capture::capture_exclusion()?)
        };
        let (cached_node, cached_allocations) = {
            let guard = self.read_model_reader()?;
            (
                guard.nodes.get(id).cloned(),
                guard.allocations_by_node.get(id).cloned(),
            )
        };
        let (node, allocations) = {
            let conn = self.read_connection()?;
            let node = conn
                .prepare_cached(GET_NODE.as_str())?
                .query_row([id], |row| self.node_from_row(row))
                .optional()?;
            let allocations = Self::collect_rows(
                conn.prepare_cached(GET_ALLOCS_FOR_NODE.as_str())?
                    .query_map([id], |row| self.allocation_from_row(row))?,
            )?;
            (node, allocations)
        };

        let mut guard = self.read_model_writer()?;
        if !same_arc(&cached_node, guard.nodes.get(id))
            || !same_arc(&cached_allocations, guard.allocations_by_node.get(id))
        {
            return Ok(false);
        }
        let node_matches = match (guard.nodes.get(id), &node) {
            (Some(cached), Some(committed)) => {
                comparable_node(cached)? == comparable_node(committed)?
            }
            (None, None) => true,
            _ => false,
        };
        let mut committed_sorted = allocations.clone();
        committed_sorted.sort_by_key(|allocation| allocation.id);
        let allocations_match =
            sorted_allocations(guard.allocations_by_node.get(id)) == committed_sorted;
        if node_matches && allocations_match {
            return Ok(false);
        }

        warn!(
            node_id = id,
            node_matches,
            allocations_match,
            "Cached scheduling state disagreed with the state database; reloaded the node"
        );
        match node {
            Some(node) => {
                guard.nodes.insert(id.to_string(), Arc::new(node));
            }
            None => {
                guard.nodes.remove(id);
                guard.indexes.remove_node(id);
                guard.node_pools.remove(id);
            }
        }
        guard.nodes_changed();
        if allocations.is_empty() {
            guard.remove_node_allocations(id);
        } else {
            guard.set_node_allocations(id.to_string(), allocations);
        }
        // The usage memo summarized the old list; the reprojection below
        // rebuilds it from the reloaded one.
        guard.node_usage.remove(id);
        guard.generation = guard.generation.wrapping_add(1);
        Self::reproject_model_nodes(&mut guard, &self.resource_policy, &[id]);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::scheduler::{PlanMode, Scheduler};
    use crate::structs::{JobConfig, ManagerConfigFull, NodeMsg, Submission};
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-read-model-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        let mut config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("temp dir is not utf-8")?
                .to_string(),
            ..Default::default()
        };
        config.encryption.root_key.path =
            Some(crate::storage::keyring::test_support::shared_root_key_path());
        let store = Store::start_storage(&config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn node_msg(id: &str) -> NodeMsg {
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: r#"{"worker":"pass"}"#.into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        }
    }

    fn submit(store: &Store, name: &str) -> R<String> {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
            pending_extensions: Default::default(),
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.ncl".into(),
                content: name.into(),
                is_primary: true,
            }],
        )?;
        Ok(store.get_job_config(name)?.id())
    }

    fn assert_coherent(store: &Store, after: &str) -> R<()> {
        let divergence = store.read_model_divergence()?;
        assert!(
            divergence.is_empty(),
            "read model diverged after {after}:\n  {}",
            divergence.join("\n  ")
        );
        Ok(())
    }

    // The four drift classes `main` had in `local_disk` mode
    // (read-model-coherence plan § 1). Each op is also covered by the
    // shadow check wherever the suite calls it; these name the class.

    #[test]
    fn registration_never_caches_health_ahead_of_the_database() -> R<()> {
        let store = setup_store()?;
        let _check = ShadowCheckSuspended::new();
        // The first half of `insert_node` used to cache the message's
        // health checks before the status-lane seed had stored them.
        store.replicated(Lane::Control, "insert_node", {
            let msg = node_msg("node-a");
            move |s| s.insert_node_apply(msg)
        })?;
        assert_coherent(&store, "the control half of insert_node")?;
        drop(_check);
        store.insert_node(node_msg("node-b"))?;
        assert_coherent(&store, "insert_node")
    }

    #[test]
    fn marking_lost_never_caches_availability_ahead_of_the_database() -> R<()> {
        let store = setup_store()?;
        store.insert_node(node_msg("node-a"))?;
        let _check = ShadowCheckSuspended::new();
        store.replicated(Lane::Status, "mark_nodes_as_lost", |s| {
            s.mark_nodes_as_lost_apply(&["node-a".to_string()])
        })?;
        assert_coherent(&store, "the status half of mark_nodes_as_lost")?;
        drop(_check);
        store.mark_nodes_as_lost(&["node-a".to_string()])?;
        assert_coherent(&store, "mark_nodes_as_lost")?;
        let node = store.cached_node_for_test("node-a")?;
        assert_eq!(node.status, "lost");
        assert_eq!(node.availability, "unavailable");
        Ok(())
    }

    #[test]
    fn completing_an_allocation_updates_its_cached_status() -> R<()> {
        let store = setup_store()?;
        store.insert_node(node_msg("node-a"))?;
        let job_config_id = submit(&store, "batch")?;
        let outcome = Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let allocation_id = outcome.allocations[0].id;
        store.complete_allocation(allocation_id, true)?;
        let cached = store.cached_allocations_by_node()?;
        let status = cached["node-a"]
            .iter()
            .find(|allocation| allocation.id == allocation_id)
            .map(|allocation| allocation.status.clone());
        assert_eq!(status.as_deref(), Some("complete"));
        assert_coherent(&store, "complete_allocation")
    }

    #[test]
    fn destroying_a_job_drops_its_cached_task_configs() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit(&store, "doomed")?;
        assert!(!store
            .cached_task_configs_for_job_config(&job_config_id)?
            .is_empty());
        store.stop_job("doomed")?;
        store.destroy_job("doomed")?;
        assert!(
            !store
                .read_model_reader()?
                .task_configs
                .contains_key(&job_config_id),
            "the destroyed config's task list must leave the cache, not linger empty"
        );
        assert_coherent(&store, "destroy_job")
    }

    #[test]
    fn a_full_rebuild_keeps_speculative_start() -> R<()> {
        let store = setup_store()?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "edge".into(),
            tasks: vec![TaskConfig {
                speculative_start: Some(crate::structs::edge_boot::SpeculativeStartConfig {
                    mode: Some("full".into()),
                    ..Default::default()
                }),
                ..TaskConfig::new("sandbox", "noop")
            }],
            version: None,
            pending_extensions: Default::default(),
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.ncl".into(),
                content: "edge".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config("edge")?.id();
        // A restart, snapshot install, or outside-write rebuild.
        store.refresh_read_model()?;
        let tasks = store.cached_task_configs_for_job_config(&job_config_id)?;
        assert!(
            tasks[0].1.speculative_start.is_some(),
            "the full load dropped speculative_start"
        );
        assert_coherent(&store, "a full rebuild")
    }

    #[test]
    fn divergence_names_what_drifted() -> R<()> {
        let store = setup_store()?;
        store.insert_node(node_msg("node-a"))?;
        let _check = ShadowCheckSuspended::new();
        {
            let conn = store.write_connection()?;
            conn.execute("UPDATE node SET pool = 'gpu' WHERE id = 'node-a';", [])?;
        }
        let divergence = store.read_model_divergence()?;
        assert_eq!(divergence.len(), 1, "{divergence:?}");
        assert!(
            divergence[0].starts_with("node node-a: cached"),
            "{divergence:?}"
        );
        Ok(())
    }

    #[test]
    fn a_body_that_fails_after_committing_still_reaches_the_cache() -> R<()> {
        let store = setup_store()?;
        store.insert_node(node_msg("node-a"))?;
        let result: R<()> = store.replicated(Lane::Control, "partial", |s| {
            {
                let conn = s.write_connection()?;
                conn.execute("UPDATE node SET pool = 'gpu' WHERE id = 'node-a';", [])?;
            }
            bail!("failed after the commit")
        });
        assert!(result.is_err());
        assert_eq!(store.cached_node_for_test("node-a")?.pool, "gpu");
        assert_coherent(&store, "a failed body")
    }

    #[test]
    fn outside_commits_rebuild_the_read_model() -> R<()> {
        let store = setup_store()?;
        store.insert_node(node_msg("node-a"))?;
        // The first check only records the baseline.
        assert!(!store.check_external_writes()?);
        // Our own writes and a checkpoint from another connection are not
        // outside commits.
        store.insert_node(node_msg("node-b"))?;
        assert!(!store.check_external_writes()?, "own write tripped it");
        // The store's own checkpointer runs PASSIVE checkpoints from a
        // separate connection every second, and the next write then
        // restarts the WAL; neither may read as an outside commit.
        let outside = Connection::open(&*store.db_path)?;
        for round in 0..3 {
            outside.query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |_| Ok(()))?;
            assert!(
                !store.check_external_writes()?,
                "checkpoint {round} tripped it"
            );
            store.insert_node(node_msg(&format!("node-c{round}")))?;
            assert!(
                !store.check_external_writes()?,
                "WAL restart {round} tripped it"
            );
        }

        let _check = ShadowCheckSuspended::new();
        outside.execute("UPDATE node SET pool = 'gpu' WHERE id = 'node-a';", [])?;
        assert_eq!(store.cached_node_for_test("node-a")?.pool, "default");
        assert!(store.check_external_writes()?);
        assert_eq!(store.cached_node_for_test("node-a")?.pool, "gpu");
        assert_coherent(&store, "an outside commit")?;
        assert!(
            !store.check_external_writes()?,
            "one rebuild per outside commit"
        );
        Ok(())
    }

    #[test]
    fn the_scrub_repairs_drift_within_one_sweep() -> R<()> {
        let store = setup_store()?;
        for id in ["node-a", "node-b", "node-c"] {
            store.insert_node(node_msg(id))?;
        }
        let job_config_id = submit(&store, "web")?;
        Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(
            store.scrub_read_model_slice(10)?,
            0,
            "a coherent cache needs no repair"
        );

        let _check = ShadowCheckSuspended::new();
        // Drift of every kind the scrub owns: a node the cache lost, a node
        // row behind the database, and an allocation list emptied.
        {
            let mut guard = store.read_model_writer()?;
            guard.nodes.remove("node-a");
            Arc::make_mut(guard.nodes.get_mut("node-b").expect("cached")).pool = "stale".into();
            guard.allocations_by_node = Default::default();
        }
        assert!(!store.read_model_divergence()?.is_empty());
        let mut repaired = 0;
        for _ in 0..3 {
            repaired += store.scrub_read_model_slice(1)?;
        }
        assert!(repaired >= 2, "repaired {repaired}");
        assert_coherent(&store, "one sweep of the scrub")
    }

    /// Phase 4 relies on this for `destroy_job`: a session changeset
    /// records the child rows an `ON DELETE CASCADE` removes, so the
    /// projection sees the task configs go even though no statement named
    /// `task_config`.
    #[test]
    fn session_changesets_record_cascaded_deletes() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit(&store, "cascade")?;
        store.stop_job("cascade")?;
        let conn = Connection::open(&*store.db_path)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let mut session = rusqlite::session::Session::new(&conn)?;
        session.attach(None)?;
        conn.execute("DELETE FROM job WHERE name = 'cascade';", [])?;
        let mut changeset = Vec::new();
        session.changeset_strm(&mut changeset)?;
        drop(session);

        use fallible_streaming_iterator::FallibleStreamingIterator;
        let reader: &mut dyn std::io::Read = &mut &*changeset;
        let mut iter = rusqlite::session::ChangesetIter::start_strm(&reader)?;
        let mut tables = BTreeSet::new();
        while let Some(item) = iter.next()? {
            tables.insert(item.op()?.table_name().to_string());
        }
        assert!(tables.contains("job_config"), "{tables:?}");
        assert!(tables.contains("task_config"), "{tables:?}");
        let _ = job_config_id;
        Ok(())
    }
}
