//! Host side of scheduling-index kind plugins (docs/scheduling-indexes.md,
//! "Index kinds"; candidates-and-indexes plan § 6.7).
//!
//! An index declared with `kind = "<name>"` takes its geometry from the
//! dylib at `plugins/indexes/<name>/`. The library exports an
//! [`IndexKindFactory`] (`legion::export_index_kind!`); each declared index
//! of that kind gets its own [`IndexKind`] from the factory's `build`. Every
//! call into the plugin is guarded: a panic quarantines the kind for the
//! life of the process, its indexes report unhealthy, selection skips them,
//! and planning falls back to the scan. A plugin can therefore make the
//! candidate order worse, never a placement invalid (the index proposes, the
//! ledger disposes).

use std::sync::{Arc, Mutex, OnceLock};

use libloading::Library;

use crate::plugin_runtime::containment::{check_quarantine, guard, PanicProbe};
use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;
use crate::resources::ResourcePool;
use crate::scheduling_index::{CellRank, IndexKind, IndexKindFactory, IndexRequest, IndexSettings};
use crate::structs::Node;
use crate::util::plugin_root_dir;

/// The plugin directory kind (`plugins/indexes/<name>/`) and the kind
/// recorded in quarantine and panic metrics.
pub const INDEX_KIND_PLUGIN_KIND: &str = "indexes";

/// A loaded factory and what keeps it callable. Libraries are never
/// unloaded: index settings live for the life of the process.
struct LoadedFactory {
    factory: Box<dyn IndexKindFactory>,
    probe: PanicProbe,
    _library: Library,
}

/// Every factory loaded so far, by kind name.
type LoadedFactories = Mutex<Vec<(String, Arc<LoadedFactory>)>>;

fn loaded() -> &'static LoadedFactories {
    static LOADED: OnceLock<LoadedFactories> = OnceLock::new();
    LOADED.get_or_init(|| Mutex::new(Vec::new()))
}

fn load_factory(name: &str) -> R<Arc<LoadedFactory>> {
    let mut loaded = loaded()
        .lock()
        .map_err(|_| anyhow!("index kind registry lock poisoned"))?;
    if let Some((_, factory)) = loaded.iter().find(|(loaded, _)| loaded == name) {
        return Ok(Arc::clone(factory));
    }
    let root = plugin_root_dir();
    let paths = plugin_paths(INDEX_KIND_PLUGIN_KIND, name, &root);
    validate_manifest(INDEX_KIND_PLUGIN_KIND, name, &paths.root)
        .with_context(|| format!("invalid index kind plugin manifest for {name}"))?;
    // SAFETY: the operator installed this library under the plugin root and
    // named it in the manager config — the same trust every dylib plugin
    // gets; `open_plugin_library` refuses one built against another ABI.
    let factory = unsafe {
        let library =
            crate::plugin_runtime::open_plugin_library(INDEX_KIND_PLUGIN_KIND, name, &paths.dylib)?;
        let (factory, probe) = crate::plugin_runtime::contained_constructor::<dyn IndexKindFactory>(
            INDEX_KIND_PLUGIN_KIND,
            name,
            &library,
        )?;
        Arc::new(LoadedFactory {
            factory,
            probe,
            _library: library,
        })
    };
    loaded.push((name.to_string(), Arc::clone(&factory)));
    Ok(factory)
}

/// Resolves every plugin-kind declaration in `settings` from the plugin
/// directory. A kind that is missing, built for another ABI, or refuses an
/// index's options fails the whole load, naming the index: indexes silently
/// doing nothing is the failure the parse-time checks exist to prevent.
pub fn resolve_plugin_kinds(settings: &mut IndexSettings) -> R<()> {
    if settings.plugin_kinds().is_empty() {
        return Ok(());
    }
    settings.resolve_kinds(|kind, index, options| {
        let factory = load_factory(kind)?;
        let probe = factory.probe;
        let built = guard(INDEX_KIND_PLUGIN_KIND, kind, "build", Some(&probe), || {
            factory.factory.build(index, options)
        })??;
        info!(kind, index, "Loaded scheduling index kind plugin");
        Ok(Arc::new(GuardedIndexKind::new(kind, built, Some(probe))) as Arc<dyn IndexKind>)
    })
}

/// The host's wrapper around one plugin geometry: every call is guarded,
/// a panic quarantines the kind, and a quarantined kind answers inert
/// values and reports itself unhealthy, so selection stops choosing it.
pub struct GuardedIndexKind {
    kind: String,
    reported_name: &'static str,
    inner: Box<dyn IndexKind>,
    probe: Option<PanicProbe>,
}

impl GuardedIndexKind {
    /// `probe` is the dylib's panic probe; `None` for an in-process kind
    /// (tests), whose panics the guard catches directly.
    pub fn new(kind: &str, inner: Box<dyn IndexKind>, probe: Option<PanicProbe>) -> Self {
        let reported_name = guard(INDEX_KIND_PLUGIN_KIND, kind, "name", probe.as_ref(), || {
            inner.name()
        })
        .unwrap_or("quarantined");
        GuardedIndexKind {
            kind: kind.to_string(),
            reported_name,
            inner,
            probe,
        }
    }

    fn call<T>(&self, op: &str, fallback: T, call: impl FnOnce() -> T) -> T {
        match guard(
            INDEX_KIND_PLUGIN_KIND,
            &self.kind,
            op,
            self.probe.as_ref(),
            call,
        ) {
            Ok(value) => value,
            Err(error) => {
                debug!(kind = %self.kind, error = %format!("{error:#}"), "Index kind plugin unavailable");
                fallback
            }
        }
    }
}

impl IndexKind for GuardedIndexKind {
    fn name(&self) -> &'static str {
        self.reported_name
    }

    fn project(&self, node: &Node, free: &ResourcePool) -> Vec<u16> {
        self.call("project", Vec::new(), || self.inner.project(node, free))
    }

    fn rank(&self, cell: &[u16], request: &IndexRequest) -> CellRank {
        self.call(
            "rank",
            CellRank {
                fits: false,
                distance: f64::INFINITY,
            },
            || self.inner.rank(cell, request),
        )
    }

    fn coverage(&self, request: &IndexRequest) -> usize {
        self.call("coverage", 0, || self.inner.coverage(request))
    }

    fn healthy(&self) -> bool {
        check_quarantine(INDEX_KIND_PLUGIN_KIND, &self.kind).is_ok()
            && self.call("healthy", false, || self.inner.healthy())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduling_index::{IndexQueryOutcome, IndexSet};
    use std::collections::HashMap;

    /// Ranks by the node's `slot` attribute; panics in `rank` when asked.
    struct Slots {
        panic_in_rank: bool,
    }

    impl IndexKind for Slots {
        fn name(&self) -> &'static str {
            "slots"
        }
        fn project(&self, node: &Node, _free: &ResourcePool) -> Vec<u16> {
            vec![node
                .config_attrs
                .get("slot")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u16]
        }
        fn rank(&self, cell: &[u16], _request: &IndexRequest) -> CellRank {
            if self.panic_in_rank {
                panic!("rank exploded");
            }
            CellRank {
                fits: true,
                distance: f64::from(cell[0]),
            }
        }
    }

    fn node(id: &str, slot: u64) -> Node {
        Node {
            id: id.into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "slot": slot }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn set_with(kind: &str, panic_in_rank: bool) -> IndexSet {
        let mut settings = IndexSettings::from_scheduler_settings(
            Some(&json!({
                "index_min_nodes": 1,
                "indexes": {
                    "fit": false,
                    "custom": { "kind": kind, "partition_by": ["pool"] },
                },
            })),
            "bin_pack",
        )
        .expect("settings");
        settings
            .resolve_kinds(|kind, _, _| {
                Ok(Arc::new(GuardedIndexKind::new(
                    kind,
                    Box::new(Slots { panic_in_rank }),
                    None,
                )) as Arc<dyn IndexKind>)
            })
            .expect("resolve");
        let mut set = IndexSet::new(Arc::new(settings));
        for (id, slot) in [("c", 3), ("a", 1), ("b", 2)] {
            set.reindex_node(&node(id, slot), &ResourcePool::default());
        }
        set
    }

    #[test]
    fn a_guarded_kind_orders_candidates() {
        let set = set_with("test_slots_ok", false);
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&bindings, &IndexRequest::default(), None)
        else {
            panic!("expected candidates");
        };
        assert_eq!(candidates.node_ids, vec!["a", "b", "c"]);
    }

    /// A panicking kind is quarantined: the query that hit the panic still
    /// returns the whole shard (tail order), and every later query reports
    /// the index unusable, so planning scans.
    #[test]
    fn a_panicking_kind_is_quarantined_and_skipped() {
        let kind = "test_slots_panics";
        let set = set_with(kind, true);
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let IndexQueryOutcome::Candidates(first) =
            set.query(&bindings, &IndexRequest::default(), None)
        else {
            panic!("expected candidates from the first query");
        };
        assert_eq!(first.node_ids.len(), 3, "the shard is still whole");
        let IndexQueryOutcome::Unusable { reason } =
            set.query(&bindings, &IndexRequest::default(), None)
        else {
            panic!("expected the quarantined kind to be skipped");
        };
        assert!(reason.contains("quarantined"), "{reason}");
        // The quarantine is process-wide; this kind name is this test's own.
    }

    #[test]
    fn a_missing_kind_plugin_fails_the_load_naming_the_index() {
        let mut settings = IndexSettings::from_scheduler_settings(
            Some(&json!({
                "indexes": { "custom": { "kind": "no_such_kind_plugin", "partition_by": ["pool"] } },
            })),
            "bin_pack",
        )
        .expect("settings");
        let error = resolve_plugin_kinds(&mut settings).expect_err("missing plugin");
        let message = format!("{error:#}");
        assert!(message.contains("index custom"), "{message}");
        assert!(message.contains("no_such_kind_plugin"), "{message}");
    }
}
