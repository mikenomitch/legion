//! Node checks: what a worker reports about its node (docs/plugin-system.md,
//! "Node checks").
//!
//! A node check contributes **fingerprint attributes** (static facts merged
//! into the node's `fingerprint_attrs` at registration: `os`, `arch`, CPU
//! and memory, VMM versions) and/or a **health report** (a typed
//! [`NodeCheckReport`], re-run on every node-check pass and stored under
//! the check's name in the node's health checks).
//!
//! Built-in checks live in [`node_checkers`]. Plugins are dylibs under
//! `plugins/node_checks/<name>/` built with `legion::export_node_checker!`,
//! named by a worker type's `additionalNodeChecks`. A plugin is loaded
//! **once** per worker process and reused on every pass (it used to be
//! dlopened on every pass), and every call into it is guarded
//! ([`crate::plugin_runtime::containment`]): a plugin that panics is
//! quarantined and reported as a failing check, and the worker keeps
//! running.

use std::collections::HashMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::plugin_runtime::containment::{self, PanicProbe};
use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;

use libloading::Library;

mod node_checkers;
pub(crate) use node_checkers::checkpoint_compat::local_cpu_attrs;

use crate::util::plugin_root_dir;
use node_checkers::{
    ArchNodeChecker, CheckpointCompatNodeChecker, DiskChecker, KitchenSinkNodeChecker,
    OSNodeChecker,
};

/// A health report's verdict. `warn` keeps the node healthy; `fail` marks
/// it unhealthy in `legion node list` and the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

/// What one node check reports on a pass. Stored as
/// `health_checks.<check name> = { status, message, attributes }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCheckReport {
    pub status: CheckStatus,
    /// One line for an operator (`/var has 5% free`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Structured detail (`lowestFreePercent`).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Value>,
}

impl NodeCheckReport {
    pub fn pass(message: impl Into<String>) -> Self {
        NodeCheckReport {
            status: CheckStatus::Pass,
            message: message.into(),
            attributes: Map::new(),
        }
    }

    pub fn fail(message: impl Into<String>) -> Self {
        NodeCheckReport {
            status: CheckStatus::Fail,
            message: message.into(),
            attributes: Map::new(),
        }
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// The node-check plugin contract (plugin API version 3).
pub trait NodeChecker: Send + Sync {
    fn name(&self) -> &'static str;

    /// Static facts merged into the node's fingerprint attributes at
    /// registration. Default: none.
    fn fingerprint(&self) -> Map<String, Value> {
        Map::new()
    }

    /// A health report, run on every node-check pass (every 30 seconds).
    /// Default: none (the check does no health checking).
    fn health_check(&self) -> Option<NodeCheckReport> {
        None
    }
}

/// A dylib check's methods run under its own `catch_unwind`. Generic, so
/// `legion::export_node_checker!` compiles this impl into the plugin.
impl<T: NodeChecker> NodeChecker for containment::Contained<T> {
    fn name(&self) -> &'static str {
        containment::contained("name", || "panicked", || self.0.name())
    }

    fn fingerprint(&self) -> Map<String, Value> {
        containment::contained("fingerprint", Map::new, || self.0.fingerprint())
    }

    fn health_check(&self) -> Option<NodeCheckReport> {
        containment::contained(
            "health_check",
            || Some(NodeCheckReport::fail("the node check panicked")),
            || self.0.health_check(),
        )
    }
}

/// A loaded node-check plugin, kept for the life of the process.
struct LoadedCheck {
    checker: Box<dyn NodeChecker>,
    probe: PanicProbe,
    _lib: Library,
}

/// A loaded plugin, or why it failed to load.
type LoadResult = Result<Arc<LoadedCheck>, String>;

/// Plugins by name: loaded on first use, then reused. A load failure is
/// remembered too, so a missing plugin is reported every pass without being
/// retried (and re-logged) every pass; a worker restart retries it.
static LOADED: LazyLock<Mutex<HashMap<String, LoadResult>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn loaded(name: &str) -> Result<Arc<LoadedCheck>, String> {
    let mut cache = LOADED.lock().unwrap_or_else(|err| err.into_inner());
    if let Some(entry) = cache.get(name) {
        return entry.clone();
    }
    let entry = load(name).map(Arc::new).map_err(|err| {
        let message = format!("{err:#}");
        warn!(plugin = name, error = %message, "Could not load node-check plugin");
        message
    });
    cache.insert(name.to_string(), entry.clone());
    entry
}

fn builtins() -> Vec<Box<dyn NodeChecker>> {
    vec![
        Box::new(ArchNodeChecker {}),
        Box::new(OSNodeChecker {}),
        Box::new(KitchenSinkNodeChecker {}),
        Box::new(CheckpointCompatNodeChecker {}),
        Box::new(DiskChecker {}),
    ]
}

/// Every check's fingerprint attributes, built-ins first, then the named
/// plugins. A plugin that cannot load or panics contributes nothing.
#[instrument(skip_all)]
pub fn fingerprint_node(additional_node_checks: Vec<String>) -> Map<String, Value> {
    let mut ret = Map::new();
    for check in builtins() {
        ret.extend(check.fingerprint());
    }
    for name in &additional_node_checks {
        let Ok(plugin) = loaded(name) else {
            continue;
        };
        match containment::guard(
            "node_checks",
            name,
            "fingerprint",
            Some(&plugin.probe),
            || plugin.checker.fingerprint(),
        ) {
            Ok(attrs) => ret.extend(attrs),
            Err(err) => {
                warn!(plugin = %name, error = %format!("{err:#}"), "Node-check fingerprint failed")
            }
        }
    }
    ret
}

/// Every check's health report, by check name, plus the task runtimes'
/// readiness (`task_plugins`) and any quarantined plugin of this process
/// (`plugins`). A plugin that cannot load or panics reports a failing check.
#[instrument(skip_all)]
pub fn run_health_checks(
    additional_node_checks: Vec<String>,
    task_readiness: Option<Map<String, Value>>,
) -> Map<String, Value> {
    let mut ret = Map::new();
    for check in builtins() {
        if let Some(report) = check.health_check() {
            ret.insert(check.name().to_string(), report.to_value());
        }
    }
    for name in &additional_node_checks {
        let report = match loaded(name) {
            Err(message) => Some(NodeCheckReport::fail(format!(
                "plugin failed to load: {message}"
            ))),
            Ok(plugin) => match containment::guard(
                "node_checks",
                name,
                "health_check",
                Some(&plugin.probe),
                || plugin.checker.health_check(),
            ) {
                Ok(report) => report,
                Err(err) => Some(NodeCheckReport::fail(format!("{err:#}"))),
            },
        };
        if let Some(report) = report {
            ret.insert(name.clone(), report.to_value());
        }
    }

    if let Some(readiness) = task_readiness {
        ret.insert("task_plugins".into(), Value::Object(readiness));
    }
    // Any dylib plugin this worker quarantined (task runtimes' fetchers,
    // node checks): a failing check, so the node reads as degraded.
    if let Some(quarantined) = containment::quarantine_health() {
        ret.insert("plugins".into(), quarantined);
    }

    ret
}

/// The resolved dylib path for a node-check plugin — test-only, used to
/// decide whether the example plugin's assertions can run (same pattern
/// as `task_plugins::task_plugin_name_to_path`).
#[cfg(test)]
fn fp_plugin_name_to_path(name: &str) -> String {
    let plugin_root = plugin_root_dir();
    plugin_paths("node_checks", name, &plugin_root)
        .dylib
        .to_string_lossy()
        .into_owned()
}

/// Load a node-check plugin by NAME: manifest from the plugin's root,
/// dylib from wherever `plugin_paths` resolves it (in-tree or the
/// workspace `plugin-build` layout). Never derive the root from the
/// dylib path — the two live in different trees under the workspace
/// build layout.
#[instrument(err, skip_all)]
fn load(name: &str) -> R<LoadedCheck> {
    let plugin_root = plugin_root_dir();
    let paths = plugin_paths("node_checks", name, &plugin_root);
    validate_manifest("node_checks", name, &paths.root)
        .with_context(|| format!("invalid node_check plugin manifest for {}", name))?;

    unsafe {
        let lib = crate::plugin_runtime::open_plugin_library("node_checks", name, &paths.dylib)?;
        let (checker, probe) = crate::plugin_runtime::contained_constructor::<dyn NodeChecker>(
            "node_checks",
            name,
            &lib,
        )?;
        Ok(LoadedCheck {
            checker,
            probe,
            _lib: lib,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn fingerprint_node_works() -> R<()> {
        let fps = vec!["example".into()];
        let res = fingerprint_node(fps);

        let os = res.get("os").context("no os")?.as_str().context("no os")?;
        assert_eq!(std::env::consts::OS, os);

        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_err() {
            // Test that the plugin works when built
            let plugin_path = fp_plugin_name_to_path("example");
            if Path::new(&plugin_path).exists() {
                let basic = res
                    .get("basic")
                    .context("no basic")?
                    .as_str()
                    .context("no basic")?;
                assert_eq!("output", basic);
            } else {
                assert!(res.get("basic").is_none());
            }
        }

        Ok(())
    }

    /// A plugin that cannot load is reported as a failing check, and the
    /// load is not retried (it is cached) until the worker restarts.
    #[test]
    fn a_missing_plugin_is_a_failing_check_and_is_loaded_once_test() {
        let checks = run_health_checks(vec!["legion-no-such-check".into()], None);
        let report: NodeCheckReport =
            serde_json::from_value(checks["legion-no-such-check"].clone()).expect("report");
        assert_eq!(report.status, CheckStatus::Fail);
        assert!(report.message.contains("failed to load"), "{report:?}");
        assert!(LOADED
            .lock()
            .unwrap()
            .get("legion-no-such-check")
            .is_some_and(Result::is_err));
        // The built-in disk check reports a typed report too.
        let disk: NodeCheckReport =
            serde_json::from_value(checks["disk"].clone()).expect("disk report");
        assert!(!disk.message.is_empty());
    }

    struct Exploding;

    impl NodeChecker for Exploding {
        fn name(&self) -> &'static str {
            "exploding"
        }
        fn health_check(&self) -> Option<NodeCheckReport> {
            panic!("the check fell over")
        }
    }

    /// The wrapper a plugin gets from `export_node_checker!` turns a panic
    /// into a failing report, and the host's guard quarantines the plugin.
    #[test]
    fn a_panicking_node_check_is_contained_test() {
        let checker: Box<dyn NodeChecker> = Box::new(containment::Contained(Exploding));
        let probe = PanicProbe::in_process();
        let err = containment::guard(
            "node_checks",
            "exploding-test",
            "health_check",
            Some(&probe),
            || checker.health_check(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("fell over"), "{err}");
        assert!(containment::check_quarantine("node_checks", "exploding-test").is_err());
        containment::release_quarantine("node_checks", "exploding-test");
    }
}
