use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;

use libloading::{Library, Symbol};

mod node_checkers;

use crate::util::plugin_root_dir;
use node_checkers::{ArchNodeChecker, DiskChecker, KitchenSinkNodeChecker, OSNodeChecker};

// NOTE: Improve current fingerprinters:
// Network - speed, IP addresses, etc

// NOTE: Potential additional fingerprinters:
// CNI - which plugins are on the machine
// Landlock - is it available?
// Cloud provider fingerprinters?

// Helpful links for reference later:
// https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html
// https://crates.io/crates/sysinfo

#[instrument(skip_all)]
pub fn fingerprint_node(additional_node_checks: Vec<String>) -> Map<String, Value> {
    let fps: Vec<Box<dyn NodeChecker>> = vec![
        Box::new(ArchNodeChecker {}),
        Box::new(OSNodeChecker {}),
        Box::new(KitchenSinkNodeChecker {}),
    ];

    let mut ret = Map::new();

    fps.iter().filter(|f| f.has_fingerprinter()).for_each(|f| {
        ret.extend(f.fingerprint());
    });

    additional_node_checks
        .iter()
        .for_each(|name| match call_dynamic(name) {
            Ok((_lib, fp)) => {
                ret.extend(fp.fingerprint());
            }
            Err(e) => {
                warn!("Error loading fingerprinter {name}, {e}");
            }
        });

    ret
}

#[instrument(skip_all)]
pub fn run_health_checks(
    additional_node_checks: Vec<String>,
    task_readiness: Option<Map<String, Value>>,
) -> Map<String, Value> {
    let fps: Vec<Box<dyn NodeChecker>> = vec![Box::new(DiskChecker {})];

    let mut ret = Map::new();

    fps.iter().filter(|f| f.has_health_check()).for_each(|f| {
        ret.extend(f.health_check());
    });

    additional_node_checks
        .iter()
        .for_each(|name| match call_dynamic(name) {
            Ok((_lib, fp)) => {
                ret.extend(fp.health_check());
            }
            Err(e) => {
                error!("Error loading fingerprinter {name}: {e}");
            }
        });

    if let Some(readiness) = task_readiness {
        ret.insert("task_plugins".into(), Value::Object(readiness));
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
fn call_dynamic(name: &str) -> R<(Library, Box<dyn NodeChecker>)> {
    let plugin_root = plugin_root_dir();
    let paths = plugin_paths("node_checks", name, &plugin_root);
    validate_manifest("node_checks", name, &paths.root)
        .with_context(|| format!("invalid node_check plugin manifest for {}", name))?;

    unsafe {
        let lib = crate::plugin_runtime::open_plugin_library("node_checks", name, &paths.dylib)?;
        let fp = {
            let func: Symbol<unsafe extern "C" fn() -> Box<dyn NodeChecker>> = lib.get(b"get")?;
            func()
        };
        Ok((lib, fp))
    }
}

pub trait NodeChecker {
    fn name(&self) -> &'static str;

    fn has_fingerprinter(&self) -> bool {
        false
    }

    fn has_health_check(&self) -> bool {
        false
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".into(), Value::String("bar".into()));
        map
    }

    fn health_check(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("alive".into(), Value::String("pass".into()));
        map
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
            // Test that the plugin works
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
}
