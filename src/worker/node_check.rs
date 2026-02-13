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
        .map(|name| fp_plugin_name_to_path(name))
        .for_each(|path| match call_dynamic(&path) {
            Ok((_lib, fp)) => {
                ret.extend(fp.fingerprint());
            }
            Err(e) => {
                warn!("Error loading fingerprinter at path {path}, {e}");
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
        .map(|name| fp_plugin_name_to_path(name))
        .for_each(|path| match call_dynamic(&path) {
            Ok((_lib, fp)) => {
                ret.extend(fp.health_check());
            }
            Err(e) => {
                error!("Error loading fingerprinter: {e}");
            }
        });

    if let Some(readiness) = task_readiness {
        ret.insert("task_plugins".into(), Value::Object(readiness));
    }

    ret
}

fn fp_plugin_name_to_path(name: &str) -> String {
    let plugin_root = plugin_root_dir();
    plugin_paths("node_checks", name, &plugin_root)
        .dylib
        .to_string_lossy()
        .into_owned()
}

#[instrument(err, skip_all)]
fn call_dynamic(path: &str) -> R<(Library, Box<dyn NodeChecker>)> {
    let plugin_path = std::path::Path::new(path);
    let Some(plugin_root) = plugin_path
        .parent()
        .and_then(|v| v.parent())
        .and_then(|v| v.parent())
    else {
        bail!("invalid node_check plugin path {}", path);
    };
    let name = plugin_root
        .file_name()
        .and_then(|v| v.to_str())
        .context("invalid node_check plugin name")?;
    validate_manifest("node_checks", name, plugin_root)
        .with_context(|| format!("invalid node_check plugin manifest for {}", name))?;

    unsafe {
        let lib = Library::new(path)?;
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
