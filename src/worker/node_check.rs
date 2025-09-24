use crate::prelude::*;

use libloading::{Library, Symbol};

mod node_checkers;

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
        .map(fp_plugin_name_to_path)
        .for_each(|path| match call_dynamic(&path) {
            Ok(fp) => {
                ret.extend(fp.fingerprint());
            }
            Err(e) => {
                warn!("Error loading fingerprinter at path {path}, {e}");
            }
        });

    ret
}

#[instrument(skip_all)]
pub fn run_health_checks(additional_node_checks: Vec<String>) -> Map<String, Value> {
    let fps: Vec<Box<dyn NodeChecker>> = vec![Box::new(DiskChecker {})];

    let mut ret = Map::new();

    fps.iter().filter(|f| f.has_health_check()).for_each(|f| {
        ret.extend(f.health_check());
    });

    additional_node_checks
        .iter()
        .map(fp_plugin_name_to_path)
        .for_each(|path| match call_dynamic(&path) {
            Ok(fp) => {
                ret.extend(fp.health_check());
            }
            Err(e) => {
                error!("Error loading fingerprinter: {e}");
            }
        });

    ret
}

fn fp_plugin_name_to_path(name: &String) -> String {
    format!(
        "/Users/mike/Code/legion/plugins/node_checks/{}/target/release/libexample.dylib",
        name
    )
}

#[instrument(err, skip_all)]
fn call_dynamic(path: &str) -> R<Box<dyn NodeChecker>> {
    unsafe {
        let lib = Library::new(path)?;
        let func: Symbol<unsafe extern "C" fn() -> Box<dyn NodeChecker>> = lib.get(b"get")?;
        Ok(func())
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

    #[test]
    fn fingerprint_node_works() -> R<()> {
        let fps = vec!["example".into()];
        let res = fingerprint_node(fps);

        let os = res.get("os").ok_or("no os")?.as_str().ok_or("no os")?;
        assert_eq!("macos", os);

        // Test that the plugin works
        let basic = res
            .get("basic")
            .ok_or("no basic")?
            .as_str()
            .ok_or("no basic")?;
        assert_eq!("output", basic);

        Ok(())
    }
}
