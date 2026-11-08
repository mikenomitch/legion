#![crate_type = "cdylib"]

//! The example node-check plugin (docs/plugin-system.md, "Node checks"):
//! one fingerprint attribute and one typed health report.

use legion::worker::node_check::{NodeCheckReport, NodeChecker};
use serde_json::{Map, Value};

struct BasicNodeChecker;

impl NodeChecker for BasicNodeChecker {
    fn name(&self) -> &'static str {
        "basic"
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("basic".into(), Value::String("output".into()));
        map
    }

    fn health_check(&self) -> Option<NodeCheckReport> {
        Some(NodeCheckReport::pass("the example check always passes"))
    }
}

// Exports the ABI fingerprint, the panic probe, and the contained
// constructor: a panic in a check quarantines this plugin and shows as a
// failing check, instead of taking the worker down.
legion::export_node_checker!(BasicNodeChecker);
