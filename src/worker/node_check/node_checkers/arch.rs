use crate::prelude::*;
use crate::worker::node_check::NodeChecker;
use std::env::consts::ARCH;

pub struct ArchNodeChecker {}

impl NodeChecker for ArchNodeChecker {
    fn name(&self) -> &'static str {
        "arch"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("arch".into(), Value::String(ARCH.to_string()));
        map
    }
}
