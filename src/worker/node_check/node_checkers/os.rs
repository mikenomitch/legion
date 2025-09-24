use crate::prelude::*;
use crate::worker::node_check::NodeChecker;
use std::env::consts::OS;

pub struct OSNodeChecker {}

impl NodeChecker for OSNodeChecker {
    fn name(&self) -> &'static str {
        "os"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("os".into(), Value::String(OS.into()));
        map
    }
}
