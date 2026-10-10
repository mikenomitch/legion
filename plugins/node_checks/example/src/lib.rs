#![crate_type = "cdylib"]

use serde_json::{Map, Number, Value};
use std::collections::HashMap;

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

struct BasicNodeChecker {}
impl NodeChecker for BasicNodeChecker {
    fn name(&self) -> &'static str {
        "basic"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn has_health_check(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("basic".into(), Value::String("output".into()));
        map
    }

    fn health_check(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("basic".into(), Value::String("pass".into()));
        map
    }
}

// Lets the host refuse this dylib if it was built with a different rustc or
// legion version (Rust has no stable ABI).
legion::declare_plugin_abi!();

#[no_mangle]
pub extern "C" fn get() -> Box<dyn NodeChecker> {
    Box::new(BasicNodeChecker {})
}
