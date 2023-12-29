#![crate_type = "cdylib"]

use serde_json::{Map, Number, Value};
use std::collections::HashMap;

trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Vec<str>;

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".into(), Value::String("bar".into()));
        map
    }
}

struct BasicFingerprinter {}

impl Fingerprinter for BasicFingerprinter {
    fn name(&self) -> &'static str { "basic" }

    fn capabilities(&self) -> Vec<str> {
        vec!["static", "resources", "health"]
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("basic".into(), Value::String("output".into()));
        map
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn Fingerprinter> {
    Box::new(BasicFingerprinter{})
}
