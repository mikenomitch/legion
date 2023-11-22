#![crate_type = "cdylib"]

// TODO: GET THE TRAIT FROM LEGION

use serde_json::{Map, Number, Value};
use std::collections::HashMap;

trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".to_string(), Value::String("bar".to_string()));
        map
    }
}

struct BasicFingerprinter {}

impl Fingerprinter for BasicFingerprinter {
    fn name(&self) -> &'static str { "basic" }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("basic".to_string(), Value::String("output".to_string()));
        map
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn Fingerprinter> {
    Box::new(BasicFingerprinter{})
}
