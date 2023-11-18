#![crate_type = "cdylib"]

// TODO: GET THE TRAIT FROM LEGION

use std::collections::HashMap;

trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([("foo".to_string(), "bar".to_string())])
    }
}

struct BasicFingerprinter {}

impl Fingerprinter for BasicFingerprinter {
    fn name(&self) -> &'static str { "basic" }

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([
            ("basic".to_string(), "output".to_string())
        ])
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn Fingerprinter> {
    Box::new(BasicFingerprinter{})
}
