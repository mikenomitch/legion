// TODO: GET THE TRAIT FROM LEGION!!

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

pub fn get() -> Box<dyn Fingerprinter> {
    Box::new(BasicFingerprinter{})
}
