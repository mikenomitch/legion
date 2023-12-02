pub trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".to_string(), Value::String("bar".to_string()));
        map
    }
}