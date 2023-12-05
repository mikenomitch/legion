use std::error::Error;
use serde_json::{Value, to_string_pretty};
use std::collections::HashMap;

// Adds convenience for Result for brevity
pub type R<T> = Result<T, Box<dyn Error>>;

// pub fn merge_json(a: Value, b: Value) -> Value {
//     let mut data_a: HashMap<String, Value> = serde_json::from_str(&a).unwrap();
//     let data_b: HashMap<String, Value> = serde_json::from_str(&b).unwrap();
//     data_a.extend(data_b);
//     json!(data_a)
// }

pub fn merge_json_strings(a: String, b: String) -> String {
    let mut data_a: HashMap<String, Value> = serde_json::from_str(&a).unwrap();
    let data_b: HashMap<String, Value> = serde_json::from_str(&b).unwrap();
    data_a.extend(data_b);
    to_string_pretty(&data_a).unwrap()
}
