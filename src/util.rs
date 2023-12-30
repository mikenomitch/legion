use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_json::{to_string_pretty, Value};
use std::collections::HashMap;
use std::error::Error;

// Adds convenience for Result for brevity
pub type R<T> = Result<T, Box<dyn Error>>;

pub fn merge_json_strings(a: String, b: String) -> String {
    let mut data_a: HashMap<String, Value> = serde_json::from_str(&a).unwrap();
    let data_b: HashMap<String, Value> = serde_json::from_str(&b).unwrap();
    data_a.extend(data_b);
    to_string_pretty(&data_a).unwrap()
}

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
