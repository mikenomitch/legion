use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::error::Error;

// Adds convenience for Result for brevity
pub type R<T> = Result<T, Box<dyn Error>>;

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
