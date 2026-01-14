use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::path::PathBuf;

// Adds convenience for Result for brevity
pub type R<T> = anyhow::Result<T>;

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn plugin_root() -> PathBuf {
    if let Ok(dir) = std::env::var("LEGION_PLUGIN_DIR") {
        return PathBuf::from(dir);
    }
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("plugins");
    path
}
