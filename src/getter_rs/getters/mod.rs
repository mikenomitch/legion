use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::getter_rs::Source;

mod file;
mod git;
mod http;

pub use file::FileGetter;
pub use git::GitGetter;
pub use http::HttpGetter;

pub trait Getter: Send + Sync {
    fn scheme(&self) -> &'static str;
    fn get(&self, source: &Source, dest: &Path) -> Result<PathBuf>;
}
