use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};

use crate::getter_rs::getters::Getter;
use crate::getter_rs::Source;

#[derive(Debug, Default)]
pub struct GitGetter;

impl GitGetter {
    pub const SCHEME: &'static str = "git";

    pub fn new() -> Self {
        Self
    }
}

impl Getter for GitGetter {
    fn scheme(&self) -> &'static str {
        Self::SCHEME
    }

    fn get(&self, source: &Source, dest: &Path) -> Result<PathBuf> {
        let url = match source {
            Source::Url(url) => url,
            Source::Path(_) => return Err(anyhow!("git getter expects a URL")),
        };

        if url.scheme() != Self::SCHEME {
            return Err(anyhow!("unsupported scheme {}", url.scheme()));
        }

        if dest.exists() {
            return Err(anyhow!("destination already exists"));
        }

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
        }

        let status = Command::new("git")
            .arg("clone")
            .arg("--depth")
            .arg("1")
            .arg(url.as_str())
            .arg(dest)
            .status()
            .with_context(|| format!("spawn git clone for {}", url))?;

        if !status.success() {
            return Err(anyhow!("git clone failed with status {}", status));
        }

        Ok(dest.to_path_buf())
    }
}
