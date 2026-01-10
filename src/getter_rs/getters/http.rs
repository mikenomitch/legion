use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;

use crate::getter_rs::getters::Getter;
use crate::getter_rs::Source;

#[derive(Debug, Default)]
pub struct HttpGetter {
    client: Client,
}

impl HttpGetter {
    pub const HTTP_SCHEME: &'static str = "http";
    pub const HTTPS_SCHEME: &'static str = "https";

    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

impl Getter for HttpGetter {
    fn scheme(&self) -> &'static str {
        Self::HTTP_SCHEME
    }

    fn get(&self, source: &Source, dest: &Path) -> Result<PathBuf> {
        let url = match source {
            Source::Url(url) => url,
            Source::Path(_) => return Err(anyhow!("http getter expects a URL")),
        };

        if url.scheme() != Self::HTTP_SCHEME && url.scheme() != Self::HTTPS_SCHEME {
            return Err(anyhow!("unsupported scheme {}", url.scheme()));
        }

        let dest_path = prepare_http_destination(dest, source);
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
        }

        let mut response = self
            .client
            .get(url.clone())
            .send()
            .with_context(|| format!("download {}", url))?
            .error_for_status()
            .with_context(|| format!("request {} returned error", url))?;

        let mut file = File::create(&dest_path)
            .with_context(|| format!("create file {}", dest_path.display()))?;
        let mut bytes = vec![];
        response
            .copy_to(&mut bytes)
            .with_context(|| format!("read response body from {}", url))?;
        file.write_all(&bytes)
            .with_context(|| format!("write {} bytes to {}", bytes.len(), dest_path.display()))?;

        Ok(dest_path)
    }
}

fn prepare_http_destination(dest: &Path, source: &Source) -> PathBuf {
    if dest.exists() && dest.is_dir() {
        return dest.join(source.display_name());
    }

    dest.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn http_destination_uses_basename_for_directory() {
        let dest_dir = tempfile::tempdir().unwrap();
        let url = Url::parse("https://example.com/archive.zip").unwrap();
        let dest = prepare_http_destination(dest_dir.path(), &Source::Url(url));

        assert_eq!(dest, dest_dir.path().join("archive.zip"));
    }
}
