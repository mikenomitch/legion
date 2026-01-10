use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tracing::instrument;
use url::Url;

mod getters;

pub use getters::{FileGetter, Getter, GitGetter, HttpGetter};

#[derive(Debug, Clone)]
pub enum Source {
    Path(PathBuf),
    Url(Url),
}

impl Source {
    pub fn parse(input: &str) -> Result<Self> {
        if input.contains("://") {
            let url = Url::parse(input).context("parse source URL")?;
            if url.scheme() == "file" {
                return Ok(Source::Path(
                    url.to_file_path()
                        .map_err(|_| anyhow!("invalid file URL"))?,
                ));
            }
            return Ok(Source::Url(url));
        }

        Ok(Source::Path(PathBuf::from(input)))
    }

    pub fn display_name(&self) -> String {
        match self {
            Source::Path(path) => path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "source".to_string()),
            Source::Url(url) => url
                .path_segments()
                .and_then(|mut segments| segments.next_back())
                .filter(|segment| !segment.is_empty())
                .unwrap_or("download")
                .to_string(),
        }
    }
}

pub struct Client {
    getters: HashMap<String, Arc<dyn Getter>>,
}

impl Default for Client {
    fn default() -> Self {
        let mut client = Client::new();
        client.register(FileGetter::new());
        client.register(HttpGetter::new());
        client.register(GitGetter::new());
        client
    }
}

impl Client {
    pub fn new() -> Self {
        Self {
            getters: HashMap::new(),
        }
    }

    pub fn register<G>(&mut self, getter: G)
    where
        G: Getter + 'static,
    {
        self.getters
            .insert(getter.scheme().to_string(), Arc::new(getter));
    }

    #[instrument(skip(self))]
    pub fn get(&self, source: &str, dest: &Path) -> Result<PathBuf> {
        let source = Source::parse(source)?;
        let getter = match &source {
            Source::Path(_) => self
                .getters
                .get(FileGetter::SCHEME)
                .context("file getter not registered")?,
            Source::Url(url) => self
                .getters
                .get(url.scheme())
                .or_else(|| {
                    if url.scheme() == HttpGetter::HTTPS_SCHEME {
                        self.getters.get(HttpGetter::HTTP_SCHEME)
                    } else {
                        None
                    }
                })
                .with_context(|| format!("no getter registered for scheme {}", url.scheme()))?,
        };

        getter.get(&source, dest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_file_source_without_scheme() {
        let source = Source::parse("/tmp/example").unwrap();
        match source {
            Source::Path(path) => assert_eq!(path, PathBuf::from("/tmp/example")),
            Source::Url(_) => panic!("expected path"),
        }
    }

    #[test]
    fn parse_file_source_with_scheme() {
        let source = Source::parse("file:///tmp/example").unwrap();
        match source {
            Source::Path(path) => assert_eq!(path, PathBuf::from("/tmp/example")),
            Source::Url(_) => panic!("expected path"),
        }
    }
}
