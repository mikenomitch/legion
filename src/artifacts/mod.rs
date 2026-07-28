//! The artifact system: one fetch/verify/unpack/stage pipeline shared by
//! every task runtime (see `internal_docs/plans/artifact-system-plan.md` and the
//! user-facing `docs/artifacts.md`).
//!
//! Layout mirrors the plan:
//! - [`fetcher`] — the [`fetcher::ArtifactFetcher`] trait + scheme registry.
//! - [`fetchers`] — built-in `http(s)`, `file`, `git`, `s3`, `oci` fetchers.
//! - [`auth`] — cluster auth matchers and effective-auth normalization.
//! - [`stage`] — the staging pipeline (sink, verify, unpack, atomic move).
//! - [`status`] — per-artifact state reporting through heartbeats.
//!
//! Spec types ([`crate::structs::ArtifactSpec`] and friends) live in
//! `crate::structs::artifact` so job/store/proto plumbing stays in `structs`.

use std::path::{Path, PathBuf};

use crate::prelude::*;
use crate::structs::parse_artifact_source;

pub mod auth;
pub mod fetcher;
pub mod fetchers;
pub mod stage;
pub mod status;

pub use fetcher::{ArtifactFetcher, Credential, FetchLimits, FetchRequest, FetcherRegistry};
pub use stage::{AnonymousCredentials, ArtifactSink, ArtifactStager, CredentialSource};
pub use status::{ArtifactState, ArtifactStatus, ArtifactStatusBoard};

/// What staging produced for one artifact, attached to `TaskInstance` before
/// `prepare()` so plugins consume typed payloads instead of guessing paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedArtifact {
    pub name: Option<String>,
    /// Content digest when the payload has one (single files, OCI manifests).
    pub digest: Option<String>,
    /// Bytes fetched from origin for this artifact.
    pub bytes: u64,
    pub payload: StagedPayload,
}

/// Typed staged payloads (artifact system plan §8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagedPayload {
    /// A single file at `path` (mode applied). The common exec case.
    File { path: PathBuf },
    /// An unpacked tree rooted at `dir`.
    Tree { dir: PathBuf },
    /// An OCI image as an on-disk OCI layout. Image-consuming runtimes turn
    /// this into whatever their runtime wants.
    Image {
        oci_layout: PathBuf,
        reference: String,
    },
    /// mediaType-selected ORAS layers staged into `dir`.
    Layers {
        dir: PathBuf,
        files: Vec<PathBuf>,
        media_type: String,
    },
}

impl StagedArtifact {
    /// The path `${artifact.<name>}` and `LEGION_ARTIFACT_<NAME>` expand to:
    /// the file for single files, the directory for everything else.
    pub fn reference_path(&self) -> &Path {
        match &self.payload {
            StagedPayload::File { path } => path,
            StagedPayload::Tree { dir } => dir,
            StagedPayload::Image { oci_layout, .. } => oci_layout,
            StagedPayload::Layers { dir, .. } => dir,
        }
    }
}

/// Fetches a source into `dest_dir` with the built-in fetcher registry, no
/// auth, and default limits. This is the CLI jobspec-fetch path (`job run
/// https://…`), which replaces the old `getter_rs` client — one fetch stack
/// in the tree.
pub fn fetch_source_to_dir(source: &str, dest_dir: &Path) -> R<PathBuf> {
    // Bare paths (no scheme) are local files; mirror the old getter behavior.
    let normalized = if source.contains("://") || source.starts_with("git::") {
        source.to_string()
    } else {
        return copy_local_path(Path::new(source), dest_dir);
    };
    let parsed = parse_artifact_source(&normalized)?;
    if parsed.scheme == "file" {
        let path = parsed
            .url
            .to_file_path()
            .map_err(|_| anyhow!("invalid file URL {source}"))?;
        return copy_local_path(&path, dest_dir);
    }

    let fetcher = FetcherRegistry::with_builtins().fetcher(&parsed.scheme)?;
    let limits = FetchLimits::default();
    let redacted = crate::structs::redact_source(&normalized);
    let request = FetchRequest {
        scheme: &parsed.scheme,
        url: &parsed.url,
        redacted_source: &redacted,
        checksum: None,
        media_type: None,
        credential: None,
        limits: &limits,
    };
    let mut sink = ArtifactSink::new(dest_dir.join(".fetch"), limits.max_bytes)?;
    let fetched = fetcher.fetch(&request, &mut sink)?;
    let result = match fetched.payload {
        fetcher::FetchedPayload::File { path, .. } => {
            let file_name = path.file_name().context("fetched file has no name")?;
            let target = dest_dir.join(file_name);
            std::fs::rename(&path, &target)?;
            target
        }
        fetcher::FetchedPayload::Tree { dir } => {
            let target = dest_dir.join("source");
            std::fs::rename(&dir, &target)?;
            target
        }
        other => bail!("source {redacted} produced an unsupported payload {other:?}"),
    };
    let _ = std::fs::remove_dir_all(dest_dir.join(".fetch"));
    Ok(result)
}

fn copy_local_path(path: &Path, dest_dir: &Path) -> R<PathBuf> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("read metadata for {}", path.display()))?;
    if metadata.is_dir() {
        let target = dest_dir.join(path.file_name().unwrap_or("source".as_ref()));
        copy_dir_recursive(path, &target)?;
        return Ok(target);
    }
    let file_name = path.file_name().context("source file has no name")?;
    let target = dest_dir.join(file_name);
    std::fs::create_dir_all(dest_dir)?;
    std::fs::copy(path, &target)
        .with_context(|| format!("copy {} to {}", path.display(), target.display()))?;
    Ok(target)
}

pub(crate) fn copy_dir_recursive(source: &Path, dest: &Path) -> R<()> {
    std::fs::create_dir_all(dest)?;
    for entry in
        std::fs::read_dir(source).with_context(|| format!("read directory {}", source.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let target = dest.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&path, &target)?;
        } else {
            std::fs::copy(&path, &target)
                .with_context(|| format!("copy {} to {}", path.display(), target.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn fetch_source_to_dir_handles_bare_paths() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("spec.jsonnet");
        fs::write(&file, "{}")?;

        let dest = tempdir()?;
        let fetched = fetch_source_to_dir(file.to_str().unwrap(), dest.path())?;
        assert_eq!(fs::read_to_string(fetched)?, "{}");
        Ok(())
    }

    #[test]
    fn fetch_source_to_dir_handles_file_urls() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("spec.jsonnet");
        fs::write(&file, "{ name: 'x' }")?;

        let dest = tempdir()?;
        let url = format!("file://{}", file.display());
        let fetched = fetch_source_to_dir(&url, dest.path())?;
        assert_eq!(fs::read_to_string(fetched)?, "{ name: 'x' }");
        Ok(())
    }

    #[test]
    fn staged_artifact_reference_paths() {
        let file = StagedArtifact {
            name: Some("tool".into()),
            digest: None,
            bytes: 0,
            payload: StagedPayload::File {
                path: PathBuf::from("/task/tool"),
            },
        };
        assert_eq!(file.reference_path(), Path::new("/task/tool"));

        let layers = StagedArtifact {
            name: Some("model".into()),
            digest: None,
            bytes: 0,
            payload: StagedPayload::Layers {
                dir: PathBuf::from("/task/models"),
                files: vec![PathBuf::from("/task/models/w.bin")],
                media_type: "application/x".into(),
            },
        };
        assert_eq!(layers.reference_path(), Path::new("/task/models"));
    }
}
