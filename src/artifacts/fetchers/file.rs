//! `file://` fetcher: copies local files or directory trees into the sink.
//! Useful for dev workflows and node-local pre-provisioned artifacts.

use std::io::Read;
use std::path::Path;

use crate::artifacts::fetcher::{ArtifactFetcher, FetchRequest, FetchedArtifact, FetchedPayload};
use crate::artifacts::stage::ArtifactSink;
use crate::prelude::*;

#[derive(Debug, Default)]
pub struct FileFetcher;

impl FileFetcher {
    pub fn new() -> Self {
        Self
    }
}

impl ArtifactFetcher for FileFetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["file"]
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let source_path = req
            .url
            .to_file_path()
            .map_err(|_| anyhow!("invalid file URL {}", req.redacted_source))?;
        let metadata = std::fs::metadata(&source_path)
            .with_context(|| format!("read metadata for {}", req.redacted_source))?;

        if metadata.is_dir() {
            let tree_dir = sink.dir().join("tree");
            crate::artifacts::copy_dir_recursive(&source_path, &tree_dir)?;
            sink.record_external_bytes(dir_size(&tree_dir)?)?;
            return Ok(FetchedArtifact {
                bytes: sink.bytes_written(),
                payload: FetchedPayload::Tree { dir: tree_dir },
            });
        }

        let file_name = source_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "artifact".to_string());
        sink.set_bytes_total(Some(metadata.len()));
        let mut writer = sink.file_writer(&file_name)?;
        let mut reader = std::fs::File::open(&source_path)
            .with_context(|| format!("open {}", req.redacted_source))?;
        copy_with_deadline(&mut reader, &mut writer, req)?;
        let (path, digest, bytes) = writer.finish()?;
        Ok(FetchedArtifact {
            bytes,
            payload: FetchedPayload::File { path, digest },
        })
    }
}

/// Streams a reader into a sink writer, checking the fetch deadline between
/// chunks. Shared by the file and http fetchers.
pub(crate) fn copy_with_deadline<RD: Read, W: std::io::Write>(
    reader: &mut RD,
    writer: &mut W,
    req: &FetchRequest<'_>,
) -> R<u64> {
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        req.limits.check_deadline(req.redacted_source)?;
        let read = reader
            .read(&mut buf)
            .with_context(|| format!("read from {}", req.redacted_source))?;
        if read == 0 {
            return Ok(total);
        }
        writer
            .write_all(&buf[..read])
            .with_context(|| format!("write fetched bytes for {}", req.redacted_source))?;
        total += read as u64;
    }
}

fn dir_size(dir: &Path) -> R<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::fetcher::FetchLimits;
    use crate::structs::parse_artifact_source;
    use std::fs;
    use tempfile::tempdir;

    fn request<'a>(
        parsed: &'a crate::structs::ParsedSource,
        limits: &'a FetchLimits,
        redacted: &'a str,
    ) -> FetchRequest<'a> {
        FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: redacted,
            checksum: None,
            media_type: None,
            credential: None,
            limits,
        }
    }

    #[test]
    fn copies_single_file_and_hashes() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("tool.bin");
        fs::write(&file, b"hello artifact")?;

        let staging = tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().join("sink"), 1024)?;
        let source = format!("file://{}", file.display());
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits::default();
        let fetched = FileFetcher::new().fetch(&request(&parsed, &limits, &source), &mut sink)?;

        match fetched.payload {
            FetchedPayload::File { path, digest } => {
                assert_eq!(fs::read(path)?, b"hello artifact");
                // sha256 of "hello artifact"
                assert!(digest.starts_with("sha256:"), "{digest}");
            }
            other => panic!("expected file payload, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn copies_directory_as_tree() -> R<()> {
        let src = tempdir()?;
        fs::create_dir_all(src.path().join("nested"))?;
        fs::write(src.path().join("nested/inner.txt"), "x")?;

        let staging = tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().join("sink"), 1024)?;
        let source = format!("file://{}", src.path().display());
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits::default();
        let fetched = FileFetcher::new().fetch(&request(&parsed, &limits, &source), &mut sink)?;

        match fetched.payload {
            FetchedPayload::Tree { dir } => {
                assert_eq!(fs::read_to_string(dir.join("nested/inner.txt"))?, "x");
            }
            other => panic!("expected tree payload, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn size_limit_enforced() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("big.bin");
        fs::write(&file, vec![0u8; 4096])?;

        let staging = tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().join("sink"), 100)?;
        let source = format!("file://{}", file.display());
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits {
            max_bytes: 100,
            deadline: None,
        };
        let err = FileFetcher::new()
            .fetch(&request(&parsed, &limits, &source), &mut sink)
            .unwrap_err();
        assert!(err.to_string().contains("write fetched bytes"), "{err:#}");
        Ok(())
    }
}
