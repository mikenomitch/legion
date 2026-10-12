//! Shared OCI image sourcing for image-backed task runtimes (`container`,
//! `sandbox`).
//!
//! Both runtimes need the same thing before they can launch anything: the image's
//! on-disk **OCI layout**. It can arrive three ways, in priority order:
//!
//! 1. staged through the artifact pipeline (`artifacts.containerImageDesugar`),
//!    recorded on the instance by the instance's `consume_artifact`;
//! 2. already pulled by an earlier start of the same allocation (in-place
//!    restarts must not refetch);
//! 3. pulled anonymously through the shared `oci://` fetcher, which serves
//!    every blob the node content store already holds (from any earlier pull
//!    on this node) and keeps the ones it downloads.
//!
//! Keeping the resolution here means a new image-backed kind gets the whole
//! ladder — artifact pipeline included — for one function call.

use crate::prelude::*;
use crate::task_plugins::TaskInstance;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The OCI layout recorded on `instance.state` by `consume_artifact` when the
/// image was staged through the artifact pipeline.
pub(crate) fn staged_layout(instance: &TaskInstance) -> Option<PathBuf> {
    instance
        .state
        .get("artifactImage")
        .and_then(|image| image.get("ociLayout"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
}

/// The reference of the staged image, recorded alongside its layout. This is
/// the *pinned* reference the artifact pipeline resolved and imported, so
/// launching it (instead of the jobspec's tag) is what makes digest pinning
/// mean anything: a tag can move between the pin and the run.
pub(crate) fn staged_reference(instance: &TaskInstance) -> Option<String> {
    instance
        .state
        .get("artifactImage")
        .and_then(|image| image.get("reference"))
        .and_then(Value::as_str)
        .filter(|reference| !reference.is_empty())
        .map(str::to_string)
}

/// Records a staged image layout on the instance so [`staged_layout`] (and
/// therefore [`resolve_layout`]) finds it at start.
pub(crate) fn record_staged_layout(
    instance: &mut TaskInstance,
    oci_layout: &Path,
    reference: &str,
    digest: Option<&str>,
) {
    let record = json!({
        "ociLayout": oci_layout.display().to_string(),
        "reference": reference,
        "digest": digest,
    });
    if let Some(state) = instance.state.as_object_mut() {
        state.insert("artifactImage".into(), record);
    } else {
        instance.state = json!({ "artifactImage": record });
    }
}

/// Pulls `image` into an on-disk OCI layout via the shared `oci://` fetcher
/// (anonymous), returning the layout root. Used when no image artifact was
/// staged for the task — neither a low-level OCI instance nor a microVM can
/// pull an image reference itself.
///
/// Goes through the worker's node content store when it has one, so a
/// second allocation of the same image on this node downloads nothing (a
/// tag costs one `HEAD` request to check it has not moved).
pub(crate) fn pull_layout(image: &str, dest_root: &Path) -> R<PathBuf> {
    use crate::artifacts::fetcher::{ArtifactFetcher, FetchLimits, FetchRequest, FetchedPayload};
    use crate::artifacts::fetchers::OciFetcher;
    use crate::artifacts::stage::ArtifactSink;
    use crate::structs::{parse_artifact_source, ArtifactSpec};

    let source = ArtifactSpec::oci_image(image).source;
    let parsed = parse_artifact_source(&source)?;
    let limits = FetchLimits::default();
    let request = FetchRequest {
        scheme: &parsed.scheme,
        url: &parsed.url,
        redacted_source: &source,
        checksum: None,
        media_type: None,
        credential: None,
        limits: &limits,
    };

    std::fs::create_dir_all(dest_root)
        .with_context(|| format!("create image dir {}", dest_root.display()))?;
    let mut sink = ArtifactSink::new(dest_root.to_path_buf(), limits.max_bytes)?;
    // Layers already on this node (any earlier pull, staged or not) are
    // linked from the node content store instead of downloaded again.
    sink.set_content_store(crate::content_store::node_store());
    let fetched = OciFetcher::new()
        .fetch(&request, &mut sink)
        .with_context(|| format!("pull container image {image}"))?;
    match fetched.payload {
        FetchedPayload::Image { oci_layout, .. } => Ok(oci_layout),
        other => bail!("expected an OCI image payload for {image}, got {other:?}"),
    }
}

/// Resolves the OCI layout for `image` under an allocation's `scratch` dir,
/// walking the ladder documented at the module level. `scratch/image/layout`
/// is where a previous pull for this allocation landed; `index.json` is
/// written last by the fetcher, so its presence marks a complete pull.
pub(crate) fn resolve_layout(image: &str, instance: &TaskInstance, scratch: &Path) -> R<PathBuf> {
    if let Some(layout) = staged_layout(instance) {
        return Ok(layout);
    }
    let image_root = scratch.join("image");
    let pulled = image_root.join("layout");
    if pulled.join("index.json").is_file() {
        return Ok(pulled);
    }
    pull_layout(image, &image_root)
}

/// Imports a staged OCI layout into Apple `container` so a subsequent run
/// finds the image without a registry pull (tar the layout → `image load`).
pub(crate) fn apple_load_image(oci_layout: &Path, reference: &str) -> R<()> {
    let archive_path = oci_layout.with_extension("oci.tar");
    let archive = std::fs::File::create(&archive_path)
        .with_context(|| format!("create image archive {}", archive_path.display()))?;
    let mut builder = tar::Builder::new(archive);
    builder
        .append_dir_all(".", oci_layout)
        .context("archive OCI layout")?;
    builder.finish().context("finish OCI layout archive")?;

    let output = Command::new("container")
        .args(["image", "load", "--input"])
        .arg(&archive_path)
        .output()
        .context("spawn container image load")?;
    let _ = std::fs::remove_file(&archive_path);
    if !output.status.success() {
        bail!(
            "container image load failed for {reference}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_layout_reads_and_records_state() {
        let mut instance = TaskInstance::default();
        assert_eq!(staged_layout(&instance), None);

        record_staged_layout(
            &mut instance,
            Path::new("/tmp/x/layout"),
            "docker.io/library/alpine:latest",
            Some("sha256:abc"),
        );
        assert_eq!(
            staged_layout(&instance),
            Some(PathBuf::from("/tmp/x/layout"))
        );
        assert_eq!(
            instance.state["artifactImage"]["reference"],
            "docker.io/library/alpine:latest"
        );
    }

    #[test]
    fn resolve_layout_prefers_staged_then_previous_pull() -> R<()> {
        let scratch = tempfile::tempdir()?;

        // Staged wins outright.
        let mut instance = TaskInstance::default();
        record_staged_layout(&mut instance, Path::new("/staged/layout"), "alpine", None);
        assert_eq!(
            resolve_layout("alpine", &instance, scratch.path())?,
            PathBuf::from("/staged/layout")
        );

        // Without a staged layout, a complete previous pull is reused (no
        // network) — this is what makes in-place restarts cheap.
        let instance = TaskInstance::default();
        let pulled = scratch.path().join("image").join("layout");
        std::fs::create_dir_all(&pulled)?;
        std::fs::write(pulled.join("index.json"), b"{}")?;
        assert_eq!(resolve_layout("alpine", &instance, scratch.path())?, pulled);
        Ok(())
    }
}
