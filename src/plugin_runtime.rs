use crate::prelude::*;
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub const PLUGIN_API_VERSION: u32 = 2;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginManifest {
    pub kind: String,
    pub name: String,
    pub legion_api_version: u32,
}

#[derive(Debug, Clone)]
pub struct PluginPaths {
    pub root: PathBuf,
    pub dylib: PathBuf,
}

pub fn plugin_paths(kind: &str, name: &str, root_dir: &Path) -> PluginPaths {
    let root = root_dir.join(kind).join(name);
    let dylib = root.join("target").join("release").join(format!(
        "{}{}{}",
        std::env::consts::DLL_PREFIX,
        name,
        std::env::consts::DLL_SUFFIX
    ));

    PluginPaths { root, dylib }
}

#[instrument(err, skip_all)]
pub fn validate_manifest(kind: &str, name: &str, plugin_root: &Path) -> R<PluginManifest> {
    let manifest_path = plugin_root.join("plugin.json");
    let bytes = std::fs::read(&manifest_path)
        .with_context(|| format!("missing plugin manifest at {}", manifest_path.display()))?;
    let manifest: PluginManifest = serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid plugin manifest at {}", manifest_path.display()))?;

    if manifest.kind != kind {
        bail!(
            "plugin manifest kind mismatch for {}: expected {}, got {}",
            name,
            kind,
            manifest.kind
        );
    }

    if manifest.name != name {
        bail!(
            "plugin manifest name mismatch for {}: got {}",
            name,
            manifest.name
        );
    }

    if manifest.legion_api_version != PLUGIN_API_VERSION {
        bail!(
            "plugin {} has unsupported legionApiVersion {} (host supports {})",
            name,
            manifest.legion_api_version,
            PLUGIN_API_VERSION
        );
    }

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("legion-plugin-runtime-test-{unique}"))
    }

    #[test]
    fn validate_manifest_accepts_matching_manifest() -> R<()> {
        let root = temp_dir();
        std::fs::create_dir_all(&root)?;
        // Built from the constant so a version bump does not need this test
        // edited -- only the in-tree plugin.json manifests.
        std::fs::write(
            root.join("plugin.json"),
            format!(r#"{{"kind":"tasks","name":"noop","legionApiVersion":{PLUGIN_API_VERSION}}}"#),
        )?;

        let manifest = validate_manifest("tasks", "noop", &root)?;
        assert_eq!(manifest.kind, "tasks");
        assert_eq!(manifest.name, "noop");
        assert_eq!(manifest.legion_api_version, PLUGIN_API_VERSION);

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn validate_manifest_rejects_version_mismatch() -> R<()> {
        let root = temp_dir();
        std::fs::create_dir_all(&root)?;
        std::fs::write(
            root.join("plugin.json"),
            r#"{"kind":"tasks","name":"noop","legionApiVersion":99}"#,
        )?;

        let err = validate_manifest("tasks", "noop", &root).expect_err("expected mismatch");
        assert!(err.to_string().contains("unsupported legionApiVersion"));

        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
