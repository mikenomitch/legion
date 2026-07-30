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
    let dylib_name = format!(
        "{}{}{}",
        std::env::consts::DLL_PREFIX,
        name,
        std::env::consts::DLL_SUFFIX
    );
    // Two build layouts produce the dylib:
    //  - `cargo build` inside the plugin dir (and deployed bundles):
    //    <root>/<kind>/<name>/target/release/
    //  - the workspace build with LEGION_BUILD_PLUGINS=1: build.rs
    //    redirects CARGO_TARGET_DIR so per-plugin target/ dirs don't
    //    litter the tree, landing artifacts in
    //    <root>/../target/plugin-build/<kind>/<name>/release/
    // Prefer the in-tree path; fall back to the workspace layout when
    // only it exists. A missing plugin reports the in-tree path (the
    // canonical one) in its error.
    let in_tree = root.join("target").join("release").join(&dylib_name);
    let dylib = if in_tree.exists() {
        in_tree
    } else {
        let workspace = root_dir
            .parent()
            .unwrap_or(root_dir)
            .join("target")
            .join("plugin-build")
            .join(kind)
            .join(name)
            .join("release")
            .join(&dylib_name);
        if workspace.exists() {
            workspace
        } else {
            in_tree
        }
    };

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
    fn plugin_paths_prefer_in_tree_and_fall_back_to_workspace_layout() -> R<()> {
        let base = temp_dir();
        let plugin_root = base.join("plugins");
        let dylib_name = format!(
            "{}noop{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        );

        // Neither layout present: the canonical in-tree path is reported.
        std::fs::create_dir_all(plugin_root.join("tasks").join("noop"))?;
        let paths = plugin_paths("tasks", "noop", &plugin_root);
        assert!(paths
            .dylib
            .ends_with(Path::new("plugins/tasks/noop/target/release").join(&dylib_name)));

        // Only the workspace (LEGION_BUILD_PLUGINS) layout present: found.
        let workspace = base
            .join("target")
            .join("plugin-build")
            .join("tasks")
            .join("noop")
            .join("release");
        std::fs::create_dir_all(&workspace)?;
        std::fs::write(workspace.join(&dylib_name), b"")?;
        let paths = plugin_paths("tasks", "noop", &plugin_root);
        assert_eq!(paths.dylib, workspace.join(&dylib_name));

        // In-tree appears (manual `cargo build` in the plugin dir): wins.
        let in_tree = plugin_root
            .join("tasks")
            .join("noop")
            .join("target")
            .join("release");
        std::fs::create_dir_all(&in_tree)?;
        std::fs::write(in_tree.join(&dylib_name), b"")?;
        let paths = plugin_paths("tasks", "noop", &plugin_root);
        assert_eq!(paths.dylib, in_tree.join(&dylib_name));

        std::fs::remove_dir_all(base)?;
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
