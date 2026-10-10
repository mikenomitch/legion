use crate::prelude::*;
use libloading::Library;
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub const PLUGIN_API_VERSION: u32 = 2;

// Defines `PLUGIN_BUILD_INFO`: rustc version and commit, this crate's name
// and version, target triple, and panic strategy (see build.rs).
include!(concat!(env!("OUT_DIR"), "/plugin_build_info.rs"));

/// Identity of the build a plugin or host was compiled in.
///
/// Plugins are Rust dylibs whose `get()` returns `Box<dyn Trait>`, and Rust
/// has no stable ABI: a plugin built by another rustc, against another legion
/// version, or with a dependency whose layout differs (a different
/// `serde_json` version, or `serde_json/preserve_order` switched on by some
/// other crate in the plugin's build) loads fine and then corrupts memory.
/// The loader therefore refuses any plugin whose fingerprint differs from the
/// host's. The fingerprint hashes [`PLUGIN_BUILD_INFO`] plus the size and
/// alignment of the types that cross the plugin boundary, which is what
/// catches dependency drift the build info can't see. The build profile is
/// not part of it, so a debug host can load release plugins.
///
/// This is a guard, not a stable ABI: two builds with equal fingerprints are
/// trusted to agree on layout, which holds for the same compiler, crate
/// versions, and features.
pub const PLUGIN_ABI_FINGERPRINT: u64 = {
    use std::mem::{align_of, size_of};
    let mut hash = fnv1a(FNV_OFFSET, PLUGIN_BUILD_INFO.to_bytes());
    let layouts = [
        (size_of::<Value>(), align_of::<Value>()),
        (
            size_of::<Map<String, Value>>(),
            align_of::<Map<String, Value>>(),
        ),
        (size_of::<anyhow::Error>(), align_of::<anyhow::Error>()),
        (size_of::<String>(), align_of::<String>()),
        (
            size_of::<std::time::Duration>(),
            align_of::<std::time::Duration>(),
        ),
        (
            size_of::<crate::structs::Allocation>(),
            align_of::<crate::structs::Allocation>(),
        ),
        (
            size_of::<crate::structs::TaskConfig>(),
            align_of::<crate::structs::TaskConfig>(),
        ),
        (
            size_of::<crate::structs::JobConfig>(),
            align_of::<crate::structs::JobConfig>(),
        ),
        (
            size_of::<crate::task_plugins::TaskInstance>(),
            align_of::<crate::task_plugins::TaskInstance>(),
        ),
        (
            size_of::<crate::task_plugins::StopContext>(),
            align_of::<crate::task_plugins::StopContext>(),
        ),
    ];
    let mut i = 0;
    while i < layouts.len() {
        hash = fnv1a(hash, &(layouts[i].0 as u64).to_le_bytes());
        hash = fnv1a(hash, &(layouts[i].1 as u64).to_le_bytes());
        i += 1;
    }
    hash
};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

const fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        i += 1;
    }
    hash
}

/// Symbols [`declare_plugin_abi!`] exports. Both are plain C ABI, so reading
/// them is safe even from a plugin whose Rust ABI is incompatible.
const PLUGIN_ABI_FINGERPRINT_SYMBOL: &[u8] = b"legion_plugin_abi_fingerprint";
const PLUGIN_ABI_BUILD_INFO_SYMBOL: &[u8] = b"legion_plugin_abi_build_info";

/// Export this plugin's build fingerprint so the host can refuse a plugin
/// built by a different toolchain or legion version. Every dylib plugin must
/// invoke it once, next to its `get` constructor. Task plugins get it from
/// [`crate::declare_task_plugin_abi!`] and don't call it directly.
///
/// ```ignore
/// legion::declare_plugin_abi!();
///
/// #[no_mangle]
/// pub extern "C" fn get() -> Box<dyn SchedulingPolicy> { Box::new(MyPolicy) }
/// ```
#[macro_export]
macro_rules! declare_plugin_abi {
    () => {
        #[no_mangle]
        pub extern "C" fn legion_plugin_abi_fingerprint() -> u64 {
            $crate::plugin_runtime::PLUGIN_ABI_FINGERPRINT
        }

        #[no_mangle]
        pub extern "C" fn legion_plugin_abi_build_info() -> *const ::std::os::raw::c_char {
            $crate::plugin_runtime::PLUGIN_BUILD_INFO.as_ptr()
        }
    };
}

/// Opens a plugin dylib and refuses it unless its ABI fingerprint matches
/// this binary's (see [`PLUGIN_ABI_FINGERPRINT`]). Every dylib loader goes
/// through here before it touches a Rust-ABI symbol such as `get`.
///
/// # Safety
///
/// Loading a library runs its initializers. The caller must trust the file,
/// the same contract as `libloading::Library::new`.
pub unsafe fn open_plugin_library(kind: &str, name: &str, path: &Path) -> R<Library> {
    let lib = Library::new(path)
        .with_context(|| format!("failed opening {kind} plugin {name} at {}", path.display()))?;
    let fingerprint = lib
        .get::<unsafe extern "C" fn() -> u64>(PLUGIN_ABI_FINGERPRINT_SYMBOL)
        .ok()
        .map(|symbol| symbol());
    let build_info = lib
        .get::<unsafe extern "C" fn() -> *const std::os::raw::c_char>(PLUGIN_ABI_BUILD_INFO_SYMBOL)
        .ok()
        .map(|symbol| symbol())
        .filter(|ptr| !ptr.is_null())
        .map(|ptr| std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned());
    check_plugin_fingerprint(kind, name, fingerprint, build_info.as_deref())?;
    Ok(lib)
}

/// The comparison behind [`open_plugin_library`], split out for tests.
fn check_plugin_fingerprint(
    kind: &str,
    name: &str,
    fingerprint: Option<u64>,
    build_info: Option<&str>,
) -> R<()> {
    let host_info = PLUGIN_BUILD_INFO.to_string_lossy();
    match fingerprint {
        Some(found) if found == PLUGIN_ABI_FINGERPRINT => Ok(()),
        Some(found) => bail!(
            "{kind} plugin {name} was built differently from this legion binary and cannot be \
             loaded safely: Rust plugins have no stable ABI. Plugin: {} (fingerprint \
             {found:016x}). Host: {host_info} (fingerprint {PLUGIN_ABI_FINGERPRINT:016x}). Rebuild \
             the plugin with the same rustc against this legion version, from a lockfile that \
             matches the host's (see docs/plugin-system.md)",
            build_info.unwrap_or("unknown build")
        ),
        None => bail!(
            "{kind} plugin {name} exports no ABI fingerprint, so it predates fingerprint checks \
             and cannot be loaded safely. Add `legion::declare_plugin_abi!();` (task plugins: \
             `legion::declare_task_plugin_abi!();`) next to its `get` constructor and rebuild \
             it against this legion version (see docs/plugin-system.md)"
        ),
    }
}

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
    fn plugin_fingerprint_check_accepts_only_the_host_build_test() {
        assert!(
            check_plugin_fingerprint("tasks", "noop", Some(PLUGIN_ABI_FINGERPRINT), None).is_ok()
        );

        let err = check_plugin_fingerprint(
            "rollouts",
            "slow_start",
            Some(PLUGIN_ABI_FINGERPRINT ^ 1),
            Some("rustc 1.0.0 (old)"),
        )
        .expect_err("a different build must be refused")
        .to_string();
        assert!(err.contains("rollouts plugin slow_start"), "{err}");
        assert!(err.contains("rustc 1.0.0 (old)"), "{err}");
        assert!(err.contains("Rebuild"), "{err}");

        let err = check_plugin_fingerprint("scheduling", "random", None, None)
            .expect_err("a plugin without a fingerprint must be refused")
            .to_string();
        assert!(err.contains("declare_plugin_abi"), "{err}");
    }

    #[test]
    fn plugin_build_info_names_the_compiler_and_crate_test() {
        let info = PLUGIN_BUILD_INFO.to_string_lossy();
        assert!(info.starts_with("rustc "), "{info}");
        assert!(info.contains(env!("CARGO_PKG_VERSION")), "{info}");
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
