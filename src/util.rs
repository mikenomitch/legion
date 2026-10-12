use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::path::{Path, PathBuf};

// Adds convenience for Result for brevity
pub type R<T> = anyhow::Result<T>;

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Restrict `dir` so only its owner can enter it (`0o700`; no-op off Unix).
///
/// Worker-created allocation, task, and log directories use this as the
/// on-disk isolation boundary between workloads sharing a node: left to the
/// process umask (commonly producing `0755`) any local process — including
/// other tasks — could read staged artifacts, injected secrets, and captured
/// task output. Task runtimes that drop to a dedicated `user` hand the
/// allocation tree over via chown so the boundary holds for them too.
pub fn restrict_dir_to_owner(dir: &std::path::Path) -> crate::util::R<()> {
    #[cfg(unix)]
    {
        use anyhow::Context;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to restrict {} to owner-only", dir.display()))?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// First match for `name` on `PATH`. A name containing a path separator is
/// treated as an explicit path and only checked for existence.
///
/// The single lookup shared by everything that shells out to discovered
/// binaries (VMM monitors, `mkfs.*`, `mount`), so availability probes and the
/// actual invocation can never disagree about what was found.
pub fn find_on_path(name: &str) -> Option<PathBuf> {
    if name.contains(std::path::MAIN_SEPARATOR) {
        let path = PathBuf::from(name);
        return path.exists().then_some(path);
    }
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// Root of the dynamically loaded plugins (`<root>/<kind>/<name>/`, see
/// docs/plugin-system.md).
///
/// `LEGION_PLUGIN_DIR` when set. Otherwise the first of these that exists:
///
/// 1. `<dir of the legion binary>/plugins` — a bundle unpacked next to the
///    binary;
/// 2. `<dir of the legion binary>/../lib/legion/plugins` — the FHS layout of
///    an installed binary (`/usr/local/bin/legion` →
///    `/usr/local/lib/legion/plugins`);
/// 3. the source checkout the binary was built from — `cargo run` and
///    `cargo test`, whose binaries live under `target/`.
///
/// When none exists the first candidate is returned. The build checkout is
/// deliberately last and only used when it is really there: a binary built
/// in a container at `/src` and installed elsewhere used to look for
/// plugins under `/src/plugins` on every host it ran on.
pub fn plugin_root_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("LEGION_PLUGIN_DIR") {
        return PathBuf::from(dir);
    }
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    default_plugin_root(exe_dir.as_deref(), Path::new(env!("CARGO_MANIFEST_DIR")))
}

/// [`plugin_root_dir`]'s search, without the environment: `exe_dir` is the
/// directory holding the running binary, `build_dir` the checkout it was
/// built from.
fn default_plugin_root(exe_dir: Option<&Path>, build_dir: &Path) -> PathBuf {
    let mut candidates = Vec::new();
    if let Some(exe_dir) = exe_dir {
        candidates.push(exe_dir.join("plugins"));
        candidates.push(
            exe_dir
                .join("..")
                .join("lib")
                .join("legion")
                .join("plugins"),
        );
    }
    candidates.push(build_dir.join("plugins"));
    candidates
        .iter()
        .find(|candidate| candidate.is_dir())
        .or_else(|| candidates.first())
        .cloned()
        .unwrap_or_else(|| build_dir.join("plugins"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_root_prefers_a_bundle_next_to_the_binary_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("bin");
        let build = root.path().join("src");
        std::fs::create_dir_all(bin.join("plugins")).expect("bundle dir");
        std::fs::create_dir_all(build.join("plugins")).expect("checkout dir");
        assert_eq!(default_plugin_root(Some(&bin), &build), bin.join("plugins"));
    }

    #[test]
    fn plugin_root_finds_the_installed_lib_layout_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("usr/local/bin");
        let lib = root.path().join("usr/local/lib/legion/plugins");
        std::fs::create_dir_all(&bin).expect("bin dir");
        std::fs::create_dir_all(&lib).expect("lib dir");
        let found = default_plugin_root(Some(&bin), &root.path().join("nowhere"));
        assert_eq!(
            found.canonicalize().expect("exists"),
            lib.canonicalize().expect("exists")
        );
    }

    /// The finding this guards: a binary built at `/src` and installed on
    /// another host must not look for plugins in a checkout that is not
    /// there.
    #[test]
    fn plugin_root_ignores_a_missing_build_checkout_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("bin");
        std::fs::create_dir_all(&bin).expect("bin dir");
        let missing_checkout = root.path().join("src");
        assert_eq!(
            default_plugin_root(Some(&bin), &missing_checkout),
            bin.join("plugins")
        );
    }

    #[test]
    fn plugin_root_uses_the_checkout_for_cargo_builds_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("target/debug");
        let build = root.path().to_path_buf();
        std::fs::create_dir_all(&bin).expect("target dir");
        std::fs::create_dir_all(build.join("plugins")).expect("checkout dir");
        assert_eq!(
            default_plugin_root(Some(&bin), &build),
            build.join("plugins")
        );
    }
}

/// Wall-clock now, in unix milliseconds.
pub fn now_unix_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
