use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::path::PathBuf;

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

pub fn plugin_root_dir() -> PathBuf {
    std::env::var("LEGION_PLUGIN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("plugins");
            path
        })
}
