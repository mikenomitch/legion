fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit rerun triggers replace Cargo's default of re-running on any
    // change in the package.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=LEGION_SKIP_PLUGIN_BUILD");
    println!("cargo:rerun-if-env-changed=LEGION_BUILD_PLUGINS");
    println!("cargo:rerun-if-env-changed=LEGION_PLUGIN_DIR");

    pack_relative_relocs();

    let skip_plugins = std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_ok();
    let build_plugins = std::env::var("LEGION_BUILD_PLUGINS").is_ok();
    let plugin_root = std::env::var("LEGION_PLUGIN_DIR").unwrap_or_else(|_| "plugins".into());
    if build_plugins && !skip_plugins {
        println!("cargo:rerun-if-changed={plugin_root}");
    }

    if build_plugins && !skip_plugins {
        let manifest_dir = std::path::PathBuf::from(
            std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"),
        );
        let plugin_target_root = manifest_dir.join("target").join("plugin-build");

        // For every plugin of every kind, build a release.
        std::fs::read_dir(&plugin_root)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_dir() {
                    Some(path)
                } else {
                    None
                }
            })
            .for_each(|path| match std::fs::read_dir(path) {
                Ok(dir) => dir.for_each(|entry| match entry.ok() {
                    None => {
                        println!("Error reading entry");
                    }
                    Some(entry) => {
                        let path = entry.path();
                        // Only Rust plugins build here: exec plugins (device,
                        // keyring, cluster CA) are scripts with no crate.
                        if path.is_dir() && path.join("Cargo.toml").is_file() {
                            let plugin_name = path
                                .file_name()
                                .map(|name| name.to_string_lossy().to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            let plugin_kind = path
                                .parent()
                                .and_then(|parent| parent.file_name())
                                .map(|name| name.to_string_lossy().to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            let plugin_target_dir =
                                plugin_target_root.join(plugin_kind).join(plugin_name);

                            let mut cmd = std::process::Command::new("cargo");
                            cmd.arg("build").arg("--release");
                            cmd.env("LEGION_SKIP_PLUGIN_BUILD", "1");
                            cmd.env("CARGO_TARGET_DIR", plugin_target_dir);
                            cmd.current_dir(path);
                            // A failed plugin build used to vanish here, and the
                            // first sign was a deploy with no plugin dylibs.
                            let status = cmd.spawn().unwrap().wait().unwrap();
                            if !status.success() {
                                println!(
                                    "cargo:warning=plugin {} failed to build ({status})",
                                    entry.path().display()
                                );
                            }
                        }
                    }
                }),
                Err(e) => {
                    println!("Error reading dir: {e}");
                }
            });
    } else if !skip_plugins {
        println!(
            "cargo:warning=Plugin build skipped; set LEGION_BUILD_PLUGINS=1 to build plugins."
        );
    }

    Ok(())
}

/// The first glibc whose loader reads packed relative relocations.
const RELR_MIN_GLIBC: (u32, u32) = (2, 36);

/// Links the `legion` binary with packed relative relocations (`DT_RELR`)
/// where the result is known to load.
///
/// A position-independent executable carries one 24-byte relocation for every
/// pointer in its data (vtables, string tables, statics), and the shipping
/// build has ~125,000 of them. `-z pack-relative-relocs` stores them as a
/// bitmap instead: 3.0 MB down to 50 KB, with no change to the code.
///
/// The catch is the loader. glibc reads the packed form from 2.36, and the
/// linker marks the binary as needing it (`GLIBC_ABI_DT_RELR`), so on an
/// older glibc the binary refuses to start. Hence the gate: only a native
/// linux-gnu build on a host whose own glibc is new enough gets the flag, so
/// a build never asks for a newer glibc than the machine it was built on.
/// Cross builds and musl are left alone.
///
/// `LEGION_PACK_RELATIVE_RELOCS=0` turns it off (for a binary built here and
/// copied to an older distribution); `=1` forces it on for a linux-gnu target
/// the check cannot see, such as a cross build.
fn pack_relative_relocs() {
    println!("cargo:rerun-if-env-changed=LEGION_PACK_RELATIVE_RELOCS");
    let cfg = |name: &str| std::env::var(name).unwrap_or_default();
    if cfg("CARGO_CFG_TARGET_OS") != "linux" || cfg("CARGO_CFG_TARGET_ENV") != "gnu" {
        return;
    }
    let enabled = match std::env::var("LEGION_PACK_RELATIVE_RELOCS").as_deref() {
        Ok("0") => false,
        Ok("1") => true,
        _ => {
            cfg("HOST") == cfg("TARGET")
                && host_glibc_version().is_some_and(|version| version >= RELR_MIN_GLIBC)
        }
    };
    if enabled {
        println!("cargo:rustc-link-arg-bins=-Wl,-z,pack-relative-relocs");
    }
}

/// The glibc this build script is running against, as `(major, minor)`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn host_glibc_version() -> Option<(u32, u32)> {
    extern "C" {
        fn gnu_get_libc_version() -> *const std::ffi::c_char;
    }
    // SAFETY: glibc returns a pointer to a static NUL-terminated string.
    let version = unsafe { std::ffi::CStr::from_ptr(gnu_get_libc_version()) };
    let mut parts = version.to_str().ok()?.split('.');
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn host_glibc_version() -> Option<(u32, u32)> {
    None
}
