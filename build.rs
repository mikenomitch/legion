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
        // Every plugin builds into one shared target dir, so the ~470 crates
        // they have in common (Legion itself, tokio, tonic…) compile once,
        // not once per plugin. Each plugin's own artifacts are then copied
        // to `plugin-build/<kind>/<name>/release/`, the layout the loader
        // (`plugin_runtime::plugin_paths`) and the rig's bundle script read.
        let shared_target_dir = plugin_target_root.join("shared");

        let mut plugins: Vec<std::path::PathBuf> = Vec::new();
        for kind in std::fs::read_dir(&plugin_root)? {
            let kind = kind?.path();
            if !kind.is_dir() {
                continue;
            }
            match std::fs::read_dir(&kind) {
                Ok(dir) => {
                    for entry in dir.flatten() {
                        let path = entry.path();
                        // Only Rust plugins build here: exec plugins (device,
                        // keyring, cluster CA) are scripts with no crate.
                        if path.is_dir() && path.join("Cargo.toml").is_file() {
                            plugins.push(path);
                        }
                    }
                }
                Err(e) => println!("cargo:warning=error reading {}: {e}", kind.display()),
            }
        }
        // A stable order, so two packages that happen to share a name build
        // (and overwrite each other's outputs in the shared dir) the same
        // way every time; each is copied out before the next one builds.
        plugins.sort();

        for path in plugins {
            let name_of = |p: &std::path::Path| {
                p.file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            };
            let plugin_name = name_of(&path);
            let plugin_kind = path
                .parent()
                .map(name_of)
                .unwrap_or_else(|| "unknown".to_string());
            let out_dir = plugin_target_root
                .join(&plugin_kind)
                .join(&plugin_name)
                .join("release");

            let mut cmd = std::process::Command::new("cargo");
            cmd.args([
                "build",
                "--release",
                "--message-format=json-render-diagnostics",
            ]);
            cmd.env("LEGION_SKIP_PLUGIN_BUILD", "1");
            cmd.env("CARGO_TARGET_DIR", &shared_target_dir);
            cmd.current_dir(&path);
            // stdout carries cargo's JSON messages (and must not reach this
            // script's stdout, which cargo reads as directives); diagnostics
            // still go to stderr.
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::inherit());
            let output = match cmd.output() {
                Ok(output) => output,
                Err(e) => {
                    println!("cargo:warning=plugin {} did not build: {e}", path.display());
                    continue;
                }
            };
            // A failed plugin build used to vanish here, and the first sign
            // was a deploy with no plugin dylibs.
            if !output.status.success() {
                println!(
                    "cargo:warning=plugin {} failed to build ({})",
                    path.display(),
                    output.status
                );
                continue;
            }
            let manifest = path.join("Cargo.toml");
            let artifacts = own_artifacts(&String::from_utf8_lossy(&output.stdout), &manifest);
            if artifacts.is_empty() {
                println!(
                    "cargo:warning=plugin {} built nothing to copy",
                    path.display()
                );
                continue;
            }
            if let Err(e) = std::fs::create_dir_all(&out_dir) {
                println!("cargo:warning=cannot create {}: {e}", out_dir.display());
                continue;
            }
            for artifact in artifacts {
                let Some(file_name) = artifact.file_name() else {
                    continue;
                };
                let dest = out_dir.join(file_name);
                // Replace, never write through: a running worker may have
                // the old dylib mapped.
                let _ = std::fs::remove_file(&dest);
                if let Err(e) = std::fs::copy(&artifact, &dest) {
                    println!(
                        "cargo:warning=cannot copy {} to {}: {e}",
                        artifact.display(),
                        dest.display()
                    );
                }
            }
        }
    } else if !skip_plugins {
        println!(
            "cargo:warning=Plugin build skipped; set LEGION_BUILD_PLUGINS=1 to build plugins."
        );
    }

    Ok(())
}

/// The files cargo built for the package whose manifest is `manifest`, read
/// from `cargo build --message-format=json` output: its dylibs and binaries,
/// not its dependencies' rlibs. Only the shared-target plugin build needs it,
/// and the build script has no JSON parser, so this reads just the two
/// fields it needs (`manifest_path` and `filenames`) out of each
/// `compiler-artifact` line.
fn own_artifacts(messages: &str, manifest: &std::path::Path) -> Vec<std::path::PathBuf> {
    let manifest = std::fs::canonicalize(manifest).unwrap_or_else(|_| manifest.to_path_buf());
    let mut out = Vec::new();
    for line in messages.lines() {
        if !line.contains(r#""reason":"compiler-artifact""#) {
            continue;
        }
        let Some(manifest_path) = json_string_field(line, "manifest_path") else {
            continue;
        };
        let manifest_path = std::path::PathBuf::from(manifest_path);
        let manifest_path = std::fs::canonicalize(&manifest_path).unwrap_or(manifest_path);
        if manifest_path != manifest {
            continue;
        }
        let Some(start) = line.find(r#""filenames":["#) else {
            continue;
        };
        let rest = &line[start + r#""filenames":["#.len()..];
        let Some(end) = rest.find(']') else {
            continue;
        };
        for quoted in rest[..end].split("\",\"") {
            let file = unescape_json(quoted.trim_matches('"'));
            let file = std::path::PathBuf::from(file);
            // `.rlib`/`.rmeta` are a library's inputs to other crates;
            // only something a worker loads or runs is a plugin artifact.
            let keep = !matches!(
                file.extension().and_then(|ext| ext.to_str()),
                Some("rlib") | Some("rmeta") | Some("d")
            );
            if keep && file.is_file() {
                out.push(file);
            }
        }
    }
    out
}

/// The string value of `"field":"…"` in one line of JSON, unescaped.
fn json_string_field(line: &str, field: &str) -> Option<String> {
    let key = format!("\"{field}\":\"");
    let start = line.find(&key)? + key.len();
    let rest = &line[start..];
    let mut end = None;
    let mut escaped = false;
    for (i, c) in rest.char_indices() {
        match c {
            '\\' if !escaped => escaped = true,
            '"' if !escaped => {
                end = Some(i);
                break;
            }
            _ => escaped = false,
        }
    }
    Some(unescape_json(&rest[..end?]))
}

/// Undoes JSON's escaping of `\` and `"`, the only escapes a path carries.
fn unescape_json(value: &str) -> String {
    value.replace("\\\"", "\"").replace("\\\\", "\\")
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
