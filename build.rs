fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit rerun triggers replace Cargo's default of re-running on any
    // change in the package.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=LEGION_SKIP_PLUGIN_BUILD");
    println!("cargo:rerun-if-env-changed=LEGION_BUILD_PLUGINS");
    println!("cargo:rerun-if-env-changed=LEGION_PLUGIN_DIR");

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
