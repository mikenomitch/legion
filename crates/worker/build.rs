fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    write_plugin_build_info()
}

/// Writes `$OUT_DIR/plugin_build_info.rs`: the toolchain and crate identity
/// that `plugin_runtime::PLUGIN_ABI_FINGERPRINT` hashes.
///
/// Plugins are Rust dylibs that hand `Box<dyn Trait>` across the boundary,
/// and Rust has no stable ABI, so a plugin is only sound when it was compiled
/// by the same rustc against the same legion crate as the host. This build
/// script runs in both builds (a plugin depends on this crate by path), so
/// each side bakes in what it was built with and the loader compares the two.
/// The profile is deliberately left out: a debug host loads release plugins
/// in development, and the profile does not change type layout.
fn write_plugin_build_info() -> Result<(), Box<dyn std::error::Error>> {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let verbose = std::process::Command::new(&rustc).arg("-vV").output()?;
    let verbose = String::from_utf8_lossy(&verbose.stdout);
    let rustc_line = verbose.lines().next().unwrap_or("rustc unknown");
    let commit = verbose
        .lines()
        .find_map(|line| line.strip_prefix("commit-hash: "))
        .unwrap_or("unknown");
    let info = format!(
        "{rustc_line} [{commit}]; {} {}; target {}; panic={}",
        std::env::var("CARGO_PKG_NAME")?,
        std::env::var("CARGO_PKG_VERSION")?,
        std::env::var("TARGET")?,
        std::env::var("CARGO_CFG_PANIC").unwrap_or_else(|_| "unwind".into()),
    );
    // Printable ASCII only, so it embeds safely as a C string literal.
    let info: String = info
        .chars()
        .map(|c| match c {
            ' '..='~' if c != '"' && c != '\\' => c,
            _ => '?',
        })
        .collect();
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("plugin_build_info.rs");
    std::fs::write(
        out,
        format!("pub const PLUGIN_BUILD_INFO: &std::ffi::CStr = c\"{info}\";\n"),
    )?;
    Ok(())
}
