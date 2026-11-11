fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // `legion-isolate engine-info` reports the target it compiles for.
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".into());
    println!("cargo:rustc-env=LEGION_ISOLATE_TARGET={target}");
}
