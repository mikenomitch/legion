fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "proto/legion.proto";

    // TODO: Confirm that this works
    tonic_build::configure()
        .build_server(true)
        .type_attribute("Node", "#[derive(serde::Deserialize, serde::Serialize)]")
        .compile(&[proto_file], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    // tonic_build::compile_protos("proto/legion.proto")?;

    // For every plugin of every type, build a release
    std::fs::read_dir("./plugins")?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                Some(path)
            } else {
                None
            }
        })
        .for_each(|path| {
            std::fs::read_dir(path).
                unwrap().
                for_each( |entry| {
                    let entry = entry.ok().unwrap();
                    let path = entry.path();
                    if path.is_dir() {
                        let mut cmd = std::process::Command::new("cargo");
                        cmd.arg("build").arg("--release");
                        cmd.current_dir(path);
                        cmd.spawn().unwrap().wait().unwrap();
                    }
                })
        });

    Ok(())
}
