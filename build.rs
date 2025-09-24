fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "proto/legion.proto";

    tonic_build::configure()
        .build_server(true)
        .type_attribute("NodeMsg", "#[derive(serde::Deserialize, serde::Serialize)]")
        .type_attribute(
            "Job",
            "#[derive(serde::Deserialize, serde::Serialize, tabled::Tabled)]",
        )
        .type_attribute(
            "Submission",
            "#[derive(serde::Deserialize, serde::Serialize, tabled::Tabled)]",
        )
        .compile_protos(&[proto_file], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {e}"));

    // tonic_build::compile_protos("proto/legion.proto")?;

    // For every plugin of every kind, build a release
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
        .for_each(|path| match std::fs::read_dir(path) {
            Ok(dir) => dir.for_each(|entry| match entry.ok() {
                None => {
                    println!("Error reading entry");
                }
                Some(entry) => {
                    let path = entry.path();
                    if path.is_dir() {
                        let mut cmd = std::process::Command::new("cargo");
                        cmd.arg("build").arg("--release");
                        cmd.current_dir(path);
                        cmd.spawn().unwrap().wait().unwrap();
                    }
                }
            }),
            Err(e) => {
                println!("Error reading dir: {e}");
            }
        });

    Ok(())
}
