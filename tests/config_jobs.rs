use jsonnet::JsonnetVm;
use legion::structs::JobConfig;

fn render_job_spec(path: &str) -> String {
    let mut vm = JsonnetVm::new();
    vm.import_callback(|_vm, base, rel| {
        let path = std::path::Path::new(base).join(rel);
        let contents = std::fs::read_to_string(&path).expect("imported file should read");
        Ok((base.into(), contents))
    });

    let rendered = vm
        .evaluate_file(path)
        .expect("jsonnet should evaluate")
        .to_string();
    rendered
}

#[test]
fn config_jobs_parse() {
    let jobs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("jobs");

    for entry in std::fs::read_dir(&jobs_dir).expect("jobs dir should read") {
        let entry = entry.expect("entry should read");
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonnet") {
            continue;
        }

        let path_str = path.to_string_lossy();
        let rendered = render_job_spec(&path_str);
        let config: JobConfig = serde_json::from_str(&rendered)
            .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

        assert!(
            !config.name.is_empty(),
            "job {} should have a name",
            path_str
        );
        assert!(
            !config.tasks.is_empty(),
            "job {} should have tasks",
            path_str
        );
    }
}
