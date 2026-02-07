use jsonnet::JsonnetVm;
use legion::structs::JobConfig;
use serde_json::Value;

#[derive(Default)]
struct JsonnetVars {
    ext_vars: Vec<(String, String)>,
    ext_codes: Vec<(String, String)>,
    tla_vars: Vec<(String, String)>,
    tla_codes: Vec<(String, String)>,
}

fn render_job_spec(path: &str, vars: &JsonnetVars) -> String {
    let mut vm = JsonnetVm::new();
    vm.import_callback(|_vm, base, rel| {
        let path = std::path::Path::new(base).join(rel);
        let contents = std::fs::read_to_string(&path).expect("imported file should read");
        Ok((base.into(), contents))
    });

    for (key, value) in &vars.ext_vars {
        vm.ext_var(key, value);
    }
    for (key, value) in &vars.ext_codes {
        vm.ext_code(key, value);
    }
    for (key, value) in &vars.tla_vars {
        vm.tla_var(key, value);
    }
    for (key, value) in &vars.tla_codes {
        vm.tla_code(key, value);
    }

    let rendered = vm
        .evaluate_file(path)
        .expect("jsonnet should evaluate")
        .to_string();
    rendered
}

fn default_jsonnet_vars() -> JsonnetVars {
    JsonnetVars {
        ext_vars: vec![
            ("task_kind".to_string(), "exec".to_string()),
            ("task_count".to_string(), "1".to_string()),
            ("message".to_string(), "hello".to_string()),
        ],
        ext_codes: vec![],
        tla_vars: vec![("job_name".to_string(), "vars-default".to_string())],
        tla_codes: vec![("cpu".to_string(), "100".to_string())],
    }
}

fn value_is_non_empty(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Object(map) => !map.is_empty(),
        Value::Array(items) => !items.is_empty(),
        _ => true,
    }
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
        let rendered = render_job_spec(&path_str, &default_jsonnet_vars());
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

#[test]
fn maximal_job_spec_parse() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("jobs")
        .join("maximal.jsonnet");
    let path_str = path.to_string_lossy();
    let rendered = render_job_spec(&path_str, &default_jsonnet_vars());
    let config: JobConfig = serde_json::from_str(&rendered)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    assert_eq!(config.name, "maximal-demo");
    assert_eq!(config.version, Some(1));
    assert_eq!(config.tasks.len(), 4);
    for task in &config.tasks {
        assert!(!task.name.is_empty(), "task name should be set");
        assert!(
            value_is_non_empty(&task.config),
            "task config should be set"
        );
        assert!(
            value_is_non_empty(&task.resources),
            "task resources should be set"
        );
        assert!(
            value_is_non_empty(&task.constraints),
            "task constraints should be set"
        );
        assert!(
            task.permit.is_null() || task.permit.is_object() || task.permit.is_array(),
            "task permit should parse as valid json type"
        );
        assert!(
            value_is_non_empty(&task.affinities),
            "task affinities should be set"
        );
    }
}

#[test]
fn variable_job_spec_renders_with_different_values() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("jobs")
        .join("vars.jsonnet");
    let path_str = path.to_string_lossy();

    let vars_one = JsonnetVars {
        ext_vars: vec![
            ("task_kind".to_string(), "exec".to_string()),
            ("task_count".to_string(), "1".to_string()),
            ("message".to_string(), "hello-one".to_string()),
        ],
        tla_vars: vec![("job_name".to_string(), "vars-one".to_string())],
        tla_codes: vec![("cpu".to_string(), "100".to_string())],
        ..JsonnetVars::default()
    };

    let rendered_one = render_job_spec(&path_str, &vars_one);
    let config_one: JobConfig = serde_json::from_str(&rendered_one)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    let vars_two = JsonnetVars {
        ext_vars: vec![
            ("task_kind".to_string(), "container".to_string()),
            ("task_count".to_string(), "2".to_string()),
            ("message".to_string(), "hello-two".to_string()),
        ],
        tla_vars: vec![("job_name".to_string(), "vars-two".to_string())],
        tla_codes: vec![("cpu".to_string(), "250".to_string())],
        ..JsonnetVars::default()
    };

    let rendered_two = render_job_spec(&path_str, &vars_two);
    let config_two: JobConfig = serde_json::from_str(&rendered_two)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    assert_ne!(config_one.name, config_two.name);
    assert_eq!(config_one.tasks.len(), 1);
    assert_eq!(config_two.tasks.len(), 1);
    assert_ne!(config_one.tasks[0].kind, config_two.tasks[0].kind);
    assert_ne!(config_one.tasks[0].count, config_two.tasks[0].count);
    assert_ne!(config_one.tasks[0].resources, config_two.tasks[0].resources);
}
