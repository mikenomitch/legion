use legion::config_lang;
use legion::manager::rollout_strategy::validate_rollout_strategy_name;
use legion::plugin_runtime::plugin_paths;
use legion::structs::JobConfig;
use legion::task_plugins::{validate_job_config, TaskPluginRegistry};
use legion::util::plugin_root_dir;
use serde_json::Value;

/// Evaluates a shipped example the way `legion job run` does.
///
/// A `.json` example is handed no variables: JSON cannot read them, and
/// supplying them is an error rather than a silent no-op.
fn render_job_spec(path: &str, vars: &serde_json::Map<String, Value>) -> String {
    let vars = match config_lang::format_for(path) {
        config_lang::ConfigFormat::Json => None,
        config_lang::ConfigFormat::Nickel => Some(vars),
    };
    config_lang::eval_file_with_vars(path, vars)
        .unwrap_or_else(|e| panic!("{path} should evaluate:\n{e:#}"))
}

/// The `vars` record the examples are exercised with.
///
/// Only `vars.ncl` reads any of it; every other example ignores `vars`
/// entirely, which is the point — an unparameterized jobspec never has to
/// mention it.
fn default_vars() -> serde_json::Map<String, Value> {
    let mut vars = serde_json::Map::new();
    vars.insert("job_name".into(), Value::from("vars-default"));
    vars.insert("task_runtime".into(), Value::from("exec"));
    vars.insert("task_count".into(), Value::from(1));
    vars.insert("message".into(), Value::from("hello"));
    vars.insert("cpu".into(), Value::from(100));
    vars
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
        if !config_lang::is_config_file(&path) {
            continue;
        }

        let path_str = path.to_string_lossy();
        let rendered = render_job_spec(&path_str, &default_vars());
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

/// Reports why `validate_job_config` cannot run for this config in the current
/// environment: a task kind or rollout strategy that resolves to a dylib
/// plugin shipped in `plugins/` but not built here (plugins only build with
/// LEGION_BUILD_PLUGINS=1). Uses the same loaders production uses, so builtin
/// kinds/strategies never trigger a skip, and a name with no plugin directory
/// at all is a real validation failure rather than a skip.
fn unbuilt_plugin_dependency(config: &JobConfig) -> Option<String> {
    let plugin_root = plugin_root_dir();
    let mut registry = TaskPluginRegistry::new();
    for task in &config.tasks {
        if registry.load_plugin(&task.runtime, false).is_err()
            && !plugin_paths("tasks", &task.runtime, &plugin_root)
                .dylib
                .exists()
        {
            return Some(format!("task kind {} is not built", task.runtime));
        }
    }

    let strategies = config
        .rollout
        .iter()
        .chain(config.rollout_by_group.values())
        .filter_map(|rollout| rollout.strategy.as_deref());
    for strategy in strategies {
        if validate_rollout_strategy_name(strategy).is_err()
            && !plugin_paths("rollouts", strategy, &plugin_root)
                .dylib
                .exists()
        {
            return Some(format!("rollout strategy {} is not built", strategy));
        }
    }
    None
}

// Every shipped example must survive the full submit-time validation the
// manager (and `legion job validate`) applies, not just parsing — this is
// what catches an example drifting from the validator (e.g. a sidecar
// declaring a cross-group dependency).
#[test]
fn config_jobs_pass_full_validation() {
    let jobs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("jobs");

    let mut failures = Vec::new();
    for entry in std::fs::read_dir(&jobs_dir).expect("jobs dir should read") {
        let entry = entry.expect("entry should read");
        let path = entry.path();
        if !config_lang::is_config_file(&path) {
            continue;
        }

        let path_str = path.to_string_lossy();
        let rendered = render_job_spec(&path_str, &default_vars());
        let config: JobConfig = serde_json::from_str(&rendered)
            .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

        if let Some(reason) = unbuilt_plugin_dependency(&config) {
            eprintln!("skipping validation of {}: {}", path_str, reason);
            continue;
        }

        if let Err(e) = validate_job_config(&config) {
            failures.push(format!("{} failed full validation: {:#}", path_str, e));
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

// The canonical example must always validate end to end with builtin task
// kinds only, so `legion job validate configs/jobs/maximal.ncl` works on
// any checkout without built plugins. No skip gate on purpose: if maximal
// ever grows a plugin-only surface, this failing is the prompt to decide
// that deliberately.
#[test]
fn maximal_job_spec_passes_full_validation() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("jobs")
        .join("maximal.ncl");
    let path_str = path.to_string_lossy();
    let rendered = render_job_spec(&path_str, &default_vars());
    let config: JobConfig = serde_json::from_str(&rendered)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    validate_job_config(&config)
        .unwrap_or_else(|e| panic!("maximal.ncl failed full validation: {:#}", e));
}

#[test]
fn maximal_job_spec_parse() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("jobs")
        .join("maximal.ncl");
    let path_str = path.to_string_lossy();
    let rendered = render_job_spec(&path_str, &default_vars());
    let config: JobConfig = serde_json::from_str(&rendered)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    assert_eq!(config.name, "maximal-demo");
    assert_eq!(config.version, Some(1));
    assert_eq!(config.tasks.len(), 6);

    // The keyed-mode stateful task (sandbox group) round-trips and validates.
    let sandbox = config
        .tasks
        .iter()
        .find(|task| task.name == "sandbox")
        .expect("maximal must declare the keyed sandbox task");
    let stateful = sandbox.stateful.as_ref().expect("stateful block parsed");
    assert_eq!(stateful.mode, legion::structs::InstanceMode::Keyed);
    assert!(stateful.require_id);
    sandbox
        .validate_stateful()
        .expect("keyed sandbox task validates");

    // The microVM sandbox task's kind config round-trips through the plugin's
    // schema (guest sizing, kernel, network), and its persistent state is a
    // block-mode volume — the shape a guest kernel can actually attach.
    let microvm = config
        .tasks
        .iter()
        .find(|task| task.name == "microvm")
        .expect("maximal must declare the sandbox-kind task");
    assert_eq!(microvm.runtime, "sandbox");
    let storage = microvm
        .stateful
        .as_ref()
        .and_then(|stateful| stateful.storage.as_ref())
        .expect("microvm task declares stateful.storage");
    assert_eq!(storage.path, "/workspace");
    assert_eq!(storage.volume_mode(), legion::volumes::VolumeMode::Block);
    // The snapshots block parses and resolves, and the gated
    // sandbox in the shipped spec asks for disk snapshots only.
    let snapshots = microvm
        .snapshots
        .as_ref()
        .expect("microvm declares a snapshots block")
        .resolve()
        .expect("snapshots block resolves");
    assert_eq!(snapshots.keep, 5);
    assert_eq!(
        snapshots.ttl,
        Some(std::time::Duration::from_secs(7 * 86_400))
    );
    // Gated, and memory snapshots work on gated sandboxes (gated plan
    // Phase 1).
    assert!(snapshots.memory);

    // No stateful task may sit under a createFirst (replace-before-stop)
    // rollout: that would double-bind one instance id + storage handle (see
    // docs/stateful-workloads.md). Verify the shipped spec honors this.
    for task in &config.tasks {
        if !task.is_stateful() {
            continue;
        }
        let replacement = config
            .rollout_config_for_group(&task.group)
            .and_then(|rollout| rollout.replacement);
        assert_ne!(
            replacement,
            Some(legion::structs::RolloutReplacement::CreateFirst),
            "stateful task {} (group {}) must not use createFirst",
            task.name,
            task.group
        );
    }

    // The KV injection policy keys are camelCase and actually parse (a
    // snake_case `on_change` would be silently dropped by serde).
    let injected = config
        .tasks
        .iter()
        .find_map(|task| task.kv_injections.as_ref().and_then(|kv| kv.kv.as_ref()))
        .expect("maximal must declare task-level kv injection defaults");
    assert_eq!(injected.watch, Some(true));
    assert_eq!(
        injected.on_change,
        Some(legion::structs::KvInjectionAction::Restart)
    );
    assert_eq!(
        injected.on_delete,
        Some(legion::structs::KvInjectionAction::Fail)
    );
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
        .join("vars.ncl");
    let path_str = path.to_string_lossy();

    let mut vars_one = default_vars();
    vars_one.insert("job_name".into(), Value::from("vars-one"));
    vars_one.insert("message".into(), Value::from("hello-one"));

    let rendered_one = render_job_spec(&path_str, &vars_one);
    let config_one: JobConfig = serde_json::from_str(&rendered_one)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    let mut vars_two = default_vars();
    vars_two.insert("job_name".into(), Value::from("vars-two"));
    vars_two.insert("task_runtime".into(), Value::from("container"));
    vars_two.insert("task_count".into(), Value::from(2));
    vars_two.insert("message".into(), Value::from("hello-two"));
    vars_two.insert("cpu".into(), Value::from(250));

    let rendered_two = render_job_spec(&path_str, &vars_two);
    let config_two: JobConfig = serde_json::from_str(&rendered_two)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path_str, e));

    assert_ne!(config_one.name, config_two.name);
    assert_eq!(config_one.tasks.len(), 1);
    assert_eq!(config_two.tasks.len(), 1);
    assert_ne!(config_one.tasks[0].runtime, config_two.tasks[0].runtime);
    assert_ne!(config_one.tasks[0].count, config_two.tasks[0].count);
    assert_ne!(config_one.tasks[0].resources, config_two.tasks[0].resources);
}

/// The shipped manager configs evaluate and deserialize.
///
/// `legion start manager -c …` is the only thing that reads these, and a
/// stale example is only noticed when someone tries to boot from it.
#[test]
fn config_manager_files_parse() {
    let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("configs");

    let mut seen = 0;
    for entry in std::fs::read_dir(&configs_dir).expect("configs dir should read") {
        let path = entry.expect("entry should read").path();
        if !config_lang::is_config_file(&path) {
            continue;
        }

        let path_str = path.to_string_lossy();
        let rendered = config_lang::eval_file(&path)
            .unwrap_or_else(|e| panic!("{path_str} should evaluate:\n{e:#}"));
        serde_json::from_str::<legion::structs::ManagerConfig>(&rendered)
            .unwrap_or_else(|e| panic!("{path_str} is not a manager config: {e}"));
        seen += 1;
    }

    assert!(
        seen >= 3,
        "expected the shipped manager configs, found {seen}"
    );
}

/// The shipped worker configs evaluate and deserialize.
///
/// The manager loads every file in `workerConfigPath` at startup, so a
/// broken example here fails the whole boot, not just one job.
#[test]
fn config_worker_files_parse() {
    let workers_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("workers");

    let mut seen = 0;
    for entry in std::fs::read_dir(&workers_dir).expect("workers dir should read") {
        let path = entry.expect("entry should read").path();
        if !config_lang::is_config_file(&path) {
            continue;
        }

        let path_str = path.to_string_lossy();
        let rendered = config_lang::eval_file(&path)
            .unwrap_or_else(|e| panic!("{path_str} should evaluate:\n{e:#}"));
        serde_json::from_str::<legion::structs::WorkerConfigAlt>(&rendered)
            .unwrap_or_else(|e| panic!("{path_str} is not a worker config: {e}"));
        seen += 1;
    }

    assert!(
        seen >= 3,
        "expected the shipped worker configs, found {seen}"
    );
}

/// The shipped job template evaluates, publishes a manifest, and renders.
///
/// This is the one example that goes through the manager's sandboxed
/// template path rather than the client-side jobspec path.
#[tokio::test]
async fn config_template_files_parse() {
    let templates_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs")
        .join("templates");

    let mut seen = 0;
    for entry in std::fs::read_dir(&templates_dir).expect("templates dir should read") {
        let path = entry.expect("entry should read").path();
        // Nickel only: a template declares `job` as a function of its
        // variables, so unlike every other config surface it has no JSON form.
        if path.extension().and_then(|ext| ext.to_str()) != Some(config_lang::CONFIG_EXTENSION) {
            continue;
        }

        let path_str = path.to_string_lossy();
        let source = std::fs::read_to_string(&path).expect("template source should read");
        let manifest = legion::manager::job_templates::parse_manifest_bounded(source.clone())
            .await
            .unwrap_or_else(|e| panic!("{path_str} is not a valid template:\n{e:#}"));

        // Every declared variable has a default here, so the template renders
        // with no caller input — the same trial render the manager runs at
        // publish time.
        let values = legion::manager::job_templates::resolve_variable_values(
            &manifest.variables,
            r#"{"scene": "demo"}"#,
        )
        .unwrap_or_else(|e| panic!("{path_str} defaults do not resolve: {e:#}"));
        let rendered = legion::manager::job_templates::render_bounded(&source, &values)
            .await
            .unwrap_or_else(|e| panic!("{path_str} does not render:\n{e:#}"));
        let config: JobConfig = serde_json::from_str(&rendered)
            .unwrap_or_else(|e| panic!("{path_str} renders invalid json: {e}"));
        validate_job_config(&config)
            .unwrap_or_else(|e| panic!("{path_str} renders an invalid jobspec: {e:#}"));
        seen += 1;
    }

    assert!(seen >= 1, "expected the shipped job template, found {seen}");
}

/// A JSON jobspec is accepted anywhere a Nickel one is.
///
/// The shipped `machine-generated.json` is covered by the directory scans
/// above; this pins the property those scans rely on — that the two languages
/// are two spellings of one job, not two dialects with different reach.
#[test]
fn json_and_nickel_jobspecs_produce_the_same_job() {
    let dir = tempfile::tempdir().expect("tempdir");

    let json_path = dir.path().join("job.json");
    std::fs::write(
        &json_path,
        r#"{
          "name": "equivalent",
          "priority": "high",
          "tasks": [{
            "name": "server",
            "runtime": "exec",
            "lifecycle": "service",
            "group": "api",
            "count": 2,
            "resources": {"cpu": 200, "memory": 256},
            "config": {"command": ["echo", "hi"]}
          }]
        }"#,
    )
    .expect("write json spec");

    let ncl_path = dir.path().join("job.ncl");
    std::fs::write(
        &ncl_path,
        r#"{
          name = "equivalent",
          priority = "high",
          tasks = [{
            name = "server",
            runtime = "exec",
            lifecycle = "service",
            group = "api",
            count = 2,
            resources = { cpu = 200, memory = 256 },
            config = { command = ["echo", "hi"] },
          }],
        }"#,
    )
    .expect("write nickel spec");

    let from_json: JobConfig = serde_json::from_str(&render_job_spec(
        &json_path.to_string_lossy(),
        &default_vars(),
    ))
    .expect("json spec should deserialize");
    let from_ncl: JobConfig = serde_json::from_str(&render_job_spec(
        &ncl_path.to_string_lossy(),
        &default_vars(),
    ))
    .expect("nickel spec should deserialize");

    assert_eq!(from_json, from_ncl);
    validate_job_config(&from_json).expect("json spec should pass full validation");
}

/// Nickel reads an imported `.json` file natively.
///
/// This is the path for mixing the two: generated data, hand-written
/// structure. It is also what the local-jobspec resolution fix protects —
/// the import has to resolve against the spec's own directory.
#[test]
fn a_nickel_jobspec_can_import_generated_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("fleet.json"), r#"{"count": 3, "cpu": 150}"#)
        .expect("write generated json");

    let spec = dir.path().join("job.ncl");
    std::fs::write(
        &spec,
        r#"
        let fleet = import "fleet.json" in
        {
          name = "hybrid",
          tasks = [{
            name = "server",
            runtime = "exec",
            group = "api",
            count = fleet.count,
            resources = { cpu = fleet.cpu },
            config = { command = ["echo", "hybrid"] },
          }],
        }
        "#,
    )
    .expect("write spec");

    let rendered = render_job_spec(&spec.to_string_lossy(), &default_vars());
    let config: JobConfig = serde_json::from_str(&rendered).expect("should deserialize");
    assert_eq!(config.name, "hybrid");
    assert_eq!(config.tasks[0].count.num, 3);
    validate_job_config(&config).expect("hybrid spec should pass full validation");
}
