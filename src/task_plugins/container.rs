use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, spawn_task_process, TaskPlugin, TaskRuntime,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::process::Command;

fn default_container_env() -> HashMap<String, String> {
    HashMap::new()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct ContainerTaskConfig {
    /// Container image reference used for task execution.
    image: String,

    /// Command and arguments to run inside the container.
    command: Vec<String>,

    /// Environment variables passed through to the container runtime.
    #[serde(default = "default_container_env")]
    #[schemars(default = "default_container_env")]
    env: HashMap<String, String>,

    /// Additional plugin-specific fields reserved for forward compatibility.
    #[serde(default, flatten)]
    #[schemars(skip)]
    _extra: HashMap<String, Value>,
}

impl ContainerTaskConfig {
    fn from_value(config: &Value) -> R<Self> {
        let parsed: Self = serde_json::from_value(config.clone())
            .context("container task config must be an object")?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> R<()> {
        if self.command.is_empty() {
            bail!("container task command must include at least one entry");
        }
        Ok(())
    }
}

/// Environment override for runtime selection (`docker` | `container` |
/// `youki`). Lets an operator pin a node's container runtime regardless of
/// what auto-detection would pick.
const RUNTIME_ENV: &str = "LEGION_CONTAINER_RUNTIME";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerRuntimeKind {
    /// Docker / Moby CLI — the portable default that "just works" on a Mac
    /// (Docker Desktop) and on Linux, and the runtime most local-dev and
    /// Kamal-style workflows already have installed.
    Docker,
    /// Apple's `container` toolchain (macOS only).
    AppleContainer,
    /// `youki` low-level OCI runtime (Linux).
    Youki,
}

impl ContainerRuntimeKind {
    /// Selects the container runtime for this node.
    ///
    /// Resolution order:
    /// 1. The `LEGION_CONTAINER_RUNTIME` env override, when set.
    /// 2. The first runtime whose binary is on `PATH`, in a per-OS preference
    ///    order (macOS prefers Apple `container` then Docker; Linux prefers
    ///    Docker then `youki`).
    /// 3. The OS default, so a spawn failure still names the expected binary
    ///    when nothing is installed.
    fn detect() -> R<Self> {
        if let Ok(name) = std::env::var(RUNTIME_ENV) {
            let name = name.trim();
            if !name.is_empty() {
                return Self::from_name(name);
            }
        }

        let preference: &[Self] = if cfg!(target_os = "macos") {
            &[Self::AppleContainer, Self::Docker]
        } else if cfg!(target_os = "linux") {
            &[Self::Docker, Self::Youki]
        } else {
            bail!("container tasks are not supported on this OS");
        };

        for kind in preference {
            if binary_on_path(kind.binary()) {
                return Ok(*kind);
            }
        }
        Ok(preference[0])
    }

    fn from_name(name: &str) -> R<Self> {
        match name.to_ascii_lowercase().as_str() {
            "docker" => Ok(Self::Docker),
            "container" | "apple" | "apple-container" => Ok(Self::AppleContainer),
            "youki" => Ok(Self::Youki),
            other => {
                bail!("unknown {RUNTIME_ENV}={other:?}; expected one of docker, container, youki")
            }
        }
    }

    fn binary(&self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::AppleContainer => "container",
            Self::Youki => "youki",
        }
    }

    /// Whether the runtime's `run` invocation needs a `--` terminator before
    /// the command. Apple `container` and `youki` (swift-argument-parser /
    /// clap style) use it so flags in the command pass through; Docker treats
    /// everything after the image as the command and rejects a literal `--`.
    fn uses_command_separator(&self) -> bool {
        matches!(self, Self::AppleContainer | Self::Youki)
    }

    /// Teardown command (by container name) for daemon-backed runtimes.
    ///
    /// Docker and Apple `container` run the workload in a daemon-managed
    /// container; killing the foreground `run` CLI only detaches it, leaving
    /// the container running. Stopping it requires an explicit by-name command.
    /// `youki` runs the container init as a direct child, so killing that child
    /// stops it — no separate teardown needed.
    fn teardown_args(&self, name: &str) -> Option<Vec<String>> {
        match self {
            Self::Docker => Some(vec!["rm".into(), "-f".into(), name.into()]),
            Self::AppleContainer => Some(vec!["stop".into(), name.into()]),
            Self::Youki => None,
        }
    }
}

/// Best-effort check for an executable on `PATH` (no spawn). Used only to pick
/// among installed runtimes; the eventual spawn is the real availability test.
fn binary_on_path(name: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| {
        let candidate = dir.join(name);
        candidate.is_file() || candidate.is_symlink()
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerLaunch {
    program: String,
    args: Vec<String>,
}

fn build_launch(
    runtime: ContainerRuntimeKind,
    allocation: &Allocation,
    config: &ContainerTaskConfig,
) -> ContainerLaunch {
    let name = format!("legion-{}", allocation.id);
    let mut args = Vec::new();

    match runtime {
        ContainerRuntimeKind::Docker | ContainerRuntimeKind::AppleContainer => {
            // Docker and Apple `container` share a docker-compatible surface:
            // `run --rm --name <name> [--env K=V ...] <image> [-- ] <cmd...>`.
            args.extend([
                "run".to_string(),
                "--rm".to_string(),
                "--name".to_string(),
                name,
            ]);
            for (key, value) in &config.env {
                args.push("--env".to_string());
                args.push(format!("{key}={value}"));
            }
            args.push(config.image.clone());
            if runtime.uses_command_separator() {
                args.push("--".to_string());
            }
            args.extend(config.command.clone());
        }
        ContainerRuntimeKind::Youki => {
            args.extend([
                "run".to_string(),
                "--name".to_string(),
                name,
                "--image".to_string(),
                config.image.clone(),
            ]);
            for (key, value) in &config.env {
                args.push("--env".to_string());
                args.push(format!("{key}={value}"));
            }
            if runtime.uses_command_separator() {
                args.push("--".to_string());
            }
            args.extend(config.command.clone());
        }
    }

    ContainerLaunch {
        program: runtime.binary().to_string(),
        args,
    }
}

/// Imports a staged OCI layout into the local container runtime so the
/// subsequent `run` finds the image without a registry pull.
fn load_image(
    runtime: ContainerRuntimeKind,
    oci_layout: &std::path::Path,
    reference: &str,
) -> R<()> {
    match runtime {
        // Docker (with the containerd image store) and Apple `container` both
        // ingest an OCI archive: tar the staged layout and `load` it.
        ContainerRuntimeKind::Docker | ContainerRuntimeKind::AppleContainer => {
            let archive_path = oci_layout.with_extension("oci.tar");
            let archive = std::fs::File::create(&archive_path)
                .with_context(|| format!("create image archive {}", archive_path.display()))?;
            let mut builder = tar::Builder::new(archive);
            builder
                .append_dir_all(".", oci_layout)
                .context("archive OCI layout")?;
            builder.finish().context("finish OCI layout archive")?;

            // `docker load --input` / `container image load --input`.
            let mut cmd = Command::new(runtime.binary());
            if matches!(runtime, ContainerRuntimeKind::AppleContainer) {
                cmd.arg("image");
            }
            cmd.arg("load").arg("--input").arg(&archive_path);
            let output = cmd
                .output()
                .with_context(|| format!("spawn {} image load", runtime.binary()))?;
            let _ = std::fs::remove_file(&archive_path);
            if !output.status.success() {
                bail!(
                    "{} image load failed for {reference}: {}",
                    runtime.binary(),
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(())
        }
        ContainerRuntimeKind::Youki => bail!(
            "Legion-managed image artifacts are not yet supported for the youki runtime \
             (OCI layout -> runtime bundle conversion is pending); disable \
             artifacts.containerImageDesugar for Linux container workloads"
        ),
    }
}

#[derive(Default)]
pub struct ContainerTaskPlugin;

impl TaskPlugin for ContainerTaskPlugin {
    fn name(&self) -> &'static str {
        "container"
    }

    fn schema(&self) -> Value {
        derived_schema::<ContainerTaskConfig>()
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("container".into(), Value::String("pass".into()));
        map
    }

    fn supports_kv_env_injection(&self) -> bool {
        true
    }

    // Identity delivery mirrors the container kind's existing injection surface:
    // env injection is supported, file injection is not (delivering a file into a
    // container needs volume plumbing the container plugin doesn't yet implement,
    // same as kv file injection). An env-delivered identity on a container must
    // therefore refresh via `changeMode: restart`.
    fn supports_identity_env(&self) -> bool {
        true
    }

    fn supports_artifacts(&self) -> bool {
        true
    }

    /// Desugars `config.image` into an implicit `oci://` artifact (artifact
    /// system plan §8). The manager calls this at admission only when the
    /// cluster's `artifacts.containerImageDesugar` flag is on — without the
    /// artifact cache, Legion-managed pulls would re-download per allocation.
    fn implicit_artifacts(&self, task_config: &Value) -> R<Vec<crate::structs::ArtifactSpec>> {
        let image = task_config
            .get("image")
            .and_then(Value::as_str)
            .context("container task config requires image")?;
        Ok(vec![crate::structs::ArtifactSpec::oci_image(image)])
    }

    /// Consumes the staged `Image` payload: hands the on-disk OCI layout to
    /// the runtime. File/tree artifacts are deferred until the kind grows
    /// volume plumbing (same limitation as its KV file injection).
    fn consume_artifact(
        &self,
        staged: &crate::artifacts::StagedArtifact,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        if let crate::artifacts::StagedPayload::Image {
            oci_layout,
            reference,
        } = &staged.payload
        {
            let runtime_kind = ContainerRuntimeKind::detect()?;
            load_image(runtime_kind, oci_layout, reference)?;
            if let Some(state) = runtime.state.as_object_mut() {
                state.insert(
                    "artifactImage".into(),
                    json!({
                        "ociLayout": oci_layout.display().to_string(),
                        "reference": reference,
                        "digest": staged.digest,
                    }),
                );
            } else {
                runtime.state = json!({
                    "artifactImage": {
                        "ociLayout": oci_layout.display().to_string(),
                        "reference": reference,
                        "digest": staged.digest,
                    }
                });
            }
        }
        Ok(())
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _runtime: &mut TaskRuntime,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing container task"
        );
        Ok(())
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        let config = ContainerTaskConfig::from_value(&task.config)?;
        let runtime_kind = ContainerRuntimeKind::detect()?;
        let launch = build_launch(runtime_kind, allocation, &config);

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            program = %launch.program,
            "Starting container task"
        );

        let mut cmd = Command::new(&launch.program);
        cmd.args(&launch.args);
        configure_task_log_capture(&mut cmd, runtime)?;

        let child = spawn_task_process(&mut cmd).with_context(|| {
            format!(
                "Failed to spawn container task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        runtime.pid = Some(child.id());
        runtime.child = Some(child);
        Ok(())
    }

    fn stop(&self, allocation: &Allocation, task: &TaskConfig, runtime: &mut TaskRuntime) -> R<()> {
        // Daemon-backed runtimes (Docker, Apple `container`) keep the container
        // alive when the launching CLI is killed, so tear it down by name
        // first. Best-effort: a `--rm` container that already exited cleanly is
        // gone, and that's success, not an error.
        if let Ok(runtime_kind) = ContainerRuntimeKind::detect() {
            let name = format!("legion-{}", allocation.id);
            if let Some(args) = runtime_kind.teardown_args(&name) {
                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    runtime = runtime_kind.binary(),
                    "Tearing down container by name"
                );
                let _ = Command::new(runtime_kind.binary()).args(&args).output();
            }
        }

        if let Some(mut child) = runtime.child.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Stopping container task"
            );
            let _ = child.kill();
            let _ = child.wait();
            runtime.pid = None;
        } else if let Some(pid) = runtime.pid.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                pid,
                "Stopping container task by pid"
            );
            let mut system = sysinfo::System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
                process.kill();
            }
        }
        Ok(())
    }

    fn destroy(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Destroying container task"
        );
        self.stop(allocation, task, runtime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn alloc() -> Allocation {
        Allocation {
            id: 7,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
        }
    }

    #[test]
    fn container_config_parses_required_fields() -> R<()> {
        let config = json!({
            "image": "alpine:latest",
            "command": ["echo", "hello"],
            "env": {
                "MODE": "test"
            }
        });

        let parsed = ContainerTaskConfig::from_value(&config)?;
        assert_eq!(parsed.image, "alpine:latest");
        assert_eq!(
            parsed.command,
            vec!["echo".to_string(), "hello".to_string()]
        );
        assert_eq!(parsed.env.get("MODE"), Some(&"test".to_string()));
        Ok(())
    }

    #[test]
    fn container_config_requires_image() {
        let config = json!({"command": ["echo"]});
        let err = ContainerTaskConfig::from_value(&config).expect_err("missing image");
        let details = format!("{err:#}");
        assert!(details.contains("missing field `image`"));
    }

    #[test]
    fn container_config_requires_command() {
        let config = json!({"image": "alpine"});
        let err = ContainerTaskConfig::from_value(&config).expect_err("missing command");
        let details = format!("{err:#}");
        assert!(details.contains("missing field `command`"));
    }

    #[test]
    fn container_config_rejects_empty_command() {
        let config = json!({"image": "alpine", "command": []});
        let err = ContainerTaskConfig::from_value(&config).expect_err("empty command");
        assert!(err.to_string().contains("must include at least one entry"));
    }

    #[test]
    fn container_config_rejects_non_string_env() {
        let config = json!({
            "image": "alpine",
            "command": ["echo"],
            "env": { "COUNT": 1 }
        });
        let err = ContainerTaskConfig::from_value(&config).expect_err("bad env");
        let details = format!("{err:#}");
        assert!(details.contains("invalid type"));
    }

    #[test]
    fn container_plugin_validates_schema() -> R<()> {
        let plugin = ContainerTaskPlugin::default();
        let config = json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        });
        plugin.validate_config(&config)?;
        Ok(())
    }

    #[test]
    fn launch_command_uses_runtime_binary() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let launch = build_launch(ContainerRuntimeKind::Youki, &alloc(), &config);
        assert_eq!(launch.program, "youki");
        assert!(launch.args.contains(&"--image".to_string()));
        Ok(())
    }

    #[test]
    fn docker_launch_has_no_command_separator() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine:latest",
            "command": ["echo", "ok"],
            "env": { "MODE": "test" }
        }))?;
        let launch = build_launch(ContainerRuntimeKind::Docker, &alloc(), &config);
        assert_eq!(launch.program, "docker");
        // Docker rejects a literal `--` between image and command.
        assert!(!launch.args.contains(&"--".to_string()));
        // `--rm`, `--name`, env, image, then the command — in that order.
        assert_eq!(launch.args[0], "run");
        assert!(launch.args.contains(&"--rm".to_string()));
        assert!(launch.args.contains(&"--env".to_string()));
        assert!(launch.args.contains(&"MODE=test".to_string()));
        let image_idx = launch
            .args
            .iter()
            .position(|a| a == "alpine:latest")
            .expect("image present");
        assert_eq!(launch.args[image_idx + 1], "echo");
        assert_eq!(launch.args[image_idx + 2], "ok");
        Ok(())
    }

    #[test]
    fn apple_container_keeps_command_separator() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["ls", "-l"]
        }))?;
        let launch = build_launch(ContainerRuntimeKind::AppleContainer, &alloc(), &config);
        assert_eq!(launch.program, "container");
        // The separator lets flag-bearing commands (`-l`) pass through.
        let sep = launch
            .args
            .iter()
            .position(|a| a == "--")
            .expect("separator present");
        let image_idx = launch
            .args
            .iter()
            .position(|a| a == "alpine")
            .expect("image present");
        assert_eq!(sep, image_idx + 1);
        assert_eq!(launch.args[sep + 1], "ls");
        Ok(())
    }

    #[test]
    fn runtime_from_name_parses_known_runtimes() -> R<()> {
        assert_eq!(
            ContainerRuntimeKind::from_name("docker")?,
            ContainerRuntimeKind::Docker
        );
        assert_eq!(
            ContainerRuntimeKind::from_name("Container")?,
            ContainerRuntimeKind::AppleContainer
        );
        assert_eq!(
            ContainerRuntimeKind::from_name("youki")?,
            ContainerRuntimeKind::Youki
        );
        assert!(ContainerRuntimeKind::from_name("podman").is_err());
        Ok(())
    }

    fn container_task(name: &str, command: Value) -> TaskConfig {
        serde_json::from_value(json!({
            "name": name,
            "kind": "container",
            "config": { "image": "alpine:latest", "command": command },
        }))
        .expect("task config deserializes")
    }

    /// Real end-to-end run via the Docker runtime: spawn a container, let it
    /// exit, and assert the worker's log-capture wiring wrote stdout/stderr.
    /// Exercises `start()` + `configure_task_log_capture` together.
    ///
    /// Run with:
    /// `cargo test --lib docker_runs_container -- --ignored --nocapture`
    #[test]
    #[ignore = "requires docker + network: runs a real container"]
    fn docker_runs_container_and_captures_logs() -> R<()> {
        std::env::set_var(RUNTIME_ENV, "docker");
        let plugin = ContainerTaskPlugin::default();
        let allocation = alloc();
        let task = container_task(
            "logtest",
            json!(["sh", "-c", "echo stdout-line; echo stderr-line 1>&2"]),
        );

        let tmp = tempfile::tempdir()?;
        let log_dir = tmp.path().join("logs");
        let mut runtime = TaskRuntime {
            log_dir: Some(log_dir.clone()),
            ..TaskRuntime::default()
        };

        plugin.start(&allocation, &task, &mut runtime)?;
        let mut child = runtime.child.take().expect("child handle set");
        let status = child.wait()?;
        assert!(status.success(), "container exited cleanly: {status:?}");

        let stdout = std::fs::read_to_string(log_dir.join("stdout.log"))?;
        let stderr = std::fs::read_to_string(log_dir.join("stderr.log"))?;
        assert!(
            stdout.contains("stdout-line"),
            "stdout captured: {stdout:?}"
        );
        assert!(
            stderr.contains("stderr-line"),
            "stderr captured: {stderr:?}"
        );
        Ok(())
    }

    /// Real end-to-end stop: start a long-running container and confirm
    /// `stop()` tears it down (the `--rm` run leaves nothing behind).
    #[test]
    #[ignore = "requires docker + network: runs a real container"]
    fn docker_stop_kills_running_container() -> R<()> {
        std::env::set_var(RUNTIME_ENV, "docker");
        let plugin = ContainerTaskPlugin::default();
        let allocation = alloc();
        let task = container_task("stoptest", json!(["sleep", "60"]));
        let name = format!("legion-{}", allocation.id);

        let mut runtime = TaskRuntime::default();
        plugin.start(&allocation, &task, &mut runtime)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let running = Command::new("docker")
            .args(["ps", "-q", "--filter", &format!("name={name}")])
            .output()?;
        assert!(
            !String::from_utf8_lossy(&running.stdout).trim().is_empty(),
            "container should be running before stop"
        );

        plugin.stop(&allocation, &task, &mut runtime)?;
        std::thread::sleep(std::time::Duration::from_secs(2));

        let after = Command::new("docker")
            .args(["ps", "-q", "--filter", &format!("name={name}")])
            .output()?;
        assert!(
            String::from_utf8_lossy(&after.stdout).trim().is_empty(),
            "container should be gone after stop"
        );
        Ok(())
    }

    /// The image-desugar consume path: fetch a real image into an OCI layout
    /// and import it into Docker via `load_image` (what `consume_artifact`
    /// calls). Requires Docker with the containerd image store (OCI-archive
    /// `docker load` support). Override the image with `LEGION_OCI_TEST_IMAGE`.
    #[test]
    #[ignore = "requires docker + network: loads a fetched OCI layout into docker"]
    fn docker_loads_fetched_oci_layout() -> R<()> {
        use crate::artifacts::fetcher::{
            ArtifactFetcher, FetchLimits, FetchRequest, FetchedPayload,
        };
        use crate::artifacts::fetchers::OciFetcher;
        use crate::artifacts::stage::ArtifactSink;
        use crate::structs::parse_artifact_source;

        let source = std::env::var("LEGION_OCI_TEST_IMAGE")
            .unwrap_or_else(|_| "oci://docker.io/library/hello-world:latest".to_string());
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits::default();
        let req = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: &source,
            checksum: None,
            media_type: None,
            credential: None,
            limits: &limits,
        };

        let staging = tempfile::tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().to_path_buf(), limits.max_bytes)?;
        let fetched = OciFetcher::new().fetch(&req, &mut sink)?;
        let FetchedPayload::Image {
            oci_layout,
            reference,
            ..
        } = fetched.payload
        else {
            panic!("expected an Image payload");
        };

        load_image(ContainerRuntimeKind::Docker, &oci_layout, &reference)?;
        Ok(())
    }
}
