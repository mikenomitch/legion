use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, oci_bundle, spawn_task_process, TaskPlugin,
    TaskRuntime,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::{Path, PathBuf};
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

/// Env override selecting the Linux OCI runtime binary. Defaults to `youki`;
/// any OCI-spec runtime (e.g. `runc`, `crun`) works since they share the
/// bundle interface.
const OCI_RUNTIME_ENV: &str = "LEGION_OCI_RUNTIME";
const DEFAULT_OCI_RUNTIME: &str = "youki";

/// The container runtime for this node, chosen by OS.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ContainerRuntime {
    /// macOS: Apple's `container` toolchain — a high-level runtime that pulls
    /// and runs images by reference (docker-compatible surface).
    AppleContainer,
    /// Linux: a low-level OCI runtime (youki by default) run against a bundle
    /// Legion builds from the image's OCI layout. The runtime itself neither
    /// pulls nor understands image references.
    Oci { binary: String },
}

impl ContainerRuntime {
    /// macOS uses Apple `container`; Linux uses an OCI runtime (youki, or
    /// `LEGION_OCI_RUNTIME`). Other operating systems are unsupported.
    fn detect() -> R<Self> {
        if cfg!(target_os = "macos") {
            Ok(Self::AppleContainer)
        } else if cfg!(target_os = "linux") {
            let binary = std::env::var(OCI_RUNTIME_ENV)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| DEFAULT_OCI_RUNTIME.to_string());
            Ok(Self::Oci { binary })
        } else {
            bail!("container tasks are not supported on this OS");
        }
    }
}

/// Deterministic per-allocation container name / OCI container id.
fn container_name(allocation: &Allocation) -> String {
    format!("legion-{}", allocation.id)
}

/// Per-allocation scratch root for the image layout and bundle. Prefers the
/// worker-provided allocation/task directory; falls back to a temp dir keyed
/// by allocation id when the worker threaded none through.
fn work_root(allocation: &Allocation, runtime: &TaskRuntime) -> PathBuf {
    runtime
        .alloc_dir
        .clone()
        .or_else(|| runtime.task_dir.clone())
        .unwrap_or_else(|| std::env::temp_dir().join(format!("legion-container-{}", allocation.id)))
        .join(".container")
}

/// Apple `container run` launch (docker-compatible). The `--` terminator lets a
/// flag-bearing command pass through swift-argument-parser.
fn build_apple_launch(allocation: &Allocation, config: &ContainerTaskConfig) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        container_name(allocation),
    ];
    for (key, value) in &config.env {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }
    args.push(config.image.clone());
    args.push("--".to_string());
    args.extend(config.command.clone());
    args
}

/// Imports a staged OCI layout into Apple `container` so the subsequent `run`
/// finds the image without a registry pull (tar the layout → `image load`).
fn apple_load_image(oci_layout: &Path, reference: &str) -> R<()> {
    let archive_path = oci_layout.with_extension("oci.tar");
    let archive = std::fs::File::create(&archive_path)
        .with_context(|| format!("create image archive {}", archive_path.display()))?;
    let mut builder = tar::Builder::new(archive);
    builder
        .append_dir_all(".", oci_layout)
        .context("archive OCI layout")?;
    builder.finish().context("finish OCI layout archive")?;

    let output = Command::new("container")
        .args(["image", "load", "--input"])
        .arg(&archive_path)
        .output()
        .context("spawn container image load")?;
    let _ = std::fs::remove_file(&archive_path);
    if !output.status.success() {
        bail!(
            "container image load failed for {reference}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Pulls `image` into an on-disk OCI layout via the shared `oci://` fetcher
/// (anonymous), returning the layout root. Used on Linux when no image artifact
/// was staged for the task (the runtime cannot pull it itself).
fn pull_image_layout(image: &str, dest_root: &Path) -> R<PathBuf> {
    use crate::artifacts::fetcher::{ArtifactFetcher, FetchLimits, FetchRequest, FetchedPayload};
    use crate::artifacts::fetchers::OciFetcher;
    use crate::artifacts::stage::ArtifactSink;
    use crate::structs::{parse_artifact_source, ArtifactSpec};

    let source = ArtifactSpec::oci_image(image).source;
    let parsed = parse_artifact_source(&source)?;
    let limits = FetchLimits::default();
    let request = FetchRequest {
        scheme: &parsed.scheme,
        url: &parsed.url,
        redacted_source: &source,
        checksum: None,
        media_type: None,
        credential: None,
        limits: &limits,
    };

    std::fs::create_dir_all(dest_root)
        .with_context(|| format!("create image dir {}", dest_root.display()))?;
    let mut sink = ArtifactSink::new(dest_root.to_path_buf(), limits.max_bytes)?;
    let fetched = OciFetcher::new()
        .fetch(&request, &mut sink)
        .with_context(|| format!("pull container image {image}"))?;
    match fetched.payload {
        FetchedPayload::Image { oci_layout, .. } => Ok(oci_layout),
        other => bail!("expected an OCI image payload for {image}, got {other:?}"),
    }
}

/// The OCI layout recorded on `runtime.state` by `consume_artifact` when the
/// image was staged through the artifact pipeline.
fn staged_image_layout(runtime: &TaskRuntime) -> Option<PathBuf> {
    runtime
        .state
        .get("artifactImage")
        .and_then(|image| image.get("ociLayout"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
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

    /// Consumes the staged `Image` payload. On macOS the OCI layout is loaded
    /// into Apple `container` immediately; on Linux the layout path is recorded
    /// and the bundle is built at `start` (the runtime runs a bundle, not an
    /// image). File/tree artifacts are deferred until the kind grows volume
    /// plumbing (same limitation as its KV file injection).
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
            if let ContainerRuntime::AppleContainer = ContainerRuntime::detect()? {
                apple_load_image(oci_layout, reference)?;
            }
            let record = json!({
                "ociLayout": oci_layout.display().to_string(),
                "reference": reference,
                "digest": staged.digest,
            });
            if let Some(state) = runtime.state.as_object_mut() {
                state.insert("artifactImage".into(), record);
            } else {
                runtime.state = json!({ "artifactImage": record });
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
        let runtime_kind = ContainerRuntime::detect()?;

        let mut cmd = match &runtime_kind {
            ContainerRuntime::AppleContainer => {
                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    program = "container",
                    "Starting container task"
                );
                let mut cmd = Command::new("container");
                cmd.args(build_apple_launch(allocation, &config));
                cmd
            }
            ContainerRuntime::Oci { binary } => {
                // The runtime runs a bundle, not an image: get the image's OCI
                // layout (from the artifact pipeline if staged, else pull it),
                // convert it to a bundle, then `run` it.
                let scratch = work_root(allocation, runtime);
                let layout = match staged_image_layout(runtime) {
                    Some(layout) => layout,
                    None => pull_image_layout(&config.image, &scratch.join("image"))?,
                };
                let bundle = scratch.join("bundle");
                oci_bundle::build_bundle(
                    &layout,
                    &bundle,
                    &config.command,
                    &config.env,
                    &container_name(allocation),
                )?;

                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    program = %binary,
                    "Starting container task"
                );
                let mut cmd = Command::new(binary);
                cmd.arg("run")
                    .arg("--bundle")
                    .arg(&bundle)
                    .arg(container_name(allocation));
                cmd
            }
        };

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
        // Both runtimes keep the container alive when the launching CLI is
        // killed (Apple `container` is daemon-backed; an OCI runtime reparents
        // the container init when the foreground `run` is killed). Tear it down
        // explicitly by name first. Best-effort: an already-exited `--rm` /
        // deleted container is gone, which is success.
        if let Ok(runtime_kind) = ContainerRuntime::detect() {
            let name = container_name(allocation);
            let teardown: Option<(&str, Vec<String>)> = match &runtime_kind {
                ContainerRuntime::AppleContainer => {
                    Some(("container", vec!["stop".into(), name.clone()]))
                }
                ContainerRuntime::Oci { binary } => Some((
                    binary.as_str(),
                    vec!["delete".into(), "--force".into(), name.clone()],
                )),
            };
            if let Some((program, args)) = teardown {
                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    runtime = program,
                    "Tearing down container by name"
                );
                let _ = Command::new(program).args(&args).output();
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
            "env": { "MODE": "test" }
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
    fn apple_launch_terminates_args_with_separator() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["ls", "-l"],
            "env": { "MODE": "test" }
        }))?;
        let args = build_apple_launch(&alloc(), &config);
        assert_eq!(args[0], "run");
        assert!(args.contains(&"--rm".to_string()));
        assert!(args.contains(&"--env".to_string()));
        assert!(args.contains(&"MODE=test".to_string()));
        let sep = args
            .iter()
            .position(|a| a == "--")
            .expect("separator present");
        let image_idx = args
            .iter()
            .position(|a| a == "alpine")
            .expect("image present");
        assert_eq!(sep, image_idx + 1);
        assert_eq!(args[sep + 1], "ls");
        Ok(())
    }

    #[test]
    fn staged_image_layout_reads_state() {
        let mut runtime = TaskRuntime {
            state: json!({ "artifactImage": { "ociLayout": "/tmp/x/layout" } }),
            ..TaskRuntime::default()
        };
        assert_eq!(
            staged_image_layout(&runtime),
            Some(PathBuf::from("/tmp/x/layout"))
        );
        runtime.state = json!({});
        assert_eq!(staged_image_layout(&runtime), None);
    }

    fn container_task(name: &str, command: Value) -> TaskConfig {
        // Override to a Docker Hub mirror when docker.io anonymous pulls are
        // rate-limited (e.g. LEGION_CONTAINER_TEST_IMAGE=mirror.gcr.io/library/alpine:latest).
        let image = std::env::var("LEGION_CONTAINER_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        serde_json::from_value(json!({
            "name": name,
            "kind": "container",
            "config": { "image": image, "command": command },
        }))
        .expect("task config deserializes")
    }

    /// End-to-end Linux run via an OCI runtime: pull alpine through the shared
    /// fetcher, build a bundle, run it, and assert stdout/stderr log capture.
    /// Point `LEGION_OCI_RUNTIME` at an installed runtime (`runc`/`crun`/
    /// `youki`). Override the image with `LEGION_OCI_TEST_IMAGE`.
    ///
    /// Run with:
    /// `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_runs_container \
    ///   -- --ignored --nocapture`
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_runs_container_and_captures_logs() -> R<()> {
        let plugin = ContainerTaskPlugin::default();
        let allocation = alloc();
        let task = container_task(
            "logtest",
            json!(["/bin/sh", "-c", "echo stdout-line; echo stderr-line 1>&2"]),
        );

        let tmp = tempfile::tempdir()?;
        let mut runtime = TaskRuntime {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskRuntime::default()
        };

        plugin.start(&allocation, &task, &mut runtime)?;
        let mut child = runtime.child.take().expect("child handle set");
        let status = child.wait()?;
        assert!(status.success(), "container exited cleanly: {status:?}");

        let stdout = std::fs::read_to_string(runtime.log_dir.as_ref().unwrap().join("stdout.log"))?;
        let stderr = std::fs::read_to_string(runtime.log_dir.as_ref().unwrap().join("stderr.log"))?;
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

    /// End-to-end stop: start a long-running container and confirm `stop()`
    /// tears it down via `<runtime> delete --force` (killing the foreground
    /// `run` alone leaves the container init reparented and alive).
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_stop_kills_running_container() -> R<()> {
        let binary = std::env::var(OCI_RUNTIME_ENV).unwrap_or_else(|_| DEFAULT_OCI_RUNTIME.into());
        let plugin = ContainerTaskPlugin::default();
        let allocation = alloc();
        let name = container_name(&allocation);
        let task = container_task("stoptest", json!(["/bin/sleep", "60"]));

        let tmp = tempfile::tempdir()?;
        let mut runtime = TaskRuntime {
            alloc_dir: Some(tmp.path().to_path_buf()),
            ..TaskRuntime::default()
        };
        plugin.start(&allocation, &task, &mut runtime)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let running = Command::new(&binary).args(["state", &name]).output()?;
        assert!(
            running.status.success(),
            "container should exist before stop"
        );

        plugin.stop(&allocation, &task, &mut runtime)?;
        std::thread::sleep(std::time::Duration::from_secs(1));

        let after = Command::new(&binary).args(["state", &name]).output()?;
        assert!(
            !after.status.success(),
            "container should be gone after stop"
        );
        Ok(())
    }
}
