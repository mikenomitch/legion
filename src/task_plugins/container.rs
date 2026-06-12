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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerRuntimeKind {
    AppleContainer,
    Youki,
}

impl ContainerRuntimeKind {
    fn detect() -> R<Self> {
        if cfg!(target_os = "macos") {
            Ok(Self::AppleContainer)
        } else if cfg!(target_os = "linux") {
            Ok(Self::Youki)
        } else {
            bail!("container tasks are not supported on this OS");
        }
    }

    fn binary(&self) -> &'static str {
        match self {
            Self::AppleContainer => "container",
            Self::Youki => "youki",
        }
    }
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
        ContainerRuntimeKind::AppleContainer => {
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
            args.push("--".to_string());
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
            args.push("--".to_string());
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
        ContainerRuntimeKind::AppleContainer => {
            // `container image load` consumes an OCI archive: tar the layout.
            let archive_path = oci_layout.with_extension("oci.tar");
            let archive = std::fs::File::create(&archive_path)
                .with_context(|| format!("create image archive {}", archive_path.display()))?;
            let mut builder = tar::Builder::new(archive);
            builder
                .append_dir_all(".", oci_layout)
                .context("archive OCI layout")?;
            builder.finish().context("finish OCI layout archive")?;

            let output = Command::new("container")
                .arg("image")
                .arg("load")
                .arg("--input")
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
}
