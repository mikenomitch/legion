use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, oci_bundle, spawn_pipe, spawn_pty,
    spawn_task_process, ExecOptions, ExecSession, ExecSupport, TaskInstance, TaskPlugin,
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

/// Apple's container CLI (macOS). Installed via `brew install container`.
const APPLE_CONTAINER_BIN: &str = "container";

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

    /// Whether this runtime can exec into a running container. Apple's
    /// `container exec` works; the low-level OCI runtimes need different flag
    /// handling and are deferred (Q1 in the plan), so they report unsupported.
    fn supports_exec(&self) -> bool {
        matches!(self, Self::AppleContainer)
    }

    /// Confirms the runtime can actually launch a container, with an error the
    /// operator can act on. Without this the failure surfaces as a bare
    /// "No such file or directory" from `spawn`, or — worse on macOS, where
    /// the CLI is installed but its daemon is not running — as a started
    /// allocation whose container exits immediately.
    fn preflight(&self) -> R<()> {
        match self {
            Self::AppleContainer => {
                // One call covers both: a missing CLI errors as NotFound, and a
                // stopped daemon exits non-zero.
                let status = Command::new(APPLE_CONTAINER_BIN)
                    .args(["system", "status"])
                    .output();
                match status {
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => bail!(
                        "the Apple `container` runtime is not installed on this node \
                         (install it with `brew install container`)"
                    ),
                    Err(err) => bail!("failed to probe the Apple `container` runtime: {err}"),
                    Ok(output) if !output.status.success() => bail!(
                        "the Apple `container` service is not running on this node \
                         (start it with `container system start`): {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ),
                    Ok(_) => Ok(()),
                }
            }
            Self::Oci { binary } => match Command::new(binary).arg("--version").output() {
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => bail!(
                    "OCI runtime `{binary}` is not installed on this node \
                     (install youki/runc/crun, or set {OCI_RUNTIME_ENV} to one that is)"
                ),
                Err(err) => bail!("failed to probe OCI runtime `{binary}`: {err}"),
                // A runtime that answers `--version` at all is present; some
                // report a non-zero status for unrelated reasons, which is not
                // worth failing the allocation over.
                Ok(_) => Ok(()),
            },
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
fn work_root(allocation: &Allocation, instance: &TaskInstance) -> PathBuf {
    instance
        .alloc_dir
        .clone()
        .or_else(|| instance.task_dir.clone())
        .unwrap_or_else(|| std::env::temp_dir().join(format!("legion-container-{}", allocation.id)))
        .join(".container")
}

/// A positive numeric entry from the task's free-form `resources` map.
/// Legion's scheduler reads `memory` as MiB and `cores` as a core count
/// (`src/manager/scheduler.rs`), so those are the units used here.
fn resource_amount(resources: &Value, key: &str) -> Option<f64> {
    resources
        .get(key)
        .and_then(Value::as_f64)
        .filter(|amount| *amount > 0.0)
}

/// Apple `container run` launch (docker-compatible). `env` is the merged
/// Legion + task env (task values already win on conflicts).
///
/// Note there is no `--` terminator before the command: Apple's parser treats
/// `--` as the target executable and the container fails to start. It is not
/// needed either — everything after the image is captured as process
/// arguments, flags included.
fn build_apple_launch(
    allocation: &Allocation,
    task: &TaskConfig,
    config: &ContainerTaskConfig,
    env: &HashMap<String, String>,
    volumes: &[crate::task_plugins::MountedVolume],
    image: &str,
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        container_name(allocation),
        // The task's stdout/stderr are captured as its logs; Apple's image
        // fetch progress would otherwise be interleaved into stderr.log.
        "--progress".to_string(),
        "none".to_string(),
    ];

    // Declared resources become the guest VM's size. Apple `container` boots a
    // VM per container, so an unsized guest gets the runtime default (1 GiB /
    // 4 CPUs) no matter what the job asked the scheduler to reserve.
    if let Some(memory_mib) = resource_amount(&task.resources, "memory") {
        args.push("--memory".to_string());
        args.push(format!("{}M", memory_mib.round().max(1.0) as u64));
    }
    if let Some(cores) = resource_amount(&task.resources, "cores") {
        args.push("--cpus".to_string());
        args.push(format!("{}", cores.round().max(1.0) as u64));
    }

    // Assigned ports are host ports, but the container has its own IP inside a
    // VM, so they must be published to be reachable — by peers via service
    // discovery and by the worker's own health checks. `to` is the
    // container-side port when the job pinned one; otherwise the task is
    // expected to bind the host port it was handed via `LEGION_PORT_<NAME>`.
    if let Some(network) = allocation.network.as_ref() {
        for port in &network.ports {
            args.push("--publish".to_string());
            args.push(format!(
                "{}:{}",
                port.host_port,
                port.to.unwrap_or(port.host_port)
            ));
        }
    }

    // Sorted so the launch argv is stable across starts (HashMap order is not).
    let mut env: Vec<_> = env.iter().collect();
    env.sort_by_key(|(key, _)| *key);
    for (key, value) in env {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }
    // Docker-compatible `--volume host:dest[:ro]`. Volumes with no declared
    // path are skipped; admission rejects that for container tasks.
    for volume in volumes {
        let Some(destination) = volume.path.as_deref() else {
            continue;
        };
        args.push("--volume".to_string());
        let suffix = if volume.read_only { ":ro" } else { "" };
        args.push(format!(
            "{}:{destination}{suffix}",
            volume.host_path.display()
        ));
    }

    args.push(image.to_string());
    args.extend(config.command.clone());
    args
}

/// Best-effort removal of a container left behind under this allocation's
/// name. `run --name` fails outright when the name is taken, which happens
/// whenever a previous start was not torn down cleanly (worker crash, or a
/// `--rm` run killed before the runtime could reap it). Errors are ignored:
/// "no such container" is the normal case.
fn remove_stale_container(oci_runtime: &ContainerRuntime, name: &str) {
    let (program, args): (&str, Vec<&str>) = match oci_runtime {
        ContainerRuntime::AppleContainer => (APPLE_CONTAINER_BIN, vec!["rm", "--force", name]),
        ContainerRuntime::Oci { binary } => (binary.as_str(), vec!["delete", "--force", name]),
    };
    let _ = Command::new(program).args(&args).output();
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

    let output = Command::new(APPLE_CONTAINER_BIN)
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

/// Builds the argv that execs `command` inside the allocation's container.
fn build_exec_command(
    oci_runtime: &ContainerRuntime,
    allocation_id: i64,
    tty: bool,
    command: &[String],
) -> Vec<String> {
    let program = match oci_runtime {
        ContainerRuntime::AppleContainer => APPLE_CONTAINER_BIN.to_string(),
        ContainerRuntime::Oci { binary } => binary.clone(),
    };
    let name = format!("legion-{allocation_id}");
    let mut argv = vec![program, "exec".to_string()];
    if tty {
        argv.push("--tty".to_string());
        argv.push("--interactive".to_string());
    }
    argv.push(name);
    argv.extend(command.iter().cloned());
    argv
}

/// The OCI layout recorded on `instance.state` by `consume_artifact` when the
/// image was staged through the artifact pipeline.
fn staged_image_layout(instance: &TaskInstance) -> Option<PathBuf> {
    instance
        .state
        .get("artifactImage")
        .and_then(|image| image.get("ociLayout"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
}

/// The reference of the staged image, recorded alongside its layout. This is
/// the *pinned* reference the artifact pipeline resolved and imported, so
/// launching it (instead of the jobspec's tag) is what makes digest pinning
/// mean anything: a tag can move between the pin and the run.
fn staged_image_reference(instance: &TaskInstance) -> Option<String> {
    instance
        .state
        .get("artifactImage")
        .and_then(|image| image.get("reference"))
        .and_then(Value::as_str)
        .filter(|reference| !reference.is_empty())
        .map(str::to_string)
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

    /// Reported per node so operators can see which nodes can actually run
    /// containers. The key is omitted (rather than reported as failing) when
    /// the runtime is unusable: a node without a container runtime is still a
    /// perfectly healthy node for every other kind, and node health is the
    /// conjunction of every check it publishes.
    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        match ContainerRuntime::detect().and_then(|oci_runtime| oci_runtime.preflight()) {
            Ok(()) => {
                map.insert("container".into(), Value::String("pass".into()));
            }
            Err(err) => {
                debug!(error = %err, "Container runtime unavailable on this node");
            }
        }
        map
    }

    fn supports_kv_env_injection(&self) -> bool {
        true
    }

    // Identity delivery mirrors the container runtime’s existing injection surface:
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

    fn exec_support(&self) -> ExecSupport {
        // Exec is sandboxed inside the container (host_level = false). Whether
        // it is available at all depends on the detected runtime.
        let supported = ContainerRuntime::detect()
            .map(|rt| rt.supports_exec())
            .unwrap_or(false);
        ExecSupport {
            exec: supported,
            pty: supported,
            host_level: false,
        }
    }

    fn exec(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
        mut opts: ExecOptions,
    ) -> R<Box<dyn ExecSession>> {
        let oci_runtime = ContainerRuntime::detect()?;
        if !oci_runtime.supports_exec() {
            bail!("this container runtime does not support exec yet");
        }
        // Delegate to the runtime's `exec` subcommand against the running
        // container. The command runs inside the container's namespaces, so
        // it is genuinely sandboxed exec (unlike the exec runtime).
        opts.command = build_exec_command(&oci_runtime, allocation.id, opts.tty, &opts.command);

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            tty = opts.tty,
            "Opening exec session for container task"
        );

        if opts.tty {
            spawn_pty(&opts)
        } else {
            spawn_pipe(&opts)
        }
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
    /// image). File/tree artifacts are deferred until the runtime grows volume
    /// plumbing (same limitation as its KV file injection).
    fn consume_artifact(
        &self,
        staged: &crate::artifacts::StagedArtifact,
        instance: &mut TaskInstance,
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
            if let Some(state) = instance.state.as_object_mut() {
                state.insert("artifactImage".into(), record);
            } else {
                instance.state = json!({ "artifactImage": record });
            }
        }
        Ok(())
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
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
        instance: &mut TaskInstance,
    ) -> R<()> {
        let config = ContainerTaskConfig::from_value(&task.config)?;
        let oci_runtime = ContainerRuntime::detect()?;
        oci_runtime.preflight()?;

        // Legion-provided env (allocation identity + assigned ports) first;
        // user-declared env overrides on key conflicts.
        let mut merged_env: HashMap<String, String> =
            crate::task_plugins::allocation_env(allocation, task)
                .into_iter()
                .collect();
        merged_env.extend(config.env.clone());
        // Volume vars carry the IN-CONTAINER destination (the worker resolved
        // them that way for this kind), so they are meaningful inside the
        // sandbox — unlike LEGION_ALLOC_DIR and friends, which name host paths
        // the container cannot see.
        for (key, value) in &instance.extra_env {
            if key.starts_with("LEGION_VOLUME_") || key == "LEGION_STORAGE_DIR" {
                merged_env.insert(key.clone(), value.clone());
            }
        }

        // A container still holding this allocation's name would make `run`
        // fail; restarts and worker crashes both leave one behind.
        remove_stale_container(&oci_runtime, &container_name(allocation));

        let mut cmd = match &oci_runtime {
            ContainerRuntime::AppleContainer => {
                // `consume_artifact` already imported the staged layout, so
                // launching its pinned reference runs exactly the image the
                // artifact pipeline resolved — and needs no registry pull.
                // Without a staged image, the jobspec's own reference is used
                // and Apple `container` pulls it itself.
                let image =
                    staged_image_reference(instance).unwrap_or_else(|| config.image.clone());
                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    program = APPLE_CONTAINER_BIN,
                    %image,
                    "Starting container task"
                );
                let mut cmd = Command::new(APPLE_CONTAINER_BIN);
                cmd.args(build_apple_launch(
                    allocation,
                    task,
                    &config,
                    &merged_env,
                    &instance.volumes,
                    &image,
                ));
                cmd
            }
            ContainerRuntime::Oci { binary } => {
                // The runtime runs a bundle, not an image: get the image's OCI
                // layout (from the artifact pipeline if staged, else pull it),
                // convert it to a bundle, then `run` it.
                let scratch = work_root(allocation, instance);
                let pulled_layout = scratch.join("image").join("layout");
                let layout = match staged_image_layout(instance) {
                    Some(layout) => layout,
                    // A previous start of this allocation already pulled the
                    // layout; reuse it so in-place restarts don't refetch.
                    // `index.json` is written last by the fetcher, so its
                    // presence marks a complete pull.
                    None if pulled_layout.join("index.json").is_file() => pulled_layout,
                    None => pull_image_layout(&config.image, &scratch.join("image"))?,
                };
                let bundle = scratch.join("bundle");
                oci_bundle::build_bundle(
                    &layout,
                    &bundle,
                    &config.command,
                    &merged_env,
                    &container_name(allocation),
                    &instance.volumes,
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

        configure_task_log_capture(&mut cmd, instance)?;
        let child = spawn_task_process(&mut cmd).with_context(|| {
            format!(
                "Failed to spawn container task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        instance.pid = Some(child.id());
        instance.child = Some(child);
        Ok(())
    }

    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        // Both runtimes keep the container alive when the launching CLI is
        // killed (Apple `container` is daemon-backed; an OCI runtime reparents
        // the container init when the foreground `run` is killed). Tear it down
        // explicitly by name first. Best-effort: an already-exited `--rm` /
        // deleted container is gone, which is success.
        if let Ok(oci_runtime) = ContainerRuntime::detect() {
            let name = container_name(allocation);
            let (program, args): (&str, Vec<&str>) = match &oci_runtime {
                // `stop` signals the container's init and waits, so the
                // workload gets a chance to shut down; `--rm` then reaps it.
                ContainerRuntime::AppleContainer => (APPLE_CONTAINER_BIN, vec!["stop", &name]),
                ContainerRuntime::Oci { binary } => {
                    (binary.as_str(), vec!["delete", "--force", &name])
                }
            };
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                oci_runtime = program,
                "Tearing down container by name"
            );
            let _ = Command::new(program).args(&args).output();
        }

        if let Some(mut child) = instance.child.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Stopping container task"
            );
            let _ = child.kill();
            let _ = child.wait();
            instance.pid = None;
        } else if let Some(pid) = instance.pid.take() {
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
        instance: &mut TaskInstance,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Destroying container task"
        );
        self.stop(allocation, task, instance)?;
        // Drop the image/bundle scratch. Best-effort: when it lives under the
        // worker's alloc dir the worker removes that tree anyway, but the
        // temp-dir fallback (no alloc dir threaded through) would leak a
        // multi-hundred-MB rootfs without this.
        let scratch = work_root(allocation, instance);
        if scratch.exists() {
            if let Err(err) = std::fs::remove_dir_all(&scratch) {
                warn!(
                    allocation_id = allocation.id,
                    error = %err,
                    "Failed to remove container scratch dir"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A minimal container task carrying the given `resources` (and optionally
    /// a different name), for launch-argv assertions.
    fn task_with(resources: Value, name: Option<&str>) -> TaskConfig {
        serde_json::from_value(json!({
            "name": name.unwrap_or("web"),
            "runtime": "container",
            "config": { "image": "alpine", "command": ["echo", "ok"] },
            "resources": resources,
        }))
        .expect("task config deserializes")
    }

    fn alloc() -> Allocation {
        alloc_with_id(7)
    }

    /// End-to-end tests run concurrently and name their container after the
    /// allocation id, so each needs its own.
    fn alloc_with_id(id: i64) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
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

    /// A missing runtime binary must name itself and how to install it — the
    /// bare `spawn` failure it replaces was "No such file or directory".
    #[test]
    fn preflight_reports_a_missing_oci_runtime() {
        let oci_runtime = ContainerRuntime::Oci {
            binary: "legion-no-such-oci-runtime".into(),
        };
        let err = oci_runtime.preflight().expect_err("missing runtime");
        let message = err.to_string();
        assert!(message.contains("legion-no-such-oci-runtime"), "{message}");
        assert!(message.contains(OCI_RUNTIME_ENV), "{message}");
    }

    /// On a node whose runtime actually works, preflight must not fail the
    /// allocation. Gated on a real runtime operation rather than on the binary
    /// existing: macOS ships the CLI and the daemon separately, and a node with
    /// the CLI but no running daemon genuinely cannot start containers — which
    /// is the case preflight exists to report. Skipped where that operation
    /// does not succeed (most CI machines).
    #[test]
    fn preflight_passes_when_the_runtime_is_usable() {
        let oci_runtime = ContainerRuntime::detect().expect("supported OS");
        let (probe, args) = match &oci_runtime {
            // `ls` needs the daemon, so it fails exactly where a launch would.
            ContainerRuntime::AppleContainer => (APPLE_CONTAINER_BIN, ["ls"]),
            ContainerRuntime::Oci { binary } => (binary.as_str(), ["--version"]),
        };
        let usable = Command::new(probe)
            .args(args)
            .output()
            .is_ok_and(|output| output.status.success());
        if !usable {
            return;
        }
        assert!(oci_runtime.preflight().is_ok(), "usable runtime must pass");
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

    /// Apple's parser reads `--` as the target executable ("failed to find
    /// target executable --"), and captures everything after the image as
    /// process arguments regardless, so the command must follow the image
    /// directly — flags and all.
    #[test]
    fn apple_launch_passes_command_after_image_without_separator() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["ls", "-l"],
            "env": { "MODE": "test" }
        }))?;
        let args = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            &config.image,
        );
        assert_eq!(args[0], "run");
        assert!(args.contains(&"--rm".to_string()));
        assert!(args.contains(&"--env".to_string()));
        assert!(args.contains(&"MODE=test".to_string()));
        assert!(!args.contains(&"--".to_string()), "no separator: {args:?}");
        let image_idx = args
            .iter()
            .position(|a| a == "alpine")
            .expect("image present");
        assert_eq!(args[image_idx + 1..], ["ls".to_string(), "-l".to_string()]);
        Ok(())
    }

    /// Image-fetch progress would otherwise be interleaved into the task's
    /// captured stderr.
    #[test]
    fn apple_launch_silences_runtime_progress() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let args = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            &config.image,
        );
        let idx = args
            .iter()
            .position(|a| a == "--progress")
            .expect("progress flag present");
        assert_eq!(args[idx + 1], "none");
        Ok(())
    }

    #[test]
    fn apple_launch_maps_declared_resources() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let task = task_with(json!({ "memory": 256, "cores": 2 }), None);
        let args = build_apple_launch(&alloc(), &task, &config, &config.env, &[], &config.image);
        let memory = args
            .iter()
            .position(|a| a == "--memory")
            .expect("memory flag");
        assert_eq!(args[memory + 1], "256M");
        let cpus = args.iter().position(|a| a == "--cpus").expect("cpus flag");
        assert_eq!(args[cpus + 1], "2");

        // Unset resources leave the runtime's own defaults alone.
        let bare = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            &config.image,
        );
        assert!(!bare.contains(&"--memory".to_string()));
        assert!(!bare.contains(&"--cpus".to_string()));
        Ok(())
    }

    /// The container has its own IP inside a VM, so assigned host ports only
    /// reach it when published.
    #[test]
    fn apple_launch_publishes_assigned_ports() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let mut allocation = alloc();
        allocation.network = Some(crate::structs::network::AllocNetwork {
            address: Some("127.0.0.1".into()),
            ports: vec![
                crate::structs::network::AllocPort {
                    name: "http".into(),
                    host_port: 21000,
                    to: Some(8080),
                },
                crate::structs::network::AllocPort {
                    name: "metrics".into(),
                    host_port: 21001,
                    to: None,
                },
            ],
        });
        let args = build_apple_launch(
            &allocation,
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            &config.image,
        );
        assert!(args.contains(&"21000:8080".to_string()), "{args:?}");
        // No container-side port declared: the task binds the port it was
        // handed via LEGION_PORT_METRICS, so both sides are the host port.
        assert!(args.contains(&"21001:21001".to_string()), "{args:?}");
        Ok(())
    }

    /// A staged image is launched by the reference the artifact pipeline
    /// pinned, not the jobspec's tag — otherwise a moving tag could run a
    /// different image than the one that was resolved and imported.
    #[test]
    fn staged_image_reference_overrides_configured_tag() -> R<()> {
        let instance = TaskInstance {
            state: json!({
                "artifactImage": {
                    "ociLayout": "/tmp/x/layout",
                    "reference": "docker.io/library/alpine@sha256:abc",
                }
            }),
            ..TaskInstance::default()
        };
        let image = staged_image_reference(&instance).expect("staged reference");
        assert_eq!(image, "docker.io/library/alpine@sha256:abc");

        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine:latest",
            "command": ["echo", "ok"]
        }))?;
        let args = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            &image,
        );
        assert!(args.contains(&image), "{args:?}");
        assert!(!args.contains(&"alpine:latest".to_string()), "{args:?}");

        // No staged image: the jobspec's own reference is what runs.
        assert_eq!(staged_image_reference(&TaskInstance::default()), None);
        Ok(())
    }

    #[test]
    fn staged_image_layout_reads_state() {
        let mut instance = TaskInstance {
            state: json!({ "artifactImage": { "ociLayout": "/tmp/x/layout" } }),
            ..TaskInstance::default()
        };
        assert_eq!(
            staged_image_layout(&instance),
            Some(PathBuf::from("/tmp/x/layout"))
        );
        instance.state = json!({});
        assert_eq!(staged_image_layout(&instance), None);
    }

    fn container_task(name: &str, command: Value) -> TaskConfig {
        // Override to a Docker Hub mirror when docker.io anonymous pulls are
        // rate-limited (e.g. LEGION_CONTAINER_TEST_IMAGE=mirror.gcr.io/library/alpine:latest).
        let image = std::env::var("LEGION_CONTAINER_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        serde_json::from_value(json!({
            "name": name,
            "runtime": "container",
            "config": { "image": image, "command": command },
        }))
        .expect("task config deserializes")
    }

    /// End-to-end macOS run via Apple `container`: launch alpine, assert
    /// stdout/stderr capture, that the declared resources sized the guest, and
    /// that the runtime's own progress output stayed out of the task's logs.
    ///
    /// Run with:
    /// `cargo test --lib apple_container_runs -- --ignored --nocapture`
    #[test]
    #[ignore = "requires Apple `container` (macOS) + network"]
    fn apple_container_runs_and_captures_logs() -> R<()> {
        let plugin = ContainerTaskPlugin::default();
        let allocation = alloc_with_id(9001);
        let mut task = container_task(
            "logtest",
            json!([
                "/bin/sh",
                "-c",
                "echo stdout-line; echo stderr-line 1>&2; nproc"
            ]),
        );
        task.resources = json!({ "memory": 256, "cores": 1 });

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };

        plugin.start(&allocation, &task, &mut instance)?;
        let status = instance.child.take().expect("child handle set").wait()?;
        assert!(status.success(), "container exited cleanly: {status:?}");

        let log_dir = instance.log_dir.as_ref().expect("log dir");
        let stdout = std::fs::read_to_string(log_dir.join("stdout.log"))?;
        let stderr = std::fs::read_to_string(log_dir.join("stderr.log"))?;
        assert!(stdout.contains("stdout-line"), "stdout: {stdout:?}");
        assert!(stderr.contains("stderr-line"), "stderr: {stderr:?}");
        // Apple reports one CPU more than requested (the guest's own
        // housekeeping core), so an unsized guest's default of 4 would show 5.
        assert!(
            stdout.contains("\n2\n") || stdout.trim_end().ends_with('2'),
            "guest sized to the requested core count: {stdout:?}"
        );
        assert!(
            !stderr.contains("Fetching image"),
            "runtime progress must stay out of task logs: {stderr:?}"
        );
        Ok(())
    }

    /// End-to-end macOS stop: Apple `container` is daemon-backed, so killing
    /// the foreground `run` leaves the container up — `stop()` must tear it
    /// down by name. Also covers the restart path, since a surviving container
    /// would make the next `run --name` fail.
    #[test]
    #[ignore = "requires Apple `container` (macOS) + network"]
    fn apple_container_stop_kills_running_container() -> R<()> {
        let plugin = ContainerTaskPlugin::default();
        let allocation = alloc_with_id(9002);
        let name = container_name(&allocation);
        let task = container_task("stoptest", json!(["/bin/sleep", "120"]));

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(20));

        let running = Command::new(APPLE_CONTAINER_BIN)
            .args(["inspect", &name])
            .output()?;
        assert!(
            running.status.success(),
            "container should exist before stop"
        );

        plugin.stop(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let after = Command::new(APPLE_CONTAINER_BIN)
            .args(["inspect", &name])
            .output()?;
        assert!(
            !after.status.success(),
            "container should be gone after stop"
        );
        Ok(())
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
        let allocation = alloc_with_id(9003);
        // Also proves the host's DNS files are bind-mounted in (host networking
        // is useless without resolver config) and that env reaches the task.
        let task = container_task(
            "logtest",
            json!([
                "/bin/sh",
                "-c",
                "echo stdout-line; echo stderr-line 1>&2; \
                 test -s /etc/resolv.conf || { echo missing-resolv-conf 1>&2; exit 1; }; \
                 test -e /etc/hosts || { echo missing-hosts 1>&2; exit 1; }"
            ]),
        );

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };

        plugin.start(&allocation, &task, &mut instance)?;
        let mut child = instance.child.take().expect("child handle set");
        let status = child.wait()?;
        assert!(status.success(), "container exited cleanly: {status:?}");

        let stdout =
            std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stdout.log"))?;
        let stderr =
            std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stderr.log"))?;
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
        let allocation = alloc_with_id(9004);
        let name = container_name(&allocation);
        let task = container_task("stoptest", json!(["/bin/sleep", "60"]));

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let running = Command::new(&binary).args(["state", &name]).output()?;
        assert!(
            running.status.success(),
            "container should exist before stop"
        );

        plugin.stop(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(1));

        let after = Command::new(&binary).args(["state", &name]).output()?;
        assert!(
            !after.status.success(),
            "container should be gone after stop"
        );
        Ok(())
    }

    #[test]
    fn launch_command_includes_legion_env() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let mut env: HashMap<String, String> = config.env.clone();
        env.insert("LEGION_PORT_HTTP".to_string(), "20000".to_string());
        let args = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &env,
            &[],
            &config.image,
        );
        assert!(args.contains(&"LEGION_PORT_HTTP=20000".to_string()));
        Ok(())
    }
}
