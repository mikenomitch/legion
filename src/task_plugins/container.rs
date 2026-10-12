use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, oci_bundle, oci_image, reap_task_process,
    spawn_pipe, spawn_pty, spawn_task_process, ExecOptions, ExecSession, ExecSupport, TaskInstance,
    TaskPlugin,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;
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

    /// Files to materialize in the task directory before the container
    /// starts, visible inside at `/legion/task/<path>`.
    ///
    /// This is the shape KV file injection renders into (see
    /// `kvInjections.files` in docs/kv-store.md); paths are relative to the
    /// task directory, and an absolute path under `/legion/task` is accepted
    /// and treated the same way.
    #[serde(default)]
    files: Vec<ContainerInjectedFile>,

    /// The container's privileges (Linux). Today this is the capability set;
    /// see `security.capabilities`.
    #[serde(default)]
    security: ContainerSecurity,

    /// Resource limits (`setrlimit`) for the container's process (Linux), by
    /// lowercase name: `nofile`, `nproc`, `memlock`, `core`, `stack`, ...
    /// Each value is a number (soft and hard alike), `"unlimited"`, or
    /// `{ soft, hard }`. Each declared limit replaces the default of the same
    /// name; the only default is `nofile = 1048576` (lowered to the host's
    /// `fs.nr_open` when that is smaller).
    #[serde(default)]
    ulimits: std::collections::BTreeMap<String, Value>,

    /// Additional plugin-specific fields reserved for forward compatibility.
    #[serde(default, flatten)]
    #[schemars(skip)]
    _extra: HashMap<String, Value>,
}

/// `config.security`: what the container process may do.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ContainerSecurity {
    /// Adjustments to the default Linux capability set (Docker's default
    /// set, less `NET_RAW`), with `docker run --cap-add/--cap-drop`
    /// semantics. Names are case-insensitive, with or without the `CAP_`
    /// prefix; `ALL` means every capability.
    #[serde(default)]
    capabilities: CapabilityAdjustments,
}

/// `config.security.capabilities`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CapabilityAdjustments {
    /// Capabilities to grant on top of the defaults, e.g. `["NET_RAW"]`.
    /// `["ALL"]` grants every capability except those in `drop`.
    #[serde(default)]
    add: Vec<String>,
    /// Capabilities to remove from the defaults, e.g. `["MKNOD"]`.
    /// `["ALL"]` starts from nothing, so the container gets exactly `add`.
    #[serde(default)]
    drop: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct ContainerInjectedFile {
    /// Destination, relative to `/legion/task` (an absolute path under it is
    /// also accepted).
    path: String,
    /// Octal file mode, e.g. `"0640"`.
    #[serde(default)]
    mode: Option<String>,
    content: String,
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
        self.process_policy()?;
        Ok(())
    }

    /// The bundle's capability set and rlimits for this task: the defaults,
    /// adjusted by `security.capabilities` and `ulimits`.
    fn process_policy(&self) -> R<oci_bundle::ProcessPolicy> {
        let capabilities = &self.security.capabilities;
        Ok(oci_bundle::ProcessPolicy {
            capabilities: oci_bundle::resolve_capabilities(&capabilities.add, &capabilities.drop)?,
            rlimits: oci_bundle::resolve_rlimits(&self.ulimits)?,
        })
    }

    /// Whether the task adjusts anything only the Linux OCI path applies.
    fn adjusts_linux_process(&self) -> bool {
        !self.security.capabilities.add.is_empty()
            || !self.security.capabilities.drop.is_empty()
            || !self.ulimits.is_empty()
    }
}

/// The smallest VM Apple `container` boots, in MiB. Below it, `container run`
/// refuses the launch, and the error used to surface only in the task's
/// stderr.
pub(crate) const APPLE_CONTAINER_MIN_MEMORY_MIB: u64 = 200;

/// The guest size an Apple `container` launch would get from `resources`:
/// `memoryMax` when set (the ceiling the task may use), else `memory`.
/// `None` when neither is declared, which leaves the runtime's own default.
fn apple_guest_memory_mib(resources: &Value) -> Option<f64> {
    resource_amount(resources, crate::resources::MEMORY_MAX_KEY)
        .or_else(|| resource_amount(resources, "memory"))
}

/// Whether a task's own constraints pin it to macOS nodes (`os = "macos"`),
/// which is the only case where the manager knows at submit that the task
/// will run on Apple `container`.
pub(crate) fn constrained_to_macos(task: &TaskConfig) -> bool {
    crate::constraints::ConstraintSet::new(&task.constraints)
        .map(|set| {
            set.equality_bindings()
                .get("os")
                .and_then(Value::as_str)
                .is_some_and(|os| os == "macos")
        })
        .unwrap_or(false)
}

/// The error for a guest below Apple `container`'s floor. `what` names the
/// field that sized it, so the message points at the line to change.
pub(crate) fn apple_memory_floor_error(task: &str, what: &str, mib: f64) -> anyhow::Error {
    anyhow!(
        "task {task} sizes its macOS VM at {mib} MiB ({what}), below the \
         {APPLE_CONTAINER_MIN_MEMORY_MIB} MiB Apple `container` requires; raise it to at least \
         {APPLE_CONTAINER_MIN_MEMORY_MIB}, or constrain the task to Linux nodes"
    )
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

    /// Whether this runtime can exec into a running container. Both dialects
    /// do: Apple's `container exec`, and `<runtime> exec` on youki/runc/crun.
    /// Reported from runtime detection alone — no probe of the binary (an
    /// exotic OCI runtime without `exec` fails at session open with its own
    /// error; see the plan's Q1).
    fn supports_exec(&self) -> bool {
        true
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

/// Fails a macOS start whose VM would be under Apple `container`'s floor,
/// with a message naming the floor and the field to raise. Submit already
/// rejects this for a task constrained to macOS; this catches the rest, which
/// only become macOS tasks at placement. A no-op on Linux, where `memory` is a
/// cgroup limit and has no floor.
fn check_apple_memory_floor(oci_runtime: &ContainerRuntime, task: &TaskConfig) -> R<()> {
    if *oci_runtime != ContainerRuntime::AppleContainer {
        return Ok(());
    }
    match apple_guest_memory_mib(&task.resources) {
        Some(mib) if mib < APPLE_CONTAINER_MIN_MEMORY_MIB as f64 => Err(apple_memory_floor_error(
            &task.name,
            "resources.memoryMax, else resources.memory",
            mib,
        )),
        _ => Ok(()),
    }
}

/// Deterministic per-allocation container name / OCI container id.
fn container_name(allocation: &Allocation) -> String {
    format!("legion-{}", allocation.id)
}

/// Where the allocation's host task directory is mounted inside the
/// container. Everything the worker stages — artifacts, KV-rendered files,
/// identity tokens — is readable here, and refreshes are visible without a
/// restart because the *directory* is mounted, never individual files.
const TASK_DIR_MOUNT: &str = crate::constants::CONTAINER_TASK_DIR;

/// The worker-injected env a container can actually use, translated to
/// in-container paths.
///
/// This extends the convention volumes already set (in-task destinations for
/// isolating runtimes) to the rest of the worker's env:
///
/// - `LEGION_TASK_DIR` becomes [`TASK_DIR_MOUNT`];
/// - `LEGION_ARTIFACT_<NAME>` has the host task-dir prefix swapped for it;
/// - `LEGION_ALLOC_DIR` is withheld — the alloc dir is *not* mounted, and
///   pointing env at an invisible path is the bug this translation exists to
///   fix. It returns if the shared-alloc-dir proposal ever lands.
///
/// An artifact staged outside the task dir is omitted rather than leaked as a
/// dead host path. That cannot happen today; it is defense against a future
/// stager that stages elsewhere.
fn translated_worker_env(instance: &TaskInstance) -> Vec<(String, String)> {
    let mut translated = Vec::new();
    let task_dir = instance.task_dir.as_ref();
    for (key, value) in &instance.extra_env {
        if key == "LEGION_ALLOC_DIR" {
            continue;
        }
        if key == "LEGION_TASK_DIR" {
            translated.push((key.clone(), TASK_DIR_MOUNT.to_string()));
            continue;
        }
        if let Some(name) = key.strip_prefix("LEGION_ARTIFACT_") {
            let Some(in_task) = task_dir.and_then(|dir| translate_task_path(dir, value)) else {
                debug!(
                    artifact = name,
                    "Artifact staged outside the task directory is not visible in the container"
                );
                continue;
            };
            translated.push((key.clone(), in_task));
            continue;
        }
        // Volume vars already carry in-container destinations (the worker
        // resolved them that way for this runtime).
        if key.starts_with("LEGION_VOLUME_") || key == "LEGION_STORAGE_DIR" {
            translated.push((key.clone(), value.clone()));
        }
    }
    translated
}

/// Write the task's declared files into the host task directory, at the
/// destinations they will be readable from inside the container.
///
/// The path rule is re-checked here rather than trusted from admission — the
/// same two-layer stance identity naming takes — so a spec that reached this
/// worker by any other route still cannot write outside the task dir.
fn materialize_task_files(files: &[ContainerInjectedFile], instance: &TaskInstance) -> R<()> {
    if files.is_empty() {
        return Ok(());
    }
    let task_dir = instance
        .task_dir
        .as_ref()
        .context("container task declares files but the worker threaded no task directory")?;

    for file in files {
        let relative =
            crate::task_plugins::resolve_task_dir_destination(TASK_DIR_MOUNT, &file.path)
                .map_err(|err| anyhow!("declared file path {err}"))?;
        let destination = task_dir.join(relative);
        crate::worker::identity::write_token_file(
            &destination,
            &file.content,
            crate::task_plugins::injected_file_mode(file.mode.as_deref()),
        )
        .with_context(|| format!("write {}", destination.display()))?;
    }
    Ok(())
}

/// Swap a host path's task-dir prefix for the in-container mount point.
/// `None` when the path is not under the task dir at all.
fn translate_task_path(task_dir: &std::path::Path, host_path: &str) -> Option<String> {
    let relative = std::path::Path::new(host_path)
        .strip_prefix(task_dir)
        .ok()?;
    let mut mounted = PathBuf::from(TASK_DIR_MOUNT);
    mounted.push(relative);
    Some(mounted.display().to_string())
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
    task_dir: Option<&std::path::Path>,
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
    // 4 CPUs) no matter what the job asked the scheduler to reserve. A
    // `memoryMax` is the ceiling the task may use, so it sizes the VM when
    // set, the same rule the `sandbox` runtime follows.
    if let Some(memory_mib) = resource_amount(&task.resources, crate::resources::MEMORY_MAX_KEY)
        .or_else(|| resource_amount(&task.resources, "memory"))
    {
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
            // With an ingress chain the datapath owns the advertised port and
            // the container is published on its private twin.
            args.push(format!(
                "{}:{}",
                port.bind_port(),
                port.to.unwrap_or(port.bind_port())
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
    // The allocation's task directory, before declared volumes so an
    // explicit volume can still shadow it — the same precedence the mount
    // list already gives its defaults.
    if let Some(task_dir) = task_dir {
        args.push("--volume".to_string());
        args.push(format!("{}:{TASK_DIR_MOUNT}", task_dir.display()));
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

/// Builds the argv that delivers `signal` to the container's init process.
///
/// ```text
/// Apple:                 container kill --signal SIGHUP legion-<alloc>
/// OCI (youki/runc/crun): <runtime> kill legion-<alloc> SIGHUP
/// ```
///
/// The signal *name* is passed through rather than a number, so each runtime
/// does its own resolution. The name is canonicalized first
/// ([`crate::signals::canonical_signal_name`]) so `hup` and `SIGHUP` reach the
/// CLI identically.
fn build_signal_command(
    oci_runtime: &ContainerRuntime,
    allocation: &Allocation,
    signal: &str,
) -> R<Vec<String>> {
    let signal = crate::signals::canonical_signal_name(signal)?;
    let name = container_name(allocation);
    Ok(match oci_runtime {
        ContainerRuntime::AppleContainer => vec![
            APPLE_CONTAINER_BIN.to_string(),
            "kill".to_string(),
            "--signal".to_string(),
            signal,
            name,
        ],
        ContainerRuntime::Oci { binary } => {
            vec![binary.clone(), "kill".to_string(), name, signal]
        }
    })
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

/// Builds the argv that execs `command` inside the allocation's container.
///
/// The two dialects differ deliberately:
///
/// ```text
/// Apple:                container exec [--tty --interactive] legion-<alloc> CMD…
/// OCI (youki/runc/crun): <runtime> exec [--tty] [--env K=V]… legion-<alloc> -- CMD…
/// ```
///
/// - **`--interactive` is Apple-only.** The OCI runtimes have no such flag;
///   stdin wiring is implicit when `exec` runs in the foreground, which is
///   how Legion runs it (under the PTY `spawn_pty` allocates, or with piped
///   stdio from `spawn_pipe`).
/// - **`--` before the command on the OCI path**, so a remote argv beginning
///   with `-` cannot be eaten by the runtime's flag parser. Asymmetric on
///   purpose: Apple's parser reads `--` as the target executable — the same
///   quirk [`build_apple_launch`] documents.
/// - **Caller env** (`ExecOptions.env`, e.g. the operator's `TERM`) becomes
///   repeated `--env K=V` flags on the OCI path; setting it on the runtime
///   CLI's own process would not reach the container. The Apple path ignores
///   it, as it always has.
///
/// Everything else is inherited from the bundle: `<runtime> exec` builds the
/// process spec from the bundle's `config.json` — the task's env, cwd, and
/// user — replacing only argv, and exits with the process's own status.
fn build_exec_command(
    oci_runtime: &ContainerRuntime,
    allocation_id: i64,
    tty: bool,
    command: &[String],
    env: &std::collections::BTreeMap<String, String>,
) -> Vec<String> {
    let name = format!("legion-{allocation_id}");
    match oci_runtime {
        ContainerRuntime::AppleContainer => {
            let mut argv = vec![APPLE_CONTAINER_BIN.to_string(), "exec".to_string()];
            if tty {
                argv.push("--tty".to_string());
                argv.push("--interactive".to_string());
            }
            argv.push(name);
            argv.extend(command.iter().cloned());
            argv
        }
        ContainerRuntime::Oci { binary } => {
            let mut argv = vec![binary.clone(), "exec".to_string()];
            if tty {
                argv.push("--tty".to_string());
            }
            // BTreeMap iteration is sorted, so the argv is stable across runs.
            for (key, value) in env {
                argv.push("--env".to_string());
                argv.push(format!("{key}={value}"));
            }
            argv.push(name);
            argv.push("--".to_string());
            argv.extend(command.iter().cloned());
            argv
        }
    }
}

/// Key under which the image config's `StopSignal` is recorded on the
/// instance at start, so teardown can consult it without re-reading the OCI
/// layout (which `destroy` may already have removed).
const IMAGE_STOP_SIGNAL_KEY: &str = "imageStopSignal";

/// The signal to deliver when stopping this container, docker's default
/// chain: the jobspec's `stop.signal`, else the image config's `StopSignal`,
/// else `SIGTERM`.
///
/// This is why "undeclared" has to survive the trip to the worker as a state
/// distinct from "declared SIGTERM": only the silent case consults the image.
/// An image declaring a signal outside Legion's vocabulary is ignored rather
/// than failing the stop — an unstoppable container is a worse outcome than a
/// SIGTERM the image did not ask for.
fn container_stop_signal(task: &TaskConfig, instance: &TaskInstance) -> String {
    if let Some(stop) = task.stop.as_ref() {
        return stop.signal.clone();
    }
    instance
        .state
        .get(IMAGE_STOP_SIGNAL_KEY)
        .and_then(Value::as_str)
        .and_then(|declared| match crate::signals::canonical_signal_name(declared) {
            Ok(signal) => Some(signal),
            Err(err) => {
                debug!(declared, error = %err, "Ignoring image StopSignal outside Legion's vocabulary");
                None
            }
        })
        .unwrap_or_else(|| crate::signals::DEFAULT_STOP_SIGNAL.to_string())
}

#[derive(Default)]
pub struct ContainerTaskPlugin;

impl ContainerTaskPlugin {}

impl TaskPlugin for ContainerTaskPlugin {
    /// A gated allocation's namespace is a real boundary: nothing leaves it
    /// except through the datapath, which holds its policy until ratified.
    fn network_gate_for(&self, allocation: &Allocation) -> crate::task_plugins::NetworkGate {
        if crate::task_plugins::allocation_is_gated(allocation) {
            crate::task_plugins::NetworkGate::Enforced
        } else {
            crate::task_plugins::NetworkGate::Advisory
        }
    }

    fn set_network_gate(
        &self,
        allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
        open: bool,
    ) -> R<()> {
        crate::task_plugins::set_gated_namespace_gate(allocation, open)
    }

    fn name(&self) -> &'static str {
        "container"
    }

    /// The tracked process is the container runtime child; when it exits the
    /// container is gone, so its exit is the task's exit.
    fn supervises_process(&self) -> bool {
        true
    }

    fn schema(&self) -> Value {
        derived_schema::<ContainerTaskConfig>()
    }

    /// The JSON schema, then the typed parse the worker runs at start, so a
    /// misspelled capability, a bad `ulimits` entry, or an empty `command`
    /// fails at submit instead of on some node at start.
    fn validate_config(&self, config: &Value) -> R<()> {
        crate::task_plugins::validate_schema(&self.schema(), config)?;
        ContainerTaskConfig::from_value(config)?;
        Ok(())
    }

    /// Apple `container` refuses a VM under
    /// [`APPLE_CONTAINER_MIN_MEMORY_MIB`]. When the task's own constraints
    /// pin it to macOS, that is known at submit and the job is rejected.
    /// Otherwise the target OS is only known at placement, and a macOS worker
    /// fails the start with the same message (see [`Self::start`]) instead of
    /// letting the runtime's error land in the task's stderr.
    fn check_resources(&self, task: &TaskConfig) -> R<Vec<String>> {
        if constrained_to_macos(task) {
            if let Some(mib) = apple_guest_memory_mib(&task.resources) {
                if mib < APPLE_CONTAINER_MIN_MEMORY_MIB as f64 {
                    return Err(apple_memory_floor_error(
                        &task.name,
                        "resources.memoryMax, else resources.memory",
                        mib,
                    ));
                }
            }
        }
        Ok(Vec::new())
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

    /// The allocation's task directory is bind-mounted at [`TASK_DIR_MOUNT`],
    /// so a rendered file written on the host is readable inside — and a
    /// re-render (atomic rename within the mounted directory) is visible
    /// without restarting the container.
    fn supports_kv_file_injection(&self) -> bool {
        true
    }

    fn supports_identity_env(&self) -> bool {
        true
    }

    /// Same mechanism as KV files: the token is written into the mounted task
    /// directory. Paired with `changeMode: signal` (which the container
    /// runtime now also supports), a rotation reaches the workload with no
    /// restart at all.
    fn supports_identity_file(&self) -> bool {
        true
    }

    /// Container tasks see the task directory at [`TASK_DIR_MOUNT`] and
    /// nothing else of the host, so every declared file destination must
    /// resolve under it.
    fn file_delivery_root(&self) -> Option<&'static str> {
        Some(TASK_DIR_MOUNT)
    }

    fn supports_artifacts(&self) -> bool {
        true
    }

    /// Signals reach the container's init through the runtime CLI (see
    /// [`TaskPlugin::signal`]), so `changeMode: signal` identities and
    /// declared `stopSignal`s are honored on both platforms.
    fn supports_signal(&self) -> bool {
        true
    }

    /// The OCI runtime creates and owns the container's cgroup, so Legion
    /// hands it the worker's decision through the generated spec
    /// (`cgroupsPath` + `linux.resources`) instead of writing cgroup files
    /// behind its back.
    fn resource_enforcement(&self) -> crate::task_plugins::ResourceEnforcement {
        crate::task_plugins::ResourceEnforcement::RuntimeApplied
    }

    /// Signal the container's *init*, not the tracked pid — the tracked
    /// process is the runtime CLI supervisor, and killing it would leave the
    /// workload running unsignaled (the bug this override exists to avoid).
    fn signal(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
        signal: &str,
    ) -> R<()> {
        let oci_runtime = ContainerRuntime::detect()?;
        let argv = build_signal_command(&oci_runtime, allocation, signal)?;
        let (program, args) = argv.split_first().expect("signal argv is never empty");
        let output = Command::new(program)
            .args(args)
            .output()
            .with_context(|| format!("failed to run `{program}` to signal the container"))?;
        if !output.status.success() {
            bail!(
                "{program} could not deliver {signal} to container {}: {}",
                container_name(allocation),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        debug!(
            allocation_id = allocation.id,
            task_name = %task.name,
            signal,
            "Delivered signal to container init"
        );
        Ok(())
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
        opts.command = build_exec_command(
            &oci_runtime,
            allocation.id,
            opts.tty,
            &opts.command,
            &opts.env,
        );

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
                oci_image::apple_load_image(oci_layout, reference)?;
            }
            oci_image::record_staged_layout(
                instance,
                oci_layout,
                reference,
                staged.digest.as_deref(),
            );
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
        check_apple_memory_floor(&oci_runtime, task)?;
        oci_runtime.preflight()?;
        if oci_runtime == ContainerRuntime::AppleContainer && config.adjusts_linux_process() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "security.capabilities and ulimits apply to Linux containers only; Apple \
                 `container` runs this one in its own VM with the runtime's defaults"
            );
        }

        // Legion-provided env (allocation identity + assigned ports) first;
        // user-declared env overrides on key conflicts.
        let mut merged_env: HashMap<String, String> =
            crate::task_plugins::allocation_env(allocation, task)
                .into_iter()
                .collect();
        // Worker-injected env, translated to the in-container view: volume
        // destinations, `LEGION_TASK_DIR=/legion/task`, and artifact paths
        // rebased onto the mount. Applied before the task's own env so a
        // jobspec can still override.
        let translated = translated_worker_env(instance);
        for (key, value) in translated {
            merged_env.insert(key, value);
        }
        merged_env.extend(config.env.clone());

        // Files the manager rendered for this task (KV file injection) land
        // in the host task dir, which the mount below makes readable inside.
        materialize_task_files(&config.files, instance).with_context(|| {
            format!(
                "failed to materialize declared files for container task {}",
                task.name
            )
        })?;

        // A container still holding this allocation's name would make `run`
        // fail; restarts and worker crashes both leave one behind.
        remove_stale_container(&oci_runtime, &container_name(allocation));

        // The uid/gid the Linux container runs as, to hand it its logs.
        let mut log_owner: Option<(u32, u32)> = None;
        let mut cmd = match &oci_runtime {
            ContainerRuntime::AppleContainer => {
                // `consume_artifact` already imported the staged layout, so
                // launching its pinned reference runs exactly the image the
                // artifact pipeline resolved — and needs no registry pull.
                // Without a staged image, the jobspec's own reference is used
                // and Apple `container` pulls it itself.
                let image =
                    oci_image::staged_reference(instance).unwrap_or_else(|| config.image.clone());
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
                    instance.task_dir.as_deref(),
                    &image,
                ));
                cmd
            }
            ContainerRuntime::Oci { binary } => {
                // The runtime runs a bundle, not an image: get the image's OCI
                // layout (from the artifact pipeline if staged, else pull it),
                // convert it to a bundle, then `run` it.
                let scratch = work_root(allocation, instance);
                let layout = oci_image::resolve_layout(&config.image, instance, &scratch)?;
                let bundle = scratch.join("bundle");
                // Name-is-config: point the container at the allocation's
                // own resolver when it was assigned one.
                let resolver = allocation
                    .network
                    .as_ref()
                    .and_then(|network| network.resolver_address());
                // A gated allocation joins its own namespace
                // (docs/network-handlers.md), created here if the heartbeat
                // has not made it yet; failing to create it is a hard stop
                // rather than a silent fall back to host networking.
                let gated_netns = crate::task_plugins::ensure_gated_namespace(allocation)?;
                let policy = config.process_policy()?;
                let built = oci_bundle::build_bundle(
                    &layout,
                    &bundle,
                    &config.command,
                    &merged_env,
                    &oci_bundle::BundleContext {
                        hostname: &container_name(allocation),
                        volumes: &instance.volumes,
                        task_dir: instance.task_dir.as_deref(),
                        assignment: instance.resource_assignment.as_ref(),
                        resolver: resolver.as_deref(),
                        resolv_conf: None,
                        netns: gated_netns.as_deref(),
                        policy: &policy,
                    },
                )?;
                log_owner = Some((built.uid, built.gid));
                // Recorded now, while the layout is still on disk: teardown
                // needs it (`destroy` removes the scratch tree) and it is the
                // middle layer of the stop-signal default chain.
                if let Some(signal) = built.stop_signal {
                    let signal = Value::String(signal);
                    match instance.state.as_object_mut() {
                        Some(state) => {
                            state.insert(IMAGE_STOP_SIGNAL_KEY.into(), signal);
                        }
                        None => instance.state = json!({ IMAGE_STOP_SIGNAL_KEY: signal }),
                    }
                }

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
        // A non-root image user must be able to reopen /dev/stdout and
        // /dev/stderr, which are these files (see `hand_task_logs_to`).
        if let Some((uid, gid)) = log_owner {
            oci_bundle::hand_task_logs_to(instance.log_dir.as_deref(), uid, gid);
        }
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

    /// Deliver the stop notice **into the container**, not to the launching
    /// CLI: both runtimes keep the container alive when the foreground
    /// process is signalled, so signalling the tracked host process would
    /// notify nobody. The container's init (PID 1 in its namespace) is the
    /// process the workload actually runs as.
    fn stop_with(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        ctx: &crate::task_plugins::StopContext,
    ) -> R<bool> {
        let Ok(oci_runtime) = ContainerRuntime::detect() else {
            return Ok(false);
        };
        let name = container_name(allocation);
        // `ctx.signal` is the worker's resolved ladder, which cannot know
        // about the image: when the jobspec declared no `stop` block, docker's
        // chain says the image's own STOPSIGNAL wins before SIGTERM.
        let signal = container_stop_signal(task, instance);
        let grace_secs = ctx.grace.as_secs().max(1).to_string();
        let (program, args): (&str, Vec<String>) = match &oci_runtime {
            // `runc`/`youki`-style `kill` signals the container init and
            // returns immediately, which is exactly the contract here — the
            // worker's reaper owns the deadline.
            ContainerRuntime::Oci { binary } => (
                binary.as_str(),
                vec![
                    "kill".to_string(),
                    name.clone(),
                    // The OCI CLI takes the bare name (`TERM`), not `SIGTERM`.
                    signal.trim_start_matches("SIG").to_string(),
                ],
            ),
            // Apple's `container stop` signals and then waits out its own
            // timeout, so it is run detached below rather than inline. It
            // owns the ladder end-to-end; Legion's `stop.signal` reaches it,
            // but the runtime may SIGKILL before Legion's deadline if
            // `killAfter` is longer than what `--time` accepts.
            ContainerRuntime::AppleContainer => (
                APPLE_CONTAINER_BIN,
                vec![
                    "stop".to_string(),
                    "--signal".to_string(),
                    signal.clone(),
                    "--time".to_string(),
                    grace_secs,
                    name.clone(),
                ],
            ),
        };
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            oci_runtime = program,
            %signal,
            grace_secs = ctx.grace.as_secs(),
            cause = %ctx.cause,
            "Signalling container to stop"
        );
        // Spawned, never waited on: `stop_with` runs under the worker's
        // task-manager mutex and must not hold it for the grace window.
        // Failures are ignored on purpose — an already-exited container is a
        // successful stop, and the reaper cleans up either way.
        match Command::new(program)
            .args(&args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(_child) => Ok(true),
            Err(err) => {
                warn!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    error = %err,
                    "Failed to signal container; falling back to a hard stop"
                );
                Ok(false)
            }
        }
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

        reap_task_process(allocation.id, &task.name, instance, "container");
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
        let plugin = ContainerTaskPlugin;
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
            None,
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
            None,
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
        let args = build_apple_launch(
            &alloc(),
            &task,
            &config,
            &config.env,
            &[],
            None,
            &config.image,
        );
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
            None,
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
            upstreams: Vec::new(),
            address: Some("127.0.0.1".into()),
            ports: vec![
                crate::structs::network::AllocPort {
                    name: "http".into(),
                    host_port: 21000,
                    to: Some(8080),
                    private_port: None,
                },
                crate::structs::network::AllocPort {
                    name: "metrics".into(),
                    host_port: 21001,
                    to: None,
                    private_port: None,
                },
            ],
            vip_block: None,
            mode: None,
            ingress_tls: None,
        });
        let args = build_apple_launch(
            &allocation,
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            None,
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
        let image = oci_image::staged_reference(&instance).expect("staged reference");
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
            None,
            &image,
        );
        assert!(args.contains(&image), "{args:?}");
        assert!(!args.contains(&"alpine:latest".to_string()), "{args:?}");

        // No staged image: the jobspec's own reference is what runs.
        assert_eq!(oci_image::staged_reference(&TaskInstance::default()), None);
        Ok(())
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
        let plugin = ContainerTaskPlugin;
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
        let plugin = ContainerTaskPlugin;
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
        let plugin = ContainerTaskPlugin;
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
        let plugin = ContainerTaskPlugin;
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

    /// The OCI dialect: `--tty` but never `--interactive` (runc/crun/youki
    /// have no such flag), caller env as repeated `--env`, and a `--`
    /// separator so a command starting with `-` reaches the container instead
    /// of the runtime's flag parser.
    #[test]
    fn oci_exec_argv_uses_the_runtime_dialect() {
        let oci_runtime = ContainerRuntime::Oci {
            binary: "runc".into(),
        };
        let env = std::collections::BTreeMap::from([("TERM".to_string(), "xterm".to_string())]);
        let argv = build_exec_command(
            &oci_runtime,
            42,
            true,
            &["-l".to_string(), "--color".to_string()],
            &env,
        );
        assert_eq!(
            argv,
            vec![
                "runc",
                "exec",
                "--tty",
                "--env",
                "TERM=xterm",
                "legion-42",
                "--",
                "-l",
                "--color",
            ]
        );
        assert!(
            !argv.contains(&"--interactive".to_string()),
            "OCI runtimes have no --interactive: {argv:?}"
        );

        // Non-tty one-shot: no --tty, everything else unchanged.
        let argv = build_exec_command(
            &oci_runtime,
            42,
            false,
            &["/bin/true".to_string()],
            &Default::default(),
        );
        assert_eq!(argv, vec!["runc", "exec", "legion-42", "--", "/bin/true"]);
    }

    /// The Apple arm keeps its own dialect: `--interactive` alongside
    /// `--tty`, no `--` separator (the parser would read it as the target
    /// executable), and caller env is not forwarded.
    #[test]
    fn apple_exec_argv_is_unchanged() {
        let env = std::collections::BTreeMap::from([("TERM".to_string(), "xterm".to_string())]);
        let argv = build_exec_command(
            &ContainerRuntime::AppleContainer,
            7,
            true,
            &["/bin/sh".to_string()],
            &env,
        );
        assert_eq!(
            argv,
            vec![
                APPLE_CONTAINER_BIN,
                "exec",
                "--tty",
                "--interactive",
                "legion-7",
                "/bin/sh",
            ]
        );
        assert!(!argv.contains(&"--".to_string()), "{argv:?}");
        assert!(!argv.contains(&"--env".to_string()), "{argv:?}");
    }

    /// Exec into a container is sandboxed on every platform, and both
    /// runtimes support it — the Linux gap this phase closed.
    #[test]
    fn exec_support_is_reported_for_every_detected_runtime() {
        assert!(ContainerRuntime::AppleContainer.supports_exec());
        assert!(ContainerRuntime::Oci {
            binary: "youki".into()
        }
        .supports_exec());

        let support = ContainerTaskPlugin.exec_support();
        // Detection succeeds on both supported operating systems; the report
        // is only false on an OS with no container runtime at all.
        let detected = ContainerRuntime::detect().is_ok();
        assert_eq!(support.exec, detected);
        assert_eq!(support.pty, detected);
        assert!(!support.host_level, "container exec is never host-level");
    }

    /// The mount goes in before declared volumes, so a job can still shadow
    /// it deliberately — the same precedence the mount list gives its other
    /// defaults.
    #[test]
    fn apple_launch_mounts_the_task_dir_before_declared_volumes() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let volumes = [crate::task_plugins::MountedVolume {
            name: "data".into(),
            mode: crate::volumes::VolumeMode::Filesystem,
            host_path: PathBuf::from("/var/legion/data"),
            path: Some("/data".into()),
            fs: None,
            fresh: false,
            read_only: false,
        }];
        let args = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &config.env,
            &volumes,
            Some(std::path::Path::new("/var/legion/allocs/7/web")),
            &config.image,
        );

        let task_mount = args
            .iter()
            .position(|arg| arg == "/var/legion/allocs/7/web:/legion/task")
            .expect("task dir mounted");
        let volume_mount = args
            .iter()
            .position(|arg| arg == "/var/legion/data:/data")
            .expect("declared volume mounted");
        assert!(
            task_mount < volume_mount,
            "task dir mounts first so a volume can shadow it: {args:?}"
        );

        // No task dir threaded through (a bare unit-test instance): no mount.
        let bare = build_apple_launch(
            &alloc(),
            &task_with(json!({}), None),
            &config,
            &config.env,
            &[],
            None,
            &config.image,
        );
        assert!(
            !bare.iter().any(|arg| arg.contains("/legion/task")),
            "{bare:?}"
        );
        Ok(())
    }

    /// The env a container actually gets: in-container task dir, artifact
    /// paths rebased onto the mount, volume vars passed through — and
    /// `LEGION_ALLOC_DIR` withheld, because the alloc dir is not mounted.
    #[test]
    fn worker_env_is_translated_to_the_in_container_view() {
        let task_dir = PathBuf::from("/var/legion/allocs/7/web");
        let instance = TaskInstance {
            task_dir: Some(task_dir.clone()),
            extra_env: std::collections::BTreeMap::from([
                (
                    "LEGION_ALLOC_DIR".to_string(),
                    "/var/legion/allocs/7".to_string(),
                ),
                (
                    "LEGION_TASK_DIR".to_string(),
                    task_dir.display().to_string(),
                ),
                (
                    "LEGION_ARTIFACT_MODEL".to_string(),
                    format!("{}/artifacts/model", task_dir.display()),
                ),
                // Staged somewhere the mount does not reach: omitted rather
                // than leaked as a dead host path.
                (
                    "LEGION_ARTIFACT_ELSEWHERE".to_string(),
                    "/var/cache/legion/blob".to_string(),
                ),
                ("LEGION_VOLUME_DATA".to_string(), "/data".to_string()),
                ("LEGION_STORAGE_DIR".to_string(), "/workspace".to_string()),
            ]),
            ..TaskInstance::default()
        };

        let env: HashMap<String, String> = translated_worker_env(&instance).into_iter().collect();
        assert_eq!(
            env.get("LEGION_TASK_DIR").map(String::as_str),
            Some("/legion/task")
        );
        assert_eq!(
            env.get("LEGION_ARTIFACT_MODEL").map(String::as_str),
            Some("/legion/task/artifacts/model")
        );
        assert!(
            !env.contains_key("LEGION_ARTIFACT_ELSEWHERE"),
            "an artifact outside the mount is omitted, not leaked: {env:?}"
        );
        assert!(
            !env.contains_key("LEGION_ALLOC_DIR"),
            "the alloc dir is not mounted, so its path means nothing inside"
        );
        assert_eq!(
            env.get("LEGION_VOLUME_DATA").map(String::as_str),
            Some("/data")
        );
        assert_eq!(
            env.get("LEGION_STORAGE_DIR").map(String::as_str),
            Some("/workspace")
        );
    }

    /// Declared files land in the host task dir at the path they will be
    /// readable from inside, and the path rule is re-checked here rather than
    /// trusted from admission.
    #[test]
    fn declared_files_materialize_under_the_task_dir() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let instance = TaskInstance {
            task_dir: Some(tmp.path().to_path_buf()),
            ..TaskInstance::default()
        };

        materialize_task_files(
            &[
                ContainerInjectedFile {
                    path: "config/app.toml".into(),
                    mode: Some("0640".into()),
                    content: "tenant=acme\n".into(),
                },
                ContainerInjectedFile {
                    path: "/legion/task/other.txt".into(),
                    mode: None,
                    content: "ok".into(),
                },
            ],
            &instance,
        )?;

        assert_eq!(
            std::fs::read_to_string(tmp.path().join("config/app.toml"))?,
            "tenant=acme\n"
        );
        assert_eq!(std::fs::read_to_string(tmp.path().join("other.txt"))?, "ok");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(tmp.path().join("config/app.toml"))?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o640, "declared mode applied, got {mode:o}");
        }

        // Defense in depth: a path the worker should never have been handed
        // still cannot write outside the tree.
        let err = materialize_task_files(
            &[ContainerInjectedFile {
                path: "../escape".into(),
                mode: None,
                content: "no".into(),
            }],
            &instance,
        )
        .expect_err("traversal is refused on the worker too");
        assert!(err.to_string().contains("escapes"), "{err}");
        Ok(())
    }

    #[test]
    fn signal_argv_is_per_runtime() -> R<()> {
        let allocation = alloc_with_id(11);
        assert_eq!(
            build_signal_command(&ContainerRuntime::AppleContainer, &allocation, "hup")?,
            vec![
                APPLE_CONTAINER_BIN,
                "kill",
                "--signal",
                "SIGHUP",
                "legion-11"
            ]
        );
        assert_eq!(
            build_signal_command(
                &ContainerRuntime::Oci {
                    binary: "crun".into()
                },
                &allocation,
                "SIGHUP",
            )?,
            vec!["crun", "kill", "legion-11", "SIGHUP"]
        );

        // A signal Legion cannot deliver is rejected before it reaches a CLI.
        assert!(
            build_signal_command(&ContainerRuntime::AppleContainer, &allocation, "SIGKILL")
                .is_err()
        );
        Ok(())
    }

    /// Docker's default chain: the jobspec wins, then the image's own
    /// `StopSignal`, then `SIGTERM`.
    #[test]
    fn stop_signal_follows_the_jobspec_then_image_then_sigterm_chain() {
        let bare = task_with(json!({}), None);
        let image_signal = TaskInstance {
            state: json!({ IMAGE_STOP_SIGNAL_KEY: "SIGQUIT" }),
            ..TaskInstance::default()
        };

        assert_eq!(
            container_stop_signal(&bare, &TaskInstance::default()),
            "SIGTERM"
        );
        assert_eq!(container_stop_signal(&bare, &image_signal), "SIGQUIT");

        let declared = TaskConfig {
            stop: Some(crate::structs::StopConfig {
                signal: "SIGINT".into(),
                ..Default::default()
            }),
            ..bare.clone()
        };
        assert_eq!(
            container_stop_signal(&declared, &image_signal),
            "SIGINT",
            "the jobspec outranks the image"
        );

        // An image asking for something Legion cannot deliver falls back
        // rather than making the container unstoppable.
        let exotic = TaskInstance {
            state: json!({ IMAGE_STOP_SIGNAL_KEY: "SIGRTMIN+3" }),
            ..TaskInstance::default()
        };
        assert_eq!(container_stop_signal(&bare, &exotic), "SIGTERM");
    }

    /// End-to-end Linux graceful stop: a container that traps its declared
    /// signal exits inside the window, and `delete --force` finds it gone.
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_stop_lets_a_trapping_container_drain() -> R<()> {
        let binary = std::env::var(OCI_RUNTIME_ENV).unwrap_or_else(|_| DEFAULT_OCI_RUNTIME.into());
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9007);
        let name = container_name(&allocation);
        let mut task = container_task(
            "draintest",
            json!([
                "/bin/sh",
                "-c",
                "trap 'echo draining; exit 0' INT; while :; do sleep 0.2; done"
            ]),
        );
        task.stop = Some(crate::structs::StopConfig {
            signal: "SIGINT".into(),
            kill_after: std::time::Duration::from_secs(30),
            drain: None,
        });

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let started = std::time::Instant::now();
        plugin.stop(&allocation, &task, &mut instance)?;
        let elapsed = started.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(25),
            "a trapping container must exit well inside its 30s window, took {elapsed:?}"
        );
        let stdout =
            std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stdout.log"))?;
        assert!(stdout.contains("draining"), "trap ran: {stdout:?}");
        assert!(
            !Command::new(&binary)
                .args(["state", &name])
                .output()?
                .status
                .success(),
            "container is gone after stop"
        );
        Ok(())
    }

    /// The other half: a container that ignores the signal is force-killed at
    /// the deadline rather than hanging the stop forever.
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_stop_force_kills_at_the_deadline() -> R<()> {
        let binary = std::env::var(OCI_RUNTIME_ENV).unwrap_or_else(|_| DEFAULT_OCI_RUNTIME.into());
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9008);
        let name = container_name(&allocation);
        let mut task = container_task(
            "ignoretest",
            json!(["/bin/sh", "-c", "trap '' TERM; while :; do sleep 0.2; done"]),
        );
        task.stop = Some(crate::structs::StopConfig {
            signal: "SIGTERM".into(),
            kill_after: std::time::Duration::from_secs(3),
            drain: None,
        });

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let started = std::time::Instant::now();
        plugin.stop(&allocation, &task, &mut instance)?;
        let elapsed = started.elapsed();

        assert!(
            elapsed >= std::time::Duration::from_secs(3),
            "the full window must be honored before the kill, took {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(20),
            "and the kill must actually happen at the deadline, took {elapsed:?}"
        );
        assert!(
            !Command::new(&binary)
                .args(["state", &name])
                .output()?
                .status
                .success(),
            "container is gone after stop"
        );
        Ok(())
    }

    /// End-to-end Linux exec: start a container, exec a one-shot command into
    /// it, and assert the task's own env is visible and the exit code
    /// propagates. Run with:
    /// `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_exec -- --ignored`
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_exec_runs_inside_the_container() -> R<()> {
        use std::io::Read;

        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9005);
        let mut task = container_task("exectest", json!(["/bin/sleep", "60"]));
        task.config["env"] = json!({ "TASK_MARKER": "from-task" });

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let opts = ExecOptions::new(
            vec![
                "/bin/sh".into(),
                "-c".into(),
                "echo marker=$TASK_MARKER; exit 3".into(),
            ],
            false,
        );
        let mut session = plugin.exec(&allocation, &task, &mut instance, opts)?;
        let mut streams = session.take_output()?;
        let mut stdout = String::new();
        streams.stdout.read_to_string(&mut stdout)?;
        let code = session.wait()?;

        plugin.stop(&allocation, &task, &mut instance)?;

        assert!(
            stdout.contains("marker=from-task"),
            "exec'd process inherits the task env: {stdout:?}"
        );
        assert_eq!(code, 3, "exec propagates the process's own exit status");
        Ok(())
    }

    /// End-to-end Linux interactive exec: a shell round-trip through a PTY.
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_exec_supports_a_tty_session() -> R<()> {
        use std::io::Read;

        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9006);
        let task = container_task("ttytest", json!(["/bin/sleep", "60"]));

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut session = plugin.exec(
            &allocation,
            &task,
            &mut instance,
            ExecOptions::new(vec!["/bin/sh".into()], true),
        )?;
        let mut streams = session.take_output()?;
        session.write_stdin(b"echo pty-round-trip\n")?;
        session.resize(120, 40)?;
        session.write_stdin(b"exit 0\n")?;
        let mut output = String::new();
        streams.stdout.read_to_string(&mut output)?;
        session.wait()?;

        plugin.stop(&allocation, &task, &mut instance)?;
        assert!(output.contains("pty-round-trip"), "pty output: {output:?}");
        Ok(())
    }

    /// End-to-end Linux limits: a memory hog dies at its declared limit, and
    /// the cgroup records the OOM so the failure record can name it.
    ///
    /// Run on a node with cgroup v2 and a root worker:
    /// `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_enforces -- --ignored`
    #[test]
    #[ignore = "requires an OCI runtime + cgroup v2 + root"]
    fn oci_runtime_enforces_a_declared_memory_limit() -> R<()> {
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9009);
        let mut task = container_task(
            "oomtest",
            // Allocate well past the limit; the kernel kills the cgroup.
            json!([
                "/bin/sh",
                "-c",
                "dd if=/dev/zero of=/dev/shm/fill bs=1M count=512"
            ]),
        );
        task.resources = json!({ "memory": 32 });

        let assignment = crate::task_plugins::ResourceAssignment {
            slice: crate::task_plugins::Slice::Share,
            cgroup_path: std::path::PathBuf::from("legion/share/legion-9009"),
            cores: None,
            cpu_weight: None,
            memory_limit_bytes: Some(32 * 1024 * 1024),
            memory_low_bytes: None,
        };
        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            resource_assignment: Some(assignment.clone()),
            ..TaskInstance::default()
        };

        plugin.start(&allocation, &task, &mut instance)?;
        let status = instance.child.take().expect("child handle").wait()?;
        assert!(!status.success(), "the hog must not succeed: {status:?}");
        assert!(
            crate::worker::resources::oom_kill_count(&assignment) > 0,
            "the cgroup records the OOM kill that produced the failure"
        );

        plugin.stop(&allocation, &task, &mut instance)?;
        Ok(())
    }

    /// End-to-end Linux pinning: a `cores: 2` container sees exactly its two
    /// assigned CPUs inside its own namespace.
    #[test]
    #[ignore = "requires an OCI runtime + cgroup v2 + root"]
    fn oci_runtime_pins_a_container_to_its_assigned_cores() -> R<()> {
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9010);
        let mut task = container_task(
            "pintest",
            json!(["/bin/sh", "-c", "grep Cpus_allowed_list /proc/self/status"]),
        );
        task.resources = json!({ "cores": 2 });

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            log_dir: Some(tmp.path().join("logs")),
            resource_assignment: Some(crate::task_plugins::ResourceAssignment {
                slice: crate::task_plugins::Slice::Reserve,
                cgroup_path: std::path::PathBuf::from("legion/reserve/legion-9010"),
                cores: Some(vec![0, 1]),
                cpu_weight: None,
                memory_limit_bytes: None,
                memory_low_bytes: None,
            }),
            ..TaskInstance::default()
        };

        plugin.start(&allocation, &task, &mut instance)?;
        let status = instance.child.take().expect("child handle").wait()?;
        assert!(status.success(), "container exited cleanly: {status:?}");
        let stdout =
            std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stdout.log"))?;
        assert!(
            stdout.contains("0-1") || stdout.contains("0,1"),
            "the container sees exactly its assigned cores: {stdout:?}"
        );

        plugin.stop(&allocation, &task, &mut instance)?;
        Ok(())
    }

    /// An instance for a stock-image end-to-end test. With
    /// `LEGION_OCI_TEST_LAYOUT_DIR` set, a pre-fetched OCI layout at
    /// `<dir>/<name>` stands in for the registry (useful where anonymous
    /// pulls are rate-limited); otherwise `start` pulls `image` as usual.
    fn stock_image_instance(name: &str, image: &str, root: &std::path::Path) -> TaskInstance {
        let mut instance = TaskInstance {
            alloc_dir: Some(root.to_path_buf()),
            log_dir: Some(root.join("logs")),
            ..TaskInstance::default()
        };
        if let Ok(dir) = std::env::var("LEGION_OCI_TEST_LAYOUT_DIR") {
            let layout = std::path::Path::new(&dir).join(name);
            if layout.join("index.json").is_file() {
                oci_image::record_staged_layout(&mut instance, &layout, image, None);
            }
        }
        instance
    }

    fn wait_for_log(instance: &TaskInstance, file: &str, needle: &str, secs: u64) -> String {
        let path = instance.log_dir.as_ref().expect("log dir").join(file);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
        loop {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if text.contains(needle) || std::time::Instant::now() > deadline {
                return text;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }

    /// P0-7 end to end, with the official nginx image as it ships: the
    /// default capabilities let its root master `chown` its cache and run
    /// workers as `nginx`; extraction kept the image's `chown nginx`
    /// (uid 101); its `access.log -> /dev/stdout` symlink reaches the task's
    /// log; and the open-file limit is no longer 1024.
    ///
    /// `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_runs_stock_nginx -- --ignored`
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun), root, and the nginx image"]
    fn oci_runtime_runs_stock_nginx() -> R<()> {
        use std::io::{Read as _, Write as _};
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9012);
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let task: TaskConfig = serde_json::from_value(json!({
            "name": "nginx",
            "runtime": "container",
            "config": {
                "image": "nginx:1.27-alpine",
                "command": ["sh", "-c", format!(
                    "stat -c 'cache-owner=%u' /var/cache/nginx; \
                     echo \"nofile=$(ulimit -n)\"; \
                     sed -i 's/listen  *80;/listen {port};/' /etc/nginx/conf.d/default.conf && \
                     exec nginx -g 'daemon off;'"
                )],
            },
        }))?;
        let tmp = tempfile::tempdir()?;
        let mut instance = stock_image_instance("nginx", "nginx:1.27-alpine", tmp.path());
        plugin.start(&allocation, &task, &mut instance)?;

        let stdout = wait_for_log(&instance, "stdout.log", "nofile=", 30);
        let mut response = String::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
                stream.write_all(b"GET / HTTP/1.0\r\n\r\n")?;
                stream.read_to_string(&mut response)?;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        let access = wait_for_log(&instance, "stdout.log", "\"GET / HTTP/1.0\"", 10);
        plugin.stop(&allocation, &task, &mut instance)?;
        let stderr = std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stderr.log"))
            .unwrap_or_default();

        assert!(
            stdout.contains("cache-owner=101"),
            "layer ownership kept: {stdout}"
        );
        assert!(
            stdout.contains("nofile=1048576") || !oci_bundle_root(),
            "{stdout}"
        );
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "nginx serves: {response:?} {stderr}"
        );
        assert!(
            !stderr.contains("Operation not permitted"),
            "no capability failures: {stderr}"
        );
        assert!(
            access.contains("\"GET / HTTP/1.0\""),
            "access log via /dev/stdout: {access}"
        );
        Ok(())
    }

    fn oci_bundle_root() -> bool {
        // SAFETY: geteuid cannot fail.
        unsafe { libc::geteuid() == 0 }
    }

    /// The official Postgres entrypoint as `configs/jobs/postgres.ncl` runs
    /// it: as root it creates and `chown`s PGDATA, `su-exec`s to `postgres`
    /// (a setuid/setgid drop that `noNewPrivileges` allows), runs initdb, and
    /// starts the server on the assigned port.
    ///
    /// `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_runs_the_postgres_entrypoint -- --ignored`
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun), root, and the postgres image"]
    fn oci_runtime_runs_the_postgres_entrypoint() -> R<()> {
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9013);
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let tmp = tempfile::tempdir()?;
        let data = tmp.path().join("volume");
        std::fs::create_dir_all(&data)?;
        let task: TaskConfig = serde_json::from_value(json!({
            "name": "postgres",
            "runtime": "container",
            "config": {
                "image": "postgres:16-alpine",
                "command": ["sh", "-c", "exec docker-entrypoint.sh postgres -p \"$PG_PORT\""],
                "env": {
                    "PG_PORT": port.to_string(),
                    "PGDATA": "/var/lib/postgresql/data/pgdata",
                    "POSTGRES_PASSWORD": "legion-demo",
                },
            },
        }))?;
        let mut instance = stock_image_instance("postgres", "postgres:16-alpine", tmp.path());
        instance.volumes = vec![crate::task_plugins::MountedVolume {
            name: "data".into(),
            mode: crate::volumes::VolumeMode::Filesystem,
            host_path: data.clone(),
            path: Some("/var/lib/postgresql/data".into()),
            fs: None,
            fresh: true,
            read_only: false,
        }];
        plugin.start(&allocation, &task, &mut instance)?;
        let ready = "database system is ready to accept connections";
        let stderr = wait_for_log(&instance, "stderr.log", ready, 120);
        let listening = std::net::TcpStream::connect(("127.0.0.1", port)).is_ok();
        plugin.stop(&allocation, &task, &mut instance)?;
        let stdout = std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stdout.log"))
            .unwrap_or_default();

        assert!(
            stderr.contains(ready),
            "postgres came up:\n{stdout}\n{stderr}"
        );
        assert!(listening, "listening on the assigned port");
        assert!(!stderr.contains("Operation not permitted"), "{stderr}");
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(data.join("pgdata"))?.uid(),
            70,
            "PGDATA chowned to the image's postgres user"
        );
        Ok(())
    }

    /// An image whose `USER` is not root (the Prometheus exporter runs as
    /// `nobody`) can reopen its own stdout and stderr by path.
    ///
    /// `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_lets_a_non_root_image -- --ignored`
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun), root, and the exporter image"]
    fn oci_runtime_lets_a_non_root_image_reopen_its_stdio() -> R<()> {
        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9014);
        let image = "quay.io/prometheuscommunity/postgres-exporter:v0.15.0";
        let task: TaskConfig = serde_json::from_value(json!({
            "name": "exporter",
            "runtime": "container",
            "config": {
                "image": image,
                "command": ["sh", "-c", "echo uid=$(id -u) > /dev/stdout; echo to-stderr > /dev/stderr"],
            },
        }))?;
        let tmp = tempfile::tempdir()?;
        let mut instance = stock_image_instance("exporter", image, tmp.path());
        plugin.start(&allocation, &task, &mut instance)?;
        let status = instance.child.take().expect("child").wait()?;
        let logs = instance.log_dir.clone().unwrap();
        let stdout = std::fs::read_to_string(logs.join("stdout.log"))?;
        let stderr = std::fs::read_to_string(logs.join("stderr.log"))?;
        plugin.stop(&allocation, &task, &mut instance)?;
        assert!(status.success(), "{status:?}: {stdout} {stderr}");
        assert!(stdout.contains("uid=65534"), "{stdout}");
        assert!(stderr.contains("to-stderr"), "{stderr}");
        Ok(())
    }

    /// End-to-end: a file rendered on the host is readable inside at its
    /// `/legion/task` path, and a host-side atomic rewrite is visible without
    /// restarting the container — the property that makes KV re-render and
    /// identity rotation work at all.
    #[test]
    #[ignore = "requires an OCI runtime (youki/runc/crun) + network"]
    fn oci_runtime_sees_task_dir_files_and_their_rewrites() -> R<()> {
        use std::io::Read;

        let plugin = ContainerTaskPlugin;
        let allocation = alloc_with_id(9011);
        let mut task = container_task("filetest", json!(["/bin/sleep", "60"]));
        task.config["files"] = json!([{
            "path": "config/app.toml",
            "content": "tenant=first\n",
        }]);

        let tmp = tempfile::tempdir()?;
        let task_dir = tmp.path().join("web");
        std::fs::create_dir_all(&task_dir)?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().to_path_buf()),
            task_dir: Some(task_dir.clone()),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };

        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_secs(3));

        let read_config =
            |plugin: &ContainerTaskPlugin, instance: &mut TaskInstance| -> R<String> {
                let mut session = plugin.exec(
                    &allocation,
                    &task,
                    instance,
                    ExecOptions::new(
                        vec!["/bin/cat".into(), "/legion/task/config/app.toml".into()],
                        false,
                    ),
                )?;
                let mut streams = session.take_output()?;
                let mut out = String::new();
                streams.stdout.read_to_string(&mut out)?;
                session.wait()?;
                Ok(out)
            };

        assert!(
            read_config(&plugin, &mut instance)?.contains("tenant=first"),
            "the rendered file is readable at its in-container path"
        );

        // Atomic replace, exactly as the worker's refresh machinery does it.
        let staged = task_dir.join("config/.app.toml.tmp");
        std::fs::write(&staged, "tenant=second\n")?;
        std::fs::rename(&staged, task_dir.join("config/app.toml"))?;
        assert!(
            read_config(&plugin, &mut instance)?.contains("tenant=second"),
            "a host-side atomic rewrite is visible inside without a restart"
        );

        plugin.stop(&allocation, &task, &mut instance)?;
        Ok(())
    }

    /// `security.capabilities` and `ulimits` are validated at submit (the
    /// typed parse runs in `validate_config`), not first noticed on a node.
    #[test]
    fn capability_and_ulimit_mistakes_fail_at_submit() -> R<()> {
        let plugin = ContainerTaskPlugin;
        plugin.validate_config(&json!({
            "image": "alpine",
            "command": ["ping", "-c1", "1.1.1.1"],
            "security": { "capabilities": { "add": ["NET_RAW"], "drop": ["mknod"] } },
            "ulimits": { "nofile": 65536, "nproc": { "soft": 512, "hard": 1024 } },
        }))?;

        for (config, needle) in [
            (
                json!({ "image": "a", "command": ["x"],
                        "security": { "capabilities": { "add": ["NET_RAWW"] } } }),
                "NET_RAWW",
            ),
            (
                json!({ "image": "a", "command": ["x"],
                        "security": { "capabilites": { "add": ["NET_RAW"] } } }),
                "capabilites",
            ),
            (
                json!({ "image": "a", "command": ["x"], "ulimits": { "nofile": { "soft": 9, "hard": 1 } } }),
                "must not exceed",
            ),
            (json!({ "image": "a", "command": [] }), "at least one entry"),
        ] {
            let err = plugin.validate_config(&config).expect_err("rejected");
            assert!(format!("{err:#}").contains(needle), "{config}: {err:#}");
        }
        Ok(())
    }

    /// The jobspec's adjustments become the bundle's process policy.
    #[test]
    fn process_policy_carries_the_jobspec_adjustments() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["true"],
            "security": { "capabilities": { "drop": ["ALL"], "add": ["NET_BIND_SERVICE"] } },
            "ulimits": { "nofile": 4096 },
        }))?;
        let policy = config.process_policy()?;
        assert_eq!(
            policy.capabilities,
            vec!["CAP_NET_BIND_SERVICE".to_string()]
        );
        let nofile = policy
            .rlimits
            .iter()
            .find(|limit| limit.kind == "RLIMIT_NOFILE")
            .expect("nofile");
        assert_eq!((nofile.soft, nofile.hard), (4096, 4096));
        assert!(config.adjusts_linux_process());

        // Nothing adjusted: the defaults.
        let bare = ContainerTaskConfig::from_value(&json!({ "image": "a", "command": ["x"] }))?;
        assert_eq!(bare.process_policy()?, oci_bundle::ProcessPolicy::default());
        assert!(!bare.adjusts_linux_process());
        Ok(())
    }

    /// P2-6: Apple `container` refuses VMs under 200 MiB. A task constrained
    /// to macOS is rejected at submit; any other macOS start fails early with
    /// the same message; Linux has no floor.
    #[test]
    fn apple_memory_floor_is_enforced_where_the_target_is_known() -> R<()> {
        let mut small = task_with(json!({ "memory": 128 }), Some("web"));
        // Not pinned to macOS: nothing to say at submit (it may never land on
        // a Mac), and Linux starts are unaffected.
        assert!(ContainerTaskPlugin.check_resources(&small)?.is_empty());
        let linux = ContainerRuntime::Oci {
            binary: "youki".into(),
        };
        check_apple_memory_floor(&linux, &small)?;

        // On a macOS worker the start fails before the runtime is invoked,
        // naming the floor and the field to raise.
        let err = check_apple_memory_floor(&ContainerRuntime::AppleContainer, &small)
            .expect_err("below the floor");
        let message = err.to_string();
        assert!(message.contains("200 MiB"), "{message}");
        assert!(message.contains("128"), "{message}");
        assert!(message.contains("resources.memory"), "{message}");

        // Constrained to macOS: rejected at submit.
        small.constraints = json!({ "os": "macos" });
        let err = ContainerTaskPlugin
            .check_resources(&small)
            .expect_err("rejected at submit");
        assert!(err.to_string().contains("200 MiB"), "{err}");
        // `memoryMax` sizes the VM when set, so it is what must clear the floor.
        let burst = TaskConfig {
            resources: json!({ "memory": 128, "memoryMax": 256 }),
            ..small.clone()
        };
        assert!(ContainerTaskPlugin.check_resources(&burst)?.is_empty());
        check_apple_memory_floor(&ContainerRuntime::AppleContainer, &burst)?;
        // Unsized: the runtime's own (1 GiB) default applies.
        let unsized_task = TaskConfig {
            resources: json!({}),
            ..small
        };
        check_apple_memory_floor(&ContainerRuntime::AppleContainer, &unsized_task)?;
        assert!(ContainerTaskPlugin
            .check_resources(&unsized_task)?
            .is_empty());
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
            None,
            &config.image,
        );
        assert!(args.contains(&"LEGION_PORT_HTTP=20000".to_string()));
        Ok(())
    }
}
