//! The `isolate` task runtime's jobspec surface (isolate-runtime plan §7.1,
//! §9): the typed `config` block, the defaults submit writes into a task,
//! the cross-task rules `job validate` checks, and the small facts the
//! worker and the manager share about built-in images.
//!
//! An isolate task is a session task (`stateful.mode = "session"`) whose
//! units are WebAssembly isolates in a node's `legion-isolate` host. Nothing
//! here runs anything: the worker's isolate supervisor does, against the
//! task's generated envelope.
//!
//! ```nickel
//! {
//!   name = "run",
//!   runtime = "isolate",
//!   config = {
//!     image = "python",
//!     limits = { cpu = "100ms", wall = "30s" },
//!     vm = { task = "machine" },
//!   },
//!   resources = { cpu = 10, memory = 16, memoryMax = 128 },
//!   stateful = { mode = "session", envelope = { resources = { cpu = 16000, memory = 32768 } } },
//!   children = { tasks = ["machine"] },
//! }
//! ```

use crate::prelude::*;
use crate::structs::{JobConfig, TaskConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// The runtime name a task declares.
pub const ISOLATE_RUNTIME: &str = "isolate";

/// Worker capability: the node found the `legion-isolate` binary and runs
/// isolate hosts. An isolate task's envelope carries an implicit
/// `capabilities contains "isolates"` constraint, so it is never placed on a
/// node that cannot run its isolates.
pub const ISOLATES_CAPABILITY: &str = "isolates";

/// Default `resources.memoryMax` for an isolate, in MiB: the cap on its
/// linear memory (calls entry 21).
pub const DEFAULT_MEMORY_MAX_MIB: u64 = 128;

/// The largest `resources.memoryMax` an isolate may declare, in MiB: a
/// 32-bit linear memory's whole address space, and the default worker
/// `isolate.maxMemory`.
pub const MAX_MEMORY_MAX_MIB: u64 = 4096;

/// Default `config.limits.output`.
pub const DEFAULT_OUTPUT: &str = "1MiB";

/// Default `config.vm.memoFor`.
pub const DEFAULT_MEMO_FOR: &str = "1h";

/// Default `resources.cpu` for an isolate, in MHz. An isolate's CPU is its
/// weight in its host's shard scheduler, not a reservation of a core; the
/// envelope's CPU is the job's share of the node.
pub const DEFAULT_CPU_MHZ: u64 = 10;

/// Default `resources.memory` (the reservation) for a `module` task, in MiB,
/// whose idle size Legion cannot know ahead of time.
pub const MODULE_RESERVATION_MIB: u64 = 1;

/// Where a run's files live inside an isolate's in-memory scratch tree: the
/// same path a VM run uses ([`crate::structs::run::RUN_DIR`]).
pub const RUN_DIR: &str = crate::structs::run::RUN_DIR;

/// The HTTP request header that names a request's tenant on ingress
/// (`lifetime = "tenant"`), as `RunSessionRequest.tenant` does for a run.
pub const TENANT_HEADER: &str = "Legion-Tenant";

/// The HTTP request header that names a session isolate on ingress
/// (`lifetime = "session"`), as `RunSessionRequest.session_id` does for a
/// run.
pub const SESSION_HEADER: &str = "Legion-Session";

/// The longest tenant key, in bytes.
pub const MAX_TENANT_KEY_LEN: usize = 128;

/// The longest `config.limits.wall`: a run's.
pub const MAX_WALL: Duration = crate::structs::run::MAX_RUN_WALL;

/// The largest `config.limits.output`: a run's reply is one message.
pub const MAX_OUTPUT: u64 = crate::structs::run::MAX_RUN_OUTPUT;

/// The `js` image's initial linear memory, in MiB, rounded up: the image
/// build measured 9,502,720 bytes (StarlingMonkey, componentize-js 0.23.0).
/// Submit refuses a `memoryMax` below it; a node checks the exact number in
/// its own image manifest when it prepares the task.
pub const JS_INITIAL_MEMORY_MIB: u64 = 10;
/// The `js` image's default reservation (`resources.memory`), in MiB: its
/// idle private memory, measured at about 0.75 MiB per instance, rounded
/// up.
pub const JS_DEFAULT_RESERVATION_MIB: u64 = 1;
/// The `python` image's initial linear memory, in MiB, rounded up: the
/// image build measured 24,313,856 bytes (CPython 3.14, componentize-py
/// 0.25.1).
pub const PYTHON_INITIAL_MEMORY_MIB: u64 = 24;
/// The `python` image's default reservation, in MiB: its idle private
/// memory, measured at about 1.5 MiB per instance, rounded up.
pub const PYTHON_DEFAULT_RESERVATION_MIB: u64 = 2;

/// What Legion knows about one built-in image without loading it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageFacts {
    /// The name a jobspec uses (`config.image`).
    pub name: &'static str,
    /// Source file extensions, preferred first. A run's entry is
    /// `main.<ext>` for the first of these the request carries.
    pub extensions: &'static [&'static str],
    /// The file a VM run stages the entry as, under [`RUN_DIR`].
    pub vm_entry: &'static str,
    /// The image's initial linear memory, in MiB: `memoryMax` may not be
    /// lower.
    pub initial_memory_mib: u64,
    /// `resources.memory` when the task sets none, in MiB.
    pub default_reservation_mib: u64,
}

/// The built-in images, by name.
pub const IMAGES: &[ImageFacts] = &[
    ImageFacts {
        name: "js",
        extensions: &["mjs", "js"],
        vm_entry: "main.mjs",
        initial_memory_mib: JS_INITIAL_MEMORY_MIB,
        default_reservation_mib: JS_DEFAULT_RESERVATION_MIB,
    },
    ImageFacts {
        name: "python",
        extensions: &["py"],
        vm_entry: "main.py",
        initial_memory_mib: PYTHON_INITIAL_MEMORY_MIB,
        default_reservation_mib: PYTHON_DEFAULT_RESERVATION_MIB,
    },
];

/// The facts of a built-in image, by its name or its pinned form
/// (`python@sha256:...`).
pub fn image_facts(image: &str) -> Option<&'static ImageFacts> {
    let name = image_name(image);
    IMAGES.iter().find(|facts| facts.name == name)
}

/// The image name of `config.image`, without a pinned digest.
pub fn image_name(image: &str) -> &str {
    image
        .trim()
        .split_once('@')
        .map_or(image.trim(), |(name, _)| name)
}

/// The built-in image names, for messages.
pub fn image_names() -> String {
    IMAGES
        .iter()
        .map(|facts| format!("\"{}\"", facts.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// How long an isolate lives (`config.lifetime`, plan §6.8).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IsolateLifetime {
    /// A fresh instance per request or run, destroyed after it.
    #[default]
    Request,
    /// One warm instance per tenant key, reused while it is resident.
    Tenant,
    /// An addressable instance with a session id, hibernated when idle.
    Session,
}

impl IsolateLifetime {
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolateLifetime::Request => "request",
            IsolateLifetime::Tenant => "tenant",
            IsolateLifetime::Session => "session",
        }
    }
}

/// Which host process an isolate shares (`config.cordon`, plan §6.3).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IsolateCordon {
    /// The host every isolate task of the job's namespace shares.
    #[default]
    Namespace,
    /// A host for this job alone.
    Job,
    /// One host for every namespace whose policy allows it.
    Shared,
}

impl IsolateCordon {
    /// Every cordon kind, in the order messages list them.
    pub const ALL: [IsolateCordon; 3] = [
        IsolateCordon::Namespace,
        IsolateCordon::Job,
        IsolateCordon::Shared,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            IsolateCordon::Namespace => "namespace",
            IsolateCordon::Job => "job",
            IsolateCordon::Shared => "shared",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == raw.trim())
    }
}

/// A component to run instead of a built-in image (`config.module`).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolateModule {
    /// Where the WebAssembly component comes from: an `https://`, `http://`,
    /// `s3://`, or `oci://` URL (`file://` too, which makes the task
    /// host-level). Fetched once per node through the worker's artifact
    /// fetchers and kept in the node content store.
    pub source: String,
    /// The SHA-256 of the component's bytes, as 64 hex digits or
    /// `sha256:<hex>`. Required: a task always runs exactly the module it
    /// was submitted with.
    pub sha256: String,
}

impl IsolateModule {
    /// The checksum in the `sha256:<hex>` form the artifact stager takes.
    pub fn checksum(&self) -> String {
        let hex = self.sha256.trim().to_ascii_lowercase();
        if hex.starts_with("sha256:") {
            hex
        } else {
            format!("sha256:{hex}")
        }
    }
}

/// Per-invocation budgets (`config.limits`, plan §6.11). Budgets, not
/// reservations: a run stopped by one says so in its reply.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolateLimits {
    /// CPU time per invocation, its turns summed, e.g. `"100ms"`. Required:
    /// budgets differ by orders of magnitude between a webhook and an agent
    /// loop.
    pub cpu: String,
    /// Wall-clock time per invocation, e.g. `"30s"`, at most `24h`.
    /// Required.
    pub wall: String,
    /// The most stdout plus stderr one invocation returns, e.g. `"256KiB"`.
    /// Default and maximum `1MiB`; output beyond it is dropped and the reply
    /// says `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// The VM a run of this task can go to instead (`config.vm`, plan §6.12).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolateVmConfig {
    /// The sibling `sandbox` session task whose `run` block a `tier = "vm"`
    /// run executes. Default: the only entry in `children.tasks`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    /// How long a node remembers that a code hash needed a VM, and sends
    /// `tier = "auto"` runs of it straight to one. Default `"1h"`; `"0s"`
    /// remembers nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memo_for: Option<String>,
}

fn default_isolate_env() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// `config` of an `isolate` task.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolateTaskConfig {
    /// A built-in image to run: `"js"` or `"python"`, optionally pinned to a
    /// build as `name@sha256:<hex>`. Exactly one of `image` and `module`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// The script an image runs for every invocation, staged from the task's
    /// `artifacts`: a path relative to the task directory (`"main.py"`) or
    /// `${artifact.<name>}`. Omit for dynamic code, which each run brings in
    /// its files. Images only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
    /// A WebAssembly component to run instead of an image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<IsolateModule>,
    /// How long an instance lives: `"request"` (a fresh instance per
    /// invocation, the default), `"tenant"` (one warm instance per tenant
    /// key), or `"session"` (an addressable instance with a session id).
    #[serde(default)]
    pub lifetime: IsolateLifetime,
    /// Per-invocation CPU, wall-clock, and output budgets. `cpu` and `wall`
    /// are required.
    pub limits: IsolateLimits,
    /// Which host process the task's isolates share: `"namespace"` (the
    /// default), `"job"`, or `"shared"` (only where namespace policy lists
    /// it in `isolateCordons`).
    #[serde(default)]
    pub cordon: IsolateCordon,
    /// The VM tier: the sibling sandbox task a run can execute in instead.
    /// Absent: this task's runs always run as isolates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<IsolateVmConfig>,
    /// Environment variables every isolate of the task sees.
    #[serde(default = "default_isolate_env")]
    #[schemars(default = "default_isolate_env")]
    pub env: BTreeMap<String, String>,
}

/// A built-in image reference, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub facts: &'static ImageFacts,
    /// The pinned build, `sha256:<hex>`, when the reference pins one.
    pub digest: Option<String>,
}

/// [`IsolateTaskConfig`] with its budgets parsed and its rules checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIsolate {
    /// The built-in image, for an image task.
    pub image: Option<ImageRef>,
    pub entry: Option<String>,
    /// The component, for a module task (`source`, `sha256:<hex>`).
    pub module: Option<IsolateModule>,
    pub lifetime: IsolateLifetime,
    pub cordon: IsolateCordon,
    pub cpu: Duration,
    pub wall: Duration,
    /// `limits.wall` as written, for messages.
    pub wall_text: String,
    pub output: u64,
    /// `vm.task` as written (see [`vm_task_name`] for the default).
    pub vm_task: Option<String>,
    /// `vm.memoFor`: zero when the node should remember nothing.
    pub memo_for: Duration,
    pub env: BTreeMap<String, String>,
}

impl ResolvedIsolate {
    /// `resources.memory` this task's isolates default to, in MiB.
    pub fn default_reservation_mib(&self) -> u64 {
        self.image.as_ref().map_or(MODULE_RESERVATION_MIB, |image| {
            image.facts.default_reservation_mib
        })
    }
}

impl IsolateTaskConfig {
    /// Parse `config` of an isolate task. The error for a missing `cpu` or
    /// `wall` is the one plan §7.1 gives, not serde's.
    pub fn from_value(config: &Value) -> Result<Self, String> {
        let limits = config.get("limits");
        let has = |key: &str| {
            limits
                .and_then(|limits| limits.get(key))
                .is_some_and(|value| !value.is_null())
        };
        if config.is_object() && (!has("cpu") || !has("wall")) {
            return Err(LIMITS_REQUIRED.to_string());
        }
        serde_json::from_value(config.clone()).map_err(|err| format!("config: {err}"))
    }

    /// The config of `task`, when it is an isolate task.
    pub fn of(task: &TaskConfig) -> R<Option<Self>> {
        if !is_isolate_task(task) {
            return Ok(None);
        }
        Self::from_value(&task.config)
            .map(Some)
            .map_err(|err| anyhow!("task {}: {err}", task.name))
    }

    /// Check the rules that need only this block and parse its budgets.
    /// Messages name the field and carry no task prefix.
    pub fn resolve(&self) -> Result<ResolvedIsolate, String> {
        let image =
            match (self.image.as_deref(), self.module.as_ref()) {
                (Some(_), Some(_)) => return Err(
                    "config sets both image and module; set exactly one: image runs a built-in \
                     interpreter, module runs your own component"
                        .into(),
                ),
                (None, None) => {
                    return Err(format!(
                        "config needs image or module: image = one of {} runs a built-in \
                     interpreter, module = {{ source, sha256 }} runs your own component",
                        image_names()
                    ))
                }
                (Some(raw), None) => Some(parse_image(raw)?),
                (None, Some(_)) => None,
            };
        let module = self.module.as_ref().map(validate_module).transpose()?;
        if let Some(entry) = self.entry.as_deref() {
            if module.is_some() {
                return Err(
                    "config.entry is for images (the script an interpreter runs); a module \
                     names its own entry point\nHint: remove config.entry"
                        .into(),
                );
            }
            validate_entry(entry)?;
        }
        let cpu = parse_budget("config.limits.cpu", &self.limits.cpu)?;
        let wall = parse_budget("config.limits.wall", &self.limits.wall)?;
        if wall > MAX_WALL {
            return Err(format!(
                "config.limits.wall {} exceeds the {} maximum\nHint: run longer work with tier = \
                 \"vm\", or start a session",
                self.limits.wall.trim(),
                humantime::format_duration(MAX_WALL)
            ));
        }
        if cpu > wall {
            return Err(format!(
                "config.limits.cpu {} is above config.limits.wall {}, so it can never be \
                 reached: an isolate runs on one thread\nHint: lower limits.cpu to at most \
                 limits.wall",
                self.limits.cpu.trim(),
                self.limits.wall.trim()
            ));
        }
        let output = match self.limits.output.as_deref() {
            None => MAX_OUTPUT,
            Some(raw) => {
                let bytes = crate::structs::config::parse_byte_size(raw).map_err(|err| {
                    format!("config.limits.output {raw:?} is not a size: {err:#}")
                })?;
                if bytes == 0 {
                    return Err("config.limits.output must be greater than zero".into());
                }
                if bytes > MAX_OUTPUT {
                    return Err(format!(
                        "config.limits.output {} exceeds the 1MiB maximum: a run's reply is one \
                         message\nHint: write larger results to a file the caller gets",
                        raw.trim()
                    ));
                }
                bytes
            }
        };
        let (vm_task, memo_for) = match self.vm.as_ref() {
            None => (None, parse_memo_for(DEFAULT_MEMO_FOR)?),
            Some(vm) => {
                let task = vm
                    .task
                    .as_deref()
                    .map(str::trim)
                    .map(|task| {
                        if task.is_empty() {
                            Err("config.vm.task must name a task".to_string())
                        } else {
                            Ok(task.to_string())
                        }
                    })
                    .transpose()?;
                let memo_for = parse_memo_for(vm.memo_for.as_deref().unwrap_or(DEFAULT_MEMO_FOR))?;
                (task, memo_for)
            }
        };
        for key in self.env.keys() {
            if key.is_empty() || key.contains('=') || key.contains('\0') {
                return Err(format!(
                    "config.env key {key:?} is not a valid variable name (empty, or contains \
                     '=' or NUL)"
                ));
            }
        }
        Ok(ResolvedIsolate {
            image,
            entry: self.entry.as_deref().map(|entry| entry.trim().to_string()),
            module,
            lifetime: self.lifetime,
            cordon: self.cordon,
            cpu,
            wall,
            wall_text: self.limits.wall.trim().to_string(),
            output,
            vm_task,
            memo_for,
            env: self.env.clone(),
        })
    }
}

/// The error for a missing `limits.cpu` or `limits.wall` (calls entry 21).
const LIMITS_REQUIRED: &str = "config.limits.cpu and config.limits.wall are required (budgets \
                               differ by orders of magnitude between a webhook and an agent \
                               loop; there is no default)";

fn parse_budget(field: &str, raw: &str) -> Result<Duration, String> {
    if raw.trim().is_empty() {
        return Err(LIMITS_REQUIRED.into());
    }
    let parsed = humantime::parse_duration(raw.trim())
        .map_err(|err| format!("{field} {raw:?} is not a duration ({err}); use e.g. \"100ms\""))?;
    if parsed.is_zero() {
        return Err(format!("{field} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_memo_for(raw: &str) -> Result<Duration, String> {
    humantime::parse_duration(raw.trim()).map_err(|err| {
        format!("config.vm.memoFor {raw:?} is not a duration ({err}); use e.g. \"1h\", or \"0s\"")
    })
}

fn parse_image(raw: &str) -> Result<ImageRef, String> {
    let raw = raw.trim();
    let (name, digest) = match raw.split_once('@') {
        None => (raw, None),
        Some((name, digest)) => {
            let hex = digest.strip_prefix("sha256:").unwrap_or_default();
            if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!(
                    "config.image {raw:?} pins a digest that is not sha256:<64 hex>\nHint: \
                     write {name}@sha256:<hex>, or {name:?} for the node's current build"
                ));
            }
            (name, Some(format!("sha256:{}", hex.to_ascii_lowercase())))
        }
    };
    let facts = image_facts(name).ok_or_else(|| {
        format!(
            "config.image {name:?} is not a built-in image; the images are {}\nHint: run your \
             own component with module = {{ source = \"...\", sha256 = \"...\" }}",
            image_names()
        )
    })?;
    Ok(ImageRef { facts, digest })
}

fn validate_module(module: &IsolateModule) -> Result<IsolateModule, String> {
    let source = module.source.trim();
    if source.is_empty() {
        return Err(
            "config.module.source is required: an https://, s3://, or oci:// URL of the \
             component"
                .into(),
        );
    }
    let parsed = crate::structs::artifact::parse_artifact_source(source)
        .map_err(|err| format!("config.module.source: {err:#}"))?;
    if !["http", "https", "s3", "oci", "file"].contains(&parsed.scheme.as_str()) {
        return Err(format!(
            "config.module.source scheme '{}' cannot serve a component; use https://, http://, \
             s3://, oci://, or file://",
            parsed.scheme
        ));
    }
    let checksum = IsolateModule {
        source: source.to_string(),
        sha256: module.sha256.clone(),
    }
    .checksum();
    if module.sha256.trim().is_empty() {
        return Err(
            "config.module.sha256 is required: the SHA-256 of the component (`sha256sum \
             handler.wasm` prints it)"
                .into(),
        );
    }
    crate::structs::artifact::validate_artifact_checksum(&checksum).map_err(|_| {
        format!(
            "config.module.sha256 {:?} is not a SHA-256: write 64 hex digits, or \
             sha256:<64 hex>",
            module.sha256.trim()
        )
    })?;
    Ok(IsolateModule {
        source: source.to_string(),
        sha256: checksum,
    })
}

/// `config.entry`: `${artifact.<name>}`, or a plain relative path.
fn validate_entry(entry: &str) -> Result<(), String> {
    let entry = entry.trim();
    if entry.is_empty() {
        return Err("config.entry must name a file; remove it for dynamic code".into());
    }
    if entry_artifact(entry).is_some() {
        return Ok(());
    }
    if entry.contains("${") {
        return Err(format!(
            "config.entry {entry:?} may only reference a whole artifact, as \
             ${{artifact.<name>}}"
        ));
    }
    if entry.starts_with('/') {
        return Err(format!(
            "config.entry {entry:?} must be relative to the task directory, where the task's \
             artifacts are staged (e.g. \"main.py\")"
        ));
    }
    if entry
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!(
            "config.entry {entry:?} must be a plain relative path (no empty, `.` or `..` \
             components)"
        ));
    }
    Ok(())
}

/// The artifact name an entry of the form `${artifact.<name>}` names.
pub fn entry_artifact(entry: &str) -> Option<&str> {
    entry
        .trim()
        .strip_prefix("${artifact.")?
        .strip_suffix('}')
        .filter(|name| !name.is_empty())
}

/// Whether `task` runs on the isolate runtime.
pub fn is_isolate_task(task: &TaskConfig) -> bool {
    task.runtime == ISOLATE_RUNTIME
}

/// The cordon an isolate of `job_id` with `config` runs in: the job's
/// namespace, `job:<job id>`, or `shared`. One host process (or a set of
/// replicas) per cordon per node.
pub fn cordon_name(job_id: &str, config: &IsolateTaskConfig) -> String {
    match config.cordon {
        IsolateCordon::Namespace => crate::structs::namespace::job_namespace(job_id).to_string(),
        IsolateCordon::Job => format!("job:{job_id}"),
        IsolateCordon::Shared => "shared".to_string(),
    }
}

/// The task a `tier = "vm"` run of `task` executes in: `config.vm.task`,
/// else, when `config.vm` is present, the only entry in `children.tasks`.
/// `None` when the task has no VM tier (no `config.vm`, or no single child
/// to default to).
pub fn vm_task_name(task: &TaskConfig, config: &IsolateTaskConfig) -> Option<String> {
    let vm = config.vm.as_ref()?;
    if let Some(name) = vm.task.as_deref().map(str::trim) {
        return (!name.is_empty()).then(|| name.to_string());
    }
    match task
        .children
        .as_ref()
        .map(|children| children.tasks.as_slice())
    {
        Some([only]) => Some(only.clone()),
        _ => None,
    }
}

fn resource_mib(task: &TaskConfig, key: &str) -> Option<u64> {
    task.resources
        .get(key)
        .and_then(Value::as_f64)
        .filter(|mib| mib.is_finite() && *mib >= 0.0)
        .map(|mib| mib.ceil() as u64)
}

/// The isolate's linear-memory ceiling, in MiB: `resources.memoryMax`, else
/// [`DEFAULT_MEMORY_MAX_MIB`].
pub fn memory_max_mib(task: &TaskConfig) -> u64 {
    resource_mib(task, crate::resources::MEMORY_MAX_KEY).unwrap_or(DEFAULT_MEMORY_MAX_MIB)
}

/// What one resident isolate is charged against its envelope, in MiB:
/// `resources.memory`, else the image's default reservation (a module's is
/// [`MODULE_RESERVATION_MIB`]).
pub fn reservation_mib(task: &TaskConfig) -> u64 {
    if let Some(mib) = resource_mib(task, "memory") {
        return mib;
    }
    task.config
        .get("image")
        .and_then(Value::as_str)
        .and_then(image_facts)
        .map_or(MODULE_RESERVATION_MIB, |facts| {
            facts.default_reservation_mib
        })
}

/// The file a run executes, among the names a request stages: `main.<ext>`
/// for the image's first extension the request carries (any top-level
/// `main.*` for a module task, whose `image` is `None`), else the first
/// file. Names may be relative or under [`RUN_DIR`]. `None` when the
/// request stages nothing.
pub fn run_entry<'a, S: AsRef<str>>(image: Option<&str>, files: &'a [S]) -> Option<&'a str> {
    let relative = |name: &'a str| -> &'a str {
        let trimmed = name.trim();
        match trimmed.strip_prefix(RUN_DIR) {
            Some(rest) if rest.starts_with('/') => rest.trim_start_matches('/'),
            _ => trimmed,
        }
    };
    let names: Vec<&'a str> = files.iter().map(|file| file.as_ref()).collect();
    let found = match image.and_then(image_facts) {
        Some(facts) => facts.extensions.iter().find_map(|ext| {
            names
                .iter()
                .copied()
                .find(|name| relative(name) == format!("main.{ext}"))
        }),
        None => names.iter().copied().find(|name| {
            relative(name)
                .strip_prefix("main.")
                .is_some_and(|ext| !ext.is_empty() && !ext.contains('/'))
        }),
    };
    found.or_else(|| names.first().copied())
}

/// The name a VM run stages an image task's entry as, under [`RUN_DIR`]:
/// `main.py` for `python`, `main.mjs` for `js`, `main` otherwise.
pub fn vm_entry_name(image: &str) -> &'static str {
    image_facts(image).map_or("main", |facts| facts.vm_entry)
}

/// Check a tenant key (`RunSessionRequest.tenant`, the [`TENANT_HEADER`]
/// header): 1 to [`MAX_TENANT_KEY_LEN`] bytes of printable ASCII without
/// spaces.
pub fn validate_tenant_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("the tenant key is empty".into());
    }
    if key.len() > MAX_TENANT_KEY_LEN {
        return Err(format!(
            "the tenant key is {} bytes, over the {MAX_TENANT_KEY_LEN}-byte maximum",
            key.len()
        ));
    }
    if let Some(bad) = key.chars().find(|c| !c.is_ascii_graphic()) {
        return Err(format!(
            "the tenant key contains {bad:?}; use printable ASCII without spaces"
        ));
    }
    Ok(())
}

/// The error `instance promote` answers for an isolate session (calls entry
/// 10), when `task` is an isolate task.
pub fn promote_refusal(task: &TaskConfig) -> Option<String> {
    is_isolate_task(task).then(|| {
        "isolate sessions cannot be promoted (a keyed instance is an allocation each); give the \
         session a stable id with an idempotency key, and keep what must outlive the node in \
         legion:isolate/state or an upstream store"
            .to_string()
    })
}

/// Fill an isolate task's resource defaults at submit (plan §6.11), ahead
/// of the cluster's `taskResourceDefaults`: `memoryMax` 128 MiB, `memory`
/// the image's default reservation (never above `memoryMax`), and `cpu`
/// [`DEFAULT_CPU_MHZ`] when neither `cpu` nor `cpus` nor `cores` is set.
pub fn normalize_task(task: &mut TaskConfig) {
    if !is_isolate_task(task) {
        return;
    }
    let reservation = reservation_mib(task);
    let ceiling = memory_max_mib(task);
    if !task.resources.is_object() {
        task.resources = Value::Object(Default::default());
    }
    let Some(resources) = task.resources.as_object_mut() else {
        return;
    };
    resources
        .entry(crate::resources::MEMORY_MAX_KEY)
        .or_insert_with(|| Value::from(ceiling));
    resources
        .entry("memory")
        .or_insert_with(|| Value::from(reservation.min(ceiling)));
    if !["cpu", "cpus", "cores"]
        .iter()
        .any(|key| resources.contains_key(*key))
    {
        resources.insert("cpu".into(), Value::from(DEFAULT_CPU_MHZ));
    }
}

/// Submit-time rules for every isolate task of `config` that need more than
/// the task's own `config` block: its session form, its resources, its VM
/// task, its `suspend` and `run` blocks, its network, and its artifacts.
/// Runs on a parsed (session-normalized) job. Each message starts
/// `[isolate] <job>/<task>: `.
pub fn validate_isolate_tasks(config: &JobConfig) -> Result<(), String> {
    for task in config.tasks.iter().filter(|task| is_isolate_task(task)) {
        validate_isolate_task(config, task)?;
    }
    Ok(())
}

fn validate_isolate_task(config: &JobConfig, task: &TaskConfig) -> Result<(), String> {
    let job = config.name.as_str();
    let prefix = format!("[isolate] {job}/{}", task.name);
    if !crate::structs::session::is_session_task(task) {
        return Err(format!(
            "{prefix}: runtime \"isolate\" needs stateful.mode = \"session\" (isolates are \
             node-owned and never written to the store; see docs/task-runtime-isolate.md)"
        ));
    }
    let parsed =
        IsolateTaskConfig::from_value(&task.config).map_err(|err| format!("{prefix}: {err}"))?;
    let resolved = parsed.resolve().map_err(|err| format!("{prefix}: {err}"))?;

    // Resources: the ceiling, and what an isolate cannot use.
    let ceiling = memory_max_mib(task);
    if ceiling > MAX_MEMORY_MAX_MIB {
        return Err(format!(
            "{prefix}: resources.memoryMax {ceiling} MiB is above this cluster's isolate maximum \
             {MAX_MEMORY_MAX_MIB} MiB (worker isolate.maxMemory); run it with tier = \"vm\" for \
             more"
        ));
    }
    if let Some(image) = resolved.image.as_ref() {
        if ceiling < image.facts.initial_memory_mib {
            return Err(format!(
                "{prefix}: resources.memoryMax {ceiling} MiB is below image \"{}\"'s initial \
                 linear memory ({} MiB)",
                image.facts.name, image.facts.initial_memory_mib
            ));
        }
    }
    if task.resources.get("cores").is_some() {
        return Err(format!(
            "{prefix}: resources.cores reserves whole CPUs, which an isolate cannot use: its CPU \
             is a weight in its host's shard scheduler\nHint: use resources.cpu (MHz)"
        ));
    }

    // Runs: an isolate task takes them natively.
    if task.run.is_some() {
        return Err(format!(
            "{prefix}: an isolate task takes runs without a run block (its budgets are \
             config.limits, its entry the run's main file)\nHint: remove run; put run.command \
             on the VM task (config.vm.task) instead"
        ));
    }

    // Promotion never applies (calls entry 10).
    if task
        .stateful
        .as_ref()
        .is_some_and(|stateful| stateful.promote.is_some())
    {
        return Err(format!(
            "{prefix}: stateful.promote does not apply: isolate sessions are never promoted to \
             keyed instances\nHint: remove promote; a named isolate takes a stable id from an \
             idempotency key"
        ));
    }

    // `suspend`: session isolates only, and the isolate's own checkpoint.
    if let Some(suspend) = task.suspend.as_ref() {
        if resolved.lifetime != IsolateLifetime::Session {
            return Err(format!(
                "{prefix}: suspend applies to session isolates only, and config.lifetime is \
                 \"{}\": only a session isolate has an id to suspend under and wake by\nHint: \
                 set config.lifetime = \"session\", or remove suspend",
                resolved.lifetime.as_str()
            ));
        }
        let refused = [
            ("checkpoint", suspend.checkpoint.is_some()),
            ("checkpointEvery", suspend.checkpoint_every.is_some()),
            ("archive", suspend.archive.is_some()),
            ("park", suspend.park.is_some()),
            ("onResume", suspend.on_resume.is_some()),
            ("wake", suspend.wake.is_some()),
        ];
        if let Some((field, _)) = refused.iter().find(|(_, set)| *set) {
            return Err(format!(
                "{prefix}: suspend.{field} does not apply to an isolate: an isolate hibernates \
                 to its own checkpoint format at a turn boundary, and wakes on its next \
                 request\nHint: remove suspend.{field}; idle.after, keepFor, durability, and \
                 onWorkerStop apply"
            ));
        }
        // Not built yet: the node keeps an isolate's checkpoint in its own
        // content store and never publishes it (the node refuses it too, as
        // a backstop for a job stored before this check).
        if suspend
            .durability
            .as_deref()
            .is_some_and(|durability| durability.trim().eq_ignore_ascii_case("store"))
        {
            return Err(format!(
                "{prefix}: suspend.durability = \"store\" is not available for isolates yet: a \
                 session isolate's checkpoint stays on the node that took it, so it wakes only \
                 there and is lost with the node\nHint: remove suspend.durability (the default \
                 is \"local\"), and keep what must outlive the node in an upstream store"
            ));
        }
    }

    // Network: the envelope carries it, gated, into the node datapath.
    if let Some(network) = task.network.as_ref() {
        let mode = network.mode.trim().to_ascii_lowercase();
        if !mode.is_empty()
            && mode != crate::structs::netpolicy::MODE_AUTO
            && mode != crate::structs::netpolicy::MODE_GATED
        {
            return Err(format!(
                "{prefix}: network.mode = {:?} does not apply: an isolate has no network of its \
                 own, and all its I/O goes through the node datapath (gated)\nHint: remove \
                 network.mode",
                network.mode
            ));
        }
    }

    if task
        .connect
        .as_ref()
        .is_some_and(|connect| !connect.templates.is_empty())
    {
        return Err(format!(
            "{prefix}: connect.templates renders files into a task directory, which an isolate \
             does not have\nHint: remove templates; an isolate reaches an upstream by its \
             name through the host API"
        ));
    }

    // Artifacts deliver the entry, and nothing else reads them.
    match resolved.entry.as_deref() {
        None if !task.artifacts.is_empty() => {
            return Err(format!(
                "{prefix}: artifacts on an isolate task deliver its config.entry, and it sets \
                 none\nHint: set config.entry to the staged script, or remove artifacts"
            ))
        }
        Some(_) if task.artifacts.is_empty() => {
            return Err(format!(
                "{prefix}: config.entry names a file the task's artifacts deliver, and it \
                 declares none\nHint: add an artifact (artifacts = [{{ source = \"https://...\", \
                 dest = \"main.py\" }}]), or remove entry to take code with each run"
            ))
        }
        Some(entry) => {
            if let Some(name) = entry_artifact(entry) {
                if !task
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.name.as_deref() == Some(name))
                {
                    return Err(format!(
                        "{prefix}: config.entry names artifact {name:?}, which the task does \
                         not declare\nHint: give one of its artifacts name = {name:?}"
                    ));
                }
            }
        }
        None => {}
    }

    // The VM tier.
    if let Some(vm) = parsed.vm.as_ref() {
        let children: &[String] = task
            .children
            .as_ref()
            .map(|children| children.tasks.as_slice())
            .unwrap_or_default();
        let vm_task = match vm_task_name(task, &parsed) {
            Some(name) => name,
            None if vm.task.is_none() => {
                return Err(format!(
                    "{prefix}: config.vm has no task, and children.tasks lists {} tasks, so \
                     there is no default\nHint: set config.vm.task to the sandbox task runs \
                     burst to, and list it in children.tasks",
                    children.len()
                ))
            }
            None => {
                return Err(format!("{prefix}: config.vm.task must name a task"));
            }
        };
        if !children.contains(&vm_task) {
            return Err(format!(
                "{prefix}: config.vm.task \"{vm_task}\" is not in children.tasks; add it there, \
                 which is what lets this task's isolates start it"
            ));
        }
        let Some(machine) = config.tasks.iter().find(|other| other.name == vm_task) else {
            return Err(format!(
                "{prefix}: config.vm.task \"{vm_task}\" is not a task of this job"
            ));
        };
        let machine_prefix = format!("[isolate] {job}/{vm_task}");
        if machine.runtime != "sandbox" || !crate::structs::session::is_session_task(machine) {
            return Err(format!(
                "{machine_prefix}: config.vm.task of {job}/{} names {vm_task}, which is not a \
                 session task on the sandbox runtime (runtime {:?}); a VM run is a fresh \
                 single-use sandbox session\nHint: give {vm_task} runtime = \"sandbox\" and \
                 stateful.mode = \"session\"",
                task.name, machine.runtime
            ));
        }
        if machine.run.is_none() {
            return Err(format!(
                "{machine_prefix}: config.vm.task of {job}/{} names {vm_task}, which has no run \
                 block; add run.command so tier = \"vm\" runs know what to execute",
                task.name
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Worker config: the `isolate` block (plan §9, "Worker config")
// ---------------------------------------------------------------------------

/// Default `isolate.maxMemory`: the largest `memoryMax` an isolate on this
/// worker type may have, and the size of one pooling-allocator slot.
pub const DEF_WORKER_MAX_MEMORY: &str = "4GiB";
/// Default `isolate.slotsPerHost`.
pub const DEF_SLOTS_PER_HOST: u32 = 10_000;
/// Default `isolate.epoch`.
pub const DEF_EPOCH: &str = "1ms";
/// Default `isolate.slice`.
pub const DEF_SLICE: &str = "5ms";
/// Default `isolate.pageOutAfter`.
pub const DEF_PAGE_OUT_AFTER: &str = "2s";
/// Default `isolate.maxBodyBytes`.
pub const DEF_MAX_BODY_BYTES: &str = "16MiB";
/// The directory beside the worker's executable that holds built image
/// components and their manifest, when `isolate.images` is unset.
pub const DEF_IMAGES_DIR: &str = "isolate-images";
/// The environment variable naming the image directory, read when
/// `isolate.images` is unset and no [`DEF_IMAGES_DIR`] is beside the
/// worker.
pub const ENV_ISOLATE_IMAGES: &str = "LEGION_ISOLATE_IMAGES";
/// The environment variable naming the `legion-isolate` binary, read when
/// none is beside the worker's executable and `isolate.binary` is unset.
pub const ENV_ISOLATE_BINARY: &str = "LEGION_ISOLATE_BINARY";
/// The binary's file name, looked for beside the worker's executable.
pub const ISOLATE_BINARY_NAME: &str = "legion-isolate";

/// What separates one cordon from another on a node (`isolate.cordonBoundary`).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum CordonBoundary {
    /// A confined host process per cordon.
    #[default]
    Process,
    /// A microVM per cordon, the host process inside it. Not available:
    /// [`WorkerIsolateConfigFull::validate`] refuses it, so a worker config
    /// that asks for it stops the manager instead of being run as
    /// `process`.
    Vm,
}

/// How isolates' linear memories are laid out in a host's address space
/// (`isolate.layout`, plan §6.4).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SlotLayout {
    /// A 4 GiB reservation plus guard pages per slot: bounds checks elided,
    /// about 16,000 slots per process on x86-64.
    #[default]
    GuardPages,
    /// Memory-protection-key striping: up to 15 slots share a reservation.
    /// x86-64 Linux with PKU only.
    Mpk,
    /// Small reservations with explicit bounds checks: the densest and the
    /// slowest.
    Small,
}

impl SlotLayout {
    /// The most slots one host process can lay out this way on a 47-bit
    /// x86-64 address space, with room left for the host itself.
    pub fn max_slots(&self) -> u32 {
        match self {
            SlotLayout::GuardPages => 16_000,
            SlotLayout::Mpk => 200_000,
            SlotLayout::Small => 1_000_000,
        }
    }
}

/// How strictly a host must confine itself before it serves
/// (`isolate.confine`).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ConfineMode {
    /// Every layer (namespaces, `no_new_privs`, Landlock, seccomp) must
    /// apply, or the host exits before it loads a module.
    #[default]
    Enforce,
    /// Apply what the kernel offers and report the rest.
    BestEffort,
    /// Apply nothing: development and tests only.
    Off,
}

/// A worker type's `isolate` block, as written (docs/task-runtime-isolate.md).
/// Every field is optional; [`WorkerIsolateConfig::with_defaults`] fills
/// them.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerIsolateConfig {
    /// The `legion-isolate` binary. Default: the one beside the worker's
    /// executable, then `LEGION_ISOLATE_BINARY`. A worker that finds none
    /// does not advertise the `isolates` capability.
    #[serde(default)]
    pub binary: Option<String>,
    /// The largest `resources.memoryMax` an isolate may have on this worker
    /// type, which is also the size of one memory slot. Default `4GiB`, at
    /// most `4GiB` (a 32-bit linear memory).
    #[serde(default)]
    pub max_memory: Option<String>,
    /// Linear-memory slots per host process: the most isolates one process
    /// holds resident. Default 10000; at most what the layout fits in the
    /// address space (16000 for `guard-pages`). A cordon that needs more
    /// runs another host process.
    #[serde(default)]
    pub slots_per_host: Option<u32>,
    /// Shard threads per host process. `0`, the default, is one per CPU the
    /// host may run on.
    #[serde(default)]
    pub shards: Option<u32>,
    /// How often the epoch ticker advances, which bounds how late a CPU
    /// limit or a slice is noticed. Default `1ms`.
    #[serde(default)]
    pub epoch: Option<String>,
    /// How long a turn runs before it yields its shard to the next isolate.
    /// Default `5ms`; at least `epoch`.
    #[serde(default)]
    pub slice: Option<String>,
    /// Idle time after which a resident isolate's private pages are paged
    /// out. Default `2s`; `0s` never pages out.
    #[serde(default)]
    pub page_out_after: Option<String>,
    /// What separates cordons: `process` (the default and the only value
    /// available: a confined host process each). `vm` (a microVM each) is
    /// refused at validation; it is not built.
    #[serde(default)]
    pub cordon_boundary: Option<CordonBoundary>,
    /// How slots are laid out: `guard-pages` (the default), `mpk`, or
    /// `small`.
    #[serde(default)]
    pub layout: Option<SlotLayout>,
    /// How strictly a host confines itself: `enforce` (the default),
    /// `best-effort`, or `off`.
    #[serde(default)]
    pub confine: Option<ConfineMode>,
    /// The directory of built image components and their manifest. Default:
    /// `isolate-images` beside the worker's executable, then
    /// `LEGION_ISOLATE_IMAGES`.
    #[serde(default)]
    pub images: Option<String>,
    /// The largest request or response body a host buffers. Default
    /// `16MiB`.
    #[serde(default)]
    pub max_body_bytes: Option<String>,
}

/// Resolved [`WorkerIsolateConfig`]: what the manager carries to the worker
/// in [`crate::structs::WorkerArtifactsPayload::isolate`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkerIsolateConfigFull {
    /// `None`: look beside the worker, then `LEGION_ISOLATE_BINARY`.
    pub binary: Option<String>,
    pub max_memory: String,
    pub slots_per_host: u32,
    /// `0` means one per CPU, resolved on the node.
    pub shards: u32,
    pub epoch: String,
    pub slice: String,
    pub page_out_after: String,
    pub cordon_boundary: CordonBoundary,
    pub layout: SlotLayout,
    pub confine: ConfineMode,
    /// `None`: `isolate-images` beside the worker, then
    /// `LEGION_ISOLATE_IMAGES`.
    pub images: Option<String>,
    pub max_body_bytes: String,
}

impl Default for WorkerIsolateConfigFull {
    fn default() -> Self {
        WorkerIsolateConfigFull {
            binary: None,
            max_memory: DEF_WORKER_MAX_MEMORY.into(),
            slots_per_host: DEF_SLOTS_PER_HOST,
            shards: 0,
            epoch: DEF_EPOCH.into(),
            slice: DEF_SLICE.into(),
            page_out_after: DEF_PAGE_OUT_AFTER.into(),
            cordon_boundary: CordonBoundary::default(),
            layout: SlotLayout::default(),
            confine: ConfineMode::default(),
            images: None,
            max_body_bytes: DEF_MAX_BODY_BYTES.into(),
        }
    }
}

impl WorkerIsolateConfig {
    pub fn with_defaults(&self) -> WorkerIsolateConfigFull {
        let defaults = WorkerIsolateConfigFull::default();
        WorkerIsolateConfigFull {
            binary: self.binary.clone().or(defaults.binary),
            max_memory: self.max_memory.clone().unwrap_or(defaults.max_memory),
            slots_per_host: self.slots_per_host.unwrap_or(defaults.slots_per_host),
            shards: self.shards.unwrap_or(defaults.shards),
            epoch: self.epoch.clone().unwrap_or(defaults.epoch),
            slice: self.slice.clone().unwrap_or(defaults.slice),
            page_out_after: self
                .page_out_after
                .clone()
                .unwrap_or(defaults.page_out_after),
            cordon_boundary: self.cordon_boundary.unwrap_or(defaults.cordon_boundary),
            layout: self.layout.unwrap_or(defaults.layout),
            confine: self.confine.unwrap_or(defaults.confine),
            images: self.images.clone().or(defaults.images),
            max_body_bytes: self
                .max_body_bytes
                .clone()
                .unwrap_or(defaults.max_body_bytes),
        }
    }
}

fn worker_duration(field: &str, raw: &str) -> R<Duration> {
    humantime::parse_duration(raw.trim())
        .map_err(|err| anyhow!("isolate.{field} {raw:?} is not a duration ({err})"))
}

impl WorkerIsolateConfigFull {
    /// Reject values a node cannot run with, so a bad worker config stops
    /// the manager at startup instead of failing on every node of the type.
    pub fn validate(&self) -> R<()> {
        for (field, path) in [("binary", &self.binary), ("images", &self.images)] {
            if let Some(path) = path.as_deref() {
                if !path.starts_with('/') {
                    bail!("isolate.{field} must be an absolute path, got {path:?}");
                }
            }
        }
        if self.cordon_boundary == CordonBoundary::Vm {
            bail!(
                "isolate.cordonBoundary = \"vm\" is not available: cordon hosts run as \
                 confined processes on this release\nHint: remove isolate.cordonBoundary, or \
                 set it to \"process\""
            );
        }
        let max_memory = self.max_memory_bytes()?;
        if !(1 << 20..=MAX_MEMORY_MAX_MIB << 20).contains(&max_memory) {
            bail!(
                "isolate.maxMemory {:?} must be between 1MiB and 4GiB (a 32-bit linear memory)",
                self.max_memory
            );
        }
        let max_slots = self.layout.max_slots();
        if self.slots_per_host == 0 || self.slots_per_host > max_slots {
            bail!(
                "isolate.slotsPerHost must be between 1 and {max_slots} with layout {:?}, got {}: \
                 each slot reserves address space, and a cordon that needs more slots runs \
                 another host process",
                serde_json::to_value(self.layout)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_string))
                    .unwrap_or_default(),
                self.slots_per_host
            );
        }
        let epoch = self.epoch()?;
        let slice = self.slice()?;
        if epoch.is_zero() {
            bail!("isolate.epoch must be greater than zero");
        }
        if slice < epoch {
            bail!(
                "isolate.slice {:?} is shorter than isolate.epoch {:?}; a turn is only noticed \
                 at an epoch tick",
                self.slice,
                self.epoch
            );
        }
        self.page_out_after()?;
        if self.max_body_bytes()? == 0 {
            bail!("isolate.maxBodyBytes must be greater than zero");
        }
        Ok(())
    }

    /// `maxMemory` in bytes.
    pub fn max_memory_bytes(&self) -> R<u64> {
        crate::structs::config::parse_byte_size(&self.max_memory)
            .with_context(|| format!("invalid isolate.maxMemory {:?}", self.max_memory))
    }

    /// `maxBodyBytes` in bytes.
    pub fn max_body_bytes(&self) -> R<u64> {
        crate::structs::config::parse_byte_size(&self.max_body_bytes)
            .with_context(|| format!("invalid isolate.maxBodyBytes {:?}", self.max_body_bytes))
    }

    pub fn epoch(&self) -> R<Duration> {
        worker_duration("epoch", &self.epoch)
    }

    pub fn slice(&self) -> R<Duration> {
        worker_duration("slice", &self.slice)
    }

    /// `pageOutAfter`; zero means never.
    pub fn page_out_after(&self) -> R<Duration> {
        worker_duration("pageOutAfter", &self.page_out_after)
    }

    /// The shard count this node runs with: the configured value, or one per
    /// CPU.
    pub fn resolved_shards(&self) -> u32 {
        if self.shards > 0 {
            return self.shards;
        }
        std::thread::available_parallelism()
            .map(|cores| cores.get() as u32)
            .unwrap_or(1)
    }
}

#[cfg(test)]
mod isolate_test {
    use super::*;

    const HEX: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    fn session() -> Value {
        json!({
            "mode": "session",
            "envelope": { "resources": { "cpu": 16000, "memory": 32768 } },
        })
    }

    fn isolate_task(config: Value) -> Value {
        json!({
            "name": "run",
            "runtime": "isolate",
            "config": config,
            "resources": { "cpu": 10, "memory": 16, "memoryMax": 128 },
            "stateful": session(),
            "children": { "tasks": ["machine"] },
        })
    }

    fn machine() -> Value {
        json!({
            "name": "machine",
            "runtime": "sandbox",
            "config": { "image": "python:3.13-slim", "command": ["sleep", "infinity"] },
            "resources": { "cpu": 1000, "memory": 1024 },
            "stateful": {
                "mode": "session",
                "envelope": { "resources": { "cpu": 8000, "memory": 16384 } },
            },
            "run": { "command": ["python3", "/legion/run/main.py"], "wall": "5m" },
        })
    }

    fn base_config() -> Value {
        json!({
            "image": "python",
            "limits": { "cpu": "100ms", "wall": "30s" },
            "vm": { "task": "machine" },
        })
    }

    fn job(tasks: Vec<Value>) -> JobConfig {
        serde_json::from_value(json!({ "name": "code-runner", "tasks": tasks })).expect("job")
    }

    fn check(tasks: Vec<Value>) -> Result<(), String> {
        validate_isolate_tasks(&job(tasks))
    }

    fn with(mut task: Value, path: &[&str], value: Value) -> Value {
        let mut at = &mut task;
        for key in &path[..path.len() - 1] {
            at = at.get_mut(*key).expect("path");
        }
        at[path[path.len() - 1]] = value;
        task
    }

    #[test]
    fn the_plan_example_validates_and_resolves_test() {
        check(vec![isolate_task(base_config()), machine()]).unwrap();
        let config = IsolateTaskConfig::from_value(&base_config()).unwrap();
        let resolved = config.resolve().unwrap();
        assert_eq!(resolved.image.as_ref().unwrap().facts.name, "python");
        assert_eq!(resolved.cpu, Duration::from_millis(100));
        assert_eq!(resolved.wall, Duration::from_secs(30));
        assert_eq!(resolved.output, MAX_OUTPUT);
        assert_eq!(resolved.lifetime, IsolateLifetime::Request);
        assert_eq!(resolved.cordon, IsolateCordon::Namespace);
        assert_eq!(resolved.memo_for, Duration::from_secs(3600));
        assert_eq!(
            resolved.default_reservation_mib(),
            PYTHON_DEFAULT_RESERVATION_MIB
        );
    }

    #[test]
    fn limits_cpu_and_wall_are_required_test() {
        for limits in [json!({ "cpu": "1s" }), json!({ "wall": "1s" }), Value::Null] {
            let mut config = base_config();
            config["limits"] = limits;
            let err = check(vec![isolate_task(config), machine()]).unwrap_err();
            assert_eq!(
                err,
                "[isolate] code-runner/run: config.limits.cpu and config.limits.wall are \
                 required (budgets differ by orders of magnitude between a webhook and an \
                 agent loop; there is no default)"
            );
        }
    }

    #[test]
    fn config_rules_name_the_field_test() {
        let cases = [
            (
                json!({ "image": "python", "module": { "source": "https://x/a.wasm", "sha256": HEX } }),
                "exactly one",
            ),
            (json!({}), "needs image or module"),
            (
                json!({ "image": "ruby" }),
                "not a built-in image; the images are \"js\", \"python\"",
            ),
            (json!({ "image": "js@sha256:abc" }), "not sha256:<64 hex>"),
            (
                json!({ "module": { "source": "https://x/a.wasm", "sha256": "nope" } }),
                "is not a SHA-256",
            ),
            (
                json!({ "module": { "source": "https://x/a.wasm", "sha256": "" } }),
                "sha256 is required",
            ),
            (
                json!({ "module": { "source": "ftp://x/a.wasm", "sha256": HEX } }),
                "cannot serve a component",
            ),
            (
                json!({ "module": { "source": "https://x/a.wasm", "sha256": HEX }, "entry": "main.py" }),
                "entry is for images",
            ),
            (
                json!({ "image": "python", "entry": "/abs/main.py" }),
                "relative to the task directory",
            ),
            (
                json!({ "image": "python", "entry": "../main.py" }),
                "plain relative path",
            ),
            (
                json!({ "image": "python", "limits": { "cpu": "soon", "wall": "1s" } }),
                "limits.cpu \"soon\" is not a duration",
            ),
            (
                json!({ "image": "python", "limits": { "cpu": "2s", "wall": "1s" } }),
                "can never be reached",
            ),
            (
                json!({ "image": "python", "limits": { "cpu": "1s", "wall": "48h" } }),
                "exceeds the 1day maximum",
            ),
            (
                json!({ "image": "python", "limits": { "cpu": "1s", "wall": "2s", "output": "2MiB" } }),
                "1MiB maximum",
            ),
            (
                json!({ "image": "python", "vm": { "task": "machine", "memoFor": "later" } }),
                "memoFor",
            ),
            (
                json!({ "image": "python", "lifetime": "forever" }),
                "unknown variant",
            ),
            (
                json!({ "image": "python", "lifetme": "tenant" }),
                "unknown field",
            ),
        ];
        for (overlay, needle) in cases {
            let mut config = json!({ "limits": { "cpu": "100ms", "wall": "30s" } });
            for (key, value) in overlay.as_object().unwrap() {
                config[key] = value.clone();
            }
            let err = check(vec![isolate_task(config.clone()), machine()])
                .expect_err(&format!("{config}"));
            assert!(err.starts_with("[isolate] code-runner/run: "), "{err}");
            assert!(err.contains(needle), "{needle}: {err}");
        }
    }

    #[test]
    fn a_pinned_image_and_a_module_resolve_test() {
        let pinned = IsolateTaskConfig::from_value(&json!({
            "image": format!("js@sha256:{}", HEX.to_uppercase()),
            "limits": { "cpu": "10ms", "wall": "1s", "output": "64KiB" },
        }))
        .unwrap()
        .resolve()
        .unwrap();
        let image = pinned.image.unwrap();
        assert_eq!(image.facts.name, "js");
        assert_eq!(image.digest, Some(format!("sha256:{HEX}")));
        assert_eq!(pinned.output, 64 * 1024);

        let module = IsolateTaskConfig::from_value(&json!({
            "module": { "source": "oci://ghcr.io/acme/handler:1.4", "sha256": HEX },
            "limits": { "cpu": "10ms", "wall": "1s" },
        }))
        .unwrap()
        .resolve()
        .unwrap();
        assert_eq!(
            module.module.as_ref().unwrap().sha256,
            format!("sha256:{HEX}")
        );
        assert_eq!(module.default_reservation_mib(), MODULE_RESERVATION_MIB);
    }

    #[test]
    fn isolates_must_be_session_tasks_test() {
        let mut task = isolate_task(base_config());
        task.as_object_mut().unwrap().remove("stateful");
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(
            err.starts_with(
                "[isolate] code-runner/run: runtime \"isolate\" needs stateful.mode = \"session\""
            ),
            "{err}"
        );
    }

    #[test]
    fn memory_max_is_bounded_by_the_cluster_and_the_image_test() {
        let task = with(
            isolate_task(base_config()),
            &["resources", "memoryMax"],
            json!(8192),
        );
        assert_eq!(
            check(vec![task, machine()]).unwrap_err(),
            "[isolate] code-runner/run: resources.memoryMax 8192 MiB is above this cluster's \
             isolate maximum 4096 MiB (worker isolate.maxMemory); run it with tier = \"vm\" for \
             more"
        );
        let mut config = base_config();
        config["image"] = json!("js");
        let task = with(isolate_task(config), &["resources", "memoryMax"], json!(8));
        let task = with(task, &["resources", "memory"], json!(1));
        assert_eq!(
            check(vec![task, machine()]).unwrap_err(),
            "[isolate] code-runner/run: resources.memoryMax 8 MiB is below image \"js\"'s \
             initial linear memory (10 MiB)"
        );
        let task = with(
            isolate_task(base_config()),
            &["resources", "cores"],
            json!(1),
        );
        assert!(check(vec![task, machine()])
            .unwrap_err()
            .contains("resources.cores"));
    }

    #[test]
    fn the_vm_task_must_be_a_listed_sandbox_session_with_a_run_block_test() {
        let task = with(
            isolate_task(base_config()),
            &["children", "tasks"],
            json!(["other"]),
        );
        let other = with(machine(), &["name"], json!("other"));
        assert_eq!(
            check(vec![task, machine(), other]).unwrap_err(),
            "[isolate] code-runner/run: config.vm.task \"machine\" is not in children.tasks; \
             add it there, which is what lets this task's isolates start it"
        );

        let mut no_run = machine();
        no_run.as_object_mut().unwrap().remove("run");
        assert_eq!(
            check(vec![isolate_task(base_config()), no_run]).unwrap_err(),
            "[isolate] code-runner/machine: config.vm.task of code-runner/run names machine, \
             which has no run block; add run.command so tier = \"vm\" runs know what to execute"
        );

        let exec = with(machine(), &["runtime"], json!("exec"));
        let err = check(vec![isolate_task(base_config()), exec]).unwrap_err();
        assert!(
            err.contains("not a session task on the sandbox runtime"),
            "{err}"
        );

        // The default is the only child; with two there is none.
        let mut config = base_config();
        config["vm"] = json!({});
        check(vec![isolate_task(config.clone()), machine()]).unwrap();
        let two = with(
            isolate_task(config),
            &["children", "tasks"],
            json!(["machine", "other"]),
        );
        let other = with(machine(), &["name"], json!("other"));
        let err = check(vec![two, machine(), other]).unwrap_err();
        assert!(err.contains("there is no default"), "{err}");

        // No vm block: no VM tier, and no VM checks.
        let mut config = base_config();
        config.as_object_mut().unwrap().remove("vm");
        check(vec![isolate_task(config), machine()]).unwrap();
    }

    #[test]
    fn run_promote_and_suspend_are_refused_where_they_do_not_apply_test() {
        let task = with(
            isolate_task(base_config()),
            &["run"],
            json!({ "command": ["python3"], "wall": "5s" }),
        );
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(err.contains("takes runs without a run block"), "{err}");

        let task = with(
            isolate_task(base_config()),
            &["stateful", "promote"],
            json!({}),
        );
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(err.contains("never promoted"), "{err}");

        let task = with(
            isolate_task(base_config()),
            &["suspend"],
            json!({ "idle": { "after": "1m" } }),
        );
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(
            err.contains("session isolates only, and config.lifetime is \"request\""),
            "{err}"
        );

        let mut config = base_config();
        config["lifetime"] = json!("session");
        let task = with(
            isolate_task(config.clone()),
            &["suspend"],
            json!({ "idle": { "after": "1m" }, "keepFor": "2h", "durability": "local" }),
        );
        check(vec![task, machine()]).unwrap();
        // `store` is refused at submit, so `job validate` says so.
        let task = with(
            isolate_task(config.clone()),
            &["suspend"],
            json!({ "idle": { "after": "1m" }, "durability": "store" }),
        );
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(
            err.starts_with(
                "[isolate] code-runner/run: suspend.durability = \"store\" is not available for \
                 isolates yet"
            ),
            "{err}"
        );
        assert!(err.contains("Hint: remove suspend.durability"), "{err}");
        for (field, value) in [
            ("checkpoint", json!("memory")),
            ("checkpointEvery", json!("5m")),
            ("onResume", json!(["true"])),
        ] {
            let task = with(
                isolate_task(config.clone()),
                &["suspend"],
                json!({ field: value }),
            );
            let err = check(vec![task, machine()]).unwrap_err();
            assert!(
                err.contains(&format!("suspend.{field} does not apply")),
                "{err}"
            );
        }
    }

    #[test]
    fn network_mode_and_artifacts_test() {
        let task = with(
            isolate_task(base_config()),
            &["network"],
            json!({ "mode": "host" }),
        );
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(err.contains("no network of its own"), "{err}");
        let task = with(
            isolate_task(base_config()),
            &["network"],
            json!({ "mode": "gated", "ports": [{ "name": "http" }] }),
        );
        check(vec![task, machine()]).unwrap();

        let artifact =
            json!([{ "source": "https://example.com/main.py", "name": "code", "dest": "main.py" }]);
        let task = with(
            isolate_task(base_config()),
            &["artifacts"],
            artifact.clone(),
        );
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(err.contains("sets none"), "{err}");

        let mut config = base_config();
        config["entry"] = json!("main.py");
        let err = check(vec![isolate_task(config.clone()), machine()]).unwrap_err();
        assert!(err.contains("declares none"), "{err}");
        let task = with(isolate_task(config), &["artifacts"], artifact.clone());
        check(vec![task, machine()]).unwrap();

        let mut config = base_config();
        config["entry"] = json!("${artifact.missing}");
        let task = with(isolate_task(config), &["artifacts"], artifact.clone());
        let err = check(vec![task, machine()]).unwrap_err();
        assert!(err.contains("artifact \"missing\""), "{err}");
        let mut config = base_config();
        config["entry"] = json!("${artifact.code}");
        let task = with(isolate_task(config), &["artifacts"], artifact);
        check(vec![task, machine()]).unwrap();
    }

    #[test]
    fn helpers_answer_from_the_task_test() {
        let config = job(vec![isolate_task(base_config()), machine()]);
        let task = &config.tasks[0];
        let parsed = IsolateTaskConfig::of(task).unwrap().unwrap();
        assert!(IsolateTaskConfig::of(&config.tasks[1]).unwrap().is_none());
        assert_eq!(cordon_name("team-a/code-runner", &parsed), "team-a");
        assert_eq!(cordon_name("code-runner", &parsed), "default");
        let mut by_job = parsed.clone();
        by_job.cordon = IsolateCordon::Job;
        assert_eq!(
            cordon_name("team-a/code-runner", &by_job),
            "job:team-a/code-runner"
        );
        by_job.cordon = IsolateCordon::Shared;
        assert_eq!(cordon_name("team-a/code-runner", &by_job), "shared");
        assert_eq!(vm_task_name(task, &parsed).as_deref(), Some("machine"));
        assert_eq!(memory_max_mib(task), 128);
        assert_eq!(reservation_mib(task), 16);
        assert!(promote_refusal(task)
            .unwrap()
            .contains("cannot be promoted"));
        assert!(promote_refusal(&config.tasks[1]).is_none());
    }

    #[test]
    fn normalization_fills_the_defaults_test() {
        let mut task = TaskConfig::new("run", ISOLATE_RUNTIME);
        task.config = json!({ "image": "js" });
        normalize_task(&mut task);
        assert_eq!(
            task.resources,
            json!({ "memoryMax": 128, "memory": JS_DEFAULT_RESERVATION_MIB, "cpu": DEFAULT_CPU_MHZ })
        );
        // Idempotent, and explicit values win.
        let before = task.resources.clone();
        normalize_task(&mut task);
        assert_eq!(task.resources, before);

        let mut task = TaskConfig::new("run", ISOLATE_RUNTIME);
        task.config = json!({ "module": { "source": "https://x/a.wasm", "sha256": HEX } });
        task.resources = json!({ "cpus": 50, "memoryMax": 64 });
        normalize_task(&mut task);
        assert_eq!(
            task.resources,
            json!({ "cpus": 50, "memoryMax": 64, "memory": 1 })
        );

        // Never above the ceiling.
        let mut task = TaskConfig::new("run", ISOLATE_RUNTIME);
        task.config = json!({ "image": "python" });
        task.resources = json!({ "memoryMax": 1 });
        normalize_task(&mut task);
        assert_eq!(task.resources["memory"], json!(1));

        let mut other = TaskConfig::new("box", "sandbox");
        normalize_task(&mut other);
        assert_eq!(other.resources, json!({}));
    }

    #[test]
    fn the_run_entry_is_main_for_the_image_else_the_first_file_test() {
        let files = ["data.csv", "/legion/run/main.py", "main.mjs"];
        assert_eq!(
            run_entry(Some("python"), &files),
            Some("/legion/run/main.py")
        );
        assert_eq!(run_entry(Some("js"), &files), Some("main.mjs"));
        assert_eq!(
            run_entry(Some("js"), &["main.js", "main.mjs"]),
            Some("main.mjs")
        );
        assert_eq!(run_entry(Some("python"), &["a.py", "b.py"]), Some("a.py"));
        assert_eq!(
            run_entry(None, &["lib/x.wasm", "main.wasm"]),
            Some("main.wasm")
        );
        assert_eq!(run_entry::<&str>(Some("python"), &[]), None);
        assert_eq!(
            run_entry(Some("python"), &[String::from("main.py")]),
            Some("main.py")
        );
        assert_eq!(vm_entry_name("python"), "main.py");
        assert_eq!(vm_entry_name(&format!("js@sha256:{HEX}")), "main.mjs");
        assert_eq!(vm_entry_name("other"), "main");
    }

    #[test]
    fn tenant_keys_are_short_printable_ascii_test() {
        validate_tenant_key("user-42@acme.com").unwrap();
        assert!(validate_tenant_key("").is_err());
        assert!(validate_tenant_key("a b").is_err());
        assert!(validate_tenant_key(&"x".repeat(129)).is_err());
        validate_tenant_key(&"x".repeat(128)).unwrap();
    }

    #[test]
    fn the_config_round_trips_and_its_schema_documents_every_field_test() {
        let config = IsolateTaskConfig::from_value(&json!({
            "image": "python",
            "entry": "main.py",
            "lifetime": "tenant",
            "limits": { "cpu": "100ms", "wall": "30s", "output": "1MiB" },
            "cordon": "job",
            "vm": { "task": "machine", "memoFor": "1h" },
            "env": { "MODE": "fast" },
        }))
        .unwrap();
        let value = serde_json::to_value(&config).unwrap();
        assert_eq!(IsolateTaskConfig::from_value(&value).unwrap(), config);
        let schema = serde_json::to_value(schemars::schema_for!(IsolateTaskConfig)).unwrap();
        assert_eq!(schema["additionalProperties"], json!(false));
        let required = schema["required"].as_array().unwrap();
        assert_eq!(required, &vec![json!("limits")]);
    }

    #[test]
    fn validate_suspend_names_the_isolate_checkpoint_test() {
        let mut task = job(vec![isolate_task(base_config()), machine()]).tasks[0].clone();
        task.suspend = Some(serde_json::from_value(json!({ "checkpoint": "memory" })).unwrap());
        let err = crate::structs::suspend::validate_suspend(&task).unwrap_err();
        assert!(err.contains("its own checkpoint format"), "{err}");
    }

    #[test]
    fn the_worker_isolate_block_defaults_test() {
        let full = WorkerIsolateConfig::default().with_defaults();
        assert_eq!(full, WorkerIsolateConfigFull::default());
        full.validate().unwrap();
        assert_eq!(full.max_memory_bytes().unwrap(), 4 << 30);
        assert_eq!(full.slots_per_host, DEF_SLOTS_PER_HOST);
        assert_eq!(full.epoch().unwrap(), Duration::from_millis(1));
        assert_eq!(full.slice().unwrap(), Duration::from_millis(5));
        assert_eq!(full.page_out_after().unwrap(), Duration::from_secs(2));
        assert_eq!(full.max_body_bytes().unwrap(), 16 << 20);
        assert_eq!(full.layout, SlotLayout::GuardPages);
        assert_eq!(full.confine, ConfineMode::Enforce);
        assert_eq!(full.cordon_boundary, CordonBoundary::Process);
        assert!(full.resolved_shards() >= 1);

        // The wire form round-trips, and the written form is camelCase with
        // kebab-case values.
        let parsed: WorkerIsolateConfig = serde_json::from_value(json!({
            "binary": "/opt/legion/legion-isolate",
            "maxMemory": "1GiB",
            "slotsPerHost": 2000,
            "shards": 4,
            "layout": "mpk",
            "confine": "best-effort",
            "cordonBoundary": "process",
            "pageOutAfter": "0s",
        }))
        .unwrap();
        let full = parsed.with_defaults();
        full.validate().unwrap();
        assert_eq!(full.layout, SlotLayout::Mpk);
        assert_eq!(full.confine, ConfineMode::BestEffort);
        assert_eq!(full.page_out_after().unwrap(), Duration::ZERO);
        let wire = serde_json::to_string(&full).unwrap();
        assert_eq!(
            serde_json::from_str::<WorkerIsolateConfigFull>(&wire).unwrap(),
            full
        );
        assert!(
            serde_json::from_value::<WorkerIsolateConfig>(json!({ "hostMemroy": "1GiB" })).is_err()
        );
    }

    /// The block rides the artifacts payload to the worker, resolved; a
    /// payload without it (an older manager) parses as "the defaults".
    #[test]
    fn the_worker_isolate_block_rides_the_artifacts_payload_test() {
        let isolate = WorkerIsolateConfig {
            slots_per_host: Some(500),
            ..Default::default()
        }
        .with_defaults();
        let payload = crate::structs::WorkerArtifactsPayload {
            isolate: Some(isolate.clone()),
            ..Default::default()
        };
        let wire = serde_json::to_string(&payload).unwrap();
        let parsed: crate::structs::WorkerArtifactsPayload = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed.isolate, Some(isolate));
        let bare: crate::structs::WorkerArtifactsPayload = serde_json::from_str("{}").unwrap();
        assert_eq!(
            bare.isolate.unwrap_or_default(),
            WorkerIsolateConfigFull::default()
        );
        let alt: crate::structs::WorkerConfigAlt = serde_json::from_value(json!({
            "workerType": "edge",
            "isolate": { "maxMemory": "512MiB" },
        }))
        .unwrap();
        assert_eq!(
            alt.isolate.unwrap().with_defaults().max_memory,
            "512MiB".to_string()
        );
    }

    #[test]
    fn the_worker_isolate_block_refuses_what_a_node_cannot_run_test() {
        let resolved = |block: Value| {
            serde_json::from_value::<WorkerIsolateConfig>(block)
                .unwrap()
                .with_defaults()
                .validate()
                .unwrap_err()
                .to_string()
        };
        for (block, needle) in [
            (json!({ "binary": "legion-isolate" }), "absolute path"),
            (json!({ "images": "images" }), "absolute path"),
            (json!({ "maxMemory": "8GiB" }), "between 1MiB and 4GiB"),
            (json!({ "maxMemory": "lots" }), "invalid isolate.maxMemory"),
            (json!({ "slotsPerHost": 0 }), "between 1 and 16000"),
            (
                json!({ "slotsPerHost": 20000 }),
                "with layout \"guard-pages\"",
            ),
            (json!({ "epoch": "0s" }), "epoch must be greater than zero"),
            (
                json!({ "epoch": "10ms", "slice": "5ms" }),
                "shorter than isolate.epoch",
            ),
            (json!({ "slice": "soon" }), "isolate.slice"),
            (json!({ "pageOutAfter": "x" }), "isolate.pageOutAfter"),
            (
                json!({ "maxBodyBytes": "0" }),
                "maxBodyBytes must be greater",
            ),
            // Parsed (the enum keeps the value) but not built: refused,
            // never run as `process`.
            (
                json!({ "cordonBoundary": "vm" }),
                "isolate.cordonBoundary = \"vm\" is not available",
            ),
        ] {
            let err = resolved(block.clone());
            assert!(err.contains(needle), "{block}: {needle}: {err}");
        }
        // A denser layout fits more slots.
        let mut small = WorkerIsolateConfigFull {
            layout: SlotLayout::Small,
            slots_per_host: 100_000,
            ..Default::default()
        };
        small.validate().unwrap();
        small.slots_per_host = 2_000_000;
        assert!(small.validate().is_err());
    }
}
