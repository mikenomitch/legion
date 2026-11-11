//! Getting a task's component compiled for this node's engine (plan §6.5):
//! fetch the bytes (a `config.module` through the artifact stager into the
//! content store, or a built-in image from the images directory), then
//! compile them once per node with a `legion-isolate compile` child
//! process into derived content (`DerivedKind::Cwasm`, a confined recipe
//! keyed by the component's digest and the engine's compatibility key).

use super::binary::{ImageCatalog, IsolateBinary};
use crate::content_store::{ContentDigest, ContentStore, DerivedKind, Recipe};
use crate::prelude::*;
use crate::structs::isolate::{IsolateModule, WorkerIsolateConfigFull};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long one compile may take.
pub const COMPILE_TIMEOUT: Duration = Duration::from_secs(300);

/// The compile process's address-space limit. Generous: Cranelift on a
/// large interpreter image needs a few GiB of virtual memory.
pub const COMPILE_ADDRESS_SPACE: u64 = 16 << 30;

/// The artifact file inside a derived entry.
pub const ARTIFACT_FILE: &str = "module.cwasm";

/// What a task runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModuleWant {
    /// `config.module`.
    Component(IsolateModule),
    /// `config.image`, as written (possibly pinned).
    Image(String),
}

/// A compiled module a host can load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleArtifact {
    /// The derived-content key: the module's id in every host.
    pub module_id: String,
    /// The `.cwasm` file, inside one of the hosts' `read_dirs`.
    pub path: PathBuf,
}

/// Fetches and compiles modules. Blocking; called on a blocking thread.
pub trait ModuleSource: Send + Sync + 'static {
    fn compiled(&self, want: &ModuleWant) -> Result<ModuleArtifact, String>;

    /// `config.entry`'s file: its name in the run directory and its bytes,
    /// staged from the task's artifacts (no allocation exists for an
    /// isolate task to stage them into).
    fn entry_file(
        &self,
        job_id: &str,
        task: &crate::structs::TaskConfig,
        allocation_id: i64,
        entry: &str,
    ) -> Result<(String, Vec<u8>), String>;
}

/// The real source: the artifact stager, the images directory, the node
/// content store, and the `legion-isolate` binary.
pub struct Compiler {
    pub binary: IsolateBinary,
    pub config: WorkerIsolateConfigFull,
    pub stager: Option<Arc<crate::artifacts::stage::ArtifactStager>>,
    pub store: Option<Arc<ContentStore>>,
    pub images: Option<ImageCatalog>,
    /// Where artifacts go when the content store cannot keep them.
    pub fallback_dir: PathBuf,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompileReport {
    ok: bool,
    #[serde(default)]
    error: String,
    #[serde(default)]
    kind: String,
}

impl Compiler {
    /// The component's bytes on disk and their digest.
    fn component(&self, want: &ModuleWant) -> Result<(PathBuf, ContentDigest), String> {
        match want {
            ModuleWant::Component(module) => {
                let stager = self
                    .stager
                    .as_ref()
                    .ok_or("this node has no artifact stager to fetch the module with")?;
                let mut spec = crate::structs::artifact::ArtifactSpec::from_source(&module.source);
                spec.checksum = Some(module.checksum());
                let fetched = stager
                    .fetch_into_store(
                        &spec,
                        crate::content_store::ContentClass::Artifacts,
                        crate::content_store::PutOptions::default().label("isolate module"),
                    )
                    .map_err(|err| format!("fetching config.module.source: {err:#}"))?;
                Ok((fetched.entry.path, fetched.entry.digest))
            }
            ModuleWant::Image(image) => {
                let catalog = self.images.as_ref().ok_or_else(|| {
                    "this node has no isolate images (set isolate.images, or install \
                     isolate-images beside legion-isolate)"
                        .to_string()
                })?;
                let (entry, path) = catalog.resolve(image)?;
                let digest = ContentDigest::of_file(&path)
                    .map_err(|err| format!("reading image {}: {err:#}", path.display()))?;
                if !entry.sha256.is_empty() {
                    let declared = ContentDigest::parse(&if entry.sha256.starts_with("sha256:") {
                        entry.sha256.clone()
                    } else {
                        format!("sha256:{}", entry.sha256)
                    })
                    .map_err(|err| format!("the image manifest's sha256 is invalid: {err:#}"))?;
                    if declared != digest {
                        return Err(format!(
                            "image file {} does not match its manifest sha256 ({declared}); the \
                             images directory is damaged",
                            path.display()
                        ));
                    }
                }
                Ok((path, digest))
            }
        }
    }

    fn recipe(&self, digest: &ContentDigest) -> Recipe {
        Recipe::new(DerivedKind::Cwasm, self.binary.engine_version.clone())
            .digest("component", digest)
            .fingerprint("compat", self.binary.compat_key.as_bytes())
            .confined()
    }

    /// Run `legion-isolate compile` with a timeout and an address-space
    /// limit, and read its report.
    fn compile(&self, input: &Path, output: &Path) -> Result<(), String> {
        use std::os::unix::process::CommandExt;
        let started = Instant::now();
        let mut command = std::process::Command::new(&self.binary.path);
        command
            .arg("compile")
            .arg("--input")
            .arg(input)
            .arg("--output")
            .arg(output)
            .args(super::binary::engine_args(&self.config))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .env_clear();
        // SAFETY: setrlimit is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                let limit = libc::rlimit {
                    rlim_cur: COMPILE_ADDRESS_SPACE as libc::rlim_t,
                    rlim_max: COMPILE_ADDRESS_SPACE as libc::rlim_t,
                };
                if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .map_err(|err| format!("could not start legion-isolate compile: {err}"))?;
        // Drain stdout/stderr on threads so a chatty compiler never blocks.
        let mut stdout = child.stdout.take().expect("piped");
        let mut stderr = child.stderr.take().expect("piped");
        let out = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = std::io::Read::read_to_end(&mut stdout, &mut buf);
            buf
        });
        let err = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = std::io::Read::read_to_end(&mut stderr, &mut buf);
            buf
        });
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if started.elapsed() > COMPILE_TIMEOUT => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "compiling the module took longer than {}s",
                        COMPILE_TIMEOUT.as_secs()
                    ));
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(err) => return Err(format!("waiting for the compiler: {err}")),
            }
        };
        let stdout = out.join().unwrap_or_default();
        let stderr = err.join().unwrap_or_default();
        let seconds = started.elapsed().as_secs_f64();
        crate::telemetry::worker_metrics()
            .isolate_compile_seconds
            .observe(seconds);
        if let Some(store) = self.store.as_ref() {
            store.derived_observe_build(DerivedKind::Cwasm, seconds);
        }
        let line = String::from_utf8_lossy(&stdout);
        let report: Option<CompileReport> = line
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str(line.trim()).ok());
        match report {
            Some(report) if report.ok && status.success() => Ok(()),
            Some(report) => Err(if report.kind.is_empty() {
                format!("the module failed to compile: {}", report.error)
            } else {
                format!(
                    "the module failed to compile ({}): {}",
                    report.kind, report.error
                )
            }),
            None => Err(format!(
                "legion-isolate compile {status} without a report: {}",
                String::from_utf8_lossy(&stderr).trim()
            )),
        }
    }

    fn compile_to_fallback(&self, input: &Path, key: &str) -> Result<ModuleArtifact, String> {
        let dir = self.fallback_dir.join(key);
        let path = dir.join(ARTIFACT_FILE);
        if !path.is_file() {
            std::fs::create_dir_all(&dir)
                .map_err(|err| format!("creating {}: {err}", dir.display()))?;
            self.compile(input, &path)?;
        }
        Ok(ModuleArtifact {
            module_id: key.to_string(),
            path,
        })
    }
}

impl ModuleSource for Compiler {
    fn entry_file(
        &self,
        job_id: &str,
        task: &crate::structs::TaskConfig,
        allocation_id: i64,
        entry: &str,
    ) -> Result<(String, Vec<u8>), String> {
        self.stage_entry(job_id, task, allocation_id, entry)
    }

    fn compiled(&self, want: &ModuleWant) -> Result<ModuleArtifact, String> {
        let (input, digest) = self.component(want)?;
        let recipe = self.recipe(&digest);
        let key = recipe.key();
        let Some(store) = self.store.as_ref() else {
            return self.compile_to_fallback(&input, &key);
        };
        loop {
            if let Ok(Some(entry)) = store.derived_get(&recipe) {
                return Ok(ModuleArtifact {
                    module_id: key,
                    path: entry.path.join(ARTIFACT_FILE),
                });
            }
            // Single-flight: one compile per key on the node; the rest wait
            // and look again.
            let Some(flight) = store.derived_claim(DerivedKind::Cwasm, &key) else {
                continue;
            };
            let stage = store
                .derived_stage()
                .map_err(|err| format!("staging the compiled module: {err:#}"))?;
            if let Err(err) = self.compile(&input, &stage.join(ARTIFACT_FILE)) {
                let _ = std::fs::remove_dir_all(&stage);
                return Err(err);
            }
            let committed = store.derived_commit(&recipe, &stage);
            drop(flight);
            return match committed {
                Ok(entry) => Ok(ModuleArtifact {
                    module_id: key,
                    path: entry.path.join(ARTIFACT_FILE),
                }),
                Err(crate::content_store::DerivedSkip::AlreadyPresent) => continue,
                Err(skip) => {
                    debug!(reason = skip.reason(), "Compiled module not kept as derived content; using the worker's own directory");
                    self.compile_to_fallback(&input, &key)
                }
            };
        }
    }
}

impl Compiler {
    fn stage_entry(
        &self,
        job_id: &str,
        task: &crate::structs::TaskConfig,
        allocation_id: i64,
        entry: &str,
    ) -> Result<(String, Vec<u8>), String> {
        let stager = self
            .stager
            .as_ref()
            .ok_or("this node has no artifact stager to fetch config.entry with")?;
        let dir = self
            .fallback_dir
            .parent()
            .unwrap_or(&self.fallback_dir)
            .join("entries")
            .join(format!(
                "{}-{}",
                crate::structs::session::derived_session_id(&task.name, job_id, "entry"),
                allocation_id
            ));
        std::fs::create_dir_all(&dir)
            .map_err(|err| format!("creating {}: {err}", dir.display()))?;
        let board = crate::artifacts::status::ArtifactStatusBoard::new();
        let staged = stager
            .stage_task_artifacts(allocation_id, &dir, &task.artifacts, &board)
            .map_err(|err| format!("staging the task's artifacts for config.entry: {err:#}"))?;
        let path = match crate::structs::isolate::entry_artifact(entry) {
            Some(name) => staged
                .iter()
                .find(|artifact| artifact.name.as_deref() == Some(name))
                .map(|artifact| artifact.reference_path().to_path_buf())
                .ok_or_else(|| {
                    format!("config.entry names artifact {name:?}, which the task does not declare")
                })?,
            None => dir.join(entry.trim()),
        };
        let content = std::fs::read(&path)
            .map_err(|err| format!("reading config.entry {}: {err}", path.display()))?;
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "main".into());
        Ok((name, content))
    }
}

/// The `cwasm` derived-content directory, which every host may read.
pub fn derived_dir(store: &ContentStore) -> PathBuf {
    store
        .derived_path(DerivedKind::Cwasm, &"0".repeat(64))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default()
}
