//! The staging pipeline (artifact system plan §4): fetch into a hashing sink
//! under `.staging/`, verify checksums, unpack safely, and move atomically
//! into the task directory. Corrupt bytes never reach a task directory.

use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Condvar;
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::artifacts::fetcher::{
    Credential, FetchLimits, FetchRequest, FetchedArtifact, FetchedPayload, FetcherRegistry,
};
use crate::artifacts::status::{ArtifactState, ArtifactStatusBoard};
use crate::artifacts::{StagedArtifact, StagedPayload};
use crate::prelude::*;
use crate::structs::{
    parse_artifact_source, ArtifactSpec, ArtifactUnpack, WorkerArtifactFetchConfigFull,
};

/// Resolves auth references into credentials at fetch time. The worker
/// implementation reads KV through the manager channel; tests use
/// [`AnonymousCredentials`]. Identity-token methods surface the workload
/// identity plan dependency: until issuance/delivery (WI plan phases 2 and 4)
/// are wired, they return an actionable error.
pub trait CredentialSource: Send + Sync {
    /// Reads a KV credential value. `redacted_source` is audit context only.
    fn kv_credential(&self, kv_path: &str, redacted_source: &str) -> R<Credential>;

    /// A task workload-identity token for bearer presentation / STS exchange.
    fn identity_token(&self, identity: &str) -> R<String> {
        bail!(
            "artifact auth identity {identity:?} requires workload-identity token issuance, \
             which is not wired yet (workload-identity plan phases 2 and 4); use auth.kv or \
             anonymous access until then"
        )
    }

    /// A node workload-identity token (cluster-grain credential).
    fn node_identity_token(&self, name: &str) -> R<String> {
        bail!(
            "artifact auth nodeIdentity {name:?} requires workload-identity token issuance, \
             which is not wired yet (workload-identity plan phases 2 and 4); use auth.kv or \
             anonymous access until then"
        )
    }
}

/// Credential source that only serves anonymous fetches (CLI jobspec
/// fetching, tests).
pub struct AnonymousCredentials;

impl CredentialSource for AnonymousCredentials {
    fn kv_credential(&self, kv_path: &str, _redacted_source: &str) -> R<Credential> {
        bail!("no credential source available to read kv {kv_path}")
    }
}

/// Counting semaphore bounding concurrent fetches per worker (the plan's
/// `artifacts.fetch.parallelism`). Plain std primitives: staging runs on
/// blocking threads, never the async runtime.
pub struct FetchSemaphore {
    permits: Mutex<usize>,
    available: Condvar,
}

impl FetchSemaphore {
    pub fn new(permits: usize) -> Self {
        FetchSemaphore {
            permits: Mutex::new(permits.max(1)),
            available: Condvar::new(),
        }
    }

    pub fn acquire(&self) -> FetchPermit<'_> {
        let mut permits = self.permits.lock().expect("fetch semaphore");
        while *permits == 0 {
            permits = self.available.wait(permits).expect("fetch semaphore");
        }
        *permits -= 1;
        FetchPermit { semaphore: self }
    }
}

pub struct FetchPermit<'a> {
    semaphore: &'a FetchSemaphore,
}

impl Drop for FetchPermit<'_> {
    fn drop(&mut self) {
        let mut permits = self.semaphore.permits.lock().expect("fetch semaphore");
        *permits += 1;
        self.semaphore.available.notify_one();
    }
}

/// Progress callback fed by sink writers: (bytes_fetched, bytes_total).
pub type ProgressFn = Box<dyn Fn(u64, Option<u64>) + Send>;

/// Where fetchers write. In this plan it is a hashing writer into a
/// per-allocation `.staging` temp directory; the cache plan substitutes a
/// content-addressed-store writer without fetchers noticing.
pub struct ArtifactSink {
    staging_dir: PathBuf,
    max_bytes: u64,
    bytes_written: u64,
    progress: Option<ProgressFn>,
    bytes_total_hint: Option<u64>,
}

impl ArtifactSink {
    pub fn new(staging_dir: PathBuf, max_bytes: u64) -> R<Self> {
        fs::create_dir_all(&staging_dir)
            .with_context(|| format!("create artifact staging dir {}", staging_dir.display()))?;
        Ok(ArtifactSink {
            staging_dir,
            max_bytes,
            bytes_written: 0,
            progress: None,
            bytes_total_hint: None,
        })
    }

    pub fn set_progress(&mut self, progress: ProgressFn) {
        self.progress = Some(progress);
    }

    /// Hint the expected total size (HTTP Content-Length, object meta) so
    /// progress reporting can show a percentage.
    pub fn set_bytes_total(&mut self, total: Option<u64>) {
        self.bytes_total_hint = total;
        if let (Some(progress), Some(total)) = (self.progress.as_ref(), total) {
            progress(self.bytes_written, Some(total));
        }
    }

    /// Directory for fetchers that materialize trees directly (git clone,
    /// OCI layouts).
    pub fn dir(&self) -> &Path {
        &self.staging_dir
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Counts bytes produced outside a [`SinkWriter`] (tree fetchers) against
    /// the size budget.
    pub fn record_external_bytes(&mut self, bytes: u64) -> R<()> {
        self.bytes_written = self.bytes_written.saturating_add(bytes);
        if self.bytes_written > self.max_bytes {
            bail!(
                "artifact exceeds maxArtifactSize ({} bytes)",
                self.max_bytes
            );
        }
        if let Some(progress) = self.progress.as_ref() {
            progress(self.bytes_written, self.bytes_total_hint);
        }
        Ok(())
    }

    /// Creates a hashing writer for a single staged file.
    pub fn file_writer(&mut self, file_name: &str) -> R<SinkWriter<'_>> {
        let safe_name = Path::new(file_name)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "artifact".to_string());
        let path = self.staging_dir.join(safe_name);
        let file = fs::File::create(&path)
            .with_context(|| format!("create staging file {}", path.display()))?;
        Ok(SinkWriter {
            sink: self,
            path,
            file,
            hasher: Sha256::new(),
            bytes: 0,
        })
    }
}

/// `io::Write` adapter that hashes, counts, enforces the size cap, and
/// reports progress while streaming.
pub struct SinkWriter<'a> {
    sink: &'a mut ArtifactSink,
    path: PathBuf,
    file: fs::File,
    hasher: Sha256,
    bytes: u64,
}

impl SinkWriter<'_> {
    /// Finalizes the staged file, returning `(path, sha256 digest, bytes)`.
    pub fn finish(self) -> R<(PathBuf, String, u64)> {
        self.file.sync_all().ok();
        let digest = format!("sha256:{:x}", self.hasher.finalize());
        Ok((self.path, digest, self.bytes))
    }
}

impl Write for SinkWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes += written as u64;
        self.sink.bytes_written = self.sink.bytes_written.saturating_add(written as u64);
        if self.sink.bytes_written > self.sink.max_bytes {
            return Err(std::io::Error::other(format!(
                "artifact exceeds maxArtifactSize ({} bytes)",
                self.sink.max_bytes
            )));
        }
        if let Some(progress) = self.sink.progress.as_ref() {
            progress(self.sink.bytes_written, self.sink.bytes_total_hint);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Archive extensions `unpack: auto` recognizes.
pub fn looks_like_archive(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".tar.gz")
        || lower.ends_with(".tgz")
        || lower.ends_with(".tar")
        || lower.ends_with(".gz")
}

/// Validates that an archive entry path stays inside the extraction root.
fn safe_entry_path(root: &Path, entry_path: &Path) -> R<PathBuf> {
    let mut depth: i64 = 0;
    for component in entry_path.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    bail!(
                        "archive entry {} escapes the extraction root",
                        entry_path.display()
                    );
                }
            }
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => {
                bail!(
                    "archive entry {} has an absolute path",
                    entry_path.display()
                );
            }
        }
    }
    Ok(root.join(entry_path))
}

/// Validates a symlink target: relative targets may not escape the extraction
/// root from the link's location; absolute targets are rejected outright.
fn safe_link_target(root: &Path, link_path: &Path, target: &Path) -> R<()> {
    if target.is_absolute() {
        bail!(
            "archive symlink {} targets absolute path {}",
            link_path.display(),
            target.display()
        );
    }
    let link_dir_depth = link_path.components().count().saturating_sub(1) as i64;
    let mut depth = link_dir_depth;
    for component in target.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    bail!(
                        "archive symlink {} escapes the extraction root (target {})",
                        link_path.display(),
                        target.display()
                    );
                }
            }
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => {
                bail!(
                    "archive symlink {} has invalid target {}",
                    link_path.display(),
                    target.display()
                );
            }
        }
    }
    let _ = root;
    Ok(())
}

/// Extracts a `.tar[.gz]`/`.tgz` archive (or gunzips a bare `.gz`) into
/// `dest_dir` with path-traversal and symlink-escape protection plus a
/// decompressed-size budget.
pub fn unpack_archive(archive: &Path, dest_dir: &Path, size_budget: u64) -> R<()> {
    let name = archive
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
        .to_ascii_lowercase();
    fs::create_dir_all(dest_dir)
        .with_context(|| format!("create unpack dir {}", dest_dir.display()))?;

    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        let file = fs::File::open(archive)?;
        let decoder = flate2::read::GzDecoder::new(file);
        return unpack_tar(decoder, dest_dir, size_budget);
    }
    if name.ends_with(".tar") {
        let file = fs::File::open(archive)?;
        return unpack_tar(file, dest_dir, size_budget);
    }
    if name.ends_with(".gz") {
        let file = fs::File::open(archive)?;
        let mut decoder = flate2::read::GzDecoder::new(file).take(size_budget.saturating_add(1));
        let target = dest_dir.join(name.trim_end_matches(".gz"));
        let mut out =
            fs::File::create(&target).with_context(|| format!("create {}", target.display()))?;
        let copied = std::io::copy(&mut decoder, &mut out)?;
        if copied > size_budget {
            let _ = fs::remove_file(&target);
            bail!("decompressed artifact exceeds maxArtifactSize ({size_budget} bytes)");
        }
        return Ok(());
    }
    bail!(
        "artifact {} is not a recognized archive (.tar.gz, .tgz, .tar, .gz); \
         set unpack: \"file\" to stage it as-is",
        archive.display()
    )
}

fn unpack_tar<RD: Read>(reader: RD, dest_dir: &Path, size_budget: u64) -> R<()> {
    let mut archive = tar::Archive::new(reader);
    let mut extracted: u64 = 0;
    for entry in archive.entries().context("read tar entries")? {
        let mut entry = entry.context("read tar entry")?;
        let entry_path = entry.path().context("tar entry path")?.into_owned();
        let target = safe_entry_path(dest_dir, &entry_path)?;

        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            let link_target = entry
                .link_name()
                .context("tar link name")?
                .with_context(|| format!("tar link entry {} has no target", entry_path.display()))?
                .into_owned();
            safe_link_target(dest_dir, &entry_path, &link_target)?;
        }

        extracted = extracted.saturating_add(entry.size());
        if extracted > size_budget {
            bail!("decompressed artifact exceeds maxArtifactSize ({size_budget} bytes)");
        }

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        entry
            .unpack(&target)
            .with_context(|| format!("unpack tar entry {}", entry_path.display()))?;
    }
    Ok(())
}

/// The worker's staging engine: one per task manager, shared by every
/// staging thread.
pub struct ArtifactStager {
    registry: Mutex<FetcherRegistry>,
    config: WorkerArtifactFetchConfigFull,
    semaphore: FetchSemaphore,
    credentials: Box<dyn CredentialSource>,
}

impl ArtifactStager {
    pub fn new(
        config: WorkerArtifactFetchConfigFull,
        credentials: Box<dyn CredentialSource>,
    ) -> Self {
        let parallelism = config.parallelism;
        ArtifactStager {
            registry: Mutex::new(FetcherRegistry::with_builtins()),
            config,
            semaphore: FetchSemaphore::new(parallelism),
            credentials,
        }
    }

    pub fn config(&self) -> &WorkerArtifactFetchConfigFull {
        &self.config
    }

    /// Stages every artifact of one task into `task_dir`, reporting progress
    /// through `board`. All-or-nothing: the first artifact that fails (after
    /// per-artifact retries) fails the whole staging, which the caller turns
    /// into an allocation failure through the existing `record_failure` path.
    ///
    /// When the task directory holds a [`StagedManifest`] from a previous
    /// fully-successful run of the *same* artifact list (a worker restart or
    /// an in-place re-dispatch), the staged content is reused instead of
    /// refetched. Artifacts with an explicit `resolve: "always"` opt out of
    /// reuse — that is the "re-resolve at every staging" contract.
    #[instrument(skip_all, fields(allocation_id, task_dir = %task_dir.display()))]
    pub fn stage_task_artifacts(
        &self,
        allocation_id: i64,
        task_dir: &Path,
        artifacts: &[ArtifactSpec],
        board: &ArtifactStatusBoard,
    ) -> R<Vec<StagedArtifact>> {
        let fingerprint = spec_fingerprint(artifacts)?;
        if reuse_allowed(artifacts) {
            if let Some(staged) = try_reuse_staged(task_dir, &fingerprint, artifacts) {
                for (index, artifact) in staged.iter().enumerate() {
                    board.update(allocation_id, index, |row| {
                        row.state = ArtifactState::Staged;
                        row.digest = artifact.digest.clone();
                        row.bytes_fetched = Some(artifact.bytes);
                        row.bytes_total = Some(artifact.bytes);
                        row.error = None;
                    });
                }
                info!(
                    allocation_id,
                    artifacts = staged.len(),
                    "Reusing previously staged artifacts"
                );
                return Ok(staged);
            }
        }

        // Fresh staging starts from a clean slate: leftovers from an
        // interrupted or non-matching previous run would collide with the
        // staged content.
        clear_dir_contents(task_dir)?;

        let staging_root = task_dir.join(".staging");
        let mut staged = Vec::with_capacity(artifacts.len());
        let result = (|| {
            for (index, spec) in artifacts.iter().enumerate() {
                let outcome = self
                    .stage_one(allocation_id, task_dir, &staging_root, index, spec, board)
                    .with_context(|| {
                        format!("artifact {} failed to stage", spec.redacted_source())
                    });
                match outcome {
                    Ok(artifact) => staged.push(artifact),
                    Err(err) => {
                        board.update(allocation_id, index, |row| {
                            row.state = ArtifactState::Failed;
                            row.error = Some(format!("{err:#}"));
                        });
                        return Err(err);
                    }
                }
            }
            Ok(())
        })();
        // Always clear the temp area; staged content has been moved out.
        let _ = fs::remove_dir_all(&staging_root);
        let staged = result.map(|()| staged)?;
        // Record the successful run so a restart can reuse it. Best-effort:
        // a missing manifest only costs a refetch.
        if let Err(err) = write_staged_manifest(task_dir, &fingerprint, &staged) {
            warn!(
                allocation_id,
                error = ?err,
                "Failed to write staged-artifact manifest; restart will refetch"
            );
        }
        Ok(staged)
    }

    fn stage_one(
        &self,
        allocation_id: i64,
        task_dir: &Path,
        staging_root: &Path,
        index: usize,
        spec: &ArtifactSpec,
        board: &ArtifactStatusBoard,
    ) -> R<StagedArtifact> {
        let parsed = parse_artifact_source(&spec.source)?;
        let redacted = spec.redacted_source();

        // Defense in depth: admission already enforced the cluster
        // allowlist; a worker also refuses schemes its config disallows.
        if !self
            .config
            .allowed_schemes
            .iter()
            .any(|allowed| allowed == &parsed.scheme)
        {
            bail!(
                "scheme {} is not allowed by this worker's artifacts.fetch.allowedSchemes",
                parsed.scheme
            );
        }

        let fetcher = self
            .registry
            .lock()
            .expect("fetcher registry")
            .fetcher(&parsed.scheme)?;
        let credential = self.resolve_credential(spec, &redacted)?;

        let started = Instant::now();
        let deadline = Instant::now() + std::time::Duration::from_secs(self.config.timeout_secs);
        let limits = FetchLimits {
            max_bytes: self.config.max_artifact_size_bytes,
            deadline: Some(deadline),
        };

        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=self.config.attempts.max(1) {
            if attempt > 1 {
                std::thread::sleep(std::time::Duration::from_millis(500 * u64::from(attempt)));
            }
            if let Err(err) = limits.check_deadline(&redacted) {
                last_err =
                    Some(last_err.map_or(err, |prior| prior.context("fetch deadline exceeded")));
                break;
            }
            board.update(allocation_id, index, |row| {
                row.state = ArtifactState::Fetching;
                row.attempts = Some(attempt);
                row.error = None;
            });

            let attempt_dir = staging_root.join(format!("artifact-{index}-attempt-{attempt}"));
            let outcome = self.fetch_verify_place(
                &fetcher,
                spec,
                &parsed,
                &redacted,
                credential.as_ref(),
                &limits,
                &attempt_dir,
                task_dir,
                allocation_id,
                index,
                board,
            );
            let _ = fs::remove_dir_all(&attempt_dir);
            match outcome {
                Ok(staged) => {
                    board.update(allocation_id, index, |row| {
                        row.state = ArtifactState::Staged;
                        row.digest = staged.digest.clone();
                        row.duration_ms = Some(started.elapsed().as_millis() as u64);
                        row.error = None;
                    });
                    info!(
                        allocation_id,
                        source = %redacted,
                        bytes = staged.bytes,
                        duration_ms = started.elapsed().as_millis() as u64,
                        "Staged artifact"
                    );
                    return Ok(staged);
                }
                Err(err) => {
                    warn!(
                        allocation_id,
                        source = %redacted,
                        attempt,
                        error = %format!("{err:#}"),
                        "Artifact fetch attempt failed"
                    );
                    last_err = Some(err);
                }
            }
        }

        Err(last_err
            .unwrap_or_else(|| anyhow!("artifact fetch failed before any attempt ran"))
            .context(format!(
                "after {} attempt(s) ({})",
                self.config.attempts,
                spec.auth_description()
            )))
    }

    /// One full attempt: fetch into the sink, verify the checksum, unpack if
    /// requested, and move the result into the task directory.
    #[allow(clippy::too_many_arguments)]
    fn fetch_verify_place(
        &self,
        fetcher: &Arc<dyn crate::artifacts::fetcher::ArtifactFetcher>,
        spec: &ArtifactSpec,
        parsed: &crate::structs::ParsedSource,
        redacted: &str,
        credential: Option<&Credential>,
        limits: &FetchLimits,
        attempt_dir: &Path,
        task_dir: &Path,
        allocation_id: i64,
        index: usize,
        board: &ArtifactStatusBoard,
    ) -> R<StagedArtifact> {
        let mut sink = ArtifactSink::new(attempt_dir.to_path_buf(), limits.max_bytes)?;
        let progress_board = board.clone();
        let last_reported = std::sync::atomic::AtomicU64::new(0);
        sink.set_progress(Box::new(move |bytes_fetched, bytes_total| {
            // Throttle board updates to ~1MiB granularity to keep lock
            // traffic negligible on fast streams.
            let last = last_reported.load(std::sync::atomic::Ordering::Relaxed);
            if bytes_fetched.saturating_sub(last) < 1024 * 1024
                && Some(bytes_fetched) != bytes_total
            {
                return;
            }
            last_reported.store(bytes_fetched, std::sync::atomic::Ordering::Relaxed);
            progress_board.update(allocation_id, index, |row| {
                row.bytes_fetched = Some(bytes_fetched);
                row.bytes_total = bytes_total;
            });
        }));

        let fetched = {
            let _permit = self.semaphore.acquire();
            let request = FetchRequest {
                scheme: &parsed.scheme,
                url: &parsed.url,
                redacted_source: redacted,
                checksum: spec.checksum.as_deref(),
                media_type: spec.media_type.as_deref(),
                credential,
                limits,
            };
            fetcher.fetch(&request, &mut sink)?
        };

        board.update(allocation_id, index, |row| {
            row.state = ArtifactState::Verifying;
        });
        verify_checksum(spec, &fetched)?;

        place_fetched_artifact(spec, fetched, task_dir, attempt_dir, limits.max_bytes)
    }

    fn resolve_credential(&self, spec: &ArtifactSpec, redacted: &str) -> R<Option<Credential>> {
        let Some(auth) = spec.auth.as_ref() else {
            return Ok(None);
        };
        if let Some(kv_path) = auth.kv.as_deref() {
            let credential = self
                .credentials
                .kv_credential(kv_path, redacted)
                .with_context(|| format!("credentials from kv {kv_path}"))?;
            return Ok(Some(credential));
        }
        if let Some(identity) = auth.identity.as_deref() {
            let token = self
                .credentials
                .identity_token(identity)
                .with_context(|| format!("credentials from identity {identity}"))?;
            return Ok(Some(Credential::Bearer { token }));
        }
        if let Some(node_identity) = auth.node_identity.as_deref() {
            let token = self
                .credentials
                .node_identity_token(node_identity)
                .with_context(|| format!("credentials from nodeIdentity {node_identity}"))?;
            return Ok(Some(Credential::Bearer { token }));
        }
        Ok(None)
    }
}

/// Verifies the declared checksum against the streaming digest. Only
/// single-file payloads carry a byte-level digest; declaring a checksum on a
/// tree-producing source is a staging error rather than silent skip.
fn verify_checksum(spec: &ArtifactSpec, fetched: &FetchedArtifact) -> R<()> {
    let Some(expected) = spec.checksum.as_deref() else {
        return Ok(());
    };
    match &fetched.payload {
        FetchedPayload::File { digest, .. } => {
            if digest != expected {
                bail!(
                    "checksum mismatch: expected {expected}, fetched content is {digest} \
                     (corrupt bytes were discarded)"
                );
            }
            Ok(())
        }
        FetchedPayload::Image { digest, .. } | FetchedPayload::Layers { digest, .. } => {
            if digest != expected {
                bail!("checksum mismatch: expected {expected}, manifest digest is {digest}");
            }
            Ok(())
        }
        FetchedPayload::Tree { .. } => bail!(
            "checksum verification is not supported for tree sources (git/file directories); \
             remove the checksum or fetch an archive"
        ),
    }
}

/// Moves a fetched payload from the attempt's staging dir into its final
/// place under the task directory, unpacking archives when requested.
fn place_fetched_artifact(
    spec: &ArtifactSpec,
    fetched: FetchedArtifact,
    task_dir: &Path,
    attempt_dir: &Path,
    size_budget: u64,
) -> R<StagedArtifact> {
    let bytes = fetched.bytes;
    match fetched.payload {
        FetchedPayload::File { path, digest } => {
            let file_name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "artifact".to_string());
            let unpack = match spec.unpack {
                ArtifactUnpack::Auto => looks_like_archive(&file_name),
                ArtifactUnpack::Archive => true,
                ArtifactUnpack::File => false,
            };
            if unpack {
                let unpack_dir = attempt_dir.join(".unpacked");
                unpack_archive(&path, &unpack_dir, size_budget)?;
                let dest_dir = resolve_dest_dir(task_dir, spec.dest.as_deref())?;
                merge_move(&unpack_dir, &dest_dir)?;
                Ok(StagedArtifact {
                    name: spec.name.clone(),
                    digest: Some(digest),
                    bytes,
                    payload: StagedPayload::Tree { dir: dest_dir },
                })
            } else {
                let dest_path = resolve_dest_file(task_dir, spec.dest.as_deref(), &file_name)?;
                if let Some(parent) = dest_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                if dest_path.exists() {
                    bail!(
                        "artifact destination {} already exists",
                        dest_path.display()
                    );
                }
                fs::rename(&path, &dest_path).with_context(|| {
                    format!("move staged artifact into {}", dest_path.display())
                })?;
                apply_mode(&dest_path, spec.mode.as_deref())?;
                Ok(StagedArtifact {
                    name: spec.name.clone(),
                    digest: Some(digest),
                    bytes,
                    payload: StagedPayload::File { path: dest_path },
                })
            }
        }
        FetchedPayload::Tree { dir } => {
            let dest_dir = resolve_dest_dir(task_dir, spec.dest.as_deref())?;
            merge_move(&dir, &dest_dir)?;
            Ok(StagedArtifact {
                name: spec.name.clone(),
                digest: None,
                bytes,
                payload: StagedPayload::Tree { dir: dest_dir },
            })
        }
        FetchedPayload::Image {
            oci_layout,
            reference,
            digest,
        } => {
            let default_dest = format!(".oci/{}", spec.name.as_deref().unwrap_or("image"));
            let dest_dir = resolve_dest_dir(
                task_dir,
                Some(spec.dest.as_deref().unwrap_or(&default_dest)),
            )?;
            merge_move(&oci_layout, &dest_dir)?;
            Ok(StagedArtifact {
                name: spec.name.clone(),
                digest: Some(digest),
                bytes,
                payload: StagedPayload::Image {
                    oci_layout: dest_dir,
                    reference,
                },
            })
        }
        FetchedPayload::Layers {
            files,
            media_type,
            digest,
        } => {
            let dest_dir = resolve_dest_dir(task_dir, spec.dest.as_deref())?;
            fs::create_dir_all(&dest_dir)?;
            let mut placed = Vec::with_capacity(files.len());
            for file in files {
                let file_name = file
                    .file_name()
                    .context("ORAS layer file has no name")?
                    .to_os_string();
                let target = dest_dir.join(&file_name);
                if target.exists() {
                    bail!("artifact destination {} already exists", target.display());
                }
                fs::rename(&file, &target)
                    .with_context(|| format!("move ORAS layer into {}", target.display()))?;
                apply_mode(&target, spec.mode.as_deref())?;
                placed.push(target);
            }
            Ok(StagedArtifact {
                name: spec.name.clone(),
                digest: Some(digest),
                bytes,
                payload: StagedPayload::Layers {
                    dir: dest_dir,
                    files: placed,
                    media_type,
                },
            })
        }
    }
}

/// Resolves a directory destination under the task dir (validated relative).
fn resolve_dest_dir(task_dir: &Path, dest: Option<&str>) -> R<PathBuf> {
    let dest = dest.unwrap_or(".");
    crate::structs::validate_artifact_dest(dest)?;
    // Avoid `…/tool-task/.` showing up in env vars and status surfaces.
    let resolved = if dest == "." || dest == "./" {
        task_dir.to_path_buf()
    } else {
        task_dir.join(dest)
    };
    fs::create_dir_all(&resolved)
        .with_context(|| format!("create artifact dest {}", resolved.display()))?;
    Ok(resolved)
}

/// Resolves a single-file destination: explicit `dest` (a trailing `/` means
/// "directory; keep the source basename"), defaulting to the source basename
/// in the task dir root.
fn resolve_dest_file(task_dir: &Path, dest: Option<&str>, file_name: &str) -> R<PathBuf> {
    match dest {
        None => Ok(task_dir.join(file_name)),
        Some(dest) => {
            crate::structs::validate_artifact_dest(dest)?;
            if dest.ends_with('/') || dest == "." {
                Ok(task_dir.join(dest).join(file_name))
            } else {
                Ok(task_dir.join(dest))
            }
        }
    }
}

/// Moves every top-level entry of `src_dir` into `dest_dir`, erroring on
/// collisions rather than overwriting existing task files.
fn merge_move(src_dir: &Path, dest_dir: &Path) -> R<()> {
    fs::create_dir_all(dest_dir)?;
    for entry in fs::read_dir(src_dir)
        .with_context(|| format!("read staged artifact dir {}", src_dir.display()))?
    {
        let entry = entry?;
        let target = dest_dir.join(entry.file_name());
        if target.exists() {
            bail!(
                "artifact content {} collides with existing path {}",
                entry.file_name().to_string_lossy(),
                target.display()
            );
        }
        fs::rename(entry.path(), &target)
            .with_context(|| format!("move staged artifact entry into {}", target.display()))?;
    }
    Ok(())
}

fn apply_mode(path: &Path, mode: Option<&str>) -> R<()> {
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::PermissionsExt;
        let bits = u32::from_str_radix(mode.trim_start_matches('0'), 8)
            .with_context(|| format!("artifact mode {mode} must be octal"))?;
        fs::set_permissions(path, fs::Permissions::from_mode(bits))
            .with_context(|| format!("chmod {} to {mode}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
    Ok(())
}

/// Marker file recording a fully successful staging run, written into the
/// task directory so a worker restart (or in-place re-dispatch with an
/// unchanged artifact list) reuses the staged content instead of refetching.
const STAGED_MANIFEST_FILE: &str = ".staged.json";
const STAGED_MANIFEST_VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StagedManifest {
    version: u32,
    /// sha256 of the normalized artifact list; reuse requires an exact match.
    spec_fingerprint: String,
    artifacts: Vec<StagedRecord>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StagedRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    digest: Option<String>,
    bytes: u64,
    payload: StagedRecordPayload,
}

/// Payloads recorded with task-dir-relative paths so the manifest stays
/// valid wherever the worker state dir is mounted.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
enum StagedRecordPayload {
    File {
        path: String,
    },
    Tree {
        dir: String,
    },
    Image {
        oci_layout: String,
        reference: String,
    },
    Layers {
        dir: String,
        files: Vec<String>,
        media_type: String,
    },
}

/// Identity of an artifact list for reuse purposes: the hash of its
/// normalized JSON (post-admission, so pinned digests and baked auth are
/// part of the identity).
fn spec_fingerprint(artifacts: &[ArtifactSpec]) -> R<String> {
    let serialized = serde_json::to_string(artifacts).context("serialize artifact specs")?;
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serialized.as_bytes())
    ))
}

/// Reuse is disabled when any artifact explicitly opts into per-staging
/// resolution (`resolve: "always"`); pinned/checksummed lists are
/// deterministic and safe to reuse.
fn reuse_allowed(artifacts: &[ArtifactSpec]) -> bool {
    artifacts
        .iter()
        .all(|spec| spec.resolve != Some(crate::structs::ArtifactResolve::Always))
}

fn relative_to_task_dir(task_dir: &Path, path: &Path) -> R<String> {
    if path == task_dir {
        return Ok(".".to_string());
    }
    let relative = path.strip_prefix(task_dir).with_context(|| {
        format!(
            "staged path {} is outside the task directory",
            path.display()
        )
    })?;
    Ok(relative.to_string_lossy().into_owned())
}

fn join_task_dir(task_dir: &Path, relative: &str) -> PathBuf {
    if relative == "." {
        task_dir.to_path_buf()
    } else {
        task_dir.join(relative)
    }
}

fn write_staged_manifest(task_dir: &Path, fingerprint: &str, staged: &[StagedArtifact]) -> R<()> {
    let records = staged
        .iter()
        .map(|artifact| {
            let payload = match &artifact.payload {
                StagedPayload::File { path } => StagedRecordPayload::File {
                    path: relative_to_task_dir(task_dir, path)?,
                },
                StagedPayload::Tree { dir } => StagedRecordPayload::Tree {
                    dir: relative_to_task_dir(task_dir, dir)?,
                },
                StagedPayload::Image {
                    oci_layout,
                    reference,
                } => StagedRecordPayload::Image {
                    oci_layout: relative_to_task_dir(task_dir, oci_layout)?,
                    reference: reference.clone(),
                },
                StagedPayload::Layers {
                    dir,
                    files,
                    media_type,
                } => StagedRecordPayload::Layers {
                    dir: relative_to_task_dir(task_dir, dir)?,
                    files: files
                        .iter()
                        .map(|file| relative_to_task_dir(task_dir, file))
                        .collect::<R<Vec<_>>>()?,
                    media_type: media_type.clone(),
                },
            };
            Ok(StagedRecord {
                name: artifact.name.clone(),
                digest: artifact.digest.clone(),
                bytes: artifact.bytes,
                payload,
            })
        })
        .collect::<R<Vec<_>>>()?;

    let manifest = StagedManifest {
        version: STAGED_MANIFEST_VERSION,
        spec_fingerprint: fingerprint.to_string(),
        artifacts: records,
    };
    // Write-then-rename so a crash mid-write never leaves a manifest that
    // claims success.
    let temp_path = task_dir.join(format!("{STAGED_MANIFEST_FILE}.tmp"));
    fs::write(&temp_path, serde_json::to_vec_pretty(&manifest)?)?;
    fs::rename(&temp_path, task_dir.join(STAGED_MANIFEST_FILE))?;
    Ok(())
}

/// Rebuilds the staged artifacts from a previous run when the manifest
/// matches the requested artifact list and every recorded payload still
/// exists. Any mismatch returns `None` and the caller stages fresh.
fn try_reuse_staged(
    task_dir: &Path,
    fingerprint: &str,
    artifacts: &[ArtifactSpec],
) -> Option<Vec<StagedArtifact>> {
    let raw = fs::read(task_dir.join(STAGED_MANIFEST_FILE)).ok()?;
    let manifest: StagedManifest = serde_json::from_slice(&raw).ok()?;
    if manifest.version != STAGED_MANIFEST_VERSION
        || manifest.spec_fingerprint != fingerprint
        || manifest.artifacts.len() != artifacts.len()
    {
        return None;
    }

    let mut staged = Vec::with_capacity(manifest.artifacts.len());
    for record in manifest.artifacts {
        let payload = match record.payload {
            StagedRecordPayload::File { path } => {
                let path = join_task_dir(task_dir, &path);
                if !path.is_file() {
                    return None;
                }
                StagedPayload::File { path }
            }
            StagedRecordPayload::Tree { dir } => {
                let dir = join_task_dir(task_dir, &dir);
                if !dir.is_dir() {
                    return None;
                }
                StagedPayload::Tree { dir }
            }
            StagedRecordPayload::Image {
                oci_layout,
                reference,
            } => {
                let oci_layout = join_task_dir(task_dir, &oci_layout);
                if !oci_layout.is_dir() {
                    return None;
                }
                StagedPayload::Image {
                    oci_layout,
                    reference,
                }
            }
            StagedRecordPayload::Layers {
                dir,
                files,
                media_type,
            } => {
                let dir = join_task_dir(task_dir, &dir);
                let files: Vec<PathBuf> = files
                    .iter()
                    .map(|file| join_task_dir(task_dir, file))
                    .collect();
                if !dir.is_dir() || files.iter().any(|file| !file.is_file()) {
                    return None;
                }
                StagedPayload::Layers {
                    dir,
                    files,
                    media_type,
                }
            }
        };
        staged.push(StagedArtifact {
            name: record.name,
            digest: record.digest,
            bytes: record.bytes,
            payload,
        });
    }
    Some(staged)
}

/// Removes everything inside `dir` (the dir itself stays, so paths handed to
/// runtimes remain valid).
fn clear_dir_contents(dir: &Path) -> R<()> {
    if !dir.exists() {
        fs::create_dir_all(dir)?;
        return Ok(());
    }
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            fs::remove_dir_all(&path).with_context(|| format!("remove {}", path.display()))?;
        } else {
            fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::status::ArtifactState;
    use crate::structs::ArtifactAuth;
    use tempfile::tempdir;

    #[test]
    fn archive_detection() {
        assert!(looks_like_archive("tool-linux-amd64.tar.gz"));
        assert!(looks_like_archive("a.TGZ"));
        assert!(looks_like_archive("a.tar"));
        assert!(looks_like_archive("a.gz"));
        assert!(!looks_like_archive("binary"));
        assert!(!looks_like_archive("a.zip"));
    }

    /// Builds a .tar.gz holding `entries` of (path, content).
    fn tar_gz(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        for (path, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, path, content.as_bytes())
                .expect("append tar entry");
        }
        builder
            .into_inner()
            .expect("finish tar")
            .finish()
            .expect("finish gzip")
    }

    fn write_archive(dir: &Path, name: &str, entries: &[(&str, &str)]) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, tar_gz(entries)).expect("write archive");
        path
    }

    #[test]
    fn unpack_extracts_safe_entries() -> R<()> {
        let dir = tempdir()?;
        let archive = write_archive(
            dir.path(),
            "tool.tar.gz",
            &[("tool/bin/run", "#!/bin/sh"), ("tool/README", "docs")],
        );
        let dest = dir.path().join("out");
        unpack_archive(&archive, &dest, 1024 * 1024)?;
        assert_eq!(fs::read_to_string(dest.join("tool/bin/run"))?, "#!/bin/sh");
        assert_eq!(fs::read_to_string(dest.join("tool/README"))?, "docs");
        Ok(())
    }

    #[test]
    fn unpack_rejects_path_traversal() -> R<()> {
        let dir = tempdir()?;
        // The tar builder refuses to *create* `..` entries, so write the
        // header name bytes directly the way a hostile archive would.
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        let content = b"pwn";
        let mut header = tar::Header::new_gnu();
        {
            let gnu = header.as_gnu_mut().expect("gnu header");
            let name = b"../escape.txt";
            gnu.name[..name.len()].copy_from_slice(name);
        }
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, content.as_slice())?;
        let bytes = builder.into_inner()?.finish()?;
        let archive = dir.path().join("evil.tar.gz");
        fs::write(&archive, bytes)?;

        let dest = dir.path().join("out");
        let err = unpack_archive(&archive, &dest, 1024 * 1024).unwrap_err();
        assert!(err.to_string().contains("escapes"), "{err}");
        assert!(!dir.path().join("escape.txt").exists());
        Ok(())
    }

    #[test]
    fn unpack_rejects_escaping_symlink() -> R<()> {
        let dir = tempdir()?;
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_cksum();
        builder.append_link(&mut header, "link", "../../etc/passwd")?;
        let bytes = builder.into_inner()?.finish()?;
        let archive = dir.path().join("symlink.tar.gz");
        fs::write(&archive, bytes)?;

        let err = unpack_archive(&archive, &dir.path().join("out"), 1024 * 1024).unwrap_err();
        assert!(err.to_string().contains("escapes"), "{err}");

        // Absolute symlink targets are rejected too.
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_cksum();
        builder.append_link(&mut header, "link", "/etc/passwd")?;
        let bytes = builder.into_inner()?.finish()?;
        let archive = dir.path().join("abs.tar.gz");
        fs::write(&archive, bytes)?;
        let err = unpack_archive(&archive, &dir.path().join("out2"), 1024 * 1024).unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
        Ok(())
    }

    #[test]
    fn unpack_enforces_decompressed_size_budget() -> R<()> {
        let dir = tempdir()?;
        let big = "x".repeat(4096);
        let archive = write_archive(dir.path(), "big.tar.gz", &[("big.txt", &big)]);
        let err = unpack_archive(&archive, &dir.path().join("out"), 100).unwrap_err();
        assert!(err.to_string().contains("maxArtifactSize"), "{err}");
        Ok(())
    }

    #[test]
    fn gunzips_bare_gz_files() -> R<()> {
        let dir = tempdir()?;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"raw contents")?;
        let archive = dir.path().join("data.txt.gz");
        fs::write(&archive, encoder.finish()?)?;

        let dest = dir.path().join("out");
        unpack_archive(&archive, &dest, 1024)?;
        assert_eq!(fs::read_to_string(dest.join("data.txt"))?, "raw contents");
        Ok(())
    }

    #[test]
    fn fetch_semaphore_bounds_concurrency() {
        let semaphore = Arc::new(FetchSemaphore::new(2));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let semaphore = semaphore.clone();
            let active = active.clone();
            let peak = peak.clone();
            handles.push(std::thread::spawn(move || {
                let _permit = semaphore.acquire();
                let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(20));
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }
        for handle in handles {
            handle.join().expect("worker thread");
        }
        assert!(peak.load(std::sync::atomic::Ordering::SeqCst) <= 2);
    }

    fn test_stager(attempts: u32) -> ArtifactStager {
        ArtifactStager::new(
            WorkerArtifactFetchConfigFull {
                attempts,
                allowed_schemes: vec!["http".into(), "https".into(), "file".into()],
                ..Default::default()
            },
            Box::new(AnonymousCredentials),
        )
    }

    fn spec(source: &str) -> ArtifactSpec {
        ArtifactSpec::from_source(source)
    }

    #[test]
    fn stages_archive_and_reports_states() -> R<()> {
        let src = tempdir()?;
        let archive = write_archive(src.path(), "tool.tar.gz", &[("tool/run.sh", "echo hi")]);

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", archive.display()));
        artifact.name = Some("tool".into());
        board.init(1, std::slice::from_ref(&artifact));

        let staged =
            test_stager(1).stage_task_artifacts(1, task_dir.path(), &[artifact], &board)?;

        assert_eq!(staged.len(), 1);
        // Archives auto-unpack into the task directory root by default.
        assert_eq!(
            fs::read_to_string(task_dir.path().join("tool/run.sh"))?,
            "echo hi"
        );
        assert_eq!(staged[0].reference_path(), task_dir.path());
        assert!(staged[0].digest.as_deref().unwrap().starts_with("sha256:"));
        assert!(
            !task_dir.path().join(".staging").exists(),
            "staging cleaned"
        );

        let rows = board.snapshot(1).expect("rows");
        assert_eq!(rows[0].state, ArtifactState::Staged);
        assert!(rows[0].duration_ms.is_some());
        Ok(())
    }

    #[test]
    fn stages_single_file_with_mode_and_dest() -> R<()> {
        let src = tempdir()?;
        let binary = src.path().join("tool");
        fs::write(&binary, "#!/bin/sh")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", binary.display()));
        artifact.name = Some("tool".into());
        artifact.dest = Some("bin/".into());
        artifact.mode = Some("0755".into());
        board.init(1, std::slice::from_ref(&artifact));

        let staged =
            test_stager(1).stage_task_artifacts(1, task_dir.path(), &[artifact], &board)?;
        let staged_path = task_dir.path().join("bin/tool");
        assert!(staged_path.is_file());
        assert_eq!(staged[0].reference_path(), staged_path);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&staged_path)?.permissions().mode();
            assert_eq!(mode & 0o777, 0o755, "mode applied");
        }
        Ok(())
    }

    #[test]
    fn checksum_mismatch_fails_and_discards_bytes() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("tool.bin");
        fs::write(&file, "actual contents")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", file.display()));
        artifact.checksum = Some(format!("sha256:{}", "0".repeat(64)));
        board.init(1, std::slice::from_ref(&artifact));

        let err = test_stager(2)
            .stage_task_artifacts(1, task_dir.path(), &[artifact], &board)
            .unwrap_err();
        assert!(format!("{err:#}").contains("checksum mismatch"), "{err:#}");

        // Corrupt bytes never reach the task dir; staging temp is cleaned.
        assert!(!task_dir.path().join("tool.bin").exists());
        assert!(!task_dir.path().join(".staging").exists());

        let rows = board.snapshot(1).expect("rows");
        assert_eq!(rows[0].state, ArtifactState::Failed);
        assert_eq!(rows[0].attempts, Some(2), "retries exhausted");
        assert!(rows[0].error.as_deref().unwrap().contains("checksum"));
        Ok(())
    }

    #[test]
    fn checksum_match_stages_file() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("tool.bin");
        fs::write(&file, "bytes")?;
        let digest = {
            use sha2::Digest as _;
            format!("sha256:{:x}", Sha256::digest(b"bytes"))
        };

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", file.display()));
        artifact.checksum = Some(digest.clone());
        board.init(1, std::slice::from_ref(&artifact));

        let staged =
            test_stager(1).stage_task_artifacts(1, task_dir.path(), &[artifact], &board)?;
        assert_eq!(staged[0].digest.as_deref(), Some(digest.as_str()));
        Ok(())
    }

    #[test]
    fn disallowed_scheme_rejected_on_worker() -> R<()> {
        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let artifact = spec("s3://bucket/key");
        board.init(1, std::slice::from_ref(&artifact));

        // Stager config allows only http/https/file (see test_stager).
        let err = test_stager(1)
            .stage_task_artifacts(1, task_dir.path(), &[artifact], &board)
            .unwrap_err();
        assert!(format!("{err:#}").contains("allowedSchemes"), "{err:#}");
        Ok(())
    }

    #[test]
    fn all_or_nothing_staging_stops_at_first_failure() -> R<()> {
        let src = tempdir()?;
        let good = src.path().join("good.bin");
        fs::write(&good, "ok")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let artifacts = vec![
            spec(&format!("file://{}", good.display())).named("good"),
            spec(&format!("file://{}/missing.bin", src.path().display())).named("missing"),
        ];
        board.init(1, &artifacts);

        let err = test_stager(1)
            .stage_task_artifacts(1, task_dir.path(), &artifacts, &board)
            .unwrap_err();
        assert!(format!("{err:#}").contains("missing.bin"), "{err:#}");

        let rows = board.snapshot(1).expect("rows");
        assert_eq!(rows[0].state, ArtifactState::Staged, "first staged");
        assert_eq!(rows[1].state, ArtifactState::Failed, "second failed");
        Ok(())
    }

    #[test]
    fn kv_auth_without_credential_source_names_the_path() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("tool.bin");
        fs::write(&file, "x")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", file.display()));
        artifact.auth = Some(ArtifactAuth {
            kv: Some("secrets/build-store".into()),
            ..Default::default()
        });
        board.init(1, std::slice::from_ref(&artifact));

        let err = test_stager(1)
            .stage_task_artifacts(1, task_dir.path(), &[artifact], &board)
            .unwrap_err();
        let rendered = format!("{err:#}");
        // Errors name the auth *source* (the KV path), never values.
        assert!(rendered.contains("secrets/build-store"), "{rendered}");
        Ok(())
    }

    #[test]
    fn identity_auth_fails_actionably_until_wi_issuance_lands() -> R<()> {
        let src = tempdir()?;
        let file = src.path().join("tool.bin");
        fs::write(&file, "x")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", file.display()));
        artifact.auth = Some(ArtifactAuth {
            node_identity: Some("artifact-pull".into()),
            ..Default::default()
        });
        board.init(1, std::slice::from_ref(&artifact));

        let err = test_stager(1)
            .stage_task_artifacts(1, task_dir.path(), &[artifact], &board)
            .unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("workload-identity"), "{rendered}");
        assert!(rendered.contains("artifact-pull"), "{rendered}");
        Ok(())
    }

    #[test]
    fn collision_between_artifacts_fails() -> R<()> {
        // Fresh staging clears the task dir first, so pre-existing content
        // never collides — but two artifacts in one task unpacking the same
        // path still must error rather than silently overwrite.
        let src = tempdir()?;
        let first = write_archive(src.path(), "a.tar.gz", &[("config.json", "from-a")]);
        let second = write_archive(src.path(), "b.tar.gz", &[("config.json", "from-b")]);

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let artifacts = vec![
            spec(&format!("file://{}", first.display())).named("a"),
            spec(&format!("file://{}", second.display())).named("b"),
        ];
        board.init(1, &artifacts);

        let err = test_stager(1)
            .stage_task_artifacts(1, task_dir.path(), &artifacts, &board)
            .unwrap_err();
        assert!(format!("{err:#}").contains("collides"), "{err:#}");
        // The first artifact's content is what landed.
        assert_eq!(
            fs::read_to_string(task_dir.path().join("config.json"))?,
            "from-a"
        );
        Ok(())
    }

    #[test]
    fn unpack_file_mode_stages_archive_as_is() -> R<()> {
        let src = tempdir()?;
        let archive = write_archive(src.path(), "data.tar.gz", &[("inner.txt", "x")]);

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", archive.display()));
        artifact.unpack = crate::structs::ArtifactUnpack::File;
        board.init(1, std::slice::from_ref(&artifact));

        let staged =
            test_stager(1).stage_task_artifacts(1, task_dir.path(), &[artifact], &board)?;
        assert!(task_dir.path().join("data.tar.gz").is_file());
        assert!(matches!(
            staged[0].payload,
            crate::artifacts::StagedPayload::File { .. }
        ));
        Ok(())
    }

    // --- restart reuse via the staging manifest ---

    #[test]
    fn restaging_reuses_previous_fetch_without_touching_origin() -> R<()> {
        let src = tempdir()?;
        let archive = write_archive(src.path(), "tool.tar.gz", &[("tool/run.sh", "echo hi")]);

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", archive.display()));
        artifact.name = Some("tool".into());
        board.init(1, std::slice::from_ref(&artifact));

        let stager = test_stager(1);
        let first = stager.stage_task_artifacts(
            1,
            task_dir.path(),
            std::slice::from_ref(&artifact),
            &board,
        )?;
        assert!(
            task_dir.path().join(STAGED_MANIFEST_FILE).is_file(),
            "manifest written after success"
        );

        // The origin is gone: any refetch would fail, so success proves reuse.
        fs::remove_file(&archive)?;
        board.init(1, std::slice::from_ref(&artifact));
        let second = stager.stage_task_artifacts(
            1,
            task_dir.path(),
            std::slice::from_ref(&artifact),
            &board,
        )?;

        assert_eq!(first, second, "reused payloads match the original run");
        assert_eq!(
            fs::read_to_string(task_dir.path().join("tool/run.sh"))?,
            "echo hi"
        );
        let rows = board.snapshot(1).expect("rows");
        assert_eq!(rows[0].state, ArtifactState::Staged);
        assert_eq!(rows[0].digest, first[0].digest);
        Ok(())
    }

    #[test]
    fn restaging_refetches_when_specs_changed() -> R<()> {
        let src = tempdir()?;
        let binary = src.path().join("tool.bin");
        fs::write(&binary, "v1")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", binary.display()));
        artifact.unpack = crate::structs::ArtifactUnpack::File;
        board.init(1, std::slice::from_ref(&artifact));

        let stager = test_stager(1);
        stager.stage_task_artifacts(1, task_dir.path(), std::slice::from_ref(&artifact), &board)?;

        // A different artifact list (changed dest) must not reuse the old run.
        fs::write(&binary, "v2")?;
        let mut changed = artifact.clone();
        changed.dest = Some("bin/".into());
        board.init(1, std::slice::from_ref(&changed));
        let staged = stager.stage_task_artifacts(
            1,
            task_dir.path(),
            std::slice::from_ref(&changed),
            &board,
        )?;
        assert_eq!(
            fs::read_to_string(staged[0].reference_path())?,
            "v2",
            "changed spec refetches from origin"
        );
        // The fresh run cleared the previous layout entirely.
        assert!(!task_dir.path().join("tool.bin").exists());
        Ok(())
    }

    #[test]
    fn restaging_refetches_when_staged_payload_is_missing() -> R<()> {
        let src = tempdir()?;
        let binary = src.path().join("tool.bin");
        fs::write(&binary, "bytes")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", binary.display()));
        artifact.unpack = crate::structs::ArtifactUnpack::File;
        board.init(1, std::slice::from_ref(&artifact));

        let stager = test_stager(1);
        stager.stage_task_artifacts(1, task_dir.path(), std::slice::from_ref(&artifact), &board)?;

        // Manifest intact but the payload vanished: reuse must not serve a
        // dangling path.
        fs::remove_file(task_dir.path().join("tool.bin"))?;
        board.init(1, std::slice::from_ref(&artifact));
        let staged = stager.stage_task_artifacts(
            1,
            task_dir.path(),
            std::slice::from_ref(&artifact),
            &board,
        )?;
        assert!(staged[0].reference_path().is_file());
        Ok(())
    }

    #[test]
    fn resolve_always_disables_reuse() -> R<()> {
        let src = tempdir()?;
        let binary = src.path().join("tool.bin");
        fs::write(&binary, "v1")?;

        let task_dir = tempdir()?;
        let board = ArtifactStatusBoard::new();
        let mut artifact = spec(&format!("file://{}", binary.display()));
        artifact.unpack = crate::structs::ArtifactUnpack::File;
        artifact.resolve = Some(crate::structs::ArtifactResolve::Always);
        board.init(1, std::slice::from_ref(&artifact));

        let stager = test_stager(1);
        let first = stager.stage_task_artifacts(
            1,
            task_dir.path(),
            std::slice::from_ref(&artifact),
            &board,
        )?;

        // resolve: "always" means every staging re-reads the source.
        fs::write(&binary, "v2-different-content")?;
        board.init(1, std::slice::from_ref(&artifact));
        let second = stager.stage_task_artifacts(
            1,
            task_dir.path(),
            std::slice::from_ref(&artifact),
            &board,
        )?;
        assert_ne!(first[0].digest, second[0].digest, "refetched from origin");
        assert_eq!(
            fs::read_to_string(second[0].reference_path())?,
            "v2-different-content"
        );
        Ok(())
    }

    #[test]
    fn manifest_round_trip_covers_all_payload_kinds() -> R<()> {
        let task_dir = tempdir()?;
        let task_dir = task_dir.path();
        fs::write(task_dir.join("tool"), "x")?;
        fs::create_dir_all(task_dir.join("repo"))?;
        fs::create_dir_all(task_dir.join(".oci/image"))?;
        fs::create_dir_all(task_dir.join("models"))?;
        fs::write(task_dir.join("models/w.bin"), "w")?;

        let staged = vec![
            StagedArtifact {
                name: Some("tool".into()),
                digest: Some(format!("sha256:{}", "a".repeat(64))),
                bytes: 1,
                payload: StagedPayload::File {
                    path: task_dir.join("tool"),
                },
            },
            StagedArtifact {
                name: None,
                digest: None,
                bytes: 2,
                payload: StagedPayload::Tree {
                    // dest "." trees reference the task dir itself.
                    dir: task_dir.to_path_buf(),
                },
            },
            StagedArtifact {
                name: Some("image".into()),
                digest: Some(format!("sha256:{}", "b".repeat(64))),
                bytes: 3,
                payload: StagedPayload::Image {
                    oci_layout: task_dir.join(".oci/image"),
                    reference: "ghcr.io/acme/api:v1".into(),
                },
            },
            StagedArtifact {
                name: Some("model".into()),
                digest: Some(format!("sha256:{}", "c".repeat(64))),
                bytes: 4,
                payload: StagedPayload::Layers {
                    dir: task_dir.join("models"),
                    files: vec![task_dir.join("models/w.bin")],
                    media_type: "application/vnd.acme.weights".into(),
                },
            },
        ];

        let specs = [spec("https://example.com/a"), spec("https://example.com/b")];
        let fingerprint_specs = [
            specs[0].clone(),
            specs[1].clone(),
            specs[0].clone(),
            specs[1].clone(),
        ];
        let fingerprint = spec_fingerprint(&fingerprint_specs)?;
        write_staged_manifest(task_dir, &fingerprint, &staged)?;

        let reused =
            try_reuse_staged(task_dir, &fingerprint, &fingerprint_specs).expect("manifest matches");
        assert_eq!(reused, staged);

        // A different fingerprint is rejected outright.
        assert!(try_reuse_staged(task_dir, "sha256:other", &fingerprint_specs).is_none());
        Ok(())
    }
}
