//! Worker-side workload-identity delivery and refresh.
//!
//! The worker — never the task, and never the scheduler's desired-state
//! payload — fetches tokens from the manager (`IssueWorkloadIdentityToken`)
//! and materializes them for the task:
//!
//! - **env delivery**: the token is injected into the task's kind-config
//!   `env` map before start (same mechanism as the manager's
//!   `LEGION_INSTANCE_*` injection, but performed worker-side so token bytes
//!   never transit the scheduler). Because a live process's environment is
//!   fixed at exec time, every env-delivered identity is **also** mirrored to
//!   a well-known per-identity file, `.legion/identity/<alloc>/<name>/token`
//!   (relative to the worker's working directory), and a companion
//!   `<ENV>_FILE` variable points at it — so a `changeMode: signal` handler
//!   has somewhere to re-read the fresh token from. With `changeMode:
//!   restart` the stored task config's env is updated before the restart, so
//!   the relaunched process sees the new token directly.
//! - **file delivery**: the token is written to the identity's `file.path`
//!   (absolute, or relative to the worker's working directory — the same
//!   rooting as `exec` injected files) with `file.mode` (default `0400`),
//!   atomically (temp file + rename, the `src/certs.rs` convention).
//!
//! A per-identity refresh loop re-issues each token at roughly half its
//! remaining TTL (with jitter, and retry/backoff on failure), rewrites the
//! file(s) atomically, then applies the identity's `changeMode`:
//! `noop` (nothing — SDKs re-read the file), `signal` (deliver
//! `changeSignal` to the task process), or `restart` (restart the allocation
//! in place via the task manager). Loops stop when the allocation stops.
//!
//! If the manager has workload identity disabled, issuance returns
//! FailedPrecondition; the worker logs one warning and treats the task's
//! identities as inert (the task still starts, with no tokens delivered).
//!
//! Token bytes are never logged (see AGENTS.md tracing rules).

use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use crate::structs::workload_identity::{ChangeMode, WorkloadIdentity};
use crate::structs::{IssueWorkloadIdentityTokenRequest, TaskAllocation};
use crate::util::random_string;
use crate::worker::task_manager::WorkerTaskManager;

use rand::Rng;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::Code;

/// Default token file mode: owner-read only.
pub const DEFAULT_TOKEN_FILE_MODE: u32 = 0o400;

/// Root (relative to the worker's working directory) for the well-known
/// per-identity token files that back env-delivered identities.
pub const IDENTITY_TOKEN_DIR: &str = ".legion/identity";

/// Maximum backoff between re-issue retries inside the refresh loop.
const REFRESH_RETRY_MAX: Duration = Duration::from_secs(30);

/// A token as issued by the manager. The expiry drives the refresh schedule.
#[derive(Debug, Clone)]
pub struct IssuedIdentityToken {
    pub token: String,
    pub expires_at: u64,
}

/// Source of identity tokens. The production implementation calls the
/// manager's `IssueWorkloadIdentityToken` RPC; tests substitute a fake.
#[tonic::async_trait]
pub trait TokenIssuer: Send + Sync + 'static {
    /// Issue a token for `(alloc_id, identity_name)`. `Ok(None)` means the
    /// manager has workload identity disabled (identities are inert).
    async fn issue(&self, alloc_id: i64, identity_name: &str) -> R<Option<IssuedIdentityToken>>;
}

/// gRPC-backed issuer: asks the manager that placed the allocation.
pub struct ManagerTokenIssuer {
    pub client: ManagerClient<Channel>,
    /// This worker's node id (the same id used in heartbeats); the manager
    /// rejects requests for allocations placed on other nodes.
    pub node_id: String,
    /// Worker-plane credentials; identity issuance requires the node token
    /// when ACLs are enforced.
    pub credentials: std::sync::Arc<crate::worker::daemon::WorkerCredentials>,
}

#[tonic::async_trait]
impl TokenIssuer for ManagerTokenIssuer {
    async fn issue(&self, alloc_id: i64, identity_name: &str) -> R<Option<IssuedIdentityToken>> {
        let mut client = self.client.clone();
        let request =
            self.credentials
                .attach(tonic::Request::new(IssueWorkloadIdentityTokenRequest {
                    alloc_id,
                    identity_name: identity_name.to_string(),
                    node_id: self.node_id.clone(),
                }));
        match client.issue_workload_identity_token(request).await {
            Ok(reply) => {
                let reply = reply.into_inner();
                Ok(Some(IssuedIdentityToken {
                    token: reply.token,
                    expires_at: reply.expires_at.max(0) as u64,
                }))
            }
            Err(status) if status.code() == Code::FailedPrecondition => Ok(None),
            Err(status) => Err(anyhow!(
                "manager refused identity token for alloc {alloc_id} identity \
                 {identity_name:?}: {status}"
            )),
        }
    }
}

/// One identity that was delivered at launch, plus the expiry that seeds its
/// refresh loop.
#[derive(Debug, Clone)]
pub struct PreparedIdentity {
    pub identity: WorkloadIdentity,
    pub expires_at: u64,
}

/// Owns token delivery and the per-allocation refresh loops on a worker.
pub struct WorkerIdentityManager {
    issuer: Arc<dyn TokenIssuer>,
    refreshers: Mutex<HashMap<i64, Vec<JoinHandle<()>>>>,
    /// Backing token file(s) delivered per allocation, so they can be removed
    /// when the allocation stops (short-TTL hygiene: live token material must
    /// not linger on disk past the workload's life).
    delivered_files: Mutex<HashMap<i64, Vec<PathBuf>>>,
    warned_disabled: AtomicBool,
}

impl WorkerIdentityManager {
    pub fn new(issuer: Arc<dyn TokenIssuer>) -> Self {
        Self {
            issuer,
            refreshers: Mutex::new(HashMap::new()),
            delivered_files: Mutex::new(HashMap::new()),
            warned_disabled: AtomicBool::new(false),
        }
    }

    /// Fetch and deliver every identity declared on `allocation` **before**
    /// the task starts: env tokens are injected into the task-config env map
    /// (mutating `allocation.task_config`), file tokens are written
    /// atomically. Returns the prepared identities for
    /// [`WorkerIdentityManager::spawn_refreshers`].
    ///
    /// Fails if any token cannot be issued (a task must not start without an
    /// identity it declared) — except when the manager reports the feature
    /// disabled, in which case identities are inert and an empty list is
    /// returned (warned once).
    #[instrument(skip_all, fields(allocation_id = allocation.allocation_id))]
    pub async fn prepare_task_allocation(
        &self,
        allocation: &mut TaskAllocation,
    ) -> R<Vec<PreparedIdentity>> {
        let identities = parse_identities(&allocation.task_identities)?;
        if identities.is_empty() {
            return Ok(Vec::new());
        }

        let alloc_id = allocation.allocation_id;
        let mut prepared = Vec::new();
        let mut env_injections: Vec<(String, String)> = Vec::new();

        for identity in identities {
            let Some(issued) = self.issuer.issue(alloc_id, &identity.name).await? else {
                if !self.warned_disabled.swap(true, Ordering::Relaxed) {
                    warn!(
                        "Manager has workload identity disabled; task identities are inert \
                         (no tokens will be delivered)"
                    );
                }
                return Ok(Vec::new());
            };

            write_identity_files(&identity, alloc_id, &issued.token)?;
            if let Some(backing) = backing_file_path(&identity, alloc_id) {
                self.delivered_files
                    .lock()
                    .expect("identity delivered-files lock")
                    .entry(alloc_id)
                    .or_default()
                    .push(backing);
            }

            if let Some(env_key) = identity.env.as_deref().filter(|e| !e.trim().is_empty()) {
                env_injections.push((env_key.to_string(), issued.token.clone()));
                // Companion var so a `changeMode: signal` handler can re-read
                // the refreshed token from the backing file (env itself cannot
                // change). Pointless for `restart` (the relaunch re-injects the
                // fresh token directly into env) and for kinds whose filesystem
                // the host path doesn't reach — so only emit it when a signal
                // handler would actually use it.
                if identity.change_mode == ChangeMode::Signal {
                    if let Some(backing) = backing_file_path(&identity, alloc_id) {
                        env_injections.push((
                            format!("{env_key}_FILE"),
                            backing.to_string_lossy().to_string(),
                        ));
                    }
                }
            }

            info!(
                identity = %identity.name,
                expires_at = issued.expires_at,
                "Delivered workload identity token"
            );
            prepared.push(PreparedIdentity {
                identity,
                expires_at: issued.expires_at,
            });
        }

        if !env_injections.is_empty() {
            inject_env(&mut allocation.task_config, &env_injections)
                .context("failed to inject identity env vars into task config")?;
        }

        Ok(prepared)
    }

    /// Spawn one refresh loop per prepared identity for this allocation.
    /// Loops re-issue at ~half TTL, rewrite atomically, and apply the
    /// identity's change mode; they stop when the allocation stops (or when
    /// [`WorkerIdentityManager::retain`] / [`WorkerIdentityManager::stop`]
    /// aborts them).
    pub fn spawn_refreshers(
        self: &Arc<Self>,
        alloc_id: i64,
        prepared: Vec<PreparedIdentity>,
        task_manager: Arc<Mutex<WorkerTaskManager>>,
    ) {
        if prepared.is_empty() {
            return;
        }
        let mut handles = Vec::new();
        for entry in prepared {
            let issuer = self.issuer.clone();
            let task_manager = task_manager.clone();
            handles.push(tokio::spawn(async move {
                refresh_loop(
                    issuer,
                    alloc_id,
                    entry.identity,
                    entry.expires_at,
                    task_manager,
                )
                .await;
            }));
        }
        let mut refreshers = self.refreshers.lock().expect("identity refreshers lock");
        refreshers.entry(alloc_id).or_default().extend(handles);
    }

    /// Stop refresh loops for every allocation not in `live` (called after
    /// each reconcile so loops never outlive their allocation).
    pub fn retain(&self, live: &[i64]) {
        let live: std::collections::HashSet<i64> = live.iter().copied().collect();
        let mut pruned = Vec::new();
        {
            let mut refreshers = self.refreshers.lock().expect("identity refreshers lock");
            refreshers.retain(|alloc_id, handles| {
                if live.contains(alloc_id) {
                    return true;
                }
                for handle in handles.iter() {
                    handle.abort();
                }
                debug!(allocation_id = alloc_id, "Stopped identity refresh loops");
                pruned.push(*alloc_id);
                false
            });
        }
        for alloc_id in pruned {
            self.cleanup_files(alloc_id);
        }
    }

    /// Stop refresh loops for one allocation.
    pub fn stop(&self, alloc_id: i64) {
        {
            let mut refreshers = self.refreshers.lock().expect("identity refreshers lock");
            if let Some(handles) = refreshers.remove(&alloc_id) {
                for handle in handles {
                    handle.abort();
                }
            }
        }
        self.cleanup_files(alloc_id);
    }

    /// Remove the token material delivered for `alloc_id`: each declared
    /// `file.path` target plus the env-fallback directory
    /// `.legion/identity/<alloc>`. Never logs token bytes; best-effort (a
    /// failure to unlink is warned, not fatal).
    fn cleanup_files(&self, alloc_id: i64) {
        let paths = self
            .delivered_files
            .lock()
            .expect("identity delivered-files lock")
            .remove(&alloc_id);
        for path in paths.into_iter().flatten() {
            if let Err(err) = fs::remove_file(&path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!(allocation_id = alloc_id, error = ?err, "Failed to remove identity token file");
                }
            }
        }
        // Drop the per-allocation env-fallback tree, if any.
        let fallback_dir = {
            let mut root = PathBuf::from(IDENTITY_TOKEN_DIR);
            if !root.is_absolute() {
                if let Ok(cwd) = std::env::current_dir() {
                    root = cwd.join(root);
                }
            }
            root.join(alloc_id.to_string())
        };
        if let Err(err) = fs::remove_dir_all(&fallback_dir) {
            if err.kind() != std::io::ErrorKind::NotFound {
                debug!(allocation_id = alloc_id, error = ?err, "Failed to remove identity fallback dir");
            }
        }
    }
}

/// Parse the JSON-encoded identity list off a [`TaskAllocation`].
pub fn parse_identities(raw: &str) -> R<Vec<WorkloadIdentity>> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(raw).context("invalid task identities JSON")
}

/// Where the refreshable copy of this identity's token lives: the declared
/// `file.path` when file delivery is configured, else the well-known fallback
/// for env-delivered identities.
fn backing_file_path(identity: &WorkloadIdentity, alloc_id: i64) -> Option<PathBuf> {
    if let Some(file) = identity.file.as_ref().filter(|f| !f.path.trim().is_empty()) {
        return Some(PathBuf::from(&file.path));
    }
    if identity.delivers_env() {
        return Some(fallback_token_path(alloc_id, &identity.name));
    }
    None
}

/// Well-known per-identity token path backing env-delivered identities:
/// `<cwd>/.legion/identity/<alloc>/<name>/token`, **absolute** (rooted at the
/// worker's working directory at startup).
///
/// It must be absolute because the same value is injected as the `<ENV>_FILE`
/// companion for a `changeMode: signal` handler to re-read, and a task's own
/// cwd is its private task dir (see `exec`), not the worker's — a relative
/// path would resolve against the wrong directory and the handler would not
/// find the file.
///
/// Defense in depth: the manager already rejects identity names that aren't
/// `[A-Za-z0-9_-]{1,64}` at admission (`validate_identity_name`), but the
/// worker re-checks before joining the name into a path so a name like
/// `../../tmp/evil` can never escape the per-allocation token directory even if
/// a malformed spec reached the worker. An invalid name is reduced to a safe
/// `_`-only placeholder rather than panicking, so token delivery degrades
/// instead of writing outside the task dir.
pub fn fallback_token_path(alloc_id: i64, identity_name: &str) -> PathBuf {
    let safe_name = sanitize_identity_name_component(identity_name);
    let mut root = PathBuf::from(IDENTITY_TOKEN_DIR);
    if !root.is_absolute() {
        if let Ok(cwd) = std::env::current_dir() {
            root = cwd.join(root);
        }
    }
    root.join(alloc_id.to_string())
        .join(safe_name)
        .join("token")
}

/// Return `identity_name` unchanged if it passes the strict admission charset,
/// otherwise a placeholder built only from its allowed characters (and never
/// empty, never `.`/`..`), so it can never act as a path-traversal component.
fn sanitize_identity_name_component(identity_name: &str) -> String {
    use crate::structs::workload_identity::validate_identity_name;
    if validate_identity_name(identity_name).is_ok() {
        return identity_name.to_string();
    }
    warn!(
        "Identity name failed the strict charset check on the worker; \
         sanitizing before path join (defense in depth)"
    );
    let filtered: String = identity_name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .take(crate::structs::workload_identity::MAX_IDENTITY_NAME_LEN)
        .collect();
    if filtered.is_empty() {
        "_invalid".to_string()
    } else {
        filtered
    }
}

/// Write/rewrite every file this identity is materialized at.
fn write_identity_files(identity: &WorkloadIdentity, alloc_id: i64, token: &str) -> R<()> {
    let Some(path) = backing_file_path(identity, alloc_id) else {
        return Ok(());
    };
    let mode = identity
        .file
        .as_ref()
        .and_then(|f| f.mode.as_deref())
        .map(crate::structs::workload_identity::parse_file_mode)
        .transpose()?
        .unwrap_or(DEFAULT_TOKEN_FILE_MODE);
    write_token_file(&path, token, mode)
}

/// Atomically write a token file (temp file + rename, the `src/certs.rs`
/// convention) with restrictive permissions applied before the rename, so the
/// token is never observable at a broader mode.
pub fn write_token_file(path: &Path, token: &str, mode: u32) -> R<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create token directory {parent:?}"))?;
        }
    }
    let tmp = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "token".into()),
        random_string(6)
    ));

    // Create the temp file with the restrictive mode *from the start* so the
    // token bytes are never observable at the process umask (e.g. 0644) in the
    // window between create and chmod. On any error after creation, remove the
    // temp file so a failed write never leaves a stray (and potentially
    // readable) token fragment behind.
    let result = write_token_tmp(&tmp, token, mode);
    if let Err(err) = result {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }

    if let Err(err) = fs::rename(&tmp, path)
        .with_context(|| format!("failed to move token file into place at {path:?}"))
    {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }
    Ok(())
}

/// Create the temp file at `mode` (atomically, before any bytes exist),
/// write the token, and fsync so the rename publishes durable content.
fn write_token_tmp(tmp: &Path, token: &str, mode: u32) -> R<()> {
    use std::io::Write;

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;

    let mut file = options
        .open(tmp)
        .with_context(|| format!("failed to create token temp file {tmp:?}"))?;
    file.write_all(token.as_bytes())
        .with_context(|| format!("failed to write token file {tmp:?}"))?;
    file.flush()
        .with_context(|| format!("failed to flush token file {tmp:?}"))?;
    file.sync_all()
        .with_context(|| format!("failed to fsync token file {tmp:?}"))?;
    Ok(())
}

/// Inject env vars into a JSON-encoded task kind config (the same `env` map
/// the manager's `LEGION_INSTANCE_*` injection uses). Values are written into
/// the config string, never logged.
fn inject_env(task_config_json: &mut String, vars: &[(String, String)]) -> R<()> {
    let mut config: Value =
        serde_json::from_str(task_config_json).context("task config is not valid JSON")?;
    let object = config
        .as_object_mut()
        .context("task config is not a JSON object; cannot deliver env identities")?;
    let env = object
        .entry("env")
        .or_insert_with(|| Value::Object(Default::default()));
    let env = env
        .as_object_mut()
        .context("task config env is not an object")?;
    for (key, value) in vars {
        env.insert(key.clone(), Value::String(value.clone()));
    }
    *task_config_json = serde_json::to_string(&config)?;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// When to wake for the next refresh: roughly half the remaining TTL, with a
/// little jitter so a fleet of identities doesn't thunder at the manager, and
/// never less than one second.
fn refresh_delay(expires_at: u64, now: u64) -> Duration {
    let remaining = expires_at.saturating_sub(now);
    let half = remaining / 2;
    let jitter = if half >= 10 {
        rand::thread_rng().gen_range(0..=half / 5)
    } else {
        0
    };
    Duration::from_secs(half.saturating_sub(jitter).max(1))
}

/// The long-running refresh loop for one identity on one allocation.
async fn refresh_loop(
    issuer: Arc<dyn TokenIssuer>,
    alloc_id: i64,
    identity: WorkloadIdentity,
    mut expires_at: u64,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
) {
    loop {
        tokio::time::sleep(refresh_delay(expires_at, unix_now())).await;

        if !allocation_is_live(&task_manager, alloc_id) {
            debug!(
                allocation_id = alloc_id,
                identity = %identity.name,
                "Allocation stopped; ending identity refresh loop"
            );
            return;
        }

        let issued = match reissue_with_backoff(&issuer, alloc_id, &identity.name).await {
            Some(issued) => issued,
            // Manager reports the feature disabled (or the alloc is gone for
            // good): nothing to refresh anymore.
            None => return,
        };

        match apply_refreshed_token(&identity, alloc_id, &issued.token, &task_manager) {
            Ok(()) => {
                info!(
                    allocation_id = alloc_id,
                    identity = %identity.name,
                    expires_at = issued.expires_at,
                    change_mode = ?identity.change_mode,
                    "Refreshed workload identity token"
                );
                expires_at = issued.expires_at;
            }
            Err(err) => {
                warn!(
                    allocation_id = alloc_id,
                    identity = %identity.name,
                    error = ?err,
                    "Failed to apply refreshed identity token; will retry next cycle"
                );
                expires_at = issued.expires_at;
            }
        }
    }
}

fn allocation_is_live(task_manager: &Arc<Mutex<WorkerTaskManager>>, alloc_id: i64) -> bool {
    task_manager
        .lock()
        .map(|manager| manager.has_allocation(alloc_id))
        .unwrap_or(false)
}

/// Re-issue with exponential backoff. Returns `None` when refresh should stop
/// (feature disabled, or the allocation no longer exists locally).
async fn reissue_with_backoff(
    issuer: &Arc<dyn TokenIssuer>,
    alloc_id: i64,
    identity_name: &str,
) -> Option<IssuedIdentityToken> {
    let mut backoff = Duration::from_millis(500);
    loop {
        match issuer.issue(alloc_id, identity_name).await {
            Ok(Some(issued)) => return Some(issued),
            Ok(None) => {
                debug!(
                    allocation_id = alloc_id,
                    identity = identity_name,
                    "Workload identity disabled on manager; ending refresh loop"
                );
                return None;
            }
            Err(err) => {
                warn!(
                    allocation_id = alloc_id,
                    identity = identity_name,
                    error = ?err,
                    backoff_ms = backoff.as_millis() as u64,
                    "Identity token re-issue failed; backing off"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(REFRESH_RETRY_MAX);
            }
        }
    }
}

/// Apply one refreshed token: rewrite the backing file(s) atomically, update
/// the stored task env (so any restart relaunches with the fresh value), then
/// apply the identity's change mode.
pub(crate) fn apply_refreshed_token(
    identity: &WorkloadIdentity,
    alloc_id: i64,
    token: &str,
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
) -> R<()> {
    write_identity_files(identity, alloc_id, token)?;

    let mut manager = task_manager
        .lock()
        .map_err(|_| anyhow!("task manager lock poisoned"))?;

    if let Some(env_key) = identity.env.as_deref().filter(|e| !e.trim().is_empty()) {
        // A live process's env cannot change; this updates the *stored*
        // config so signal handlers read the backing file (see module docs)
        // and any restart starts with the fresh token.
        manager.update_task_env(alloc_id, env_key, token)?;
    }

    match identity.change_mode {
        ChangeMode::Noop => {}
        ChangeMode::Signal => {
            let signal = identity
                .change_signal
                .as_deref()
                .context("changeMode signal requires changeSignal (validated at admission)")?;
            manager.signal_allocation(alloc_id, signal)?;
        }
        ChangeMode::Restart => {
            manager.restart_allocation(alloc_id)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::workload_identity::WorkloadIdentityFile;
    use crate::structs::TaskConfig;
    use crate::worker::state::WorkerStateStore;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;

    fn task_manager(dir: &TempDir) -> Arc<Mutex<WorkerTaskManager>> {
        let state_store = WorkerStateStore::new(dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();
        Arc::new(Mutex::new(manager))
    }

    fn alloc(id: i64) -> crate::structs::Allocation {
        crate::structs::Allocation {
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

    /// Fake issuer: counts issuance calls and mints `tok-<n>` tokens with a
    /// configurable TTL.
    struct FakeIssuer {
        calls: AtomicU64,
        ttl_secs: u64,
        disabled: bool,
    }

    impl FakeIssuer {
        fn new(ttl_secs: u64) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicU64::new(0),
                ttl_secs,
                disabled: false,
            })
        }
    }

    #[tonic::async_trait]
    impl TokenIssuer for FakeIssuer {
        async fn issue(
            &self,
            _alloc_id: i64,
            _identity_name: &str,
        ) -> R<Option<IssuedIdentityToken>> {
            if self.disabled {
                return Ok(None);
            }
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Some(IssuedIdentityToken {
                token: format!("tok-{n}"),
                expires_at: unix_now() + self.ttl_secs,
            }))
        }
    }

    fn file_identity(path: &Path, change_mode: ChangeMode) -> WorkloadIdentity {
        WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            file: Some(WorkloadIdentityFile {
                path: path.to_string_lossy().to_string(),
                mode: None,
            }),
            change_mode,
            ..Default::default()
        }
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn token_file_written_with_default_0400_and_rewritten_atomically() -> R<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("identity/aws/token");

        write_token_file(&path, "first", DEFAULT_TOKEN_FILE_MODE)?;
        assert_eq!(fs::read_to_string(&path)?, "first");
        #[cfg(unix)]
        assert_eq!(mode_of(&path), 0o400);

        // Rewrite in place: content replaced, restrictive mode kept, and no
        // temp files left behind.
        write_token_file(&path, "second", DEFAULT_TOKEN_FILE_MODE)?;
        assert_eq!(fs::read_to_string(&path)?, "second");
        #[cfg(unix)]
        assert_eq!(mode_of(&path), 0o400);
        let leftovers: Vec<_> = fs::read_dir(path.parent().unwrap())?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() != "token")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn temp_file_is_never_observable_at_a_broader_mode() -> R<()> {
        // Use a deliberately custom target mode (0600, broader than the 0400
        // default) and assert the *temp* file already carries exactly that mode
        // the instant it exists — proving there is no umask window before a
        // chmod. We intercept the temp file by overriding the create step:
        // since write_token_file removes the temp on success via rename, we
        // assert on a freshly created temp directly through write_token_tmp.
        let dir = TempDir::new()?;
        let tmp = dir.path().join(".token.tmp-test");
        write_token_tmp(&tmp, "secret", 0o600)?;
        assert_eq!(mode_of(&tmp), 0o600);
        assert_eq!(fs::read_to_string(&tmp)?, "secret");

        // And the public path: the final file equals the requested mode and no
        // temp remains.
        let path = dir.path().join("identity/token");
        write_token_file(&path, "secret", 0o600)?;
        assert_eq!(mode_of(&path), 0o600);
        let leftovers: Vec<_> = fs::read_dir(path.parent().unwrap())?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() != "token")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
        Ok(())
    }

    #[test]
    fn temp_file_is_removed_after_a_mid_write_failure() -> R<()> {
        // Simulate a mid-write failure by pre-creating the temp file so
        // `create_new` fails; write_token_tmp must error and write_token_file
        // must leave no stray temp behind. We reproduce the temp naming by
        // calling write_token_tmp directly on an existing path, then assert the
        // public API cleans up by checking the directory has no leftovers after
        // a failing write into a path whose parent is a file (forcing rename to
        // fail) is not portable; instead assert create_new semantics directly.
        let dir = TempDir::new()?;
        let tmp = dir.path().join(".token.tmp-existing");
        fs::write(&tmp, "stale")?;
        // create_new must refuse to clobber → error.
        assert!(write_token_tmp(&tmp, "secret", 0o400).is_err());

        // Now prove write_token_file removes its temp on a rename failure: make
        // the destination's parent read-only so rename fails after the temp is
        // created, then assert no temp file with our prefix survives.
        let target_dir = dir.path().join("sub");
        fs::create_dir_all(&target_dir)?;
        let path = target_dir.join("token");
        // Pre-create the destination as a directory so the rename of a file
        // over a non-empty directory fails on all platforms.
        fs::create_dir_all(&path)?;
        fs::write(path.join("keep"), "x")?;
        let err = write_token_file(&path, "secret", 0o400);
        assert!(err.is_err(), "expected rename to fail onto a directory");
        let leftovers: Vec<_> = fs::read_dir(&target_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".token.tmp-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp file not cleaned after failure: {leftovers:?}"
        );
        Ok(())
    }

    #[test]
    fn fallback_token_path_sanitizes_traversal_names() {
        // A traversal name must not escape the per-alloc identity directory.
        let p = fallback_token_path(7, "../../tmp/evil");
        let s = p.to_string_lossy();
        assert!(!s.contains(".."), "path still contains traversal: {s}");
        // Absolute (rooted at cwd) but still under the identity token dir.
        assert!(s.contains(IDENTITY_TOKEN_DIR), "{s}");
        assert!(s.ends_with("token"), "{s}");
        // A valid name is preserved verbatim.
        let ok = fallback_token_path(7, "vault");
        assert!(ok.to_string_lossy().contains("/vault/"), "{ok:?}");
    }

    #[test]
    fn custom_file_mode_is_applied() -> R<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("token");
        let identity = WorkloadIdentity {
            file: Some(WorkloadIdentityFile {
                path: path.to_string_lossy().to_string(),
                mode: Some("0440".into()),
            }),
            ..file_identity(&path, ChangeMode::Noop)
        };
        write_identity_files(&identity, 1, "tok")?;
        #[cfg(unix)]
        assert_eq!(mode_of(&path), 0o440);
        assert_eq!(fs::read_to_string(&path)?, "tok");
        Ok(())
    }

    #[tokio::test]
    async fn prepare_injects_env_and_writes_files() -> R<()> {
        let dir = TempDir::new()?;
        let token_path = dir.path().join("aws-token");
        let identities = vec![
            file_identity(&token_path, ChangeMode::Noop),
            WorkloadIdentity {
                name: "vault".into(),
                audience: vec!["vault.internal".into()],
                env: Some("VAULT_WORKLOAD_TOKEN".into()),
                change_mode: ChangeMode::Signal,
                change_signal: Some("SIGHUP".into()),
                ..Default::default()
            },
        ];

        let manager = Arc::new(WorkerIdentityManager::new(FakeIssuer::new(900)));
        let mut allocation = TaskAllocation {
            allocation_id: 7,
            job_id: "job".into(),
            job_config_id: "jc".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "api".into(),
            task_kind: "exec".into(),
            task_config: json!({"command": ["sleep", "1"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: serde_json::to_string(&identities)?,
            self_terminate_after_seconds: 0,
            task_artifacts: String::new(),

            network: String::new(),
            task_connect: String::new(),
        };

        let prepared = manager.prepare_task_allocation(&mut allocation).await?;
        assert_eq!(prepared.len(), 2);

        // File identity: token written at the declared path.
        assert!(fs::read_to_string(&token_path)?.starts_with("tok-"));

        // Env identity: token in the env map, plus a *_FILE companion
        // pointing at the well-known fallback file (which is also written).
        let config: Value = serde_json::from_str(&allocation.task_config)?;
        let env = config["env"].as_object().context("env object")?;
        assert!(env["VAULT_WORKLOAD_TOKEN"]
            .as_str()
            .unwrap()
            .starts_with("tok-"));
        let fallback = fallback_token_path(7, "vault");
        assert_eq!(
            env["VAULT_WORKLOAD_TOKEN_FILE"].as_str().unwrap(),
            fallback.to_string_lossy()
        );
        assert!(fs::read_to_string(&fallback)?.starts_with("tok-"));
        #[cfg(unix)]
        assert_eq!(mode_of(&fallback), 0o400);
        fs::remove_dir_all(fallback.parent().unwrap().parent().unwrap()).ok();
        Ok(())
    }

    #[tokio::test]
    async fn prepare_is_inert_when_manager_has_feature_disabled() -> R<()> {
        let issuer = Arc::new(FakeIssuer {
            calls: AtomicU64::new(0),
            ttl_secs: 900,
            disabled: true,
        });
        let manager = Arc::new(WorkerIdentityManager::new(issuer));
        let identities = vec![WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            env: Some("AWS_TOKEN".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        }];
        let mut allocation = TaskAllocation {
            allocation_id: 8,
            job_id: "job".into(),
            job_config_id: "jc".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "api".into(),
            task_kind: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: serde_json::to_string(&identities)?,
            self_terminate_after_seconds: 0,
            task_artifacts: String::new(),

            network: String::new(),
            task_connect: String::new(),
        };

        let prepared = manager.prepare_task_allocation(&mut allocation).await?;
        assert!(prepared.is_empty());
        // No env injection happened.
        let config: Value = serde_json::from_str(&allocation.task_config)?;
        assert!(config.get("env").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn refresh_loop_reissues_before_expiry_and_rewrites_file() -> R<()> {
        let dir = TempDir::new()?;
        let token_path = dir.path().join("token");
        let task_manager = task_manager(&dir);

        // A real (short-lived) exec allocation so the loop sees it as live.
        let allocation = alloc(11);
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..TaskConfig::new("api", "exec")
        };
        task_manager
            .lock()
            .unwrap()
            .run_allocation(&allocation, &task)?;

        let issuer = FakeIssuer::new(2); // 2s TTL → refresh roughly every 1s
        let manager = Arc::new(WorkerIdentityManager::new(issuer.clone()));
        let identity = file_identity(&token_path, ChangeMode::Noop);
        write_identity_files(&identity, 11, "tok-0")?;
        manager.spawn_refreshers(
            11,
            vec![PreparedIdentity {
                identity,
                expires_at: unix_now() + 2,
            }],
            task_manager.clone(),
        );

        // Within ~3.5s the loop must have re-issued at least twice and the
        // file must carry the newest token.
        let deadline = std::time::Instant::now() + Duration::from_secs(6);
        loop {
            if issuer.calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "refresh loop only issued {} tokens before deadline",
                    issuer.calls.load(Ordering::SeqCst)
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let content = fs::read_to_string(&token_path)?;
        assert!(
            content.starts_with("tok-"),
            "unexpected content {content:?}"
        );
        assert_ne!(content, "tok-0");

        // Stopping the allocation stops the loop (retain with no live allocs).
        manager.retain(&[]);
        let calls_after_stop = issuer.calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(issuer.calls.load(Ordering::SeqCst), calls_after_stop);

        task_manager.lock().unwrap().reconcile_allocations(&[]);
        Ok(())
    }

    #[tokio::test]
    async fn change_mode_noop_does_not_restart_the_task() -> R<()> {
        let dir = TempDir::new()?;
        let token_path = dir.path().join("token");
        let task_manager = task_manager(&dir);
        let allocation = alloc(12);
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..TaskConfig::new("api", "exec")
        };
        task_manager
            .lock()
            .unwrap()
            .run_allocation(&allocation, &task)?;
        let pid_before = task_manager.lock().unwrap().allocation_pid(12);

        let identity = file_identity(&token_path, ChangeMode::Noop);
        apply_refreshed_token(&identity, 12, "tok-new", &task_manager)?;

        assert_eq!(fs::read_to_string(&token_path)?, "tok-new");
        assert_eq!(task_manager.lock().unwrap().allocation_pid(12), pid_before);
        task_manager.lock().unwrap().reconcile_allocations(&[]);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn change_mode_signal_delivers_change_signal_to_the_process() -> R<()> {
        let dir = TempDir::new()?;
        let marker = dir.path().join("got-hup");
        let token_path = dir.path().join("token");
        let task_manager = task_manager(&dir);

        // A shell that traps SIGHUP and writes a marker file.
        let script = format!(
            "trap 'touch {}' HUP; for i in $(seq 1 100); do sleep 0.1; done",
            marker.display()
        );
        let allocation = alloc(13);
        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", script]}),
            ..TaskConfig::new("api", "exec")
        };
        task_manager
            .lock()
            .unwrap()
            .run_allocation(&allocation, &task)?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let identity = WorkloadIdentity {
            change_mode: ChangeMode::Signal,
            change_signal: Some("SIGHUP".into()),
            ..file_identity(&token_path, ChangeMode::Signal)
        };
        apply_refreshed_token(&identity, 13, "tok-new", &task_manager)?;

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !marker.exists() {
            if std::time::Instant::now() > deadline {
                panic!("task never observed SIGHUP");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(fs::read_to_string(&token_path)?, "tok-new");
        task_manager.lock().unwrap().reconcile_allocations(&[]);
        Ok(())
    }

    #[tokio::test]
    async fn change_mode_restart_restarts_with_the_fresh_env_token() -> R<()> {
        let dir = TempDir::new()?;
        let task_manager = task_manager(&dir);
        let allocation = alloc(14);
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"], "env": {"AWS_TOKEN": "tok-old"}}),
            ..TaskConfig::new("api", "exec")
        };
        task_manager
            .lock()
            .unwrap()
            .run_allocation(&allocation, &task)?;
        let pid_before = task_manager.lock().unwrap().allocation_pid(14);
        assert!(pid_before.is_some());

        let identity = WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            env: Some("AWS_TOKEN".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        };
        apply_refreshed_token(&identity, 14, "tok-new", &task_manager)?;

        let guard = task_manager.lock().unwrap();
        let pid_after = guard.allocation_pid(14);
        assert!(pid_after.is_some());
        assert_ne!(pid_after, pid_before, "restart must relaunch the process");
        // The stored config now carries the fresh token, so the relaunched
        // process (and any future restart) sees it.
        let stored_env = guard.task_env(14, "AWS_TOKEN");
        assert_eq!(stored_env.as_deref(), Some("tok-new"));
        drop(guard);
        // Fallback file backs the env delivery for future re-reads.
        let fallback = fallback_token_path(14, "aws");
        assert_eq!(fs::read_to_string(&fallback)?, "tok-new");
        fs::remove_dir_all(fallback.parent().unwrap().parent().unwrap()).ok();
        task_manager.lock().unwrap().reconcile_allocations(&[]);
        Ok(())
    }

    #[test]
    fn refresh_delay_is_half_ttl_with_floor() {
        let now = 1_000_000;
        // 900s TTL → ~450s minus up to 20% jitter.
        let delay = refresh_delay(now + 900, now);
        assert!(delay >= Duration::from_secs(360), "{delay:?}");
        assert!(delay <= Duration::from_secs(450), "{delay:?}");
        // Expired/short TTLs floor at 1s.
        assert_eq!(refresh_delay(now, now), Duration::from_secs(1));
        assert_eq!(refresh_delay(0, now), Duration::from_secs(1));
    }

    #[test]
    fn parse_identities_handles_empty_and_invalid() {
        assert!(parse_identities("").unwrap().is_empty());
        assert!(parse_identities("[]").unwrap().is_empty());
        assert!(parse_identities("not json").is_err());
    }
}
