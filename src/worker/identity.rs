//! Worker-side workload-identity delivery and refresh.
//!
//! The worker — never the task, and never the scheduler's desired-state
//! payload — fetches tokens from the manager (`IssueWorkloadIdentityToken`)
//! and materializes them for the task:
//!
//! - **env delivery**: the token is injected into the task’s runtime config
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
//! When a fork or snapshot of an allocation's instance is captured, the
//! manager raises the instance's identity epoch and refuses its older
//! tokens (copies of them sit in the checkpoint). The desired set carries the
//! epoch (`TaskAllocation.identity_epoch`), and a rise wakes the allocation's
//! refresh loops at once, so the running instance gets a current token within
//! a heartbeat rather than at its next half-TTL refresh.
//!
//! If the manager has workload identity disabled, issuance returns
//! FailedPrecondition; the worker logs one warning and treats the task's
//! identities as inert (the task still starts, with no tokens delivered).
//!
//! Token bytes are never logged (see AGENTS.md tracing rules).

use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use crate::structs::workload_identity::{
    ChangeMode, WorkloadIdentity, IDENTITY_TOKEN_FILE_ENV, MANAGER_ADDR_ENV, SECRETS_DIR_ENV,
    SECRETS_SUBDIR,
};
use crate::structs::{IssueWorkloadIdentityTokenRequest, TaskAllocation};
use crate::util::random_string;
use crate::worker::api_socket::{AllocationSocket, ApiSocketContext};
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

/// Subdirectory of the data dir (`LEGION_DATA_DIR`, else `./.legion`) for the
/// well-known per-identity token files that back env-delivered identities.
pub const IDENTITY_TOKEN_SUBDIR: &str = "identity";

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
    /// Where this allocation's file destinations resolve on the host, for a
    /// runtime that mounts its task directory elsewhere (see
    /// [`FileDeliveryBase`]). Carried so the refresh loop rewrites the same
    /// file the launch wrote.
    pub delivery_base: Option<FileDeliveryBase>,
}

/// A runtime's file-delivery mapping: the root a jobspec's paths are declared
/// against (`/legion/task`) and the host directory it is mounted from.
///
/// An `in_task_root` of `None` means the runtime's filesystem *is* the
/// host's (the `exec` runtime): a declared absolute path is already the real
/// path, and a relative one is below `host_task_dir`, the task directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDeliveryBase {
    pub in_task_root: Option<&'static str>,
    pub host_task_dir: PathBuf,
    /// Whether a unix socket the worker binds under `host_task_dir` is one
    /// the task can connect to: true where the task directory is the
    /// task's own (`exec`, and `container` through its mount), false for a
    /// sandbox guest, which sees only copied-in files.
    pub host_sockets: bool,
}

impl FileDeliveryBase {
    /// `relative` (below the delivery root) as the task sees it: under the
    /// runtime's in-task root, or the host path for a runtime that runs on
    /// the host's filesystem.
    pub fn in_task_path(&self, relative: &Path) -> PathBuf {
        match self.in_task_root {
            Some(root) => Path::new(root).join(relative),
            None => self.host_task_dir.join(relative),
        }
    }
}

/// Where a token file goes: below a task directory the task can write (so
/// it is written without following links, [`crate::util::write_file_beneath`]),
/// or a path the task cannot touch.
enum TokenFile {
    Beneath { root: PathBuf, relative: PathBuf },
    Path(PathBuf),
}

impl TokenFile {
    fn host_path(&self) -> PathBuf {
        match self {
            TokenFile::Beneath { root, relative } => root.join(relative),
            TokenFile::Path(path) => path.clone(),
        }
    }
}

/// Owns token delivery and the per-allocation refresh loops on a worker.
pub struct WorkerIdentityManager {
    issuer: Arc<dyn TokenIssuer>,
    refreshers: Mutex<HashMap<i64, Vec<JoinHandle<()>>>>,
    /// The identity epoch last seen for each allocation, as a watch its
    /// refresh loops wait on beside their timer
    /// ([`WorkerIdentityManager::observe_identity_epochs`]).
    epochs: Mutex<HashMap<i64, tokio::sync::watch::Sender<u64>>>,
    /// Backing token file(s) delivered per allocation, so they can be removed
    /// when the allocation stops (short-TTL hygiene: live token material must
    /// not linger on disk past the workload's life).
    delivered_files: Mutex<HashMap<i64, Vec<PathBuf>>>,
    warned_disabled: AtomicBool,
    /// How allocations' API sockets reach the managers; `None` serves none
    /// (tests, and workers built without a manager channel).
    api_socket_context: Option<ApiSocketContext>,
    /// The API socket of each allocation that declares the `legion`
    /// identity, on a runtime that can reach one.
    api_sockets: Mutex<HashMap<i64, AllocationSocket>>,
}

impl WorkerIdentityManager {
    pub fn new(issuer: Arc<dyn TokenIssuer>) -> Self {
        Self {
            issuer,
            refreshers: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            delivered_files: Mutex::new(HashMap::new()),
            warned_disabled: AtomicBool::new(false),
            api_socket_context: None,
            api_sockets: Mutex::new(HashMap::new()),
        }
    }

    /// Serve a per-allocation API socket to every allocation that declares
    /// the `legion` identity (docs/workload-identity.md, "The API socket").
    pub fn with_api_sockets(mut self, context: ApiSocketContext) -> Self {
        self.api_socket_context = Some(context);
        self
    }

    /// Start (or restart) `alloc_id`'s API socket, when this worker serves
    /// them and the runtime can reach one. `token` is the allocation's
    /// current `legion` token when the worker holds one; a restored
    /// allocation's refresher fills it in on its first re-issue.
    pub fn serve_api_socket(
        &self,
        alloc_id: i64,
        delivery_base: Option<&FileDeliveryBase>,
        token: Option<String>,
    ) -> Option<PathBuf> {
        let context = self.api_socket_context.clone()?;
        let base = delivery_base.filter(|base| base.host_sockets)?;
        // An earlier socket for the allocation (a relaunch) goes first: its
        // drop removes the file the new one binds.
        self.stop_api_socket(alloc_id);
        match crate::worker::api_socket::serve(context, alloc_id, &base.host_task_dir, token) {
            Ok(socket) => {
                self.api_sockets
                    .lock()
                    .expect("identity api sockets lock")
                    .insert(alloc_id, socket);
                Some(base.in_task_path(
                    &Path::new(SECRETS_SUBDIR).join(crate::worker::api_socket::API_SOCKET_NAME),
                ))
            }
            Err(err) => {
                warn!(
                    allocation_id = alloc_id,
                    error = ?err,
                    "Failed to serve the allocation's API socket; the task can still use its \
                     token file"
                );
                None
            }
        }
    }

    /// The token cell of `alloc_id`'s API socket, for its `legion` identity's
    /// refresh loop to keep current.
    fn api_socket_token(&self, alloc_id: i64) -> Option<Arc<std::sync::RwLock<Option<String>>>> {
        self.api_sockets
            .lock()
            .expect("identity api sockets lock")
            .get(&alloc_id)
            .map(AllocationSocket::token_cell)
    }

    fn stop_api_socket(&self, alloc_id: i64) {
        let removed = self
            .api_sockets
            .lock()
            .expect("identity api sockets lock")
            .remove(&alloc_id);
        if removed.is_some() {
            debug!(
                allocation_id = alloc_id,
                "Stopped the allocation's API socket"
            );
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
        delivery_base: Option<FileDeliveryBase>,
    ) -> R<Vec<PreparedIdentity>> {
        let identities = parse_identities(&allocation.task_identities)?;
        if identities.is_empty() {
            return Ok(Vec::new());
        }

        let alloc_id = allocation.allocation_id;
        let mut prepared = Vec::new();
        let mut env_injections: Vec<(String, String)> = Vec::new();
        let mut control_plane_token: Option<String> = None;

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

            write_identity_files(&identity, alloc_id, &issued.token, delivery_base.as_ref())?;
            if let Some(backing) = backing_file_path(&identity, alloc_id, delivery_base.as_ref()) {
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
                // fresh token directly into env) and for runtimes whose filesystem
                // the host path doesn't reach — so only emit it when a signal
                // handler would actually use it.
                if identity.change_mode == ChangeMode::Signal {
                    if let Some(backing) =
                        backing_file_path(&identity, alloc_id, delivery_base.as_ref())
                    {
                        // For a runtime that mounts its task dir elsewhere,
                        // the var must name the path the *task* sees. A
                        // backing file outside that mount is unreadable from
                        // inside, so the companion is omitted rather than
                        // pointing at a path that does not exist there
                        // (env-delivered identities on such a runtime back
                        // onto the worker's own directory — use file delivery
                        // if the workload needs to re-read the token).
                        let in_task = match delivery_base
                            .as_ref()
                            .and_then(|b| Some((b.in_task_root?, b)))
                        {
                            None => Some(backing.to_string_lossy().to_string()),
                            Some((in_task_root, base)) => backing
                                .strip_prefix(&base.host_task_dir)
                                .ok()
                                .map(|relative| {
                                    PathBuf::from(in_task_root)
                                        .join(relative)
                                        .to_string_lossy()
                                        .to_string()
                                }),
                        };
                        if let Some(path) = in_task {
                            env_injections.push((format!("{env_key}_FILE"), path));
                        }
                    }
                }
            }

            info!(
                identity = %identity.name,
                expires_at = issued.expires_at,
                "Delivered workload identity token"
            );
            if identity.is_control_plane() {
                control_plane_token = Some(issued.token.clone());
            }
            prepared.push(PreparedIdentity {
                identity,
                expires_at: issued.expires_at,
                delivery_base: delivery_base.clone(),
            });
        }

        if !env_injections.is_empty() {
            inject_env(&mut allocation.task_config, &env_injections)
                .context("failed to inject identity env vars into task config")?;
        }

        // The `legion` identity is also the task's way back to Legion: its
        // API socket and the ambient env that points at it.
        if let Some(token) = control_plane_token {
            let identity = prepared
                .iter()
                .find(|prepared| prepared.identity.is_control_plane())
                .map(|prepared| prepared.identity.clone())
                .expect("a control-plane token came from a prepared identity");
            let socket = self.serve_api_socket(alloc_id, delivery_base.as_ref(), Some(token));
            let ambient = ambient_env(&identity, delivery_base.as_ref(), socket.as_deref());
            inject_env_defaults(&mut allocation.task_config, &ambient)
                .context("failed to inject the legion identity's env vars into task config")?;
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
        let epoch = self.epoch_sender(alloc_id);
        let mut handles = Vec::new();
        for entry in prepared {
            let issuer = self.issuer.clone();
            let task_manager = task_manager.clone();
            let epoch = epoch.subscribe();
            // The `legion` identity's fresh token also goes to the API
            // socket, which attaches it to the task's calls.
            let socket_token = entry
                .identity
                .is_control_plane()
                .then(|| self.api_socket_token(alloc_id))
                .flatten();
            handles.push(tokio::spawn(async move {
                refresh_loop(
                    issuer,
                    alloc_id,
                    entry.identity,
                    entry.expires_at,
                    entry.delivery_base,
                    task_manager,
                    epoch,
                    socket_token,
                )
                .await;
            }));
        }
        let mut refreshers = self.refreshers.lock().expect("identity refreshers lock");
        refreshers.entry(alloc_id).or_default().extend(handles);
    }

    /// The epoch watch for one allocation, created at epoch 0 when the
    /// desired set has not shown it yet.
    fn epoch_sender(&self, alloc_id: i64) -> tokio::sync::watch::Sender<u64> {
        self.epochs
            .lock()
            .expect("identity epochs lock")
            .entry(alloc_id)
            .or_insert_with(|| tokio::sync::watch::channel(0).0)
            .clone()
    }

    /// Record each desired allocation's identity epoch
    /// (`TaskAllocation.identity_epoch`). A rise wakes that allocation's
    /// refresh loops to re-issue now: the manager refuses its older tokens
    /// once a fork or snapshot of its instance is captured. The first
    /// sighting only records, since the tokens the allocation started with
    /// were issued at that epoch or later. Call it before
    /// [`WorkerIdentityManager::spawn_refreshers`] for the same reply.
    pub fn observe_identity_epochs(&self, allocations: impl IntoIterator<Item = (i64, u64)>) {
        let mut epochs = self.epochs.lock().expect("identity epochs lock");
        for (alloc_id, epoch) in allocations {
            match epochs.entry(alloc_id) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let raised = entry.get().send_if_modified(|current| {
                        let raised = epoch > *current;
                        if raised {
                            *current = epoch;
                        }
                        raised
                    });
                    if raised {
                        info!(
                            allocation_id = alloc_id,
                            identity_epoch = epoch,
                            "Instance identity epoch raised; re-issuing its tokens now"
                        );
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(tokio::sync::watch::channel(epoch).0);
                }
            }
        }
    }

    /// Stop refresh loops for every allocation not in `live` (called after
    /// each reconcile so loops never outlive their allocation).
    pub fn retain(&self, live: &[i64]) {
        let live: std::collections::HashSet<i64> = live.iter().copied().collect();
        self.epochs
            .lock()
            .expect("identity epochs lock")
            .retain(|alloc_id, _| live.contains(alloc_id));
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
        let stale: Vec<i64> = self
            .api_sockets
            .lock()
            .expect("identity api sockets lock")
            .keys()
            .filter(|alloc_id| !live.contains(alloc_id))
            .copied()
            .collect();
        for alloc_id in stale {
            self.stop_api_socket(alloc_id);
        }
    }

    /// Stop refresh loops for one allocation.
    pub fn stop(&self, alloc_id: i64) {
        self.epochs
            .lock()
            .expect("identity epochs lock")
            .remove(&alloc_id);
        {
            let mut refreshers = self.refreshers.lock().expect("identity refreshers lock");
            if let Some(handles) = refreshers.remove(&alloc_id) {
                for handle in handles {
                    handle.abort();
                }
            }
        }
        self.stop_api_socket(alloc_id);
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
        let fallback_dir = identity_token_root().join(alloc_id.to_string());
        if let Err(err) = fs::remove_dir_all(&fallback_dir) {
            if err.kind() != std::io::ErrorKind::NotFound {
                debug!(allocation_id = alloc_id, error = ?err, "Failed to remove identity fallback dir");
            }
        }
    }
}

/// Parse the JSON-encoded identity list off a [`TaskAllocation`].
/// The identities the worker delivers to the task (env or file). One that is
/// only named by a `connect.upstreams` credential is left out: the manager
/// mints it for the node's forwarder through `FetchDatapathMaterial`, and
/// nothing is written into the task for it.
pub fn parse_identities(raw: &str) -> R<Vec<WorkloadIdentity>> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let identities: Vec<WorkloadIdentity> =
        serde_json::from_str(raw).context("invalid task identities JSON")?;
    Ok(identities
        .into_iter()
        .filter(WorkloadIdentity::is_delivered)
        .map(WorkloadIdentity::with_default_delivery)
        .collect())
}

/// The env the `legion` identity gives a task, in the task's own view
/// (docs/workload-identity.md, "Ambient env"):
///
/// - `LEGION_SECRETS_DIR`: where its token file and API socket live;
/// - `LEGION_IDENTITY_TOKEN_FILE`: its token file, for clients that cannot
///   use a socket;
/// - `LEGION_ADDR=unix://<secrets>/legion.sock`, when the worker serves the
///   allocation a socket (`api_socket`, already in the task's view).
///
/// Nothing for a runtime the worker has no delivery mapping for.
pub fn ambient_env(
    identity: &WorkloadIdentity,
    delivery_base: Option<&FileDeliveryBase>,
    api_socket: Option<&Path>,
) -> Vec<(String, String)> {
    let Some(base) = delivery_base else {
        return Vec::new();
    };
    let mut vars = vec![(
        SECRETS_DIR_ENV.to_string(),
        base.in_task_path(Path::new(SECRETS_SUBDIR))
            .display()
            .to_string(),
    )];
    if let Some(file) = identity
        .file
        .as_ref()
        .filter(|file| !file.path.trim().is_empty())
    {
        let declared = Path::new(file.path.trim());
        let in_task = if declared.is_absolute() {
            declared.to_path_buf()
        } else {
            base.in_task_path(declared)
        };
        vars.push((
            IDENTITY_TOKEN_FILE_ENV.to_string(),
            in_task.display().to_string(),
        ));
    }
    if let Some(socket) = api_socket {
        vars.push((
            MANAGER_ADDR_ENV.to_string(),
            format!("unix://{}", socket.display()),
        ));
    }
    vars
}

/// Where the refreshable copy of this identity's token lives: the declared
/// `file.path` when file delivery is configured, else the well-known fallback
/// for env-delivered identities.
fn backing_file_path(
    identity: &WorkloadIdentity,
    alloc_id: i64,
    delivery_base: Option<&FileDeliveryBase>,
) -> Option<PathBuf> {
    backing_file(identity, alloc_id, delivery_base).map(|file| file.host_path())
}

fn backing_file(
    identity: &WorkloadIdentity,
    alloc_id: i64,
    delivery_base: Option<&FileDeliveryBase>,
) -> Option<TokenFile> {
    if let Some(file) = identity.file.as_ref().filter(|f| !f.path.trim().is_empty()) {
        let declared = file.path.trim();
        let Some(base) = delivery_base else {
            return Some(TokenFile::Path(PathBuf::from(declared)));
        };
        // A runtime that mounts its task dir declares destinations against
        // the mount point, so the host path is the task dir plus whatever
        // the jobspec put under it. Re-resolved here rather than trusted
        // from admission — the same two-layer stance identity *naming*
        // already takes — so a malformed spec cannot write outside the tree.
        // On the host's own filesystem an absolute path is the real one, and
        // a relative path is below the task directory, not the worker's cwd.
        let resolved = match base.in_task_root {
            None if Path::new(declared).is_absolute() => {
                return Some(TokenFile::Path(PathBuf::from(declared)))
            }
            None if Path::new(declared)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir)) =>
            {
                Err(format!("{declared:?} climbs out of the task directory"))
            }
            None => Ok(PathBuf::from(declared)),
            Some(root) => crate::task_plugins::resolve_task_dir_destination(root, declared),
        };
        return match resolved {
            Ok(relative) => Some(TokenFile::Beneath {
                root: base.host_task_dir.clone(),
                relative,
            }),
            Err(err) => {
                warn!(
                    identity = %identity.name,
                    error = %err,
                    "Identity file path is outside the task directory; not delivering it"
                );
                None
            }
        };
    }
    if identity.delivers_env() {
        return Some(TokenFile::Path(fallback_token_path(
            alloc_id,
            &identity.name,
        )));
    }
    None
}

/// Well-known per-identity token path backing env-delivered identities:
/// `<data dir>/identity/<alloc>/<name>/token`, **absolute** (a relative data
/// dir is rooted at the worker's working directory).
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
    identity_token_root()
        .join(alloc_id.to_string())
        .join(safe_name)
        .join("token")
}

/// `<data dir>/identity`, made absolute against the worker's cwd.
fn identity_token_root() -> PathBuf {
    let root = data_dir().join(IDENTITY_TOKEN_SUBDIR);
    if root.is_absolute() {
        return root;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(root),
        Err(_) => root,
    }
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
fn write_identity_files(
    identity: &WorkloadIdentity,
    alloc_id: i64,
    token: &str,
    delivery_base: Option<&FileDeliveryBase>,
) -> R<()> {
    let Some(file) = backing_file(identity, alloc_id, delivery_base) else {
        return Ok(());
    };
    let mode = identity
        .file
        .as_ref()
        .and_then(|f| f.mode.as_deref())
        .map(crate::structs::workload_identity::parse_file_mode)
        .transpose()?
        .unwrap_or(DEFAULT_TOKEN_FILE_MODE);
    match file {
        // Refreshed while the task runs, in a directory it can write.
        TokenFile::Beneath { root, relative } => {
            crate::util::write_file_beneath(&root, &relative, token.as_bytes(), mode)
        }
        TokenFile::Path(path) => write_token_file(&path, token, mode),
    }
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

/// Inject env vars into a JSON-encoded task runtime config (the same `env` map
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

/// [`inject_env`], leaving any variable the task's own config already sets:
/// a job that points `LEGION_ADDR` somewhere on purpose keeps it.
fn inject_env_defaults(task_config_json: &mut String, vars: &[(String, String)]) -> R<()> {
    let config: Value =
        serde_json::from_str(task_config_json).context("task config is not valid JSON")?;
    let declared = config.get("env").and_then(Value::as_object);
    let missing: Vec<(String, String)> = vars
        .iter()
        .filter(|(key, _)| declared.is_none_or(|env| !env.contains_key(key)))
        .cloned()
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    inject_env(task_config_json, &missing)
}

/// An app task's `LEGION_APP_CONFIG` (docs/apps.md, "Config"), made a path
/// the task can open. The app reconciler renders the app's config as a file
/// below the task directory and names it relatively; this resolves it to
/// the task's own view of that directory (the host task directory for
/// `exec`, the mount for `container`). An absolute value, and a task with no
/// delivery mapping, are left alone. Runs for every task that sets the
/// variable, whether or not workload identity is on.
pub fn resolve_app_config_env(
    task_config_json: &mut String,
    delivery_base: Option<&FileDeliveryBase>,
) -> R<()> {
    let Some(base) = delivery_base else {
        return Ok(());
    };
    let config: Value = match serde_json::from_str(task_config_json) {
        Ok(config) => config,
        Err(_) => return Ok(()),
    };
    let Some(relative) = config
        .get("env")
        .and_then(|env| env.get(crate::structs::app::APP_CONFIG_ENV))
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty() && !Path::new(path).is_absolute())
    else {
        return Ok(());
    };
    if Path::new(relative)
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        bail!(
            "{} {relative:?} climbs out of the task directory",
            crate::structs::app::APP_CONFIG_ENV
        );
    }
    let resolved = base.in_task_path(Path::new(relative));
    inject_env(
        task_config_json,
        &[(
            crate::structs::app::APP_CONFIG_ENV.to_string(),
            resolved.display().to_string(),
        )],
    )
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

/// The long-running refresh loop for one identity on one allocation. It
/// wakes at about half the token's remaining TTL, or at once when the
/// allocation's identity epoch rises (`epoch`). The `legion` identity's
/// loop also hands each fresh token to the allocation's API socket
/// (`socket_token`).
#[allow(clippy::too_many_arguments)]
async fn refresh_loop(
    issuer: Arc<dyn TokenIssuer>,
    alloc_id: i64,
    identity: WorkloadIdentity,
    mut expires_at: u64,
    delivery_base: Option<FileDeliveryBase>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    mut epoch: tokio::sync::watch::Receiver<u64>,
    socket_token: Option<Arc<std::sync::RwLock<Option<String>>>>,
) {
    let mut epoch_open = true;
    loop {
        let delay = refresh_delay(expires_at, unix_now());
        if epoch_open {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = epoch.changed() => {
                    // A closed watch (the allocation was pruned) never
                    // fires again: fall back to the timer alone.
                    epoch_open = changed.is_ok();
                    if !epoch_open {
                        continue;
                    }
                }
            }
        } else {
            tokio::time::sleep(delay).await;
        }

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
        if let Some(cell) = &socket_token {
            if let Ok(mut current) = cell.write() {
                *current = Some(issued.token.clone());
            }
        }

        match apply_refreshed_token(
            &identity,
            alloc_id,
            &issued.token,
            delivery_base.as_ref(),
            &task_manager,
        ) {
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
    delivery_base: Option<&FileDeliveryBase>,
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
) -> R<()> {
    write_identity_files(identity, alloc_id, token, delivery_base)?;

    let mut manager = task_manager
        .lock()
        .map_err(|_| anyhow!("task manager lock poisoned"))?;

    if let Some(env_key) = identity.env.as_deref().filter(|e| !e.trim().is_empty()) {
        // A live process's env cannot change; this updates the *stored*
        // config so signal handlers read the backing file (see module docs)
        // and any restart starts with the fresh token.
        manager.update_task_env(alloc_id, env_key, token)?;
    }
    // A runtime with an isolated guest (a sandbox) copies the fresh token in
    // itself; for the rest the host file just written is what the task reads.
    if identity.change_mode != ChangeMode::Restart {
        manager.identity_tokens_refreshed(alloc_id)?;
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

    /// Issues a token that names the allocation it was issued for, as the
    /// manager's `legion.io/alloc` claim does.
    struct AllocNamingIssuer;

    #[tonic::async_trait]
    impl TokenIssuer for AllocNamingIssuer {
        async fn issue(
            &self,
            alloc_id: i64,
            _identity_name: &str,
        ) -> R<Option<IssuedIdentityToken>> {
            Ok(Some(IssuedIdentityToken {
                token: format!("token-for-alloc-{alloc_id}"),
                expires_at: unix_now() + 900,
            }))
        }
    }

    /// Suspend plan §6.12: a restored guest's token must name the NEW
    /// allocation. The resume allocation's tokens are issued before the
    /// restore like any start's, and the identity disk the guest re-reads
    /// carries them, with the new allocation id.
    #[tokio::test]
    async fn a_restored_guest_gets_a_token_naming_the_new_allocation() -> R<()> {
        let identities = vec![WorkloadIdentity {
            name: "acme".into(),
            audience: vec!["acme-api".into()],
            env: Some("ACME_TOKEN".into()),
            ..Default::default()
        }];
        let manager = Arc::new(WorkerIdentityManager::new(Arc::new(AllocNamingIssuer)));
        // The resume runs as allocation 52; the suspended one was 51.
        let mut allocation = TaskAllocation {
            allocation_id: 52,
            job_id: "agents".into(),
            task_name: "workspace".into(),
            task_runtime: "sandbox".into(),
            task_config: json!({ "image": "alpine", "command": ["sleep", "1"] }).to_string(),
            task_identities: serde_json::to_string(&identities)?,
            ..Default::default()
        };
        manager
            .prepare_task_allocation(&mut allocation, None)
            .await?;
        let (alloc, task) = crate::worker::task_manager::task_and_allocation(&allocation)?;
        let restore = crate::structs::SnapshotRestore {
            snapshot_id: "snap-suspend".into(),
            kind: "memory".into(),
            manifest_json: "{}".into(),
            on_resume: vec!["/bin/reconnect".into()],
            ..Default::default()
        };
        let identity = crate::worker::checkpoints::resume_identity(&alloc, &task, &restore);
        assert_eq!(identity.alloc_id, 52);
        assert_eq!(identity.token.as_deref(), Some("token-for-alloc-52"));
        assert_eq!(
            identity.env.get("ACME_TOKEN").map(String::as_str),
            Some("token-for-alloc-52")
        );
        assert_eq!(
            identity.env.get("LEGION_ALLOC_ID").map(String::as_str),
            Some("52")
        );
        assert_eq!(identity.hostname, "legion-sbx-52");
        assert_eq!(identity.on_resume, vec!["/bin/reconnect".to_string()]);
        assert!(identity.host_time_unix_ms > 0);
        assert_eq!(identity.entropy.len(), 64);
        Ok(())
    }

    /// Phase 6 (plan §9): three forks of one checkpoint diverge in
    /// everything that names them. Each restores as its own allocation, so
    /// it gets its own token (the `legion.io/alloc` claim names it), its own
    /// instance id, hostname, MAC, and entropy. The TAP each one gets is
    /// covered in `sandbox_checkpoint`.
    #[tokio::test]
    async fn three_forks_get_divergent_identities() -> R<()> {
        let identities = vec![WorkloadIdentity {
            name: "acme".into(),
            audience: vec!["acme-api".into()],
            env: Some("ACME_TOKEN".into()),
            ..Default::default()
        }];
        let manager = Arc::new(WorkerIdentityManager::new(Arc::new(AllocNamingIssuer)));
        let mut seen: Vec<crate::structs::checkpoint::ResumeIdentity> = Vec::new();
        for (n, allocation_id) in [(1, 61), (2, 62), (3, 63)] {
            let mut allocation = TaskAllocation {
                allocation_id,
                job_id: "agents".into(),
                task_name: "workspace".into(),
                task_runtime: "sandbox".into(),
                task_config: json!({ "image": "alpine", "command": ["sleep", "1"] }).to_string(),
                task_identities: serde_json::to_string(&identities)?,
                ..Default::default()
            };
            manager
                .prepare_task_allocation(&mut allocation, None)
                .await?;
            let (alloc, task) = crate::worker::task_manager::task_and_allocation(&allocation)?;
            let restore = crate::structs::SnapshotRestore {
                snapshot_id: "snap-fork".into(),
                kind: "memory".into(),
                manifest_json: "{}".into(),
                fork: true,
                instance_id: format!("run-17-f{n}"),
                ..Default::default()
            };
            let identity = crate::worker::checkpoints::resume_identity(&alloc, &task, &restore);
            assert!(identity.fork);
            assert_eq!(
                identity.env.get("LEGION_INSTANCE_ID").map(String::as_str),
                Some(format!("run-17-f{n}").as_str())
            );
            seen.push(identity);
        }
        let distinct = |field: fn(&crate::structs::checkpoint::ResumeIdentity) -> String| {
            seen.iter()
                .map(field)
                .collect::<std::collections::HashSet<_>>()
                .len()
        };
        assert_eq!(distinct(|i| i.alloc_id.to_string()), 3);
        assert_eq!(distinct(|i| i.instance_id.clone().unwrap_or_default()), 3);
        assert_eq!(distinct(|i| i.hostname.clone()), 3);
        assert_eq!(distinct(|i| i.mac.clone().unwrap_or_default()), 3);
        assert_eq!(distinct(|i| i.token.clone().unwrap_or_default()), 3);
        assert_eq!(distinct(|i| i.entropy.clone()), 3);
        assert_eq!(seen[0].token.as_deref(), Some("token-for-alloc-61"));

        // A resume of the same instance keeps its NIC: no new MAC.
        let (alloc, task) = crate::worker::task_manager::task_and_allocation(&TaskAllocation {
            allocation_id: 64,
            task_name: "workspace".into(),
            task_runtime: "sandbox".into(),
            task_config: "{}".into(),
            ..Default::default()
        })?;
        let resume = crate::worker::checkpoints::resume_identity(
            &alloc,
            &task,
            &crate::structs::SnapshotRestore {
                instance_id: "user-42".into(),
                ..Default::default()
            },
        );
        assert!(resume.mac.is_none() && !resume.fork);
        Ok(())
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
        assert!(s.contains("/identity/7/"), "{s}");
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
        write_identity_files(&identity, 1, "tok", None)?;
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
            provisional: None,
            task_volumes: String::new(),
            allocation_id: 7,
            job_id: "job".into(),
            job_config_id: "jc".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "api".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["sleep", "1"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: serde_json::to_string(&identities)?,
            self_terminate_after_seconds: 0,
            task_artifacts: String::new(),
            task_resources: String::new(),

            network: String::new(),
            task_connect: String::new(),
            ..Default::default()
        };

        let prepared = manager
            .prepare_task_allocation(&mut allocation, None)
            .await?;
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

    fn legion_allocation(alloc_id: i64, runtime: &str, env: Value) -> R<TaskAllocation> {
        let legion = WorkloadIdentity {
            name: "legion".into(),
            permissions: serde_json::from_value(json!([
                { "resources": ["kv"], "actions": ["read"], "paths": ["certs/*"] },
            ]))?,
            ..Default::default()
        };
        Ok(TaskAllocation {
            allocation_id: alloc_id,
            job_id: "job".into(),
            job_config_id: "jc".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "api".into(),
            task_runtime: runtime.into(),
            task_config: json!({ "command": ["sleep", "1"], "env": env }).to_string(),
            task_identities: serde_json::to_string(&[legion])?,
            ..Default::default()
        })
    }

    /// The `legion` identity with no delivery of its own: its token lands
    /// in the task's secrets directory, and the task's env says where, in
    /// the task's own view, without overriding what the job set itself.
    #[tokio::test]
    async fn the_legion_identity_gets_a_token_file_and_ambient_env_test() -> R<()> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let manager = Arc::new(WorkerIdentityManager::new(FakeIssuer::new(900)));

        // `exec`: the task sees host paths.
        let task_dir = dir.path().join("exec-task");
        let mut allocation = legion_allocation(41, "exec", json!({}))?;
        let base = FileDeliveryBase {
            in_task_root: None,
            host_task_dir: task_dir.clone(),
            host_sockets: true,
        };
        let prepared = manager
            .prepare_task_allocation(&mut allocation, Some(base))
            .await?;
        assert_eq!(prepared.len(), 1);
        let token = task_dir.join("secrets/legion.token");
        assert!(fs::read_to_string(&token)?.starts_with("tok-"));
        #[cfg(unix)]
        assert_eq!(mode_of(&token), 0o400);
        let config: Value = serde_json::from_str(&allocation.task_config)?;
        let env = &config["env"];
        assert_eq!(env["LEGION_SECRETS_DIR"], json!(task_dir.join("secrets")));
        assert_eq!(env["LEGION_IDENTITY_TOKEN_FILE"], json!(token));
        // No socket context on this worker: no address.
        assert!(env.get("LEGION_ADDR").is_none(), "{env}");
        assert!(env.get("LEGION_TOKEN_FILE").is_none(), "{env}");

        // `container`: the in-task view of the same files, and a job's own
        // setting wins.
        let task_dir = dir.path().join("container-task");
        let mut allocation =
            legion_allocation(42, "container", json!({ "LEGION_SECRETS_DIR": "/mine" }))?;
        let base = FileDeliveryBase {
            in_task_root: Some("/legion/task"),
            host_task_dir: task_dir.clone(),
            host_sockets: true,
        };
        manager
            .prepare_task_allocation(&mut allocation, Some(base))
            .await?;
        assert!(task_dir.join("secrets/legion.token").exists());
        let config: Value = serde_json::from_str(&allocation.task_config)?;
        assert_eq!(config["env"]["LEGION_SECRETS_DIR"], "/mine");
        assert_eq!(
            config["env"]["LEGION_IDENTITY_TOKEN_FILE"],
            "/legion/task/secrets/legion.token"
        );
        Ok(())
    }

    /// With a socket context, the allocation gets its API socket and
    /// `LEGION_ADDR` points at it; stopping the allocation removes it. A
    /// sandbox (which cannot reach a host socket) gets the token file only.
    #[tokio::test]
    async fn the_legion_identity_gets_an_api_socket_test() -> R<()> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let context = ApiSocketContext {
            channel: tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy(),
            managers: None,
            peer_check: crate::worker::api_socket::PeerCheck::FileMode,
        };
        let manager =
            Arc::new(WorkerIdentityManager::new(FakeIssuer::new(900)).with_api_sockets(context));
        let task_dir = dir.path().join("task");
        let mut allocation = legion_allocation(43, "exec", json!({}))?;
        let base = FileDeliveryBase {
            in_task_root: None,
            host_task_dir: task_dir.clone(),
            host_sockets: true,
        };
        manager
            .prepare_task_allocation(&mut allocation, Some(base))
            .await?;
        let socket = task_dir.join("secrets/legion.sock");
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            assert!(fs::metadata(&socket)?.file_type().is_socket());
        }
        let config: Value = serde_json::from_str(&allocation.task_config)?;
        assert_eq!(
            config["env"]["LEGION_ADDR"],
            json!(format!("unix://{}", socket.display()))
        );
        manager.stop(43);
        assert!(!socket.exists());

        let sandbox_dir = dir.path().join("sandbox-task/identity");
        fs::create_dir_all(dir.path().join("sandbox-task"))?;
        let mut allocation = legion_allocation(44, "sandbox", json!({}))?;
        let base = FileDeliveryBase {
            in_task_root: Some("/legion/identity"),
            host_task_dir: sandbox_dir.clone(),
            host_sockets: false,
        };
        manager
            .prepare_task_allocation(&mut allocation, Some(base))
            .await?;
        assert!(sandbox_dir.join("secrets/legion.token").exists());
        assert!(!sandbox_dir.join("secrets/legion.sock").exists());
        let config: Value = serde_json::from_str(&allocation.task_config)?;
        assert!(config["env"].get("LEGION_ADDR").is_none());
        assert_eq!(
            config["env"]["LEGION_IDENTITY_TOKEN_FILE"],
            "/legion/identity/secrets/legion.token"
        );
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
            provisional: None,
            task_volumes: String::new(),
            allocation_id: 8,
            job_id: "job".into(),
            job_config_id: "jc".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "api".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: serde_json::to_string(&identities)?,
            self_terminate_after_seconds: 0,
            task_artifacts: String::new(),
            task_resources: String::new(),

            network: String::new(),
            task_connect: String::new(),
            ..Default::default()
        };

        let prepared = manager
            .prepare_task_allocation(&mut allocation, None)
            .await?;
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
        write_identity_files(&identity, 11, "tok-0", None)?;
        manager.spawn_refreshers(
            11,
            vec![PreparedIdentity {
                identity,
                expires_at: unix_now() + 2,
                delivery_base: None,
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

    /// A fork or snapshot of the instance raises its identity epoch, and the
    /// manager refuses the allocation's older tokens from then on. The rise
    /// must re-issue now, not at the next half-TTL refresh an hour away.
    #[tokio::test]
    async fn a_raised_identity_epoch_reissues_tokens_at_once() -> R<()> {
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

        let issuer = FakeIssuer::new(7200);
        let manager = Arc::new(WorkerIdentityManager::new(issuer.clone()));
        let identity = file_identity(&token_path, ChangeMode::Noop);
        write_identity_files(&identity, 12, "tok-0", None)?;
        // The first sighting records the epoch the tokens were issued at.
        manager.observe_identity_epochs([(12, 2)]);
        manager.spawn_refreshers(
            12,
            vec![PreparedIdentity {
                identity,
                expires_at: unix_now() + 7200,
                delivery_base: None,
            }],
            task_manager.clone(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(issuer.calls.load(Ordering::SeqCst), 0, "no refresh is due");

        // The same epoch again changes nothing; a raised one re-issues.
        manager.observe_identity_epochs([(12, 2)]);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(issuer.calls.load(Ordering::SeqCst), 0);
        manager.observe_identity_epochs([(12, 3)]);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while issuer.calls.load(Ordering::SeqCst) < 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "the raised epoch did not re-issue"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(fs::read_to_string(&token_path)?, "tok-1");
        assert_eq!(
            issuer.calls.load(Ordering::SeqCst),
            1,
            "one re-issue per rise"
        );

        manager.retain(&[]);
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
        apply_refreshed_token(&identity, 12, "tok-new", None, &task_manager)?;

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
        apply_refreshed_token(&identity, 13, "tok-new", None, &task_manager)?;

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
        apply_refreshed_token(&identity, 14, "tok-new", None, &task_manager)?;

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
        // An injection-only identity (no env, no file) is the forwarder's,
        // not the task's.
        let parsed = parse_identities(
            r#"[{"name":"api","audience":["a"]},{"name":"aws","audience":["b"],"file":{"path":"aws/token"}}]"#,
        )
        .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, "aws");
    }

    /// An app's config file, named relative to the task directory, becomes
    /// the path the task sees: the host path for `exec`, the mount for
    /// `container`; an absolute value and a climbing one are not rewritten.
    #[test]
    fn app_config_env_resolves_to_the_tasks_view() -> R<()> {
        let config = |value: &str| {
            json!({ "env": { crate::structs::app::APP_CONFIG_ENV: value } }).to_string()
        };
        let read = |json: &str| -> R<String> {
            let value: Value = serde_json::from_str(json)?;
            Ok(value["env"][crate::structs::app::APP_CONFIG_ENV]
                .as_str()
                .unwrap_or_default()
                .to_string())
        };
        let exec = FileDeliveryBase {
            in_task_root: None,
            host_task_dir: PathBuf::from("/var/lib/legion/allocs/7/agent"),
            host_sockets: true,
        };
        let mut task = config("legion-app-config.json");
        resolve_app_config_env(&mut task, Some(&exec))?;
        assert_eq!(
            read(&task)?,
            "/var/lib/legion/allocs/7/agent/legion-app-config.json"
        );

        let container = FileDeliveryBase {
            in_task_root: Some("/legion/task"),
            host_task_dir: PathBuf::from("/host/allocs/7/agent"),
            host_sockets: true,
        };
        let mut task = config("legion-app-config.json");
        resolve_app_config_env(&mut task, Some(&container))?;
        assert_eq!(read(&task)?, "/legion/task/legion-app-config.json");

        let mut task = config("/etc/app.json");
        resolve_app_config_env(&mut task, Some(&exec))?;
        assert_eq!(read(&task)?, "/etc/app.json");

        let mut task = config("../../etc/shadow");
        assert!(resolve_app_config_env(&mut task, Some(&exec)).is_err());

        let mut untouched = json!({ "env": {} }).to_string();
        resolve_app_config_env(&mut untouched, Some(&exec))?;
        assert_eq!(untouched, json!({ "env": {} }).to_string());
        Ok(())
    }
}
