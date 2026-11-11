//! Session runs on the node (docs/session-runs.md): one call that starts a
//! fresh, single-use session, stages the caller's files, executes the
//! task's `run.command` once, collects the outcome, and ends the session.
//!
//! Everything here is node-local. Nothing is written to raft, and the only
//! state that outlives a run is its reply, kept for idempotent retries
//! ([`RunRegistry`]).
//!
//! **Every runtime takes the same path.** Staging, the command, and
//! collection are all execs into the running session
//! ([`WorkerTaskManager::open_exec_session`]), so `exec`, `container`, and
//! `sandbox` (over the guest agent) differ only in where the run directory
//! is: `/legion/run` inside a unit with its own root filesystem, and
//! `<task dir>/run` on `exec`, whose sessions share the host's.
//!
//! **A run's session always ends.** The run is driven by a detached task,
//! so a caller that hangs up does not cancel it half way; every exit from
//! the driver ends the session; and a session whose origin is `run` that no
//! live driver owns (the worker restarted mid-run) is ended by the session
//! maintenance loop ([`end_orphaned_runs`]).

use super::WorkerTaskManager;
use crate::prelude::*;
use crate::structs::run::{
    append_capped, command_not_allowed, derived_run_id, no_run_block, rewrite_run_dir,
    run_relative_path, ResolvedRun, MAX_RUN_STAGED, RUN_DIR, RUN_INPUT_FILE, RUN_REPLY_TTL,
    RUN_RESULT_FILE,
};
use crate::structs::{
    RunDryRun, RunOutputFile, RunSessionRequest, RunSessionResponse, RunStop, TaskConfig,
};
use crate::task_plugins::ExecOptions;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tonic::{Code, Status};

/// The origin a run's session carries in its claim and its session report.
pub const RUN_ORIGIN: &str = "run";

/// How long a run waits for its session to reach `running`, by default and
/// at most.
const DEFAULT_START_WAIT: Duration = Duration::from_secs(60);
const MAX_START_WAIT: Duration = Duration::from_secs(600);

/// How long a run waits for a unit that reports `running` to accept its
/// first exec. A container exists a moment after its runtime was spawned,
/// and a guest's agent listens a moment after the VM booted.
const EXEC_READY_WAIT: Duration = Duration::from_secs(20);

/// The deadline of one staging or collection exec.
const TRANSFER_DEADLINE: Duration = Duration::from_secs(60);

/// How long the driver waits for a finished command's output readers. A
/// grandchild that kept the pipe open must not hold the reply.
const READER_GRACE: Duration = Duration::from_secs(2);

/// What a finished or failed run hands the callers waiting on it.
type Outcome = Result<RunSessionResponse, (Code, String)>;

fn outcome_of(result: &Result<RunSessionResponse, Status>) -> Outcome {
    match result {
        Ok(reply) => Ok(reply.clone()),
        Err(status) => Err((status.code(), status.message().to_string())),
    }
}

fn result_of(outcome: Outcome) -> Result<RunSessionResponse, Status> {
    outcome.map_err(|(code, message)| Status::new(code, message))
}

#[derive(Default)]
struct RegistryState {
    /// Keyed runs in flight: a retry waits on the first attempt.
    in_flight: HashMap<String, watch::Receiver<Option<Outcome>>>,
    /// The sessions of runs a live driver owns.
    sessions: HashSet<String>,
    /// Finished keyed runs, by run id, with when they finished.
    done: HashMap<String, (i64, RunSessionResponse)>,
}

/// The node's memory of runs: which are in flight, and the replies of
/// finished ones for [`RUN_REPLY_TTL`], in memory and in `sessions.db` so a
/// retry after a worker restart is still answered.
pub struct RunRegistry {
    state: Mutex<RegistryState>,
    db_path: PathBuf,
    db: Mutex<Option<rusqlite::Connection>>,
}

impl std::fmt::Debug for RunRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunRegistry")
            .field("db_path", &self.db_path)
            .finish()
    }
}

/// What [`RunRegistry::begin`] found for a run id.
pub enum Begin {
    /// Nobody has run it: the caller does, and settles the ticket.
    Run(RunTicket),
    /// Another call is running it now.
    Wait(watch::Receiver<Option<Outcome>>),
    /// It finished within the reply window.
    Done(Box<RunSessionResponse>),
}

/// The right to run one keyed run. Dropped unsettled (a panic), it frees
/// the id and wakes the waiters with an error.
pub struct RunTicket {
    registry: Arc<RunRegistry>,
    run_id: String,
    tx: watch::Sender<Option<Outcome>>,
    settled: bool,
}

impl RunTicket {
    /// Hand the outcome to the waiters; keep a successful reply for
    /// retries. A failed run is not kept: nothing ran to completion, so a
    /// retry runs again.
    fn settle(mut self, result: &Result<RunSessionResponse, Status>, now_ms: i64) {
        self.settled = true;
        if let Ok(reply) = result {
            self.registry.remember(&self.run_id, reply, now_ms);
        }
        if let Ok(mut state) = self.registry.state.lock() {
            state.in_flight.remove(&self.run_id);
        }
        let _ = self.tx.send(Some(outcome_of(result)));
    }
}

impl Drop for RunTicket {
    fn drop(&mut self) {
        if !self.settled {
            if let Ok(mut state) = self.registry.state.lock() {
                state.in_flight.remove(&self.run_id);
            }
        }
    }
}

impl RunRegistry {
    /// A registry whose replies are also kept in the sqlite file at
    /// `db_path` (the node's `sessions.db`), once that file exists.
    pub fn new(db_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RegistryState::default()),
            db_path,
            db: Mutex::new(None),
        })
    }

    /// Run `f` against the reply table. The session log creates
    /// `sessions.db` owner-only; this never creates it, so the file's mode
    /// is always the log's.
    fn with_db<T>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>,
    ) -> Option<T> {
        let mut guard = self.db.lock().ok()?;
        if guard.is_none() {
            if !self.db_path.exists() {
                return None;
            }
            let conn = rusqlite::Connection::open(&self.db_path).ok()?;
            let _ = conn.busy_timeout(Duration::from_secs(2));
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS run_replies (
                    run_id TEXT PRIMARY KEY,
                    finished_at_ms INTEGER NOT NULL,
                    reply BLOB NOT NULL
                )",
            )
            .ok()?;
            *guard = Some(conn);
        }
        match f(guard.as_ref()?) {
            Ok(value) => Some(value),
            Err(err) => {
                debug!(error = %err, "Run reply table access failed");
                None
            }
        }
    }

    fn remember(&self, run_id: &str, reply: &RunSessionResponse, now_ms: i64) {
        let cutoff = now_ms - RUN_REPLY_TTL.as_millis() as i64;
        if let Ok(mut state) = self.state.lock() {
            state.done.retain(|_, (at, _)| *at >= cutoff);
            state
                .done
                .insert(run_id.to_string(), (now_ms, reply.clone()));
        }
        let blob = prost::Message::encode_to_vec(reply);
        self.with_db(|conn| {
            conn.execute(
                "DELETE FROM run_replies WHERE finished_at_ms < ?1",
                [cutoff],
            )?;
            conn.execute(
                "INSERT OR REPLACE INTO run_replies (run_id, finished_at_ms, reply) \
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![run_id, now_ms, blob],
            )
        });
    }

    fn recall(&self, run_id: &str, now_ms: i64) -> Option<RunSessionResponse> {
        let cutoff = now_ms - RUN_REPLY_TTL.as_millis() as i64;
        if let Ok(state) = self.state.lock() {
            if let Some((at, reply)) = state.done.get(run_id) {
                return (*at >= cutoff).then(|| reply.clone());
            }
        }
        let blob: Vec<u8> = self.with_db(|conn| {
            conn.query_row(
                "SELECT reply FROM run_replies WHERE run_id = ?1 AND finished_at_ms >= ?2",
                rusqlite::params![run_id, cutoff],
                |row| row.get(0),
            )
        })?;
        <RunSessionResponse as prost::Message>::decode(blob.as_slice()).ok()
    }

    /// Claim a keyed run id, or find who has it.
    pub fn begin(self: &Arc<Self>, run_id: &str, now_ms: i64) -> Begin {
        if let Some(reply) = self.recall(run_id, now_ms) {
            return Begin::Done(Box::new(reply));
        }
        let mut state = self.state.lock().expect("run registry");
        if let Some(rx) = state.in_flight.get(run_id) {
            return Begin::Wait(rx.clone());
        }
        let (tx, rx) = watch::channel(None);
        state.in_flight.insert(run_id.to_string(), rx);
        Begin::Run(RunTicket {
            registry: self.clone(),
            run_id: run_id.to_string(),
            tx,
            settled: false,
        })
    }

    fn track(&self, session_id: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.sessions.insert(session_id.to_string());
        }
    }

    fn untrack(&self, session_id: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.sessions.remove(session_id);
        }
    }

    /// Whether a live driver owns this session.
    pub fn owns(&self, session_id: &str) -> bool {
        self.state
            .lock()
            .map(|state| state.sessions.contains(session_id))
            .unwrap_or(true)
    }
}

/// Where a run's session runs and how to reach its files.
#[derive(Debug, Clone)]
pub struct RunTarget {
    pub local_key: i64,
    /// The run directory as the command sees it.
    pub run_dir: String,
    /// Whether the session shares the host's root filesystem (`exec`).
    pub host_root: bool,
    /// The session's cgroup, where the node governs it.
    pub cgroup: Option<PathBuf>,
}

impl WorkerTaskManager {
    /// The node's run registry.
    pub fn run_registry(&self) -> Arc<RunRegistry> {
        self.run_registry.clone()
    }

    /// Where a running session's run directory is.
    pub fn run_target(&self, session_id: &str) -> Option<RunTarget> {
        let claim = self.claims.by_instance(session_id)?;
        let local_key = claim.local_key;
        let task = self.task_configs.get(&local_key)?;
        let instance = self.instances.get(&local_key)?;
        let host_root = task.runtime == "exec";
        let run_dir = if host_root {
            instance
                .task_dir
                .as_ref()
                .map(|dir| dir.join("run").display().to_string())?
        } else {
            RUN_DIR.to_string()
        };
        Some(RunTarget {
            local_key,
            run_dir,
            host_root,
            cgroup: instance
                .resource_assignment
                .as_ref()
                .map(|assignment| assignment.absolute_cgroup_path()),
        })
    }

    /// Why this node would refuse one more session of a task right now:
    /// a disabled runtime, no envelope, or a full one. `None` when it has
    /// room. A dry run's answer; an actual start is admitted again.
    pub fn session_room_refusal(&mut self, job_id: &str, task: &TaskConfig) -> Option<String> {
        if let Some(disable) = self
            .runtime_gate()
            .current()
            .refusing(&task.runtime, job_id)
        {
            return Some(
                crate::structs::runtime_state::RuntimeDisabledRefusal::new(disable, job_id)
                    .to_string(),
            );
        }
        let Some((local, envelope)) = self.envelope_for(job_id, &task.name) else {
            return Some(
                super::SessionRefusal::NoEnvelope {
                    job_id: job_id.to_string(),
                    task_name: task.name.clone(),
                }
                .to_string(),
            );
        };
        let room = self
            .envelope_sizes
            .get(&local)
            .map(|size| size.room())
            .unwrap_or_else(|| crate::structs::session::resource_amounts(&envelope.resources));
        let needs = crate::structs::session::resource_amounts(&task.resources);
        self.forget_stopped_promotions();
        let used = self.claims.session_usage(job_id, &task.name);
        super::envelope_refusal(&room, &used, &needs).map(|refusal| refusal.to_string())
    }

    /// The sessions runs left behind: origin `run`, and no live driver.
    fn orphaned_run_sessions(&self) -> Vec<String> {
        self.claims
            .sessions()
            .into_iter()
            .filter(|claim| claim.origin == RUN_ORIGIN && !claim.is_ratified())
            .map(|claim| claim.instance_id)
            .filter(|session_id| !self.run_registry.owns(session_id))
            .collect()
    }
}

/// Whether a workload of `caller` may run `target`, a session task of the
/// same job, through its API socket with no credential (the `Workload`
/// service). The jobspec decides, never the caller: a task runs a sibling
/// task only when it declares it.
///
/// A task may always run a fresh session of itself, and a session of any
/// task its `children.tasks` names (docs/child-sessions.md): the block that
/// lets it start a child of the task lets it run one once.
pub fn workload_may_run(caller: &TaskConfig, target: &TaskConfig) -> Result<(), String> {
    if caller.name == target.name
        || caller
            .children
            .as_ref()
            .is_some_and(|block| block.declares(&target.name))
    {
        return Ok(());
    }
    Err(format!(
        "task {} may not run task {}: a workload runs a sibling task only when its own task \
         names it in children.tasks; add it there, or send the run through a manager with a \
         token that holds job:write",
        caller.name, target.name
    ))
}

impl WorkerTaskManager {
    /// The job and task of a running allocation, by the manager's id.
    pub fn allocation_task(&self, allocation_id: i64) -> Option<(String, TaskConfig)> {
        let local = self.local_key(allocation_id);
        let allocation = self.allocations.get(&local)?;
        let task = self.task_configs.get(&local)?;
        Some((allocation.job_id.clone(), task.clone()))
    }
}

/// End every run session no live driver owns: the ones a worker restart
/// re-adopted from the session log. Returns how many it ended.
pub fn end_orphaned_runs(task_manager: &Arc<Mutex<WorkerTaskManager>>) -> usize {
    let mut guard = task_manager.lock().expect("task manager");
    let orphans = guard.orphaned_run_sessions();
    if orphans.is_empty() {
        return 0;
    }
    let ended = guard.stop_sessions(&orphans, "run interrupted (its driver is gone)");
    for session_id in &ended {
        warn!(session_id, "Ended a session a run left behind");
    }
    ended.len()
}

/// One run, as the node's `RunSession` handler resolved it.
#[derive(Debug, Clone)]
pub struct RunCall {
    pub job_id: String,
    pub task_name: String,
    pub request: RunSessionRequest,
    /// The session ticket's id, for the session's report; empty for a
    /// manager-routed run.
    pub ticket_jti: String,
}

/// A run with everything checked, ready to start.
struct Prepared {
    spec: super::SessionSpec,
    run: ResolvedRun,
    command: Vec<String>,
    /// (path relative to the run directory, bytes, executable).
    files: Vec<(String, Vec<u8>, bool)>,
    /// (path as asked, path relative to the run directory).
    get: Vec<(String, String)>,
    has_input: bool,
    start_wait: Duration,
}

/// A run's files, input, and `get` paths, checked: relative paths with no
/// `..`, no file staged twice, `input` JSON, at most [`MAX_RUN_STAGED`]
/// bytes together.
pub struct StagedFiles {
    /// (path relative to the run directory, bytes, executable). `input`
    /// is staged as [`RUN_INPUT_FILE`].
    pub files: Vec<(String, Vec<u8>, bool)>,
    /// (path as asked, path relative to the run directory).
    pub get: Vec<(String, String)>,
    pub has_input: bool,
}

/// Check a run request's staging (shared with isolate runs).
pub fn staged_files(request: &RunSessionRequest) -> Result<StagedFiles, Status> {
    let mut staged = 0u64;
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    for file in &request.files {
        let path = run_relative_path(&file.path)
            .map_err(|err| Status::invalid_argument(format!("file {err}")))?;
        if !seen.insert(path.clone()) {
            return Err(Status::invalid_argument(format!(
                "file {path:?} is staged twice"
            )));
        }
        staged += file.content.len() as u64;
        files.push((path, file.content.clone(), file.executable));
    }
    let mut has_input = false;
    if let Some(input) = request.input.as_deref() {
        serde_json::from_str::<Value>(input)
            .map_err(|err| Status::invalid_argument(format!("input is not JSON: {err}")))?;
        if !seen.insert(RUN_INPUT_FILE.to_string()) {
            return Err(Status::invalid_argument(format!(
                "input is staged as {RUN_INPUT_FILE}, which a file also names"
            )));
        }
        staged += input.len() as u64;
        files.push((RUN_INPUT_FILE.to_string(), input.as_bytes().to_vec(), false));
        has_input = true;
    }
    if staged > MAX_RUN_STAGED {
        return Err(Status::invalid_argument(format!(
            "the run stages {staged} bytes; files and input together may be at most \
             {MAX_RUN_STAGED} (3 MiB). Put larger inputs in an artifact, a volume, or the \
             object store"
        )));
    }
    let mut get = Vec::new();
    for path in &request.get {
        let relative = run_relative_path(path)
            .map_err(|err| Status::invalid_argument(format!("get {err}")))?;
        get.push((path.clone(), relative));
    }
    Ok(StagedFiles {
        files,
        get,
        has_input,
    })
}

/// The isolate-only field a request sets, if any.
pub fn isolate_only_field(request: &RunSessionRequest) -> Option<&'static str> {
    if !request.tier.trim().is_empty() {
        Some("tier")
    } else if !request.tenant.is_empty() {
        Some("tenant")
    } else if !request.session_id.is_empty() {
        Some("session_id")
    } else if !request.args.is_empty() {
        Some("args")
    } else {
        None
    }
}

fn prepare(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    call: &RunCall,
    extra_args: &[String],
) -> Result<Prepared, Status> {
    let spec = task_manager
        .lock()
        .map_err(|_| Status::internal("Task manager lock poisoned"))?
        .session_spec(&call.job_id, &call.task_name)
        .ok_or_else(|| {
            Status::failed_precondition(
                super::SessionRefusal::NoEnvelope {
                    job_id: call.job_id.clone(),
                    task_name: call.task_name.clone(),
                }
                .to_string(),
            )
        })?;
    if crate::structs::isolate::is_isolate_task(&spec.task) {
        // The server sends isolate tasks to the isolate supervisor; reaching
        // here means this node has none.
        return Err(Status::failed_precondition(format!(
            "task {} runs on the isolate runtime, which this node does not run (no \
             legion-isolate binary)",
            call.task_name
        )));
    }
    if let Some(field) = isolate_only_field(&call.request) {
        return Err(Status::invalid_argument(format!(
            "{field} applies only to tasks of the isolate runtime; task {} runs on {}",
            call.task_name, spec.task.runtime
        )));
    }
    let run = spec
        .task
        .run
        .as_ref()
        .ok_or_else(|| Status::failed_precondition(no_run_block(&call.job_id, &call.task_name)))?
        .resolve()
        .map_err(|err| Status::failed_precondition(format!("task {}: {err}", call.task_name)))?;
    let request = &call.request;
    let command = if request.command.is_empty() {
        run.command.clone()
    } else {
        if !run.allow_command {
            return Err(Status::failed_precondition(command_not_allowed(
                &call.task_name,
            )));
        }
        if request.command[0].trim().is_empty() {
            return Err(Status::invalid_argument("command's first element is empty"));
        }
        request.command.clone()
    };
    // An isolate run sent to its VM task: the run's `args` follow the VM
    // task's own program (the worker builds this, not the caller).
    let mut command = command;
    command.extend(extra_args.iter().cloned());
    let StagedFiles {
        files,
        get,
        has_input,
    } = staged_files(request)?;
    let start_wait = request
        .wait_timeout_seconds
        .map(|seconds| Duration::from_secs(u64::from(seconds.max(1))).min(MAX_START_WAIT))
        .unwrap_or(DEFAULT_START_WAIT);
    Ok(Prepared {
        spec,
        run,
        command,
        files,
        get,
        has_input,
        start_wait,
    })
}

/// Run once in a fresh session on this node. The caller authorized the
/// job, the task, and (for a replaced command) the caller's right to it.
pub async fn run_session(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    launch_pool: Arc<crate::worker::launch_pool::LaunchPool>,
    node_id: String,
    call: RunCall,
) -> Result<RunSessionResponse, Status> {
    run_session_with_args(task_manager, launch_pool, node_id, call, Vec::new()).await
}

/// [`run_session`] with `extra_args` appended to the task's command: the
/// VM tier of an isolate run (docs/task-runtime-isolate.md), whose `args`
/// go to the VM task's own `run.command`.
pub async fn run_session_with_args(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    launch_pool: Arc<crate::worker::launch_pool::LaunchPool>,
    node_id: String,
    call: RunCall,
    extra_args: Vec<String>,
) -> Result<RunSessionResponse, Status> {
    let prepared = prepare(&task_manager, &call, &extra_args)?;
    if call.request.dry_run {
        let reason = task_manager
            .lock()
            .map_err(|_| Status::internal("Task manager lock poisoned"))?
            .session_room_refusal(&call.job_id, &prepared.spec.task);
        return Ok(RunSessionResponse {
            node_id: node_id.clone(),
            task_name: call.task_name.clone(),
            dry_run: Some(RunDryRun {
                node_id,
                has_room: reason.is_none(),
                reason: reason.unwrap_or_default(),
                command: prepared.command,
                wall: prepared.run.wall_text,
                output_bytes: prepared.run.output,
                ..Default::default()
            }),
            ..Default::default()
        });
    }

    let registry = task_manager
        .lock()
        .map_err(|_| Status::internal("Task manager lock poisoned"))?
        .run_registry();
    let job_id = call.job_id.clone();
    let task_name = call.task_name.clone();
    let request = call.request.clone();
    let driver_node = node_id.clone();
    let driver_registry = registry.clone();
    run_keyed(
        &registry,
        &job_id,
        &task_name,
        &node_id,
        &request,
        move |run_id| async move {
            let result = drive(
                &task_manager,
                &launch_pool,
                &driver_registry,
                &driver_node,
                &call,
                prepared,
                &run_id,
            )
            .await;
            let metrics = crate::telemetry::worker_metrics();
            metrics.session_runs_total.inc(&[match &result {
                Ok(reply) if reply.stopped.is_some() => "stopped",
                Ok(_) => "ran",
                Err(_) => "failed",
            }]);
            result
        },
    )
    .await
}

/// Run `run` once per idempotency key (docs/session-runs.md): a request
/// with no key gets a minted run id; a keyed one derives its id from the
/// key and the request's content, and a retry is answered from the kept
/// reply or waits for the attempt in flight. `run` is driven by a detached
/// task, so a caller that hangs up does not cancel it half way. Shared by
/// session runs and isolate runs.
pub async fn run_keyed<F, Fut>(
    registry: &Arc<RunRegistry>,
    job_id: &str,
    task_name: &str,
    node_id: &str,
    request: &RunSessionRequest,
    run: F,
) -> Result<RunSessionResponse, Status>
where
    F: FnOnce(String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<RunSessionResponse, Status>> + Send + 'static,
{
    let key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let (run_id, ticket) = if key.is_empty() {
        (crate::structs::mint_session_id("run", node_id), None)
    } else {
        // A key names one request, not just one string: the request's own
        // content is part of the id, so another caller of the same job and
        // task who reuses a key with different files or input starts its own
        // run and is never answered with this one's stored reply.
        let key = format!("{key}\n{}", request_fingerprint(request));
        let run_id = derived_run_id(job_id, task_name, &key);
        match registry.begin(&run_id, crate::util::now_unix_ms()) {
            Begin::Done(reply) => {
                return Ok(RunSessionResponse {
                    deduplicated: true,
                    ..*reply
                })
            }
            Begin::Wait(mut rx) => {
                let outcome = loop {
                    if let Some(outcome) = rx.borrow().clone() {
                        break outcome;
                    }
                    if rx.changed().await.is_err() {
                        break Err((
                            Code::Unavailable,
                            "the run with this idempotency key was interrupted before it \
                             finished; retry"
                                .to_string(),
                        ));
                    }
                };
                return result_of(outcome).map(|reply| RunSessionResponse {
                    deduplicated: true,
                    ..reply
                });
            }
            Begin::Run(ticket) => (run_id, Some(ticket)),
        }
    };

    // Detached: a caller that hangs up must not cancel the run between its
    // start and its end, where the session would be left running.
    let driver = tokio::spawn(async move {
        let result = run(run_id).await;
        if let Some(ticket) = ticket {
            ticket.settle(&result, crate::util::now_unix_ms());
        }
        result
    });
    driver
        .await
        .map_err(|err| Status::internal(format!("the run's driver failed: {err}")))?
}

/// Ends the run's session however the driver leaves.
struct SessionEnd {
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    registry: Arc<RunRegistry>,
    session_id: String,
    done: bool,
}

impl SessionEnd {
    fn end_now(&mut self, reason: &str) {
        if self.done {
            return;
        }
        self.done = true;
        if let Ok(mut guard) = self.task_manager.lock() {
            guard.stop_sessions(std::slice::from_ref(&self.session_id), reason);
        }
        self.registry.untrack(&self.session_id);
    }

    /// End the session off the async runtime: a stop can wait on the
    /// runtime's own teardown.
    async fn end(mut self, reason: &'static str) {
        let _ = tokio::task::spawn_blocking(move || self.end_now(reason)).await;
    }
}

impl Drop for SessionEnd {
    fn drop(&mut self) {
        // A panic or a cancelled driver: still end it.
        self.end_now("run interrupted");
    }
}

async fn drive(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    launch_pool: &Arc<crate::worker::launch_pool::LaunchPool>,
    registry: &Arc<RunRegistry>,
    node_id: &str,
    call: &RunCall,
    prepared: Prepared,
    run_id: &str,
) -> Result<RunSessionResponse, Status> {
    let slot = launch_pool
        .acquire()
        .await
        .map_err(|full| Status::resource_exhausted(full.to_string()))?;
    let session_id = crate::structs::mint_session_id(&call.task_name, node_id);
    // Tracked before it is admitted, so the orphan sweep never sees a run
    // session without its driver.
    registry.track(&session_id);
    let started = {
        let task_manager = task_manager.clone();
        let spec = prepared.spec.clone();
        let (job_id, node_id, session_id, jti) = (
            call.job_id.clone(),
            node_id.to_string(),
            session_id.clone(),
            call.ticket_jti.clone(),
        );
        tokio::task::spawn_blocking(move || -> R<()> {
            let (payload, provisional) = crate::worker::edge::session_payload(
                &spec,
                &job_id,
                &session_id,
                &node_id,
                RUN_ORIGIN,
                &jti,
                crate::util::now_unix_ms(),
                None,
            )?;
            super::session_store::claim_new_store_session(&task_manager, &payload)?;
            crate::worker::edge::admit_and_launch(
                &task_manager,
                &payload,
                &provisional,
                Some(slot),
                None,
            )?;
            Ok(())
        })
        .await
        .map_err(|err| Status::internal(format!("session start panicked: {err}")))
    };
    match started {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            registry.untrack(&session_id);
            return Err(crate::worker::server::admission_status(&err));
        }
        Err(status) => {
            registry.untrack(&session_id);
            return Err(status);
        }
    }
    let end = SessionEnd {
        task_manager: task_manager.clone(),
        registry: registry.clone(),
        session_id: session_id.clone(),
        done: false,
    };
    info!(
        run_id,
        session_id = %session_id,
        job_id = %call.job_id,
        task = %call.task_name,
        "Run started"
    );

    match crate::worker::edge::wait_until_running(task_manager, &session_id, prepared.start_wait)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            end.end("run abandoned (session did not start in time)")
                .await;
            return Err(Status::deadline_exceeded(format!(
                "the run's session {session_id} was still starting after {}s; it was ended and \
                 nothing ran. Retry, or raise wait_timeout_seconds",
                prepared.start_wait.as_secs()
            )));
        }
        Err(err) => {
            end.end("run abandoned (session failed to start)").await;
            return Err(crate::worker::server::task_start_status(
                "Failed to start the run's session",
                &err,
            ));
        }
    }

    let executed = {
        let task_manager = task_manager.clone();
        let session_id = session_id.clone();
        tokio::task::spawn_blocking(move || execute(&task_manager, &session_id, &prepared))
            .await
            .map_err(|err| Status::internal(format!("the run panicked: {err}")))
    };
    end.end("run finished").await;
    let mut reply = executed??;
    reply.node_id = node_id.to_string();
    reply.session_id = session_id;
    reply.task_name = call.task_name.clone();
    reply.run_id = run_id.to_string();
    info!(
        run_id,
        session_id = %reply.session_id,
        exit = ?reply.exit,
        stopped = reply.stopped.as_ref().map(|stop| stop.reason.as_str()).unwrap_or(""),
        wall_ms = reply.wall_ms,
        "Run finished"
    );
    Ok(reply)
}

/// What one exec into the session produced.
#[derive(Debug, Default)]
pub(crate) struct Captured {
    pub(crate) exit: Option<i32>,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) truncated: bool,
    pub(crate) timed_out: bool,
    pub(crate) session_gone: bool,
    pub(crate) elapsed: Duration,
}

#[derive(Default)]
struct Sink {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    budget: u64,
    truncated: bool,
}

/// Run one command in the session and capture its output, up to `budget`
/// bytes of stdout plus stderr, killing it at `deadline` or when the
/// session ends under it.
pub(crate) fn exec_capture(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    local_key: i64,
    opts: ExecOptions,
    stdin: Option<&[u8]>,
    deadline: Duration,
    budget: u64,
) -> R<Captured> {
    let mut session = task_manager
        .lock()
        .map_err(|_| anyhow!("task manager lock poisoned"))?
        .open_exec_session(local_key, None, opts)?;
    let started = Instant::now();
    let sink = Arc::new(Mutex::new(Sink {
        budget,
        ..Default::default()
    }));
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let streams = session.take_output()?;
    let mut readers = 0usize;
    let mut spawn_reader = |mut reader: Box<dyn std::io::Read + Send>, is_stderr: bool| {
        let sink = sink.clone();
        let done = done_tx.clone();
        readers += 1;
        std::thread::spawn(move || {
            let mut buffer = [0u8; 16 * 1024];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        let mut guard = sink.lock().expect("run sink");
                        let Sink {
                            stdout,
                            stderr,
                            budget,
                            truncated,
                        } = &mut *guard;
                        let into = if is_stderr { stderr } else { stdout };
                        *truncated |= append_capped(into, &buffer[..read], budget);
                    }
                }
            }
            let _ = done.send(());
        });
    };
    spawn_reader(streams.stdout, false);
    if let Some(stderr) = streams.stderr {
        spawn_reader(stderr, true);
    }
    drop(done_tx);

    if let Some(data) = stdin {
        // The readers are already draining, so a command that writes while
        // it reads cannot wedge this.
        let _ = session.write_stdin(data);
    }
    session.close_stdin();

    let mut captured = Captured::default();
    let mut last_liveness = Instant::now();
    loop {
        match session.try_wait() {
            Ok(Some(code)) => {
                captured.exit = Some(code);
                break;
            }
            Ok(None) => {}
            Err(err) => {
                session.kill();
                bail!("waiting for the command failed: {err}");
            }
        }
        if started.elapsed() >= deadline {
            session.kill();
            captured.timed_out = true;
            break;
        }
        if last_liveness.elapsed() >= Duration::from_millis(250) {
            last_liveness = Instant::now();
            let running = task_manager
                .lock()
                .ok()
                .and_then(|guard| guard.session_state(session_id))
                .is_some_and(|state| state == "running");
            if !running {
                session.kill();
                captured.session_gone = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    captured.elapsed = started.elapsed();
    let grace = Instant::now() + READER_GRACE;
    for _ in 0..readers {
        let left = grace.saturating_duration_since(Instant::now());
        if done_rx.recv_timeout(left).is_err() {
            break;
        }
    }
    let mut guard = sink.lock().expect("run sink");
    captured.stdout = std::mem::take(&mut guard.stdout);
    captured.stderr = std::mem::take(&mut guard.stderr);
    captured.truncated = guard.truncated;
    Ok(captured)
}

/// The shell that writes one staged file: stdin to `$1`, mode `$2`.
pub(crate) const STAGE_SCRIPT: &str =
    r#"set -e; mkdir -p "$(dirname "$1")"; cat > "$1"; chmod "$2" "$1""#;

/// The shell that reads at most `$2` bytes of the file `$1`.
pub(crate) const FETCH_SCRIPT: &str = r#"[ -f "$1" ] && head -c "$2" "$1""#;

pub(crate) fn shell(script: &str, args: &[&str]) -> ExecOptions {
    let mut command = vec!["/bin/sh".to_string(), "-c".to_string(), script.to_string()];
    command.push("legion-run".to_string());
    command.extend(args.iter().map(|arg| arg.to_string()));
    ExecOptions::new(command, false)
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_string()
}

/// Read a file of the run directory through the session, at most `budget`
/// bytes of it. `None` when the command left no such file.
fn fetch(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    target: &RunTarget,
    relative: &str,
    budget: u64,
) -> R<Option<(Vec<u8>, bool)>> {
    let path = format!("{}/{relative}", target.run_dir);
    // One byte more than the budget tells a file that fits from one cut.
    let ask = budget.saturating_add(1);
    let captured = exec_capture(
        task_manager,
        session_id,
        target.local_key,
        shell(FETCH_SCRIPT, &[&path, &ask.to_string()]),
        None,
        TRANSFER_DEADLINE,
        ask,
    )?;
    if captured.exit != Some(0) {
        return Ok(None);
    }
    let mut content = captured.stdout;
    let truncated = content.len() as u64 > budget;
    content.truncate(budget as usize);
    Ok(Some((content, truncated)))
}

/// How many times the kernel OOM-killed something in a cgroup.
fn oom_kills(cgroup: &std::path::Path) -> u64 {
    std::fs::read_to_string(cgroup.join("memory.events"))
        .ok()
        .and_then(|events| {
            events.lines().find_map(|line| {
                line.strip_prefix("oom_kill ")
                    .and_then(|count| count.trim().parse().ok())
            })
        })
        .unwrap_or(0)
}

/// Why a run was stopped, if it was. A stopped run has no exit status.
fn classify(
    captured: &Captured,
    oom_killed: bool,
    run: &ResolvedRun,
    task: &TaskConfig,
) -> Option<RunStop> {
    if captured.timed_out {
        return Some(RunStop {
            reason: "wall".into(),
            limit: run.wall_text.clone(),
            detail: format!(
                "the command was still running after run.wall ({}) and was killed",
                run.wall_text
            ),
            hint: "raise run.wall on the task, or start a session and use `legion exec` for a \
                   long job"
                .into(),
        });
    }
    if oom_killed && (captured.session_gone || captured.exit == Some(137)) {
        let limit = task
            .resources
            .get("memoryMax")
            .or_else(|| task.resources.get("memory"))
            .map(|value| format!("{value} MiB"))
            .unwrap_or_default();
        return Some(RunStop {
            reason: "memory".into(),
            detail: "the kernel killed the command for exceeding the session's memory limit".into(),
            hint: "raise resources.memory (or memoryMax) on the task, or use less memory".into(),
            limit,
        });
    }
    if captured.session_gone {
        return Some(RunStop {
            reason: "session_ended".into(),
            limit: String::new(),
            detail: "the run's session ended while the command was running (stopped by an \
                     operator or a drain, or its runtime exited)"
                .into(),
            hint: "retry the run".into(),
        });
    }
    None
}

/// Stage, execute, and collect, in a session that is running. The caller
/// ends the session afterwards, whatever this returns.
fn execute(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    prepared: &Prepared,
) -> Result<RunSessionResponse, Status> {
    let gone = || {
        Status::unavailable(format!(
            "the run's session {session_id} ended before the command ran; retry"
        ))
    };
    let target = task_manager
        .lock()
        .map_err(|_| Status::internal("Task manager lock poisoned"))?
        .run_target(session_id)
        .ok_or_else(gone)?;

    // A unit with its own root is entered through its runtime, which can
    // lag the session's `running` by a moment: wait until it takes an exec.
    if !target.host_root {
        let waited = Instant::now();
        loop {
            let probe = exec_capture(
                task_manager,
                session_id,
                target.local_key,
                shell(":", &[]),
                None,
                TRANSFER_DEADLINE,
                4096,
            );
            let why = match probe {
                Ok(probe) if probe.exit == Some(0) => break,
                Ok(probe) if probe.session_gone => return Err(gone()),
                Ok(probe) => format!(
                    "{}: {}",
                    match probe.exit {
                        Some(code) => format!("exit {code}"),
                        None => "timed out".to_string(),
                    },
                    text(&probe.stderr)
                ),
                Err(err) => format!("{err:#}"),
            };
            if waited.elapsed() >= EXEC_READY_WAIT {
                return Err(Status::failed_precondition(format!(
                    "the run's session did not accept an exec within {}s ({why})\nHint: a run \
                     needs /bin/sh in the task's image",
                    EXEC_READY_WAIT.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    // Stage.
    for (relative, content, executable) in &prepared.files {
        let path = format!("{}/{relative}", target.run_dir);
        let mode = if *executable { "755" } else { "644" };
        let staged = exec_capture(
            task_manager,
            session_id,
            target.local_key,
            shell(STAGE_SCRIPT, &[&path, mode]),
            Some(content),
            TRANSFER_DEADLINE,
            4096,
        )
        .map_err(|err| {
            Status::failed_precondition(format!(
                "could not stage {relative} in the run's session: {err:#}"
            ))
        })?;
        if staged.session_gone {
            return Err(gone());
        }
        if staged.exit != Some(0) {
            return Err(Status::failed_precondition(format!(
                "could not stage {relative} at {path} in the run's session ({}): {}\nHint: \
                 staging needs /bin/sh, mkdir, cat and chmod in the task's image, and a writable \
                 {}",
                match staged.exit {
                    Some(code) => format!("exit {code}"),
                    None => "timed out".to_string(),
                },
                text(&staged.stderr),
                target.run_dir,
            )));
        }
    }

    // Execute.
    let mut opts = ExecOptions::new(rewrite_run_dir(&prepared.command, &target.run_dir), false);
    opts.env
        .insert("LEGION_RUN_DIR".into(), target.run_dir.clone());
    if prepared.has_input {
        opts.env.insert(
            "LEGION_RUN_INPUT".into(),
            format!("{}/{RUN_INPUT_FILE}", target.run_dir),
        );
    }
    if target.host_root {
        // The host has no /legion/run: the session's own directory is the
        // working directory, created even when nothing was staged.
        let dir = PathBuf::from(&target.run_dir);
        std::fs::create_dir_all(&dir).map_err(|err| {
            Status::internal(format!("could not create {}: {err}", dir.display()))
        })?;
        opts.cwd = Some(dir);
    }
    let ooms_before = target.cgroup.as_deref().map(oom_kills).unwrap_or(0);
    let captured = exec_capture(
        task_manager,
        session_id,
        target.local_key,
        opts,
        None,
        prepared.run.wall,
        prepared.run.output,
    )
    .map_err(|err| {
        Status::failed_precondition(format!(
            "could not execute {:?} in the run's session: {err:#}",
            prepared.command.first().map(String::as_str).unwrap_or("")
        ))
    })?;
    let cpu_ms = target
        .cgroup
        .as_deref()
        .and_then(crate::worker::usage::read_cgroup_sample)
        .map(|sample| sample.cpu_usage_usec / 1000)
        .unwrap_or(0);
    let oom_killed = target.cgroup.as_deref().map(oom_kills).unwrap_or(0) > ooms_before;
    let stopped = classify(&captured, oom_killed, &prepared.run, &prepared.spec.task);

    // Collect. A session that is gone has nothing left to read.
    let mut result = String::new();
    let mut files = Vec::new();
    let mut budget = prepared.run.output;
    let readable = !captured.session_gone;
    if readable {
        if let Ok(Some((content, false))) = fetch(
            task_manager,
            session_id,
            &target,
            RUN_RESULT_FILE,
            prepared.run.output,
        ) {
            if let Ok(text) = String::from_utf8(content) {
                if serde_json::from_str::<Value>(&text).is_ok() {
                    result = text.trim().to_string();
                }
            }
        }
    }
    for (asked, relative) in &prepared.get {
        let fetched = if readable {
            fetch(task_manager, session_id, &target, relative, budget)
                .ok()
                .flatten()
        } else {
            None
        };
        files.push(match fetched {
            Some((content, truncated)) => {
                budget -= content.len() as u64;
                RunOutputFile {
                    path: asked.clone(),
                    content,
                    truncated,
                    missing: false,
                }
            }
            None => RunOutputFile {
                path: asked.clone(),
                missing: true,
                ..Default::default()
            },
        });
    }

    Ok(RunSessionResponse {
        exit: if stopped.is_some() {
            None
        } else {
            captured.exit
        },
        stdout: captured.stdout,
        stderr: captured.stderr,
        truncated: captured.truncated,
        result,
        files,
        wall_ms: captured.elapsed.as_millis() as u64,
        cpu_ms,
        stopped,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn captured(exit: Option<i32>) -> Captured {
        Captured {
            exit,
            ..Default::default()
        }
    }

    fn run() -> ResolvedRun {
        ResolvedRun {
            command: vec!["true".into()],
            wall: Duration::from_secs(30),
            wall_text: "30s".into(),
            output: 1024,
            allow_command: false,
        }
    }

    fn task() -> TaskConfig {
        serde_json::from_value(serde_json::json!({
            "name": "machine",
            "runtime": "exec",
            "resources": { "cpu": 100, "memory": 64, "memoryMax": 128 },
        }))
        .unwrap()
    }

    #[test]
    fn an_exit_of_any_status_is_not_a_stop_test() {
        for code in [0, 1, 137] {
            assert!(classify(&captured(Some(code)), false, &run(), &task()).is_none());
        }
    }

    #[test]
    fn a_wall_limit_stop_names_the_limit_test() {
        let stop = classify(
            &Captured {
                timed_out: true,
                ..Default::default()
            },
            false,
            &run(),
            &task(),
        )
        .unwrap();
        assert_eq!(stop.reason, "wall");
        assert_eq!(stop.limit, "30s");
        assert!(stop.hint.contains("run.wall"));
    }

    #[test]
    fn an_oom_kill_is_a_memory_stop_test() {
        let stop = classify(&captured(Some(137)), true, &run(), &task()).unwrap();
        assert_eq!(stop.reason, "memory");
        assert_eq!(stop.limit, "128 MiB");
        // An OOM kill elsewhere in the cgroup that the command survived.
        assert!(classify(&captured(Some(0)), true, &run(), &task()).is_none());
    }

    #[test]
    fn a_session_that_ended_under_the_run_is_a_stop_test() {
        let stop = classify(
            &Captured {
                session_gone: true,
                ..Default::default()
            },
            false,
            &run(),
            &task(),
        )
        .unwrap();
        assert_eq!(stop.reason, "session_ended");
    }

    fn reply(stdout: &str) -> RunSessionResponse {
        RunSessionResponse {
            exit: Some(0),
            stdout: stdout.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn a_finished_run_is_answered_from_the_registry_test() {
        let registry = RunRegistry::new(PathBuf::from("/nonexistent/sessions.db"));
        let Begin::Run(ticket) = registry.begin("run-a", 1_000) else {
            panic!("a new id runs");
        };
        assert!(matches!(registry.begin("run-a", 1_000), Begin::Wait(_)));
        ticket.settle(&Ok(reply("42\n")), 2_000);
        let Begin::Done(kept) = registry.begin("run-a", 3_000) else {
            panic!("a finished run is kept");
        };
        assert_eq!(kept.stdout, b"42\n");
        // Past the window it runs again.
        let later = 2_000 + RUN_REPLY_TTL.as_millis() as i64 + 1;
        assert!(matches!(registry.begin("run-a", later), Begin::Run(_)));
    }

    #[test]
    fn a_failed_run_is_not_kept_and_wakes_its_waiters_test() {
        let registry = RunRegistry::new(PathBuf::from("/nonexistent/sessions.db"));
        let Begin::Run(ticket) = registry.begin("run-b", 1_000) else {
            panic!("a new id runs");
        };
        let Begin::Wait(rx) = registry.begin("run-b", 1_000) else {
            panic!("a second call waits");
        };
        ticket.settle(&Err(Status::resource_exhausted("envelope full")), 2_000);
        let outcome = rx.borrow().clone().expect("settled");
        assert_eq!(outcome.unwrap_err().0, Code::ResourceExhausted);
        assert!(matches!(registry.begin("run-b", 2_500), Begin::Run(_)));
    }

    #[test]
    fn a_dropped_ticket_frees_its_id_test() {
        let registry = RunRegistry::new(PathBuf::from("/nonexistent/sessions.db"));
        let Begin::Run(ticket) = registry.begin("run-c", 1_000) else {
            panic!("a new id runs");
        };
        drop(ticket);
        assert!(matches!(registry.begin("run-c", 1_000), Begin::Run(_)));
    }

    #[test]
    fn replies_survive_in_the_session_database_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sessions.db");
        // The session log creates the file; the registry never does.
        let first = RunRegistry::new(path.clone());
        let Begin::Run(ticket) = first.begin("run-d", 1_000) else {
            panic!("a new id runs");
        };
        ticket.settle(&Ok(reply("memory only")), 1_500);
        assert!(!path.exists());
        drop(crate::worker::session_log::SessionLog::open(&path)?);
        let Begin::Run(ticket) = first.begin("run-e", 2_000) else {
            panic!("a new id runs");
        };
        ticket.settle(&Ok(reply("kept")), 2_500);

        // A restarted worker has an empty memory and the same file.
        let second = RunRegistry::new(path);
        let Begin::Done(kept) = second.begin("run-e", 3_000) else {
            panic!("the reply is read back from sessions.db");
        };
        assert_eq!(kept.stdout, b"kept");
        let later = 2_500 + RUN_REPLY_TTL.as_millis() as i64 + 1;
        assert!(matches!(second.begin("run-e", later), Begin::Run(_)));
        Ok(())
    }

    #[test]
    fn a_workload_runs_only_what_its_task_may_test() {
        let caller = task();
        workload_may_run(&caller, &caller).unwrap();
        let mut other = task();
        other.name = "tools".into();
        let refusal = workload_may_run(&caller, &other).unwrap_err();
        assert!(refusal.contains("task machine may not run task tools"));
    }

    #[test]
    fn a_tracked_session_has_an_owner_test() {
        let registry = RunRegistry::new(PathBuf::from("/nonexistent/sessions.db"));
        assert!(!registry.owns("machine-a.1"));
        registry.track("machine-a.1");
        assert!(registry.owns("machine-a.1"));
        registry.untrack("machine-a.1");
        assert!(!registry.owns("machine-a.1"));
    }
}

/// A digest of everything in a run request that decides what runs: its
/// files, input, the paths to collect, and the command.
fn request_fingerprint(request: &RunSessionRequest) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut part = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };
    part(&(request.files.len() as u64).to_be_bytes());
    for file in &request.files {
        part(file.path.as_bytes());
        part(&file.content);
        part(&[file.executable as u8]);
    }
    match &request.input {
        Some(input) => part(input.as_bytes()),
        None => part(&[]),
    }
    part(&(request.get.len() as u64).to_be_bytes());
    for path in &request.get {
        part(path.as_bytes());
    }
    part(&(request.command.len() as u64).to_be_bytes());
    for arg in &request.command {
        part(arg.as_bytes());
    }
    // Isolate runs: where and for whom it runs is part of what it is.
    if !(request.tier.is_empty()
        && request.tenant.is_empty()
        && request.session_id.is_empty()
        && request.args.is_empty())
    {
        part(request.tier.as_bytes());
        part(request.tenant.as_bytes());
        part(request.session_id.as_bytes());
        part(&(request.args.len() as u64).to_be_bytes());
        for arg in &request.args {
            part(arg.as_bytes());
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod request_fingerprint_test {
    use super::*;

    #[test]
    fn a_reused_key_with_different_content_is_a_different_request_test() {
        let base = RunSessionRequest {
            job_id: "job".into(),
            task_name: "task".into(),
            input: Some("{\"a\":1}".into()),
            ..Default::default()
        };
        let mut other = base.clone();
        other.input = Some("{\"a\":2}".into());
        assert_eq!(
            request_fingerprint(&base),
            request_fingerprint(&base.clone())
        );
        assert_ne!(request_fingerprint(&base), request_fingerprint(&other));
        let mut with_file = base.clone();
        with_file.files.push(crate::structs::RunFile {
            path: "main.py".into(),
            content: b"print(1)".to_vec(),
            executable: false,
        });
        assert_ne!(request_fingerprint(&base), request_fingerprint(&with_file));
        // The key, ticket and node are not content.
        let mut retry = base.clone();
        retry.ticket = "t".into();
        retry.node_id = "n".into();
        assert_eq!(request_fingerprint(&base), request_fingerprint(&retry));
    }
}
