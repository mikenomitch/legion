//! The park socket: the workload says when it is waiting, and its node
//! suspends it then (docs/park.md; plan internal_docs/plans/park-plan.md).
//!
//! A task whose `suspend` block sets `park` gets `$LEGION_PARK_SOCKET`, a
//! unix stream socket only its own processes can reach: in its task
//! directory for `exec` and `container`, and inside a sandbox guest at
//! `/legion/park.sock`, which the guest agent relays to the VMM's
//! `vsock.sock_1090` on the host ([`host_socket_path`]). Each line on it is
//! a request, answered with one line of the CLI's JSON envelope:
//!
//! ```text
//! park [LABEL] [after=DURATION] [checkpoint=memory|disk]
//! unpark
//! busy
//! status
//! ```
//!
//! A `park` arms a deadline (`after`, default `suspend.park.after`). When it
//! passes with no `unpark`, `busy`, or newer `park` in between, the node
//! suspends the allocation itself, with no manager round trip first
//! ([`Suspender`]): a keyed instance through the same local suspension the
//! memory pressure response takes, reported with reason `park`; a session
//! on its own node. `busy` and a pending park keep the idle policy off
//! ([`ParkService::held`]).
//!
//! The state is per allocation, in [`ParkMachine`], which is pure so every
//! transition is tested without a socket. [`ParkService`] owns the sockets,
//! the deadlines, and the suspends. It lives in the worker's memory: a
//! worker restart re-binds every socket (the tasks keep their env) and
//! forgets pending parks, which leaves a parked task awake rather than
//! suspending one that never asked (plan Phase 5).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::prelude::*;
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::suspend::ResolvedPark;

/// The env var naming the park socket inside the task.
pub const PARK_SOCKET_ENV: &str = "LEGION_PARK_SOCKET";

/// The socket's file name in the task directory (`exec`, `container`).
pub const PARK_SOCKET_FILE: &str = "park.sock";

/// Longest request line accepted, newline included. A request is a verb
/// and a few short options; anything longer is not one.
pub const MAX_LINE: usize = 1024;

/// Connections one allocation may hold open at once. Each costs the node a
/// task; a workload needs one, a few at most.
pub const MAX_CONNECTIONS: usize = 16;

/// Longest `park` label accepted.
pub const MAX_LABEL: usize = 128;

/// The requests, for error hints.
const USAGE: &str =
    "send one of: park [LABEL] [after=DURATION] [checkpoint=memory|disk], unpark, busy, status";

/// Where the worker listens for an allocation's park socket: the VMM's
/// host socket for guest connections to vsock port 1090 on a sandbox, the
/// task directory otherwise. The directory around it (owner-only, or the
/// VMM's own) is what keeps other allocations out.
pub fn host_socket_path(runtime: &str, alloc_dir: &Path, task_dir: &Path) -> PathBuf {
    if runtime == "sandbox" {
        crate::worker::netd::guest_agent::host_socket_path(
            &crate::task_plugins::vsock_socket_base_for(alloc_dir),
            crate::worker::netd::guest_agent::PARK_PORT,
        )
    } else {
        task_dir.join(PARK_SOCKET_FILE)
    }
}

/// One request line, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Suspend once `after` passes (`None`: the task's `suspend.park.after`),
    /// with `checkpoint` (`None`: the task's default kind).
    Park {
        label: Option<String>,
        after: Option<Duration>,
        checkpoint: Option<SnapshotKind>,
    },
    Unpark,
    Busy,
    Status,
}

/// A refused request: the error half of the envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkError {
    /// A gRPC code name, like the CLI's envelope (`InvalidArgument`,
    /// `FailedPrecondition`, `ResourceExhausted`, `NotFound`).
    pub code: &'static str,
    pub message: String,
    pub hint: Option<String>,
}

impl ParkError {
    fn new(code: &'static str, message: impl Into<String>, hint: Option<&str>) -> Self {
        ParkError {
            code,
            message: message.into(),
            hint: hint.map(str::to_string),
        }
    }

    fn invalid(message: impl Into<String>, hint: Option<&str>) -> Self {
        Self::new("InvalidArgument", message, hint)
    }

    /// A refusal whose text may end in `\nHint: ...`, the way the worker's
    /// own errors carry their fix.
    fn precondition(text: &str) -> Self {
        match text.split_once("\nHint: ") {
            Some((message, hint)) => Self::new("FailedPrecondition", message, Some(hint)),
            None => Self::new("FailedPrecondition", text, None),
        }
    }

    /// `{ ok: false, error: { code, message, hint, retryable } }`.
    pub fn envelope(&self) -> Value {
        let mut error = json!({
            "code": self.code,
            "message": self.message,
            "retryable": false,
        });
        if let Some(hint) = self.hint.as_deref() {
            error["hint"] = json!(hint);
        }
        json!({ "ok": false, "error": error })
    }
}

/// Parse one request line (without its newline).
pub fn parse_request(line: &str) -> Result<Request, ParkError> {
    let mut words = line.split_whitespace();
    let Some(verb) = words.next() else {
        return Err(ParkError::invalid("empty request", Some(USAGE)));
    };
    match verb {
        "park" => {
            let mut label = None;
            let mut after = None;
            let mut checkpoint = None;
            for word in words {
                match word.split_once('=') {
                    Some(("after", raw)) => {
                        let parsed = humantime::parse_duration(raw).map_err(|_| {
                            ParkError::invalid(
                                format!("after {raw:?} is not a duration"),
                                Some("use a duration like 10s or 2m; 0s suspends at once"),
                            )
                        })?;
                        if parsed > crate::structs::suspend::MAX_PARK_AFTER {
                            return Err(ParkError::invalid(
                                format!("after {raw:?} is longer than 24h"),
                                Some(
                                    "a park suspends soon after the wait starts; use a few \
                                     seconds, or 0s to suspend at once",
                                ),
                            ));
                        }
                        after = Some(parsed);
                    }
                    Some(("checkpoint", raw)) => {
                        checkpoint =
                            Some(match raw {
                                "memory" => SnapshotKind::Memory,
                                "disk" => SnapshotKind::Disk,
                                _ => return Err(ParkError::invalid(
                                    format!("checkpoint {raw:?} is not valid"),
                                    Some(
                                        "use checkpoint=memory or checkpoint=disk, or leave it \
                                         out for the task's default",
                                    ),
                                )),
                            });
                    }
                    Some((key, _)) => {
                        return Err(ParkError::invalid(
                            format!("park has no option {key:?}"),
                            Some("park takes a label, after=, and checkpoint="),
                        ))
                    }
                    None if label.is_none() => label = Some(parse_label(word)?),
                    None => {
                        return Err(ParkError::invalid(
                            format!("park takes one label, and {word:?} would be a second"),
                            Some("options are key=value: after=10s, checkpoint=disk"),
                        ))
                    }
                }
            }
            Ok(Request::Park {
                label,
                after,
                checkpoint,
            })
        }
        "unpark" | "busy" | "status" => {
            if let Some(extra) = words.next() {
                return Err(ParkError::invalid(
                    format!("{verb} takes no arguments, got {extra:?}"),
                    Some(USAGE),
                ));
            }
            Ok(match verb {
                "unpark" => Request::Unpark,
                "busy" => Request::Busy,
                _ => Request::Status,
            })
        }
        other => Err(ParkError::invalid(
            format!("unknown request {other:?}"),
            Some(USAGE),
        )),
    }
}

/// A label goes to the node's log and back in `status`, so it is short and
/// plain: letters, digits, and `.`, `_`, `-`, `:`, `/`, `@`.
fn parse_label(word: &str) -> Result<String, ParkError> {
    let plain = word
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':' | '/' | '@'));
    if !plain || word.len() > MAX_LABEL {
        return Err(ParkError::invalid(
            format!("label {word:?} is not valid"),
            Some(
                "a label is up to 128 letters, digits, and . _ - : / @ (it names what you wait \
                 on, like ci-4821)",
            ),
        ));
    }
    Ok(word.to_string())
}

/// A park waiting for its deadline, or the suspend it started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPark {
    pub label: Option<String>,
    pub after: Duration,
    /// The kind the suspend takes, resolved when the park was accepted.
    pub checkpoint: SnapshotKind,
    /// Unix milliseconds of the deadline.
    pub suspends_at_ms: i64,
}

/// What an allocation's workload last said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParkState {
    /// Nothing, or `unpark`: the idle policy, if any, decides.
    Running,
    /// `busy`: the idle policy treats the allocation as active.
    Busy { since_ms: i64 },
    /// `park`: the allocation suspends at the deadline.
    Parked(PendingPark),
    /// The deadline passed and the suspend is running. The allocation stops
    /// when it finishes; a failed one goes back to `Running`.
    Suspending(PendingPark),
}

/// A deadline a `park` armed: call [`ParkMachine::fire`] with `generation`
/// once `delay` has passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Armed {
    pub generation: u64,
    pub delay: Duration,
}

/// One allocation's park state. Every request that changes it bumps the
/// generation, so a deadline armed for an older park finds nothing to do.
#[derive(Debug, Clone)]
pub struct ParkMachine {
    defaults: ResolvedPark,
    state: ParkState,
    generation: u64,
    last_error: Option<String>,
}

impl ParkMachine {
    pub fn new(defaults: ResolvedPark) -> Self {
        ParkMachine {
            defaults,
            state: ParkState::Running,
            generation: 0,
            last_error: None,
        }
    }

    pub fn state(&self) -> &ParkState {
        &self.state
    }

    /// Whether the idle policy should treat the allocation as active: it is
    /// busy, or a park owns when it suspends.
    pub fn holds(&self) -> bool {
        !matches!(self.state, ParkState::Running)
    }

    fn refuse_while_suspending(&self) -> Result<(), ParkError> {
        if matches!(self.state, ParkState::Suspending(_)) {
            return Err(ParkError::new(
                "FailedPrecondition",
                "the park's deadline passed and the suspend has started; this allocation stops \
                 when it finishes",
                Some("send `status` to follow it; a failed suspend leaves the task running"),
            ));
        }
        Ok(())
    }

    /// Start (or replace) a park. `checkpoint` is the kind the caller
    /// resolved for this allocation.
    pub fn park(
        &mut self,
        label: Option<String>,
        after: Option<Duration>,
        checkpoint: SnapshotKind,
        now_ms: i64,
    ) -> Result<Armed, ParkError> {
        self.refuse_while_suspending()?;
        let after = after.unwrap_or(self.defaults.after);
        let delay_ms = i64::try_from(after.as_millis()).unwrap_or(i64::MAX);
        self.generation += 1;
        self.last_error = None;
        self.state = ParkState::Parked(PendingPark {
            label,
            after,
            checkpoint,
            suspends_at_ms: now_ms.saturating_add(delay_ms),
        });
        Ok(Armed {
            generation: self.generation,
            delay: after,
        })
    }

    /// Cancel a park or a `busy`. Whether there was one to cancel.
    pub fn unpark(&mut self) -> Result<bool, ParkError> {
        self.refuse_while_suspending()?;
        let had = matches!(self.state, ParkState::Parked(_));
        self.generation += 1;
        self.state = ParkState::Running;
        Ok(had)
    }

    /// Hold the idle policy off. Replaces a pending park.
    pub fn busy(&mut self, now_ms: i64) -> Result<bool, ParkError> {
        self.refuse_while_suspending()?;
        let had = matches!(self.state, ParkState::Parked(_));
        self.generation += 1;
        if !matches!(self.state, ParkState::Busy { .. }) {
            self.state = ParkState::Busy { since_ms: now_ms };
        }
        Ok(had)
    }

    /// The deadline armed as `generation` passed: when that park is still
    /// the current state, it becomes the suspend to run.
    pub fn fire(&mut self, generation: u64) -> Option<PendingPark> {
        if generation != self.generation {
            return None;
        }
        let ParkState::Parked(park) = &self.state else {
            return None;
        };
        let park = park.clone();
        self.state = ParkState::Suspending(park.clone());
        Some(park)
    }

    /// The suspend `generation` started failed: the task keeps running,
    /// and `status` says why.
    pub fn suspend_failed(&mut self, generation: u64, error: String) {
        if generation == self.generation && matches!(self.state, ParkState::Suspending(_)) {
            self.state = ParkState::Running;
            self.last_error = Some(error);
        }
    }

    /// The `data` of every successful reply: the state after the request.
    pub fn status(&self) -> Value {
        let mut data = match &self.state {
            ParkState::Running => json!({ "state": "running" }),
            ParkState::Busy { since_ms } => json!({
                "state": "busy",
                "since": rfc3339_ms(*since_ms),
            }),
            ParkState::Parked(park) | ParkState::Suspending(park) => {
                let state = if matches!(self.state, ParkState::Parked(_)) {
                    "parked"
                } else {
                    "suspending"
                };
                let mut data = json!({
                    "state": state,
                    "after": humantime::format_duration(park.after).to_string(),
                    "checkpoint": park.checkpoint.as_str(),
                    "suspendsAt": rfc3339_ms(park.suspends_at_ms),
                });
                if let Some(label) = park.label.as_deref() {
                    data["label"] = json!(label);
                }
                data
            }
        };
        if let Some(error) = self.last_error.as_deref() {
            data["lastError"] = json!(error);
        }
        data
    }
}

fn rfc3339_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// What a park does to the allocation it belongs to. The worker's is
/// [`TaskManagerSuspender`]; tests use a fake.
pub trait Suspender: Send + Sync + 'static {
    /// The kind a park of `allocation_id` takes: `requested` when the
    /// allocation can take it, else the task's default. `Err` is a refusal,
    /// which may end in `\nHint: ...`.
    fn checkpoint_kind(
        &self,
        allocation_id: i64,
        requested: Option<SnapshotKind>,
    ) -> Result<SnapshotKind, String>;

    /// Checkpoint and stop `allocation_id` now. Blocking.
    fn suspend(&self, allocation_id: i64, kind: SnapshotKind) -> Result<(), String>;
}

/// One allocation's socket and state.
struct Slot {
    socket: PathBuf,
    machine: ParkMachine,
    /// The armed deadline, if any. One per allocation: every change aborts
    /// it, so a workload that re-parks in a loop holds one timer, not one
    /// per request.
    timer: Option<tokio::task::AbortHandle>,
    /// Flipped to `true` when the allocation goes, which ends its listener
    /// and every connection on it.
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl Slot {
    /// Start the state over (the task started again), dropping any armed
    /// deadline. `defaults` replaces the task's park settings when given.
    fn restart(&mut self, defaults: Option<ResolvedPark>) {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        self.machine = ParkMachine {
            generation: self.machine.generation + 1,
            ..ParkMachine::new(defaults.unwrap_or(self.machine.defaults))
        };
    }
}

/// Every park socket on this worker.
pub struct ParkService {
    runtime: tokio::runtime::Handle,
    suspender: Arc<dyn Suspender>,
    slots: Mutex<HashMap<i64, Slot>>,
}

impl ParkService {
    pub fn new(runtime: tokio::runtime::Handle, suspender: Arc<dyn Suspender>) -> Arc<Self> {
        Arc::new(ParkService {
            runtime,
            suspender,
            slots: Mutex::new(HashMap::new()),
        })
    }

    fn slots(&self) -> std::sync::MutexGuard<'_, HashMap<i64, Slot>> {
        self.slots.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Serve `allocation_id`'s park socket at `socket`. Opening an
    /// allocation that is already served at the same path starts its state
    /// over (its task was just started again) and keeps the listener.
    /// A leftover file at the path (a previous worker's) is replaced.
    pub fn open(
        self: &Arc<Self>,
        allocation_id: i64,
        socket: &Path,
        defaults: ResolvedPark,
    ) -> R<()> {
        {
            let mut slots = self.slots();
            if let Some(slot) = slots.get_mut(&allocation_id) {
                if slot.socket == socket && socket.exists() {
                    slot.restart(Some(defaults));
                    return Ok(());
                }
            }
        }
        self.close(allocation_id);
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let _ = std::fs::remove_file(socket);
        let listener = std::os::unix::net::UnixListener::bind(socket)
            .with_context(|| format!("bind the park socket {}", socket.display()))?;
        listener.set_nonblocking(true)?;
        // The directory is the boundary (owner-only, or the VMM's own), so
        // the socket itself is open to whichever user the task runs as.
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o666))
                .with_context(|| format!("chmod {}", socket.display()))?;
        }
        let (shutdown, watch) = tokio::sync::watch::channel(false);
        self.slots().insert(
            allocation_id,
            Slot {
                socket: socket.to_path_buf(),
                machine: ParkMachine::new(defaults),
                timer: None,
                shutdown,
            },
        );
        let service = self.clone();
        self.runtime.spawn(async move {
            service.accept(allocation_id, listener, watch).await;
        });
        debug!(allocation_id, socket = %socket.display(), "Park socket open");
        Ok(())
    }

    /// Start `allocation_id`'s state over, keeping its socket: its task was
    /// restarted in place, and a park the old process made must not suspend
    /// the new one. Any deadline already armed finds nothing to do.
    pub fn reset(&self, allocation_id: i64) {
        if let Some(slot) = self.slots().get_mut(&allocation_id) {
            slot.restart(None);
        }
    }

    /// Stop serving `allocation_id`: its listener and connections end, and
    /// the socket file goes.
    pub fn close(&self, allocation_id: i64) {
        let Some(mut slot) = self.slots().remove(&allocation_id) else {
            return;
        };
        if let Some(timer) = slot.timer.take() {
            timer.abort();
        }
        let _ = slot.shutdown.send(true);
        let _ = std::fs::remove_file(&slot.socket);
    }

    /// Allocations whose idle policy is held off: busy, or parked.
    pub fn held(&self) -> HashSet<i64> {
        self.slots()
            .iter()
            .filter(|(_, slot)| slot.machine.holds())
            .map(|(id, _)| *id)
            .collect()
    }

    /// The state of one allocation, for tests and diagnostics.
    pub fn state(&self, allocation_id: i64) -> Option<ParkState> {
        self.slots()
            .get(&allocation_id)
            .map(|slot| slot.machine.state().clone())
    }

    async fn accept(
        self: Arc<Self>,
        allocation_id: i64,
        listener: std::os::unix::net::UnixListener,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let listener = match tokio::net::UnixListener::from_std(listener) {
            Ok(listener) => listener,
            Err(err) => {
                warn!(allocation_id, error = %err, "Park socket could not be served");
                return;
            }
        };
        let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
        loop {
            let stream = tokio::select! {
                _ = shutdown.changed() => return,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(err) => {
                        debug!(allocation_id, error = %err, "Park socket accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                },
            };
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                let refusal = ParkError::new(
                    "ResourceExhausted",
                    format!("this allocation already has {MAX_CONNECTIONS} park connections open"),
                    Some("send each request on a short connection, or reuse one"),
                );
                tokio::spawn(async move {
                    let (mut read, mut write) = stream.into_split();
                    let _ = write
                        .write_all(reply_line(&refusal.envelope()).as_bytes())
                        .await;
                    drain(&mut read, &mut write).await;
                });
                continue;
            };
            let service = self.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                service.connection(allocation_id, stream, shutdown).await;
                drop(permit);
            });
        }
    }

    /// Answer each request line on one connection until it closes, the
    /// allocation goes, or a line runs past [`MAX_LINE`].
    async fn connection(
        self: Arc<Self>,
        allocation_id: i64,
        stream: tokio::net::UnixStream,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        loop {
            let mut line = Vec::new();
            let mut limited = (&mut read).take(MAX_LINE as u64);
            let read_line = limited.read_until(b'\n', &mut line);
            let n = tokio::select! {
                _ = shutdown.changed() => return,
                n = read_line => match n {
                    Ok(n) => n,
                    Err(_) => return,
                },
            };
            if n == 0 {
                return;
            }
            if line.last() != Some(&b'\n') && line.len() >= MAX_LINE {
                let refusal = ParkError::invalid(
                    format!("a request is at most {MAX_LINE} bytes"),
                    Some(USAGE),
                );
                let _ = write
                    .write_all(reply_line(&refusal.envelope()).as_bytes())
                    .await;
                drain(&mut read, &mut write).await;
                return;
            }
            let text = String::from_utf8_lossy(&line);
            if text.trim().is_empty() {
                continue;
            }
            let reply = self.handle(allocation_id, text.trim()).await;
            if write
                .write_all(reply_line(&reply).as_bytes())
                .await
                .is_err()
            {
                return;
            }
        }
    }

    /// Apply one request line and build its reply.
    pub async fn handle(self: &Arc<Self>, allocation_id: i64, line: &str) -> Value {
        let request = match parse_request(line) {
            Ok(request) => request,
            Err(err) => return err.envelope(),
        };
        // A park's kind is resolved against the running task, which reads
        // the task manager: off the async threads.
        let kind = match &request {
            Request::Park { checkpoint, .. } => {
                let suspender = self.suspender.clone();
                let requested = *checkpoint;
                match tokio::task::spawn_blocking(move || {
                    suspender.checkpoint_kind(allocation_id, requested)
                })
                .await
                {
                    Ok(Ok(kind)) => Some(kind),
                    Ok(Err(refusal)) => return ParkError::precondition(&refusal).envelope(),
                    Err(err) => {
                        return ParkError::new("Internal", err.to_string(), None).envelope()
                    }
                }
            }
            _ => None,
        };
        let now_ms = chrono::Utc::now().timestamp_millis();
        let metrics = crate::manager::telemetry::worker_metrics();
        let (result, armed, parked) = {
            let mut slots = self.slots();
            let Some(slot) = slots.get_mut(&allocation_id) else {
                return ParkError::new(
                    "NotFound",
                    "this allocation is no longer running on the node",
                    None,
                )
                .envelope();
            };
            let changes = !matches!(request, Request::Status);
            let machine = &mut slot.machine;
            let outcome = match request {
                Request::Park { label, after, .. } => {
                    let kind = kind.expect("resolved above");
                    machine.park(label, after, kind, now_ms).map(|armed| {
                        metrics.parks_total.inc(&["parked"]);
                        Some(armed)
                    })
                }
                Request::Unpark => machine.unpark().map(|had| {
                    if had {
                        metrics.parks_total.inc(&["unparked"]);
                    }
                    None
                }),
                Request::Busy => machine.busy(now_ms).map(|had| {
                    if had {
                        metrics.parks_total.inc(&["unparked"]);
                    }
                    None
                }),
                Request::Status => Ok(None),
            };
            let parked = match machine.state() {
                ParkState::Parked(park) => Some(park.clone()),
                _ => None,
            };
            let (result, armed) = match outcome {
                Ok(armed) => (Ok(machine.status()), armed),
                Err(err) => (Err(err), None),
            };
            if changes && result.is_ok() {
                if let Some(timer) = slot.timer.take() {
                    timer.abort();
                }
                slot.timer = armed.map(|armed| self.arm(allocation_id, armed));
            }
            (result, armed, parked)
        };
        if let (Some(_), Some(park)) = (armed, parked) {
            info!(
                allocation_id,
                label = park.label.as_deref().unwrap_or(""),
                after = %humantime::format_duration(park.after),
                checkpoint = park.checkpoint.as_str(),
                "Parked"
            );
        }
        match result {
            Ok(data) => json!({ "ok": true, "data": data }),
            Err(err) => err.envelope(),
        }
    }

    fn arm(self: &Arc<Self>, allocation_id: i64, armed: Armed) -> tokio::task::AbortHandle {
        let service = self.clone();
        self.runtime
            .spawn(async move {
                tokio::time::sleep(armed.delay).await;
                service.fire(allocation_id, armed.generation);
            })
            .abort_handle()
    }

    /// A deadline passed: suspend, unless a newer request replaced the park.
    fn fire(self: &Arc<Self>, allocation_id: i64, generation: u64) {
        let park = self.slots().get_mut(&allocation_id).and_then(|slot| {
            let park = slot.machine.fire(generation)?;
            slot.timer = None;
            Some(park)
        });
        let Some(park) = park else {
            return;
        };
        info!(
            allocation_id,
            label = park.label.as_deref().unwrap_or(""),
            checkpoint = park.checkpoint.as_str(),
            "Park deadline passed; suspending"
        );
        let service = self.clone();
        self.runtime.spawn_blocking(move || {
            let metrics = crate::manager::telemetry::worker_metrics();
            match service.suspender.suspend(allocation_id, park.checkpoint) {
                Ok(()) => metrics.parks_total.inc(&["suspended"]),
                Err(err) => {
                    warn!(
                        allocation_id,
                        label = park.label.as_deref().unwrap_or(""),
                        error = %err,
                        "Park suspend failed; the task keeps running"
                    );
                    metrics.parks_total.inc(&["failed"]);
                    if let Some(slot) = service.slots().get_mut(&allocation_id) {
                        slot.machine.suspend_failed(generation, err);
                    }
                }
            }
        });
    }
}

/// End a connection after a last answer: half-close, then read what the
/// client still sent, for at most a second. Closing a unix socket over
/// unread bytes resets it, and the client would lose the answer.
async fn drain<R, W>(read: &mut R, write: &mut W)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let _ = write.shutdown().await;
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        tokio::io::copy(read, &mut tokio::io::sink()),
    )
    .await;
}

fn reply_line(reply: &Value) -> String {
    let mut line = reply.to_string();
    line.push('\n');
    line
}

/// The worker's [`Suspender`]: a keyed instance through a local suspension
/// with reason `park`, a session on its node.
pub struct TaskManagerSuspender {
    manager: std::sync::Weak<Mutex<crate::worker::task_manager::WorkerTaskManager>>,
}

impl TaskManagerSuspender {
    pub fn new(manager: &Arc<Mutex<crate::worker::task_manager::WorkerTaskManager>>) -> Self {
        TaskManagerSuspender {
            manager: Arc::downgrade(manager),
        }
    }

    fn manager(
        &self,
    ) -> Result<Arc<Mutex<crate::worker::task_manager::WorkerTaskManager>>, String> {
        self.manager
            .upgrade()
            .ok_or_else(|| "the worker is stopping".to_string())
    }
}

impl Suspender for TaskManagerSuspender {
    fn checkpoint_kind(
        &self,
        allocation_id: i64,
        requested: Option<SnapshotKind>,
    ) -> Result<SnapshotKind, String> {
        let manager = self.manager()?;
        let guard = manager.lock().unwrap_or_else(|p| p.into_inner());
        guard
            .park_checkpoint_kind(allocation_id, requested)
            .map_err(|err| format!("{err:#}"))
    }

    fn suspend(&self, allocation_id: i64, kind: SnapshotKind) -> Result<(), String> {
        use crate::worker::task_manager::ParkTarget;
        let manager = self.manager()?;
        let target = manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .park_target(allocation_id)
            .map_err(|err| format!("{err:#}"))?;
        match target {
            ParkTarget::Session(session_id) => {
                crate::worker::task_manager::session_sleep::suspend_session_as(
                    &manager,
                    &session_id,
                    crate::structs::suspend::SUSPEND_REASON_PARK,
                    kind,
                )
                .map(|_| ())
            }
            // The report carries the id the manager knows; the capture
            // finds the local key from it.
            ParkTarget::Allocation(external) => {
                crate::worker::checkpoints::run_local_suspension_for(
                    &manager,
                    external,
                    crate::worker::checkpoints::LocalSuspendCause::Park { kind: Some(kind) },
                )
                .map(|_| ())
            }
        }
        .map_err(|err| format!("{err:#}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine() -> ParkMachine {
        ParkMachine::new(ResolvedPark::default())
    }

    #[test]
    fn requests_parse_with_their_options_test() {
        assert_eq!(
            parse_request("park"),
            Ok(Request::Park {
                label: None,
                after: None,
                checkpoint: None
            })
        );
        assert_eq!(
            parse_request("park ci-4821 after=30s checkpoint=disk"),
            Ok(Request::Park {
                label: Some("ci-4821".into()),
                after: Some(Duration::from_secs(30)),
                checkpoint: Some(SnapshotKind::Disk),
            })
        );
        // Options in any order, the label anywhere.
        assert_eq!(
            parse_request("park checkpoint=memory after=0s build/42"),
            Ok(Request::Park {
                label: Some("build/42".into()),
                after: Some(Duration::ZERO),
                checkpoint: Some(SnapshotKind::Memory),
            })
        );
        assert_eq!(parse_request("unpark"), Ok(Request::Unpark));
        assert_eq!(parse_request("  busy  "), Ok(Request::Busy));
        assert_eq!(parse_request("status"), Ok(Request::Status));
    }

    #[test]
    fn bad_requests_are_refused_with_a_hint_test() {
        for (line, message) in [
            ("", "empty request"),
            ("sleep", "unknown request \"sleep\""),
            ("park after=soon", "after \"soon\" is not a duration"),
            ("park checkpoint=cpu", "checkpoint \"cpu\" is not valid"),
            ("park until=5m", "park has no option \"until\""),
            ("park after=2d", "after \"2d\" is longer than 24h"),
            (
                "park a b",
                "park takes one label, and \"b\" would be a second",
            ),
            ("park ci$(id)", "label \"ci$(id)\" is not valid"),
            ("status now", "status takes no arguments, got \"now\""),
        ] {
            let err = parse_request(line).unwrap_err();
            assert_eq!(err.code, "InvalidArgument", "{line}");
            assert_eq!(err.message, message, "{line}");
            assert!(err.hint.is_some(), "{line}");
        }
        let long = format!("park {}", "a".repeat(MAX_LABEL + 1));
        assert!(parse_request(&long).is_err());
    }

    #[test]
    fn the_error_envelope_matches_the_cli_test() {
        let envelope = ParkError::precondition(
            "this allocation cannot take a memory checkpoint\nHint: park with checkpoint=disk",
        )
        .envelope();
        assert_eq!(
            envelope,
            json!({
                "ok": false,
                "error": {
                    "code": "FailedPrecondition",
                    "message": "this allocation cannot take a memory checkpoint",
                    "hint": "park with checkpoint=disk",
                    "retryable": false,
                }
            })
        );
        let plain = ParkError::precondition("no hint here").envelope();
        assert!(plain["error"].get("hint").is_none());
    }

    #[test]
    fn a_park_arms_its_deadline_and_fires_once_test() {
        let mut park = machine();
        assert!(!park.holds());
        let armed = park
            .park(Some("ci".into()), None, SnapshotKind::Disk, 1_000)
            .unwrap();
        assert_eq!(armed.delay, Duration::from_secs(10), "the task's default");
        assert!(park.holds(), "a pending park keeps the idle policy off");
        assert_eq!(
            park.status(),
            json!({
                "state": "parked",
                "label": "ci",
                "after": "10s",
                "checkpoint": "disk",
                "suspendsAt": "1970-01-01T00:00:11Z",
            })
        );
        let fired = park.fire(armed.generation).expect("the park is current");
        assert_eq!(fired.checkpoint, SnapshotKind::Disk);
        assert_eq!(park.status()["state"], "suspending");
        assert!(park.fire(armed.generation).is_none(), "fires once");
    }

    /// A deadline armed for a park that was replaced, cancelled, or turned
    /// into `busy` finds nothing to do.
    #[test]
    fn a_stale_deadline_does_nothing_test() {
        let mut park = machine();
        let first = park
            .park(None, Some(Duration::from_secs(5)), SnapshotKind::Disk, 0)
            .unwrap();
        let second = park
            .park(None, Some(Duration::from_secs(60)), SnapshotKind::Disk, 0)
            .unwrap();
        assert!(park.fire(first.generation).is_none());
        assert_eq!(park.status()["after"], "1m");

        assert_eq!(park.unpark(), Ok(true));
        assert!(park.fire(second.generation).is_none());
        assert_eq!(park.state(), &ParkState::Running);
        assert_eq!(park.unpark(), Ok(false), "nothing left to cancel");

        let third = park.park(None, None, SnapshotKind::Memory, 0).unwrap();
        assert_eq!(park.busy(5), Ok(true), "busy replaces the park");
        assert!(park.fire(third.generation).is_none());
        assert_eq!(
            park.status(),
            json!({ "state": "busy", "since": "1970-01-01T00:00:00Z" })
        );
        assert!(park.holds());
    }

    /// Once the suspend has started, nothing can take it back. A failed one
    /// leaves the task running with the reason in `status`, and the next
    /// park clears it.
    #[test]
    fn a_suspend_in_flight_refuses_changes_and_a_failure_is_reported_test() {
        let mut park = machine();
        let armed = park
            .park(None, Some(Duration::ZERO), SnapshotKind::Disk, 0)
            .unwrap();
        park.fire(armed.generation).unwrap();
        for refused in [
            park.unpark().map(|_| ()),
            park.busy(0).map(|_| ()),
            park.park(None, None, SnapshotKind::Disk, 0).map(|_| ()),
        ] {
            assert_eq!(refused.unwrap_err().code, "FailedPrecondition");
        }
        park.suspend_failed(armed.generation, "capture failed: disk full".into());
        assert_eq!(
            park.status(),
            json!({ "state": "running", "lastError": "capture failed: disk full" })
        );
        assert!(!park.holds());
        park.park(None, None, SnapshotKind::Disk, 0).unwrap();
        assert!(park.status().get("lastError").is_none());
    }

    /// A fake suspender that records what it was asked, and can refuse
    /// memory checkpoints or fail the suspend.
    #[derive(Default)]
    struct Fake {
        disk_only: bool,
        fail: bool,
        suspended: Mutex<Vec<(i64, SnapshotKind)>>,
    }

    impl Suspender for Fake {
        fn checkpoint_kind(
            &self,
            _allocation_id: i64,
            requested: Option<SnapshotKind>,
        ) -> Result<SnapshotKind, String> {
            match requested {
                Some(SnapshotKind::Memory) if self.disk_only => Err(
                    "this allocation cannot take a memory checkpoint: runtime \"exec\" supports \
                     \"disk\" only\nHint: park with checkpoint=disk"
                        .into(),
                ),
                Some(kind) => Ok(kind),
                None => Ok(SnapshotKind::Disk),
            }
        }

        fn suspend(&self, allocation_id: i64, kind: SnapshotKind) -> Result<(), String> {
            if self.fail {
                return Err("capture failed".into());
            }
            self.suspended.lock().unwrap().push((allocation_id, kind));
            Ok(())
        }
    }

    async fn request(socket: &Path, lines: &str) -> Vec<Value> {
        let stream = tokio::net::UnixStream::connect(socket)
            .await
            .expect("connect");
        let (read, mut write) = stream.into_split();
        write.write_all(lines.as_bytes()).await.unwrap();
        write.shutdown().await.unwrap();
        let mut replies = Vec::new();
        let mut reader = BufReader::new(read).lines();
        while let Some(line) = reader.next_line().await.unwrap() {
            replies.push(serde_json::from_str(&line).expect("a JSON reply"));
        }
        replies
    }

    async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        for _ in 0..200 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// The socket end to end: a park that fires suspends with the kind it
    /// named, an unpark before the deadline suspends nothing, and a refused
    /// memory checkpoint is an error reply that changes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_socket_parks_unparks_and_suspends_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join(PARK_SOCKET_FILE);
        let fake = Arc::new(Fake {
            disk_only: true,
            ..Default::default()
        });
        let service = ParkService::new(tokio::runtime::Handle::current(), fake.clone());
        service.open(7, &socket, ResolvedPark::default()).unwrap();

        // Several requests on one connection, each answered in order.
        let replies = request(&socket, "status\npark ci after=1h\nunpark\n").await;
        assert_eq!(replies.len(), 3);
        assert_eq!(
            replies[0],
            json!({ "ok": true, "data": { "state": "running" } })
        );
        assert_eq!(replies[1]["data"]["state"], "parked");
        assert_eq!(replies[1]["data"]["label"], "ci");
        assert_eq!(replies[2]["data"]["state"], "running");

        let refused = request(&socket, "park checkpoint=memory\n").await;
        assert_eq!(refused[0]["ok"], false);
        assert_eq!(refused[0]["error"]["code"], "FailedPrecondition");
        assert_eq!(refused[0]["error"]["hint"], "park with checkpoint=disk");
        assert_eq!(service.state(7), Some(ParkState::Running));

        let parked = request(&socket, "park build after=0s\n").await;
        assert_eq!(parked[0]["data"]["checkpoint"], "disk");
        wait_for("the suspend", || !fake.suspended.lock().unwrap().is_empty()).await;
        assert_eq!(
            *fake.suspended.lock().unwrap(),
            vec![(7, SnapshotKind::Disk)]
        );
        let after = request(&socket, "status\n").await;
        assert_eq!(after[0]["data"]["state"], "suspending");

        // Started again (a resume, an in-place restart): the state starts
        // over on the same socket.
        service.open(7, &socket, ResolvedPark::default()).unwrap();
        assert_eq!(service.state(7), Some(ParkState::Running));

        service.close(7);
        assert!(!socket.exists(), "the socket file goes with the allocation");
        assert!(service.state(7).is_none());
    }

    /// A task restarted in place starts with no park: the deadline the old
    /// process armed finds nothing to do, and the socket keeps serving.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_drops_the_old_process_park_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join(PARK_SOCKET_FILE);
        let fake = Arc::new(Fake::default());
        let service = ParkService::new(tokio::runtime::Handle::current(), fake.clone());
        service
            .open(
                5,
                &socket,
                ResolvedPark {
                    after: Duration::from_millis(200),
                },
            )
            .unwrap();
        request(&socket, "park\n").await;
        assert_eq!(service.held(), HashSet::from([5]));
        service.reset(5);
        assert_eq!(service.state(5), Some(ParkState::Running));
        assert!(service.held().is_empty());
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            fake.suspended.lock().unwrap().is_empty(),
            "the old deadline is stale"
        );
        let status = request(&socket, "status\n").await;
        assert_eq!(status[0]["data"]["state"], "running");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_suspend_leaves_the_task_running_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join(PARK_SOCKET_FILE);
        let fake = Arc::new(Fake {
            fail: true,
            ..Default::default()
        });
        let service = ParkService::new(tokio::runtime::Handle::current(), fake);
        service.open(9, &socket, ResolvedPark::default()).unwrap();
        request(&socket, "park after=0s\n").await;
        wait_for("the failure", || {
            service.state(9) == Some(ParkState::Running)
        })
        .await;
        let status = request(&socket, "status\n").await;
        assert_eq!(status[0]["data"]["lastError"], "capture failed");
        assert!(service.held().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn busy_and_pending_parks_are_held_test() {
        let dir = tempfile::tempdir().unwrap();
        let fake = Arc::new(Fake::default());
        let service = ParkService::new(tokio::runtime::Handle::current(), fake);
        for (id, name) in [(1, "a.sock"), (2, "b.sock"), (3, "c.sock")] {
            service
                .open(id, &dir.path().join(name), ResolvedPark::default())
                .unwrap();
        }
        request(&dir.path().join("a.sock"), "busy\n").await;
        request(&dir.path().join("b.sock"), "park after=1h\n").await;
        assert_eq!(service.held(), HashSet::from([1, 2]));
    }

    /// A line longer than [`MAX_LINE`] is refused and the connection ends;
    /// the socket keeps serving.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_overlong_line_is_refused_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join(PARK_SOCKET_FILE);
        let service =
            ParkService::new(tokio::runtime::Handle::current(), Arc::new(Fake::default()));
        service.open(4, &socket, ResolvedPark::default()).unwrap();
        let long = format!("park {}\nstatus\n", "x".repeat(MAX_LINE * 2));
        let replies = request(&socket, &long).await;
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0]["error"]["code"], "InvalidArgument");
        let status = request(&socket, "status\n").await;
        assert_eq!(status[0]["ok"], true);
    }

    #[test]
    fn the_socket_path_follows_the_runtime_test() {
        let alloc = Path::new("/var/lib/legion/allocs/7");
        let task = alloc.join("agent");
        assert_eq!(
            host_socket_path("exec", alloc, &task),
            PathBuf::from("/var/lib/legion/allocs/7/agent/park.sock")
        );
        assert_eq!(
            host_socket_path("container", alloc, &task),
            PathBuf::from("/var/lib/legion/allocs/7/agent/park.sock")
        );
        assert_eq!(
            host_socket_path("sandbox", alloc, &task),
            PathBuf::from("/var/lib/legion/allocs/7/.sandbox/vsock.sock_1090")
        );
    }
}
