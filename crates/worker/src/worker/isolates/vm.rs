//! Child VMs owned by isolates (`legion:isolate/vm`).
//!
//! A child VM is a child session of the isolate task's VM task
//! (`config.vm.task`, listed in the isolate task's `children.tasks`), started
//! on this node through the same machinery a workload's
//! `StartChildSession` uses (docs/child-sessions.md): origin `workload`, a
//! launch slot, admission against the VM task's envelope, and the isolate
//! task's `children.max` per owner.
//!
//! **Owners.** An isolate is not a claim, so it registers itself with the
//! task manager as an owner of children while it lives
//! (`WorkerTaskManager::register_isolate_owner`) and the child machinery
//! consults that registry: admission finds the isolate task's `children`
//! block through it, the child's `SessionStatus` carries the reference, and
//! `reconcile_children` keeps the children of a registered owner and ends
//! them once it is unregistered.
//!
//! - A session isolate owns its children as `{kind: "session", id: <its
//!   session id>}`: they outlive its invocations and its hibernation, and a
//!   later invocation re-attaches by id.
//! - A request or tenant isolate owns them as `{kind: "isolate", id: <the
//!   invocation id>}`: they end with the invocation.
//!
//! [`VmBackend::owner_ended`] ends an owner's children and forgets it. A
//! worker restart forgets every registration: the supervisor re-registers
//! the session isolates it restores ([`ChildVms::register_owner`]) before
//! the first child reconcile, or their children are ended as orphans.

use crate::prelude::*;
use crate::structs::children::{owner_ref, OWNER_SESSION};
use crate::structs::isolate_host::{
    Header, HttpCall, HttpOutcome, VmError, VmErrorKind, VmExecResult, VmReply, VmRequest,
};
use crate::structs::OwnerRef;
use crate::worker::isolates::egress::{
    collect_limited, connection_tokens, inbound_headers, is_hop_by_hop, BodyError,
};
use crate::worker::isolates::{IsolateTaskRef, VmBackend};
use crate::worker::launch_pool::LaunchPool;
use crate::worker::task_manager::children::{IsolateOwner, OWNER_ISOLATE};
use crate::worker::task_manager::session_runs::{
    exec_capture, shell, Captured, RunTarget, FETCH_SCRIPT, STAGE_SCRIPT,
};
use crate::worker::task_manager::WorkerTaskManager;

use bytes::Bytes;
use http_body_util::Full;
use hyper::{header, Method, Request};
use hyper_util::rt::TokioIo;
use std::time::Duration;

/// How long `acquire` waits for the child to run (a VM boot).
const START_WAIT: Duration = Duration::from_secs(60);
/// How long a child that runs may take to accept its first exec.
const EXEC_READY_WAIT: Duration = Duration::from_secs(20);
/// The timeout an `exec` gets when the guest names none, and the most it
/// may ask for.
const DEFAULT_EXEC_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_EXEC_TIMEOUT: Duration = Duration::from_secs(600);
/// How long staging or fetching one file may take.
const TRANSFER_DEADLINE: Duration = Duration::from_secs(60);
/// How long one `fetch` to a child's port may take.
const FETCH_DEADLINE: Duration = Duration::from_secs(60);

/// `VmBackend` over the node's child-session machinery.
pub struct ChildVms {
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    node_id: String,
    launch_pool: Arc<LaunchPool>,
    max_body_bytes: u64,
}

impl ChildVms {
    /// Child VMs on this node: started through `task_manager`, under
    /// `launch_pool`, with `exec` output and `put`/`get`/`fetch` bodies
    /// bounded by `max_body_bytes` (`isolate.maxBodyBytes`).
    pub fn new(
        task_manager: Arc<Mutex<WorkerTaskManager>>,
        node_id: impl Into<String>,
        launch_pool: Arc<LaunchPool>,
        max_body_bytes: u64,
    ) -> Self {
        ChildVms {
            task_manager,
            node_id: node_id.into(),
            launch_pool,
            max_body_bytes,
        }
    }

    /// The owner reference an isolate's children carry.
    pub fn owner_of(task: &IsolateTaskRef) -> OwnerRef {
        if task.session {
            owner_ref(OWNER_SESSION, &task.isolate_id)
        } else {
            owner_ref(OWNER_ISOLATE, &task.isolate_id)
        }
    }

    /// Register an isolate as a live owner of children. `acquire` does this
    /// itself; the supervisor calls it for the session isolates it restores
    /// after a worker restart, so their children are kept.
    pub fn register_owner(&self, task: &IsolateTaskRef) {
        let owner = Self::owner_of(task);
        let mut guard = lock(&self.task_manager);
        if guard.isolate_owner(&owner).is_none() {
            guard.register_isolate_owner(IsolateOwner {
                owner,
                job_id: task.job_id.clone(),
                task_name: task.task_name.clone(),
            });
        }
    }

    async fn acquire(&self, task: &IsolateTaskRef, vm_task: &str) -> VmReply {
        self.register_owner(task);
        let owner = Self::owner_of(task);
        let slot = match self.launch_pool.acquire().await {
            Ok(slot) => slot,
            Err(full) => return failure(VmErrorKind::VmResourceExhausted, full.to_string()),
        };
        let task_manager = self.task_manager.clone();
        let node_id = self.node_id.clone();
        let job_id = task.job_id.clone();
        let child_task = vm_task.to_string();
        let start_owner = owner.clone();
        let started = tokio::task::spawn_blocking(move || {
            crate::worker::edge::start_local_session(
                &task_manager,
                &node_id,
                crate::worker::edge::LocalStart {
                    job_id: &job_id,
                    task_name: &child_task,
                    origin: "workload",
                    ticket_jti: "",
                    idempotency_key: "",
                    owner: Some(start_owner),
                },
                slot,
            )
        })
        .await;
        let started = match started {
            Ok(Ok(started)) => started,
            Ok(Err(err)) => {
                let status = crate::worker::server::admission_status(&err);
                let kind = match status.code() {
                    tonic::Code::ResourceExhausted => VmErrorKind::VmResourceExhausted,
                    _ => VmErrorKind::VmFailed,
                };
                return failure(kind, status.message().to_string());
            }
            Err(err) => {
                return failure(VmErrorKind::VmFailed, format!("the start panicked: {err}"))
            }
        };
        let id = started.session_id;
        info!(
            job_id = %task.job_id,
            task = %task.task_name,
            isolate_id = %task.isolate_id,
            vm_task,
            session_id = %id,
            "Starting a child VM for an isolate"
        );
        match crate::worker::edge::wait_until_running(&self.task_manager, &id, START_WAIT).await {
            Ok(true) => {}
            Ok(false) => {
                self.stop(&id, "did not start in time").await;
                return failure(
                    VmErrorKind::VmFailed,
                    format!("child {id} did not start within {}s", START_WAIT.as_secs()),
                );
            }
            Err(err) => {
                return failure(
                    VmErrorKind::VmFailed,
                    format!("the child failed to start: {err:#}"),
                )
            }
        }
        // A unit with its own root is entered through its runtime, which
        // can lag `running` by a moment (as for session runs).
        if let Err(reply) = self.wait_exec_ready(&owner, &id).await {
            return reply;
        }
        VmReply {
            id,
            ..Default::default()
        }
    }

    async fn wait_exec_ready(&self, owner: &OwnerRef, id: &str) -> Result<(), VmReply> {
        let target = self.running_target(owner, id)?;
        if target.host_root {
            return Ok(());
        }
        let started = std::time::Instant::now();
        loop {
            let probe = self
                .capture(id, &target, shell(":", &[]), None, TRANSFER_DEADLINE, 4096)
                .await;
            let why = match probe {
                Ok(probe) if probe.exit == Some(0) => return Ok(()),
                Ok(probe) if probe.session_gone => return Err(self.gone_reply(owner, id)),
                Ok(probe) => format!(
                    "exit {:?}: {}",
                    probe.exit,
                    String::from_utf8_lossy(&probe.stderr).trim()
                ),
                Err(err) => format!("{err:#}"),
            };
            if started.elapsed() >= EXEC_READY_WAIT {
                return Err(failure(
                    VmErrorKind::VmFailed,
                    format!(
                        "child {id} did not accept an exec within {}s ({why}); the VM task's image needs /bin/sh",
                        EXEC_READY_WAIT.as_secs()
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The child is this owner's: Ok, or the `VM_CHILD_GONE` reply naming
    /// why.
    fn owned(&self, owner: &OwnerRef, id: &str) -> Result<(), VmReply> {
        let guard = lock(&self.task_manager);
        if guard
            .child_link(id)
            .is_some_and(|link| link.owner.kind == owner.kind && link.owner.id == owner.id)
        {
            return Ok(());
        }
        drop(guard);
        Err(self.gone_reply(owner, id))
    }

    /// Why a child is not (or no longer) this owner's.
    fn gone_reply(&self, owner: &OwnerRef, id: &str) -> VmReply {
        let mut guard = lock(&self.task_manager);
        guard.reconcile_children();
        let message = match guard.gone_child(id) {
            Some(gone) if gone.owner.kind == owner.kind && gone.owner.id == owner.id => {
                format!("child {id} is gone: {}", gone.reason)
            }
            _ if guard
                .child_link(id)
                .is_some_and(|link| link.owner.kind == owner.kind && link.owner.id == owner.id) =>
            {
                match guard.session_state(id).as_deref() {
                    Some("suspended") => format!(
                        "child {id} is suspended by its own idle policy; it is not reachable until resumed"
                    ),
                    Some(state) => format!("child {id} is {state}, not running"),
                    None => format!("child {id} is not running"),
                }
            }
            _ => format!("no child {id} of this isolate on this node"),
        };
        failure(VmErrorKind::VmChildGone, message)
    }

    /// The run target of an owned, running child.
    fn running_target(&self, owner: &OwnerRef, id: &str) -> Result<RunTarget, VmReply> {
        self.owned(owner, id)?;
        let target = lock(&self.task_manager).run_target(id);
        target.ok_or_else(|| self.gone_reply(owner, id))
    }

    async fn capture(
        &self,
        id: &str,
        target: &RunTarget,
        opts: crate::task_plugins::ExecOptions,
        stdin: Option<Vec<u8>>,
        deadline: Duration,
        budget: u64,
    ) -> R<Captured> {
        let task_manager = self.task_manager.clone();
        let id = id.to_string();
        let local_key = target.local_key;
        tokio::task::spawn_blocking(move || {
            exec_capture(
                &task_manager,
                &id,
                local_key,
                opts,
                stdin.as_deref(),
                deadline,
                budget,
            )
        })
        .await
        .map_err(|err| anyhow!("the exec panicked: {err}"))?
    }

    async fn exec(
        &self,
        owner: &OwnerRef,
        id: &str,
        argv: Vec<String>,
        stdin: Vec<u8>,
        timeout_millis: u32,
    ) -> VmReply {
        if argv.is_empty() || argv[0].is_empty() {
            return failure(VmErrorKind::VmFailed, "exec needs a program: argv is empty");
        }
        let target = match self.running_target(owner, id) {
            Ok(target) => target,
            Err(reply) => return reply,
        };
        let timeout = exec_timeout(timeout_millis);
        let opts = crate::task_plugins::ExecOptions::new(argv, false);
        let captured = match self
            .capture(id, &target, opts, Some(stdin), timeout, self.max_body_bytes)
            .await
        {
            Ok(captured) => captured,
            Err(err) => return failure(VmErrorKind::VmFailed, format!("exec in {id}: {err:#}")),
        };
        if captured.session_gone {
            return self.gone_reply(owner, id);
        }
        VmReply {
            id: id.to_string(),
            exec: Some(VmExecResult {
                exit: captured.exit.unwrap_or(-1),
                stdout: captured.stdout,
                stderr: captured.stderr,
                timed_out: captured.timed_out,
            }),
            ..Default::default()
        }
    }

    async fn put(&self, owner: &OwnerRef, id: &str, path: &str, content: Vec<u8>) -> VmReply {
        if content.len() as u64 > self.max_body_bytes {
            return failure(
                VmErrorKind::VmFailed,
                format!(
                    "{path} is {} bytes, over this node's isolate.maxBodyBytes ({})",
                    content.len(),
                    self.max_body_bytes
                ),
            );
        }
        let target = match self.running_target(owner, id) {
            Ok(target) => target,
            Err(reply) => return reply,
        };
        let full = match child_path(&target, path) {
            Ok(full) => full,
            Err(message) => return failure(VmErrorKind::VmFailed, message),
        };
        let staged = self
            .capture(
                id,
                &target,
                shell(STAGE_SCRIPT, &[&full, "644"]),
                Some(content),
                TRANSFER_DEADLINE,
                4096,
            )
            .await;
        match staged {
            Ok(staged) if staged.session_gone => self.gone_reply(owner, id),
            Ok(staged) if staged.exit == Some(0) => VmReply {
                id: id.to_string(),
                ..Default::default()
            },
            Ok(staged) => failure(
                VmErrorKind::VmFailed,
                format!(
                    "could not write {full} in {id}: {}",
                    String::from_utf8_lossy(&staged.stderr).trim()
                ),
            ),
            Err(err) => failure(VmErrorKind::VmFailed, format!("put in {id}: {err:#}")),
        }
    }

    async fn get(&self, owner: &OwnerRef, id: &str, path: &str) -> VmReply {
        let target = match self.running_target(owner, id) {
            Ok(target) => target,
            Err(reply) => return reply,
        };
        let full = match child_path(&target, path) {
            Ok(full) => full,
            Err(message) => return failure(VmErrorKind::VmFailed, message),
        };
        // One byte over the cap tells a file that fits from one cut.
        let ask = self.max_body_bytes.saturating_add(1);
        let fetched = self
            .capture(
                id,
                &target,
                shell(FETCH_SCRIPT, &[&full, &ask.to_string()]),
                None,
                TRANSFER_DEADLINE,
                ask,
            )
            .await;
        match fetched {
            Ok(fetched) if fetched.session_gone => self.gone_reply(owner, id),
            Ok(fetched) if fetched.exit != Some(0) => failure(
                VmErrorKind::VmFailed,
                format!("{id} has no readable file at {full}"),
            ),
            Ok(fetched) if fetched.stdout.len() as u64 > self.max_body_bytes => failure(
                VmErrorKind::VmFailed,
                format!(
                    "{full} in {id} is over this node's isolate.maxBodyBytes ({})",
                    self.max_body_bytes
                ),
            ),
            Ok(fetched) => VmReply {
                id: id.to_string(),
                content: fetched.stdout,
                ..Default::default()
            },
            Err(err) => failure(VmErrorKind::VmFailed, format!("get in {id}: {err:#}")),
        }
    }

    async fn fetch(&self, owner: &OwnerRef, id: &str, port: u32, call: HttpCall) -> VmReply {
        if let Err(reply) = self.owned(owner, id) {
            return reply;
        }
        let Ok(port) = u16::try_from(port) else {
            return failure(VmErrorKind::VmFailed, format!("{port} is not a TCP port"));
        };
        if call.body.len() as u64 > self.max_body_bytes {
            return failure(
                VmErrorKind::VmFailed,
                format!(
                    "the request body is over this node's isolate.maxBodyBytes ({})",
                    self.max_body_bytes
                ),
            );
        }
        let dial = lock(&self.task_manager).isolate_child_dial(owner, id, port);
        let target = match dial {
            Ok(target) => target,
            Err(message) => {
                let running = lock(&self.task_manager).run_target(id).is_some();
                return if running {
                    failure(VmErrorKind::VmFailed, message)
                } else {
                    self.gone_reply(owner, id)
                };
            }
        };
        match tokio::time::timeout(FETCH_DEADLINE, self.send(target, port, call)).await {
            Ok(Ok(response)) => VmReply {
                id: id.to_string(),
                response: Some(response),
                ..Default::default()
            },
            Ok(Err(message)) => failure(
                VmErrorKind::VmFailed,
                format!("fetch {id}:{port}: {message}"),
            ),
            Err(_) => failure(
                VmErrorKind::VmFailed,
                format!(
                    "fetch {id}:{port}: no answer within {}s",
                    FETCH_DEADLINE.as_secs()
                ),
            ),
        }
    }

    async fn send(
        &self,
        target: crate::worker::netd::ingress::TaskTarget,
        port: u16,
        call: HttpCall,
    ) -> Result<HttpOutcome, String> {
        let stream = target
            .connect()
            .await
            .map_err(|err| format!("nothing answers on port {port}: {err:#}"))?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|err| err.to_string())?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let method = Method::from_bytes(call.method.as_bytes())
            .map_err(|_| format!("{:?} is not an HTTP method", call.method))?;
        let path = if call.path.is_empty() {
            "/".to_string()
        } else {
            call.path.clone()
        };
        let mut builder = Request::builder().method(method).uri(path.as_str());
        let headers = builder
            .headers_mut()
            .ok_or_else(|| format!("{path:?} is not a request target"))?;
        let connection = connection_tokens(
            call.headers
                .iter()
                .filter(|h| h.name.eq_ignore_ascii_case("connection"))
                .map(|h| h.value.as_slice()),
        );
        for h in &call.headers {
            if is_hop_by_hop(&h.name, &connection)
                || h.name.eq_ignore_ascii_case("host")
                || h.name.eq_ignore_ascii_case("content-length")
            {
                continue;
            }
            let name = header::HeaderName::from_bytes(h.name.as_bytes())
                .map_err(|_| format!("{:?} is not a valid header name", h.name))?;
            let value = header::HeaderValue::from_bytes(&h.value)
                .map_err(|_| format!("the value of header {} is not valid", h.name))?;
            headers.append(name, value);
        }
        let host = if call.authority.is_empty() {
            format!("localhost:{port}")
        } else {
            call.authority.clone()
        };
        headers.insert(
            header::HOST,
            header::HeaderValue::from_str(&host).map_err(|_| format!("{host:?} is not a Host"))?,
        );
        if !call.body.is_empty() {
            headers.insert(
                header::CONTENT_LENGTH,
                header::HeaderValue::from(call.body.len()),
            );
        }
        let request = builder
            .body(Full::new(Bytes::from(call.body)))
            .map_err(|err| err.to_string())?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|err| format!("the connection closed without a response: {err}"))?;
        let status = u32::from(response.status().as_u16());
        let headers: Vec<Header> = inbound_headers(response.headers());
        let body = collect_limited(response.into_body(), self.max_body_bytes)
            .await
            .map_err(|err| match err {
                BodyError::TooLarge => format!(
                    "the response body is over this node's isolate.maxBodyBytes ({})",
                    self.max_body_bytes
                ),
                BodyError::Read(err) => format!("reading the response body: {err}"),
            })?;
        Ok(HttpOutcome {
            status,
            headers,
            body: body.to_vec(),
        })
    }

    fn resize(
        &self,
        owner: &OwnerRef,
        id: &str,
        memory_mib: u32,
        memory_max_mib: Option<u32>,
        cpu_mhz: Option<u32>,
    ) -> VmReply {
        use crate::structs::resize::ResizeOutcome;
        use crate::worker::task_manager::resize::ResizeTarget;
        if let Err(reply) = self.owned(owner, id) {
            return reply;
        }
        let size = crate::structs::resize::ResourceSize {
            memory: (memory_mib > 0).then_some(f64::from(memory_mib)),
            memory_max: memory_max_mib.map(f64::from),
            cpu: cpu_mhz.map(f64::from),
        };
        let done = lock(&self.task_manager).resize_unit(
            &ResizeTarget::Session(id.to_string()),
            &size,
            false,
            false,
        );
        match done {
            Ok(done) => match done.outcome {
                ResizeOutcome::Applied => VmReply {
                    id: id.to_string(),
                    ..Default::default()
                },
                ResizeOutcome::NeedsRestart => failure(VmErrorKind::VmNeedsRestart, done.message),
                ResizeOutcome::Deferred | ResizeOutcome::Refused => {
                    failure(VmErrorKind::VmResourceExhausted, done.message)
                }
                ResizeOutcome::NotSupported => failure(VmErrorKind::VmFailed, done.message),
            },
            Err(err) => {
                let status = crate::worker::task_manager::resize::resize_status(err);
                match status.code() {
                    tonic::Code::NotFound => self.gone_reply(owner, id),
                    _ => failure(VmErrorKind::VmFailed, status.message().to_string()),
                }
            }
        }
    }

    async fn release(&self, owner: &OwnerRef, id: &str) -> VmReply {
        if let Err(reply) = self.owned(owner, id) {
            // Releasing what already ended is done.
            let gone_here = lock(&self.task_manager)
                .gone_child(id)
                .is_some_and(|gone| gone.owner.kind == owner.kind && gone.owner.id == owner.id);
            return if gone_here {
                VmReply {
                    id: id.to_string(),
                    ..Default::default()
                }
            } else {
                reply
            };
        }
        self.stop(id, "released by its isolate").await;
        VmReply {
            id: id.to_string(),
            ..Default::default()
        }
    }

    async fn stop(&self, id: &str, reason: &str) {
        let task_manager = self.task_manager.clone();
        let ids = vec![id.to_string()];
        let reason = reason.to_string();
        let _ = tokio::task::spawn_blocking(move || {
            let mut guard = lock(&task_manager);
            guard.stop_sessions(&ids, &reason);
            guard.reconcile_children();
        })
        .await;
    }
}

#[tonic::async_trait]
impl VmBackend for ChildVms {
    async fn call(
        &self,
        task: &IsolateTaskRef,
        vm_task: Option<&str>,
        request: VmRequest,
    ) -> VmReply {
        use crate::structs::isolate_host::vm_request::Op;
        let Some(vm_task) = vm_task.filter(|task| !task.is_empty()) else {
            return failure(
                VmErrorKind::VmNotConfigured,
                format!(
                    "isolate task {} has no VM task (config.vm.task)",
                    task.task_name
                ),
            );
        };
        let owner = Self::owner_of(task);
        // An owner is bound to its job and task: a reference registered by
        // another job's (or task's) isolate is never this caller's.
        if let Some(registered) = lock(&self.task_manager).isolate_owner(&owner).cloned() {
            if registered.job_id != task.job_id || registered.task_name != task.task_name {
                return failure(
                    VmErrorKind::VmChildGone,
                    format!(
                        "isolate {} belongs to another task; no child of it is reachable from \
                         {}/{}",
                        task.isolate_id, task.job_id, task.task_name
                    ),
                );
            }
        }
        let Some(op) = request.op else {
            return failure(VmErrorKind::VmFailed, "the request names no operation");
        };
        let reply = match op {
            Op::Acquire(_) => self.acquire(task, vm_task).await,
            Op::Attach(attach) => match self.owned(&owner, &attach.id) {
                Ok(()) => VmReply {
                    id: attach.id,
                    ..Default::default()
                },
                Err(reply) => reply,
            },
            Op::Exec(exec) => {
                self.exec(&owner, &exec.id, exec.argv, exec.stdin, exec.timeout_millis)
                    .await
            }
            Op::Put(put) => self.put(&owner, &put.id, &put.path, put.content).await,
            Op::Get(get) => self.get(&owner, &get.id, &get.path).await,
            Op::Fetch(fetch) => match fetch.request {
                Some(call) => self.fetch(&owner, &fetch.id, fetch.port, call).await,
                None => failure(VmErrorKind::VmFailed, "fetch names no request"),
            },
            Op::Resize(resize) => self.resize(
                &owner,
                &resize.id,
                resize.memory_mib,
                resize.memory_max_mib,
                resize.cpu_mhz,
            ),
            Op::Release(release) => self.release(&owner, &release.id).await,
        };
        if let Some(error) = reply.error.as_ref() {
            debug!(
                job_id = %task.job_id,
                task = %task.task_name,
                isolate_id = %task.isolate_id,
                kind = ?VmErrorKind::try_from(error.kind).unwrap_or_default(),
                message = %error.message,
                "Isolate VM operation failed"
            );
        }
        reply
    }

    async fn owner_ended(&self, task: &IsolateTaskRef, reason: &str) {
        let owner = Self::owner_of(task);
        let task_manager = self.task_manager.clone();
        let reason = reason.to_string();
        let _ = tokio::task::spawn_blocking(move || {
            let mut guard = lock(&task_manager);
            if guard.isolate_owner(&owner).is_none() && guard.children_of(&owner).is_empty() {
                return;
            }
            guard.end_children(&owner, &reason);
            guard.unregister_isolate_owner(&owner);
            guard.reconcile_children();
        })
        .await;
    }

    fn child_count(&self, job_id: &str, task_name: &str) -> u32 {
        let guard = lock(&self.task_manager);
        guard
            .isolate_owners()
            .iter()
            .filter(|owner| owner.job_id == job_id && owner.task_name == task_name)
            .map(|owner| guard.children_of(&owner.owner).len() as u32)
            .sum()
    }
}

/// The exec timeout: the guest's, defaulted and capped.
fn exec_timeout(timeout_millis: u32) -> Duration {
    if timeout_millis == 0 {
        return DEFAULT_EXEC_TIMEOUT;
    }
    Duration::from_millis(u64::from(timeout_millis)).min(MAX_EXEC_TIMEOUT)
}

/// Where `put`/`get` read and write: a relative path is under the child's
/// run directory; an absolute one is taken as is, except in a child that
/// shares the host's filesystem (`exec`), which only gets its run
/// directory.
fn child_path(target: &RunTarget, path: &str) -> Result<String, String> {
    let path = path.trim();
    if path.is_empty() {
        return Err("the path is empty".into());
    }
    if path.split('/').any(|part| part == "..") {
        return Err(format!("{path}: `..` is not allowed"));
    }
    if path.starts_with('/') {
        if target.host_root {
            return Err(format!(
                "{path}: this child shares the node's filesystem, so it takes paths relative to its run directory"
            ));
        }
        return Ok(path.to_string());
    }
    Ok(format!("{}/{path}", target.run_dir.trim_end_matches('/')))
}

fn failure(kind: VmErrorKind, message: impl Into<String>) -> VmReply {
    VmReply {
        error: Some(VmError {
            kind: kind as i32,
            message: message.into(),
        }),
        ..Default::default()
    }
}

fn lock(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
) -> std::sync::MutexGuard<'_, WorkerTaskManager> {
    task_manager.lock().unwrap_or_else(|p| p.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{TaskAllocation, TaskConfig};
    use crate::worker::state::WorkerStateStore;

    const JOB: &str = "code-runner";

    fn envelope(allocation_id: i64, spec: &TaskConfig, resources: Value) -> TaskAllocation {
        TaskAllocation {
            allocation_id,
            job_id: JOB.into(),
            job_config_id: "jc-1".into(),
            node_id: "node-1".into(),
            task_config_id: allocation_id,
            task_name: crate::structs::session::envelope_task_name(&spec.name),
            task_runtime: crate::structs::session::ENVELOPE_RUNTIME.into(),
            task_config: "{}".into(),
            task_resources: resources.to_string(),
            session_task_spec: serde_json::to_string(spec).unwrap(),
            session_task_config_id: allocation_id + 1000,
            ..Default::default()
        }
    }

    /// A node holding an isolate task `run` (children: `machine`, at most
    /// one per owner) and the VM task `machine` (an `exec` session standing
    /// in for a VM: the backend only needs a session that takes an exec),
    /// each with its envelope here.
    fn node(temp: &tempfile::TempDir) -> ChildVms {
        let store = WorkerStateStore::new(temp.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(store);
        manager.load_builtin_plugins();
        let isolate = TaskConfig {
            name: "run".into(),
            runtime: "isolate".into(),
            config: json!({ "image": "python", "vm": { "task": "machine" } }),
            resources: json!({ "cpu": 100, "memory": 64 }),
            children: Some(crate::structs::children::ChildrenConfig {
                tasks: vec!["machine".into()],
                max: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let machine = TaskConfig {
            name: "machine".into(),
            runtime: "exec".into(),
            config: json!({ "command": ["sleep", "60"] }),
            resources: json!({ "cpu": 500, "memory": 256 }),
            ..Default::default()
        };
        manager
            .begin_task_allocation(&envelope(
                100,
                &isolate,
                json!({ "cpu": 1000, "memory": 512 }),
            ))
            .unwrap();
        manager
            .begin_task_allocation(&envelope(
                200,
                &machine,
                json!({ "cpu": 1000, "memory": 512 }),
            ))
            .unwrap();
        ChildVms::new(
            Arc::new(Mutex::new(manager)),
            "node-1",
            Arc::new(LaunchPool::new(4, Duration::from_secs(5))),
            1024,
        )
    }

    fn isolate(id: &str, session: bool) -> IsolateTaskRef {
        IsolateTaskRef {
            job_id: JOB.into(),
            task_name: "run".into(),
            envelope_allocation_id: 100,
            isolate_id: id.into(),
            tenant: String::new(),
            session,
        }
    }

    fn op(op: crate::structs::isolate_host::vm_request::Op) -> VmRequest {
        VmRequest {
            origin: None,
            op: Some(op),
        }
    }

    fn kind(reply: &VmReply) -> Option<VmErrorKind> {
        reply
            .error
            .as_ref()
            .map(|error| VmErrorKind::try_from(error.kind).unwrap())
    }

    fn message(reply: &VmReply) -> String {
        reply
            .error
            .as_ref()
            .map(|error| error.message.clone())
            .unwrap_or_default()
    }

    async fn acquire(vms: &ChildVms, who: &IsolateTaskRef) -> VmReply {
        use crate::structs::isolate_host::{vm_request::Op, VmAcquire};
        vms.call(who, Some("machine"), op(Op::Acquire(VmAcquire {})))
            .await
    }

    async fn exec(
        vms: &ChildVms,
        who: &IsolateTaskRef,
        id: &str,
        argv: &[&str],
        stdin: &[u8],
        timeout_millis: u32,
    ) -> VmReply {
        use crate::structs::isolate_host::{vm_request::Op, VmExec};
        vms.call(
            who,
            Some("machine"),
            op(Op::Exec(VmExec {
                id: id.into(),
                argv: argv.iter().map(|arg| arg.to_string()).collect(),
                stdin: stdin.to_vec(),
                timeout_millis,
            })),
        )
        .await
    }

    #[test]
    fn paths_resolve_under_the_run_dir_test() {
        let host = RunTarget {
            local_key: 1,
            run_dir: "/allocs/1/machine/run".into(),
            host_root: true,
            cgroup: None,
        };
        assert_eq!(
            child_path(&host, "a/b.txt").unwrap(),
            "/allocs/1/machine/run/a/b.txt"
        );
        assert!(child_path(&host, "/etc/passwd").is_err());
        assert!(child_path(&host, "../x").is_err());
        assert!(child_path(&host, " ").is_err());
        let guest = RunTarget {
            run_dir: "/legion/run".into(),
            host_root: false,
            ..host
        };
        assert_eq!(child_path(&guest, "/tmp/x").unwrap(), "/tmp/x");
        assert_eq!(child_path(&guest, "x").unwrap(), "/legion/run/x");
    }

    #[test]
    fn exec_timeouts_default_and_are_capped_test() {
        assert_eq!(exec_timeout(0), DEFAULT_EXEC_TIMEOUT);
        assert_eq!(exec_timeout(250), Duration::from_millis(250));
        assert_eq!(exec_timeout(u32::MAX), MAX_EXEC_TIMEOUT);
    }

    /// A task with no VM task answers `VM_NOT_CONFIGURED`.
    #[tokio::test]
    async fn a_task_without_a_vm_task_is_not_configured_test() {
        use crate::structs::isolate_host::{vm_request::Op, VmAcquire};
        let temp = tempfile::tempdir().unwrap();
        let vms = node(&temp);
        let reply = vms
            .call(
                &isolate("inv-1", false),
                None,
                op(Op::Acquire(VmAcquire {})),
            )
            .await;
        assert_eq!(kind(&reply), Some(VmErrorKind::VmNotConfigured));
    }

    /// The whole life of a child VM of a request isolate: acquire, exec
    /// (stdin, output, exit, timeout), put/get, the owner's `children.max`,
    /// another isolate cannot reach it, release, and a released child
    /// answers `VM_CHILD_GONE` with why.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_isolate_owns_and_drives_a_child_test() {
        use crate::structs::isolate_host::{vm_request::Op, VmAttach, VmGet, VmPut, VmRelease};
        let temp = tempfile::tempdir().unwrap();
        let vms = node(&temp);
        let me = isolate("inv-1", false);
        let acquired = acquire(&vms, &me).await;
        assert!(acquired.error.is_none(), "{acquired:?}");
        let id = acquired.id.clone();
        assert_eq!(vms.child_count(JOB, "run"), 1);
        let status = lock(&vms.task_manager).session_status(&id).unwrap();
        let owner = status.owner.expect("the child carries its owner");
        assert_eq!(
            (owner.kind.as_str(), owner.id.as_str()),
            (OWNER_ISOLATE, "inv-1")
        );

        let ran = exec(
            &vms,
            &me,
            &id,
            &["sh", "-c", "cat; echo oops >&2; exit 3"],
            b"hi",
            10_000,
        )
        .await;
        let result = ran.exec.expect("an exec result");
        assert_eq!(
            result.exit,
            3,
            "{:?}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(result.stdout, b"hi");
        assert_eq!(result.stderr, b"oops\n");
        assert!(!result.timed_out);
        let slow = exec(&vms, &me, &id, &["sleep", "5"], b"", 200).await;
        assert!(slow.exec.unwrap().timed_out);

        let put = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Put(VmPut {
                    id: id.clone(),
                    path: "data/in.txt".into(),
                    content: b"payload".to_vec(),
                })),
            )
            .await;
        assert!(put.error.is_none(), "{put:?}");
        let got = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Get(VmGet {
                    id: id.clone(),
                    path: "data/in.txt".into(),
                })),
            )
            .await;
        assert_eq!(got.content, b"payload", "{got:?}");
        let missing = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Get(VmGet {
                    id: id.clone(),
                    path: "nope".into(),
                })),
            )
            .await;
        assert_eq!(kind(&missing), Some(VmErrorKind::VmFailed));
        let too_big = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Put(VmPut {
                    id: id.clone(),
                    path: "big".into(),
                    content: vec![0u8; 2048],
                })),
            )
            .await;
        assert_eq!(kind(&too_big), Some(VmErrorKind::VmFailed));

        // `children.max = 1`: a second child is refused, retryably.
        let second = acquire(&vms, &me).await;
        assert_eq!(
            kind(&second),
            Some(VmErrorKind::VmResourceExhausted),
            "{second:?}"
        );

        // Another isolate cannot reach this one's child.
        let other = isolate("inv-2", false);
        let stolen = vms
            .call(
                &other,
                Some("machine"),
                op(Op::Attach(VmAttach { id: id.clone() })),
            )
            .await;
        assert_eq!(kind(&stolen), Some(VmErrorKind::VmChildGone));
        assert!(message(&stolen).contains("no child"), "{stolen:?}");
        let attached = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Attach(VmAttach { id: id.clone() })),
            )
            .await;
        assert_eq!(attached.id, id);

        let released = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Release(VmRelease { id: id.clone() })),
            )
            .await;
        assert!(released.error.is_none(), "{released:?}");
        let after = exec(&vms, &me, &id, &["true"], b"", 1000).await;
        assert_eq!(kind(&after), Some(VmErrorKind::VmChildGone), "{after:?}");
        assert!(message(&after).contains("gone"), "{after:?}");
        assert_eq!(vms.child_count(JOB, "run"), 0);
        // Releasing again is done, not an error.
        let again = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Release(VmRelease { id: id.clone() })),
            )
            .await;
        assert!(again.error.is_none(), "{again:?}");
    }

    /// Children end with their owner: `owner_ended` stops them with the
    /// reason, and a child of an unregistered owner is reaped by the
    /// reconcile. A session isolate owns its children as a `session`, and
    /// they survive reconciles while it is registered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn children_end_with_their_isolate_owner_test() {
        let temp = tempfile::tempdir().unwrap();
        let vms = node(&temp);
        let request = isolate("inv-9", false);
        let child = acquire(&vms, &request).await;
        assert!(child.error.is_none(), "{child:?}");
        vms.owner_ended(&request, "the request finished").await;
        let gone = exec(&vms, &request, &child.id, &["true"], b"", 1000).await;
        assert_eq!(kind(&gone), Some(VmErrorKind::VmChildGone));
        assert!(message(&gone).contains("the request finished"), "{gone:?}");

        let session = isolate("run-sess1", true);
        let kept = acquire(&vms, &session).await;
        assert!(kept.error.is_none(), "{kept:?}");
        let status = lock(&vms.task_manager).session_status(&kept.id).unwrap();
        let owner = status.owner.unwrap();
        assert_eq!(
            (owner.kind.as_str(), owner.id.as_str()),
            (OWNER_SESSION, "run-sess1")
        );
        lock(&vms.task_manager).reconcile_children();
        let alive = exec(&vms, &session, &kept.id, &["true"], b"", 5000).await;
        assert_eq!(
            alive.exec.map(|r| r.exit),
            Some(0),
            "the registered owner keeps its child"
        );

        // The supervisor forgot the session without ending it (a host
        // crash): the next reconcile ends its children as orphans.
        lock(&vms.task_manager).unregister_isolate_owner(&ChildVms::owner_of(&session));
        lock(&vms.task_manager).reconcile_children();
        let orphan = exec(&vms, &session, &kept.id, &["true"], b"", 1000).await;
        assert_eq!(kind(&orphan), Some(VmErrorKind::VmChildGone), "{orphan:?}");
        assert!(message(&orphan).contains("owner ended"), "{orphan:?}");
    }

    /// An owner reference is bound to its job and task: the same id named
    /// from another job or another task reaches none of its children.
    #[tokio::test]
    async fn another_jobs_or_tasks_isolate_cannot_use_a_child_test() {
        let temp = tempfile::tempdir().unwrap();
        let vms = node(&temp);
        let me = isolate("run-sess7", true);
        let child = acquire(&vms, &me).await;
        assert!(child.error.is_none(), "{child:?}");
        for (job, task) in [("other-job", "run"), (JOB, "other-task")] {
            let thief = IsolateTaskRef {
                job_id: job.into(),
                task_name: task.into(),
                ..me.clone()
            };
            let stolen = exec(&vms, &thief, &child.id, &["true"], b"", 1000).await;
            assert_eq!(kind(&stolen), Some(VmErrorKind::VmChildGone), "{stolen:?}");
            assert!(
                message(&stolen).contains("belongs to another task"),
                "{stolen:?}"
            );
            let more = acquire(&vms, &thief).await;
            assert_eq!(kind(&more), Some(VmErrorKind::VmChildGone), "{more:?}");
        }
        // The owner itself still drives it.
        let mine = exec(&vms, &me, &child.id, &["true"], b"", 5000).await;
        assert_eq!(mine.exec.map(|r| r.exit), Some(0));
    }

    /// `fetch` reaches a child's port only where the child has its own port
    /// space or the port is one of its own: an `exec` child on the host
    /// network with no ports cannot be used to reach the node's loopback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_refuses_the_nodes_own_loopback_test() {
        use crate::structs::isolate_host::{vm_request::Op, VmFetch};
        let temp = tempfile::tempdir().unwrap();
        let vms = node(&temp);
        let me = isolate("inv-3", false);
        let child = acquire(&vms, &me).await;
        assert!(child.error.is_none(), "{child:?}");
        let reply = vms
            .call(
                &me,
                Some("machine"),
                op(Op::Fetch(VmFetch {
                    id: child.id.clone(),
                    port: 4747,
                    request: Some(HttpCall {
                        method: "GET".into(),
                        path: "/".into(),
                        ..Default::default()
                    }),
                })),
            )
            .await;
        assert_eq!(kind(&reply), Some(VmErrorKind::VmFailed), "{reply:?}");
        assert!(
            message(&reply).contains("shares the node's network"),
            "{reply:?}"
        );
        vms.owner_ended(&me, "done").await;
    }
}
