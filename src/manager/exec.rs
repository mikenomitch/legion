// `tonic::Status` is large; like `server.rs` we accept a large Err variant
// rather than boxing every `Result`.
#![allow(clippy::result_large_err)]

//! Manager-side exec/action proxy.
//!
//! `Manager.ExecAllocation` authorizes an exec request, resolves the target
//! allocation to the worker running it, rewrites the opening `ExecStart`
//! frame (rendering an action's argv server-side when the request names an
//! action), then splices the bidirectional stream onto the worker's
//! `Worker.ExecTask`. The manager is the trust boundary: clients never learn
//! worker addresses, and an action-only caller has no path to arbitrary argv.

use std::collections::{BTreeMap, HashMap};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::manager::acl;
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope};
use crate::structs::{exec_client_frame, ExecClientFrame, ExecServerFrame, ExecStart, ExecWinsize};
use crate::task_plugins::runtime_is_host_level;

/// The response stream type for `Manager.ExecAllocation`.
pub type ExecAllocationStream = ReceiverStream<Result<ExecServerFrame, Status>>;

/// A fully-authorized, fully-resolved exec request ready to run on a worker.
struct ResolvedExec {
    allocation_id: i64,
    node_id: String,
    target: crate::manager::worker_plane::WorkerTarget,
    job_id: String,
    task_name: String,
    /// Final argv the worker will run (action commands already rendered).
    command: Vec<String>,
    tty: bool,
    winsize: Option<ExecWinsize>,
    /// Human-readable description for audit (raw command or `action:<name>`).
    audit_subject: String,
    /// Effective, redaction-applied parameter values for audit.
    audit_params: BTreeMap<String, String>,
}

/// Authorize + resolve an `ExecStart` against stored cluster state. Emits the
/// standard ACL audit denial on refusal.
fn resolve_exec_start(
    store: &Store,
    identity: &AclIdentity,
    start: &ExecStart,
) -> Result<ResolvedExec, Status> {
    let is_action = !start.action.is_empty();

    // Coarse gate before any lookup, so a credential-less caller cannot probe
    // allocation existence via NotFound-vs-PermissionDenied. Exec OR action
    // write on anything clears the gate; precise scope is enforced below.
    let coarse_ok = identity.would_allow(AclResource::Exec, AclAction::Write, &AclScope::any())
        || identity.would_allow(AclResource::Action, AclAction::Write, &AclScope::any());
    if !coarse_ok {
        // Reuse enforce to emit the audit log + shaped Status.
        acl::enforce(
            identity,
            if is_action {
                AclResource::Action
            } else {
                AclResource::Exec
            },
            AclAction::Write,
            &AclScope::any(),
        )?;
    }

    let allocation = store
        .get_allocation_by_id(start.allocation_id)
        .map_err(|e| Status::internal(format!("failed to load allocation: {e}")))?
        .ok_or_else(|| {
            Status::not_found(format!("allocation {} not found", start.allocation_id))
        })?;

    // Q8: exec targets running allocations only. A dispatched allocation is
    // `placed`; terminal/failed allocations are removed from the ledger, so
    // anything still present and not explicitly dead is a valid target.
    if !is_runnable_status(&allocation.status) {
        return Err(Status::failed_precondition(format!(
            "allocation {} is {}, not running",
            allocation.id, allocation.status
        )));
    }

    let task_configs = store
        .get_task_configs_for_job_config(&allocation.job_config_id)
        .map_err(|e| Status::internal(format!("failed to load task configs: {e}")))?;
    let (_, task) = task_configs
        .into_iter()
        .find(|(id, _)| *id == allocation.task_config_id)
        .ok_or_else(|| Status::internal("task config for allocation not found"))?;

    // Honor an explicit task selector.
    if !start.task_name.is_empty() && start.task_name != task.name {
        return Err(Status::not_found(format!(
            "allocation {} runs task `{}`, not `{}`",
            allocation.id, task.name, start.task_name
        )));
    }

    let host_level = runtime_is_host_level(&task.runtime);

    let (command, tty, audit_subject, audit_params) = if is_action {
        let action = task
            .actions
            .iter()
            .find(|a| a.name == start.action)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "task `{}` has no action `{}`",
                    task.name, start.action
                ))
            })?;

        // Union rule: an action run is allowed by EITHER action:write on the
        // (job, action) OR exec:write on the (job, host) — anyone who can open
        // a shell can type the command anyway. The reverse never holds.
        let action_ok = identity.would_allow(
            AclResource::Action,
            AclAction::Write,
            &AclScope::action(&allocation.job_id, &action.name),
        );
        let exec_ok = identity.would_allow(
            AclResource::Exec,
            AclAction::Write,
            &AclScope::exec(&allocation.job_id, host_level),
        );
        if !action_ok && !exec_ok {
            // Emit a denial keyed on the action scope (the intended grant).
            acl::enforce(
                identity,
                AclResource::Action,
                AclAction::Write,
                &AclScope::action(&allocation.job_id, &action.name),
            )?;
        }

        let supplied: HashMap<String, String> = start.action_params.clone();
        let (argv, resolved) = action
            .render_command(&supplied)
            .map_err(Status::invalid_argument)?;

        // Redact sensitive params for the audit record.
        let mut audit = BTreeMap::new();
        for (name, value) in resolved {
            let sensitive = action
                .params
                .iter()
                .find(|p| p.name == name)
                .map(|p| p.sensitive)
                .unwrap_or(false);
            audit.insert(name, if sensitive { "***".into() } else { value });
        }

        (argv, action.tty, format!("action:{}", action.name), audit)
    } else {
        if start.command.is_empty() {
            return Err(Status::invalid_argument(
                "exec requires a command (or an action name)",
            ));
        }
        acl::enforce(
            identity,
            AclResource::Exec,
            AclAction::Write,
            &AclScope::exec(&allocation.job_id, host_level),
        )?;
        (
            start.command.clone(),
            start.tty,
            format!("exec:{}", start.command.join(" ")),
            BTreeMap::new(),
        )
    };

    let node = store
        .get_node(&allocation.node_id)
        .map_err(|e| Status::internal(format!("failed to load node: {e}")))?;
    let target = crate::manager::worker_plane::WorkerTarget::for_node(&node)?;

    Ok(ResolvedExec {
        allocation_id: allocation.id,
        node_id: allocation.node_id.clone(),
        target,
        job_id: allocation.job_id.clone(),
        task_name: task.name.clone(),
        command,
        tty,
        winsize: start.winsize,
        audit_subject,
        audit_params,
    })
}

/// Whether an allocation status is a live target for exec. `placed` is the
/// dispatched/running state; the rest are the running aliases used elsewhere.
fn is_runnable_status(status: &str) -> bool {
    matches!(status, "placed" | "active" | "running")
}

/// Authorize, resolve, and proxy an exec session. `inbound` is a channel of
/// client frames *after* the opening `ExecStart` (which is passed separately),
/// so this core is shared by the gRPC handler and the WebSocket bridge. Returns
/// a receiver of server frames to write back to the caller.
///
/// `credential` is what the session was opened with. Every
/// `credential.interval` the session re-authenticates and re-runs the same
/// authorization it opened with, and ends with the auth error once the token
/// is revoked or expired or its policy no longer grants the session.
pub async fn run_exec_session(
    store: &Store,
    identity: &AclIdentity,
    start: ExecStart,
    mut inbound: mpsc::Receiver<ExecClientFrame>,
    credential: acl::StreamCredential,
) -> Result<mpsc::Receiver<Result<ExecServerFrame, Status>>, Status> {
    let resolved = resolve_exec_start(store, identity, &start)?;

    info!(
        target: "exec_audit",
        principal = %identity.principal,
        accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
        allocation_id = resolved.allocation_id,
        node_id = %resolved.node_id,
        job_id = %resolved.job_id,
        task = %resolved.task_name,
        subject = %resolved.audit_subject,
        params = ?resolved.audit_params,
        tty = resolved.tty,
        "exec session authorized"
    );

    // Dial the worker running the allocation.
    let mut worker =
        crate::manager::worker_plane::WorkerPlane::global().client_for(store, &resolved.target)?;

    // Build the outbound stream to the worker: a freshly-rendered ExecStart
    // (raw argv only — the worker never sees action names), then the client's
    // subsequent frames spliced through unchanged.
    let rewritten_start = ExecStart {
        allocation_id: resolved.allocation_id,
        task_name: resolved.task_name.clone(),
        command: resolved.command.clone(),
        action: String::new(),
        action_params: Default::default(),
        tty: resolved.tty,
        winsize: resolved.winsize,
        bearer_token: String::new(),
    };

    let (fwd_tx, fwd_rx) = mpsc::channel::<ExecClientFrame>(128);
    fwd_tx
        .send(ExecClientFrame {
            frame: Some(exec_client_frame::Frame::Start(rewritten_start)),
        })
        .await
        .map_err(|_| Status::internal("worker forward channel closed"))?;

    // Splice remaining client frames to the worker (stdin, resize, ping).
    tokio::spawn(async move {
        while let Some(frame) = inbound.recv().await {
            // Never forward a second Start frame.
            if matches!(frame.frame, Some(exec_client_frame::Frame::Start(_))) {
                continue;
            }
            if fwd_tx.send(frame).await.is_err() {
                break;
            }
        }
    });

    let worker_stream = worker
        .exec_task(ReceiverStream::new(fwd_rx))
        .await
        .map_err(|status| {
            if crate::manager::worker_plane::is_unreachable(&status) {
                crate::manager::worker_plane::worker_call_error(
                    &resolved.node_id,
                    &resolved.target.address,
                    status,
                )
            } else {
                Status::internal(format!("worker exec failed: {status}"))
            }
        })?
        .into_inner();

    // Pump the worker's server frames back to the caller, logging the exit.
    let (out_tx, out_rx) = mpsc::channel::<Result<ExecServerFrame, Status>>(128);
    let audit_alloc = resolved.allocation_id;
    let audit_subject = resolved.audit_subject.clone();
    let principal = identity.principal.clone();
    let reauth_store = store.clone();
    tokio::spawn(async move {
        let mut worker_stream = worker_stream;
        let mut reauth = tokio::time::interval(credential.interval);
        reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        reauth.tick().await; // the first tick fires immediately
        loop {
            let message = tokio::select! {
                message = worker_stream.message() => message,
                _ = reauth.tick() => {
                    let still_allowed = credential
                        .reauthenticate(&reauth_store)
                        .and_then(|identity| resolve_exec_start(&reauth_store, &identity, &start));
                    if let Err(status) = still_allowed {
                        info!(
                            target: "exec_audit",
                            principal = %principal,
                            allocation_id = audit_alloc,
                            subject = %audit_subject,
                            reason = %status.message(),
                            "exec session closed: credential no longer authorizes it"
                        );
                        // Dropping the worker stream cancels the worker-side
                        // session.
                        let _ = out_tx.send(Err(status)).await;
                        break;
                    }
                    continue;
                }
            };
            match message {
                Ok(Some(frame)) => {
                    if let Some(crate::structs::exec_server_frame::Frame::Exit(exit)) = &frame.frame
                    {
                        info!(
                            target: "exec_audit",
                            principal = %principal,
                            allocation_id = audit_alloc,
                            subject = %audit_subject,
                            exit_code = exit.code,
                            "exec session ended"
                        );
                    }
                    if out_tx.send(Ok(frame)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(status) => {
                    let _ = out_tx.send(Err(status)).await;
                    break;
                }
            }
        }
    });

    Ok(out_rx)
}

/// List the actions declared across a job's tasks (for CLI + UI discovery).
pub fn list_job_actions(
    store: &Store,
    job_id: &str,
) -> Result<Vec<crate::structs::JobAction>, Status> {
    let job_config = store
        .get_job_config(job_id)
        .map_err(|e| Status::not_found(format!("job {job_id} not found: {e}")))?;

    let mut actions = Vec::new();
    for task in &job_config.tasks {
        for action in &task.actions {
            actions.push(crate::structs::JobAction {
                task_name: task.name.clone(),
                name: action.name.clone(),
                description: action.description.clone(),
                tty: action.tty,
                params: action
                    .params
                    .iter()
                    .map(|p| crate::structs::JobActionParam {
                        name: p.name.clone(),
                        description: p.description.clone(),
                        r#type: p.param_type.as_str().to_string(),
                        required: p.is_required(),
                        default: p.default.clone().unwrap_or_default(),
                        values: p.values.clone(),
                        pattern: p.pattern.clone().unwrap_or_default(),
                        min: p.min.map(|m| m.to_string()).unwrap_or_default(),
                        max: p.max.map(|m| m.to_string()).unwrap_or_default(),
                        sensitive: p.sensitive,
                    })
                    .collect(),
            });
        }
    }
    Ok(actions)
}
