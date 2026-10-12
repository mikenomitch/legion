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
use crate::structs::{
    exec_client_frame, CreateExecKeyRequest, ExecClientFrame, ExecKey, ExecServerFrame,
    ExecSessionRef, ExecStart, ExecWinsize, MintExecTicketReply, MintExecTicketRequest,
    RevokedExecSession,
};
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
    /// The instance the allocation fulfils, when it fulfils one.
    instance_id: String,
    /// The action's name, for an action run.
    action: String,
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
    resolve_exec_target(store, identity, start, false)
}

/// Whether `ExecAllocation` would admit `start` for `identity`: its
/// authorization and resolution, without dialing a worker.
#[cfg(test)]
pub(crate) fn resolve_for_test(
    store: &Store,
    identity: &AclIdentity,
    start: &ExecStart,
) -> Result<(), Status> {
    resolve_exec_start(store, identity, start).map(|_| ())
}

/// [`resolve_exec_start`], optionally admitting an empty command with no
/// action: an exec ticket minted that way allows any command, under the
/// same `exec:write` grant a command needs.
fn resolve_exec_target(
    store: &Store,
    identity: &AclIdentity,
    start: &ExecStart,
    any_command: bool,
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

    // An instance resolves to whichever allocation fulfils it now, so a
    // caller holding a stable id reaches it across reschedules and resumes.
    let allocation_id = if start.instance_id.is_empty() {
        start.allocation_id
    } else {
        let instance = store
            .get_instance(&start.instance_id)
            .map_err(|e| Status::internal(format!("failed to load instance: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("instance {} not found", start.instance_id))
            })?;
        instance.current_alloc_id.ok_or_else(|| {
            Status::failed_precondition(format!(
                "instance {} has no running allocation (it is {})",
                instance.instance_id, instance.state
            ))
        })?
    };

    let allocation = store
        .get_allocation_by_id(allocation_id)
        .map_err(|e| Status::internal(format!("failed to load allocation: {e}")))?
        .ok_or_else(|| Status::not_found(format!("allocation {allocation_id} not found")))?;

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
        if start.command.is_empty() && !any_command {
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
        let subject = if start.command.is_empty() {
            "exec:<any command>".to_string()
        } else {
            format!("exec:{}", start.command.join(" "))
        };
        (start.command.clone(), start.tty, subject, BTreeMap::new())
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
        instance_id: allocation.instance_id.clone().unwrap_or_default(),
        action: if is_action {
            start.action.clone()
        } else {
            String::new()
        },
        command,
        tty,
        winsize: start.winsize,
        audit_subject,
        audit_params,
    })
}

/// Mint an exec ticket (`Manager.MintExecTicket`; docs/exec-and-actions.md,
/// "Direct exec"). Runs exactly the authorization `ExecAllocation` runs,
/// then signs what it authorized. Writes nothing: the mint is a signature
/// and an `exec_audit` log line.
pub fn mint_exec_ticket(
    store: &Store,
    identity: &AclIdentity,
    issuer: &crate::manager::workload_identity::WorkloadIdentityState,
    limits: &crate::structs::ExecTicketsConfigFull,
    request: &MintExecTicketRequest,
    now: std::time::SystemTime,
) -> Result<MintExecTicketReply, Status> {
    let start = ExecStart {
        allocation_id: request.allocation_id,
        instance_id: request.instance_id.clone(),
        task_name: request.task_name.clone(),
        command: request.command.clone(),
        action: request.action.clone(),
        action_params: request.action_params.clone(),
        tty: request.tty,
        ..Default::default()
    };
    let resolved = resolve_exec_target(store, identity, &start, true)?;

    let ttl = bounded_duration("ttl", &request.ttl, limits.default_ttl, limits.max_ttl)?;
    let mut max_session = bounded_duration(
        "max_session",
        &request.max_session,
        limits.max_session,
        limits.max_session,
    )?;
    let now_unix = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before 1970"))?
        .as_secs();
    // A session never outlives the credential that opened it: the proxied
    // path ends it at the next re-check after expiry, so a direct one ends
    // no later.
    if let Some(expires_at) = identity.expires_at.as_deref() {
        let expires_at =
            time::OffsetDateTime::parse(expires_at, &time::format_description::well_known::Rfc3339)
                .map_err(|e| Status::internal(format!("unreadable credential expiry: {e}")))?
                .unix_timestamp();
        let remaining = expires_at.saturating_sub(now_unix as i64);
        if remaining <= 0 {
            return Err(Status::unauthenticated(
                "the minting credential has expired",
            ));
        }
        max_session = max_session.min(std::time::Duration::from_secs(remaining as u64));
    }

    let jti = crate::structs::exec_ticket::new_exec_ticket_id();
    let subject = crate::structs::exec_ticket::subject_for(
        identity.accessor_id.as_deref(),
        &identity.principal,
    );
    let claims = crate::structs::exec_ticket::ExecTicketClaims {
        sub: subject.clone(),
        jti: jti.clone(),
        iat: now_unix as i64,
        exp: (now_unix + ttl.as_secs()) as i64,
        node: resolved.node_id.clone(),
        job: resolved.job_id.clone(),
        task: resolved.task_name.clone(),
        allocation_id: resolved.allocation_id,
        instance: (!resolved.instance_id.is_empty()).then(|| resolved.instance_id.clone()),
        argv: (!resolved.command.is_empty()).then(|| resolved.command.clone()),
        action: (!resolved.action.is_empty()).then(|| resolved.action.clone()),
        tty: resolved.tty,
        max_session: max_session.as_secs().max(1),
        uses: 1,
    };
    let issued = issuer
        .issue_exec_ticket(
            &jti,
            &subject,
            claims.to_claims(),
            now_unix,
            now_unix + ttl.as_secs(),
        )
        .map_err(|e| Status::internal(format!("failed to sign the exec ticket: {e:#}")))?;
    let ca_pem = store
        .mesh_ca()
        .map_err(|e| Status::internal(format!("failed to load the cluster CA: {e}")))?
        .map(|ca| ca.bundle_pem())
        .unwrap_or_default();

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
        jti = %jti,
        max_session_secs = claims.max_session,
        "exec ticket minted"
    );

    let expires_at = time::OffsetDateTime::from_unix_timestamp(issued.expires_at as i64)
        .ok()
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default();
    Ok(MintExecTicketReply {
        ticket: issued.token,
        worker_address: resolved.target.address.clone(),
        node: resolved.node_id,
        expires_at,
        ca_pem,
        allocation_id: resolved.allocation_id,
        jti,
        max_session_seconds: claims.max_session,
    })
}

/// A duration from a request field: `default` when empty, refused above
/// `ceiling`.
fn bounded_duration(
    field: &str,
    raw: &str,
    default: std::time::Duration,
    ceiling: std::time::Duration,
) -> Result<std::time::Duration, Status> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    let value = humantime::parse_duration(raw)
        .map_err(|e| Status::invalid_argument(format!("invalid {field} `{raw}`: {e}")))?;
    if value.is_zero() {
        return Err(Status::invalid_argument(format!(
            "{field} must be above zero"
        )));
    }
    if value > ceiling {
        return Err(Status::invalid_argument(format!(
            "{field} {raw} is above this manager's ceiling of {}\nHint: ask for less, or raise \
             exec.tickets in the manager config",
            humantime::format_duration(ceiling)
        )));
    }
    Ok(value)
}

/// Builds a delegated exec key from a `CreateExecKey` request
/// (docs/exec-and-actions.md, "Delegated minting keys"). Its ceilings default
/// to, and may not exceed, the manager's own `exec.tickets` ceilings.
pub fn new_exec_key(
    identity: &AclIdentity,
    limits: &crate::structs::ExecTicketsConfigFull,
    request: &CreateExecKeyRequest,
    now: time::OffsetDateTime,
) -> Result<ExecKey, Status> {
    let jobs = request.jobs.trim();
    if jobs.is_empty() {
        return Err(Status::invalid_argument(
            "jobs is required: a glob over the job names this key's tickets may reach, \
             e.g. agents-*",
        ));
    }
    crate::structs::exec_ticket::validate_p256_public_key(&request.public_key_pem)
        .map_err(|err| Status::invalid_argument(format!("{err:#}")))?;
    let max_ttl = bounded_duration("max_ttl", &request.max_ttl, limits.max_ttl, limits.max_ttl)?;
    let max_session = bounded_duration(
        "max_session",
        &request.max_session,
        limits.max_session,
        limits.max_session,
    )?;
    Ok(ExecKey {
        id: crate::structs::exec_ticket::new_exec_key_id(),
        name: request.name.trim().to_string(),
        jobs: jobs.to_string(),
        public_key_pem: request.public_key_pem.trim().to_string() + "\n",
        max_ttl_seconds: max_ttl.as_secs(),
        max_session_seconds: max_session.as_secs(),
        created_by: crate::structs::exec_ticket::subject_for(
            identity.accessor_id.as_deref(),
            &identity.principal,
        ),
        created_at: now
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
    })
}

/// The version string of a set of delegated keys, for
/// `HeartbeatReply.exec_keys_version`. Keys are immutable, so their ids
/// identify the set.
pub fn exec_keys_version(keys: &[ExecKey]) -> String {
    use sha2::Digest;
    let mut ids: Vec<&str> = keys.iter().map(|key| key.id.as_str()).collect();
    ids.sort_unstable();
    let digest = sha2::Sha256::digest(ids.join("\n").as_bytes());
    let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("k{}-{hex}", keys.len())
}

/// Re-checks the direct sessions a worker reports on its heartbeat, as the
/// proxied path re-checks its own every 30 seconds: the minting token must
/// still exist, be unexpired, and grant the session. Returns the sessions
/// that no longer hold, with the reason. Sessions minted by a credential
/// without an accessor (a JWT auth method) are bounded by `max_session`,
/// which the mint clamps to that credential's expiry.
pub fn revoked_exec_sessions(
    store: &Store,
    auth: &acl::Authenticator,
    sessions: &[ExecSessionRef],
) -> Vec<RevokedExecSession> {
    let mut identities: HashMap<String, Result<AclIdentity, String>> = HashMap::new();
    let mut revoked = Vec::new();
    for session in sessions {
        // A delegated key's sessions last as long as the key.
        if let Some(key) = session.subject.strip_prefix("key:") {
            if matches!(store.get_exec_key(key), Ok(None)) {
                revoked.push(RevokedExecSession {
                    jti: session.jti.clone(),
                    reason: "the exec key that minted this session was deleted".into(),
                });
            }
            continue;
        }
        let Some(accessor) = session.subject.strip_prefix("accessor:") else {
            continue;
        };
        let identity = identities.entry(accessor.to_string()).or_insert_with(|| {
            match auth.identity_for_accessor(accessor, store) {
                Ok(Some(identity)) => Ok(identity),
                Ok(None) => Err("the token that minted this session was deleted".to_string()),
                Err(status) => Err(status.message().to_string()),
            }
        });
        let identity = match identity {
            Ok(identity) => identity,
            Err(reason) => {
                revoked.push(RevokedExecSession {
                    jti: session.jti.clone(),
                    reason: reason.clone(),
                });
                continue;
            }
        };
        if !still_granted(store, identity, session) {
            revoked.push(RevokedExecSession {
                jti: session.jti.clone(),
                reason: "the token that minted this session no longer grants it".into(),
            });
        }
    }
    revoked
}

/// Whether `identity` still grants a session like `session`: the grant
/// `resolve_exec_start` checked at mint, re-run against today's policies.
/// The runtime's host-level flag comes from the allocation's task; a
/// session whose allocation is gone has nothing left to grant.
fn still_granted(store: &Store, identity: &AclIdentity, session: &ExecSessionRef) -> bool {
    let host_level = store
        .get_allocation_by_id(session.allocation_id)
        .ok()
        .flatten()
        .and_then(|allocation| {
            store
                .get_task_configs_for_job_config(&allocation.job_config_id)
                .ok()?
                .into_iter()
                .find(|(id, _)| *id == allocation.task_config_id)
                .map(|(_, task)| runtime_is_host_level(&task.runtime))
        });
    let Some(host_level) = host_level else {
        return true;
    };
    let exec_ok = identity.would_allow(
        AclResource::Exec,
        AclAction::Write,
        &AclScope::exec(&session.job, host_level),
    );
    if session.action.is_empty() {
        return exec_ok;
    }
    exec_ok
        || identity.would_allow(
            AclResource::Action,
            AclAction::Write,
            &AclScope::action(&session.job, &session.action),
        )
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
        instance_id: String::new(),
        exec_ticket: String::new(),
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
