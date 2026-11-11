//! Resizing a running unit in place (docs/resize.md): the node's half.
//!
//! A session's resize is decided here and nowhere else: the session's
//! envelope is the room, admission is the reservation, and nothing reaches
//! the replicated store. A top-level allocation's resize arrives already
//! committed by a manager (a grow) or about to be released (a shrink), and
//! the node only tells the runtime.
//!
//! Order, for a session: a grow is admitted against the envelope before the
//! runtime is told, and a shrink is applied by the runtime before the
//! envelope gets the room back. Both happen under the task manager's lock,
//! the lock that admits sessions, so no start can take room a grow was just
//! given or room a shrink has not released yet.

use super::*;
use crate::structs::resize::{
    ResizeAnswer, ResizeChange, ResizeOutcome, ResourceSize, RESIZABLE_KEYS,
};

/// How long a deferred session grow keeps asking its elastic envelope for
/// room. The caller retries; one that never does must not hold the envelope
/// up.
const RESIZE_DEMAND_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Which unit a resize names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResizeTarget {
    /// A running session, by its id.
    Session(String),
    /// A top-level allocation, by the manager's id.
    Allocation(i64),
}

/// What a resize on this node did.
#[derive(Debug, Clone, PartialEq)]
pub struct UnitResize {
    pub outcome: ResizeOutcome,
    /// The unit's resizable resources before.
    pub from: ResourceSize,
    /// The resizable resources asked for.
    pub to: ResourceSize,
    pub message: String,
    /// The envelope's free room, on a refused or deferred session grow.
    pub free: Option<std::collections::BTreeMap<String, f64>>,
    pub job_id: String,
    pub task_name: String,
    pub runtime: String,
}

/// A request the node cannot act on at all, as opposed to one it answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResizeError {
    /// This node runs no such unit.
    NotFound(String),
    /// The request itself is wrong.
    Invalid(String),
    /// The unit exists but is not running.
    NotRunning(String),
}

impl std::fmt::Display for ResizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResizeError::NotFound(message)
            | ResizeError::Invalid(message)
            | ResizeError::NotRunning(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ResizeError {}

/// The free room of an envelope for one session's grow: what the envelope
/// has left, by resource, and the first resource the grow does not fit.
fn envelope_shortfall(
    room: &std::collections::BTreeMap<String, f64>,
    used: &HashMap<String, f64>,
    current: &std::collections::BTreeMap<String, f64>,
    wanted: &std::collections::BTreeMap<String, f64>,
) -> (std::collections::BTreeMap<String, f64>, Option<String>) {
    let mut free = std::collections::BTreeMap::new();
    let mut short = None;
    for (name, capacity) in room {
        let left = (capacity - used.get(name).copied().unwrap_or(0.0)).max(0.0);
        free.insert(name.clone(), left);
        let more =
            wanted.get(name).copied().unwrap_or(0.0) - current.get(name).copied().unwrap_or(0.0);
        if more > left + 1e-9 && short.is_none() {
            short = Some(name.clone());
        }
    }
    (free, short)
}

/// The request's amounts off the wire.
pub fn requested_size(to: Option<&crate::structs::ResizeResources>) -> ResourceSize {
    to.map(|to| ResourceSize {
        memory: to.memory,
        memory_max: to.memory_max,
        cpu: to.cpu,
    })
    .unwrap_or_default()
}

/// A node's answer on the wire (`ResizeSessionReply`).
pub fn wire_reply(done: &UnitResize, node_id: &str) -> crate::structs::ResizeSessionReply {
    crate::structs::ResizeSessionReply {
        outcome: done.outcome.as_str().to_string(),
        from: done.from.to_json().to_string(),
        to: done.to.to_json().to_string(),
        message: done.message.clone(),
        free: done
            .free
            .as_ref()
            .map(|free| crate::structs::session::amounts_json(free).to_string())
            .unwrap_or_default(),
        node_id: node_id.to_string(),
        job_id: done.job_id.clone(),
        task_name: done.task_name.clone(),
        runtime: done.runtime.clone(),
        deduplicated: false,
    }
}

/// How long a node keeps the answer of a keyed session resize, the window
/// session runs keep theirs for.
const RESIZE_REPLY_TTL: std::time::Duration = std::time::Duration::from_secs(600);

type KeptReplies =
    HashMap<(String, String), (std::time::Instant, crate::structs::ResizeSessionReply)>;

fn kept_replies() -> &'static std::sync::Mutex<KeptReplies> {
    static KEPT: std::sync::OnceLock<std::sync::Mutex<KeptReplies>> = std::sync::OnceLock::new();
    KEPT.get_or_init(Default::default)
}

/// The answer this node gave an applied session resize under `key`, when
/// it still keeps it: a retry gets it back instead of resizing again.
/// Per node and in memory, so a worker restart forgets it; a session
/// resize sets absolute amounts, so the repeat that follows is harmless.
pub fn recall_resize(session_id: &str, key: &str) -> Option<crate::structs::ResizeSessionReply> {
    if key.is_empty() {
        return None;
    }
    let mut kept = kept_replies().lock().ok()?;
    let now = std::time::Instant::now();
    kept.retain(|_, (at, _)| now.duration_since(*at) < RESIZE_REPLY_TTL);
    kept.get(&(session_id.to_string(), key.to_string()))
        .map(|(_, reply)| crate::structs::ResizeSessionReply {
            deduplicated: true,
            ..reply.clone()
        })
}

/// Keep an applied session resize's answer under its idempotency key.
pub fn remember_resize(session_id: &str, key: &str, reply: &crate::structs::ResizeSessionReply) {
    if key.is_empty() {
        return;
    }
    if let Ok(mut kept) = kept_replies().lock() {
        kept.insert(
            (session_id.to_string(), key.to_string()),
            (std::time::Instant::now(), reply.clone()),
        );
    }
}

/// The status for a resize this node could not act on.
pub fn resize_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast_ref::<ResizeError>() {
        Some(ResizeError::NotFound(message)) => tonic::Status::not_found(message.clone()),
        Some(ResizeError::Invalid(message)) => tonic::Status::invalid_argument(message.clone()),
        Some(ResizeError::NotRunning(message)) => {
            tonic::Status::failed_precondition(message.clone())
        }
        None => tonic::Status::internal(format!("resize failed: {err:#}")),
    }
}

impl WorkerTaskManager {
    /// Resize one running unit in place.
    ///
    /// `Err(ResizeError)` (downcast from the error) is a request this node
    /// cannot act on; every other answer, including a runtime's
    /// `needs_restart`, is an `Ok` with its outcome. With `dry_run` the
    /// answer is what a real call would give and nothing changes.
    pub fn resize_unit(
        &mut self,
        target: &ResizeTarget,
        request: &ResourceSize,
        force: bool,
        dry_run: bool,
    ) -> R<UnitResize> {
        request
            .validate_request()
            .map_err(|err| ResizeError::Invalid(format!("{err}")))?;
        let (local, session) = match target {
            ResizeTarget::Session(session_id) => {
                if self.idle_sessions.contains_key(session_id) {
                    return Err(ResizeError::NotRunning(format!(
                        "session {session_id} is suspended; start it, then resize it"
                    ))
                    .into());
                }
                let claim = self
                    .claims
                    .by_instance(session_id)
                    .filter(|claim| claim.is_session() && !claim.is_ratified())
                    .ok_or_else(|| {
                        ResizeError::NotFound(format!("session {session_id} is not on this node"))
                    })?;
                (claim.local_key, Some(claim.clone()))
            }
            ResizeTarget::Allocation(allocation_id) => (self.local_key(*allocation_id), None),
        };
        let unit = match target {
            ResizeTarget::Session(session_id) => format!("session {session_id}"),
            ResizeTarget::Allocation(allocation_id) => format!("allocation {allocation_id}"),
        };
        let (Some(allocation), Some(task)) = (
            self.allocations.get(&local).cloned(),
            self.task_configs.get(&local).cloned(),
        ) else {
            return Err(ResizeError::NotFound(format!("{unit} is not on this node")).into());
        };
        if !self.instances.contains_key(&local) || self.terminating.contains_key(&local) {
            return Err(ResizeError::NotRunning(format!("{unit} is not running")).into());
        }
        if crate::structs::session::envelope_serves(&task).is_some() {
            return Err(ResizeError::Invalid(format!(
                "{unit} is a session envelope; it resizes through stateful.envelope \
                 (docs/session-mode.md, \"Elastic envelopes\")"
            ))
            .into());
        }
        let from = task.resources.clone();
        let to = request
            .apply_to(&from)
            .map_err(|err| ResizeError::Invalid(format!("{err}")))?;
        let mut answer = UnitResize {
            outcome: ResizeOutcome::Applied,
            from: ResourceSize::of(&from),
            to: ResourceSize::of(&to),
            message: String::new(),
            free: None,
            job_id: allocation.job_id.clone(),
            task_name: task.name.clone(),
            runtime: task.runtime.clone(),
        };

        // What the runtime can change at all, before anything is admitted.
        let support = self
            .registry
            .plugin(&task.runtime)
            .map(|plugin| plugin.resize_support())
            .unwrap_or_default();
        if let Some(resource) = support.first_unsupported(request) {
            answer.outcome = ResizeOutcome::NotSupported;
            answer.message = if support.any() {
                format!(
                    "the {} runtime cannot change {resource} on a running unit",
                    task.runtime
                )
            } else {
                format!("the {} runtime cannot resize a running unit", task.runtime)
            };
            return Ok(answer);
        }

        // A session's grow is admitted against its envelope first.
        let demand_key = (allocation.job_id.clone(), task.name.clone());
        if let Some(claim) = session.as_ref() {
            let current = crate::structs::session::resource_amounts(&from);
            let wanted = crate::structs::session::resource_amounts(&to);
            let grows = wanted
                .iter()
                .any(|(name, amount)| *amount > current.get(name).copied().unwrap_or(0.0) + 1e-9);
            if grows {
                let Some((envelope, envelope_task)) =
                    self.envelope_for(&claim.job_id, &claim.task_name)
                else {
                    return Err(ResizeError::NotRunning(format!(
                        "{unit} has no envelope on this node right now"
                    ))
                    .into());
                };
                let room = self
                    .envelope_sizes
                    .get(&envelope)
                    .map(|size| size.room())
                    .unwrap_or_else(|| {
                        crate::structs::session::resource_amounts(&envelope_task.resources)
                    });
                self.forget_stopped_promotions();
                let used = self.claims.session_usage(&claim.job_id, &claim.task_name);
                let (free, short) = envelope_shortfall(&room, &used, &current, &wanted);
                if let Some(resource) = short {
                    let more = wanted.get(&resource).copied().unwrap_or(0.0)
                        - current.get(&resource).copied().unwrap_or(0.0);
                    // An elastic envelope whose ceiling holds the grow is
                    // asked for the room on the node's next heartbeat.
                    let elastic_fits = self
                        .session_specs
                        .get(&envelope)
                        .and_then(|spec| crate::structs::session::elastic_bounds(&spec.task))
                        .is_some_and(|bounds| {
                            wanted.iter().all(|(name, amount)| {
                                let more = amount - current.get(name).copied().unwrap_or(0.0);
                                bounds.max.get(name).is_none_or(|ceiling| {
                                    used.get(name).copied().unwrap_or(0.0) + more <= ceiling + 1e-9
                                })
                            })
                        });
                    answer.free = Some(free.clone());
                    let left = free.get(&resource).copied().unwrap_or(0.0);
                    if elastic_fits {
                        answer.outcome = ResizeOutcome::Deferred;
                        answer.message = format!(
                            "the {} envelope on this node has {} {resource} free and the grow \
                             needs {} more; the envelope is elastic and is growing to make room",
                            claim.task_name,
                            crate::structs::resize::json_amount(left),
                            crate::structs::resize::json_amount(more),
                        );
                        if !dry_run {
                            let extra = wanted
                                .iter()
                                .map(|(name, amount)| {
                                    let more = amount - current.get(name).copied().unwrap_or(0.0);
                                    (name.clone(), more.max(0.0))
                                })
                                .collect();
                            self.resize_demand.insert(
                                demand_key,
                                (extra, std::time::Instant::now() + RESIZE_DEMAND_TTL),
                            );
                        }
                    } else {
                        answer.outcome = ResizeOutcome::Refused;
                        answer.message = format!(
                            "the {} envelope on this node has {} {resource} free; the grow \
                             needs {} more",
                            claim.task_name,
                            crate::structs::resize::json_amount(left),
                            crate::structs::resize::json_amount(more),
                        );
                    }
                    return Ok(answer);
                }
            }
        }

        // Tell the runtime. It changes what it can live and answers for the
        // rest; it never restarts the unit.
        let change = ResizeChange {
            from: from.clone(),
            to: to.clone(),
            force,
            dry_run,
        };
        let runtime_answer = {
            let plugin = self
                .registry
                .plugin(&task.runtime)
                .context("task runtime is not loaded")?;
            let instance = self
                .instances
                .get_mut(&local)
                .context("the unit stopped while it was being resized")?;
            plugin.resize(&allocation, &task, instance, &change)?
        };
        answer.outcome = runtime_answer.outcome();
        answer.message = runtime_answer.message().to_string();
        if runtime_answer != ResizeAnswer::Applied || dry_run {
            return Ok(answer);
        }

        // Applied: the unit's own record follows, so a restart in place, a
        // worker restart, and a suspend and resume all keep the new size.
        if let Some(task) = self.task_configs.get_mut(&local) {
            task.resources = to.clone();
        }
        if let Some(allocation) = self.allocations.get_mut(&local) {
            let mut row = allocation
                .resources
                .as_object()
                .cloned()
                .unwrap_or_default();
            for key in RESIZABLE_KEYS {
                match to.get(key) {
                    Some(value) => row.insert(key.to_string(), value.clone()),
                    None => row.remove(key),
                };
            }
            allocation.resources = Value::Object(row);
        }
        if let Some(claim) = session.as_ref() {
            self.claims.set_resources(&claim.claim_id, to.clone());
            self.resize_demand.remove(&demand_key);
            if let Some(payload) = self.session_payloads.get_mut(&claim.instance_id) {
                payload.task_resources = to.to_string();
            }
            self.log_running_session(local);
        } else {
            // A promoted session still runs inside its envelope: what it
            // holds there follows its new size.
            if let Some(claim) = self.claims.by_local_key(local).cloned() {
                self.claims.set_resources(&claim.claim_id, to.clone());
            }
            self.resize_converged.remove(&local);
            self.persist_state();
        }
        info!(
            unit = %unit,
            job_id = %answer.job_id,
            task = %answer.task_name,
            from = %answer.from.to_json(),
            to = %answer.to.to_json(),
            "Unit resized in place"
        );
        Ok(answer)
    }

    /// What deferred session grows are waiting for, by session task: added
    /// to what the task's sessions hold when its elastic envelope is sized,
    /// so the envelope grows to fit the resize. Expired demand is dropped.
    pub(crate) fn resize_demand_for(
        &mut self,
        job_id: &str,
        task_name: &str,
        now: std::time::Instant,
    ) -> std::collections::BTreeMap<String, f64> {
        self.resize_demand.retain(|_, (_, until)| *until > now);
        self.resize_demand
            .get(&(job_id.to_string(), task_name.to_string()))
            .map(|(extra, _)| extra.clone())
            .unwrap_or_default()
    }

    /// Bring running top-level allocations to the size their desired
    /// payload carries, when it differs from the size they run at: a resize
    /// a manager committed whose direct call never reached this node (the
    /// node was unreachable, or restarted in between). Best effort, once
    /// per distinct target: a runtime that cannot make the change keeps
    /// running at its old size, and the manager's row says what was asked.
    pub(crate) fn converge_resized_allocations(&mut self, desired: &[TaskAllocation]) {
        for payload in desired {
            if payload.task_resources.trim().is_empty()
                || payload.task_runtime == crate::structs::session::ENVELOPE_RUNTIME
            {
                continue;
            }
            let local = self.local_key(payload.allocation_id);
            if local < 0 || !self.instances.contains_key(&local) {
                continue;
            }
            let Some(running) = self.task_configs.get(&local) else {
                continue;
            };
            let Ok(wanted) = serde_json::from_str::<Value>(&payload.task_resources) else {
                continue;
            };
            let (have, want) = (
                ResourceSize::of(&running.resources),
                ResourceSize::of(&wanted),
            );
            if have == want {
                self.resize_converged.remove(&local);
                continue;
            }
            let fingerprint = want.to_json().to_string();
            if self.resize_converged.get(&local) == Some(&fingerprint) {
                continue;
            }
            self.resize_converged.insert(local, fingerprint);
            // Only what the payload declares: an amount the payload leaves
            // out is one the manager never set.
            let request = ResourceSize {
                memory: want.memory.filter(|_| want.memory != have.memory),
                memory_max: want
                    .memory_max
                    .filter(|_| want.memory_max != have.memory_max),
                cpu: want.cpu.filter(|_| want.cpu != have.cpu),
            };
            if request.is_empty() {
                continue;
            }
            match self.resize_unit(
                &ResizeTarget::Allocation(payload.allocation_id),
                &request,
                false,
                false,
            ) {
                Ok(done) if done.outcome == ResizeOutcome::Applied => {}
                Ok(done) => warn!(
                    allocation_id = payload.allocation_id,
                    outcome = done.outcome.as_str(),
                    message = %done.message,
                    "A committed resize could not be applied in place; the allocation keeps \
                     its running size until it restarts"
                ),
                Err(err) => debug!(
                    allocation_id = payload.allocation_id,
                    error = %err,
                    "Could not converge an allocation to its committed size"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_keyed_session_resize_is_answered_once_test() {
        let reply = crate::structs::ResizeSessionReply {
            outcome: "applied".into(),
            from: r#"{"memory":64}"#.into(),
            to: r#"{"memory":128}"#.into(),
            ..Default::default()
        };
        // No key, nothing kept.
        remember_resize("machine-a.01", "", &reply);
        assert!(recall_resize("machine-a.01", "").is_none());

        remember_resize("machine-a.01", "caller:grow-once", &reply);
        let kept = recall_resize("machine-a.01", "caller:grow-once").expect("kept");
        assert!(kept.deduplicated);
        assert_eq!(kept.from, reply.from);
        // The key is scoped to the session it was used on.
        assert!(recall_resize("machine-b.02", "caller:grow-once").is_none());
        assert!(recall_resize("machine-a.01", "caller:another").is_none());
    }

    fn amounts(pairs: &[(&str, f64)]) -> std::collections::BTreeMap<String, f64> {
        pairs
            .iter()
            .map(|(name, amount)| (name.to_string(), *amount))
            .collect()
    }

    /// A session grow is checked against what the envelope has left, by
    /// resource: only the difference has to fit.
    #[test]
    fn a_session_grow_needs_only_the_difference_free_in_its_envelope() {
        let room = amounts(&[("cpu", 4000.0), ("memory", 8192.0)]);
        let used: HashMap<String, f64> =
            [("cpu".to_string(), 3000.0), ("memory".to_string(), 6144.0)]
                .into_iter()
                .collect();
        let current = amounts(&[("cpu", 1000.0), ("memory", 1024.0)]);
        // 2 GiB more memory fits in the 2 GiB left.
        let (free, short) = envelope_shortfall(
            &room,
            &used,
            &current,
            &amounts(&[("cpu", 1000.0), ("memory", 3072.0)]),
        );
        assert_eq!(free, amounts(&[("cpu", 1000.0), ("memory", 2048.0)]));
        assert_eq!(short, None);
        // 1 MiB past it does not.
        let (_, short) = envelope_shortfall(
            &room,
            &used,
            &current,
            &amounts(&[("cpu", 1000.0), ("memory", 3073.0)]),
        );
        assert_eq!(short.as_deref(), Some("memory"));
        // A shrink never needs room.
        let (_, short) = envelope_shortfall(
            &room,
            &used,
            &current,
            &amounts(&[("cpu", 500.0), ("memory", 512.0)]),
        );
        assert_eq!(short, None);
    }
}
