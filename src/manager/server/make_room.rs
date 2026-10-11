//! Make room for a resume (docs/suspend-and-resume.md, "Capacity for
//! resumes"). A resume from a `local` checkpoint can only run on the node
//! holding the checkpoint. When that node is full, the resume would wait out
//! `suspend.resumeTimeout` and then boot cold or keep waiting. Instead, this
//! sweep suspends the most idle suspendable instances on the node until the
//! resume fits, using the order the node's `suspend` pressure step uses:
//! least important band first, then quiet longest.
//!
//! The rules that keep it from doing harm:
//!
//! - **Who can be suspended.** A running keyed instance whose task has a
//!   `suspend` block, and that either opted in to being suspended for
//!   someone else (`suspend.onPressure = true`) or is quiet right now by its
//!   own idle policy (`suspend.idle`, with the node reporting an idle-since).
//!   Its band must not outrank the resume's: making room never displaces
//!   more important work, the same rule preemption follows. Anything without
//!   a `suspend` block is never touched.
//! - **All or nothing.** Suspends start only when suspending the chosen set
//!   is enough for the resume to fit, counting what each victim keeps held
//!   (`suspend.reserve`). A node that could
//!   never fit the resume loses nothing.
//! - **No thrash.** An instance that became active (started or resumed)
//!   within [`MAKE_ROOM_COOLDOWN`] is never chosen, so two resumes cannot
//!   suspend each other back and forth. A node with any suspend in flight is
//!   left alone until it lands, and each resume gets at most one round per
//!   cooldown.
//!
//! Like the idle sweep, it reads the in-memory usage reports and the store
//! only when a resume is actually waiting, and it is off with manager
//! `suspend.makeRoom = false`.

use std::time::Instant;

use super::*;
use crate::manager::usage::UsageTable;
use crate::resources::{ResourcePool, ResourceRequest, SuspendedHold};
use crate::storage::store::WaitingResume;
use crate::structs::suspend::SUSPEND_REASON_MAKE_ROOM;
use crate::structs::Band;

/// An instance that became active this recently is never suspended to make
/// room, and a resume gets at most one make-room round this often. Long
/// enough to outlast a capture and the resume it frees room for, so the
/// instance a round suspends cannot immediately suspend its suspender.
pub const MAKE_ROOM_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(120);

/// How often a resume that now fits is re-queued for placement. Placement
/// normally follows within one eval, so this only bounds the retry rate.
const NUDGE_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// The make-room sweep's memory between passes. In memory only, like the
/// usage it reads: a manager that restarts may give a resume one more round
/// sooner than the cooldown, which the instance-age rule still bounds.
#[derive(Debug)]
pub struct MakeRoomState {
    /// Resume instance id -> when its last round started suspends.
    rounds: HashMap<String, Instant>,
    /// Resume instance id -> when it was last re-queued for placement.
    nudged: HashMap<String, Instant>,
    cooldown: std::time::Duration,
}

impl Default for MakeRoomState {
    fn default() -> Self {
        Self::with_cooldown(MAKE_ROOM_COOLDOWN)
    }
}

impl MakeRoomState {
    /// A sweep with a different anti-thrash window than
    /// [`MAKE_ROOM_COOLDOWN`]. Tests use it to make freshly created instances
    /// eligible without waiting two minutes.
    pub fn with_cooldown(cooldown: std::time::Duration) -> Self {
        Self {
            rounds: HashMap::new(),
            nudged: HashMap::new(),
            cooldown,
        }
    }
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MakeRoomOutcome {
    /// `(resume instance, suspended instance, checkpoint id)` for every
    /// suspend started.
    pub started: Vec<(String, String, String)>,
    /// Resumes that fit and were re-queued for placement.
    pub nudged: Vec<String>,
    /// `(resume instance, why nothing was done)`.
    pub waiting: Vec<(String, String)>,
}

/// One instance on the resume's node that could be suspended to make room.
#[derive(Debug, Clone)]
pub(crate) struct RoomCandidate {
    pub allocation_id: i64,
    pub instance_id: String,
    pub band: Band,
    /// Unix seconds since which it has been quiet, 0 when unknown (sorts
    /// last).
    pub quiet_since_unix: i64,
    /// What it uses running.
    pub running: ResourceRequest,
    /// What it would keep charged once suspended, if anything.
    pub held: Option<ResourceRequest>,
}

/// Pick the fewest candidates, in make-room order (band ascending, then
/// quiet longest), whose suspension lets `need` fit in `free`. `Some(empty)`
/// when it already fits, `None` when even every candidate is not enough.
pub(crate) fn choose_room(
    free: &ResourcePool,
    need: &ResourceRequest,
    mut candidates: Vec<RoomCandidate>,
) -> Option<Vec<RoomCandidate>> {
    if free.can_fit(need) {
        return Some(Vec::new());
    }
    let quiet_key = |candidate: &RoomCandidate| {
        if candidate.quiet_since_unix > 0 {
            candidate.quiet_since_unix
        } else {
            i64::MAX
        }
    };
    candidates.sort_by(|a, b| {
        a.band
            .rank()
            .cmp(&b.band.rank())
            .then(quiet_key(a).cmp(&quiet_key(b)))
            .then(a.allocation_id.cmp(&b.allocation_id))
    });
    let mut pool = free.clone();
    let mut chosen = Vec::new();
    for candidate in candidates {
        pool.release(&candidate.running);
        if let Some(held) = candidate.held.as_ref() {
            pool.consume(held);
        }
        chosen.push(candidate);
        if pool.can_fit(need) {
            return Some(chosen);
        }
    }
    None
}

/// The make-room sweep, run by the manager daemon's maintenance loop. See
/// the module docs.
pub fn make_room_sweep(
    store: &Store,
    usage: &Mutex<UsageTable>,
    state: &mut MakeRoomState,
) -> MakeRoomOutcome {
    let mut outcome = MakeRoomOutcome::default();
    let waiting = match store.waiting_pinned_resumes() {
        Ok(waiting) => waiting,
        Err(err) => {
            warn!(error = ?err, "Failed listing waiting resumes");
            return outcome;
        }
    };
    let now = Instant::now();
    let live: HashSet<&str> = waiting.iter().map(|w| w.instance_id.as_str()).collect();
    let cooldown = state.cooldown;
    state
        .rounds
        .retain(|id, at| live.contains(id.as_str()) && now.duration_since(*at) < cooldown);
    state
        .nudged
        .retain(|id, at| live.contains(id.as_str()) && now.duration_since(*at) < NUDGE_EVERY);
    if waiting.is_empty() {
        return outcome;
    }
    let metrics = crate::manager::telemetry::metrics();
    // Nodes this pass already started suspends on: the next resume waiting
    // there sees them as in flight on the following pass, not this one.
    let mut acted_on: HashSet<String> = HashSet::new();
    for resume in waiting {
        if acted_on.contains(&resume.node_id) {
            continue;
        }
        match make_room_for(store, usage, state, &resume) {
            Ok(Step::Fits) => {
                if state.nudged.contains_key(&resume.instance_id) {
                    continue;
                }
                state.nudged.insert(resume.instance_id.clone(), now);
                if let Ok(config) = store.get_job_config(&resume.job_id) {
                    match store.enqueue_eval(
                        "job_reschedule",
                        Some(&resume.job_id),
                        Some(&config.id()),
                        None,
                        Some(store.default_eval_priority),
                    ) {
                        Ok(_) => outcome.nudged.push(resume.instance_id.clone()),
                        Err(err) => {
                            warn!(error = ?err, instance_id = %resume.instance_id, "Failed queueing a resume that now fits")
                        }
                    }
                }
            }
            Ok(Step::Suspending(victims)) => {
                state.rounds.insert(resume.instance_id.clone(), now);
                acted_on.insert(resume.node_id.clone());
                let mut started = Vec::new();
                for victim in victims {
                    match super::suspend_rpcs::begin_automatic_suspend(
                        store,
                        victim.allocation_id,
                        &resume.node_id,
                        SUSPEND_REASON_MAKE_ROOM,
                    ) {
                        Ok((instance_id, snapshot_id)) => {
                            metrics.make_room_suspends_total.inc(&["started"]);
                            started.push(instance_id.clone());
                            outcome.started.push((
                                resume.instance_id.clone(),
                                instance_id,
                                snapshot_id,
                            ));
                        }
                        Err(reason) => {
                            metrics.make_room_suspends_total.inc(&["failed"]);
                            warn!(
                                resume = %resume.instance_id,
                                instance_id = %victim.instance_id,
                                node_id = %resume.node_id,
                                reason = %reason,
                                "Make-room suspend could not start"
                            );
                        }
                    }
                }
                if !started.is_empty() {
                    info!(
                        resume = %resume.instance_id,
                        node_id = %resume.node_id,
                        suspending = ?started,
                        "Making room for a resume"
                    );
                    store.events().instance_make_room(
                        &resume.job_id,
                        &resume.instance_id,
                        &resume.node_id,
                        &started,
                    );
                }
            }
            Ok(Step::Wait(reason)) => {
                debug!(resume = %resume.instance_id, node_id = %resume.node_id, reason = %reason, "Resume waits");
                outcome.waiting.push((resume.instance_id.clone(), reason));
            }
            Ok(Step::NoRoom(reason)) => {
                // Counted once per round, not once per pass.
                state.rounds.insert(resume.instance_id.clone(), now);
                metrics.make_room_suspends_total.inc(&["no_room"]);
                info!(resume = %resume.instance_id, node_id = %resume.node_id, reason = %reason, "Cannot make room for a resume");
                outcome.waiting.push((resume.instance_id.clone(), reason));
            }
            Err(err) => {
                if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) {
                    return outcome;
                }
                warn!(error = ?err, resume = %resume.instance_id, "Make-room pass failed");
            }
        }
    }
    outcome
}

enum Step {
    /// The resume fits now: queue its placement.
    Fits,
    /// Suspend these to make it fit.
    Suspending(Vec<RoomCandidate>),
    /// Nothing to do this pass (a round or a suspend is in flight).
    Wait(String),
    /// Suspending everything eligible would not be enough.
    NoRoom(String),
}

fn make_room_for(
    store: &Store,
    usage: &Mutex<UsageTable>,
    state: &MakeRoomState,
    resume: &WaitingResume,
) -> R<Step> {
    let config = store.get_job_config(&resume.job_id)?;
    let task = config
        .tasks
        .iter()
        .find(|task| task.name == resume.task_name)
        .context("the resuming task is not in its job's latest config")?;
    let policy = &store.resource_policy;
    let need = policy.usage_request(&task.resources)?;
    let resume_band = store.cached_band_for_job_config(&config.id())?;
    let node = store.get_node(&resume.node_id)?;
    let allocations = store.allocs_for_node(&resume.node_id)?;
    let free = ResourcePool::free_for_node(&node, &allocations, policy);
    if free.can_fit(&need) {
        return Ok(Step::Fits);
    }
    if state.rounds.contains_key(&resume.instance_id) {
        return Ok(Step::Wait(
            "a make-room round already ran for it within the cooldown".to_string(),
        ));
    }

    let now_unix = chrono::Utc::now().timestamp();
    let cooldown = state.cooldown.as_secs() as i64;
    let usage = usage.lock().unwrap_or_else(|p| p.into_inner());
    let mut candidates = Vec::new();
    for allocation in &allocations {
        if !crate::constants::allocation_status_is_dispatchable(&allocation.status) {
            continue;
        }
        let Some(instance_id) = allocation.instance_id.as_deref() else {
            continue;
        };
        let Some(instance) = store.get_instance(instance_id)? else {
            continue;
        };
        if instance.mode != InstanceMode::Keyed
            || instance.state != InstanceState::Active
            || instance.current_alloc_id != Some(allocation.id)
        {
            continue;
        }
        let pending = store.instance_suspend_state(instance_id)?.pending_op;
        if pending.as_deref() == Some(crate::structs::suspend::PENDING_SUSPEND) {
            // Capacity is already on its way back: let it land first.
            return Ok(Step::Wait(format!(
                "instance {instance_id} is already suspending on {}",
                resume.node_id
            )));
        }
        if pending.is_some() {
            continue;
        }
        let active_for = chrono::DateTime::parse_from_rfc3339(&instance.updated_at)
            .map(|at| now_unix - at.timestamp())
            .unwrap_or(0);
        if active_for < cooldown {
            continue;
        }
        let Some(task) = store
            .cached_task_configs_for_job_config(&allocation.job_config_id)?
            .into_iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .map(|(_, task)| task)
        else {
            continue;
        };
        let Some(settings) = task
            .suspend
            .as_ref()
            .and_then(|suspend| suspend.resolve().ok())
        else {
            continue;
        };
        let measured = usage.allocation(allocation.id);
        let quiet = settings.idle.is_some()
            && measured.is_some_and(|measured| measured.idle_since_unix > 0);
        if !(settings.on_pressure || quiet) {
            continue;
        }
        let band = store.cached_band_for_job_config(&allocation.job_config_id)?;
        if band.outranks(resume_band) {
            continue;
        }
        let Ok(running) = policy.usage_request(&allocation.resources) else {
            continue;
        };
        let held = match policy.suspended_hold(&allocation.resources, settings.reserve)? {
            SuspendedHold::Nothing => None,
            SuspendedHold::Full => Some(running.clone()),
            SuspendedHold::Partial(resources) => Some(policy.usage_request(&resources)?),
        };
        candidates.push(RoomCandidate {
            allocation_id: allocation.id,
            instance_id: instance_id.to_string(),
            band,
            quiet_since_unix: measured
                .map(crate::worker::pressure::quiet_since)
                .unwrap_or(0),
            running,
            held,
        });
    }
    drop(usage);
    let considered = candidates.len();
    match choose_room(&free, &need, candidates) {
        Some(chosen) if chosen.is_empty() => Ok(Step::Fits),
        Some(chosen) => Ok(Step::Suspending(chosen)),
        None => Ok(Step::NoRoom(format!(
            "suspending all {considered} eligible instance(s) on {} would not make room",
            resume.node_id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::ResourceValue;

    fn pool(memory: f64) -> ResourcePool {
        ResourcePool {
            values: HashMap::from([("memory".to_string(), ResourceValue::Amount(memory))]),
        }
    }

    fn request(memory: f64) -> ResourceRequest {
        ResourceRequest {
            values: HashMap::from([("memory".to_string(), ResourceValue::Amount(memory))]),
        }
    }

    fn candidate(id: i64, band: Band, quiet: i64, memory: f64, held: f64) -> RoomCandidate {
        RoomCandidate {
            allocation_id: id,
            instance_id: format!("i-{id}"),
            band,
            quiet_since_unix: quiet,
            running: request(memory),
            held: (held > 0.0).then(|| request(held)),
        }
    }

    fn ids(chosen: Option<Vec<RoomCandidate>>) -> Option<Vec<i64>> {
        chosen.map(|chosen| chosen.iter().map(|c| c.allocation_id).collect())
    }

    #[test]
    fn it_takes_the_least_important_then_the_quietest_until_the_resume_fits() {
        let candidates = vec![
            candidate(1, Band::Normal, 500, 1024.0, 0.0),
            candidate(2, Band::Low, 900, 1024.0, 0.0),
            candidate(3, Band::Normal, 100, 1024.0, 0.0),
            candidate(4, Band::Normal, 0, 1024.0, 0.0),
        ];
        // 512 free, needs 2048: two 1 GiB suspends get it to 2560.
        assert_eq!(
            ids(choose_room(
                &pool(512.0),
                &request(2048.0),
                candidates.clone()
            )),
            Some(vec![2, 3]),
            "low band first, then the normal one quiet since 100"
        );
        assert_eq!(
            ids(choose_room(
                &pool(512.0),
                &request(4096.0),
                candidates.clone()
            )),
            Some(vec![2, 3, 1, 4]),
            "unknown quiet sorts last"
        );
        assert_eq!(
            ids(choose_room(&pool(2048.0), &request(2048.0), candidates)),
            Some(vec![]),
            "nothing to do when it already fits"
        );
    }

    #[test]
    fn it_counts_what_a_victim_keeps_held_and_refuses_partial_rounds() {
        // Each victim frees 1024 but keeps 512 held: two free only 1024.
        let candidates = vec![
            candidate(1, Band::Normal, 100, 1024.0, 512.0),
            candidate(2, Band::Normal, 200, 1024.0, 512.0),
        ];
        assert_eq!(
            ids(choose_room(
                &pool(0.0),
                &request(1024.0),
                candidates.clone()
            )),
            Some(vec![1, 2])
        );
        assert_eq!(
            ids(choose_room(&pool(0.0), &request(1536.0), candidates)),
            None,
            "suspending everything would not be enough, so nothing is suspended"
        );
    }
}
