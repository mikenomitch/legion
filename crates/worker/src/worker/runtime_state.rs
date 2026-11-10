//! The worker's half of runtime disables (docs/runtime-disable.md).
//!
//! Managers send the set of disabled runtimes on every heartbeat reply and
//! push it on `StreamWork`. The worker keeps it in a [`RuntimeGate`] and
//! refuses every start of a disabled runtime itself: placements, session
//! starts (routed by a manager or presented at the edge with a ticket),
//! claims, resumes, and restarts. So a start with no manager on the path is
//! refused within one heartbeat of the disable.
//!
//! The set is written beside the worker's state file whenever it changes,
//! and read back at start: a worker that restarts while it can reach no
//! manager still holds the last set it was given.
//!
//! A disable with `drain` also takes this node's running sessions of the
//! runtime off it ([`drain_sessions`]): the ones that can be suspended are,
//! and the rest are stopped. Managers drain allocations themselves.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::prelude::*;
use crate::structs::runtime_state::{RuntimeDisableSet, RuntimeDisabledRefusal};
use crate::worker::task_manager::WorkerTaskManager;

/// File name of the persisted set, beside the worker's state file.
const FILE_NAME: &str = "runtime-states.json";

/// The set of disabled runtimes this worker holds, and what it refused.
pub struct RuntimeGate {
    set: RwLock<Arc<RuntimeDisableSet>>,
    /// Where the set is persisted; `None` keeps it in memory only.
    path: Option<PathBuf>,
    /// Starts refused since the last acknowledged heartbeat, by runtime.
    refusals: Mutex<HashMap<String, u64>>,
    /// A session drain is running.
    draining: AtomicBool,
}

impl RuntimeGate {
    /// A gate persisted beside `state_file`, holding whatever an earlier
    /// run of this worker saved there.
    pub fn beside(state_file: &Path) -> Self {
        let path = state_file.parent().map(|dir| dir.join(FILE_NAME));
        let set = path
            .as_deref()
            .and_then(|path| std::fs::read(path).ok())
            .and_then(|bytes| match serde_json::from_slice(&bytes) {
                Ok(set) => Some(RuntimeDisableSet::new(set)),
                Err(err) => {
                    warn!(error = %err, "Ignoring an unreadable runtime-states file");
                    None
                }
            })
            .unwrap_or_default();
        if !set.is_empty() {
            info!(
                disabled = %runtimes(&set),
                "Holding runtime disables from before this worker started"
            );
        }
        RuntimeGate {
            set: RwLock::new(Arc::new(set)),
            path,
            refusals: Mutex::new(HashMap::new()),
            draining: AtomicBool::new(false),
        }
    }

    /// The set held now.
    pub fn current(&self) -> Arc<RuntimeDisableSet> {
        Arc::clone(&self.set.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Replace the set with the managers' current one. Returns whether it
    /// changed. A changed set is persisted; a write failure is logged and
    /// costs only the restart-without-managers case.
    pub fn replace(&self, next: RuntimeDisableSet) -> bool {
        {
            let mut guard = self.set.write().unwrap_or_else(|p| p.into_inner());
            if **guard == next {
                return false;
            }
            info!(
                disabled = %runtimes(&next),
                was = %runtimes(&guard),
                "Runtime disables changed"
            );
            *guard = Arc::new(next.clone());
        }
        if let Some(path) = &self.path {
            let written = serde_json::to_vec(&next.states)
                .map_err(anyhow::Error::from)
                .and_then(|bytes| {
                    if let Some(dir) = path.parent() {
                        std::fs::create_dir_all(dir)?;
                    }
                    let tmp = path.with_extension("json.tmp");
                    std::fs::write(&tmp, bytes)?;
                    std::fs::rename(&tmp, path)?;
                    Ok(())
                });
            if let Err(err) = written {
                warn!(path = %path.display(), error = ?err, "Failed persisting runtime disables");
            }
        }
        true
    }

    /// Refuse a start of `runtime` for job `job_id` when a disable covers
    /// it, counting the refusal. `Ok` when nothing does.
    pub fn check(&self, runtime: &str, job_id: &str) -> Result<(), RuntimeDisabledRefusal> {
        let set = self.current();
        let Some(disable) = set.refusing(runtime, job_id) else {
            return Ok(());
        };
        *self
            .refusals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(runtime.to_string())
            .or_default() += 1;
        crate::telemetry::worker_metrics()
            .runtime_disabled_refusals_total
            .inc(&[runtime]);
        Err(RuntimeDisabledRefusal::new(disable, job_id))
    }

    /// Whether a drain covers `runtime` for job `job_id`.
    pub fn drains(&self, runtime: &str, job_id: &str) -> bool {
        self.current()
            .refusing(runtime, job_id)
            .is_some_and(|disable| disable.drain)
    }

    /// Whether any disable held asks for a drain.
    pub fn any_drain(&self) -> bool {
        self.current().states.iter().any(|state| state.drain)
    }

    /// The refusal counts to report on this heartbeat, emptied.
    pub fn take_refusals(&self) -> Vec<crate::structs::RuntimeRefusals> {
        let mut counts: Vec<_> = self
            .refusals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
            .map(|(runtime, count)| crate::structs::RuntimeRefusals { runtime, count })
            .collect();
        counts.sort_by(|left, right| left.runtime.cmp(&right.runtime));
        counts
    }

    /// Put back counts whose heartbeat was not acknowledged.
    pub fn restore_refusals(&self, counts: Vec<crate::structs::RuntimeRefusals>) {
        let mut held = self.refusals.lock().unwrap_or_else(|p| p.into_inner());
        for entry in counts {
            *held.entry(entry.runtime).or_default() += entry.count;
        }
    }
}

fn runtimes(set: &RuntimeDisableSet) -> String {
    if set.is_empty() {
        return "none".to_string();
    }
    set.states
        .iter()
        .map(|state| format!("{} ({})", state.runtime, state.scope()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// What one session drain did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SessionDrain {
    pub suspended: Vec<String>,
    pub stopped: Vec<String>,
}

/// Take this node's running sessions off every runtime a disable drains:
/// suspend each one that can be suspended (its checkpoint kept, so it
/// resumes with its state once the runtime is enabled), and stop the rest.
/// A session busy with another capture or resume is left for the next
/// call. Blocking (a suspend captures a checkpoint); one drain runs at a
/// time, and a call that finds one running returns at once.
pub fn drain_sessions(manager: &Arc<Mutex<WorkerTaskManager>>) -> SessionDrain {
    use crate::worker::task_manager::session_sleep::{suspend_session, SessionSleepError};
    let gate = manager.lock().expect("task manager").runtime_gate();
    if !gate.any_drain() || gate.draining.swap(true, Ordering::AcqRel) {
        return SessionDrain::default();
    }
    let mut drained = SessionDrain::default();
    let sessions = manager
        .lock()
        .expect("task manager")
        .running_sessions_to_drain();
    for session_id in sessions {
        match suspend_session(manager, &session_id, "runtime_disabled") {
            Ok(_) => {
                info!(session_id = %session_id, "Suspended a session of a disabled runtime");
                drained.suspended.push(session_id);
            }
            Err(err)
                if matches!(
                    err.downcast_ref::<SessionSleepError>(),
                    Some(SessionSleepError::Busy)
                ) =>
            {
                debug!(session_id = %session_id, "Session is busy; the next drain pass takes it");
            }
            Err(err) => {
                let stopped = manager
                    .lock()
                    .expect("task manager")
                    .stop_running_session(&session_id, "runtime disabled (drain)");
                if stopped {
                    info!(
                        session_id = %session_id,
                        why_not_suspended = %format!("{err:#}"),
                        "Stopped a session of a disabled runtime"
                    );
                    drained.stopped.push(session_id);
                }
            }
        }
    }
    gate.draining.store(false, Ordering::Release);
    drained
}

#[cfg(test)]
mod runtime_state_test {
    use super::*;
    use crate::structs::runtime_state::RuntimeDisable;

    fn disable(runtime: &str, namespaces: &[&str], drain: bool) -> RuntimeDisable {
        RuntimeDisable {
            runtime: runtime.into(),
            namespaces: namespaces.iter().map(|s| s.to_string()).collect(),
            reason: "CVE-1".into(),
            since: "2026-11-07T18:04:11Z".into(),
            by: "anonymous".into(),
            drain,
        }
    }

    #[test]
    fn the_gate_refuses_counts_and_reports_once() -> R<()> {
        let dir = tempfile::tempdir()?;
        let gate = RuntimeGate::beside(&dir.path().join("worker_state.json"));
        assert!(gate.check("sandbox", "tenants-a/web").is_ok());
        assert!(gate.replace(RuntimeDisableSet::new(vec![disable(
            "sandbox",
            &["tenants-*"],
            false
        )])));
        let refusal = gate.check("sandbox", "tenants-a/web").unwrap_err();
        assert_eq!(refusal.runtime, "sandbox");
        assert!(
            refusal
                .message
                .contains("is disabled in namespace tenants-a"),
            "{refusal}"
        );
        assert!(gate.check("sandbox", "tenants-b/api").is_err());
        // Out of scope, and another runtime, start as ever.
        assert!(gate.check("sandbox", "web").is_ok());
        assert!(gate.check("exec", "tenants-a/web").is_ok());
        let counts = gate.take_refusals();
        assert_eq!(counts.len(), 1);
        assert_eq!(
            (counts[0].runtime.as_str(), counts[0].count),
            ("sandbox", 2)
        );
        assert!(gate.take_refusals().is_empty(), "reported once");
        // An unacknowledged heartbeat puts them back.
        gate.restore_refusals(counts);
        assert_eq!(gate.take_refusals()[0].count, 2);
        Ok(())
    }

    #[test]
    fn a_worker_without_managers_keeps_the_last_set_across_a_restart() -> R<()> {
        let dir = tempfile::tempdir()?;
        let state_file = dir.path().join("worker_state.json");
        let gate = RuntimeGate::beside(&state_file);
        let set = RuntimeDisableSet::new(vec![disable("container", &[], true)]);
        assert!(gate.replace(set.clone()));
        assert!(!gate.replace(set.clone()), "the same set is not a change");
        // A restarted worker reads the set back before it reaches anyone.
        let restarted = RuntimeGate::beside(&state_file);
        assert_eq!(*restarted.current(), set);
        assert!(restarted.check("container", "web").is_err());
        assert!(restarted.drains("container", "web"));
        assert!(restarted.any_drain());
        // An empty set from a manager clears it, on disk too.
        assert!(restarted.replace(RuntimeDisableSet::default()));
        assert!(RuntimeGate::beside(&state_file).current().is_empty());
        Ok(())
    }

    #[test]
    fn an_unreadable_file_is_ignored() -> R<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join(FILE_NAME), b"not json")?;
        let gate = RuntimeGate::beside(&dir.path().join("worker_state.json"));
        assert!(gate.current().is_empty());
        Ok(())
    }
}
