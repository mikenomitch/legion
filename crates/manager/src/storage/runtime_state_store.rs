//! Storage for runtime disables (docs/runtime-disable.md).
//!
//! One control-lane row per disabled runtime (`runtime_state`), holding the
//! [`RuntimeDisable`] as JSON. The set is tiny and read on every plan and
//! every heartbeat, so each manager keeps it in memory
//! ([`RuntimeStateHub`]): the changeset projection reloads it on every
//! replica the moment a row changes, which is also what wakes each open
//! `StreamWork` stream to push the new set and tells the scheduler leader's
//! wake pass to look at the evals blocked on a runtime.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use rusqlite::Connection;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::raft::capture;
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::runtime_state::{RuntimeDisable, RuntimeDisableSet};

/// One manager's in-memory view of the runtime disables.
#[derive(Debug)]
pub struct RuntimeStateHub {
    set: RwLock<Arc<RuntimeDisableSet>>,
    loaded: AtomicBool,
    /// Bumped whenever the set changes on this replica; work streams watch
    /// it and push the new set.
    changed: tokio::sync::watch::Sender<u64>,
    /// The scheduler leader's wake pass has runtime-blocked evals to check.
    /// Starts set, so a manager that just started (or just took the lane)
    /// checks once.
    wake_pending: AtomicBool,
    /// Runtimes whose disable was lifted or narrowed since the last wake
    /// pass: evals blocked for another reason whose job also has a task of
    /// one of these re-plan too.
    lifted: Mutex<HashSet<String>>,
    /// Starts nodes refused, by runtime, as this manager heard of them.
    refusals: Mutex<HashMap<String, u64>>,
    /// Until when every wake pass re-checks runtime-blocked evals: an eval
    /// that planned against a disable can be written `blocked` just after
    /// the pass that handled the enable, so passes keep looking for a few
    /// seconds after one blocks.
    recheck_until: Mutex<Option<std::time::Instant>>,
}

/// How long wake passes keep re-checking after an eval blocks on a runtime.
const RECHECK_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

impl Default for RuntimeStateHub {
    fn default() -> Self {
        RuntimeStateHub {
            set: RwLock::new(Arc::new(RuntimeDisableSet::default())),
            loaded: AtomicBool::new(false),
            changed: tokio::sync::watch::channel(0).0,
            wake_pending: AtomicBool::new(true),
            lifted: Mutex::new(HashSet::new()),
            refusals: Mutex::new(HashMap::new()),
            recheck_until: Mutex::new(None),
        }
    }
}

impl RuntimeStateHub {
    fn current(&self) -> Arc<RuntimeDisableSet> {
        Arc::clone(&self.set.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Replace the set. Returns whether it differed.
    fn replace(&self, next: RuntimeDisableSet) -> bool {
        let mut guard = self.set.write().unwrap_or_else(|p| p.into_inner());
        if **guard == next {
            return false;
        }
        // A runtime whose record went away, or whose scope no longer covers
        // what it did, may have unblocked work.
        let mut lifted = self.lifted.lock().unwrap_or_else(|p| p.into_inner());
        for old in &guard.states {
            let narrowed = match next.get(&old.runtime) {
                None => true,
                Some(new) => {
                    !new.cluster_wide()
                        && (old.cluster_wide()
                            || old
                                .namespaces
                                .iter()
                                .any(|glob| !new.namespaces.contains(glob)))
                }
            };
            if narrowed {
                lifted.insert(old.runtime.clone());
            }
        }
        // Refusal counts belong to a disable: gone with its record.
        self.refusals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|runtime, _| next.get(runtime).is_some());
        *guard = Arc::new(next);
        true
    }
}

fn disable_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RuntimeDisable> {
    let raw: String = row.get(0)?;
    serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
    })
}

fn disables_on(conn: &Connection) -> R<Vec<RuntimeDisable>> {
    let mut stmt = conn.prepare_cached("SELECT object FROM runtime_state ORDER BY runtime;")?;
    let rows = stmt.query_map([], disable_from_row)?;
    Store::collect_rows(rows)
}

fn invalid(message: String) -> anyhow::Error {
    anyhow::Error::new(StorageError::InvalidInput { message })
}

/// What a `SetRuntimeEnabled` write did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStateChange {
    /// The record after the write; `None` when the runtime is now enabled
    /// everywhere.
    pub state: Option<RuntimeDisable>,
    pub changed: bool,
}

impl Store {
    /// Every runtime disable in force, from this replica's memory.
    pub fn runtime_disables(&self) -> Arc<RuntimeDisableSet> {
        let hub = &self.runtime_states;
        if !hub.loaded.load(Ordering::Acquire) {
            self.reload_runtime_states();
        }
        hub.current()
    }

    /// Re-read the set from the committed rows. Never fails: a read error is
    /// logged and the held set kept, and the next change retries.
    fn reload_runtime_states(&self) -> bool {
        let loaded = self.read_connection().and_then(|conn| disables_on(&conn));
        match loaded {
            Ok(states) => {
                let changed = self.runtime_states.replace(RuntimeDisableSet::new(states));
                self.runtime_states.loaded.store(true, Ordering::Release);
                changed
            }
            Err(err) => {
                warn!(error = ?err, "Failed loading runtime disables; keeping the set held");
                false
            }
        }
    }

    /// A committed change touched `runtime_state` (or the rows were replaced
    /// under the cache): reload, wake the work streams, and have the
    /// scheduler leader's wake pass look at runtime-blocked evals. Runs on
    /// every replica.
    pub(crate) fn runtime_states_changed(&self) {
        // Mid-capture the rows are uncommitted and will be rolled back; the
        // apply-time projection calls this again.
        if capture::in_capture() {
            return;
        }
        if self.reload_runtime_states() {
            self.runtime_states.changed.send_modify(|seq| *seq += 1);
        }
        self.runtime_states
            .wake_pending
            .store(true, Ordering::Release);
        self.capacity_notifier().notify_one();
    }

    /// Changes whenever this replica's set does.
    pub fn runtime_state_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.runtime_states.changed.subscribe()
    }

    /// Whether the wake pass has runtime-blocked evals to check, clearing
    /// the flag, and the runtimes lifted since it last asked.
    pub(crate) fn take_runtime_wake(&self) -> Option<HashSet<String>> {
        let flagged = self
            .runtime_states
            .wake_pending
            .swap(false, Ordering::AcqRel);
        let rechecking = {
            let mut until = self
                .runtime_states
                .recheck_until
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match *until {
                Some(at) if std::time::Instant::now() < at => true,
                Some(_) => {
                    *until = None;
                    false
                }
                None => false,
            }
        };
        if !flagged && !rechecking {
            return None;
        }
        Some(std::mem::take(
            &mut *self
                .runtime_states
                .lifted
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        ))
    }

    /// Put a wake back after a pass that could not finish it.
    pub(crate) fn restore_runtime_wake(&self, lifted: HashSet<String>) {
        self.runtime_states
            .lifted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(lifted);
        self.runtime_states
            .wake_pending
            .store(true, Ordering::Release);
    }

    /// Have the next wake pass check every runtime-blocked eval (a manager
    /// that just took the scheduler lane knows nothing of what changed).
    pub(crate) fn request_runtime_wake(&self) {
        self.runtime_states
            .wake_pending
            .store(true, Ordering::Release);
    }

    /// An eval is about to be written `blocked` on a runtime: keep the wake
    /// passes looking for [`RECHECK_WINDOW`], in case the enable landed
    /// between its plan and its write.
    pub(crate) fn note_runtime_blocked(&self) {
        *self
            .runtime_states
            .recheck_until
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(std::time::Instant::now() + RECHECK_WINDOW);
    }

    /// Add starts a node refused to this manager's counts. Counts for a
    /// runtime that is not disabled here are dropped.
    pub fn note_runtime_refusals(&self, refusals: &[(String, u64)]) {
        if refusals.is_empty() {
            return;
        }
        let set = self.runtime_disables();
        let mut counts = self
            .runtime_states
            .refusals
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for (runtime, count) in refusals {
            if set.get(runtime).is_some() {
                *counts.entry(runtime.clone()).or_default() += count;
            }
        }
    }

    /// Starts refused per disabled runtime, as this manager heard of them.
    pub fn runtime_refusal_counts(&self) -> BTreeMap<String, u64> {
        self.runtime_states
            .refusals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(runtime, count)| (runtime.clone(), *count))
            .collect()
    }

    /// Disable `runtime` for `namespaces` (empty: the whole cluster), or
    /// widen the disable that exists. `since` is kept from the first
    /// disable; `reason`, `by` and `drain` take this call's values.
    #[instrument(err, skip_all, fields(runtime = %runtime))]
    pub fn disable_runtime(
        &self,
        runtime: &str,
        namespaces: Vec<String>,
        reason: &str,
        by: &str,
        drain: bool,
    ) -> R<RuntimeStateChange> {
        let (runtime, reason, by) = (runtime.to_owned(), reason.to_owned(), by.to_owned());
        let change = self.replicated(Lane::Control, "disable_runtime", move |s| {
            s.disable_runtime_apply(&runtime, namespaces.clone(), &reason, &by, drain)
        })?;
        self.runtime_states_changed();
        Ok(change)
    }

    fn disable_runtime_apply(
        &self,
        runtime: &str,
        namespaces: Vec<String>,
        reason: &str,
        by: &str,
        drain: bool,
    ) -> R<RuntimeStateChange> {
        crate::structs::runtime_state::validate_runtime_name(runtime).map_err(invalid)?;
        crate::structs::runtime_state::validate_reason(reason).map_err(invalid)?;
        let namespaces =
            crate::structs::runtime_state::normalize_namespaces(&namespaces).map_err(invalid)?;
        let conn = self.write_connection()?;
        let existing = disables_on(&conn)?
            .into_iter()
            .find(|state| state.runtime == runtime);
        let next = match &existing {
            None => RuntimeDisable {
                runtime: runtime.to_string(),
                namespaces,
                reason: reason.to_string(),
                since: OffsetDateTime::now_utc()
                    .replace_nanosecond(0)?
                    .format(&Rfc3339)?,
                by: by.to_string(),
                drain,
            },
            Some(held) => {
                // Cluster-wide on either side stays cluster-wide: a disable
                // never narrows.
                let scope = if held.cluster_wide() || namespaces.is_empty() {
                    Vec::new()
                } else {
                    let mut all = held.namespaces.clone();
                    all.extend(namespaces);
                    all.sort();
                    all.dedup();
                    all
                };
                RuntimeDisable {
                    runtime: runtime.to_string(),
                    namespaces: scope,
                    reason: reason.to_string(),
                    since: held.since.clone(),
                    by: by.to_string(),
                    drain,
                }
            }
        };
        if existing.as_ref() == Some(&next) {
            return Ok(RuntimeStateChange {
                state: Some(next),
                changed: false,
            });
        }
        conn.execute(
            "INSERT INTO runtime_state (runtime, object) VALUES (?1, ?2)
             ON CONFLICT(runtime) DO UPDATE SET object = excluded.object;",
            rusqlite::params![runtime, serde_json::to_string(&next)?],
        )?;
        drop(conn);
        self.emit_event(crate::storage::events::drafts::runtime_disabled(&next));
        Ok(RuntimeStateChange {
            state: Some(next),
            changed: true,
        })
    }

    /// Enable `runtime` again: everywhere (empty `namespaces`), or for the
    /// named globs of a scoped disable. Enabling a runtime that is not
    /// disabled changes nothing.
    #[instrument(err, skip_all, fields(runtime = %runtime))]
    pub fn enable_runtime(
        &self,
        runtime: &str,
        namespaces: Vec<String>,
        by: &str,
    ) -> R<RuntimeStateChange> {
        let (runtime, by) = (runtime.to_owned(), by.to_owned());
        let change = self.replicated(Lane::Control, "enable_runtime", move |s| {
            s.enable_runtime_apply(&runtime, namespaces.clone(), &by)
        })?;
        self.runtime_states_changed();
        Ok(change)
    }

    fn enable_runtime_apply(
        &self,
        runtime: &str,
        namespaces: Vec<String>,
        by: &str,
    ) -> R<RuntimeStateChange> {
        crate::structs::runtime_state::validate_runtime_name(runtime).map_err(invalid)?;
        let namespaces =
            crate::structs::runtime_state::normalize_namespaces(&namespaces).map_err(invalid)?;
        let conn = self.write_connection()?;
        let Some(held) = disables_on(&conn)?
            .into_iter()
            .find(|state| state.runtime == runtime)
        else {
            return Ok(RuntimeStateChange {
                state: None,
                changed: false,
            });
        };
        let remaining = if namespaces.is_empty() {
            None
        } else {
            if held.cluster_wide() {
                bail!(StorageError::failed_precondition(format!(
                    "runtime {runtime:?} is disabled cluster-wide since {}; a cluster-wide \
                     disable cannot be lifted one namespace at a time\nHint: `legion operator \
                     runtime enable {runtime}`, then `legion operator runtime disable {runtime} \
                     --namespace <glob>` for the namespaces that should stay off",
                    held.since
                )));
            }
            if let Some(missing) = namespaces
                .iter()
                .find(|glob| !held.namespaces.contains(glob))
            {
                bail!(StorageError::failed_precondition(format!(
                    "runtime {runtime:?} is not disabled for {missing:?}; it is disabled for \
                     {}\nHint: name the globs as `legion operator runtime status` lists them",
                    held.scope()
                )));
            }
            let left: Vec<String> = held
                .namespaces
                .iter()
                .filter(|glob| !namespaces.contains(glob))
                .cloned()
                .collect();
            (!left.is_empty()).then_some(left)
        };
        let state = match remaining {
            Some(left) => {
                let next = RuntimeDisable {
                    namespaces: left,
                    ..held.clone()
                };
                conn.execute(
                    "UPDATE runtime_state SET object = ?2 WHERE runtime = ?1;",
                    rusqlite::params![runtime, serde_json::to_string(&next)?],
                )?;
                Some(next)
            }
            None => {
                conn.execute("DELETE FROM runtime_state WHERE runtime = ?1;", [runtime])?;
                None
            }
        };
        drop(conn);
        self.emit_event(crate::storage::events::drafts::runtime_enabled(
            &held,
            &namespaces,
            by,
        ));
        Ok(RuntimeStateChange {
            state,
            changed: true,
        })
    }
}

#[cfg(test)]
mod runtime_state_store_test {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-runtime-state-store-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        Store::start_storage(&ManagerConfigFull {
            store_path: store_dir.to_str().context("path")?.to_string(),
            ..Default::default()
        })
    }

    fn globs(list: &[&str]) -> Vec<String> {
        list.iter().map(|glob| glob.to_string()).collect()
    }

    #[test]
    fn disable_and_enable_round_trip_and_reach_the_cache() -> R<()> {
        let store = setup_store()?;
        assert!(store.runtime_disables().is_empty());
        let watch = store.runtime_state_watch();
        let change = store.disable_runtime("sandbox", vec![], "CVE-1", "anonymous", false)?;
        assert!(change.changed);
        let state = change.state.unwrap();
        assert!(state.cluster_wide());
        assert_eq!(state.reason, "CVE-1");
        assert!(watch.has_changed()?, "streams are woken");
        assert!(store
            .runtime_disables()
            .refusing("sandbox", "tenants-a/web")
            .is_some());
        // The same call again changes nothing.
        let again = store.disable_runtime("sandbox", vec![], "CVE-1", "anonymous", false)?;
        assert!(!again.changed);
        assert_eq!(again.state.unwrap().since, state.since);

        let enabled = store.enable_runtime("sandbox", vec![], "anonymous")?;
        assert!(enabled.changed);
        assert!(enabled.state.is_none());
        assert!(store.runtime_disables().is_empty());
        // Enabling what is not disabled is a no-op, not an error.
        assert!(
            !store
                .enable_runtime("sandbox", vec![], "anonymous")?
                .changed
        );
        let kinds: Vec<String> = store
            .list_scheduler_events(&Default::default())?
            .into_iter()
            .map(|event| event.kind)
            .filter(|kind| kind.starts_with("runtime."))
            .collect();
        assert!(kinds.contains(&"runtime.disabled".to_string()), "{kinds:?}");
        assert!(kinds.contains(&"runtime.enabled".to_string()), "{kinds:?}");
        Ok(())
    }

    #[test]
    fn scoped_disables_union_and_lift_one_glob_at_a_time() -> R<()> {
        let store = setup_store()?;
        store.disable_runtime("sandbox", globs(&["tenants-*"]), "a", "anonymous", false)?;
        let widened = store
            .disable_runtime("sandbox", globs(&["ci"]), "b", "anonymous", true)?
            .state
            .unwrap();
        assert_eq!(widened.namespaces, globs(&["ci", "tenants-*"]));
        assert_eq!(widened.reason, "b");
        assert!(widened.drain);
        let set = store.runtime_disables();
        assert!(set.refusing("sandbox", "ci/build").is_some());
        assert!(set.refusing("sandbox", "web").is_none());

        // A glob the record does not hold is refused by name.
        let err = store
            .enable_runtime("sandbox", globs(&["payments"]), "anonymous")
            .unwrap_err();
        assert!(format!("{err:#}").contains("is not disabled for \"payments\""));
        let left = store
            .enable_runtime("sandbox", globs(&["ci"]), "anonymous")?
            .state
            .unwrap();
        assert_eq!(left.namespaces, globs(&["tenants-*"]));
        assert!(store
            .enable_runtime("sandbox", globs(&["tenants-*"]), "anonymous")?
            .state
            .is_none());
        Ok(())
    }

    #[test]
    fn a_cluster_wide_disable_never_narrows_by_namespace() -> R<()> {
        let store = setup_store()?;
        store.disable_runtime("exec", globs(&["ci"]), "", "anonymous", false)?;
        // Cluster-wide replaces the scope.
        let wide = store
            .disable_runtime("exec", vec![], "", "anonymous", false)?
            .state
            .unwrap();
        assert!(wide.cluster_wide());
        // A scoped disable on top stays cluster-wide.
        let still = store
            .disable_runtime("exec", globs(&["ci"]), "", "anonymous", false)?
            .state
            .unwrap();
        assert!(still.cluster_wide());
        let err = store
            .enable_runtime("exec", globs(&["ci"]), "anonymous")
            .unwrap_err();
        assert!(format!("{err:#}").contains("disabled cluster-wide"));
        Ok(())
    }

    #[test]
    fn invalid_input_is_refused() -> R<()> {
        let store = setup_store()?;
        for (runtime, namespaces) in [
            ("", vec![]),
            ("envelope", vec![]),
            ("sandbox", globs(&["Not A Namespace"])),
        ] {
            let err = store
                .disable_runtime(runtime, namespaces, "", "anonymous", false)
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<StorageError>(),
                    Some(StorageError::InvalidInput { .. })
                ),
                "{err:#}"
            );
        }
        Ok(())
    }

    #[test]
    fn lifted_runtimes_and_refusal_counts_follow_the_record() -> R<()> {
        let store = setup_store()?;
        // The flag starts set (a fresh manager checks once).
        assert_eq!(store.take_runtime_wake(), Some(HashSet::new()));
        assert_eq!(store.take_runtime_wake(), None);
        store.disable_runtime("sandbox", vec![], "", "anonymous", false)?;
        assert_eq!(store.take_runtime_wake(), Some(HashSet::new()));
        store.note_runtime_refusals(&[("sandbox".into(), 3), ("exec".into(), 9)]);
        store.note_runtime_refusals(&[("sandbox".into(), 2)]);
        assert_eq!(
            store.runtime_refusal_counts(),
            BTreeMap::from([("sandbox".to_string(), 5)])
        );
        store.enable_runtime("sandbox", vec![], "anonymous")?;
        assert_eq!(
            store.take_runtime_wake(),
            Some(HashSet::from(["sandbox".to_string()]))
        );
        assert!(store.runtime_refusal_counts().is_empty());
        Ok(())
    }
}
