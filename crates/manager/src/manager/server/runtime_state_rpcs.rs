//! Runtime disable RPCs (docs/runtime-disable.md): `SetRuntimeEnabled` and
//! `ListRuntimeStates`, the drain a disable can ask for, and the warning a
//! submit carries while one of its runtimes is disabled.
//!
//! The record lives in `crate::storage::runtime_state_store`. Refusing
//! placements is the scheduler's (`Scheduler::withhold_disabled_runtimes`),
//! refusing every other start is each worker's
//! (`crate::worker::runtime_state`), and waking blocked evals on enable is
//! the wake pass's (`Store::wake_blocked_evals`).

use std::collections::{BTreeMap, HashMap};

use super::snapshot_rpcs::node_has_capability;
use super::*;
use crate::manager::sessions;
use crate::structs::namespace::job_namespace;
use crate::structs::runtime_state::{
    blocked_key_runtime, RuntimeDisable, RUNTIME_DISABLE_CAPABILITY,
};
use crate::structs::{
    Allocation, ListRuntimeStatesReply, ListRuntimeStatesRequest, QuerySessionsRequest,
    RuntimeDrainSummary, RuntimeRunning, RuntimeStateStatus, SetRuntimeEnabledReply,
    SetRuntimeEnabledRequest,
};

/// `instance.suspended`'s `reason` for a suspend a drain started.
pub(crate) const SUSPEND_REASON_RUNTIME_DISABLED: &str =
    crate::structs::suspend::SUSPEND_REASON_RUNTIME_DISABLED;

/// Live allocations whose task runs on a disabled runtime in scope, with
/// each one's task.
fn allocations_in_scope(
    store: &Store,
    disable: &RuntimeDisable,
) -> R<Vec<(Allocation, TaskConfig)>> {
    let mut tasks_by_config: HashMap<String, Vec<(i64, TaskConfig)>> = HashMap::new();
    let mut out = Vec::new();
    for allocations in store.cached_allocations_by_node()?.into_values() {
        for allocation in allocations {
            if !disable.covers_namespace(job_namespace(&allocation.job_id)) {
                continue;
            }
            if matches!(allocation.status.as_str(), "complete" | "failed") {
                continue;
            }
            let tasks = match tasks_by_config.get(&allocation.job_config_id) {
                Some(tasks) => tasks,
                None => {
                    let loaded = store
                        .cached_task_configs_for_job_config(&allocation.job_config_id)
                        .unwrap_or_default();
                    tasks_by_config
                        .entry(allocation.job_config_id.clone())
                        .or_insert(loaded)
                }
            };
            let Some((_, task)) = tasks
                .iter()
                .find(|(id, _)| *id == allocation.task_config_id)
            else {
                continue;
            };
            if task.runtime == disable.runtime {
                let task = task.clone();
                out.push((allocation, task));
            }
        }
    }
    out.sort_by_key(|(allocation, _)| allocation.id);
    Ok(out)
}

/// The session tasks of a disabled runtime that have an envelope placed, as
/// `(job id, task name)`: where its sessions can be running.
fn session_tasks_in_scope(store: &Store, disable: &RuntimeDisable) -> R<Vec<(String, String)>> {
    let mut seen: BTreeMap<(String, String), ()> = BTreeMap::new();
    let mut configs: HashMap<String, ()> = HashMap::new();
    for allocations in store.cached_allocations_by_node()?.into_values() {
        for allocation in allocations {
            if !disable.covers_namespace(job_namespace(&allocation.job_id))
                || configs
                    .insert(allocation.job_config_id.clone(), ())
                    .is_some()
            {
                continue;
            }
            let tasks = store
                .cached_task_configs_for_job_config(&allocation.job_config_id)
                .unwrap_or_default();
            for (_, task) in tasks {
                let session = task
                    .stateful
                    .as_ref()
                    .is_some_and(|stateful| stateful.mode == InstanceMode::Session);
                if session && task.runtime == disable.runtime {
                    seen.insert((allocation.job_id.clone(), task.name), ());
                }
            }
        }
    }
    Ok(seen.into_keys().collect())
}

/// The static warning a submit, plan, or validate of `config` carries while
/// a runtime one of its tasks uses is disabled for its namespace. The job
/// is accepted: its eval blocks and places when the runtime is enabled.
pub(super) fn submit_warnings(store: &Store, config: &JobConfig) -> Vec<String> {
    let disables = store.runtime_disables();
    if disables.is_empty() {
        return Vec::new();
    }
    let namespace = job_namespace(&config.name);
    let mut by_runtime: BTreeMap<&str, (Vec<&str>, &RuntimeDisable)> = BTreeMap::new();
    for task in &config.tasks {
        if let Some(disable) = disables.refusing(&task.runtime, &config.name) {
            by_runtime
                .entry(task.runtime.as_str())
                .or_insert_with(|| (Vec::new(), disable))
                .0
                .push(task.name.as_str());
        }
    }
    by_runtime
        .into_values()
        .map(|(tasks, disable)| {
            format!(
                "[runtime-disabled] {}: {}; the job is accepted, and its work on this runtime \
                 starts when the runtime is enabled",
                tasks.join(", "),
                disable.refusal(namespace)
            )
        })
        .collect()
}

impl ManagerDaemonService {
    /// What runs on a disabled runtime in scope right now.
    async fn runtime_running(&self, disable: &RuntimeDisable) -> RuntimeRunning {
        let store = self.store.clone();
        let scoped = disable.clone();
        let (allocations, session_tasks) = tokio::task::spawn_blocking(move || {
            (
                allocations_in_scope(&store, &scoped)
                    .map(|found| found.len())
                    .unwrap_or(0),
                session_tasks_in_scope(&store, &scoped).unwrap_or_default(),
            )
        })
        .await
        .unwrap_or_default();
        let mut sessions = 0usize;
        for (job_id, task_name) in session_tasks {
            let view = sessions::query(
                &self.store,
                QuerySessionsRequest {
                    job_id,
                    task_name,
                    ..Default::default()
                },
            )
            .await;
            sessions += view
                .sessions
                .iter()
                .filter(|entry| {
                    entry
                        .status
                        .as_ref()
                        .is_some_and(|status| status.state != "suspended")
                })
                .count();
        }
        RuntimeRunning {
            allocations: allocations as u32,
            sessions: sessions as u32,
        }
    }

    /// The managers' half of a drain: every live allocation of the runtime
    /// in scope is suspended (a keyed instance whose task has a `suspend`
    /// block) or removed for a graceful stop. Sessions are each node's to
    /// drain, from the record's `drain` flag.
    fn drain_allocations(store: &Store, disable: &RuntimeDisable) -> RuntimeDrainSummary {
        let mut summary = RuntimeDrainSummary::default();
        let found = match allocations_in_scope(store, disable) {
            Ok(found) => found,
            Err(err) => {
                summary.failed.push(format!("listing allocations: {err:#}"));
                return summary;
            }
        };
        let mut reschedule: BTreeMap<(String, String), String> = BTreeMap::new();
        for (allocation, task) in found {
            if task.suspend.is_some() && allocation.instance_id.is_some() {
                match super::suspend_rpcs::begin_automatic_suspend(
                    store,
                    allocation.id,
                    &allocation.node_id,
                    SUSPEND_REASON_RUNTIME_DISABLED,
                ) {
                    Ok((instance_id, snapshot_id)) => {
                        info!(
                            instance_id = %instance_id,
                            snapshot_id = %snapshot_id,
                            allocation_id = allocation.id,
                            runtime = %disable.runtime,
                            "Suspending an instance of a disabled runtime"
                        );
                        summary.suspending += 1;
                        continue;
                    }
                    // A suspend already under way is this drain's too.
                    Err(why) if why.contains("already in flight") => {
                        summary.suspending += 1;
                        continue;
                    }
                    Err(why) => {
                        debug!(
                            allocation_id = allocation.id,
                            why = %why,
                            "Could not suspend for a runtime drain; stopping instead"
                        );
                    }
                }
            }
            let explanation = crate::storage::store::StopExplanation {
                detail: format!(
                    "Stopped because {}.",
                    disable.refusal(job_namespace(&allocation.job_id))
                ),
                hint: format!(
                    "It places again on its own after `legion operator runtime enable {}`.",
                    disable.runtime
                ),
            };
            match store.remove_allocation_by_id_explained(
                allocation.id,
                StopCause::RuntimeDisabled,
                Some(explanation),
            ) {
                Ok(Some(removed)) => {
                    summary.stopping += 1;
                    reschedule.insert(
                        (removed.job_id.clone(), removed.job_config_id.clone()),
                        removed.node_id.clone(),
                    );
                }
                Ok(None) => {}
                Err(err) => summary.failed.push(format!("{}: {err:#}", allocation.id)),
            }
        }
        // One eval per job whose work was stopped: it blocks under
        // `runtime_disabled:<runtime>`, so `job status` says why nothing
        // runs, and the enable wakes it.
        for ((job_id, job_config_id), node_id) in reschedule {
            let priority = store
                .cached_band_for_job_config(&job_config_id)
                .map(|band| band.eval_priority())
                .unwrap_or(store.default_eval_priority);
            if let Err(err) = store.enqueue_eval(
                "job_reschedule",
                Some(&job_id),
                Some(&job_config_id),
                Some(&node_id),
                Some(priority),
            ) {
                warn!(job_id = %job_id, error = ?err, "Could not queue the eval for a drained job");
            }
        }
        summary
    }

    // ACL: operator:write.
    pub(super) async fn set_runtime_enabled_rpc(
        &self,
        request: Request<SetRuntimeEnabledRequest>,
    ) -> Result<Response<SetRuntimeEnabledReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let inner = request.into_inner();
        let runtime = inner.runtime.trim().to_string();
        crate::structs::runtime_state::validate_runtime_name(&runtime)
            .map_err(Status::invalid_argument)?;
        let namespaces = crate::structs::runtime_state::normalize_namespaces(&inner.namespaces)
            .map_err(Status::invalid_argument)?;
        let reason = inner.reason.trim().to_string();
        crate::structs::runtime_state::validate_reason(&reason)
            .map_err(Status::invalid_argument)?;
        if inner.enabled && inner.drain {
            return Err(Status::invalid_argument(
                "drain applies to a disable; enabling a runtime drains nothing",
            ));
        }
        if inner.enabled && !reason.is_empty() {
            return Err(Status::invalid_argument(
                "reason applies to a disable; an enable records who and when",
            ));
        }
        let by = match identity.accessor_id.as_deref() {
            Some(accessor) => format!("accessor:{accessor}"),
            None if identity.principal.is_empty() => "unknown".to_string(),
            None => identity.principal.clone(),
        };
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "set_runtime_enabled",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
            .is_some()
        {
            let state = self.store.runtime_disables().get(&runtime).cloned();
            let running = match &state {
                Some(state) => Some(self.runtime_running(state).await),
                None => None,
            };
            return Ok(Response::new(SetRuntimeEnabledReply {
                state: state.as_ref().map(RuntimeDisable::to_proto),
                changed: false,
                running,
                drained: None,
                deduplicated: true,
            }));
        }

        let store = self.store.clone();
        let write = {
            let (runtime, namespaces, reason, by) = (
                runtime.clone(),
                namespaces.clone(),
                reason.clone(),
                by.clone(),
            );
            let (enabled, drain) = (inner.enabled, inner.drain);
            tokio::task::spawn_blocking(move || {
                if enabled {
                    store.enable_runtime(&runtime, namespaces, &by)
                } else {
                    store.disable_runtime(&runtime, namespaces, &reason, &by, drain)
                }
            })
            .await
            .map_err(|err| Status::internal(format!("runtime state write panicked: {err}")))?
        };
        let change = write.map_err(|e| map_store_error("Error writing runtime state", e))?;
        let (_, deduplicated) =
            self.record_idempotent(&idempotency_key, "set_runtime_enabled", runtime.clone())?;

        let drained = match (&change.state, inner.drain && !deduplicated) {
            (Some(state), true) => {
                let store = self.store.clone();
                let state = state.clone();
                Some(
                    tokio::task::spawn_blocking(move || Self::drain_allocations(&store, &state))
                        .await
                        .map_err(|err| {
                            Status::internal(format!("runtime drain panicked: {err}"))
                        })?,
                )
            }
            _ => None,
        };
        let running = match &change.state {
            Some(state) => Some(self.runtime_running(state).await),
            None => None,
        };
        info!(
            runtime = %runtime,
            enabled = inner.enabled,
            scope = %change.state.as_ref().map(RuntimeDisable::scope).unwrap_or_else(|| "cluster".into()),
            drain = inner.drain,
            changed = change.changed,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Runtime state set"
        );
        Ok(Response::new(SetRuntimeEnabledReply {
            state: change.state.as_ref().map(RuntimeDisable::to_proto),
            changed: change.changed && !deduplicated,
            running,
            drained,
            deduplicated,
        }))
    }

    // ACL: operator:read.
    pub(super) async fn list_runtime_states_rpc(
        &self,
        request: Request<ListRuntimeStatesRequest>,
    ) -> Result<Response<ListRuntimeStatesReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let set = self.store.runtime_disables();
        if set.is_empty() {
            return Ok(Response::new(ListRuntimeStatesReply::default()));
        }
        let store = self.store.clone();
        let (blocked, unaware_nodes) = tokio::task::spawn_blocking(move || {
            let blocked = store.blocked_evals().unwrap_or_default();
            let unaware: Vec<String> = store
                .cached_nodes()
                .unwrap_or_default()
                .into_iter()
                .filter(|node| node.status == "ready")
                .filter(|node| !node_has_capability(node, RUNTIME_DISABLE_CAPABILITY))
                .map(|node| node.id)
                .collect();
            (blocked, unaware)
        })
        .await
        .map_err(|err| Status::internal(format!("runtime state read panicked: {err}")))?;
        let refusals = self.store.runtime_refusal_counts();
        let mut disabled = Vec::with_capacity(set.states.len());
        for state in &set.states {
            let blocked_evals = blocked
                .iter()
                .filter(|eval| blocked_key_runtime(&eval.key) == Some(state.runtime.as_str()))
                .count() as u32;
            let mut unaware = unaware_nodes.clone();
            unaware.sort();
            disabled.push(RuntimeStateStatus {
                state: Some(state.to_proto()),
                blocked_evals,
                edge_refusals: refusals.get(&state.runtime).copied().unwrap_or(0),
                running: Some(self.runtime_running(state).await),
                unaware_nodes: unaware,
            });
        }
        Ok(Response::new(ListRuntimeStatesReply { disabled }))
    }
}

#[cfg(test)]
mod runtime_state_rpcs_test {
    use super::*;
    use crate::structs::ManagerConfigFull;

    fn setup_store() -> R<Store> {
        let store_dir = tempfile::Builder::new()
            .prefix("legion-runtime-state-rpcs-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        Store::start_storage(&ManagerConfigFull {
            store_path: store_dir.to_str().context("path")?.to_string(),
            ..Default::default()
        })
    }

    fn job(name: &str, tasks: &[(&str, &str)]) -> JobConfig {
        JobConfig {
            name: name.to_string(),
            tasks: tasks
                .iter()
                .map(|(task, runtime)| TaskConfig::new(*task, *runtime))
                .collect(),
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
            pending_extensions: Default::default(),
            extensions: Default::default(),
        }
    }

    /// A submit warns once per disabled runtime, naming its tasks and the
    /// disable, and only for jobs in scope.
    #[test]
    fn a_submit_warns_for_each_disabled_runtime_in_scope() -> R<()> {
        let store = setup_store()?;
        let mixed = job(
            "tenants-acme/agents",
            &[
                ("machine", "sandbox"),
                ("scratch", "sandbox"),
                ("api", "exec"),
            ],
        );
        assert!(submit_warnings(&store, &mixed).is_empty());

        store.disable_runtime("sandbox", vec!["tenants-*".into()], "GHSA-1", "test", false)?;
        let warnings = submit_warnings(&store, &mixed);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].starts_with(
                "[runtime-disabled] machine, scratch: runtime \"sandbox\" is disabled in \
                 namespace tenants-acme since"
            ),
            "{warnings:?}"
        );
        assert!(warnings[0].contains("GHSA-1"), "{warnings:?}");
        assert!(
            warnings[0].ends_with("starts when the runtime is enabled"),
            "{warnings:?}"
        );
        // Out of scope, or on another runtime: no warning.
        assert!(submit_warnings(&store, &job("agents", &[("machine", "sandbox")])).is_empty());
        assert!(submit_warnings(&store, &job("tenants-acme/api", &[("api", "exec")])).is_empty());
        Ok(())
    }
}
