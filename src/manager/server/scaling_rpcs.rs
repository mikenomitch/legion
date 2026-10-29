//! The read half of the scale surface: `GetJobScaleStatus` and
//! `GetScalingInputs` (docs/autoscaling.md).
//!
//! `GetScalingInputs` joins two sources. Replica counts, the desired count,
//! and rollouts come from the replicated state store, which every manager
//! holds. Measured usage and requests come from the merged scaling-input
//! rollups (`crate::manager::scaling_inputs`), which only the status-lane
//! leader holds, so the RPC is answered there and forwarded by everyone
//! else.

use super::*;
use crate::manager::scaling_inputs::{
    HistoryPoint, TargetKey, TargetTotals, CPU_HISTOGRAM_BOUNDS, HISTORY_SPAN,
};
use crate::storage::store::{ScalingReplicaRow, TaskScale};
use crate::structs::{
    GetScalingInputsReply, GetScalingInputsRequest, ScalingCpuInputs, ScalingDataFreshness,
    ScalingInputs, ScalingInputsPoint, ScalingMemoryInputs, ScalingRequestInputs,
    ScalingReservation, ScalingTargetSelector, TaskScaleStatus,
};

/// Most selectors one GetScalingInputs call takes.
pub(super) const SCALING_TARGETS_MAX: usize = 1000;

/// The GetJobScaleStatus rows for `config`, the job's latest config: every
/// effective count read through [`Store::effective_task_count`], the one
/// accessor the scheduler, rollouts, and quota use, so the status can never
/// disagree with placement. An override that does not bind to the current
/// spec (`TaskScale::binds_to`) is not reported as the override.
pub(crate) fn task_scale_statuses(
    store: &Store,
    job_id: &str,
    config: &JobConfig,
    scales: &[TaskScale],
) -> R<Vec<TaskScaleStatus>> {
    let now_ms = super::autoscale_rpcs::now_unix_ms();
    let autoscaled = config
        .tasks
        .iter()
        .any(crate::manager::autoscale::is_autoscaled);
    let layers = if autoscaled {
        Some(crate::manager::autoscale::AutoscaleLayers::load(store)?)
    } else {
        None
    };
    let pauses = if autoscaled {
        store.scaling_pauses()?
    } else {
        Vec::new()
    };
    let board = store.scaling_status();
    config
        .tasks
        .iter()
        .map(|task| {
            let spec_count = task.count.num;
            let scale = scales
                .iter()
                .find(|scale| scale.task_group == task.group && scale.task_name == task.name);
            let effective_count =
                store.effective_task_count(job_id, &task.group, &task.name, spec_count)?;
            let is_autoscaled = crate::manager::autoscale::is_autoscaled(task);
            let policy_summary = match (is_autoscaled, layers.as_ref()) {
                (true, Some(layers)) => match layers.resolve(job_id, task) {
                    Ok(Some(resolved)) => crate::manager::autoscale::summary_line(&resolved.policy),
                    Ok(None) => String::new(),
                    Err(err) => format!("invalid: {err}"),
                },
                _ => String::new(),
            };
            let entry = is_autoscaled
                .then(|| board.target(job_id, &task.group, &task.name))
                .flatten();
            let state = match (&entry, is_autoscaled) {
                (_, false) => String::new(),
                (Some(entry), true)
                    if board.is_stale(entry.received, std::time::Instant::now()) =>
                {
                    "stale".to_string()
                }
                (Some(entry), true) => entry.state.clone(),
                (None, true) => "not-evaluated".to_string(),
            };
            Ok(TaskScaleStatus {
                autoscaled: is_autoscaled,
                policy_summary,
                pin: scale
                    .and_then(|scale| scale.pin.as_ref())
                    .filter(|pin| pin.active(now_ms))
                    .map(super::autoscale_rpcs::pin_proto),
                pause: is_autoscaled
                    .then(|| {
                        crate::storage::store::covering_pause(
                            &pauses,
                            job_id,
                            &task.group,
                            &task.name,
                            now_ms,
                        )
                        .map(super::autoscale_rpcs::pause_proto)
                    })
                    .flatten(),
                last_evaluation_unix_ms: entry
                    .as_ref()
                    .map_or(0, |entry| entry.evaluated_at_unix_ms),
                state,
                version: scale.map_or(0, |scale| scale.version),
                task_group: task.group.clone(),
                task_name: task.name.clone(),
                spec_count,
                override_count: scale
                    .filter(|scale| scale.binds_to(spec_count))
                    .and_then(|scale| scale.count),
                pending_count: scale.and_then(|scale| scale.pending_count),
                effective_count,
                source: scale.map(|scale| scale.source.clone()).unwrap_or_default(),
                reason: scale.map(|scale| scale.reason.clone()).unwrap_or_default(),
                updated_at: scale
                    .map(|scale| scale.updated_at.clone())
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// One job (and an optional narrowing) a request resolved to.
struct JobSelection {
    job: String,
    group: Option<String>,
    task: Option<String>,
}

impl JobSelection {
    fn from_selector(selector: ScalingTargetSelector) -> Self {
        let narrow = |value: Option<String>| {
            value
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Self {
            job: selector.job.trim().to_string(),
            group: narrow(selector.task_group),
            task: narrow(selector.task_name),
        }
    }

    fn error_entry(&self, error: String) -> ScalingInputs {
        ScalingInputs {
            job_id: self.job.clone(),
            task_group: self.group.clone().unwrap_or_default(),
            task_name: self.task.clone().unwrap_or_default(),
            error,
            ..Default::default()
        }
    }
}

/// What the store says about one task, before the measured inputs join it.
struct StoreFacts {
    inputs: ScalingInputs,
    key: TargetKey,
    service: Option<String>,
}

/// The replica counts, desired count, and reservation of every task of one
/// job the selection names.
/// `pauses` is every scaling pause ([`Store::scaling_pauses`]), read once
/// per call rather than once per job.
fn store_facts(
    store: &Store,
    selection: &JobSelection,
    pauses: &[crate::storage::store::PauseRow],
) -> R<Vec<Result<StoreFacts, String>>> {
    let Some(config) = store.get_job_config_optional(&selection.job)? else {
        return Ok(vec![Err("job not found".to_string())]);
    };
    let job_id = selection.job.as_str();
    let tasks: Vec<&TaskConfig> = config
        .tasks
        .iter()
        .filter(|task| {
            selection
                .group
                .as_deref()
                .is_none_or(|group| task.group == group)
        })
        .filter(|task| {
            selection
                .task
                .as_deref()
                .is_none_or(|name| task.name == name)
        })
        .collect();
    if tasks.is_empty() {
        return Ok(vec![Err(format!(
            "no task matches group={} task={}",
            selection.group.as_deref().unwrap_or("<any>"),
            selection.task.as_deref().unwrap_or("<any>"),
        ))]);
    }
    let stopped = store
        .job_lifecycle_flag(job_id)?
        .is_some_and(|flag| flag == "stopped" || flag == "failed");
    let replicas = store.scaling_replica_rows(job_id)?;
    let rollout_active = !store.list_rollouts(Some(job_id), true)?.is_empty();
    let scales = store.task_scales_for_job(job_id)?;
    let now_ms = super::autoscale_rpcs::now_unix_ms();
    let namespace = crate::structs::namespace::job_namespace(job_id);
    let mut out = Vec::with_capacity(tasks.len());
    for task in tasks {
        let mine: Vec<&ScalingReplicaRow> = replicas
            .iter()
            .filter(|row| row.task_group == task.group && row.task_name == task.name)
            .collect();
        let scale = scales
            .iter()
            .find(|scale| scale.task_group == task.group && scale.task_name == task.name);
        let placed = mine.len() as u32;
        let ready = mine.iter().filter(|row| row.healthy).count() as u32;
        let keyed = task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed);
        let count_scalable = !keyed && task.count.per.is_none();
        let desired = if stopped {
            0
        } else if count_scalable {
            store.effective_task_count(job_id, &task.group, &task.name, task.count.num)?
        } else {
            placed
        };
        // Batch and cron tasks end by design: a finished replica is not a
        // missing one.
        let runs_forever =
            task.lifecycle != crate::structs::TaskLifecycle::Batch && task.schedule.is_none();
        let unplaced = if count_scalable && runs_forever {
            desired.saturating_sub(placed)
        } else {
            0
        };
        let request = store.resource_policy.usage_request(&task.resources).ok();
        let amount = |name: &str| {
            request
                .as_ref()
                .and_then(|request| request.amount(name))
                .unwrap_or(0.0)
        };
        let service = task.service.as_ref().map(|service| {
            crate::structs::namespace::job_id(namespace, service.service_name(&task.name))
        });
        out.push(Ok(StoreFacts {
            inputs: ScalingInputs {
                job_id: job_id.to_string(),
                task_group: task.group.clone(),
                task_name: task.name.clone(),
                desired_replicas: desired,
                count_scalable,
                placed_replicas: placed,
                ready_replicas: ready,
                unplaced_replicas: unplaced,
                rollout_active,
                reservation: Some(ScalingReservation {
                    cpu_mhz: amount("cpu"),
                    cores: amount("cores"),
                    memory_mib: amount("memory"),
                }),
                override_version: scale.map_or(0, |scale| scale.version),
                pin: scale
                    .and_then(|scale| scale.pin.as_ref())
                    .filter(|pin| pin.active(now_ms))
                    .map(super::autoscale_rpcs::pin_proto),
                pause: crate::storage::store::covering_pause(
                    pauses,
                    job_id,
                    &task.group,
                    &task.name,
                    now_ms,
                )
                .map(super::autoscale_rpcs::pause_proto),
                ..Default::default()
            },
            key: TargetKey::new(job_id, &task.group, &task.name),
            service,
        }));
    }
    Ok(out)
}

/// Every task of `job_id`'s replica counts (desired, healthy, unplaced), as
/// `GetScalingInputs` reports them: the history buffer records them on its
/// points for autoscaled tasks.
pub(crate) fn scaling_replica_counts(
    store: &Store,
    job_id: &str,
) -> R<Vec<(TargetKey, crate::manager::scaling_inputs::ReplicaCounts)>> {
    let selection = JobSelection {
        job: job_id.to_string(),
        group: None,
        task: None,
    };
    let pauses = store.scaling_pauses()?;
    Ok(store_facts(store, &selection, &pauses)?
        .into_iter()
        .flatten()
        .map(|facts| {
            (
                facts.key,
                crate::manager::scaling_inputs::ReplicaCounts {
                    desired: facts.inputs.desired_replicas,
                    healthy: facts.inputs.ready_replicas,
                    unplaced: facts.inputs.unplaced_replicas,
                },
            )
        })
        .collect())
}

/// Join measured totals into a task's entry.
fn fill_measured(
    inputs: &mut ScalingInputs,
    totals: Option<&TargetTotals>,
    service: Option<String>,
    state: crate::manager::scaling_inputs::DataState,
) {
    let totals = totals.cloned().unwrap_or_default();
    inputs.cpu = Some(ScalingCpuInputs {
        measured_replicas: totals.replicas,
        used_millicores: totals.cpu_used_millicores,
        used_mhz: totals.cpu_used_mhz,
        reserved_mhz: totals.cpu_reserved_mhz,
        utilization: totals.cpu_utilization(),
        max_utilization: totals.cpu_utilization_max,
        histogram: totals.cpu_histogram.to_vec(),
        histogram_bounds: CPU_HISTOGRAM_BOUNDS.to_vec(),
        unmeasurable_replicas: totals.cpu_unmeasurable,
    });
    inputs.memory = Some(ScalingMemoryInputs {
        measured_replicas: totals.replicas,
        used_mib: totals.memory_used_mib(),
        reserved_mib: totals.memory_reserved_mib,
        utilization: totals.memory_utilization(),
        max_utilization: totals.memory_utilization_max,
    });
    inputs.requests = service.map(|service| ScalingRequestInputs {
        service,
        requests_per_second: totals.requests_per_second,
        errors_per_second: totals.errors_per_second,
        error_ratio: totals.error_ratio(),
        latency_ms_mean: totals.latency_ms_mean(),
        latency_ms_max: totals.edge_latency_ms_max,
        calling_nodes: totals.calling_nodes,
    });
    inputs.freshness = Some(ScalingDataFreshness {
        state: state.as_str().to_string(),
        sampled_at_unix_ms: totals.sampled_at_unix_ms,
        age_seconds: u32::try_from(totals.age.as_secs()).unwrap_or(u32::MAX),
        reporting_nodes: totals.reporting_nodes,
    });
}

impl ManagerDaemonService {
    #[instrument(skip_all, fields(targets = request.get_ref().targets.len()))]
    pub(super) async fn get_scaling_inputs_rpc(
        &self,
        request: Request<GetScalingInputsRequest>,
    ) -> Result<Response<GetScalingInputsReply>, Status> {
        // Coarse gate before any lookup, so a credential-less caller cannot
        // tell a missing job from a forbidden one; per job below.
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        if req.targets.len() > SCALING_TARGETS_MAX {
            return Err(Status::invalid_argument(format!(
                "at most {SCALING_TARGETS_MAX} targets per call; got {}",
                req.targets.len()
            )));
        }
        // The merged rollups live in the status-lane leader's memory. Any
        // other manager refuses with the typed not-the-leader error, which
        // the leader-forward layer turns into the leader's answer.
        self.store
            .require_lane_leader(crate::storage::Lane::Status, "GetScalingInputs")
            .map_err(|e| map_store_error("Error reading scaling inputs", e))?;

        let selections: Vec<JobSelection> = if req.targets.is_empty() {
            let namespaces = crate::structs::namespace::NamespaceSelection::parse(&req.namespaces)
                .map_err(Status::invalid_argument)?;
            let mut jobs: Vec<String> = self
                .store
                .get_jobs()
                .map_err(|e| map_store_error("Error listing jobs", e))?
                .into_iter()
                .map(|job| job.id)
                .filter(|id| namespaces.matches(crate::structs::namespace::job_namespace(id)))
                .filter(|id| {
                    identity.allows(
                        AclResource::Scale,
                        AclAction::Read,
                        &self.job_read_scope(id),
                    )
                })
                .collect();
            jobs.sort();
            jobs.into_iter()
                .map(|job| JobSelection {
                    job,
                    group: None,
                    task: None,
                })
                .collect()
        } else {
            let mut selections = Vec::with_capacity(req.targets.len());
            for selector in req.targets {
                let selection = JobSelection::from_selector(selector);
                if selection.job.is_empty() {
                    return Err(Status::invalid_argument(
                        "every target needs a job (targets[].job)",
                    ));
                }
                acl::enforce(
                    &identity,
                    AclResource::Scale,
                    AclAction::Read,
                    &self.job_read_scope(&selection.job),
                )?;
                selections.push(selection);
            }
            selections
        };

        // Store reads first, then one short hold of the merged view.
        let mut resolved: Vec<Result<StoreFacts, ScalingInputs>> = Vec::new();
        let pauses = self
            .store
            .scaling_pauses()
            .map_err(|e| map_store_error("Error reading scaling inputs", e))?;
        for selection in &selections {
            let facts = store_facts(&self.store, selection, &pauses)
                .map_err(|e| map_store_error("Error reading scaling inputs", e))?;
            for fact in facts {
                resolved.push(fact.map_err(|error| selection.error_entry(error)));
            }
        }

        let now = std::time::Instant::now();
        let history_seconds = u64::from(req.history_seconds).min(HISTORY_SPAN.as_secs());
        let since_unix_ms = OffsetDateTime::now_utc().unix_timestamp() * 1000
            - i64::try_from(history_seconds * 1000).unwrap_or(i64::MAX);
        let merge = self.store.scaling_rollups().merge();
        let leader_for_seconds = merge
            .responsible_since()
            .map_or(0, |since| now.saturating_duration_since(since).as_secs());
        let inputs = resolved
            .into_iter()
            .map(|entry| match entry {
                Err(error_entry) => error_entry,
                Ok(StoreFacts {
                    mut inputs,
                    key,
                    service,
                }) => {
                    let totals = merge.totals(&key, now);
                    let state = merge.data_state(totals.is_some(), now);
                    fill_measured(&mut inputs, totals.as_ref(), service, state);
                    if history_seconds > 0 {
                        inputs.history = merge
                            .history(&key, since_unix_ms)
                            .into_iter()
                            .map(|point| ScalingInputsPoint {
                                at_unix_ms: point.at_unix_ms,
                                measured_replicas: point.measured_replicas,
                                cpu_used_mhz: f64::from(point.cpu_used_mhz),
                                cpu_utilization: f64::from(point.cpu_utilization),
                                memory_used_mib: f64::from(point.memory_used_mib),
                                memory_utilization: f64::from(point.memory_utilization),
                                requests_per_second: f64::from(point.requests_per_second),
                                errors_per_second: f64::from(point.errors_per_second),
                                latency_ms_mean: f64::from(point.latency_ms_mean),
                                desired_replicas: HistoryPoint::count(point.desired_replicas),
                                ready_replicas: HistoryPoint::count(point.healthy_replicas),
                                unplaced_replicas: HistoryPoint::count(point.unplaced_replicas),
                            })
                            .collect();
                    }
                    inputs
                }
            })
            .collect();
        drop(merge);

        Ok(Response::new(GetScalingInputsReply {
            inputs,
            served_by: self
                .store
                .raft_node_id()
                .unwrap_or_else(|| "local".to_string()),
            leader_for_seconds,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::Submission;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    fn job(count: u32) -> JobConfig {
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "web".into(),
            tasks: vec![TaskConfig {
                count: crate::structs::TaskCount::new(count),
                config: json!({ "message": "hi" }),
                ..TaskConfig::new("api".to_string(), "noop")
            }],
            version: None,
            pending_extensions: Default::default(),
            extensions: Default::default(),
        }
    }

    fn submit(store: &Store, count: u32) -> R<()> {
        store.handle_job_submit(
            &job(count),
            &[Submission {
                path: "web.json".into(),
                content: format!("web-{count}"),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    fn status(store: &Store) -> R<TaskScaleStatus> {
        let config = store.get_job_config("web")?;
        let scales = store.task_scales_for_job("web")?;
        let rows = task_scale_statuses(store, "web", &config, &scales)?;
        rows.into_iter().next().context("one task")
    }

    /// GetJobScaleStatus agrees with `effective_task_count` in every override
    /// state: none, applied, pending-only, and one laid over a different
    /// spec count (which must not bind).
    #[test]
    fn scale_status_agrees_with_effective_task_count() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2)?;
        let effective = |store: &Store| store.effective_task_count("web", "default", "api", 2);

        // No override.
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!((row.effective_count, row.override_count), (2, None));

        // An applied override over the current spec.
        store.set_task_scale_override("web", "default", "api", 5, 2, "operator", "", "me")?;
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!((row.effective_count, row.override_count), (5, Some(5)));

        // An override laid over a DIFFERENT spec count does not bind: the
        // old code reported it as effective; the scheduler never used it.
        store.set_task_scale_override("web", "default", "api", 7, 3, "operator", "", "me")?;
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!((row.effective_count, row.override_count), (2, None));

        // Pending scale-in only.
        store.clear_task_scale("web", "default", "api")?;
        store.set_task_scale_pending("web", "default", "api", 1, 2, "operator", "", "me")?;
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!(row.effective_count, 2);
        assert_eq!(row.pending_count, Some(1));
        Ok(())
    }
}
