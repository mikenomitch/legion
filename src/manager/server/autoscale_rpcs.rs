//! Task autoscaling's RPCs (docs/autoscaling.md; autoscaler-v2 plan §6.5,
//! §6.6, §6.13): the scale write with pins and compare-and-set
//! (`ScaleTask`), the policy stream (`WatchScalingPolicies`), the cluster's
//! scaling defaults, the app-plane reports (`ReportScalingStatus`,
//! `RecordScalingAction`), and the read and pause surface people use
//! (`GetScalingStatus`, `GetScaleHistory`, `PauseScaling`).

use super::*;
use crate::autoscaler::defaults::ResolvedPolicy;
use crate::autoscaler::{AutoscaleRecord, EffectivePolicy};
use crate::manager::autoscale::{
    is_autoscaled, policy_of_target_key, policy_target_key, summary_line, target_label,
    targets_for_config, AutoscaleLayers, TargetPolicy,
};
use crate::manager::scaling_status::AppLiveness;
use crate::storage::store::{
    covering_pause, HistoryRow, NewHistory, PauseRow, ScaleWrite, ScaleWriteOp, TaskScale,
    TaskScalePin, PAUSE_ANY,
};
use crate::structs::{
    ApplyScalingDefaultsReply, ApplyScalingDefaultsRequest, GetScaleHistoryReply,
    GetScaleHistoryRequest, GetScalingDefaultsReply, GetScalingDefaultsRequest,
    GetScalingStatusReply, GetScalingStatusRequest, PauseScalingReply, PauseScalingRequest,
    RecordScalingActionReply, RecordScalingActionRequest, ReportScalingStatusReply,
    ReportScalingStatusRequest, ScaleDecision, ScaleHistoryEntry, ScalePin, ScalingAppStatus,
    ScalingPauseInfo, ScalingPolicy, ScalingPolicyChange, ScalingPolicySet, ScalingProvenanceRow,
    ScalingTargetStatus, StandaloneScalingPolicy, WatchScalingPoliciesRequest,
};

/// The bundled autoscaler app's name; its job is `legion-system/autoscaler`.
pub(crate) const AUTOSCALER_APP: &str = "autoscaler";

/// How often a `WatchScalingPolicies` stream checks for changes.
const POLICY_POLL: Duration = Duration::from_secs(1);

/// Most history rows one `GetScaleHistory` returns.
const HISTORY_LIMIT_MAX: u32 = 1000;

pub(super) fn now_unix_ms() -> i64 {
    let now = OffsetDateTime::now_utc();
    now.unix_timestamp() * 1000 + i64::from(now.millisecond())
}

fn clock(at_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at_ms)
        .map(|at| at.format("%H:%M UTC").to_string())
        .unwrap_or_else(|| at_ms.to_string())
}

/// A job's ACL scope, with its template provenance (the same as
/// `ManagerDaemonService::job_read_scope`, for code without `self`).
fn job_scope(store: &Store, job_id: &str) -> AclScope {
    let provenance = store
        .job_template_provenance(job_id)
        .unwrap_or_default()
        .map(|provenance| provenance.name);
    AclScope::job(job_id).with_template(provenance)
}

pub(super) fn pin_proto(pin: &TaskScalePin) -> ScalePin {
    ScalePin {
        until_unix_ms: pin.until_ms,
        min: pin.min,
        max: pin.max,
        by: pin.by.clone(),
        reason: pin.reason.clone(),
    }
}

pub(super) fn pause_proto(pause: &PauseRow) -> ScalingPauseInfo {
    ScalingPauseInfo {
        scope: pause.scope().to_string(),
        until_unix_ms: pause.until_ms,
        by: pause.principal.clone(),
        reason: pause.reason.clone(),
        since_unix_ms: pause.created_ms,
    }
}

fn history_proto(row: &HistoryRow, policy_json: Option<String>, detail: bool) -> ScaleHistoryEntry {
    let policy = policy_of_target_key(&row.job_id).map(str::to_string);
    ScaleHistoryEntry {
        id: row.id.max(0) as u64,
        job_id: if policy.is_some() {
            String::new()
        } else {
            row.job_id.clone()
        },
        policy: policy.unwrap_or_default(),
        task_group: row.task_group.clone(),
        task_name: row.task_name.clone(),
        at_unix_ms: row.at_ms,
        from: row.from,
        to: row.to,
        source: row.source.clone(),
        principal: row.principal.clone(),
        summary: row.summary.clone(),
        applied: row.applied,
        kind: row.kind.clone(),
        policy_digest: row.policy_digest.clone(),
        decision_json: if detail {
            row.decision.clone()
        } else {
            String::new()
        },
        policy_json: policy_json.unwrap_or_default(),
        decision_id: row.decision_id.clone(),
    }
}

fn provenance_proto(resolved: &ResolvedPolicy) -> Vec<ScalingProvenanceRow> {
    resolved
        .explain_rows()
        .into_iter()
        .map(|row| ScalingProvenanceRow {
            field: row.field,
            value: row.value,
            origin: row.origin.to_string(),
        })
        .collect()
}

/// Every autoscaling target of every running job, resolved against the
/// current layers.
pub(super) fn load_targets(store: &Store) -> R<(AutoscaleLayers, Vec<TargetPolicy>)> {
    let layers = AutoscaleLayers::load(store)?;
    let mut targets = Vec::new();
    for (job_id, _) in store.autoscaled_job_configs()? {
        if let Some(config) = store.get_job_config_optional(&job_id)? {
            targets.extend(targets_for_config(&config, &layers));
        }
    }
    Ok((layers, targets))
}

fn policy_proto(target: &TargetPolicy) -> ScalingPolicy {
    let (state, error, digest, policy_json, summary) = match &target.resolved {
        Ok(resolved) => (
            "ok".to_string(),
            String::new(),
            resolved.digest.clone(),
            resolved.policy.canonical_json(),
            summary_line(&resolved.policy),
        ),
        Err(err) => (
            "invalid".to_string(),
            err.clone(),
            String::new(),
            String::new(),
            format!("invalid: {err}"),
        ),
    };
    ScalingPolicy {
        job_id: target.job_id.clone(),
        task_group: target.task_group.clone(),
        task_name: target.task_name.clone(),
        members: target.members.clone(),
        count: target.count,
        state,
        error,
        digest,
        policy_json,
        summary,
        runtime: target.runtime.clone(),
    }
}

/// Verify a decision's policy hashes to its digest, so a history digest
/// always names the policy it claims.
fn check_decision(decision: &ScaleDecision) -> Result<(), Status> {
    if decision.policy_json.is_empty() {
        return Ok(());
    }
    let policy: EffectivePolicy = serde_json::from_str(&decision.policy_json)
        .map_err(|err| Status::invalid_argument(format!("decision.policy_json: {err}")))?;
    let digest = policy.digest();
    if digest != decision.policy_digest {
        return Err(Status::invalid_argument(format!(
            "decision.policy_digest {} does not match policy_json (which hashes to {digest})",
            decision.policy_digest
        )));
    }
    Ok(())
}

fn new_history(decision: Option<&ScaleDecision>) -> NewHistory {
    decision
        .map(|decision| NewHistory {
            summary: decision.summary.clone(),
            decision_id: decision.id.clone(),
            decision_json: decision.detail_json.clone(),
            policy_digest: decision.policy_digest.clone(),
            policy_json: decision.policy_json.clone(),
        })
        .unwrap_or_default()
}

/// The target a scale request names: the task, or, when its group has an
/// autoscaled task, that task (the owner) with every task of the group.
fn resolve_scale_target<'a>(
    config: &'a JobConfig,
    job_id: &str,
    group: Option<&str>,
    task: Option<&str>,
) -> Result<(&'a TaskConfig, Vec<String>), Status> {
    let group_filter = group.filter(|group| !group.trim().is_empty());
    let task_filter = task.filter(|task| !task.trim().is_empty());
    let matching: Vec<&TaskConfig> = config
        .tasks
        .iter()
        .filter(|task| group_filter.is_none_or(|group| task.group == group))
        .filter(|task| task_filter.is_none_or(|name| task.name == name))
        .collect();
    let groups: BTreeSet<&str> = matching.iter().map(|task| task.group.as_str()).collect();
    if groups.len() == 1 {
        let group = *groups.iter().next().expect("one group");
        if let Some(owner) = config
            .tasks
            .iter()
            .find(|task| task.group == group && is_autoscaled(task))
        {
            return Ok((
                owner,
                crate::manager::autoscale::group_members(config, owner),
            ));
        }
    }
    let task = resolve_scalable_task(config, job_id, group, task)?;
    Ok((task, vec![task.name.clone()]))
}

impl ManagerDaemonService {
    /// Whether `identity` is the bundled autoscaler app's own workload
    /// identity: a credential whose job grant belongs to an allocation of
    /// `legion-system/autoscaler` (a bearer `legion` token, or an ACL token
    /// minted from one with `legion login --method legion`, both carry that
    /// reference). Management tokens and ACL-disabled clusters count too.
    pub(super) fn is_autoscaler_identity(&self, identity: &AclIdentity) -> bool {
        if identity.is_management {
            return true;
        }
        let app_job = crate::structs::app::app_job_id(AUTOSCALER_APP);
        identity.policies.iter().any(|policy| {
            crate::structs::grant::parse_grant_policy_ref(policy).is_some_and(|allocation| {
                self.store
                    .allocation_job_id(allocation)
                    .ok()
                    .flatten()
                    .is_some_and(|job| job == app_job)
            })
        })
    }

    /// The app-plane gate (`// ACL: app-plane`): only the autoscaler's own
    /// identity, or a management token, may call.
    fn enforce_app_plane<T>(&self, request: &Request<T>) -> Result<AclIdentity, Status> {
        let identity = self.authenticate_acl(request)?;
        if self.is_autoscaler_identity(&identity) {
            return Ok(identity);
        }
        warn!(
            target: "acl_audit",
            principal = %identity.principal,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            source = identity.source.as_str(),
            resource = "app-plane",
            action = "call",
            enforced = !identity.audit_only,
            "ACL denial"
        );
        if identity.audit_only {
            return Ok(identity);
        }
        if identity.source == crate::structs::acl::AclSource::Anonymous {
            return Err(Status::unauthenticated(
                "app-plane endpoints require the autoscaler app's own workload identity",
            ));
        }
        Err(Status::permission_denied(
            "only the bundled autoscaler app (legion-system/autoscaler) may report scaling \
             status or record scaling actions",
        ))
    }

    /// The `autoscale` rules that need the cluster's scaling defaults and
    /// namespace policies (`RunJob`, `PlanJob`): each autoscaled task's
    /// resolution, with its warnings and human summary.
    pub(super) fn validate_autoscale(
        &self,
        config: &JobConfig,
    ) -> Result<Vec<crate::manager::autoscale::ResolvedTask>, Status> {
        if !config.tasks.iter().any(is_autoscaled) {
            return Ok(Vec::new());
        }
        let layers = AutoscaleLayers::load(&self.store)
            .map_err(|e| map_store_error("Error loading scaling defaults", e))?;
        crate::manager::autoscale::validate_resolved(config, &layers).map_err(|err| {
            Status::invalid_argument(format!("Error validating task configs: {err}"))
        })
    }

    /// For a resubmit that changed only `autoscale` and autoscaled tasks'
    /// `count` (`in_place_change`), move the running allocations to the new
    /// version instead of rolling out (plan §6.3). Only when no rollout is in
    /// flight and every allocation runs the previous version; otherwise
    /// `false`, and the update rolls out as usual.
    pub(super) fn update_autoscale_in_place(
        &self,
        config: &JobConfig,
        job_config_id: &str,
    ) -> Result<bool, Status> {
        if !config.tasks.iter().any(is_autoscaled) {
            return Ok(false);
        }
        let Some(previous_id) = self
            .store
            .previous_job_config_id(job_config_id)
            .map_err(|e| map_store_error("Error loading previous job config", e))?
        else {
            return Ok(false);
        };
        let Some(previous) = self
            .store
            .get_job_config_by_id_optional(&previous_id)
            .map_err(|e| map_store_error("Error loading previous job config", e))?
        else {
            return Ok(false);
        };
        // Compare the stored versions: the store normalizes what it keeps
        // (resource defaults, say), so the request's config is not what the
        // previous version is stored as.
        let Some(next) = self
            .store
            .get_job_config_by_id_optional(job_config_id)
            .map_err(|e| map_store_error("Error loading job config", e))?
        else {
            return Ok(false);
        };
        if !crate::manager::autoscale::in_place_change(&previous, &next) {
            return Ok(false);
        }
        let rollouts = self
            .store
            .list_rollouts(Some(&config.name), true)
            .map_err(|e| map_store_error("Error checking rollouts", e))?;
        if !rollouts.is_empty() {
            return Ok(false);
        }
        let allocations = self
            .store
            .allocs_for_job(&config.name)
            .map_err(|e| map_store_error("Error loading job allocations", e))?;
        if allocations
            .iter()
            .any(|allocation| allocation.job_config_id != previous_id)
        {
            return Ok(false);
        }
        let moved = self
            .store
            .migrate_allocations_to_job_config(&previous_id, job_config_id)
            .map_err(|e| map_store_error("Error updating allocations in place", e))?;
        info!(
            job_id = %config.name,
            job_config_id,
            allocations = moved.len(),
            "Autoscale-only update applied in place"
        );
        Ok(true)
    }

    /// The autoscaled tasks a namespace policy's `autoscale` (as applied,
    /// replacing any stored policy of the same name) would leave invalid.
    pub(super) fn autoscale_policy_violations(
        &self,
        policy: &crate::structs::namespace_policy::NamespacePolicy,
    ) -> Result<Vec<crate::structs::NamespacePolicyViolation>, Status> {
        let (layers, targets) = load_targets(&self.store)
            .map_err(|e| map_store_error("Error loading scaling policies", e))?;
        let mut next = layers.clone();
        next.policies
            .retain(|existing| existing.name != policy.name);
        if policy.autoscale.is_some() {
            next.policies.push(policy.clone());
        }
        let mut violations = Vec::new();
        for target in targets
            .iter()
            .filter(|target| target.resolved.is_ok())
            .filter(|target| {
                policy.matches(crate::structs::namespace::job_namespace(&target.job_id))
            })
        {
            let Some(config) = self
                .store
                .get_job_config_optional(&target.job_id)
                .map_err(|e| map_store_error("Error loading job config", e))?
            else {
                continue;
            };
            let Some(task) = config
                .tasks
                .iter()
                .find(|task| task.group == target.task_group && task.name == target.task_name)
            else {
                continue;
            };
            if let Err(err) = next.resolve(&target.job_id, task) {
                violations.push(crate::structs::NamespacePolicyViolation {
                    namespace: crate::structs::namespace::job_namespace(&target.job_id).to_string(),
                    job_id: target.job_id.clone(),
                    rule: "autoscale".to_string(),
                    message: err,
                });
            }
        }
        Ok(violations)
    }

    // ACL: scale:write on the job (resize rights without redeploy rights).
    #[instrument(skip_all, fields(job = %request.get_ref().name))]
    pub(super) async fn scale_task_rpc(
        &self,
        request: Request<ScaleTaskRequest>,
    ) -> Result<Response<ScaleTaskReply>, Status> {
        let ident = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Write,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &ident,
            "scale_task",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(ScaleTaskReply {
                message,
                deduplicated: true,
                ..Default::default()
            }));
        }

        let pinning = inner.pin_for_seconds.is_some();
        let bounds = inner.min.is_some() || inner.max.is_some();
        if inner.count.is_none() && !inner.reset && !inner.release && !bounds {
            return Err(Status::invalid_argument(
                "provide count (the desired replicas), min/max with pin_for_seconds (a pinned \
                 floor or ceiling), release (end a pin), or reset (return the count to the spec)",
            ));
        }
        if inner.reset && (inner.count.is_some() || bounds || inner.release || pinning) {
            return Err(Status::invalid_argument(
                "reset cannot be combined with count, min, max, release, or pin_for_seconds",
            ));
        }
        if inner.release && (inner.count.is_some() || bounds || pinning) {
            return Err(Status::invalid_argument(
                "release cannot be combined with count, min, max, or pin_for_seconds",
            ));
        }
        if bounds && !pinning {
            return Err(Status::invalid_argument(
                "min and max are pins: give them pin_for_seconds (`--for 1h`)",
            ));
        }
        if let (Some(min), Some(max)) = (inner.min, inner.max) {
            if min > max {
                return Err(Status::invalid_argument(format!(
                    "min {min} is above max {max}"
                )));
            }
        }
        let source = match inner.source.trim() {
            "" | "operator" => "operator",
            "autoscaler" | "wake" if self.is_autoscaler_identity(&ident) => inner.source.trim(),
            // A claim the caller cannot make: recorded as theirs.
            "autoscaler" | "wake" => "operator",
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown source '{other}': use operator, autoscaler, or wake"
                )))
            }
        }
        .to_string();
        let autoscaler = source != "operator";
        if let Some(decision) = inner.decision.as_ref() {
            check_decision(decision)?;
        }

        let name = inner.name.clone();
        let config = keyed_job_config(&self.store, &name)?;
        let (task, members) = resolve_scale_target(
            &config,
            &name,
            inner.task_group.as_deref(),
            inner.task_name.as_deref(),
        )?;
        let autoscaled = is_autoscaled(task);
        let (task_group, task_name, spec_count) =
            (task.group.clone(), task.name.clone(), task.count.num);
        let label = target_label(&name, &task_group, &task_name);
        let job_config_id = config.id();
        let reason = inner.reason.clone().unwrap_or_default();
        let now_ms = now_unix_ms();
        let previous = self
            .store
            .effective_task_count(&name, &task_group, &task_name, spec_count)
            .map_err(|e| map_store_error("Error reading scale state", e))?;

        if pinning && !autoscaled {
            return Err(Status::failed_precondition(format!(
                "{label} has no autoscale, so there is nothing to pin against: its count is \
                 yours. Hint: drop --for (and --min/--max) to set the count"
            )));
        }
        if inner.count.is_some() && autoscaled && !pinning && !autoscaler {
            return Err(Status::failed_precondition(format!(
                "{label} is autoscaled, so its count belongs to the policy; a manual count is a \
                 time-boxed pin. Hint: add --for, as in `legion job scale {name} --count {} --for \
                 1h`, or remove autoscale from the job to own the count by hand",
                inner.count.unwrap_or_default()
            )));
        }

        let op = if inner.reset {
            ScaleWriteOp::Reset
        } else if inner.release {
            ScaleWriteOp::Release
        } else if let Some(seconds) = inner.pin_for_seconds {
            if seconds == 0 {
                return Err(Status::invalid_argument("pin_for_seconds must be above 0"));
            }
            let until_ms = now_ms
                .saturating_add(i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX));
            match inner.count {
                Some(count) => ScaleWriteOp::Pin {
                    count,
                    min: Some(count),
                    max: Some(count),
                    until_ms,
                },
                None => ScaleWriteOp::Pin {
                    count: previous,
                    min: inner.min,
                    max: inner.max,
                    until_ms,
                },
            }
        } else {
            ScaleWriteOp::Set {
                count: inner.count.unwrap_or(previous),
            }
        };

        // Scale-out is new demand: a namespace policy's quota sees the job
        // at its new count (every member of a group target). Scale-in is
        // never refused.
        let target_count = match &op {
            ScaleWriteOp::Set { count } => Some(*count),
            ScaleWriteOp::Pin {
                count, min, max, ..
            } => Some((*count).max(min.unwrap_or(0)).min(max.unwrap_or(u32::MAX))),
            _ => None,
        };
        if let Some(count) = target_count.filter(|count| *count > previous) {
            let mut scaled = config.clone();
            for task in scaled.tasks.iter_mut() {
                if task.group == task_group && members.contains(&task.name) {
                    task.count.num = count;
                }
            }
            self.store
                .check_namespace_admission(&scaled)
                .map_err(|e| map_store_error("Namespace policy refused the scale", e))?;
        }

        // The ECS rule (docs/autoscaling.md): scale-out applies during a
        // rollout, scale-in defers until it finishes.
        let active_rollout = self
            .store
            .list_rollouts(Some(&name), true)
            .map_err(|e| map_store_error("Error checking rollouts", e))?
            .into_iter()
            .next();
        let op = match (op, active_rollout.as_ref()) {
            (ScaleWriteOp::Set { count }, Some(_)) if count < previous => {
                ScaleWriteOp::Pending { count }
            }
            (op, _) => op,
        };

        let write = ScaleWrite {
            job_id: name.clone(),
            task_group: task_group.clone(),
            task_name: task_name.clone(),
            members: members.clone(),
            op: op.clone(),
            base_count: spec_count,
            source: source.clone(),
            reason: reason.clone(),
            principal: ident.principal.clone(),
            expected_version: inner.expected_version,
            autoscaler,
            from: previous,
            history: new_history(inner.decision.as_ref()),
            now_ms,
        };
        let outcome = self
            .store
            .apply_scale_write(write)
            .map_err(|e| map_store_error("Error applying scale", e))?;

        let summary = inner
            .decision
            .as_ref()
            .map(|decision| decision.summary.clone())
            .filter(|summary| !summary.is_empty())
            .unwrap_or_else(|| reason.clone());
        let mut deferred = false;
        let mut pinned_until = None;
        let events = self.store.events();
        let message = match &op {
            ScaleWriteOp::Reset => {
                events.scale_reset(&name, &task_group, &task_name, "explicit reset");
                format!(
                    "{label}: {previous} -> {spec_count} (override dropped; spec owns the count)"
                )
            }
            ScaleWriteOp::Release => {
                events.scale_pinned(
                    &name,
                    &task_group,
                    &task_name,
                    None,
                    None,
                    None,
                    &ident.principal,
                    &reason,
                );
                format!("{label}: pin released at {previous}; autoscaling resumes")
            }
            ScaleWriteOp::Pending { count } => {
                deferred = true;
                let rollout_id = active_rollout.as_ref().map_or(0, |rollout| rollout.id);
                events.scale_deferred(
                    &name,
                    &task_group,
                    &task_name,
                    *count,
                    rollout_id,
                    &source,
                    &summary,
                );
                format!(
                    "deferred: rollout {rollout_id} is in progress; scale-in to {count} applies \
                     when it completes"
                )
            }
            ScaleWriteOp::Set { count } => {
                events.scale_applied(
                    &name,
                    &task_group,
                    &task_name,
                    Some(previous),
                    *count,
                    &source,
                    &summary,
                );
                format!("{label}: {previous} -> {count} (spec count {spec_count}; override active)")
            }
            ScaleWriteOp::Pin {
                min, max, until_ms, ..
            } => {
                pinned_until = Some(*until_ms);
                let count = outcome.count.unwrap_or(previous);
                events.scale_pinned(
                    &name,
                    &task_group,
                    &task_name,
                    *min,
                    *max,
                    Some(*until_ms),
                    &ident.principal,
                    &reason,
                );
                if count != previous {
                    events.scale_applied(
                        &name,
                        &task_group,
                        &task_name,
                        Some(previous),
                        count,
                        &source,
                        &reason,
                    );
                }
                let what = match (min, max) {
                    (Some(min), Some(max)) if min == max => "autoscaling pinned".to_string(),
                    (Some(min), None) => format!("floor pinned at {min}"),
                    (None, Some(max)) => format!("ceiling pinned at {max}"),
                    (min, max) => format!(
                        "pinned to {}..{}",
                        min.unwrap_or_default(),
                        max.unwrap_or_default()
                    ),
                };
                format!(
                    "{label}: {previous} -> {count}; {what} until {} (legion job scale {name} \
                     --release)",
                    clock(*until_ms)
                )
            }
        };
        let changes_count = match &op {
            ScaleWriteOp::Reset => previous != spec_count,
            ScaleWriteOp::Set { count } => *count != previous,
            ScaleWriteOp::Pin { .. } => outcome.count.is_some_and(|count| count != previous),
            _ => false,
        };
        if changes_count {
            self.store
                .enqueue_eval(
                    "job_scale",
                    Some(&name),
                    Some(&job_config_id),
                    None,
                    Some(self.store.default_eval_priority),
                )
                .map_err(|e| map_store_error("Error enqueueing scale eval", e))?;
        }

        self.store
            .idempotency_record(&idempotency_key, "scale_task", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(ScaleTaskReply {
            message,
            deduplicated: false,
            deferred,
            version: outcome.version,
            pinned_until_unix_ms: pinned_until,
        }))
    }

    // ACL: scale:read, coarse before anything is read; jobs outside the
    // caller's grant are filtered out of every message.
    pub(super) async fn watch_scaling_policies_rpc(
        &self,
        request: Request<WatchScalingPoliciesRequest>,
    ) -> Result<Response<ReceiverStream<Result<ScalingPolicySet, Status>>>, Status> {
        self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            AclScope::any(),
        )?;
        let credential = self.stream_credential(&request);
        let req = request.into_inner();
        let namespaces = if req.namespaces.is_empty() {
            None
        } else {
            Some(
                crate::structs::namespace::NamespaceSelection::parse(&req.namespaces)
                    .map_err(Status::invalid_argument)?,
            )
        };
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut identity = match credential.reauthenticate(&store) {
                Ok(identity) => identity,
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            };
            let mut reauth = tokio::time::interval(credential.interval);
            reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            reauth.tick().await;
            let mut poll = tokio::time::interval(POLICY_POLL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut fingerprint: Option<String> = None;
            let mut last_sent: Option<(Vec<ScalingPolicy>, Vec<StandaloneScalingPolicy>)> = None;
            let mut generation = 0u64;
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    _ = reauth.tick() => {
                        match credential.reauthenticate(&store) {
                            Ok(fresh) => identity = fresh,
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                return;
                            }
                        }
                        continue;
                    }
                    _ = poll.tick() => {}
                }
                // A cheap fingerprint first: the autoscaled jobs' latest
                // configs, the defaults' version, the namespace policies.
                let probe = (|| -> R<String> {
                    let configs = store.autoscaled_job_configs()?;
                    let defaults = store.scaling_defaults()?.map_or(0, |row| row.version);
                    let policies = store.namespace_policy_fingerprint()?;
                    let standalone = store.scaling_policy_fingerprint()?;
                    let pauses = store.scaling_pause_fingerprint()?;
                    let plugins = store
                        .get_app(crate::manager::autoscale::AUTOSCALER_APP)?
                        .map(|record| record.updated_at)
                        .unwrap_or_default();
                    Ok(format!(
                        "{configs:?}|{defaults}|{policies:?}|{standalone:?}|{pauses:?}|{plugins}"
                    ))
                })();
                let probe = match probe {
                    Ok(probe) => probe,
                    Err(err) => {
                        debug!("WatchScalingPolicies: reading the policy fingerprint: {err:#}");
                        continue;
                    }
                };
                if fingerprint.as_deref() == Some(probe.as_str()) && last_sent.is_some() {
                    continue;
                }
                let (layers, targets) = match load_targets(&store) {
                    Ok(loaded) => loaded,
                    Err(err) => {
                        warn!("WatchScalingPolicies: resolving policies: {err:#}");
                        continue;
                    }
                };
                fingerprint = Some(probe);
                let policies: Vec<ScalingPolicy> = targets
                    .iter()
                    .filter(|target| {
                        namespaces.as_ref().is_none_or(|selection| {
                            selection
                                .matches(crate::structs::namespace::job_namespace(&target.job_id))
                        })
                    })
                    .filter(|target| {
                        identity.allows(
                            AclResource::Scale,
                            AclAction::Read,
                            &job_scope(&store, &target.job_id),
                        )
                    })
                    .map(policy_proto)
                    .collect();
                let pauses = store.scaling_pauses().unwrap_or_default();
                let now_ms = now_unix_ms();
                let standalone: Vec<StandaloneScalingPolicy> =
                    match super::scaling_policy_rpcs::load_policies(&store, &layers) {
                        Ok(loaded) => loaded
                            .iter()
                            .filter(|policy| {
                                namespaces.as_ref().is_none_or(|selection| {
                                    selection.matches(&policy.row.namespace)
                                })
                            })
                            .filter(|policy| {
                                identity.allows(
                                    AclResource::Scale,
                                    AclAction::Read,
                                    &AclScope::job(&policy.id()),
                                )
                            })
                            .map(|policy| {
                                super::scaling_policy_rpcs::standalone_proto(
                                    policy, &pauses, now_ms,
                                )
                            })
                            .collect(),
                        Err(err) => {
                            warn!("WatchScalingPolicies: resolving standalone policies: {err:#}");
                            continue;
                        }
                    };
                let sent = (policies, standalone);
                if last_sent.as_ref() == Some(&sent) {
                    continue;
                }
                generation += 1;
                let set = ScalingPolicySet {
                    policies: sent.0.clone(),
                    generation,
                    resolved_at_unix_ms: now_ms,
                    defaults_version: layers.cluster_version,
                    standalone: sent.1.clone(),
                };
                if tx.send(Ok(set)).await.is_err() {
                    return;
                }
                last_sent = Some(sent);
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ACL: operator:write (cluster-wide defaults).
    pub(super) async fn apply_scaling_defaults_rpc(
        &self,
        request: Request<ApplyScalingDefaultsRequest>,
    ) -> Result<Response<ApplyScalingDefaultsReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let inner = request.into_inner();
        let raw = if inner.defaults_json.trim().is_empty() {
            "{}"
        } else {
            inner.defaults_json.trim()
        };
        let record: AutoscaleRecord = serde_json::from_str(raw)
            .map_err(|err| Status::invalid_argument(format!("scaling defaults: {err}")))?;
        record
            .validate_inherited("cluster defaults")
            .map_err(Status::invalid_argument)?;
        let canonical = serde_json::to_string(&record)
            .map_err(|err| Status::internal(format!("encoding the defaults: {err}")))?;

        let idempotency_key = if inner.dry_run {
            String::new()
        } else {
            scoped_idempotency_key(
                &identity,
                "apply_scaling_defaults",
                inner.idempotency_key.as_deref(),
                &inner,
            )
        };
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let version = self
                .store
                .scaling_defaults()
                .map_err(|e| map_store_error("Error reading scaling defaults", e))?
                .map_or(0, |row| row.version);
            return Ok(Response::new(ApplyScalingDefaultsReply {
                message,
                applied: true,
                version,
                deduplicated: true,
                ..Default::default()
            }));
        }

        let (layers, targets) = load_targets(&self.store)
            .map_err(|e| map_store_error("Error loading scaling policies", e))?;
        let mut next = layers.clone();
        next.cluster = (record != AutoscaleRecord::default()).then_some(record.clone());
        let mut changes = Vec::new();
        for target in &targets {
            let Some(config) = self
                .store
                .get_job_config_optional(&target.job_id)
                .map_err(|e| map_store_error("Error loading job config", e))?
            else {
                continue;
            };
            let Some(task) = config
                .tasks
                .iter()
                .find(|task| task.group == target.task_group && task.name == target.task_name)
            else {
                continue;
            };
            let after = next.resolve(&target.job_id, task);
            let before_digest = target
                .resolved
                .as_ref()
                .map(|resolved| resolved.digest.clone())
                .unwrap_or_default();
            let (after_digest, error, fields) = match &after {
                Ok(Some(resolved)) => {
                    let fields = match &target.resolved {
                        Ok(before) => changed_fields(before, resolved),
                        Err(_) => Vec::new(),
                    };
                    (resolved.digest.clone(), String::new(), fields)
                }
                Ok(None) => continue,
                Err(err) => (String::new(), err.clone(), Vec::new()),
            };
            if before_digest != after_digest || !error.is_empty() {
                changes.push(ScalingPolicyChange {
                    target: target.label(),
                    before_digest,
                    after_digest,
                    error,
                    fields,
                });
            }
        }
        // Standalone policies inherit the defaults too.
        let standalone = super::scaling_policy_rpcs::load_policies(&self.store, &layers)
            .map_err(|e| map_store_error("Error loading scaling policies", e))?;
        for policy in &standalone {
            let Ok(crate::autoscaler::policy::StandaloneSpec::External(spec)) = &policy.spec else {
                continue;
            };
            let after = next.resolve_standalone(spec);
            let before_digest = policy
                .resolved
                .as_ref()
                .map(|resolved| resolved.digest.clone())
                .unwrap_or_default();
            let (after_digest, error, fields) = match &after {
                Ok(resolved) => (
                    resolved.digest.clone(),
                    String::new(),
                    match &policy.resolved {
                        Ok(before) => changed_fields(before, resolved),
                        Err(_) => Vec::new(),
                    },
                ),
                Err(err) => (String::new(), err.clone(), Vec::new()),
            };
            if before_digest != after_digest || !error.is_empty() {
                changes.push(ScalingPolicyChange {
                    target: policy.id(),
                    before_digest,
                    after_digest,
                    error,
                    fields,
                });
            }
        }
        let invalid = changes
            .iter()
            .filter(|change| !change.error.is_empty())
            .count() as u32;
        let current = self
            .store
            .scaling_defaults()
            .map_err(|e| map_store_error("Error reading scaling defaults", e))?;
        let current_version = current.as_ref().map_or(0, |row| row.version);
        let unchanged = current
            .as_ref()
            .map(|row| row.record.as_str())
            .unwrap_or("{}")
            == canonical;
        let summary = |verb: &str, version: u64| {
            format!(
                "scaling defaults: {verb}{}; {} polic{} change, {invalid} invalid",
                if version > 0 {
                    format!(" (v{version})")
                } else {
                    String::new()
                },
                changes.len(),
                if changes.len() == 1 {
                    "y would"
                } else {
                    "ies would"
                },
            )
        };
        if inner.dry_run {
            return Ok(Response::new(ApplyScalingDefaultsReply {
                message: summary("dry run, nothing written", current_version),
                applied: false,
                version: current_version,
                changes,
                invalid,
                deduplicated: false,
            }));
        }
        if invalid > 0 && !inner.force {
            let listed: Vec<String> = changes
                .iter()
                .filter(|change| !change.error.is_empty())
                .take(10)
                .map(|change| format!("  {}", change.error))
                .collect();
            return Err(Status::failed_precondition(format!(
                "scaling defaults: the change would leave {invalid} autoscaled task(s) with an \
                 invalid policy:\n{}\nHint: fix those tasks first, or apply with --force to hold \
                 them `invalid` until fixed",
                listed.join("\n")
            )));
        }
        if unchanged {
            return Ok(Response::new(ApplyScalingDefaultsReply {
                message: format!("scaling defaults: unchanged (v{current_version})"),
                applied: false,
                version: current_version,
                changes: Vec::new(),
                invalid: 0,
                deduplicated: false,
            }));
        }
        let version = self
            .store
            .put_scaling_defaults(&canonical, &identity.principal)
            .map_err(|e| map_store_error("Error writing scaling defaults", e))?;
        let message = summary("applied", version).replace("would change", "changed");
        info!(
            version,
            changed = changes.len(),
            invalid,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Scaling defaults applied"
        );
        self.store
            .idempotency_record(&idempotency_key, "apply_scaling_defaults", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(ApplyScalingDefaultsReply {
            message,
            applied: true,
            version,
            changes,
            invalid,
            deduplicated: false,
        }))
    }

    // ACL: scale:read on the namespace.
    pub(super) async fn get_scaling_defaults_rpc(
        &self,
        request: Request<GetScalingDefaultsRequest>,
    ) -> Result<Response<GetScalingDefaultsReply>, Status> {
        let namespace = match request.get_ref().namespace.trim() {
            "" => crate::structs::namespace::DEFAULT_NAMESPACE.to_string(),
            other => other.to_string(),
        };
        self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            AclScope::namespace(&namespace),
        )?;
        let row = self
            .store
            .scaling_defaults()
            .map_err(|e| map_store_error("Error reading scaling defaults", e))?;
        let layers = AutoscaleLayers::load(&self.store)
            .map_err(|e| map_store_error("Error loading scaling defaults", e))?;
        let (effective, error) = match layers.effective_defaults(&namespace) {
            Ok(defaults) => (
                defaults
                    .explain_rows()
                    .into_iter()
                    .map(|row| ScalingProvenanceRow {
                        field: row.field,
                        value: row.value,
                        origin: row.origin.to_string(),
                    })
                    .collect(),
                String::new(),
            ),
            Err(err) => (Vec::new(), err),
        };
        Ok(Response::new(GetScalingDefaultsReply {
            cluster_json: row
                .as_ref()
                .map_or_else(|| "{}".to_string(), |row| row.record.clone()),
            version: row.as_ref().map_or(0, |row| row.version),
            updated_by: row
                .as_ref()
                .map(|row| row.updated_by.clone())
                .unwrap_or_default(),
            updated_at: row
                .as_ref()
                .map(|row| row.updated_at.clone())
                .unwrap_or_default(),
            policies: layers.policy_names(&namespace),
            namespace,
            effective,
            error,
        }))
    }

    // ACL: app-plane (the autoscaler app's own identity, or management).
    #[instrument(skip_all, fields(targets = request.get_ref().targets.len()))]
    pub(super) async fn report_scaling_status_rpc(
        &self,
        request: Request<ReportScalingStatusRequest>,
    ) -> Result<Response<ReportScalingStatusReply>, Status> {
        self.enforce_app_plane(&request)?;
        // Held by the status-lane leader; others forward.
        self.store
            .require_lane_leader(crate::storage::Lane::Status, "ReportScalingStatus")
            .map_err(|e| map_store_error("Error recording scaling status", e))?;
        let mut report = request.into_inner();
        // A standalone policy's report lives under its policy key.
        for target in report.targets.iter_mut() {
            if !target.policy.is_empty() {
                target.job_id = policy_target_key(&target.policy);
                target.task_group.clear();
                target.task_name.clear();
            }
        }
        let accepted =
            self.store
                .scaling_status()
                .record(&report, std::time::Instant::now(), now_unix_ms());
        for target in &report.targets {
            for flag in &target.events {
                if target.policy.is_empty() {
                    self.store.events().scale_flag(
                        &flag.kind,
                        &target.job_id,
                        &target.task_group,
                        &target.task_name,
                        &flag.message,
                    );
                } else {
                    self.store.events().scale_policy_flag(
                        &flag.kind,
                        &target.policy,
                        &flag.message,
                    );
                }
            }
        }
        Ok(Response::new(ReportScalingStatusReply {
            accepted,
            served_by: self
                .store
                .raft_node_id()
                .unwrap_or_else(|| "local".to_string()),
        }))
    }

    // ACL: app-plane (the autoscaler app's own identity, or management).
    pub(super) async fn record_scaling_action_rpc(
        &self,
        request: Request<RecordScalingActionRequest>,
    ) -> Result<Response<RecordScalingActionReply>, Status> {
        let identity = self.enforce_app_plane(&request)?;
        let inner = request.into_inner();
        let policy = inner.policy.trim().to_string();
        match inner.kind.as_str() {
            "would" => {}
            "scale" | "failed" if !policy.is_empty() => {}
            "scale" | "failed" => {
                return Err(Status::invalid_argument(format!(
                    "a `{}` action names a standalone policy (`policy`); a task's count \
                     changes through ScaleTask",
                    inner.kind
                )))
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown action kind '{other}': use would (a dry-run decision), or scale or \
                     failed (a standalone policy's target)"
                )))
            }
        }
        let decision = inner.decision.unwrap_or_default();
        check_decision(&decision)?;
        let now_ms = now_unix_ms();
        let (job_id, task_group, task_name) = if policy.is_empty() {
            (
                inner.job_id.clone(),
                inner.task_group.clone(),
                inner.task_name.clone(),
            )
        } else {
            (policy_target_key(&policy), String::new(), String::new())
        };
        if inner.kind == "scale" {
            // A pause holds even against a misbehaving app.
            let pauses = self
                .store
                .scaling_pauses()
                .map_err(|e| map_store_error("Error loading pauses", e))?;
            if let Some(pause) = covering_pause(&pauses, &job_id, "", "", now_ms) {
                return Err(Status::failed_precondition(format!(
                    "scaling policy {policy} is paused ({} by {}): the autoscaler may not scale \
                     it. Hint: `legion scaling resume`",
                    pause.scope(),
                    pause.principal
                )));
            }
        }
        let (kind, applied) = match inner.kind.as_str() {
            "scale" => ("scaled", true),
            "failed" => ("failed", false),
            _ => ("would", false),
        };
        let history_id = self
            .store
            .record_scale_history_row(
                &job_id,
                &task_group,
                &task_name,
                now_ms,
                inner.from,
                inner.to,
                "autoscaler",
                &identity.principal,
                kind,
                applied,
                new_history(Some(&decision)),
            )
            .map_err(|e| map_store_error("Error recording scaling action", e))?;
        if kind == "scaled" {
            self.store.events().scale_policy_applied(
                &policy,
                inner.from,
                inner.to,
                &decision.summary,
            );
        }
        Ok(Response::new(RecordScalingActionReply {
            history_id: history_id.max(0) as u64,
        }))
    }

    // ACL: scale:read, coarse before any lookup, then per job.
    pub(super) async fn get_scaling_status_rpc(
        &self,
        request: Request<GetScalingStatusRequest>,
    ) -> Result<Response<GetScalingStatusReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        let job = req.job.trim().to_string();
        let policy = req.policy.trim().to_string();
        if !job.is_empty() && !policy.is_empty() {
            return Err(Status::invalid_argument(
                "name a job or a standalone policy, not both",
            ));
        }
        if !job.is_empty() {
            acl::enforce(
                &identity,
                AclResource::Scale,
                AclAction::Read,
                &self.job_read_scope(&job),
            )?;
        }
        if !policy.is_empty() {
            acl::enforce(
                &identity,
                AclResource::Scale,
                AclAction::Read,
                &AclScope::job(&policy),
            )?;
        }
        self.store
            .require_lane_leader(crate::storage::Lane::Status, "GetScalingStatus")
            .map_err(|e| map_store_error("Error reading scaling status", e))?;
        let namespaces = if job.is_empty() && policy.is_empty() {
            Some(
                crate::structs::namespace::NamespaceSelection::parse(&req.namespaces)
                    .map_err(Status::invalid_argument)?,
            )
        } else {
            None
        };
        let (layers, targets) = load_targets(&self.store)
            .map_err(|e| map_store_error("Error loading scaling policies", e))?;
        let pauses = self
            .store
            .scaling_pauses()
            .map_err(|e| map_store_error("Error loading pauses", e))?;
        let now = std::time::Instant::now();
        let now_ms = now_unix_ms();
        let narrow = |value: &Option<String>| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let (group, task) = (narrow(&req.task_group), narrow(&req.task_name));
        let mut out = Vec::new();
        for target in targets {
            if !policy.is_empty() || (!job.is_empty() && target.job_id != job) {
                continue;
            }
            if let Some(selection) = namespaces.as_ref() {
                if !selection.matches(crate::structs::namespace::job_namespace(&target.job_id)) {
                    continue;
                }
            }
            if group
                .as_ref()
                .is_some_and(|group| *group != target.task_group)
                || task
                    .as_ref()
                    .is_some_and(|task| *task != target.task_name && !target.members.contains(task))
            {
                continue;
            }
            if !identity.allows(
                AclResource::Scale,
                AclAction::Read,
                &self.job_read_scope(&target.job_id),
            ) {
                continue;
            }
            out.push(
                self.target_status(&target, &pauses, now, now_ms, req.detail)
                    .map_err(|e| map_store_error("Error reading scaling status", e))?,
            );
        }
        if job.is_empty() {
            let loaded = super::scaling_policy_rpcs::load_policies(&self.store, &layers)
                .map_err(|e| map_store_error("Error loading scaling policies", e))?;
            let mut found = false;
            for standalone in &loaded {
                let id = standalone.id();
                if !policy.is_empty() && id != policy {
                    continue;
                }
                if let Some(selection) = namespaces.as_ref() {
                    if !selection.matches(&standalone.row.namespace) {
                        continue;
                    }
                }
                if !identity.allows(AclResource::Scale, AclAction::Read, &AclScope::job(&id)) {
                    continue;
                }
                found = true;
                out.push(
                    self.standalone_status(standalone, &pauses, now, now_ms, req.detail)
                        .map_err(|e| map_store_error("Error reading scaling status", e))?,
                );
            }
            if !policy.is_empty() && !found {
                return Err(Status::not_found(format!(
                    "scaling policy {policy} not found. Hint: `legion scaling list` shows the \
                     policies"
                )));
            }
        }
        let app = self.app_status(&pauses, now, now_ms)?;
        Ok(Response::new(GetScalingStatusReply {
            app: Some(app),
            targets: out,
        }))
    }

    /// One standalone policy's status: what the app last reported for it,
    /// its bounds and mode, its pause, and its last history row.
    fn standalone_status(
        &self,
        policy: &super::scaling_policy_rpcs::LoadedPolicy,
        pauses: &[PauseRow],
        now: std::time::Instant,
        now_ms: i64,
        detail: bool,
    ) -> R<ScalingTargetStatus> {
        let id = policy.id();
        let key = policy_target_key(&id);
        let pause = covering_pause(pauses, &key, "", "", now_ms).map(pause_proto);
        let last_change = self
            .store
            .scale_history(&key, Some(""), Some(""), 1)?
            .first()
            .map(|row| history_proto(row, None, false));
        let board = self.store.scaling_status();
        let entry = board.target(&key, "", "");
        let (mode, min, max, digest, error, policy_summary) = match &policy.resolved {
            Ok(resolved) => (
                resolved.policy.mode.to_string(),
                resolved.policy.min,
                resolved.policy.max,
                resolved.digest.clone(),
                String::new(),
                summary_line(&resolved.policy),
            ),
            Err(err) => (
                String::new(),
                0,
                0,
                String::new(),
                err.clone(),
                format!("invalid: {err}"),
            ),
        };
        let (state, current, want, evaluated, summary, decision) = match (&policy.resolved, &entry)
        {
            (Err(_), _) => ("invalid".to_string(), 0, 0, 0, error.clone(), String::new()),
            (Ok(_), Some(entry)) if board.is_stale(entry.received, now) => (
                "stale".to_string(),
                entry.current,
                entry.current,
                entry.evaluated_at_unix_ms,
                entry.summary.clone(),
                entry.decision_json.clone(),
            ),
            (Ok(_), Some(entry)) => (
                entry.state.clone(),
                entry.current,
                entry.want,
                entry.evaluated_at_unix_ms,
                entry.summary.clone(),
                entry.decision_json.clone(),
            ),
            (Ok(_), None) => (
                "not-evaluated".to_string(),
                0,
                0,
                0,
                String::new(),
                String::new(),
            ),
        };
        let (policy_rows, policy_json) = match (&policy.resolved, detail) {
            (Ok(resolved), true) => (provenance_proto(resolved), resolved.policy.canonical_json()),
            _ => (Vec::new(), String::new()),
        };
        Ok(ScalingTargetStatus {
            mode,
            now: current,
            want,
            min,
            max,
            state,
            last_evaluation_unix_ms: evaluated,
            summary,
            last_change,
            pause,
            policy_digest: digest,
            error,
            decision_json: if detail { decision } else { String::new() },
            policy: policy_rows,
            policy_json,
            policy_summary,
            policy_id: id,
            kind: "external".to_string(),
            ..Default::default()
        })
    }

    fn target_status(
        &self,
        target: &TargetPolicy,
        pauses: &[PauseRow],
        now: std::time::Instant,
        now_ms: i64,
        detail: bool,
    ) -> R<ScalingTargetStatus> {
        let (job, group, task) = (&target.job_id, &target.task_group, &target.task_name);
        let effective = self
            .store
            .effective_task_count(job, group, task, target.count)?;
        let row: Option<TaskScale> = self.store.task_scale_for(job, group, task)?;
        let pin = row
            .as_ref()
            .and_then(|row| row.pin.as_ref())
            .filter(|pin| pin.active(now_ms))
            .map(pin_proto);
        let pause = covering_pause(pauses, job, group, task, now_ms).map(pause_proto);
        let last_change = self
            .store
            .scale_history(job, Some(group), Some(task), 1)?
            .first()
            .map(|row| history_proto(row, None, false));
        let board = self.store.scaling_status();
        let entry = board.target(job, group, task);
        let (mode, min, max, digest, error, policy_summary) = match &target.resolved {
            Ok(resolved) => (
                resolved.policy.mode.to_string(),
                resolved.policy.min,
                resolved.policy.max,
                resolved.digest.clone(),
                String::new(),
                summary_line(&resolved.policy),
            ),
            Err(err) => (
                String::new(),
                0,
                0,
                String::new(),
                err.clone(),
                format!("invalid: {err}"),
            ),
        };
        let (state, want, evaluated, summary, decision) = match (&target.resolved, &entry) {
            (Err(_), _) => (
                "invalid".to_string(),
                effective,
                0,
                error.clone(),
                String::new(),
            ),
            (Ok(_), Some(entry)) if board.is_stale(entry.received, now) => (
                "stale".to_string(),
                effective,
                entry.evaluated_at_unix_ms,
                entry.summary.clone(),
                entry.decision_json.clone(),
            ),
            (Ok(_), Some(entry)) => (
                entry.state.clone(),
                entry.want,
                entry.evaluated_at_unix_ms,
                entry.summary.clone(),
                entry.decision_json.clone(),
            ),
            (Ok(_), None) => (
                "not-evaluated".to_string(),
                effective,
                0,
                String::new(),
                String::new(),
            ),
        };
        let (policy_rows, policy_json) = match (&target.resolved, detail) {
            (Ok(resolved), true) => (provenance_proto(resolved), resolved.policy.canonical_json()),
            _ => (Vec::new(), String::new()),
        };
        Ok(ScalingTargetStatus {
            job_id: job.clone(),
            task_group: group.clone(),
            task_name: task.clone(),
            members: target.members.clone(),
            mode,
            now: effective,
            want,
            min,
            max,
            state,
            last_evaluation_unix_ms: evaluated,
            summary,
            last_change,
            pin,
            pause,
            policy_digest: digest,
            error,
            decision_json: if detail { decision } else { String::new() },
            policy: policy_rows,
            policy_json,
            version: row.as_ref().map_or(0, |row| row.version),
            policy_summary,
            policy_id: String::new(),
            kind: if target.members.len() > 1 {
                "group".to_string()
            } else {
                "task".to_string()
            },
        })
    }

    fn app_status(
        &self,
        pauses: &[PauseRow],
        now: std::time::Instant,
        now_ms: i64,
    ) -> Result<ScalingAppStatus, Status> {
        let board = self.store.scaling_status();
        let held_since = self.store.scaling_rollups().merge().responsible_since();
        let liveness = board.liveness(now, held_since);
        let report = board.app();
        let record = self
            .store
            .get_app(AUTOSCALER_APP)
            .map_err(|e| map_store_error("Error loading the autoscaler app", e))?;
        let enabled = record.as_ref().is_some_and(|record| record.enabled);
        let paused_all = pauses
            .iter()
            .find(|pause| pause.scope() == "all" && pause.active(now_ms))
            .map(pause_proto);
        let (state, message) = if record.is_some() && !enabled {
            (
                "disabled".to_string(),
                "disabled (legion app enable autoscaler turns it on)".to_string(),
            )
        } else {
            match liveness {
                AppLiveness::Running => {
                    let report = report.as_ref();
                    let ago = report.map_or(0, |report| {
                        now.saturating_duration_since(report.received).as_secs()
                    });
                    (
                        "running".to_string(),
                        format!(
                            "running ({}), last evaluation {ago}s ago{}",
                            report.map(|report| report.instance.as_str()).unwrap_or("-"),
                            report
                                .filter(|report| report.mode == "dry-run")
                                .map(|_| ", every policy forced to dry-run")
                                .unwrap_or_default()
                        ),
                    )
                }
                AppLiveness::Starting => (
                    "starting".to_string(),
                    "no report yet from the autoscaler (this manager took over recently)"
                        .to_string(),
                ),
                AppLiveness::NotRunning => {
                    let since = report
                        .as_ref()
                        .map(|report| format!(" since {}", clock(report.received_unix_ms)))
                        .unwrap_or_default();
                    let job = self.app_job_note();
                    (
                        "not-running".to_string(),
                        format!("NOT RUNNING{since}{job}: nothing is being autoscaled"),
                    )
                }
            }
        };
        Ok(ScalingAppStatus {
            state,
            message,
            last_report_unix_ms: report.as_ref().map_or(0, |report| report.received_unix_ms),
            interval_seconds: report
                .as_ref()
                .map_or(0, |report| report.interval.as_secs() as u32),
            instance: report
                .as_ref()
                .map(|report| report.instance.clone())
                .unwrap_or_default(),
            mode: report
                .as_ref()
                .map(|report| report.mode.clone())
                .unwrap_or_default(),
            served_by: self
                .store
                .raft_node_id()
                .unwrap_or_else(|| "local".to_string()),
            paused_all,
        })
    }

    /// ` (app job legion-system/autoscaler: <status>)`, best effort.
    fn app_job_note(&self) -> String {
        let job = crate::structs::app::app_job_id(AUTOSCALER_APP);
        match self.store.job_lifecycle_flag(&job) {
            Ok(Some(status)) => {
                let placed = self
                    .store
                    .scaling_replica_rows(&job)
                    .map(|rows| rows.len())
                    .unwrap_or(0);
                format!(" (app job {job}: {status}, {placed} allocation(s) placed)")
            }
            Ok(None) => format!(" (app job {job} does not exist)"),
            Err(_) => String::new(),
        }
    }

    // ACL: scale:read on the job, or on the standalone policy.
    pub(super) async fn get_scale_history_rpc(
        &self,
        request: Request<GetScaleHistoryRequest>,
    ) -> Result<Response<GetScaleHistoryReply>, Status> {
        let policy = request.get_ref().policy.trim().to_string();
        let scope = if policy.is_empty() {
            self.job_read_scope(&request.get_ref().job)
        } else {
            AclScope::job(&policy)
        };
        self.check_acl(&request, AclResource::Scale, AclAction::Read, scope)?;
        let mut req = request.into_inner();
        if !policy.is_empty() {
            req.job = policy_target_key(&policy);
            req.task_group = None;
            req.task_name = None;
        }
        let limit = match req.limit {
            0 => 50,
            n => n.min(HISTORY_LIMIT_MAX),
        } as usize;
        let narrow = |value: &Option<String>| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let rows = self
            .store
            .scale_history(
                req.job.trim(),
                narrow(&req.task_group).as_deref(),
                narrow(&req.task_name).as_deref(),
                limit,
            )
            .map_err(|e| map_store_error("Error reading scale history", e))?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            let policy = if req.detail && !row.policy_digest.is_empty() {
                self.store
                    .scaling_policy_snapshot(&row.policy_digest)
                    .map_err(|e| map_store_error("Error reading policy snapshot", e))?
            } else {
                None
            };
            entries.push(history_proto(row, policy, req.detail));
        }
        Ok(Response::new(GetScaleHistoryReply { entries }))
    }

    // ACL: scale:write on the job; operator:write for `all`.
    pub(super) async fn pause_scaling_rpc(
        &self,
        request: Request<PauseScalingRequest>,
    ) -> Result<Response<PauseScalingReply>, Status> {
        let policy = request.get_ref().policy.trim().to_string();
        let identity = if request.get_ref().all {
            self.check_acl(
                &request,
                AclResource::Operator,
                AclAction::Write,
                AclScope::none(),
            )?
        } else if !policy.is_empty() {
            // The policy's scope needs no lookup: its id names it.
            self.check_acl(
                &request,
                AclResource::Scale,
                AclAction::Write,
                AclScope::job(&policy),
            )?
        } else {
            let job = request.get_ref().job.trim().to_string();
            if job.is_empty() {
                return Err(Status::invalid_argument(
                    "name a job or a standalone policy to pause, or set all for the whole cluster",
                ));
            }
            self.check_acl(
                &request,
                AclResource::Scale,
                AclAction::Write,
                self.job_read_scope(&job),
            )?
        };
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "pause_scaling",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(PauseScalingReply {
                message,
                deduplicated: true,
                until_unix_ms: None,
            }));
        }
        let now_ms = now_unix_ms();
        let (job_id, group, task, target) = if inner.all {
            (
                PAUSE_ANY.to_string(),
                PAUSE_ANY.to_string(),
                PAUSE_ANY.to_string(),
                "the whole cluster".to_string(),
            )
        } else if !policy.is_empty() {
            let (namespace, name) = crate::structs::namespace::split_job_id(&policy);
            if self
                .store
                .scaling_policy(namespace, name)
                .map_err(|e| map_store_error("Error reading scaling policy", e))?
                .is_none()
            {
                return Err(Status::not_found(format!(
                    "scaling policy {policy} not found. Hint: `legion scaling list` shows the \
                     policies"
                )));
            }
            (
                policy_target_key(&policy),
                String::new(),
                String::new(),
                format!("scaling policy {policy}"),
            )
        } else {
            let job = inner.job.trim().to_string();
            let narrowed = inner
                .task_group
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                || inner
                    .task_name
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty());
            if narrowed {
                let config = self
                    .store
                    .get_job_config(&job)
                    .map_err(|e| map_store_error("Error loading job config", e))?;
                let (owner, _) = resolve_scale_target(
                    &config,
                    &job,
                    inner.task_group.as_deref(),
                    inner.task_name.as_deref(),
                )?;
                let label = target_label(&job, &owner.group, &owner.name);
                (job, owner.group.clone(), owner.name.clone(), label)
            } else {
                // The job must exist (a pause on nothing would sit there).
                self.store
                    .get_job_config(&job)
                    .map_err(|e| map_store_error("Error loading job config", e))?;
                (
                    job.clone(),
                    PAUSE_ANY.to_string(),
                    PAUSE_ANY.to_string(),
                    format!("{job} (every autoscaled task)"),
                )
            }
        };
        let scope = if inner.all {
            "all"
        } else if !policy.is_empty() {
            "policy"
        } else if group == PAUSE_ANY {
            "job"
        } else {
            "task"
        };
        let (message, until) = if inner.resume {
            let existed = self
                .store
                .clear_scaling_pause(&job_id, &group, &task)
                .map_err(|e| map_store_error("Error resuming autoscaling", e))?;
            if existed {
                self.store.events().scale_paused(
                    scope,
                    &target,
                    false,
                    None,
                    &identity.principal,
                    &inner.reason,
                );
                (format!("resumed autoscaling for {target}"), None)
            } else {
                (
                    format!("autoscaling was not paused for {target} at that scope"),
                    None,
                )
            }
        } else {
            let until = inner.for_seconds.map(|seconds| {
                now_ms
                    .saturating_add(i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX))
            });
            self.store
                .set_scaling_pause(
                    &job_id,
                    &group,
                    &task,
                    until,
                    &inner.reason,
                    &identity.principal,
                    now_ms,
                )
                .map_err(|e| map_store_error("Error pausing autoscaling", e))?;
            self.store.events().scale_paused(
                scope,
                &target,
                true,
                until,
                &identity.principal,
                &inner.reason,
            );
            (
                format!(
                    "paused autoscaling for {target} {}",
                    until
                        .map(|until| format!("until {}", clock(until)))
                        .unwrap_or_else(|| "until `legion scaling resume`".to_string())
                ),
                until,
            )
        };
        self.store
            .idempotency_record(&idempotency_key, "pause_scaling", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(PauseScalingReply {
            message,
            deduplicated: false,
            until_unix_ms: until,
        }))
    }
}

/// `field: old -> new` for every field whose effective value differs
/// between two resolutions (`legion scaling apply --dry-run`'s diff).
pub(super) fn changed_field_values(before: &ResolvedPolicy, after: &ResolvedPolicy) -> Vec<String> {
    let before_rows: HashMap<String, String> = before
        .explain_rows()
        .into_iter()
        .map(|row| (row.field, row.value))
        .collect();
    let mut lines: Vec<String> = after
        .explain_rows()
        .into_iter()
        .filter_map(|row| match before_rows.get(&row.field) {
            Some(old) if *old == row.value => None,
            Some(old) => Some(format!("{}: {old} -> {}", row.field, row.value)),
            None => Some(format!("{}: (none) -> {}", row.field, row.value)),
        })
        .collect();
    let after_fields: BTreeSet<String> = after
        .explain_rows()
        .into_iter()
        .map(|row| row.field)
        .collect();
    for (field, old) in &before_rows {
        if !after_fields.contains(field) {
            lines.push(format!("{field}: {old} -> (none)"));
        }
    }
    lines
}

/// The field paths whose effective value differs between two resolutions.
fn changed_fields(before: &ResolvedPolicy, after: &ResolvedPolicy) -> Vec<String> {
    let before_rows: HashMap<String, String> = before
        .explain_rows()
        .into_iter()
        .map(|row| (row.field, row.value))
        .collect();
    after
        .explain_rows()
        .into_iter()
        .filter(|row| before_rows.get(&row.field) != Some(&row.value))
        .map(|row| row.field)
        .collect()
}
