//! Task autoscaling's RPCs (docs/autoscaling.md; autoscaler-v2 plan §6.5,
//! §6.6, §6.13): the scale write with pins and compare-and-set
//! (`ScaleTask`), the policy stream (`WatchScalingPolicies`), the cluster's
//! scaling defaults, the app-plane reports (`ReportScalingStatus`,
//! `RecordScalingAction` and its action intents), and the read and pause
//! surface people use (`GetScalingStatus`, `GetScaleHistory`,
//! `PauseScaling`), for task targets and the autoscaler's objects
//! (`autoscaler/ScalingPolicy`, `autoscaler/NodeGroup`; read through
//! [`super::scaling_objects`]).

use super::*;
use std::collections::BTreeMap;

use super::scaling_objects::{
    action_proto, load_objects, object_ref_label, object_ref_of, provenance_rows, ScalingObject,
};
use crate::autoscaler::defaults::ResolvedPolicy;
use crate::autoscaler::policy::StandaloneSpec;
use crate::autoscaler::{AutoscaleRecord, EffectivePolicy};
use crate::manager::autoscale::{
    is_autoscaled, object_target_key, object_uid_of_target_key, summary_line, target_label,
    targets_for_config, AutoscaleLayers, TargetPolicy, OBJECT_TARGET_PREFIX,
};
use crate::manager::scaling_status::AppLiveness;
use crate::storage::store::{
    covering_pause, ActionHistory, ActionWrite, HistoryRow, NewHistory, PauseRow, ScaleWrite,
    ScaleWriteOp, ScalingActionRow as ActionRow, TaskScale, TaskScalePin, PAUSE_ANY,
};
use crate::structs::{
    ApplyScalingDefaultsReply, ApplyScalingDefaultsRequest, GetScaleHistoryReply,
    GetScaleHistoryRequest, GetScalingDefaultsReply, GetScalingDefaultsRequest,
    GetScalingStatusReply, GetScalingStatusRequest, PauseScalingReply, PauseScalingRequest,
    RecordScalingActionReply, RecordScalingActionRequest, ReportScalingStatusReply,
    ReportScalingStatusRequest, ScaleDecision, ScaleHistoryEntry, ScalePin, ScalingActionIntent,
    ScalingAppContext, ScalingAppReplica, ScalingAppStatus, ScalingObjectRef, ScalingObjectState,
    ScalingPauseInfo, ScalingPolicy, ScalingPolicyChange, ScalingPolicySet, ScalingProvenanceRow,
    ScalingTargetStatus, WatchScalingPoliciesRequest,
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

/// A history row as the API shows it. An object target's row (keyed
/// `object:<uid>`) names `object`, when the caller knows it, instead of a
/// job.
fn history_proto(
    row: &HistoryRow,
    policy_json: Option<String>,
    detail: bool,
    object: Option<&ScalingObjectRef>,
) -> ScaleHistoryEntry {
    let uid = object_uid_of_target_key(&row.job_id);
    ScaleHistoryEntry {
        id: row.id.max(0) as u64,
        job_id: if uid.is_some() {
            String::new()
        } else {
            row.job_id.clone()
        },
        object: uid.map(|uid| {
            object.cloned().unwrap_or(ScalingObjectRef {
                uid,
                ..Default::default()
            })
        }),
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
    provenance_rows(resolved)
}

/// Every object target's scaling state that has any (its own pause, its
/// action row), keyed `object:<uid>`: what `WatchScalingPolicies` sends the
/// autoscaler for its objects.
fn object_states(
    pauses: &[PauseRow],
    actions: &[ActionRow],
    now_ms: i64,
) -> Vec<ScalingObjectState> {
    let mut states: BTreeMap<String, ScalingObjectState> = BTreeMap::new();
    for pause in pauses
        .iter()
        .filter(|pause| pause.active(now_ms) && pause.scope() == "object")
    {
        let Some(uid) = object_uid_of_target_key(&pause.job_id) else {
            continue;
        };
        states
            .entry(pause.job_id.clone())
            .or_insert_with(|| ScalingObjectState {
                key: pause.job_id.clone(),
                uid,
                ..Default::default()
            })
            .pause = Some(pause_proto(pause));
    }
    for action in actions
        .iter()
        .filter(|action| action.target.starts_with(OBJECT_TARGET_PREFIX))
    {
        let Some(uid) = object_uid_of_target_key(&action.target) else {
            continue;
        };
        states
            .entry(action.target.clone())
            .or_insert_with(|| ScalingObjectState {
                key: action.target.clone(),
                uid,
                ..Default::default()
            })
            .action = Some(action_proto(action));
    }
    states.into_values().collect()
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
/// always names the policy it claims. The policy is a task's or an external
/// target's effective policy, or a node group's.
fn check_decision(decision: &ScaleDecision) -> Result<(), Status> {
    if decision.policy_json.is_empty() {
        return Ok(());
    }
    let digest = match serde_json::from_str::<EffectivePolicy>(&decision.policy_json) {
        Ok(policy) => policy.digest(),
        Err(err) => {
            serde_json::from_str::<crate::autoscaler::EffectiveNodeGroup>(&decision.policy_json)
                .map(|group| group.digest())
                .map_err(|_| Status::invalid_argument(format!("decision.policy_json: {err}")))?
        }
    };
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
    /// minted from it with `legion login --method legion`, both carry that
    /// reference; [`Self::app_of_identity`]). Management tokens and
    /// ACL-disabled clusters count too; a delegated identity (`<viewer> via
    /// app autoscaler`) does not.
    pub(super) fn is_autoscaler_identity(&self, identity: &AclIdentity) -> bool {
        identity.is_management || self.app_of_identity(identity).as_deref() == Some(AUTOSCALER_APP)
    }

    /// The app-plane gate (`// ACL: app-plane`): only the autoscaler's own
    /// identity, or a management token, may call.
    fn enforce_app_plane<T>(&self, request: &Request<T>) -> Result<AclIdentity, Status> {
        let identity = self.authenticate_acl(request)?;
        if self.is_autoscaler_identity(&identity) {
            return Ok(identity);
        }
        if identity.source == crate::structs::acl::AclSource::Delegated {
            // Never through audit-only: an app acting for a viewer is not
            // the app plane, whatever the mode.
            warn!(
                target: "acl_audit",
                principal = %identity.principal,
                source = identity.source.as_str(),
                resource = "app-plane",
                action = "call",
                enforced = true,
                "ACL denial"
            );
            return Err(Status::permission_denied(format!(
                "app-plane endpoints take the autoscaler's own identity, not one acting for a \
                 viewer ({}): send the call without legion-on-behalf-of",
                identity.principal
            )));
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
    /// `count`, or only installed apps' jobspec extension values
    /// (`classify_in_place`), move the running allocations to the new
    /// version instead of rolling out (autoscaler-v2 plan §6.3; apps plan
    /// §6.6). Only when no rollout is in flight and every allocation runs
    /// the previous version; otherwise `None`, and the update rolls out as
    /// usual.
    pub(super) fn update_in_place(
        &self,
        config: &JobConfig,
        job_config_id: &str,
    ) -> Result<Option<crate::manager::autoscale::InPlaceChange>, Status> {
        let Some(previous_id) = self
            .store
            .previous_job_config_id(job_config_id)
            .map_err(|e| map_store_error("Error loading previous job config", e))?
        else {
            return Ok(None);
        };
        let Some(previous) = self
            .store
            .get_job_config_by_id_optional(&previous_id)
            .map_err(|e| map_store_error("Error loading previous job config", e))?
        else {
            return Ok(None);
        };
        // Compare the stored versions: the store normalizes what it keeps
        // (resource defaults, say), so the request's config is not what the
        // previous version is stored as.
        let Some(next) = self
            .store
            .get_job_config_by_id_optional(job_config_id)
            .map_err(|e| map_store_error("Error loading job config", e))?
        else {
            return Ok(None);
        };
        let Some(change) = crate::manager::autoscale::classify_in_place(&previous, &next) else {
            return Ok(None);
        };
        let rollouts = self
            .store
            .list_rollouts(Some(&config.name), true)
            .map_err(|e| map_store_error("Error checking rollouts", e))?;
        if !rollouts.is_empty() {
            return Ok(None);
        }
        let allocations = self
            .store
            .allocs_for_job(&config.name)
            .map_err(|e| map_store_error("Error loading job allocations", e))?;
        if allocations
            .iter()
            .any(|allocation| allocation.job_config_id != previous_id)
        {
            return Ok(None);
        }
        let moved = self
            .store
            .migrate_allocations_to_job_config(&previous_id, job_config_id)
            .map_err(|e| map_store_error("Error updating allocations in place", e))?;
        info!(
            job_id = %config.name,
            job_config_id,
            allocations = moved.len(),
            autoscale = change.autoscale,
            extensions = change.extensions,
            "Update applied in place"
        );
        Ok(Some(change))
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
        // An installed app's count is its `replicas` (`legion app set
        // --replicas`): a scale override would outlive every resubmit.
        self.refuse_direct_app_job_write(&inner.name, "scaling it directly")?;

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
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            AclScope::any(),
        )?;
        let credential = self.stream_credential(&request, &identity)?;
        let req = request.into_inner();
        let namespaces = if req.namespaces.is_empty() {
            None
        } else {
            Some(
                crate::structs::namespace::NamespaceSelection::parse(&req.namespaces)
                    .map_err(Status::invalid_argument)?,
            )
        };
        if let Some(shard) = req
            .shards
            .iter()
            .find(|shard| req.shard_count > 0 && **shard >= req.shard_count)
        {
            return Err(Status::invalid_argument(format!(
                "shard {shard} is not below shard_count {}",
                req.shard_count
            )));
        }
        let shard_count = req.shard_count;
        let shards: BTreeSet<u32> = req.shards.iter().copied().collect();
        // Whether a policy with shard key `key` is in the requested shards.
        let in_shards = move |key: &str| {
            shard_count == 0
                || shards.contains(&crate::autoscaler::shard::shard_of(key, shard_count))
        };
        let deltas = req.deltas;
        // The autoscaler's own identity also gets what it needs to run its
        // objects (the layers, the cluster-wide pause, each object target's
        // pause and action row); anyone else, task policies only.
        let app_plane = self.is_autoscaler_identity(&identity);
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
            let mut last_sent: Option<BTreeMap<String, WatchEntry>> = None;
            let mut last_context: Option<ScalingAppContext> = None;
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
                    let pauses = store.scaling_pause_fingerprint()?;
                    let actions = store.scaling_action_fingerprint()?;
                    let plugins = store
                        .get_app(crate::manager::autoscale::AUTOSCALER_APP)?
                        .map(|record| record.updated_at)
                        .unwrap_or_default();
                    Ok(format!(
                        "{configs:?}|{defaults}|{policies:?}|{pauses:?}|{actions:?}|{plugins}"
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
                let now_ms = now_unix_ms();
                let mut entries: BTreeMap<String, WatchEntry> = BTreeMap::new();
                for policy in policies {
                    let key = crate::autoscaler::shard::task_key(
                        &policy.job_id,
                        &policy.task_group,
                        &policy.task_name,
                    );
                    if in_shards(&key) {
                        entries.insert(key, WatchEntry::Task(Box::new(policy)));
                    }
                }
                let context = if app_plane {
                    let pauses = store.scaling_pauses().unwrap_or_default();
                    let actions = store.scaling_actions().unwrap_or_default();
                    for state in object_states(&pauses, &actions, now_ms) {
                        entries.insert(state.key.clone(), WatchEntry::Object(Box::new(state)));
                    }
                    let mut context = layers.to_proto();
                    context.paused_all = pauses
                        .iter()
                        .find(|pause| pause.scope() == "all" && pause.active(now_ms))
                        .map(pause_proto);
                    Some(context)
                } else {
                    None
                };
                let context_changed = context.is_some() && context != last_context;
                let message = watch_message(last_sent.as_ref(), &entries, deltas).or_else(|| {
                    // Only the context changed: a message that carries it
                    // (an empty delta, or the whole set again).
                    context_changed.then(|| {
                        if deltas {
                            (false, Vec::new(), Vec::new(), Vec::new())
                        } else {
                            watch_message(None, &entries, false).unwrap_or_default()
                        }
                    })
                });
                let Some((full, policies, objects, removed)) = message else {
                    continue;
                };
                let send_context = (full || context_changed).then(|| context.clone()).flatten();
                for (index, part) in watch_parts(full, policies, objects, removed, deltas)
                    .into_iter()
                    .enumerate()
                {
                    generation += 1;
                    let set = ScalingPolicySet {
                        generation,
                        resolved_at_unix_ms: now_ms,
                        defaults_version: layers.cluster_version,
                        context: if index == 0 {
                            send_context.clone()
                        } else {
                            None
                        },
                        ..part
                    };
                    if tx.send(Ok(set)).await.is_err() {
                        return;
                    }
                }
                last_context = context;
                last_sent = Some(entries);
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
        // Scaling policy objects inherit the defaults too (a node group
        // inherits nothing): read from the object store, resolved as the
        // app resolves them.
        let objects = load_objects(&self.store, &layers)
            .map_err(|e| map_store_error("Error loading scaling objects", e))?;
        for object in &objects {
            let Ok(StandaloneSpec::External(spec)) = &object.spec else {
                continue;
            };
            let after = next.resolve_standalone(spec);
            let before_digest = object
                .external()
                .map(|resolved| resolved.digest.clone())
                .unwrap_or_default();
            let (after_digest, error, fields) = match &after {
                Ok(resolved) => (
                    resolved.digest.clone(),
                    String::new(),
                    match object.external() {
                        Some(before) => changed_fields(before, resolved),
                        None => Vec::new(),
                    },
                ),
                Err(err) => (String::new(), err.clone(), Vec::new()),
            };
            if before_digest != after_digest || !error.is_empty() {
                changes.push(ScalingPolicyChange {
                    target: object.label(),
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
                "scaling defaults: the change would leave {invalid} autoscaled task(s) or \
                 scaling policies with an invalid policy:\n{}\nHint: fix those first, or apply \
                 with --force to hold them `invalid` until fixed",
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
            governing: layers.governing_proto(&namespace),
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
        // An object target's report lives under its uid's key.
        for target in report.targets.iter_mut() {
            if let Some(object) = &target.object {
                target.job_id = object_target_key(object.uid);
                target.task_group.clear();
                target.task_name.clear();
            }
        }
        let accepted =
            self.store
                .scaling_status()
                .record(&report, std::time::Instant::now(), now_unix_ms());
        for target in &report.targets {
            let object = target.object.as_ref().map(|object| {
                (
                    object,
                    crate::autoscaler::policy::object_label(&object.namespace, &object.name),
                )
            });
            for flag in &target.events {
                let Some((object, id)) = &object else {
                    self.store.events().scale_flag(
                        &flag.kind,
                        &target.job_id,
                        &target.task_group,
                        &target.task_name,
                        &flag.message,
                    );
                    continue;
                };
                let named = crate::storage::events::ObjectTarget {
                    kind: &object.kind,
                    id,
                    uid: object.uid,
                };
                if matches!(flag.kind.as_str(), "pending" | "join_timeout" | "scale_in") {
                    self.store.events().nodegroup_flag(
                        &flag.kind,
                        &named,
                        &flag.message,
                        &flag.outcome,
                    );
                } else {
                    self.store
                        .events()
                        .scale_policy_flag(&flag.kind, &named, &flag.message);
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
    #[instrument(skip_all, fields(kind = %request.get_ref().kind, uid = request.get_ref().object.as_ref().map_or(0, |object| object.uid)))]
    pub(super) async fn record_scaling_action_rpc(
        &self,
        request: Request<RecordScalingActionRequest>,
    ) -> Result<Response<RecordScalingActionReply>, Status> {
        let identity = self.enforce_app_plane(&request)?;
        let inner = request.into_inner();
        let now_ms = now_unix_ms();
        match (inner.kind.as_str(), inner.object.clone()) {
            ("would", _) => {}
            ("intent" | "outcome", None) => {
                return Err(Status::invalid_argument(format!(
                    "an `{}` names an object target (`object`: an autoscaler/ScalingPolicy or \
                     autoscaler/NodeGroup); a task's count changes through ScaleTask",
                    inner.kind
                )))
            }
            ("intent", Some(object)) => {
                let row = self.current_scaling_object(&object)?;
                return self.open_intent(&identity, inner, &row, now_ms);
            }
            ("outcome", Some(object)) => {
                let row = self.current_scaling_object(&object)?;
                return self.close_intent(&identity, inner, &row, now_ms);
            }
            (other, _) => {
                return Err(Status::invalid_argument(format!(
                    "unknown action kind '{other}': use would (a dry-run decision), intent \
                     (before acting on an object target), or outcome (after)"
                )))
            }
        }
        let decision = inner.decision.clone().unwrap_or_default();
        check_decision(&decision)?;
        let (job_id, task_group, task_name) = match &inner.object {
            None => (
                inner.job_id.clone(),
                inner.task_group.clone(),
                inner.task_name.clone(),
            ),
            Some(object) => {
                let row = self.current_scaling_object(object)?;
                (object_target_key(row.uid), String::new(), String::new())
            }
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
                "would",
                false,
                new_history(Some(&decision)),
            )
            .map_err(|e| map_store_error("Error recording scaling action", e))?;
        Ok(Response::new(RecordScalingActionReply {
            history_id: history_id.max(0) as u64,
            won: false,
            action: None,
        }))
    }

    /// `RecordScalingAction` `intent`: compare-and-set an object target's
    /// action row, with the intent's history rows.
    fn open_intent(
        &self,
        identity: &AclIdentity,
        inner: RecordScalingActionRequest,
        row: &crate::storage::store::AppObjectRow,
        now_ms: i64,
    ) -> Result<Response<RecordScalingActionReply>, Status> {
        let intent = inner
            .intent
            .ok_or_else(|| Status::invalid_argument("an `intent` carries `intent`"))?;
        let decision = inner.decision.unwrap_or_default();
        check_decision(&decision)?;
        let object = object_ref_of(row);
        let label = object_ref_label(&object);
        let key = object_target_key(row.uid);
        if intent.desired.is_some() || !intent.cordon.is_empty() {
            // A pause holds even against a misbehaving app: no new count,
            // no new drain. Drains past their drain step go on.
            let pauses = self
                .store
                .scaling_pauses()
                .map_err(|e| map_store_error("Error loading pauses", e))?;
            if let Some(pause) = covering_pause(&pauses, &key, "", "", now_ms) {
                return Err(Status::failed_precondition(format!(
                    "{label} is paused ({} by {}): the autoscaler may not scale it. Hint: \
                     `legion scaling resume`",
                    pause.scope(),
                    pause.principal
                )));
            }
        }
        let history = new_history(Some(&decision));
        let step = |kind: &str, what: String| ActionHistory {
            from: intent.from,
            to: intent.from,
            kind: kind.to_string(),
            applied: true,
            history: NewHistory {
                summary: what,
                ..history.clone()
            },
        };
        let mut rows = Vec::new();
        if let Some(desired) = intent.desired {
            rows.push(ActionHistory {
                from: intent.from,
                to: desired,
                kind: "scaled".to_string(),
                applied: true,
                history: history.clone(),
            });
        }
        for node in &intent.cordon {
            rows.push(step(
                "drain",
                format!("drain {}: {}", node.node, intent.summary),
            ));
        }
        for node in &intent.terminate {
            rows.push(step(
                "terminate",
                format!("terminate {} (instance {})", node.node, node.provider_id),
            ));
        }
        for node in &intent.forget {
            rows.push(step(
                "forget",
                format!("forget {node}: its instance is gone"),
            ));
        }
        for node in &intent.uncordon {
            rows.push(step(
                "uncordon",
                format!("uncordon {node}: drain abandoned"),
            ));
        }
        let intent_json = serde_json::to_string(&intent)
            .map_err(|err| Status::internal(format!("encoding the intent: {err}")))?;
        let written = self
            .store
            .open_scaling_action(
                &key,
                inner.expected_action_version,
                &intent_json,
                &inner.instance,
                &identity.principal,
                now_ms,
                rows,
            )
            .map_err(|e| map_store_error("Error recording the action intent", e))?;
        Ok(Response::new(match written {
            ActionWrite::Won(action) => {
                if let Some(desired) = intent.desired {
                    let id =
                        crate::autoscaler::policy::object_label(&object.namespace, &object.name);
                    self.store.events().scale_policy_applied(
                        &crate::storage::events::ObjectTarget {
                            kind: &object.kind,
                            id: &id,
                            uid: object.uid,
                        },
                        intent.from,
                        desired,
                        &decision.summary,
                    );
                }
                info!(
                    object = %label,
                    uid = object.uid,
                    version = action.version,
                    owner = %action.owner,
                    "Scaling action intent opened"
                );
                RecordScalingActionReply {
                    history_id: 0,
                    won: true,
                    action: Some(action_proto(&action)),
                }
            }
            ActionWrite::Lost(action) => {
                debug!(object = %label, uid = object.uid, expected = inner.expected_action_version, "Scaling action intent lost the compare-and-set");
                RecordScalingActionReply {
                    history_id: 0,
                    won: false,
                    action: action.as_ref().map(action_proto),
                }
            }
        }))
    }

    /// `RecordScalingAction` `outcome`: close the open intent.
    fn close_intent(
        &self,
        identity: &AclIdentity,
        inner: RecordScalingActionRequest,
        row: &crate::storage::store::AppObjectRow,
        now_ms: i64,
    ) -> Result<Response<RecordScalingActionReply>, Status> {
        let key = object_target_key(row.uid);
        let outcome = inner.outcome.trim();
        if !matches!(outcome, "succeeded" | "failed" | "reconciled") {
            return Err(Status::invalid_argument(format!(
                "outcome {outcome:?}: use succeeded, failed, or reconciled"
            )));
        }
        let current = self
            .store
            .scaling_action(&key)
            .map_err(|e| map_store_error("Error reading the action intent", e))?;
        let mut rows = Vec::new();
        if outcome == "failed" {
            if let Some(intent) = current
                .as_ref()
                .filter(|row| row.version == inner.expected_action_version)
                .and_then(|row| serde_json::from_str::<ScalingActionIntent>(&row.intent).ok())
            {
                if let Some(desired) = intent.desired {
                    rows.push(ActionHistory {
                        from: intent.from,
                        to: desired,
                        kind: "failed".to_string(),
                        applied: false,
                        history: NewHistory {
                            summary: format!("target/scale to {desired} failed: {}", inner.detail),
                            decision_id: intent.decision_id.clone(),
                            ..Default::default()
                        },
                    });
                }
            }
        }
        let written = self
            .store
            .close_scaling_action(
                &key,
                inner.expected_action_version,
                outcome,
                &inner.detail,
                &identity.principal,
                now_ms,
                rows,
            )
            .map_err(|e| map_store_error("Error recording the action outcome", e))?;
        let (won, action) = match written {
            ActionWrite::Won(action) => (true, Some(action)),
            ActionWrite::Lost(action) => (false, action),
        };
        Ok(Response::new(RecordScalingActionReply {
            history_id: 0,
            won,
            action: action.as_ref().map(action_proto),
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
        if !job.is_empty() && req.object.is_some() {
            return Err(Status::invalid_argument(
                "name a job or an object (a scaling policy or node group), not both",
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
        // One object: checked on its kind's access before it is read.
        let named = match &req.object {
            Some(reference) => Some(self.scaling_object(&identity, reference, false)?.uid),
            None => None,
        };
        self.store
            .require_lane_leader(crate::storage::Lane::Status, "GetScalingStatus")
            .map_err(|e| map_store_error("Error reading scaling status", e))?;
        let namespaces = if job.is_empty() && named.is_none() {
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
            if named.is_some() || (!job.is_empty() && target.job_id != job) {
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
            // The autoscaler's objects, read from the object store by kind
            // and resolved as the app resolves them (`scaling_objects`).
            let registry = self.object_registry()?;
            let loaded = load_objects(&self.store, &layers)
                .map_err(|e| map_store_error("Error loading scaling objects", e))?;
            for object in &loaded {
                if let Some(uid) = named {
                    if object.row.uid != uid {
                        continue;
                    }
                } else {
                    if let Some(selection) = namespaces.as_ref() {
                        if !object.row.key.namespace.is_empty()
                            && !selection.matches(&object.row.key.namespace)
                        {
                            continue;
                        }
                    }
                    if !self.may_read_scaling_object(&identity, &registry, &object.row) {
                        continue;
                    }
                }
                out.push(
                    self.object_status(object, &pauses, now, now_ms, req.detail)
                        .map_err(|e| map_store_error("Error reading scaling status", e))?,
                );
            }
        }
        let app = self.app_status(&pauses, now, now_ms)?;
        Ok(Response::new(GetScalingStatusReply {
            app: Some(app),
            targets: out,
        }))
    }

    /// One of the autoscaler's objects' status: what the app last reported
    /// for it, its bounds and mode, its pause, and its last history row, all
    /// keyed by its uid.
    fn object_status(
        &self,
        policy: &ScalingObject,
        pauses: &[PauseRow],
        now: std::time::Instant,
        now_ms: i64,
        detail: bool,
    ) -> R<ScalingTargetStatus> {
        let reference = policy.reference();
        let key = policy.key();
        let pause = covering_pause(pauses, &key, "", "", now_ms).map(pause_proto);
        let last_change = self
            .store
            .scale_history(&key, Some(""), Some(""), 1)?
            .first()
            .map(|row| history_proto(row, None, false, Some(&reference)));
        let board = self.store.scaling_status();
        let entry = board.target(&key, "", "");
        let facts = policy
            .resolved
            .as_ref()
            .ok()
            .map(|resolved| resolved.facts());
        let error = policy.resolved.as_ref().err().cloned().unwrap_or_default();
        let (state, current, want, evaluated, summary, decision) = match (&facts, &entry) {
            (None, _) => ("invalid".to_string(), 0, 0, 0, error.clone(), String::new()),
            (Some(_), Some(entry)) if board.is_stale(entry.received, now) => (
                "stale".to_string(),
                entry.current,
                entry.current,
                entry.evaluated_at_unix_ms,
                entry.summary.clone(),
                entry.decision_json.clone(),
            ),
            (Some(_), Some(entry)) => (
                entry.state.clone(),
                entry.current,
                entry.want,
                entry.evaluated_at_unix_ms,
                entry.summary.clone(),
                entry.decision_json.clone(),
            ),
            (Some(_), None) => (
                "not-evaluated".to_string(),
                0,
                0,
                0,
                String::new(),
                String::new(),
            ),
        };
        let (policy_rows, policy_json) = match (policy.external(), &facts, detail) {
            (Some(resolved), _, true) => {
                (provenance_proto(resolved), resolved.policy.canonical_json())
            }
            (None, Some(facts), true) => (Vec::new(), facts.policy_json.clone()),
            _ => (Vec::new(), String::new()),
        };
        Ok(ScalingTargetStatus {
            mode: facts.as_ref().map(|f| f.mode.clone()).unwrap_or_default(),
            now: current,
            want,
            min: facts.as_ref().map_or(0, |f| f.min),
            max: facts.as_ref().map_or(0, |f| f.max),
            state,
            last_evaluation_unix_ms: evaluated,
            summary,
            last_change,
            pause,
            policy_digest: facts.as_ref().map(|f| f.digest.clone()).unwrap_or_default(),
            decision_json: if detail { decision } else { String::new() },
            policy: policy_rows,
            policy_json,
            policy_summary: match &facts {
                Some(facts) => facts.summary.clone(),
                None => format!("invalid: {error}"),
            },
            error,
            kind: policy.kind.as_str().to_string(),
            instance: entry.map(|entry| entry.instance).unwrap_or_default(),
            object: Some(reference),
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
            .map(|row| history_proto(row, None, false, None));
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
            object: None,
            kind: if target.members.len() > 1 {
                "group".to_string()
            } else {
                "task".to_string()
            },
            instance: entry.map(|entry| entry.instance).unwrap_or_default(),
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
        let replicas = board.replicas();
        let interval = board.interval();
        let live: Vec<&crate::manager::scaling_status::AppReport> = replicas
            .iter()
            .filter(|replica| replica.fresh(now, interval))
            .collect();
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
                    // One replica: `running (alloc 41)`. Several: each
                    // with how many shards it evaluates.
                    let who = if live.len() > 1 {
                        format!(
                            "running {} replicas ({})",
                            live.len(),
                            live.iter()
                                .map(|replica| format!(
                                    "{}: {} shards",
                                    replica.instance,
                                    if replica.shards.is_empty() {
                                        crate::autoscaler::shard::SHARDS as usize
                                    } else {
                                        replica.shards.len()
                                    }
                                ))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    } else {
                        format!(
                            "running ({})",
                            report.map(|report| report.instance.as_str()).unwrap_or("-")
                        )
                    };
                    let stale = replicas.len() - live.len();
                    (
                        "running".to_string(),
                        format!(
                            "{who}, last evaluation {ago}s ago{}{}",
                            if stale > 0 {
                                format!(", {stale} replica(s) stopped reporting")
                            } else {
                                String::new()
                            },
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
            replicas: replicas
                .iter()
                .map(|replica| ScalingAppReplica {
                    instance: replica.instance.clone(),
                    last_report_unix_ms: replica.received_unix_ms,
                    interval_seconds: replica.interval.as_secs() as u32,
                    mode: replica.mode.clone(),
                    version: replica.version.clone(),
                    shards: replica.shards.clone(),
                    shard_count: replica.shard_count,
                    targets: replica.targets,
                    state: if replica.fresh(now, interval) {
                        "running".to_string()
                    } else {
                        "stale".to_string()
                    },
                })
                .collect(),
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

    // ACL: scale:read on the job; an object's with its kind's read access
    // (scale:read in its namespace for a ScalingPolicy, node:read for a
    // NodeGroup), checked before the lookup.
    pub(super) async fn get_scale_history_rpc(
        &self,
        request: Request<GetScaleHistoryRequest>,
    ) -> Result<Response<GetScaleHistoryReply>, Status> {
        let object = match request.get_ref().object.clone() {
            None => {
                let scope = self.job_read_scope(&request.get_ref().job);
                self.check_acl(&request, AclResource::Scale, AclAction::Read, scope)?;
                None
            }
            Some(reference) => {
                let identity = self.authenticate_acl(&request)?;
                Some(object_ref_of(
                    &self.scaling_object(&identity, &reference, false)?,
                ))
            }
        };
        let mut req = request.into_inner();
        if let Some(object) = &object {
            req.job = object_target_key(object.uid);
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
            entries.push(history_proto(row, policy, req.detail, object.as_ref()));
        }
        Ok(Response::new(GetScaleHistoryReply { entries }))
    }

    // ACL: scale:write on the job; operator:write for `all`.
    pub(super) async fn pause_scaling_rpc(
        &self,
        request: Request<PauseScalingRequest>,
    ) -> Result<Response<PauseScalingReply>, Status> {
        let mut object = None;
        let identity = if request.get_ref().all {
            self.check_acl(
                &request,
                AclResource::Operator,
                AclAction::Write,
                AclScope::none(),
            )?
        } else if let Some(reference) = request.get_ref().object.clone() {
            // What writing the object needs (its kind's access and
            // `requires`), checked before the lookup.
            let identity = self.authenticate_acl(&request)?;
            object = Some(self.scaling_object(&identity, &reference, true)?);
            identity
        } else {
            let job = request.get_ref().job.trim().to_string();
            if job.is_empty() {
                return Err(Status::invalid_argument(
                    "name a job or an object (a scaling policy or node group) to pause, or set \
                     all for the whole cluster",
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
        } else if let Some(row) = &object {
            (
                object_target_key(row.uid),
                String::new(),
                String::new(),
                object_ref_label(&object_ref_of(row)),
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
        } else if object.is_some() {
            "object"
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

/// One entry of the watch, by the key a delta names it with.
#[derive(Debug, Clone, PartialEq)]
enum WatchEntry {
    Task(Box<ScalingPolicy>),
    Object(Box<ScalingObjectState>),
}

/// One watch message's contents: whether it is the full set, the task
/// policies and object states it carries, and the keys removed.
type WatchMessage = (
    bool,
    Vec<ScalingPolicy>,
    Vec<ScalingObjectState>,
    Vec<String>,
);

/// What a watch sends next: the full set, or what changed since the last
/// message (`None`: nothing changed).
fn watch_message(
    last: Option<&BTreeMap<String, WatchEntry>>,
    now: &BTreeMap<String, WatchEntry>,
    deltas: bool,
) -> Option<WatchMessage> {
    // Sorted as the full set always was: tasks by job, group, task;
    // object states by uid.
    let split = |entries: &mut dyn Iterator<Item = &WatchEntry>| {
        let mut tasks: Vec<ScalingPolicy> = Vec::new();
        let mut objects: Vec<ScalingObjectState> = Vec::new();
        for entry in entries {
            match entry {
                WatchEntry::Task(policy) => tasks.push(policy.as_ref().clone()),
                WatchEntry::Object(state) => objects.push(state.as_ref().clone()),
            }
        }
        tasks.sort_by(|a, b| {
            (&a.job_id, &a.task_group, &a.task_name).cmp(&(&b.job_id, &b.task_group, &b.task_name))
        });
        objects.sort_by_key(|state| state.uid);
        (tasks, objects)
    };
    match last {
        Some(last) if last == now => None,
        Some(last) if deltas => {
            let removed: Vec<String> = last
                .keys()
                .filter(|key| !now.contains_key(*key))
                .cloned()
                .collect();
            let (tasks, objects) = split(
                &mut now
                    .iter()
                    .filter(|(key, entry)| last.get(*key) != Some(*entry))
                    .map(|(_, entry)| entry),
            );
            Some((false, tasks, objects, removed))
        }
        _ => {
            let (tasks, objects) = split(&mut now.values());
            Some((true, tasks, objects, Vec::new()))
        }
    }
}

/// The most policies one message of a delta watch carries.
const WATCH_CHUNK: usize = 1000;

/// One change as messages: all of it without deltas; with them, at most
/// [`WATCH_CHUNK`] policies a message, all but the last marked `more`, the
/// first carrying `full` and `removed`.
fn watch_parts(
    full: bool,
    policies: Vec<ScalingPolicy>,
    objects: Vec<ScalingObjectState>,
    removed: Vec<String>,
    deltas: bool,
) -> Vec<ScalingPolicySet> {
    if !deltas || policies.len() + objects.len() <= WATCH_CHUNK {
        return vec![ScalingPolicySet {
            policies,
            objects,
            removed,
            full,
            ..Default::default()
        }];
    }
    let mut parts = Vec::new();
    let mut policies = policies.into_iter().peekable();
    let mut objects = objects.into_iter().peekable();
    let mut removed = Some(removed);
    loop {
        let mut part = ScalingPolicySet {
            full: full && parts.is_empty(),
            removed: removed.take().unwrap_or_default(),
            ..Default::default()
        };
        while part.policies.len() + part.objects.len() < WATCH_CHUNK {
            if let Some(policy) = policies.next() {
                part.policies.push(policy);
            } else if let Some(state) = objects.next() {
                part.objects.push(state);
            } else {
                break;
            }
        }
        part.more = policies.peek().is_some() || objects.peek().is_some();
        let done = !part.more;
        parts.push(part);
        if done {
            return parts;
        }
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;

    fn task(job: &str, digest: &str) -> WatchEntry {
        WatchEntry::Task(Box::new(ScalingPolicy {
            job_id: job.into(),
            task_group: "default".into(),
            task_name: "web".into(),
            digest: digest.into(),
            ..Default::default()
        }))
    }

    /// Without deltas every message is the whole set; with them, only what
    /// changed and the keys removed; nothing at all when nothing changed.
    #[test]
    fn deltas_carry_changes_and_removals_test() {
        let first: BTreeMap<String, WatchEntry> = [
            ("a/default/web".to_string(), task("a", "1")),
            ("b/default/web".to_string(), task("b", "1")),
        ]
        .into_iter()
        .collect();
        let (full, tasks, _, removed) = watch_message(None, &first, true).expect("first");
        assert!(full && removed.is_empty());
        assert_eq!(tasks.len(), 2);
        assert!(watch_message(Some(&first), &first, true).is_none());
        let mut next = first.clone();
        next.insert("a/default/web".into(), task("a", "2"));
        next.remove("b/default/web");
        next.insert("c/default/web".into(), task("c", "1"));
        let (full, tasks, _, removed) = watch_message(Some(&first), &next, true).expect("delta");
        assert!(!full);
        let jobs: Vec<&str> = tasks.iter().map(|policy| policy.job_id.as_str()).collect();
        assert_eq!(jobs, vec!["a", "c"]);
        assert_eq!(removed, vec!["b/default/web".to_string()]);
        let (full, tasks, _, removed) =
            watch_message(Some(&first), &next, false).expect("whole set");
        assert!(full && removed.is_empty());
        assert_eq!(tasks.len(), 2);
    }

    /// A large change is split, every part but the last marked `more`.
    #[test]
    fn large_changes_are_split_test() {
        let policies: Vec<ScalingPolicy> = (0..2500)
            .map(|n| ScalingPolicy {
                job_id: format!("job-{n}"),
                ..Default::default()
            })
            .collect();
        let parts = watch_parts(true, policies.clone(), Vec::new(), vec!["x".into()], true);
        let shape: Vec<(bool, usize, usize, bool)> = parts
            .iter()
            .map(|part| {
                (
                    part.full,
                    part.policies.len(),
                    part.removed.len(),
                    part.more,
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                (true, 1000, 1, true),
                (false, 1000, 0, true),
                (false, 500, 0, false)
            ]
        );
        let whole = watch_parts(true, policies, Vec::new(), Vec::new(), false);
        assert_eq!(whole.len(), 1);
        assert!(!whole[0].more);
    }
}
