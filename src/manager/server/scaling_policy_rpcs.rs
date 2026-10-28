//! Standalone scaling policies' RPCs (docs/autoscaling.md, "Standalone
//! policies"; autoscaler-v2 plan §6.2, §6.13): `ApplyScalingPolicy`,
//! `DeleteScalingPolicy`, `ListScalingPolicies`, and what
//! `WatchScalingPolicies` streams of them.

use super::*;
use crate::autoscaler::defaults::ResolvedPolicy;
use crate::autoscaler::policy::{self as standalone, StandaloneSpec};
use crate::manager::autoscale::{
    policy_target_key, summary_line, AutoscaleLayers, TargetPolicy,
};
use crate::storage::store::{covering_pause, PauseRow, ScalingPolicyRow};
use crate::structs::{
    ApplyScalingPolicyReply, ApplyScalingPolicyRequest, DeleteScalingPolicyReply,
    DeleteScalingPolicyRequest, ListScalingPoliciesReply, ListScalingPoliciesRequest,
    ScalingPolicySummary, ScalingProvenanceRow, StandaloneScalingPolicy,
};

/// A stored standalone policy, parsed and resolved against the current
/// layers and plugins.
pub(super) struct LoadedPolicy {
    pub row: ScalingPolicyRow,
    pub spec: Result<StandaloneSpec, String>,
    pub resolved: Result<ResolvedPolicy, String>,
}

impl LoadedPolicy {
    pub fn id(&self) -> String {
        self.row.id()
    }

    /// The target plugin and its config, for an external policy.
    pub fn target(&self) -> (String, Value) {
        match &self.spec {
            Ok(StandaloneSpec::External(policy)) => {
                (policy.target.plugin.clone(), policy.target.config_value())
            }
            _ => (String::new(), json!({})),
        }
    }
}

/// Resolve one stored policy.
pub(super) fn load_policy(row: ScalingPolicyRow, layers: &AutoscaleLayers) -> LoadedPolicy {
    let spec = serde_json::from_str::<Value>(&row.spec)
        .map_err(|err| format!("the stored policy does not parse: {err}"))
        .and_then(|spec| {
            let namespace = if row.namespace.is_empty() {
                crate::structs::namespace::DEFAULT_NAMESPACE
            } else {
                row.namespace.as_str()
            };
            standalone::parse(&spec, namespace)
        });
    let resolved = match &spec {
        Ok(StandaloneSpec::External(policy)) => layers.resolve_standalone(policy),
        Ok(StandaloneSpec::Nodes(_)) => {
            Err("node-group policies are not run in this version".to_string())
        }
        Err(err) => Err(err.clone()),
    };
    LoadedPolicy {
        row,
        spec,
        resolved,
    }
}

/// Every stored standalone policy, resolved.
pub(super) fn load_policies(store: &Store, layers: &AutoscaleLayers) -> R<Vec<LoadedPolicy>> {
    Ok(store
        .scaling_policies()?
        .into_iter()
        .map(|row| load_policy(row, layers))
        .collect())
}

pub(super) fn provenance_rows(resolved: &ResolvedPolicy) -> Vec<ScalingProvenanceRow> {
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

/// The proto a standalone policy streams as.
pub(super) fn standalone_proto(policy: &LoadedPolicy, pauses: &[PauseRow], now_ms: i64) -> StandaloneScalingPolicy {
    let id = policy.id();
    let (plugin, config) = policy.target();
    let (state, error, digest, policy_json, summary) = match &policy.resolved {
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
    StandaloneScalingPolicy {
        id: id.clone(),
        namespace: policy.row.namespace.clone(),
        name: policy.row.name.clone(),
        kind: policy.row.kind.clone(),
        version: policy.row.version,
        state,
        error,
        digest,
        policy_json,
        target_plugin: plugin,
        target_config_json: config.to_string(),
        pause: covering_pause(pauses, &policy_target_key(&id), "", "", now_ms)
            .map(super::autoscale_rpcs::pause_proto),
        summary,
    }
}

fn task_summary(target: &TargetPolicy) -> ScalingPolicySummary {
    let namespace = crate::structs::namespace::job_namespace(&target.job_id).to_string();
    let kind = if target.members.len() > 1 { "group" } else { "task" };
    let (state, error, mode, min, max, summary) = match &target.resolved {
        Ok(resolved) => (
            "ok".to_string(),
            String::new(),
            resolved.policy.mode.to_string(),
            resolved.policy.min,
            resolved.policy.max,
            summary_line(&resolved.policy),
        ),
        Err(err) => ("invalid".to_string(), err.clone(), String::new(), 0, 0, String::new()),
    };
    ScalingPolicySummary {
        id: target.label(),
        kind: kind.to_string(),
        namespace,
        mode,
        min,
        max,
        state,
        error,
        target: target.label(),
        version: 0,
        summary,
        ..Default::default()
    }
}

fn standalone_summary(policy: &LoadedPolicy, detail: bool) -> ScalingPolicySummary {
    let (plugin, _) = policy.target();
    let (state, error, mode, min, max, summary) = match &policy.resolved {
        Ok(resolved) => (
            "ok".to_string(),
            String::new(),
            resolved.policy.mode.to_string(),
            resolved.policy.min,
            resolved.policy.max,
            summary_line(&resolved.policy),
        ),
        Err(err) => ("invalid".to_string(), err.clone(), String::new(), 0, 0, String::new()),
    };
    ScalingPolicySummary {
        id: policy.id(),
        kind: policy.row.kind.clone(),
        namespace: policy.row.namespace.clone(),
        mode,
        min,
        max,
        state,
        error,
        target: if plugin.is_empty() {
            "-".to_string()
        } else {
            format!("plugin {plugin}")
        },
        version: policy.row.version,
        summary,
        description: policy.row.description.clone(),
        updated_by: policy.row.updated_by.clone(),
        updated_at_unix_ms: policy.row.updated_ms,
        spec_json: if detail {
            policy.row.spec.clone()
        } else {
            String::new()
        },
    }
}

/// `name` or `namespace/name`, against a default namespace.
fn split_policy_ref(name: &str, namespace: &str) -> (String, String) {
    let name = name.trim();
    if name.contains('/') {
        let (namespace, name) = crate::structs::namespace::split_job_id(name);
        (namespace.to_string(), name.to_string())
    } else {
        let namespace = if namespace.trim().is_empty() {
            crate::structs::namespace::DEFAULT_NAMESPACE
        } else {
            namespace.trim()
        };
        (namespace.to_string(), name.to_string())
    }
}

impl ManagerDaemonService {
    /// The ACL a standalone policy's writes need: `scale:write` on its
    /// namespace and name; a node group, `operator:write` and `node:write`.
    fn enforce_policy_write(&self, identity: &AclIdentity, kind: &str, id: &str) -> Result<(), Status> {
        if kind == "nodes" {
            acl::enforce(identity, AclResource::Operator, AclAction::Write, &AclScope::none())?;
            acl::enforce(identity, AclResource::Node, AclAction::Write, &AclScope::any())?;
            Ok(())
        } else {
            acl::enforce(identity, AclResource::Scale, AclAction::Write, &AclScope::job(id))
        }
    }

    // ACL: two-step: the scope comes from the body. scale:write on the
    // policy's namespace and name for an external target; operator:write
    // and node:write for a node group (which needs no scale grant: a job
    // owner's broad scale:write must not add machines). Nothing is looked
    // up before the check, so there is nothing to probe.
    #[instrument(skip_all)]
    pub(super) async fn apply_scaling_policy_rpc(
        &self,
        request: Request<ApplyScalingPolicyRequest>,
    ) -> Result<Response<ApplyScalingPolicyReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        let default_namespace = if inner.namespace.trim().is_empty() {
            crate::structs::namespace::DEFAULT_NAMESPACE.to_string()
        } else {
            inner.namespace.trim().to_string()
        };
        let spec_value: Value = serde_json::from_str(&inner.spec_json)
            .map_err(|err| Status::invalid_argument(format!("the policy is not JSON: {err}")))?;
        let parsed = standalone::parse(&spec_value, &default_namespace)
            .map_err(|err| Status::invalid_argument(format!("Error validating scaling policy: {err}")))?;
        let id = parsed.id();
        self.enforce_policy_write(&identity, parsed.kind().as_str(), &id)?;

        let idempotency_key = if inner.dry_run {
            String::new()
        } else {
            scoped_idempotency_key(
                &identity,
                "apply_scaling_policy",
                inner.idempotency_key.as_deref(),
                &inner,
            )
        };
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(ApplyScalingPolicyReply {
                id,
                kind: parsed.kind().as_str().to_string(),
                message,
                deduplicated: true,
                ..Default::default()
            }));
        }
        let policy = match &parsed {
            StandaloneSpec::External(policy) => policy,
            StandaloneSpec::Nodes(_) => {
                return Err(Status::failed_precondition(format!(
                    "{id}: node-group policies are not run in this version, so this one would \
                     do nothing. Hint: `legion scaling demand -f <file>` sizes the group from the \
                     pending work today"
                )))
            }
        };
        self.ensure_namespace_exists(&policy.namespace)?;
        let layers = AutoscaleLayers::load(&self.store)
            .map_err(|e| map_store_error("Error loading scaling defaults", e))?;
        let resolved = layers
            .resolve_standalone(policy)
            .map_err(|err| Status::invalid_argument(format!("Error validating scaling policy: {err}")))?;
        let current = self
            .store
            .scaling_policy(&policy.namespace, &policy.name)
            .map_err(|e| map_store_error("Error reading scaling policy", e))?;
        let canonical = serde_json::to_string(&spec_value)
            .map_err(|err| Status::internal(format!("encoding the policy: {err}")))?;
        let diff = match &current {
            Some(row) => match load_policy(row.clone(), &layers).resolved {
                Ok(before) => super::autoscale_rpcs::changed_field_values(&before, &resolved),
                Err(err) => vec![format!("was invalid: {err}")],
            },
            None => Vec::new(),
        };
        let reply = |version: u64, created: bool, changed: bool, message: String| ApplyScalingPolicyReply {
            id: id.clone(),
            kind: "external".to_string(),
            version,
            created,
            changed,
            diff: diff.clone(),
            message,
            deduplicated: false,
            policy_json: resolved.policy.canonical_json(),
            digest: resolved.digest.clone(),
            provenance: provenance_rows(&resolved),
            dry_run: inner.dry_run,
        };
        if inner.dry_run {
            let unchanged = current.as_ref().is_some_and(|row| {
                row.spec == canonical && row.description == policy.description
            });
            let (version, verb) = match &current {
                None => (0, "would be created".to_string()),
                Some(row) if unchanged => (row.version, format!("unchanged (v{})", row.version)),
                Some(row) => (row.version, format!("would change (v{})", row.version)),
            };
            return Ok(Response::new(reply(
                version,
                current.is_none(),
                !unchanged,
                format!("scaling policy {id}: {verb}; dry run, nothing written"),
            )));
        }
        let put = self
            .store
            .put_scaling_policy(
                &policy.namespace,
                &policy.name,
                "external",
                &canonical,
                &policy.description,
                &identity.principal,
                super::autoscale_rpcs::now_unix_ms(),
            )
            .map_err(|e| map_store_error("Error writing scaling policy", e))?;
        let message = if put.created {
            format!("scaling policy {id}: created (v{})", put.version)
        } else if put.changed {
            format!(
                "scaling policy {id}: updated (v{} -> v{})",
                put.previous.unwrap_or_default(),
                put.version
            )
        } else {
            format!("scaling policy {id}: unchanged (v{})", put.version)
        };
        info!(
            policy = %id,
            version = put.version,
            created = put.created,
            changed = put.changed,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Scaling policy applied"
        );
        self.store
            .idempotency_record(&idempotency_key, "apply_scaling_policy", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(reply(put.version, put.created, put.changed, message)))
    }

    // ACL: scale:write, coarse; then scale:write on the policy's namespace
    // and name before the lookup. A stored node group (Phase 5b) would
    // also need operator:write and node:write, checked once its kind is
    // known.
    #[instrument(skip_all)]
    pub(super) async fn delete_scaling_policy_rpc(
        &self,
        request: Request<DeleteScalingPolicyRequest>,
    ) -> Result<Response<DeleteScalingPolicyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        if inner.name.trim().is_empty() {
            return Err(Status::invalid_argument("name the scaling policy to delete"));
        }
        let (namespace, name) = split_policy_ref(&inner.name, &inner.namespace);
        let id = standalone::policy_id(&namespace, &name);
        // The precise check before the lookup, so a missing policy and a
        // forbidden one read the same to a caller without the grant.
        acl::enforce(&identity, AclResource::Scale, AclAction::Write, &AclScope::job(&id))?;
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "delete_scaling_policy",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(DeleteScalingPolicyReply {
                message,
                deduplicated: true,
                id,
            }));
        }
        let row = self
            .store
            .scaling_policy(&namespace, &name)
            .map_err(|e| map_store_error("Error reading scaling policy", e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "scaling policy {id} not found. Hint: `legion scaling list` shows the policies"
                ))
            })?;
        self.enforce_policy_write(&identity, &row.kind, &id)?;
        let deleted = self
            .store
            .delete_scaling_policy(&namespace, &name)
            .map_err(|e| map_store_error("Error deleting scaling policy", e))?;
        let message = match deleted {
            Some(row) => format!("scaling policy {id}: deleted (was v{})", row.version),
            None => format!("scaling policy {id}: already deleted"),
        };
        info!(policy = %id, accessor_id = identity.accessor_id.as_deref().unwrap_or("-"), "Scaling policy deleted");
        self.store
            .idempotency_record(&idempotency_key, "delete_scaling_policy", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(DeleteScalingPolicyReply {
            message,
            deduplicated: false,
            id,
        }))
    }

    // ACL: scale:read, coarse; entries outside the caller's grant are left
    // out.
    pub(super) async fn list_scaling_policies_rpc(
        &self,
        request: Request<ListScalingPoliciesRequest>,
    ) -> Result<Response<ListScalingPoliciesReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        let selection = crate::structs::namespace::NamespaceSelection::parse(&req.namespaces)
            .map_err(Status::invalid_argument)?;
        let (layers, targets) = super::autoscale_rpcs::load_targets(&self.store)
            .map_err(|e| map_store_error("Error loading scaling policies", e))?;
        let mut policies: Vec<ScalingPolicySummary> = targets
            .iter()
            .filter(|target| {
                selection.matches(crate::structs::namespace::job_namespace(&target.job_id))
            })
            .filter(|target| {
                identity.allows(
                    AclResource::Scale,
                    AclAction::Read,
                    &self.job_read_scope(&target.job_id),
                )
            })
            .map(task_summary)
            .collect();
        let standalone = load_policies(&self.store, &layers)
            .map_err(|e| map_store_error("Error loading scaling policies", e))?;
        policies.extend(
            standalone
                .iter()
                .filter(|policy| selection.matches(&policy.row.namespace))
                .filter(|policy| {
                    identity.allows(AclResource::Scale, AclAction::Read, &AclScope::job(&policy.id()))
                })
                .map(|policy| standalone_summary(policy, req.detail)),
        );
        Ok(Response::new(ListScalingPoliciesReply { policies }))
    }
}
