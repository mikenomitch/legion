//! Standalone scaling policies' RPCs (docs/autoscaling.md, "Standalone
//! policies" and "Node groups"; autoscaler-v2 plan §6.2, §6.11, §6.13):
//! `ApplyScalingPolicy`, `DeleteScalingPolicy`, `ListScalingPolicies`, and
//! what `WatchScalingPolicies` streams of them.
//!
//! Two kinds share the `scaling_policy` table: an external target's policy
//! (namespaced, `scale:write`), and a node group (cluster-scoped, stored
//! with an empty namespace, `operator:write` and `node:write`). Both are
//! addressed by their canonical id, which for a node group and for an
//! external policy in `default` is the bare name, so the two may not share
//! a name ([`ManagerDaemonService::find_policy`]).

use super::*;
use crate::autoscaler::defaults::ResolvedPolicy;
use crate::autoscaler::nodes::EffectiveNodeGroup;
use crate::autoscaler::policy::{self as standalone, StandaloneSpec};
use crate::manager::autoscale::{policy_target_key, summary_line, AutoscaleLayers, TargetPolicy};
use crate::storage::store::{covering_pause, PauseRow, ScalingActionRow, ScalingPolicyRow};
use crate::structs::{
    ApplyScalingPolicyReply, ApplyScalingPolicyRequest, DeleteScalingPolicyReply,
    DeleteScalingPolicyRequest, ListScalingPoliciesReply, ListScalingPoliciesRequest,
    ScalingAction, ScalingPolicySummary, ScalingProvenanceRow, StandaloneScalingPolicy,
};

/// A standalone policy, resolved.
pub(super) enum Resolved {
    /// An external target's effective policy.
    External(Box<ResolvedPolicy>),
    /// A node group's, every default filled.
    Nodes(Box<EffectiveNodeGroup>),
}

/// What every surface shows of a resolved policy.
pub(super) struct Facts {
    pub mode: String,
    pub min: u32,
    pub max: u32,
    pub digest: String,
    pub policy_json: String,
    pub summary: String,
}

impl Resolved {
    pub fn facts(&self) -> Facts {
        match self {
            Resolved::External(resolved) => Facts {
                mode: resolved.policy.mode.to_string(),
                min: resolved.policy.min,
                max: resolved.policy.max,
                digest: resolved.digest.clone(),
                policy_json: resolved.policy.canonical_json(),
                summary: summary_line(&resolved.policy),
            },
            Resolved::Nodes(group) => Facts {
                mode: group.mode.to_string(),
                min: group.min,
                max: group.max,
                digest: group.digest(),
                policy_json: group.canonical_json(),
                summary: group.summary(),
            },
        }
    }
}

/// A stored standalone policy, parsed and resolved against the current
/// layers and plugins.
pub(super) struct LoadedPolicy {
    pub row: ScalingPolicyRow,
    pub spec: Result<StandaloneSpec, String>,
    pub resolved: Result<Resolved, String>,
}

impl LoadedPolicy {
    pub fn id(&self) -> String {
        self.row.id()
    }

    /// Whether it is a node group.
    pub fn is_nodes(&self) -> bool {
        self.row.kind == "nodes"
    }

    /// The target plugin and its config.
    pub fn target(&self) -> (String, Value) {
        match &self.spec {
            Ok(StandaloneSpec::External(policy)) => {
                (policy.target.plugin.clone(), policy.target.config_value())
            }
            Ok(StandaloneSpec::Nodes(policy)) => (
                policy.target.nodes.plugin.clone(),
                policy
                    .target
                    .nodes
                    .config
                    .clone()
                    .unwrap_or_else(|| json!({})),
            ),
            Err(_) => (String::new(), json!({})),
        }
    }

    /// The external policy's resolution (provenance, diffs).
    pub fn external(&self) -> Option<&ResolvedPolicy> {
        match &self.resolved {
            Ok(Resolved::External(resolved)) => Some(resolved.as_ref()),
            _ => None,
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
        Ok(StandaloneSpec::External(policy)) => layers
            .resolve_standalone(policy)
            .map(|resolved| Resolved::External(Box::new(resolved))),
        Ok(StandaloneSpec::Nodes(policy)) => layers
            .resolve_node_group(policy)
            .map(|group| Resolved::Nodes(Box::new(group))),
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

/// A policy's action row as the API shows it.
pub(super) fn action_proto(row: &ScalingActionRow) -> ScalingAction {
    ScalingAction {
        policy: row.policy.clone(),
        version: row.version,
        state: row.state.clone(),
        intent: serde_json::from_str(&row.intent).ok(),
        owner: row.owner.clone(),
        opened_at_unix_ms: row.opened_ms,
        closed_at_unix_ms: row.closed_ms.unwrap_or(0),
        detail: row.detail.clone(),
    }
}

/// The proto a standalone policy streams as.
pub(super) fn standalone_proto(
    policy: &LoadedPolicy,
    pauses: &[PauseRow],
    actions: &[ScalingActionRow],
    now_ms: i64,
) -> StandaloneScalingPolicy {
    let id = policy.id();
    let (plugin, config) = policy.target();
    let (state, error, digest, policy_json, summary) = match &policy.resolved {
        Ok(resolved) => {
            let facts = resolved.facts();
            (
                "ok".to_string(),
                String::new(),
                facts.digest,
                facts.policy_json,
                facts.summary,
            )
        }
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
        action: actions
            .iter()
            .find(|action| action.policy == id)
            .map(action_proto),
    }
}

fn task_summary(target: &TargetPolicy) -> ScalingPolicySummary {
    let namespace = crate::structs::namespace::job_namespace(&target.job_id).to_string();
    let kind = if target.members.len() > 1 {
        "group"
    } else {
        "task"
    };
    let (state, error, mode, min, max, summary) = match &target.resolved {
        Ok(resolved) => (
            "ok".to_string(),
            String::new(),
            resolved.policy.mode.to_string(),
            resolved.policy.min,
            resolved.policy.max,
            summary_line(&resolved.policy),
        ),
        Err(err) => (
            "invalid".to_string(),
            err.clone(),
            String::new(),
            0,
            0,
            String::new(),
        ),
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
        target: format!("job {}", target.job_id),
        version: 0,
        summary,
        ..Default::default()
    }
}

fn standalone_summary(policy: &LoadedPolicy, detail: bool) -> ScalingPolicySummary {
    let (plugin, _) = policy.target();
    let (state, error, facts) = match &policy.resolved {
        Ok(resolved) => ("ok".to_string(), String::new(), Some(resolved.facts())),
        Err(err) => ("invalid".to_string(), err.clone(), None),
    };
    ScalingPolicySummary {
        id: policy.id(),
        kind: policy.row.kind.clone(),
        namespace: policy.row.namespace.clone(),
        mode: facts.as_ref().map(|f| f.mode.clone()).unwrap_or_default(),
        min: facts.as_ref().map_or(0, |f| f.min),
        max: facts.as_ref().map_or(0, |f| f.max),
        state,
        error,
        target: if plugin.is_empty() {
            "-".to_string()
        } else if policy.is_nodes() {
            format!("nodes via {plugin}")
        } else {
            format!("plugin {plugin}")
        },
        version: policy.row.version,
        summary: facts.map(|f| f.summary).unwrap_or_default(),
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
    pub(super) fn enforce_policy_write(
        &self,
        identity: &AclIdentity,
        kind: &str,
        id: &str,
    ) -> Result<(), Status> {
        if kind == "nodes" {
            acl::enforce(
                identity,
                AclResource::Operator,
                AclAction::Write,
                &AclScope::none(),
            )?;
            acl::enforce(
                identity,
                AclResource::Node,
                AclAction::Write,
                &AclScope::any(),
            )
        } else {
            acl::enforce(
                identity,
                AclResource::Scale,
                AclAction::Write,
                &AclScope::job(id),
            )
        }
    }

    /// Whether `identity` may write node groups (`operator:write` and
    /// `node:write`).
    pub(super) fn may_write_node_groups(&self, identity: &AclIdentity) -> bool {
        identity.allows(AclResource::Operator, AclAction::Write, &AclScope::none())
            && identity.allows(AclResource::Node, AclAction::Write, &AclScope::any())
    }

    /// Whether `identity` may read a standalone policy of `kind`: a node
    /// group with `node:read`, an external policy with `scale:read` on it.
    pub(super) fn may_read_policy(&self, identity: &AclIdentity, kind: &str, id: &str) -> bool {
        if kind == "nodes" {
            identity.allows(AclResource::Node, AclAction::Read, &AclScope::any())
        } else {
            identity.allows(AclResource::Scale, AclAction::Read, &AclScope::job(id))
        }
    }

    /// The gate before a policy named `id` is looked up, when its kind is
    /// not known yet: the caller may write (or read) it as an external
    /// policy or as a node group. Nothing is read first, so a caller with
    /// neither cannot tell a missing policy from a forbidden one.
    pub(super) fn gate_policy_ref(
        &self,
        identity: &AclIdentity,
        id: &str,
        write: bool,
    ) -> Result<(), Status> {
        let action = if write {
            AclAction::Write
        } else {
            AclAction::Read
        };
        let external = identity.allows(AclResource::Scale, action, &AclScope::job(id));
        let nodes = if write {
            self.may_write_node_groups(identity)
        } else {
            identity.allows(AclResource::Node, AclAction::Read, &AclScope::any())
        };
        if external || nodes {
            return Ok(());
        }
        acl::enforce(identity, AclResource::Scale, action, &AclScope::job(id))
    }

    /// A standalone policy by reference: `namespace/name`, or a bare name,
    /// which is the `default` namespace's policy or else a node group.
    /// Node groups are looked for only when `nodes` (the caller may see
    /// them), so a caller without that grant learns nothing about them.
    pub(super) fn find_policy(
        &self,
        reference: &str,
        namespace: &str,
        nodes: bool,
    ) -> Result<Option<ScalingPolicyRow>, Status> {
        let reference = reference.trim();
        let explicit = reference.contains('/') || !namespace.trim().is_empty();
        let (namespace, name) = split_policy_ref(reference, namespace);
        if let Some(row) = self
            .store
            .scaling_policy(&namespace, &name)
            .map_err(|e| map_store_error("Error reading scaling policy", e))?
        {
            return Ok(Some(row));
        }
        if nodes && !explicit {
            return self
                .store
                .scaling_policy("", &name)
                .map_err(|e| map_store_error("Error reading scaling policy", e));
        }
        Ok(None)
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
        let parsed = standalone::parse(&spec_value, &default_namespace).map_err(|err| {
            Status::invalid_argument(format!("Error validating scaling policy: {err}"))
        })?;
        let id = parsed.id();
        let kind = parsed.kind().as_str();
        self.enforce_policy_write(&identity, kind, &id)?;

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
                kind: kind.to_string(),
                message,
                deduplicated: true,
                ..Default::default()
            }));
        }
        let (namespace, description) = match &parsed {
            StandaloneSpec::External(policy) => {
                self.ensure_namespace_exists(&policy.namespace)?;
                (policy.namespace.clone(), policy.description.clone())
            }
            StandaloneSpec::Nodes(_) => (String::new(), String::new()),
        };
        let name = parsed.name().to_string();
        // A bare name addresses a node group and a `default` policy alike:
        // the two may not share one.
        let other = match &parsed {
            StandaloneSpec::Nodes(_) => Some(crate::structs::namespace::DEFAULT_NAMESPACE),
            StandaloneSpec::External(policy)
                if policy.namespace == crate::structs::namespace::DEFAULT_NAMESPACE =>
            {
                Some("")
            }
            StandaloneSpec::External(_) => None,
        };
        if let Some(other) = other {
            if self
                .store
                .scaling_policy(other, &name)
                .map_err(|e| map_store_error("Error reading scaling policy", e))?
                .is_some()
            {
                return Err(Status::already_exists(format!(
                    "{name}: a {} named {name} exists, and both are addressed by the bare name. \
                     Hint: pick another name",
                    if other.is_empty() {
                        "node group"
                    } else {
                        "scaling policy in namespace default"
                    }
                )));
            }
        }
        let layers = AutoscaleLayers::load(&self.store)
            .map_err(|e| map_store_error("Error loading scaling defaults", e))?;
        let resolved = match &parsed {
            StandaloneSpec::External(policy) => layers
                .resolve_standalone(policy)
                .map(|resolved| Resolved::External(Box::new(resolved))),
            StandaloneSpec::Nodes(policy) => layers
                .resolve_node_group(policy)
                .map(|group| Resolved::Nodes(Box::new(group))),
        }
        .map_err(|err| {
            Status::invalid_argument(format!("Error validating scaling policy: {err}"))
        })?;
        let current = self
            .store
            .scaling_policy(&namespace, &name)
            .map_err(|e| map_store_error("Error reading scaling policy", e))?;
        let canonical = serde_json::to_string(&spec_value)
            .map_err(|err| Status::internal(format!("encoding the policy: {err}")))?;
        let diff = match &current {
            Some(row) => match (load_policy(row.clone(), &layers).resolved, &resolved) {
                (Ok(Resolved::External(before)), Resolved::External(after)) => {
                    super::autoscale_rpcs::changed_field_values(&before, after)
                }
                (Ok(Resolved::Nodes(before)), Resolved::Nodes(after)) => {
                    crate::autoscaler::nodes::changed_fields(&before, after)
                }
                (Ok(_), _) => vec!["kind: changed".to_string()],
                (Err(err), _) => vec![format!("was invalid: {err}")],
            },
            None => Vec::new(),
        };
        let facts = resolved.facts();
        let provenance = match &resolved {
            Resolved::External(resolved) => provenance_rows(resolved),
            Resolved::Nodes(_) => Vec::new(),
        };
        let reply =
            |version: u64, created: bool, changed: bool, message: String| ApplyScalingPolicyReply {
                id: id.clone(),
                kind: kind.to_string(),
                version,
                created,
                changed,
                diff: diff.clone(),
                message,
                deduplicated: false,
                policy_json: facts.policy_json.clone(),
                digest: facts.digest.clone(),
                provenance: provenance.clone(),
                dry_run: inner.dry_run,
            };
        if inner.dry_run {
            let unchanged = current
                .as_ref()
                .is_some_and(|row| row.spec == canonical && row.description == description);
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
                &namespace,
                &name,
                kind,
                &canonical,
                &description,
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
            kind,
            version = put.version,
            created = put.created,
            changed = put.changed,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Scaling policy applied"
        );
        self.store
            .idempotency_record(&idempotency_key, "apply_scaling_policy", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(reply(
            put.version,
            put.created,
            put.changed,
            message,
        )))
    }

    // ACL: before the lookup, scale:write on the policy's namespace and name
    // or (for a node group) operator:write and node:write; after it, the
    // grant the policy's kind needs. Node groups are looked up only for a
    // caller who may write them, so a caller cannot tell an existing
    // policy from a missing one.
    #[instrument(skip_all)]
    pub(super) async fn delete_scaling_policy_rpc(
        &self,
        request: Request<DeleteScalingPolicyRequest>,
    ) -> Result<Response<DeleteScalingPolicyReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        if inner.name.trim().is_empty() {
            return Err(Status::invalid_argument(
                "name the scaling policy to delete",
            ));
        }
        let (namespace, name) = split_policy_ref(&inner.name, &inner.namespace);
        let id = standalone::policy_id(&namespace, &name);
        self.gate_policy_ref(&identity, &id, true)?;
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
            .find_policy(
                &inner.name,
                &inner.namespace,
                self.may_write_node_groups(&identity),
            )?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "scaling policy {id} not found. Hint: `legion scaling list` shows the policies"
                ))
            })?;
        let id = row.id();
        self.enforce_policy_write(&identity, &row.kind, &id)?;
        let deleted = self
            .store
            .delete_scaling_policy(&row.namespace, &row.name)
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
    // out (node groups need node:read, and are listed whatever namespaces
    // are asked for: they have none).
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
                .filter(|policy| policy.is_nodes() || selection.matches(&policy.row.namespace))
                .filter(|policy| self.may_read_policy(&identity, &policy.row.kind, &policy.id()))
                .map(|policy| standalone_summary(policy, req.detail)),
        );
        Ok(Response::new(ListScalingPoliciesReply { policies }))
    }
}
