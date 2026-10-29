//! Where core reads the autoscaler's objects (app-object-kinds plan §8 O2;
//! docs/autoscaling.md, "Standalone policies" and "Node groups").
//!
//! Standalone scaling policies and node groups are objects of the kinds the
//! bundled autoscaler declares, `autoscaler/ScalingPolicy` and
//! `autoscaler/NodeGroup`: written with `ApplyObject`, stored in the generic
//! `app_object` table, read by the app over `WatchObjects`. Core keeps no
//! storage of its own for them. It still reads them, through the generic
//! [`Store::app_objects`] by kind, in four places, each because the answer
//! is core's to give:
//!
//! - **`ApplyScalingDefaults`** refuses a defaults change that would leave a
//!   policy invalid. ScalingPolicies inherit the same layers as tasks, so
//!   the impact check resolves them too.
//! - **`GetScalingStatus`** lists them beside the tasks, with their mode,
//!   bounds, and effective policy (resolved here, as the app resolves them)
//!   joined with the app's last report.
//! - **`RecordScalingAction`** checks an intent's object exists with the
//!   uid it names, so an intent for a deleted object (or a stale replica's,
//!   for a recreated one) is refused.
//! - **`GetScaleHistory` and `PauseScaling`** turn the object a person
//!   names into its uid, the key its history and pause live under.
//!
//! So the policy parser ([`crate::autoscaler::policy`]) stays linked into
//! the manager, as the defaults check needs it; the app uses the same code.
//! Access is the kinds' own (their manifest's `access` and `requires`),
//! through the generic object checks: no autoscaler-specific ACL is left in
//! core.

use super::*;
use crate::autoscaler::defaults::ResolvedPolicy;
use crate::autoscaler::nodes::EffectiveNodeGroup;
use crate::autoscaler::policy::{self as standalone, PolicyKind, StandaloneSpec};
use crate::manager::autoscale::{object_target_key, summary_line, AutoscaleLayers};
use crate::storage::store::{AppObjectRow, ObjectFilter, ObjectKey, ScalingActionRow};
use crate::structs::app_object::{object_document, InstalledKind, ObjectKindRegistry};
use crate::structs::{ScalingAction, ScalingObjectRef, ScalingProvenanceRow};

/// One of the autoscaler's objects, resolved.
pub(super) enum Resolved {
    /// A scaling policy's effective policy.
    External(Box<ResolvedPolicy>),
    /// A node group's, every default filled.
    Nodes(Box<EffectiveNodeGroup>),
}

/// What every surface shows of a resolved object.
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

/// A stored object of one of the autoscaler's kinds, parsed and resolved
/// against the current layers and plugins.
pub(super) struct ScalingObject {
    pub row: AppObjectRow,
    pub kind: PolicyKind,
    pub spec: Result<StandaloneSpec, String>,
    pub resolved: Result<Resolved, String>,
}

impl ScalingObject {
    /// `namespace/name`, or `name` for a node group.
    pub fn id(&self) -> String {
        standalone::object_label(&self.row.key.namespace, &self.row.key.name)
    }

    /// `object:<uid>`: where its history, pause, action row, and status
    /// board entry live.
    pub fn key(&self) -> String {
        object_target_key(self.row.uid)
    }

    /// `autoscaler/ScalingPolicy media/render-farm`.
    pub fn label(&self) -> String {
        format!("{} {}", self.kind.qualified(), self.id())
    }

    pub fn reference(&self) -> ScalingObjectRef {
        object_ref_of(&self.row)
    }

    /// The external policy's resolution (provenance, diffs).
    pub fn external(&self) -> Option<&ResolvedPolicy> {
        match &self.resolved {
            Ok(Resolved::External(resolved)) => Some(resolved.as_ref()),
            _ => None,
        }
    }
}

/// The wire reference of a stored object.
pub(super) fn object_ref_of(row: &AppObjectRow) -> ScalingObjectRef {
    ScalingObjectRef {
        kind: crate::structs::app_object::qualified_kind(&row.key.app, &row.key.kind),
        namespace: row.key.namespace.clone(),
        name: row.key.name.clone(),
        uid: row.uid,
    }
}

/// `autoscaler/ScalingPolicy media/render-farm`, for a wire reference.
pub(super) fn object_ref_label(reference: &ScalingObjectRef) -> String {
    format!(
        "{} {}",
        reference.kind,
        standalone::object_label(&reference.namespace, &reference.name)
    )
}

/// Parse and resolve one stored object. Kind schemas declare no defaults,
/// so the stored spec is the effective one: the layered defaults apply
/// live, as for a task's `autoscale`.
pub(super) fn load_object(row: AppObjectRow, layers: &AutoscaleLayers) -> Option<ScalingObject> {
    let kind = PolicyKind::of_kind(&row.key.kind)?;
    let spec = serde_json::from_str::<Value>(&row.spec)
        .map_err(|err| format!("the stored spec does not parse: {err}"))
        .and_then(|spec| {
            standalone::parse_object(&row.key.kind, &row.key.namespace, &row.key.name, &spec)
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
    Some(ScalingObject {
        row,
        kind,
        spec,
        resolved,
    })
}

/// Every stored object of the autoscaler's kinds, resolved, by kind,
/// namespace, and name.
pub(super) fn load_objects(store: &Store, layers: &AutoscaleLayers) -> R<Vec<ScalingObject>> {
    Ok(store
        .app_objects(&ObjectFilter {
            app: Some(standalone::APP.to_string()),
            ..Default::default()
        })?
        .into_iter()
        .filter_map(|row| load_object(row, layers))
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

/// An object target's action row as the API shows it.
pub(super) fn action_proto(row: &ScalingActionRow) -> ScalingAction {
    ScalingAction {
        target: row.target.clone(),
        version: row.version,
        state: row.state.clone(),
        intent: serde_json::from_str(&row.intent).ok(),
        owner: row.owner.clone(),
        opened_at_unix_ms: row.opened_ms,
        closed_at_unix_ms: row.closed_ms.unwrap_or(0),
        detail: row.detail.clone(),
    }
}

impl ManagerDaemonService {
    /// The installed kind of an autoscaler object, from the registry.
    fn scaling_kind(
        &self,
        registry: &ObjectKindRegistry,
        row: &AppObjectRow,
    ) -> Option<InstalledKind> {
        registry.get(&row.key.app, &row.key.kind).cloned()
    }

    /// Whether `identity` may read a stored autoscaler object: its kind's
    /// access, read, on the object's scope (`scale:read` in its namespace
    /// for a ScalingPolicy, `node:read` for a NodeGroup).
    pub(super) fn may_read_scaling_object(
        &self,
        identity: &AclIdentity,
        registry: &ObjectKindRegistry,
        row: &AppObjectRow,
    ) -> bool {
        self.scaling_kind(registry, row).is_some_and(|kind| {
            identity.allows(
                kind.access,
                AclAction::Read,
                &kind.access_scope(&row.key.namespace, &row.key.name),
            )
        })
    }

    /// The object a person's request names, authorized: reading needs the
    /// kind's read access on the object's scope; writing (a pause) needs
    /// what writing the object needs, its kind's write access and its
    /// `requires`. The kind is resolved and the access checked before the
    /// object is read, so a caller without it cannot tell a missing object
    /// from a forbidden one (AGENTS.md, "Check before you fetch").
    pub(super) fn scaling_object(
        &self,
        identity: &AclIdentity,
        reference: &ScalingObjectRef,
        write: bool,
    ) -> Result<AppObjectRow, Status> {
        let action = if write {
            AclAction::Write
        } else {
            AclAction::Read
        };
        let registry = self.object_registry()?;
        self.gate_objects(identity, &registry, action)?;
        let kind = self.lookup_kind(&registry, &reference.kind)?;
        if kind.app != standalone::APP || PolicyKind::of_kind(&kind.decl.name).is_none() {
            return Err(Status::invalid_argument(format!(
                "{} is not a scaling kind: name an {} or an {}",
                kind.qualified(),
                standalone::SCALING_POLICY,
                standalone::NODE_GROUP
            )));
        }
        let (namespace, name) = self.object_ref(&kind, &reference.name, &reference.namespace)?;
        self.enforce_access(identity, &kind, action, &namespace, &name)?;
        let row = self
            .store
            .app_object(&ObjectKey {
                app: kind.app.clone(),
                kind: kind.decl.name.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
            })
            .map_err(|e| map_store_error("Error reading the object", e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "{} {} not found. Hint: `legion object list {}` lists them",
                    kind.qualified(),
                    standalone::object_label(&namespace, &name),
                    kind.decl.name
                ))
            })?;
        if write {
            let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
            let doc = object_document(&name, &namespace, &row.labels, &spec, &Value::Null);
            self.enforce_requirements(
                identity,
                &kind,
                &doc,
                &namespace,
                &standalone::object_label(&namespace, &name),
            )?;
        }
        Ok(row)
    }

    /// The object an app-plane call names (`ReportScalingStatus`,
    /// `RecordScalingAction`), checked to exist now with the uid it names.
    pub(super) fn current_scaling_object(
        &self,
        reference: &ScalingObjectRef,
    ) -> Result<AppObjectRow, Status> {
        let (app, kind) = reference
            .kind
            .split_once('/')
            .filter(|(app, kind)| *app == standalone::APP && PolicyKind::of_kind(kind).is_some())
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "object kind {:?}: name {} or {}",
                    reference.kind,
                    standalone::SCALING_POLICY,
                    standalone::NODE_GROUP
                ))
            })?;
        let row = self
            .store
            .app_object(&ObjectKey {
                app: app.to_string(),
                kind: kind.to_string(),
                namespace: reference.namespace.clone(),
                name: reference.name.clone(),
            })
            .map_err(|e| map_store_error("Error reading the object", e))?
            .filter(|row| row.uid == reference.uid)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "{} (uid {}) not found: it was deleted, or replaced by a new object of the \
                     same name",
                    object_ref_label(reference),
                    reference.uid
                ))
            })?;
        Ok(row)
    }
}
