//! Objects of app-declared kinds on the manager (docs/app-objects.md;
//! app-object-kinds plan §6): the registry of installed kinds, the ACL every
//! read and write runs (the kind's `access` resource on the object's scope,
//! plus `writeRequires` on writes), the app's fail-open validate call, and
//! the seven RPCs: `ListObjectKinds`, `ApplyObject`, `GetObject`,
//! `ListObjects`, `DeleteObject`, and the app-plane `WatchObjects` and
//! `UpdateObjectStatus`.

use super::*;
use crate::storage::store::{
    AppObjectRow, DeleteObject, ObjectFilter, ObjectKey, ObjectWrite, PutObject, PutStatus,
    StatusCas, VersionCas,
};
use crate::structs::app::AppRecord;
use crate::structs::app_object::{
    judge, object_id, object_shard, object_validate_context, parse_object_input, qualified_kind,
    same_noun, spec_diff, validate_labels, validate_object_name, InstalledKind, KindLookup,
    ObjectKindRegistry, ObjectState, ObjectValidateAnswer, ObjectValidateCall,
    MAX_OBJECT_DOC_BYTES, OBJECT_VALIDATE_NAME, OBJECT_VALIDATE_PATH,
};
use crate::structs::app_ui::{inputs_digest, AssertionKind, AssertionScope, ViewerAssertion};
use crate::structs::extensions::VALIDATE_TIMEOUT;
use crate::structs::namespace::DEFAULT_NAMESPACE;
use crate::structs::{
    AppObject, ApplyObjectReply, ApplyObjectRequest, DeleteObjectReply, DeleteObjectRequest,
    GetObjectReply, GetObjectRequest, ListObjectKindsReply, ListObjectKindsRequest,
    ListObjectsReply, ListObjectsRequest, ObjectEvent, ObjectKind, ObjectKindColumn, ObjectRef,
    UpdateObjectStatusReply, UpdateObjectStatusRequest, WatchObjectsRequest,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio_stream::wrappers::ReceiverStream;

/// How often `WatchObjects` checks for changes.
const WATCH_POLL: Duration = Duration::from_secs(1);
/// How often it re-reads every object even when the fingerprints say
/// nothing changed.
const WATCH_FULL_READ: Duration = Duration::from_secs(30);

/// The kind registry cache: the apps' fingerprint it was built for, and the
/// registry.
pub(crate) type ObjectRegistryCache = Arc<Mutex<Option<(String, Arc<ObjectKindRegistry>)>>>;

fn now_ms() -> i64 {
    super::autoscale_rpcs::now_unix_ms()
}

fn json_text(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// A kind a request names: installed, or orphaned (objects stored under an
/// app that no longer declares it).
#[derive(Debug, Clone)]
pub(crate) enum ResolvedKind {
    Installed(Box<InstalledKind>),
    Orphaned { app: String, kind: String },
}

impl ResolvedKind {
    fn app(&self) -> &str {
        match self {
            ResolvedKind::Installed(kind) => &kind.app,
            ResolvedKind::Orphaned { app, .. } => app,
        }
    }

    fn name(&self) -> &str {
        match self {
            ResolvedKind::Installed(kind) => &kind.decl.name,
            ResolvedKind::Orphaned { kind, .. } => kind,
        }
    }

    fn qualified(&self) -> String {
        qualified_kind(self.app(), self.name())
    }

    /// The resource that governs the objects: the kind's `access`, or
    /// `object` for an orphan (app-object-kinds calls entry 5).
    fn access(&self) -> AclResource {
        match self {
            ResolvedKind::Installed(kind) => kind.access,
            ResolvedKind::Orphaned { .. } => AclResource::Object,
        }
    }

    fn installed(&self) -> Option<&InstalledKind> {
        match self {
            ResolvedKind::Installed(kind) => Some(kind),
            ResolvedKind::Orphaned { .. } => None,
        }
    }

    /// Whether objects live in a namespace. An orphan's scope is unknown
    /// until a row is read; requests about one say which they mean by
    /// naming a namespace or not.
    fn namespaced(&self) -> Option<bool> {
        self.installed().map(InstalledKind::is_namespaced)
    }
}

/// The ACL scope of an object for `resource`: its namespace (`*` for a
/// cluster kind's object) and name for a namespaced resource, its name for
/// a cluster-scoped one; always the `<app>/<Kind>` attribute.
fn object_scope(resource: AclResource, qualified: &str, namespace: &str, name: &str) -> AclScope {
    if resource.is_namespaced() {
        AclScope::object(qualified, namespace, name, true)
    } else {
        AclScope::object(qualified, "", name, false)
    }
}

/// The proto of an installed kind.
pub(crate) fn kind_proto(kind: &InstalledKind) -> ObjectKind {
    ObjectKind {
        name: kind.decl.name.clone(),
        app: kind.app.clone(),
        app_version: kind.app_version.clone(),
        qualified: kind.qualified(),
        description: kind.decl.description.clone(),
        scope: kind.decl.scope.as_str().to_string(),
        schema_json: json_text(&kind.decl.schema),
        status_schema_json: kind
            .decl
            .status
            .as_ref()
            .map(|status| json_text(&status.schema))
            .unwrap_or_default(),
        examples_json: json_text(&Value::Array(kind.decl.examples.clone())),
        validate: kind.decl.validate,
        access: kind.access.as_str().to_string(),
        write_requires: kind
            .decl
            .write_requires
            .iter()
            .map(|permission| permission.label())
            .collect(),
        columns: kind
            .decl
            .columns
            .iter()
            .map(|column| ObjectKindColumn {
                name: column.header(),
                pointer: column.pointer().to_string(),
            })
            .collect(),
        app_enabled: kind.enabled,
    }
}

/// The proto of a stored object, judged against `kind` (`None`: orphaned).
pub(crate) fn object_proto(
    row: &AppObjectRow,
    kind: Option<&InstalledKind>,
    shard: u32,
) -> AppObject {
    let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
    let (effective, state, detail) = judge(kind, &spec);
    AppObject {
        app: row.key.app.clone(),
        kind: row.key.kind.clone(),
        namespace: row.key.namespace.clone(),
        name: row.key.name.clone(),
        id: object_id(&row.key.namespace, &row.key.name),
        labels: row.labels.clone().into_iter().collect(),
        spec_json: row.spec.clone(),
        effective_json: json_text(&effective),
        status_json: row.status.clone(),
        observed_generation: row.observed_generation,
        generation: row.generation,
        version: row.version,
        state: state.as_str().to_string(),
        detail,
        created_by: row.created_by.clone(),
        created_at_unix_ms: row.created_ms,
        updated_by: row.updated_by.clone(),
        updated_at_unix_ms: row.updated_ms,
        status_updated_by: row.status_updated_by.clone(),
        status_updated_at_unix_ms: row.status_updated_ms,
        validated_by: row.validated_by.clone(),
        shard,
    }
}

/// `name`, or `namespace/name`, against a default namespace (empty means
/// `default`): the namespace and the bare name.
fn split_object_ref(name: &str, namespace: &str) -> (String, String) {
    let name = name.trim();
    match name.split_once('/') {
        Some((namespace, name)) => (namespace.to_string(), name.to_string()),
        None => {
            let namespace = match namespace.trim() {
                "" => DEFAULT_NAMESPACE,
                namespace => namespace,
            };
            (namespace.to_string(), name.to_string())
        }
    }
}

/// What installing or upgrading `app` to `next` does to its stored objects,
/// for the prompt: per kind, how many objects, and which fail the new
/// schema (flagged invalid); with `installing`, how many orphaned objects it
/// adopts.
pub(crate) fn objects_report(
    app: &str,
    rows: &[AppObjectRow],
    next: &ObjectKindRegistry,
    installing: bool,
) -> Vec<String> {
    let mut by_kind: BTreeMap<String, (usize, Vec<String>)> = BTreeMap::new();
    let mut orphaned: BTreeMap<String, usize> = BTreeMap::new();
    for row in rows.iter().filter(|row| row.key.app == app) {
        let Some(kind) = next.get(app, &row.key.kind) else {
            *orphaned.entry(row.key.kind.clone()).or_default() += 1;
            continue;
        };
        let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
        let entry = by_kind.entry(row.key.kind.clone()).or_default();
        entry.0 += 1;
        if let Err(errors) = kind.check_spec(&spec) {
            entry.1.push(format!(
                "  {}: {}",
                object_id(&row.key.namespace, &row.key.name),
                errors.join("; ")
            ));
        }
    }
    let mut lines = Vec::new();
    for (kind, (count, failing)) in by_kind {
        let noun = if count == 1 { "object" } else { "objects" };
        let adopted = if installing {
            " (orphaned by an earlier install; adopted again)"
        } else {
            ""
        };
        if failing.is_empty() {
            lines.push(format!(
                "{count} {kind} {noun}{adopted}; every one passes this version's schema."
            ));
        } else {
            lines.push(format!(
                "{count} {kind} {noun}{adopted}; {} fail{} this version's schema (flagged \
                 invalid; each must conform at its next apply):",
                failing.len(),
                if failing.len() == 1 { "s" } else { "" }
            ));
            lines.extend(failing);
        }
    }
    for (kind, count) in orphaned {
        if installing {
            continue;
        }
        lines.push(format!(
            "{count} {kind} object{} (this version no longer declares {kind}; they are left \
             orphaned: readable and deletable, not writable)",
            if count == 1 { "" } else { "s" }
        ));
    }
    lines
}

impl ManagerDaemonService {
    /// The installed kinds, rebuilt only when an app record changed.
    pub(crate) fn object_registry(&self) -> Result<Arc<ObjectKindRegistry>, Status> {
        cached_object_registry(&self.store, &self.object_registry, true)
    }

    /// One record's kinds as a registry of their own (`App.kinds`).
    pub(crate) fn record_kinds(&self, record: &AppRecord) -> Vec<ObjectKind> {
        let (registry, _) = ObjectKindRegistry::from_records(std::slice::from_ref(record));
        registry.kinds().iter().map(kind_proto).collect()
    }

    /// Every stored object of `app`.
    pub(crate) fn app_object_rows(&self, app: &str) -> Result<Vec<AppObjectRow>, Status> {
        self.store
            .app_objects(&ObjectFilter {
                app: Some(app.to_string()),
                kind: None,
            })
            .map_err(|e| map_store_error("Error reading objects", e))
    }

    /// How many objects each of `app`'s kinds holds.
    pub(crate) fn app_object_counts(&self, app: &str) -> Result<BTreeMap<String, u64>, Status> {
        Ok(self
            .store
            .app_object_counts()
            .map_err(|e| map_store_error("Error counting objects", e))?
            .into_iter()
            .filter(|((owner, _), _)| owner == app)
            .map(|((_, kind), count)| (kind, count))
            .collect())
    }

    /// The kind `typed` names: an installed one, else one whose objects
    /// are stored under an app that no longer declares it (reached by its
    /// short name only when no installed kind takes it).
    pub(crate) fn resolve_kind(
        &self,
        registry: &ObjectKindRegistry,
        typed: &str,
    ) -> Result<ResolvedKind, Status> {
        let typed = typed.trim();
        if typed.is_empty() {
            return Err(Status::invalid_argument(
                "name a kind (`legion object kinds` lists them)",
            ));
        }
        let lookup = match registry.resolve(typed) {
            Ok(kind) => return Ok(ResolvedKind::Installed(Box::new(kind.clone()))),
            Err(lookup) => lookup,
        };
        let stored = self
            .store
            .app_object_counts()
            .map_err(|e| map_store_error("Error reading objects", e))?;
        let orphan = stored.keys().find(|(app, kind)| {
            registry.get(app, kind).is_none()
                && match typed.split_once('/') {
                    Some((typed_app, typed_kind)) => {
                        typed_app == app && same_noun(typed_kind, kind)
                    }
                    None => same_noun(typed, kind),
                }
        });
        if let Some((app, kind)) = orphan {
            return Ok(ResolvedKind::Orphaned {
                app: app.clone(),
                kind: kind.clone(),
            });
        }
        Err(Status::invalid_argument(match lookup {
            KindLookup::Unknown { suggestion } => format!(
                "unknown kind {typed:?}{}\nHint: `legion object kinds` lists the kinds installed \
                 apps declare",
                suggestion
                    .map(|name| format!(" (did you mean {name}?)"))
                    .unwrap_or_default()
            ),
            KindLookup::NotDeclared { app } => format!(
                "app {app} declares no kind {:?}\nHint: `legion object kinds --app {app}` lists its \
                 kinds",
                typed.split_once('/').map(|(_, kind)| kind).unwrap_or(typed)
            ),
        }))
    }

    /// `action` on the kind's access resource on the object's scope; for a
    /// write, then each of the kind's `writeRequires` there.
    fn enforce_object(
        &self,
        identity: &AclIdentity,
        kind: &ResolvedKind,
        action: AclAction,
        namespace: &str,
        name: &str,
    ) -> Result<(), Status> {
        let qualified = kind.qualified();
        let resource = kind.access();
        acl::enforce(
            identity,
            resource,
            action,
            &object_scope(resource, &qualified, namespace, name),
        )?;
        if action == AclAction::Write {
            if let Some(installed) = kind.installed() {
                for (resource, action) in &installed.write_requires {
                    if acl::enforce(
                        identity,
                        *resource,
                        *action,
                        &object_scope(*resource, &qualified, namespace, name),
                    )
                    .is_err()
                    {
                        return Err(Status::permission_denied(format!(
                            "writing a {} ({}) also needs {}:{} on {}, which your token does not \
                             hold\nHint: the kind declares it in `writeRequires`; have someone who \
                             holds it make the change",
                            installed.decl.name,
                            installed.app,
                            resource.as_str(),
                            action.as_str(),
                            if namespace.is_empty() || !resource.is_namespaced() {
                                name.to_string()
                            } else {
                                format!("{namespace}/{name}")
                            }
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether `identity` may read `row`, by its kind's resource (the
    /// `object` resource for an orphan).
    fn may_read_row(
        &self,
        identity: &AclIdentity,
        registry: &ObjectKindRegistry,
        row: &AppObjectRow,
    ) -> bool {
        let qualified = qualified_kind(&row.key.app, &row.key.kind);
        let resource = registry
            .get(&row.key.app, &row.key.kind)
            .map(|kind| kind.access)
            .unwrap_or(AclResource::Object);
        identity.allows(
            resource,
            AclAction::Read,
            &object_scope(resource, &qualified, &row.key.namespace, &row.key.name),
        )
    }

    /// Ask the app whether it accepts an object (app-object-kinds plan
    /// §6.6): `POST /legion/v1/objects/validate` to one allocation, with
    /// the writer's viewer assertion. `Err` says why the app could not
    /// answer, which lets the write through with a warning.
    async fn object_validate_call(
        &self,
        identity: &AclIdentity,
        kind: &InstalledKind,
        call: &ObjectValidateCall,
    ) -> Result<ObjectValidateAnswer, String> {
        let record: AppRecord = self
            .store
            .get_app(&kind.app)
            .ok()
            .flatten()
            .ok_or_else(|| "not installed".to_string())?;
        if !record.enabled {
            return Err("disabled".into());
        }
        let ui = record
            .manifest
            .ui_spec()
            .map_err(|err| format!("its manifest's ui block is invalid: {err}"))?;
        let body = serde_json::to_value(call).map_err(|err| err.to_string())?;
        let assertion = ViewerAssertion {
            kind: AssertionKind::Validate,
            app: kind.app.clone(),
            name: OBJECT_VALIDATE_NAME.to_string(),
            principal: identity.principal.clone(),
            accessor: identity.accessor_id.clone(),
            permission: None,
            scope: AssertionScope::None,
            namespaces: Vec::new(),
            context: object_validate_context(&call.namespace),
            inputs: Some(inputs_digest(&body)),
            delegation: None,
        };
        let token = self
            .sign_viewer_assertion(identity, &assertion)
            .map_err(|status| status.message().to_string())?;
        let bytes = serde_json::to_vec(&body).map_err(|err| err.to_string())?;
        let (reply, _) = self
            .forward_to_app(
                &record,
                &ui,
                "POST",
                OBJECT_VALIDATE_PATH.to_string(),
                token,
                bytes,
                "application/json",
            )
            .await
            .map_err(|status| status.message().to_string())?;
        if reply.status != 200 {
            return Err(format!(
                "it answered HTTP {}: {}",
                reply.status,
                String::from_utf8_lossy(&reply.body)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ));
        }
        serde_json::from_slice(&reply.body)
            .map_err(|err| format!("its answer is not an object validate reply: {err}"))
    }

    // ACL: per kind: `read` on the kind's access resource somewhere (on the
    // kind, for `object`); denied (object:read) to a caller that may read
    // no kind at all.
    pub(super) async fn list_object_kinds_rpc(
        &self,
        request: Request<ListObjectKindsRequest>,
    ) -> Result<Response<ListObjectKindsReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let registry = self.object_registry()?;
        let app = request.get_ref().app.trim().to_string();
        let readable: Vec<&InstalledKind> = registry
            .kinds()
            .iter()
            .filter(|kind| {
                identity.allows_kind_somewhere(kind.access, AclAction::Read, &kind.qualified())
            })
            .collect();
        if readable.is_empty()
            && !identity.allows(AclResource::Object, AclAction::Read, &AclScope::any())
        {
            acl::enforce(
                &identity,
                AclResource::Object,
                AclAction::Read,
                &AclScope::any(),
            )?;
        }
        Ok(Response::new(ListObjectKindsReply {
            kinds: readable
                .into_iter()
                .filter(|kind| app.is_empty() || kind.app == app)
                .map(kind_proto)
                .collect(),
        }))
    }

    // ACL: two-step: the kind (public metadata) is resolved from the body,
    // then `write` on its access resource and each `writeRequires` on the
    // object's scope, before the object is read.
    #[instrument(skip_all, fields(kind, object))]
    pub(super) async fn apply_object_rpc(
        &self,
        request: Request<ApplyObjectRequest>,
    ) -> Result<Response<ApplyObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        if inner.object_json.len() > MAX_OBJECT_DOC_BYTES * 2 {
            return Err(Status::invalid_argument(format!(
                "the object is {} bytes; a spec is at most {MAX_OBJECT_DOC_BYTES}",
                inner.object_json.len()
            )));
        }
        let value: Value = serde_json::from_str(&inner.object_json)
            .map_err(|err| Status::invalid_argument(format!("the object is not JSON: {err}")))?;
        let input = parse_object_input(&value).map_err(Status::invalid_argument)?;
        let registry = self.object_registry()?;
        let kind = match self.resolve_kind(&registry, &input.kind)? {
            ResolvedKind::Installed(kind) => *kind,
            ResolvedKind::Orphaned { app, kind } => {
                return Err(Status::failed_precondition(format!(
                    "kind {kind} is orphaned: app {app} no longer declares it, so its objects \
                     can be read and deleted but not written\nHint: install (or upgrade) {app} \
                     with the kind to write them again"
                )))
            }
        };
        let mut warnings = Vec::new();
        let namespace = if kind.is_namespaced() {
            let namespace = input
                .namespace
                .clone()
                .map(|namespace| namespace.trim().to_string())
                .filter(|namespace| !namespace.is_empty())
                .or_else(|| {
                    Some(inner.namespace.trim().to_string())
                        .filter(|namespace| !namespace.is_empty())
                })
                .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());
            crate::structs::namespace::validate_namespace_name(&namespace)
                .map_err(Status::invalid_argument)?;
            namespace
        } else {
            if input.namespace.is_some() {
                return Err(Status::invalid_argument(format!(
                    "{} is cluster-scoped: its objects have no namespace; remove `namespace` from \
                     {}",
                    kind.decl.name, input.name
                )));
            }
            if !inner.namespace.trim().is_empty() {
                warnings.push(format!(
                    "{} is cluster-scoped: namespace {} ignored",
                    kind.decl.name,
                    inner.namespace.trim()
                ));
            }
            String::new()
        };
        let name = input.name.trim().to_string();
        validate_object_name(&name).map_err(Status::invalid_argument)?;
        let id = object_id(&namespace, &name);
        tracing::Span::current().record("kind", kind.qualified().as_str());
        tracing::Span::current().record("object", id.as_str());
        let resolved = ResolvedKind::Installed(Box::new(kind.clone()));
        self.enforce_object(&identity, &resolved, AclAction::Write, &namespace, &name)?;

        validate_labels(&input.labels).map_err(Status::invalid_argument)?;
        let spec_text = serde_json::to_string(&input.spec)
            .map_err(|err| Status::internal(format!("encoding the spec: {err}")))?;
        if spec_text.len() > MAX_OBJECT_DOC_BYTES {
            return Err(Status::invalid_argument(format!(
                "{} {id}: the spec is {} bytes; the limit is {MAX_OBJECT_DOC_BYTES}",
                kind.decl.name,
                spec_text.len()
            )));
        }
        if kind.is_namespaced() {
            self.ensure_namespace_exists(&namespace)?;
        }
        let effective = kind.check_spec(&input.spec).map_err(|errors| {
            Status::invalid_argument(format!(
                "{} {id}: {}\nHint: `legion object kinds -j` shows the kind's schema",
                kind.decl.name,
                errors.join("; ")
            ))
        })?;

        let idempotency_key = if inner.dry_run {
            String::new()
        } else {
            scoped_idempotency_key(
                &identity,
                "apply_object",
                inner.idempotency_key.as_deref(),
                &inner,
            )
        };
        let key = ObjectKey {
            app: kind.app.clone(),
            kind: kind.decl.name.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
        };
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let current = self
                .store
                .app_object(&key)
                .map_err(|e| map_store_error("Error reading the object", e))?;
            return Ok(Response::new(ApplyObjectReply {
                object: current.map(|row| object_proto(&row, Some(&kind), 0)),
                message,
                deduplicated: true,
                ..Default::default()
            }));
        }
        let current = self
            .store
            .app_object(&key)
            .map_err(|e| map_store_error("Error reading the object", e))?;
        let cas = VersionCas::from_expected(inner.expected_version);
        let conflict = |current: Option<&AppObjectRow>| -> Status {
            match current {
                Some(row) => Status::aborted(format!(
                    "{} {id} is at version {}, not {}: it changed since you read it\nHint: read \
                     it again (legion object get {} {}) and apply on top of it",
                    kind.decl.name,
                    row.version,
                    inner.expected_version.unwrap_or_default(),
                    kind.decl.name.to_lowercase(),
                    id
                )),
                None => Status::aborted(format!(
                    "{} {id} does not exist, and expected_version {} names a stored version\n\
                     Hint: pass 0 (or no expected version) to create it",
                    kind.decl.name,
                    inner.expected_version.unwrap_or_default()
                )),
            }
        };
        let cas_holds = match (cas, current.as_ref()) {
            (VersionCas::Any, _) => true,
            (VersionCas::Missing, current) => current.is_none(),
            (VersionCas::Version(version), Some(row)) => row.version == version,
            (VersionCas::Version(_), None) => false,
        };
        if !cas_holds {
            if matches!(cas, VersionCas::Missing) {
                return Err(Status::aborted(format!(
                    "{} {id} already exists (version {}); expected_version 0 creates only",
                    kind.decl.name,
                    current.as_ref().map(|row| row.version).unwrap_or_default()
                )));
            }
            return Err(conflict(current.as_ref()));
        }
        let previous_spec: Value = current
            .as_ref()
            .and_then(|row| serde_json::from_str(&row.spec).ok())
            .unwrap_or(Value::Null);
        let previous_labels: BTreeMap<String, String> = current
            .as_ref()
            .map(|row| row.labels.clone())
            .unwrap_or_default();
        let spec_changed = current.as_ref().is_none_or(|row| row.spec != spec_text);
        let labels_changed = current
            .as_ref()
            .is_none_or(|row| row.labels != input.labels);
        let mut diff = Vec::new();
        if current.is_some() {
            diff.extend(spec_diff("spec", &previous_spec, &input.spec));
            diff.extend(spec_diff(
                "labels",
                &serde_json::to_value(&previous_labels).unwrap_or_default(),
                &serde_json::to_value(&input.labels).unwrap_or_default(),
            ));
        }

        // The app's own checks, when the kind asks for them and the spec is
        // new or changed (app-object-kinds plan §6.6). Fails open.
        if kind.decl.validate && spec_changed {
            let call = ObjectValidateCall {
                app: kind.app.clone(),
                kind: kind.decl.name.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                operation: if current.is_some() {
                    "update"
                } else {
                    "create"
                }
                .to_string(),
                spec: input.spec.clone(),
                effective: effective.clone(),
                previous: previous_spec.clone(),
                labels: input.labels.clone(),
            };
            let outcome = tokio::time::timeout(
                VALIDATE_TIMEOUT,
                self.object_validate_call(&identity, &kind, &call),
            )
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "did not answer within {}s",
                    VALIDATE_TIMEOUT.as_secs()
                ))
            });
            match outcome {
                Ok(answer) => {
                    if !answer.errors.is_empty() {
                        return Err(Status::invalid_argument(format!(
                            "{} {id}: the {} app refused it: {}\nHint: fix the object and apply it \
                             again",
                            kind.decl.name,
                            kind.app,
                            answer.errors.join("; ")
                        )));
                    }
                    warnings.extend(answer.warnings.into_iter().map(|warning| {
                        format!("{} {id} ({} app): {warning}", kind.decl.name, kind.app)
                    }));
                }
                Err(why) => {
                    info!(app = %kind.app, kind = %kind.decl.name, object = %id, "Object not validated by the app: {why}");
                    warnings.push(format!(
                        "{} {id} was not checked by the {} app ({why}); the schema check passed",
                        kind.decl.name, kind.app
                    ));
                }
            }
        }

        if inner.dry_run {
            let now = now_ms();
            let would = AppObjectRow {
                key: key.clone(),
                spec: spec_text.clone(),
                labels: input.labels.clone(),
                status: current
                    .as_ref()
                    .map(|row| row.status.clone())
                    .unwrap_or_default(),
                observed_generation: current.as_ref().map_or(0, |row| row.observed_generation),
                generation: match &current {
                    None => 1,
                    Some(row) if spec_changed => row.generation + 1,
                    Some(row) => row.generation,
                },
                version: current.as_ref().map_or(0, |row| row.version),
                created_by: current
                    .as_ref()
                    .map(|row| row.created_by.clone())
                    .unwrap_or_else(|| identity.principal.clone()),
                created_ms: current.as_ref().map_or(now, |row| row.created_ms),
                updated_by: identity.principal.clone(),
                updated_ms: now,
                status_updated_by: current
                    .as_ref()
                    .map(|row| row.status_updated_by.clone())
                    .unwrap_or_default(),
                status_updated_ms: current.as_ref().map_or(0, |row| row.status_updated_ms),
                validated_by: kind.validated_by(),
            };
            let verb = match &current {
                None => "would be created".to_string(),
                Some(_) if !spec_changed && !labels_changed => "unchanged".to_string(),
                Some(row) if spec_changed => format!(
                    "would change (generation {} → {})",
                    row.generation,
                    row.generation + 1
                ),
                Some(_) => "would change its labels".to_string(),
            };
            return Ok(Response::new(ApplyObjectReply {
                object: Some(object_proto(&would, Some(&kind), 0)),
                created: current.is_none(),
                changed: spec_changed || labels_changed,
                diff,
                warnings,
                message: format!("{} {id}: {verb}; dry run, nothing written", kind.decl.name),
                deduplicated: false,
                dry_run: true,
            }));
        }

        let outcome = self
            .store
            .put_app_object(ObjectWrite {
                key: key.clone(),
                spec: spec_text,
                labels: input.labels.clone(),
                principal: identity.principal.clone(),
                now_ms: now_ms(),
                validated_by: kind.validated_by(),
                cas,
            })
            .map_err(|e| map_store_error("Error writing the object", e))?;
        let (row, created, changed) = match outcome {
            PutObject::Written { row, previous } => (row, previous.is_none(), true),
            PutObject::Unchanged { row } => (row, false, false),
            PutObject::Conflict { current } => return Err(conflict(current.as_ref())),
        };
        let message = if created {
            format!(
                "{} {id}: created (generation {}, version {})",
                kind.decl.name, row.generation, row.version
            )
        } else if changed {
            format!(
                "{} {id}: updated (generation {}, version {})",
                kind.decl.name, row.generation, row.version
            )
        } else {
            format!(
                "{} {id}: unchanged (generation {}, version {})",
                kind.decl.name, row.generation, row.version
            )
        };
        if changed {
            self.store.events().object_applied(
                &kind.app,
                &kind.decl.name,
                &namespace,
                &name,
                row.generation,
                row.version,
                created,
                &identity.principal,
            );
            info!(
                kind = %kind.qualified(),
                object = %id,
                generation = row.generation,
                version = row.version,
                created,
                accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
                "Object applied"
            );
        }
        self.store
            .idempotency_record(&idempotency_key, "apply_object", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(ApplyObjectReply {
            object: Some(object_proto(&row, Some(&kind), 0)),
            created,
            changed,
            diff,
            warnings,
            message,
            deduplicated: false,
            dry_run: false,
        }))
    }

    /// The namespace and name a get or delete names, for `kind`: a
    /// namespaced kind's object is `namespace/name` or a name in the
    /// request's namespace (`default`); a cluster kind's has none. For an
    /// orphaned kind, whichever the stored rows hold.
    fn object_ref(
        &self,
        kind: &ResolvedKind,
        name: &str,
        namespace: &str,
    ) -> Result<(String, String), Status> {
        let namespaced = match kind.namespaced() {
            Some(namespaced) => namespaced,
            None => {
                // An orphan: cluster-scoped when its stored objects have no
                // namespace.
                let rows = self
                    .store
                    .app_objects(&ObjectFilter {
                        app: Some(kind.app().to_string()),
                        kind: Some(kind.name().to_string()),
                    })
                    .map_err(|e| map_store_error("Error reading objects", e))?;
                rows.first().is_none_or(|row| !row.key.namespace.is_empty())
            }
        };
        let (namespace, name) = if namespaced {
            split_object_ref(name, namespace)
        } else {
            (String::new(), name.trim().to_string())
        };
        validate_object_name(&name).map_err(Status::invalid_argument)?;
        Ok((namespace, name))
    }

    // ACL: `read` on the kind's access resource on the object's scope,
    // checked before the object is looked up (`object` for an orphan).
    pub(super) async fn get_object_rpc(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        let kind = self.resolve_kind(&registry, &inner.kind)?;
        let (namespace, name) = self.object_ref(&kind, &inner.name, &inner.namespace)?;
        self.enforce_object(&identity, &kind, AclAction::Read, &namespace, &name)?;
        let row = self
            .store
            .app_object(&ObjectKey {
                app: kind.app().to_string(),
                kind: kind.name().to_string(),
                namespace: namespace.clone(),
                name: name.clone(),
            })
            .map_err(|e| map_store_error("Error reading the object", e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "{} {} not found\nHint: `legion object list {}{}` lists them",
                    kind.name(),
                    object_id(&namespace, &name),
                    kind.name().to_lowercase(),
                    if namespace.is_empty() {
                        String::new()
                    } else {
                        format!(" -n {namespace}")
                    }
                ))
            })?;
        Ok(Response::new(GetObjectReply {
            object: Some(object_proto(&row, kind.installed(), 0)),
            kind: kind.installed().map(kind_proto),
        }))
    }

    // ACL: gated on `read` on the kind's access resource anywhere (any
    // kind's, or `object`, when no kind is named), then filtered per object
    // on its scope.
    pub(super) async fn list_objects_rpc(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        let selection = crate::structs::namespace::NamespaceSelection::parse(&inner.namespaces)
            .map_err(Status::invalid_argument)?;
        let (filter, gate) = if inner.kind.trim().is_empty() {
            let any_kind = registry
                .kinds()
                .iter()
                .any(|kind| identity.allows(kind.access, AclAction::Read, &AclScope::any()));
            if !any_kind {
                acl::enforce(
                    &identity,
                    AclResource::Object,
                    AclAction::Read,
                    &AclScope::any(),
                )?;
            }
            (ObjectFilter::default(), None)
        } else {
            let kind = self.resolve_kind(&registry, &inner.kind)?;
            acl::enforce(&identity, kind.access(), AclAction::Read, &AclScope::any())?;
            (
                ObjectFilter {
                    app: Some(kind.app().to_string()),
                    kind: Some(kind.name().to_string()),
                },
                Some(kind),
            )
        };
        let rows = self
            .store
            .app_objects(&filter)
            .map_err(|e| map_store_error("Error listing objects", e))?;
        let objects: Vec<AppObject> = rows
            .iter()
            .filter(|row| row.key.namespace.is_empty() || selection.matches(&row.key.namespace))
            .filter(|row| {
                inner
                    .labels
                    .iter()
                    .all(|(key, value)| row.labels.get(key) == Some(value))
            })
            .filter(|row| self.may_read_row(&identity, &registry, row))
            .map(|row| object_proto(row, registry.get(&row.key.app, &row.key.kind), 0))
            .collect();
        let mut kinds: Vec<ObjectKind> = Vec::new();
        let mut seen = BTreeSet::new();
        if let Some(kind) = gate.as_ref().and_then(ResolvedKind::installed) {
            seen.insert(kind.qualified());
            kinds.push(kind_proto(kind));
        }
        for object in &objects {
            if let Some(kind) = registry.get(&object.app, &object.kind) {
                if seen.insert(kind.qualified()) {
                    kinds.push(kind_proto(kind));
                }
            }
        }
        Ok(Response::new(ListObjectsReply { objects, kinds }))
    }

    // ACL: `write` on the kind's access resource and each `writeRequires` on
    // the object's scope, before the lookup (`object:write` for an orphan).
    #[instrument(skip_all, fields(kind, object))]
    pub(super) async fn delete_object_rpc(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        let kind = self.resolve_kind(&registry, &inner.kind)?;
        let (namespace, name) = self.object_ref(&kind, &inner.name, &inner.namespace)?;
        let id = object_id(&namespace, &name);
        tracing::Span::current().record("kind", kind.qualified().as_str());
        tracing::Span::current().record("object", id.as_str());
        self.enforce_object(&identity, &kind, AclAction::Write, &namespace, &name)?;
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "delete_object",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(DeleteObjectReply {
                message,
                deduplicated: true,
                object: None,
            }));
        }
        let key = ObjectKey {
            app: kind.app().to_string(),
            kind: kind.name().to_string(),
            namespace: namespace.clone(),
            name: name.clone(),
        };
        let row = match self
            .store
            .delete_app_object(&key, inner.expected_version)
            .map_err(|e| map_store_error("Error deleting the object", e))?
        {
            DeleteObject::Deleted(row) => row,
            DeleteObject::Missing => {
                return Err(Status::not_found(format!("{} {id} not found", kind.name())))
            }
            DeleteObject::Conflict(row) => {
                return Err(Status::aborted(format!(
                    "{} {id} is at version {}, not {}: it changed since you read it\nHint: read \
                     it again and delete that version",
                    kind.name(),
                    row.version,
                    inner.expected_version.unwrap_or_default()
                )))
            }
        };
        self.store.events().object_deleted(
            kind.app(),
            kind.name(),
            &namespace,
            &name,
            &identity.principal,
        );
        info!(
            kind = %kind.qualified(),
            object = %id,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Object deleted"
        );
        let message = format!(
            "{} {id}: deleted (was generation {}, version {})",
            kind.name(),
            row.generation,
            row.version
        );
        self.store
            .idempotency_record(&idempotency_key, "delete_object", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(DeleteObjectReply {
            message,
            deduplicated: false,
            object: Some(object_proto(&row, kind.installed(), 0)),
        }))
    }

    /// The app an app-plane object call is about: the calling app (which
    /// may only name itself), or, for a management token (or audit-only
    /// mode), the one it names. Everyone else is refused, and the denial
    /// audited.
    fn object_plane_app<T>(
        &self,
        request: &Request<T>,
        identity: &AclIdentity,
        named: &str,
        rpc: &str,
    ) -> Result<Option<String>, Status> {
        let caller = self.calling_app(request, identity);
        match (caller, identity.is_management) {
            (Some(caller), _) if named.is_empty() || named == caller => Ok(Some(caller)),
            (Some(caller), false) => Err(Status::permission_denied(format!(
                "the {caller} app may reach only its own objects, not {named}'s"
            ))),
            (_, true) => Ok(None),
            (None, false) if identity.audit_only => {
                warn!(
                    target: "acl_audit",
                    principal = %identity.principal,
                    resource = "app-plane",
                    action = rpc,
                    enforced = false,
                    "ACL denial"
                );
                Ok(None)
            }
            (None, false) => {
                warn!(
                    target: "acl_audit",
                    principal = %identity.principal,
                    accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
                    source = identity.source.as_str(),
                    resource = "app-plane",
                    action = rpc,
                    enforced = true,
                    "ACL denial"
                );
                if identity.source == crate::structs::acl::AclSource::Anonymous {
                    return Err(Status::unauthenticated(format!(
                        "{rpc} is called by an app with its own workload identity"
                    )));
                }
                Err(Status::permission_denied(format!(
                    "{rpc} is called by the app that owns the kind, with its own workload \
                     identity (its `legion` token); people read objects with GetObject and \
                     write them with ApplyObject"
                )))
            }
        }
    }

    // ACL: app-plane (the app's own workload identity, its objects only; or
    // management naming the app).
    pub(super) async fn watch_objects_rpc(
        &self,
        request: Request<WatchObjectsRequest>,
    ) -> Result<Response<ReceiverStream<Result<ObjectEvent, Status>>>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let named = request.get_ref().app.trim().to_string();
        let app = match self.object_plane_app(&request, &identity, &named, "WatchObjects")? {
            Some(app) => app,
            None if named.is_empty() => {
                return Err(Status::invalid_argument(
                    "a caller that is not an app names the app to watch (`app`)",
                ))
            }
            None => named,
        };
        if self
            .store
            .get_app(&app)
            .map_err(|e| map_store_error("Error loading the app", e))?
            .is_none()
        {
            return Err(Status::not_found(format!(
                "no app named {app:?} is installed"
            )));
        }
        let inner = request.get_ref();
        let registry = self.object_registry()?;
        let mut kinds = BTreeSet::new();
        for typed in inner
            .kinds
            .iter()
            .map(|kind| kind.trim())
            .filter(|k| !k.is_empty())
        {
            let bare = match typed.split_once('/') {
                Some((owner, kind)) if owner == app => kind,
                Some((owner, _)) => {
                    return Err(Status::invalid_argument(format!(
                        "kind {typed:?} is the {owner} app's, not {app}'s"
                    )))
                }
                None => typed,
            };
            let kind = registry
                .of_app(&app)
                .find(|kind| same_noun(bare, &kind.decl.name))
                .ok_or_else(|| {
                    Status::invalid_argument(format!("app {app} declares no kind {typed:?}"))
                })?;
            kinds.insert(kind.decl.name.clone());
        }
        let shard_count = inner.shard_count.max(1);
        let shards: BTreeSet<u32> = inner.shards.iter().copied().collect();
        if let Some(bad) = shards.iter().find(|shard| **shard >= shard_count) {
            return Err(Status::invalid_argument(format!(
                "shard {bad} is not below shard_count {shard_count}"
            )));
        }
        let credential = self.stream_credential(&request, &identity)?;
        let source = Arc::new(ObjectWatchSource {
            store: self.store.clone(),
            object_registry: self.object_registry.clone(),
        });
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut reauth = tokio::time::interval(credential.interval);
            reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            reauth.tick().await;
            let mut poll = tokio::time::interval(WATCH_POLL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut fingerprint: Option<String> = None;
            let mut last_full = std::time::Instant::now();
            let mut sent: Option<BTreeMap<ObjectKey, AppObject>> = None;
            let mut generation = 0u64;
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    _ = reauth.tick() => {
                        if let Err(status) = credential.reauthenticate(&source.store) {
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                        continue;
                    }
                    _ = poll.tick() => {}
                }
                let probe = match (
                    source.store.app_fingerprint(),
                    source.store.app_object_fingerprint(),
                ) {
                    (Ok(apps), Ok((count, version))) => format!("{apps}|{count}|{version}"),
                    (Err(err), _) | (_, Err(err)) => {
                        debug!("WatchObjects: reading fingerprints: {err:#}");
                        continue;
                    }
                };
                let stale = last_full.elapsed() >= WATCH_FULL_READ;
                if sent.is_some() && fingerprint.as_deref() == Some(probe.as_str()) && !stale {
                    continue;
                }
                let current = match source.objects(&app, &kinds, shard_count, &shards) {
                    Ok(current) => current,
                    Err(status) => {
                        warn!(app = %app, "WatchObjects: {}", status.message());
                        continue;
                    }
                };
                fingerprint = Some(probe);
                last_full = std::time::Instant::now();
                let event = match &sent {
                    None => ObjectEvent {
                        snapshot: true,
                        upserts: current.values().cloned().collect(),
                        deletes: Vec::new(),
                        generation: 0,
                    },
                    Some(previous) => {
                        let upserts: Vec<AppObject> = current
                            .iter()
                            .filter(|(key, object)| previous.get(*key) != Some(*object))
                            .map(|(_, object)| object.clone())
                            .collect();
                        let deletes: Vec<ObjectRef> = previous
                            .keys()
                            .filter(|key| !current.contains_key(*key))
                            .map(|key| ObjectRef {
                                app: key.app.clone(),
                                kind: key.kind.clone(),
                                namespace: key.namespace.clone(),
                                name: key.name.clone(),
                            })
                            .collect();
                        if upserts.is_empty() && deletes.is_empty() {
                            continue;
                        }
                        ObjectEvent {
                            snapshot: false,
                            upserts,
                            deletes,
                            generation: 0,
                        }
                    }
                };
                generation += 1;
                let event = ObjectEvent {
                    generation,
                    ..event
                };
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
                sent = Some(current);
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ACL: app-plane (the workload identity of the app that owns the kind,
    // or management); never a delegated identity.
    #[instrument(skip_all, fields(kind, object))]
    pub(super) async fn update_object_status_rpc(
        &self,
        request: Request<UpdateObjectStatusRequest>,
    ) -> Result<Response<UpdateObjectStatusReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let caller = self.object_plane_app(&request, &identity, "", "UpdateObjectStatus")?;
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        let typed = inner.kind.trim();
        let kind: InstalledKind = match &caller {
            Some(app) => {
                let bare = match typed.split_once('/') {
                    Some((owner, kind)) if owner == app => kind,
                    Some((owner, _)) => {
                        return Err(Status::permission_denied(format!(
                            "the {app} app writes status only for its own kinds; {typed} is the \
                             {owner} app's"
                        )))
                    }
                    None => typed,
                };
                registry
                    .of_app(app)
                    .find(|kind| same_noun(bare, &kind.decl.name))
                    .cloned()
                    .ok_or_else(|| {
                        Status::invalid_argument(format!("app {app} declares no kind {typed:?}"))
                    })?
            }
            None => match self.resolve_kind(&registry, typed)? {
                ResolvedKind::Installed(kind) => *kind,
                ResolvedKind::Orphaned { app, kind } => {
                    return Err(Status::failed_precondition(format!(
                        "kind {kind} is orphaned: app {app} no longer declares it"
                    )))
                }
            },
        };
        let namespace = if kind.is_namespaced() {
            match inner.namespace.trim() {
                "" => DEFAULT_NAMESPACE.to_string(),
                namespace => namespace.to_string(),
            }
        } else {
            String::new()
        };
        let name = inner.name.trim().to_string();
        validate_object_name(&name).map_err(Status::invalid_argument)?;
        let id = object_id(&namespace, &name);
        tracing::Span::current().record("kind", kind.qualified().as_str());
        tracing::Span::current().record("object", id.as_str());
        if inner.status_json.len() > MAX_OBJECT_DOC_BYTES {
            return Err(Status::invalid_argument(format!(
                "the status is {} bytes; the limit is {MAX_OBJECT_DOC_BYTES}",
                inner.status_json.len()
            )));
        }
        let status: Value = serde_json::from_str(&inner.status_json)
            .map_err(|err| Status::invalid_argument(format!("the status is not JSON: {err}")))?;
        kind.check_status(&status).map_err(|errors| {
            Status::invalid_argument(format!("{} {id}: {}", kind.decl.name, errors.join("; ")))
        })?;
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "update_object_status",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        let key = ObjectKey {
            app: kind.app.clone(),
            kind: kind.decl.name.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
        };
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let current = self
                .store
                .app_object(&key)
                .map_err(|e| map_store_error("Error reading the object", e))?;
            return Ok(Response::new(UpdateObjectStatusReply {
                object: current.map(|row| object_proto(&row, Some(&kind), 0)),
                changed: false,
                deduplicated: true,
                message,
            }));
        }
        let cas = match (inner.expected_generation, inner.expected_version) {
            (Some(generation), _) => StatusCas::Generation(generation),
            (None, Some(version)) => StatusCas::Version(version),
            (None, None) => StatusCas::Any,
        };
        let status_text = serde_json::to_string(&status)
            .map_err(|err| Status::internal(format!("encoding the status: {err}")))?;
        let (row, changed) = match self
            .store
            .update_app_object_status(
                &key,
                &status_text,
                inner.observed_generation,
                cas,
                &identity.principal,
                now_ms(),
            )
            .map_err(|e| map_store_error("Error writing the object's status", e))?
        {
            PutStatus::Written { row, changed } => (row, changed),
            PutStatus::Missing => {
                return Err(Status::not_found(format!(
                    "{} {id} not found",
                    kind.decl.name
                )))
            }
            PutStatus::Conflict(row) => {
                return Err(Status::aborted(match cas {
                    StatusCas::Generation(expected) => format!(
                        "{} {id} is at generation {}, not {expected}: its spec changed since you \
                         read it; reconcile the new spec and report on that",
                        kind.decl.name, row.generation
                    ),
                    _ => format!(
                        "{} {id} is at version {}, not {}: it changed since you read it",
                        kind.decl.name,
                        row.version,
                        inner.expected_version.unwrap_or_default()
                    ),
                }))
            }
        };
        if changed {
            self.store.events().object_status_changed(
                &kind.app,
                &kind.decl.name,
                &namespace,
                &name,
                row.observed_generation,
                &identity.principal,
            );
        }
        let message = format!(
            "{} {id}: status {} (observed generation {}, version {})",
            kind.decl.name,
            if changed { "updated" } else { "unchanged" },
            row.observed_generation,
            row.version
        );
        self.store
            .idempotency_record(&idempotency_key, "update_object_status", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(UpdateObjectStatusReply {
            object: Some(object_proto(&row, Some(&kind), 0)),
            changed,
            deduplicated: false,
            message,
        }))
    }
}

/// What a `WatchObjects` task reads from: the store and the kind cache.
struct ObjectWatchSource {
    store: Store,
    object_registry: ObjectRegistryCache,
}

impl ObjectWatchSource {
    /// `app`'s objects of the kinds it declares now (only `kinds`, when
    /// any), in the watched shards.
    fn objects(
        &self,
        app: &str,
        kinds: &BTreeSet<String>,
        shard_count: u32,
        shards: &BTreeSet<u32>,
    ) -> Result<BTreeMap<ObjectKey, AppObject>, Status> {
        let registry = cached_object_registry(&self.store, &self.object_registry, false)?;
        let rows = self
            .store
            .app_objects(&ObjectFilter {
                app: Some(app.to_string()),
                kind: None,
            })
            .map_err(|e| map_store_error("Error reading objects", e))?;
        let mut objects = BTreeMap::new();
        for row in rows {
            if !kinds.is_empty() && !kinds.contains(&row.key.kind) {
                continue;
            }
            // Orphaned objects are no longer the app's.
            let Some(kind) = registry.get(app, &row.key.kind) else {
                continue;
            };
            let shard = object_shard(
                &row.key.app,
                &row.key.kind,
                &row.key.namespace,
                &row.key.name,
                shard_count,
            );
            if !shards.is_empty() && !shards.contains(&shard) {
                continue;
            }
            let object = object_proto(&row, Some(kind), shard);
            objects.insert(row.key.clone(), object);
        }
        Ok(objects)
    }
}

/// The installed kinds from `cache`, rebuilt when the apps' fingerprint
/// moved. `log` warns about records whose kinds no longer compile.
fn cached_object_registry(
    store: &Store,
    cache: &ObjectRegistryCache,
    log: bool,
) -> Result<Arc<ObjectKindRegistry>, Status> {
    let fingerprint = store
        .app_fingerprint()
        .map_err(|e| map_store_error("Error reading installed apps", e))?;
    if let Some((cached, registry)) = cache
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
    {
        if *cached == fingerprint {
            return Ok(registry.clone());
        }
    }
    let records = store
        .list_apps()
        .map_err(|e| map_store_error("Error reading installed apps", e))?;
    let (registry, problems) = ObjectKindRegistry::from_records(&records);
    if log {
        for problem in problems {
            warn!("Ignoring an app's object kinds: {problem}");
        }
    }
    let registry = Arc::new(registry);
    *cache.lock().unwrap_or_else(|poison| poison.into_inner()) =
        Some((fingerprint, registry.clone()));
    Ok(registry)
}

/// The state an object reads as, by name: for tests and the CLI.
#[allow(dead_code)]
pub(crate) fn state_name(state: ObjectState) -> &'static str {
    state.as_str()
}

#[cfg(test)]
mod object_rpcs_tests {
    use super::*;

    #[test]
    fn references_split_into_namespace_and_name_test() {
        assert_eq!(
            split_object_ref("team-a/web", ""),
            ("team-a".to_string(), "web".to_string())
        );
        assert_eq!(
            split_object_ref("web", "team-b"),
            ("team-b".to_string(), "web".to_string())
        );
        assert_eq!(
            split_object_ref("web", ""),
            ("default".to_string(), "web".to_string())
        );
    }

    #[test]
    fn scopes_follow_the_resource_test() {
        let namespaced = object_scope(AclResource::Object, "gitops/Repo", "team-a", "web");
        assert_eq!(namespaced.namespace.as_deref(), Some("team-a"));
        assert_eq!(namespaced.kind.as_deref(), Some("gitops/Repo"));
        let cluster = object_scope(AclResource::Object, "a/NodeGroup", "", "workers");
        assert_eq!(cluster.namespace.as_deref(), Some("*"));
        let node = object_scope(AclResource::Node, "a/NodeGroup", "", "workers");
        assert_eq!(node.namespace, None);
        assert_eq!(node.name.as_deref(), Some("workers"));
    }
}
