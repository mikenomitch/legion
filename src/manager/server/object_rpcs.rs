//! Objects of app-declared kinds on the manager (docs/app-objects.md;
//! app-object-kinds plan §6): the kind registry (from the `object_kind`
//! table), the ACL every read and write runs (the kind's `access` resource
//! on the object's scope, then, on writes, its `requires` resolved from the
//! spec and checked as the writer, fail-closed), the app's advisory
//! validate call, the lifecycle reports, and the eight RPCs:
//! `ListObjectKinds`, `ApplyObject`, `GetObject`, `ListObjects`,
//! `DeleteObject`, and the app-plane `WatchObjects`, `UpdateObjectStatus`,
//! and `ReleaseObject`.

use super::*;
use crate::storage::store::{
    AppObjectRow, AppObjectStatusRow, DeleteObject, KindWrite, ObjectFilter, ObjectKey,
    ObjectWrite, PutObject, PutStatus, ReleaseObject, StatusWrite, VersionCas,
};
use crate::structs::app::AppRecord;
use crate::structs::app_object::{
    condition_transitions, default_changes, immutable_changes, judge, manifest_kinds,
    object_document, object_id, object_shard, object_validate_context, parse_object_input,
    qualified_kind, requires_digest, resolve_requirements, same_noun, spec_diff,
    stamp_condition_times, summarize, validate_conditions, validate_labels, validate_object_name,
    InstalledKind, KindDecl, KindLookup, KindState, ObjectAuthority as AuthorityDoc,
    ObjectCondition as ConditionDoc, ObjectKindRegistry, ObjectState, ObjectStatus as StatusDoc,
    ObjectValidateAnswer, ObjectValidateCall, ResolvedPermission, SummaryInputs, MAX_SPEC_BYTES,
    MAX_STATUS_FIELDS_BYTES, OBJECT_VALIDATE_NAME, OBJECT_VALIDATE_PATH, STORED_SHARD_COUNT,
};
use crate::structs::app_ui::{inputs_digest, AssertionKind, AssertionScope, ViewerAssertion};
use crate::structs::extensions::VALIDATE_TIMEOUT;
use crate::structs::namespace::DEFAULT_NAMESPACE;
use crate::structs::{
    AppObject, ApplyObjectReply, ApplyObjectRequest, DeleteObjectReply, DeleteObjectRequest,
    GetObjectReply, GetObjectRequest, ListObjectKindsReply, ListObjectKindsRequest,
    ListObjectsReply, ListObjectsRequest, ObjectAuthority, ObjectCondition, ObjectEvent,
    ObjectKind, ObjectKindColumn, ObjectRef, ObjectStatus, ReleaseObjectReply,
    ReleaseObjectRequest, UpdateObjectStatusReply, UpdateObjectStatusRequest, WatchObjectsRequest,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio_stream::wrappers::ReceiverStream;

/// How often `WatchObjects` checks for changes.
const WATCH_POLL: Duration = Duration::from_secs(1);
/// How often it re-reads every object even when the fingerprints say
/// nothing changed.
const WATCH_FULL_READ: Duration = Duration::from_secs(30);
/// Most objects in one `WatchObjects` message.
pub(crate) const WATCH_CHUNK: usize = 1_000;
/// Most encoded bytes of objects in one `WatchObjects` message (a message
/// holds at least one object). One object is at most a few hundred KiB, so
/// a message stays under the 16 MiB the app framework decodes
/// ([`crate::apps::objects::MAX_WATCH_MESSAGE_BYTES`]).
pub(crate) const WATCH_CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// `objects` split into watch messages: at most [`WATCH_CHUNK`] objects
/// and about [`WATCH_CHUNK_BYTES`] each; one empty chunk for no objects.
pub(crate) fn watch_chunks(objects: Vec<AppObject>) -> Vec<Vec<AppObject>> {
    use prost::Message;
    let mut chunks = vec![Vec::new()];
    let mut bytes = 0usize;
    for object in objects {
        let size = object.encoded_len();
        let full = chunks.last().is_some_and(|current: &Vec<AppObject>| {
            !current.is_empty()
                && (current.len() >= WATCH_CHUNK || bytes + size > WATCH_CHUNK_BYTES)
        });
        if full {
            chunks.push(Vec::new());
            bytes = 0;
        }
        bytes += size;
        chunks.last_mut().expect("one chunk").push(object);
    }
    chunks
}

/// The error code an apply of a kind no app declares carries (on a
/// retryable `UNAVAILABLE`).
pub const KIND_NOT_INSTALLED: &str = "KIND_NOT_INSTALLED";

/// The kind registry cache: the fingerprint it was built for, and the
/// registry.
pub(crate) type ObjectRegistryCache = Arc<Mutex<Option<(String, Arc<ObjectKindRegistry>)>>>;

fn now_ms() -> i64 {
    super::autoscale_rpcs::now_unix_ms()
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn json_text(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// The event ref of a stored object.
fn event_ref<'a>(
    row: &'a AppObjectRow,
    access: &'a str,
) -> crate::storage::events::drafts::ObjectEventRef<'a> {
    crate::storage::events::drafts::ObjectEventRef {
        app: &row.key.app,
        kind: &row.key.kind,
        namespace: &row.key.namespace,
        name: &row.key.name,
        uid: row.uid,
        access,
    }
}

/// The proto of a kind.
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
        requires: kind
            .decl
            .requires
            .iter()
            .map(crate::structs::app_object::requirement_label)
            .collect(),
        requires_json: json_text(&serde_json::to_value(&kind.decl.requires).unwrap_or_default()),
        columns: kind
            .decl
            .columns
            .iter()
            .map(|column| ObjectKindColumn {
                name: column.header(),
                pointer: column.pointer().to_string(),
            })
            .collect(),
        app_enabled: kind.app_enabled,
        state: kind.state.as_str().to_string(),
        immutable: kind.decl.immutable.clone(),
        cleanup: kind.decl.cleanup.clone().unwrap_or_default(),
        max_objects: kind.decl.max_objects(),
        write_only: kind
            .schema
            .write_only
            .iter()
            .map(|path| format!("/spec/{}", path.join("/")))
            .collect(),
        manifest_digest: kind.digest.clone(),
    }
}

fn condition_proto(condition: &ConditionDoc) -> ObjectCondition {
    ObjectCondition {
        r#type: condition.kind.clone(),
        status: condition.status.clone(),
        reason: condition.reason.clone(),
        message: condition.message.clone(),
        since: condition.since.clone(),
    }
}

fn condition_doc(condition: &ObjectCondition) -> ConditionDoc {
    ConditionDoc {
        kind: condition.r#type.trim().to_string(),
        status: condition.status.trim().to_string(),
        reason: condition.reason.trim().to_string(),
        message: condition.message.clone(),
        since: String::new(),
    }
}

/// A stored status row, parsed.
fn status_doc(row: &AppObjectStatusRow) -> StatusDoc {
    StatusDoc {
        observed_generation: row.observed_generation,
        conditions: serde_json::from_str(&row.conditions).unwrap_or_default(),
        fields: serde_json::from_str(&row.fields).unwrap_or_else(|_| json!({})),
    }
}

/// What `ListObjects --authority-lapsed` says of an object's authority
/// (`ObjectAuthority.check`).
pub(crate) struct AuthorityCheck;

impl AuthorityCheck {
    /// The authorizer still holds every recorded permission.
    pub(crate) const HELD: &'static str = "held";
    /// It no longer does: its token is gone or expired, or lost a
    /// permission.
    pub(crate) const LAPSED: &'static str = "lapsed";
    /// It cannot be re-resolved (no accessor).
    pub(crate) const UNKNOWN: &'static str = "unknown";
}

/// What turning a row into its proto needs beyond the row.
pub(crate) struct ProtoContext<'a> {
    pub kind: Option<&'a InstalledKind>,
    pub status: Option<&'a AppObjectStatusRow>,
    /// Whether the owning app is enabled and running.
    pub app_running: bool,
    /// Show `writeOnly` values (the owning app's watch and replies only).
    pub reveal: bool,
    pub shard: u32,
}

/// The proto of a stored object, judged against its kind now.
pub(crate) fn object_proto(row: &AppObjectRow, ctx: &ProtoContext<'_>) -> AppObject {
    let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
    let (effective, state, detail) = judge(ctx.kind, &spec);
    let status = ctx.status.map(status_doc);
    let (summary, summary_detail) = summarize(&SummaryInputs {
        state,
        deleting: row.is_deleting(),
        app_running: ctx.app_running,
        generation: row.generation,
        status: status.as_ref(),
    });
    let (spec, effective, fields) = match (ctx.kind, ctx.reveal) {
        (Some(kind), false) => (
            kind.schema.redacted(&spec),
            kind.schema.redacted(&effective),
            status.as_ref().map(|status| match &kind.status_schema {
                Some(schema) => schema.redacted(&status.fields),
                None => status.fields.clone(),
            }),
        ),
        _ => (spec, effective, status.as_ref().map(|s| s.fields.clone())),
    };
    let authority: AuthorityDoc = serde_json::from_str(&row.authority).unwrap_or_default();
    AppObject {
        app: row.key.app.clone(),
        kind: row.key.kind.clone(),
        qualified_kind: qualified_kind(&row.key.app, &row.key.kind),
        namespace: row.key.namespace.clone(),
        name: row.key.name.clone(),
        id: object_id(&row.key.namespace, &row.key.name),
        uid: row.uid,
        labels: row.labels.clone().into_iter().collect(),
        spec_json: json_text(&spec),
        effective_json: json_text(&effective),
        generation: row.generation,
        spec_version: row.spec_version,
        status: ctx
            .status
            .zip(status.as_ref())
            .map(|(stored, doc)| ObjectStatus {
                observed_generation: doc.observed_generation,
                conditions: doc.conditions.iter().map(condition_proto).collect(),
                fields_json: json_text(fields.as_ref().unwrap_or(&json!({}))),
                updated_by: stored.updated_by.clone(),
                updated_at_unix_ms: stored.updated_ms,
            }),
        summary,
        summary_detail,
        state: state.as_str().to_string(),
        detail,
        authority: Some(ObjectAuthority {
            permissions: authority
                .permissions
                .iter()
                .map(ResolvedPermission::label)
                .collect(),
            permissions_json: json_text(
                &serde_json::to_value(&authority.permissions).unwrap_or_default(),
            ),
            authorized_by: authority.authorized_by,
            accessor: authority.accessor.unwrap_or_default(),
            authorized_at_unix_ms: authority.authorized_ms,
            check: String::new(),
            check_detail: String::new(),
        }),
        managed_by: row.managed_by.clone(),
        deleting: row.is_deleting(),
        deleting_since_unix_ms: row.deleting_since,
        cleanup_deadline_unix_ms: row.cleanup_deadline,
        created_by: row.created_by.clone(),
        created_at_unix_ms: row.created_ms,
        updated_by: row.updated_by.clone(),
        updated_at_unix_ms: row.updated_ms,
        schema_digest: row.schema_digest.clone(),
        shard: ctx.shard,
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

/// The status for a kind lookup that failed. An apply of a kind no app
/// declares is retryable (`KIND_NOT_INSTALLED`): the app may be installing.
fn lookup_status(lookup: &KindLookup, typed: &str, apply: bool) -> Status {
    let message = lookup.message(typed);
    match lookup {
        KindLookup::Unknown { .. } if apply => {
            let mut status = Status::unavailable(format!(
                "no installed app declares kind {typed:?} ({KIND_NOT_INSTALLED}); if its app is \
                 being installed, retry\n{}",
                message.split_once('\n').map(|(_, hint)| hint).unwrap_or(
                    "Hint: `legion object kinds` lists the kinds installed apps declare"
                )
            ));
            status.metadata_mut().insert(
                crate::manager::worker_plane::ERROR_CODE_METADATA,
                tonic::metadata::MetadataValue::from_static(KIND_NOT_INSTALLED),
            );
            status
        }
        _ => Status::invalid_argument(message),
    }
}

/// The kinds one record declares, as the sync writes them.
fn kind_writes(record: &AppRecord) -> Vec<KindWrite> {
    manifest_kinds(&record.manifest)
        .unwrap_or_default()
        .into_iter()
        .map(|decl| KindWrite {
            kind: decl.name.clone(),
            decl: serde_json::to_string(&decl).unwrap_or_default(),
            digest: record.manifest_digest.clone(),
        })
        .collect()
}

/// What installing or upgrading an app to the kinds `next` does to its
/// stored objects, for the prompt (app-object-kinds plan §6.9).
pub(crate) fn objects_report(
    app: &str,
    rows: &[AppObjectRow],
    before: &[InstalledKind],
    next: &[InstalledKind],
    installing: bool,
) -> Vec<String> {
    let mut lines = Vec::new();
    let by_kind = |kind: &str| -> Vec<&AppObjectRow> {
        rows.iter()
            .filter(|row| row.key.app == app && row.key.kind == kind)
            .collect()
    };
    for kind in next {
        let objects = by_kind(&kind.decl.name);
        if objects.is_empty() {
            continue;
        }
        let failing: Vec<String> = objects
            .iter()
            .filter_map(|row| {
                let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
                kind.check_spec(&spec).err().map(|errors| {
                    format!(
                        "  {}: {}",
                        object_id(&row.key.namespace, &row.key.name),
                        errors.join("; ")
                    )
                })
            })
            .collect();
        let count = objects.len();
        let noun = if count == 1 { "object" } else { "objects" };
        let previous = before.iter().find(|old| old.decl.name == kind.decl.name);
        let adopted = installing && previous.is_some_and(|old| !old.is_active());
        let lead = format!(
            "{count} {} {noun}{}",
            kind.qualified(),
            if adopted {
                " left orphaned by an earlier install, adopted again (they keep their recorded \
                 authority)"
            } else {
                ""
            }
        );
        if failing.is_empty() {
            lines.push(format!("{lead}; every one passes this version's schema."));
        } else {
            lines.push(format!(
                "{lead}; {} fail{} this version's schema (flagged invalid; each must conform at \
                 its next apply):",
                failing.len(),
                if failing.len() == 1 { "s" } else { "" }
            ));
            lines.extend(failing);
        }
        if let Some(previous) = previous {
            for (path, old, new) in default_changes(&previous.decl, &kind.decl) {
                let pointer = format!("/spec{path}");
                let keeping = objects
                    .iter()
                    .filter(|row| {
                        serde_json::from_str::<Value>(&row.spec)
                            .ok()
                            .and_then(|spec| {
                                object_document("", "", &BTreeMap::new(), &spec, &Value::Null)
                                    .pointer(&pointer)
                                    .cloned()
                            })
                            .as_ref()
                            == Some(&old)
                    })
                    .count();
                lines.push(format!(
                    "{}.{} default {old} → {new}: {keeping} existing {} keep {old}; re-apply to \
                     adopt",
                    kind.qualified(),
                    path.trim_start_matches('/').replace('/', "."),
                    if keeping == 1 { "object" } else { "objects" },
                ));
            }
            let digest = requires_digest(&kind.decl);
            let stale = objects
                .iter()
                .filter(|row| {
                    serde_json::from_str::<AuthorityDoc>(&row.authority)
                        .map(|authority| authority.requires_digest != digest)
                        .unwrap_or(true)
                })
                .count();
            if requires_digest(&previous.decl) != digest && stale > 0 {
                lines.push(format!(
                    "{stale} {} {} authorized under the previous requirements; they keep that \
                     authority until re-applied",
                    kind.qualified(),
                    if stale == 1 {
                        "object was"
                    } else {
                        "objects were"
                    }
                ));
            }
        }
    }
    for kind in before
        .iter()
        .filter(|old| !next.iter().any(|new| new.decl.name == old.decl.name))
    {
        let count = by_kind(&kind.decl.name).len();
        if count > 0 && !installing {
            lines.push(format!(
                "{count} {} object{} (this version no longer declares the kind; with --force \
                 they are left orphaned: readable and deletable, not writable)",
                kind.qualified(),
                if count == 1 { "" } else { "s" }
            ));
        }
    }
    lines
}

impl ManagerDaemonService {
    /// The kinds, rebuilt only when the kinds or the app records changed.
    pub(crate) fn object_registry(&self) -> Result<Arc<ObjectKindRegistry>, Status> {
        cached_object_registry(&self.store, &self.object_registry, true)
    }

    /// The kinds a record declares, compiled, as they would be installed.
    pub(crate) fn record_kinds(&self, record: &AppRecord) -> Vec<InstalledKind> {
        manifest_kinds(&record.manifest)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|decl| {
                InstalledKind::new(
                    &record.name,
                    decl,
                    &record.manifest_digest,
                    KindState::Active,
                    &record.display_version(),
                    record.enabled,
                )
                .ok()
            })
            .collect()
    }

    /// Every stored object of `app`.
    pub(crate) fn app_object_rows(&self, app: &str) -> Result<Vec<AppObjectRow>, Status> {
        self.store
            .app_objects(&ObjectFilter {
                app: Some(app.to_string()),
                ..Default::default()
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

    /// Bring `app`'s `object_kind` rows in line with its record (its
    /// declared kinds active, the rest orphaned while they have objects).
    /// Every app write calls it; the reconciler's full pass covers the
    /// rest.
    pub fn sync_object_kinds_for(&self, app: &str) -> Result<bool, Status> {
        let record = self
            .store
            .get_app(app)
            .map_err(|e| map_store_error("Error loading the app", e))?;
        let writes = record.as_ref().map(kind_writes).unwrap_or_default();
        self.store
            .sync_object_kinds(app, writes, now_ms())
            .map_err(|e| map_store_error("Error recording the app's object kinds", e))
    }

    /// [`Self::sync_object_kinds_for`] for every app with a record or kind
    /// rows (the reconciler's full pass).
    pub fn sync_all_object_kinds(&self) -> Result<(), Status> {
        let mut apps: BTreeSet<String> = self
            .store
            .list_apps()
            .map_err(|e| map_store_error("Error listing apps", e))?
            .into_iter()
            .map(|record| record.name)
            .collect();
        apps.extend(
            self.store
                .object_kind_apps()
                .map_err(|e| map_store_error("Error listing object kinds", e))?,
        );
        for app in apps {
            self.sync_object_kinds_for(&app)?;
        }
        Ok(())
    }

    /// Remove deleting objects whose cleanup ran out (or whose kind is
    /// orphaned), emitting `object.cleanup_abandoned` for each. The app
    /// reconciler runs it on the Control-lane leader.
    pub fn expire_object_cleanups(&self) -> Result<usize, Status> {
        let registry = self.object_registry()?;
        let orphaned: BTreeSet<(String, String)> = registry
            .kinds()
            .iter()
            .filter(|kind| !kind.is_active())
            .map(|kind| (kind.app.clone(), kind.decl.name.clone()))
            .collect();
        let now = now_ms();
        let removed = self
            .store
            .expire_app_object_cleanups(now, orphaned.clone())
            .map_err(|e| map_store_error("Error expiring object cleanups", e))?;
        for row in &removed {
            let access = registry
                .get(&row.key.app, &row.key.kind)
                .map(|kind| kind.access.as_str())
                .unwrap_or("object");
            let why = if orphaned.contains(&(row.key.app.clone(), row.key.kind.clone())) {
                "its kind is orphaned, so no app is left to release it".to_string()
            } else {
                "its cleanup window ran out".to_string()
            };
            warn!(kind = %qualified_kind(&row.key.app, &row.key.kind), object = %object_id(&row.key.namespace, &row.key.name), "Object cleanup abandoned: {why}");
            self.store
                .events()
                .object_cleanup_abandoned(&event_ref(row, access), &why);
        }
        Ok(removed.len())
    }

    /// Whether `app` is enabled and has a running allocation, for the
    /// `Stale` summary; cached in `seen` for one request.
    fn app_running(&self, app: &str, seen: &mut BTreeMap<String, bool>) -> bool {
        if let Some(running) = seen.get(app) {
            return *running;
        }
        let running = self
            .store
            .get_app(app)
            .ok()
            .flatten()
            .filter(|record| record.enabled)
            .and_then(|record| self.store.app_job_allocations(&record.job_id()).ok())
            .is_some_and(|allocations| allocations.running > 0);
        seen.insert(app.to_string(), running);
        running
    }

    /// One object's proto as a person reads it (`writeOnly` redacted).
    fn read_proto(
        &self,
        row: &AppObjectRow,
        kind: Option<&InstalledKind>,
        seen: &mut BTreeMap<String, bool>,
    ) -> Result<AppObject, Status> {
        let status = self
            .store
            .app_object_status(row.uid)
            .map_err(|e| map_store_error("Error reading the object's status", e))?;
        let running = self.app_running(&row.key.app, seen);
        Ok(object_proto(
            row,
            &ProtoContext {
                kind,
                status: status.as_ref(),
                app_running: running,
                reveal: false,
                shard: 0,
            },
        ))
    }

    /// `action` on the kind's access resource on the object's scope.
    pub(super) fn enforce_access(
        &self,
        identity: &AclIdentity,
        kind: &InstalledKind,
        action: AclAction,
        namespace: &str,
        name: &str,
    ) -> Result<(), Status> {
        acl::enforce(
            identity,
            kind.access,
            action,
            &kind.access_scope(namespace, name),
        )
    }

    /// The kind's `requires` resolved against `doc` and checked as the
    /// writer, fail-closed (app-object-kinds plan §6.4). Returns what was
    /// verified, for the object's `authority`.
    pub(super) fn enforce_requirements(
        &self,
        identity: &AclIdentity,
        kind: &InstalledKind,
        doc: &Value,
        namespace: &str,
        id: &str,
    ) -> Result<Vec<ResolvedPermission>, Status> {
        let resolved = resolve_requirements(kind, doc, namespace).map_err(|err| {
            Status::invalid_argument(format!("{} {id}: requires: {err}", kind.qualified()))
        })?;
        for permission in &resolved {
            let (Some(resource), Some(action)) = (
                AclResource::parse(&permission.resource),
                AclAction::parse(&permission.action),
            ) else {
                return Err(Status::failed_precondition(format!(
                    "{}: requires names {}:{}, which is not a permission",
                    kind.qualified(),
                    permission.resource,
                    permission.action
                )));
            };
            if acl::enforce(identity, resource, action, &permission.scope()).is_err() {
                return Err(Status::permission_denied(format!(
                    "writing {} {id} needs {}, which your token does not hold\nHint: the kind \
                     requires it of whoever writes its objects (`legion object explain {}`); have \
                     someone who holds it make the change",
                    kind.qualified(),
                    permission.label(),
                    kind.qualified()
                )));
            }
        }
        Ok(resolved)
    }

    /// The kind `typed` names (a qualified name, or a short name exactly one
    /// kind has), active or orphaned.
    /// The coarse gate before a typed kind is looked up: the caller may
    /// `action` some kind's objects somewhere (or `object` anywhere), so a
    /// caller with no object rights at all learns nothing about which kinds
    /// exist (AGENTS.md, "Check before you fetch").
    pub(super) fn gate_objects(
        &self,
        identity: &AclIdentity,
        registry: &ObjectKindRegistry,
        action: AclAction,
    ) -> Result<(), Status> {
        if registry
            .kinds()
            .iter()
            .any(|kind| identity.allows_kind_somewhere(kind.access, action, &kind.qualified()))
        {
            return Ok(());
        }
        acl::enforce(identity, AclResource::Object, action, &AclScope::any())
    }

    pub(super) fn lookup_kind(
        &self,
        registry: &ObjectKindRegistry,
        typed: &str,
    ) -> Result<InstalledKind, Status> {
        if typed.trim().is_empty() {
            return Err(Status::invalid_argument(
                "name a kind (`legion object kinds` lists them)",
            ));
        }
        registry
            .resolve(typed)
            .cloned()
            .map_err(|lookup| lookup_status(&lookup, typed.trim(), false))
    }

    /// The namespace and name a get or delete names for `kind`.
    pub(super) fn object_ref(
        &self,
        kind: &InstalledKind,
        name: &str,
        namespace: &str,
    ) -> Result<(String, String), Status> {
        let (namespace, name) = if kind.is_namespaced() {
            split_object_ref(name, namespace)
        } else {
            (String::new(), name.trim().to_string())
        };
        validate_object_name(&name).map_err(Status::invalid_argument)?;
        Ok((namespace, name))
    }

    /// The app a write is made by, for the `managed_by` stamp: the caller's
    /// own app identity, or the app acting for one of its objects (an app
    /// writing objects for another object manages them). A delegated
    /// identity (an app acting for a viewer) counts as the viewer: `None`.
    fn writing_app<T>(&self, request: &Request<T>, identity: &AclIdentity) -> Option<String> {
        if let Some(actor) = &identity.acting_for {
            return Some(actor.app.clone());
        }
        self.calling_app(request, identity)
    }

    /// Ask the app whether it accepts an object (app-object-kinds plan
    /// §6.6). `Err` says why the app could not answer, which lets the write
    /// through with a warning: the call is advice, never a control.
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
    // then `write` on its access resource on the object's scope, before the
    // object is read; then the kind's `requires`, resolved from the spec,
    // as the caller, fail-closed.
    #[instrument(skip_all, fields(kind, object))]
    pub(super) async fn apply_object_rpc(
        &self,
        request: Request<ApplyObjectRequest>,
    ) -> Result<Response<ApplyObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let writer_app = self.writing_app(&request, &identity);
        let inner = request.into_inner();
        if inner.object_json.len() > MAX_SPEC_BYTES * 4 {
            return Err(Status::invalid_argument(format!(
                "the object is {} bytes; a spec is at most {MAX_SPEC_BYTES}",
                inner.object_json.len()
            )));
        }
        let value: Value = serde_json::from_str(&inner.object_json)
            .map_err(|err| Status::invalid_argument(format!("the object is not JSON: {err}")))?;
        let registry = self.object_registry()?;
        self.gate_objects(&identity, &registry, AclAction::Write)?;
        let input = parse_object_input(&value).map_err(Status::invalid_argument)?;
        let kind = registry
            .resolve_qualified(&input.kind)
            .cloned()
            .map_err(|lookup| lookup_status(&lookup, input.kind.trim(), true))?;
        let qualified = kind.qualified();
        if !kind.is_active() {
            return Err(Status::failed_precondition(format!(
                "kind {qualified} is orphaned: app {} no longer declares it, so its objects can be \
                 read and deleted but not written\nHint: install (or upgrade) {} with the kind to \
                 write them again",
                kind.app, kind.app
            )));
        }
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
                    "{qualified} is cluster-scoped: its objects have no namespace; remove \
                     `namespace` from {}",
                    input.name
                )));
            }
            if !inner.namespace.trim().is_empty() {
                warnings.push(format!(
                    "{qualified} is cluster-scoped: namespace {} ignored",
                    inner.namespace.trim()
                ));
            }
            String::new()
        };
        let name = input.name.trim().to_string();
        validate_object_name(&name).map_err(Status::invalid_argument)?;
        let id = object_id(&namespace, &name);
        tracing::Span::current().record("kind", qualified.as_str());
        tracing::Span::current().record("object", id.as_str());
        self.enforce_access(&identity, &kind, AclAction::Write, &namespace, &name)?;

        validate_labels(&input.labels).map_err(Status::invalid_argument)?;
        if kind.is_namespaced() {
            self.ensure_namespace_exists(&namespace)?;
        }
        let effective = kind.check_spec(&input.spec).map_err(|errors| {
            Status::invalid_argument(format!(
                "{qualified} {id}: {}\nHint: `legion object explain {qualified}` shows the \
                 kind's fields",
                errors.join("; ")
            ))
        })?;
        let spec_text = serde_json::to_string(&effective)
            .map_err(|err| Status::internal(format!("encoding the spec: {err}")))?;
        if spec_text.len() > MAX_SPEC_BYTES {
            return Err(Status::invalid_argument(format!(
                "{qualified} {id}: the spec is {} bytes with its defaults; the limit is \
                 {MAX_SPEC_BYTES}",
                spec_text.len()
            )));
        }
        let doc = object_document(&name, &namespace, &input.labels, &effective, &Value::Null);
        let permissions = self.enforce_requirements(&identity, &kind, &doc, &namespace, &id)?;

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
        let mut seen = BTreeMap::new();
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
                object: match current {
                    Some(row) => Some(self.read_proto(&row, Some(&kind), &mut seen)?),
                    None => None,
                },
                message,
                deduplicated: true,
                ..Default::default()
            }));
        }
        let current = self
            .store
            .app_object(&key)
            .map_err(|e| map_store_error("Error reading the object", e))?;
        if let Some(row) = current.as_ref().filter(|row| row.is_deleting()) {
            return Err(Status::failed_precondition(format!(
                "{qualified} {id} is being deleted (its cleanup runs until {}); its spec is \
                 frozen\nHint: wait for the delete to finish, then apply it again",
                row.cleanup_deadline
            )));
        }
        if let Some(row) = current.as_ref() {
            let managed =
                !row.managed_by.is_empty() && writer_app.as_deref() != Some(&row.managed_by);
            if managed && !inner.force {
                return Err(Status::failed_precondition(format!(
                    "{qualified} {id} is managed by the {} app, which will likely overwrite a \
                     change\nHint: pass --force to change it anyway",
                    row.managed_by
                )));
            }
        }
        let cas = VersionCas::from_expected(inner.expected_version);
        let conflict = |current: Option<&AppObjectRow>| -> Status {
            match (current, cas) {
                (Some(row), VersionCas::Missing) => Status::aborted(format!(
                    "{qualified} {id} already exists (spec version {}); expected_version 0 creates \
                     only",
                    row.spec_version
                )),
                (Some(row), _) => Status::aborted(format!(
                    "{qualified} {id} is at spec version {}, not {}: it changed since you read \
                     it\nHint: read it again (legion object get {qualified} {id}) and apply on top \
                     of it",
                    row.spec_version,
                    inner.expected_version.unwrap_or_default()
                )),
                (None, _) => Status::aborted(format!(
                    "{qualified} {id} does not exist, and expected_version {} names a stored \
                     version\nHint: pass 0 (or no expected version) to create it",
                    inner.expected_version.unwrap_or_default()
                )),
            }
        };
        if !cas.holds(current.as_ref()) {
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
        if current.is_some() {
            let before = object_document(
                &name,
                &namespace,
                &previous_labels,
                &kind.schema.filled(&previous_spec),
                &Value::Null,
            );
            let changes = immutable_changes(&kind, &before, &doc);
            if !changes.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "{qualified} {id}: {}\nHint: delete and recreate the object to change an \
                     immutable field",
                    changes.join("; ")
                )));
            }
        }
        let spec_changed = current.as_ref().is_none_or(|row| row.spec != spec_text);
        let mut diff = Vec::new();
        if current.is_some() {
            let secret = |path: &[String]| kind.schema.is_write_only(path);
            diff.extend(spec_diff("spec", &previous_spec, &effective, &secret));
            diff.extend(spec_diff(
                "labels",
                &serde_json::to_value(&previous_labels).unwrap_or_default(),
                &serde_json::to_value(&input.labels).unwrap_or_default(),
                &|_: &[String]| false,
            ));
        }

        // The app's advice, when the kind asks for it and the spec is new or
        // changed. Fails open: it is never a security check.
        if kind.decl.validate && spec_changed {
            let call = ObjectValidateCall {
                kind: qualified.clone(),
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
                            "{qualified} {id}: the {} app refused it: {}\nHint: fix the object \
                             and apply it again",
                            kind.app,
                            answer.errors.join("; ")
                        )));
                    }
                    warnings.extend(
                        answer
                            .warnings
                            .into_iter()
                            .map(|warning| format!("{qualified} {id}: {warning}")),
                    );
                }
                Err(why) => {
                    info!(kind = %qualified, object = %id, "Object not validated by its app: {why}");
                    warnings.push(format!(
                        "{qualified} {id} was not checked by the {} app ({why}); the schema and \
                         requirement checks passed",
                        kind.app
                    ));
                }
            }
        }

        // The authority the write records: what was just verified, by whom.
        // An unchanged authority keeps its recorded time, so re-applying the
        // same object as the same writer writes nothing.
        let authority = AuthorityDoc {
            permissions,
            authorized_by: identity.principal.clone(),
            accessor: identity.accessor_id.clone(),
            authorized_ms: now_ms(),
            requires_digest: requires_digest(&kind.decl),
        };
        let authority_text = match current
            .as_ref()
            .and_then(|row| serde_json::from_str::<AuthorityDoc>(&row.authority).ok())
        {
            Some(stored)
                if stored.permissions == authority.permissions
                    && stored.authorized_by == authority.authorized_by
                    && stored.accessor == authority.accessor
                    && stored.requires_digest == authority.requires_digest =>
            {
                current
                    .as_ref()
                    .map(|row| row.authority.clone())
                    .unwrap_or_default()
            }
            _ => json_text(&serde_json::to_value(&authority).unwrap_or_default()),
        };
        let managed_by = writer_app.clone().unwrap_or_default();
        let shard = object_shard(
            &kind.app,
            &kind.decl.name,
            &namespace,
            &name,
            STORED_SHARD_COUNT,
        );

        if inner.dry_run {
            let now = now_ms();
            let would = AppObjectRow {
                key: key.clone(),
                uid: current.as_ref().map_or(0, |row| row.uid),
                spec: spec_text.clone(),
                labels: input.labels.clone(),
                generation: match &current {
                    None => 1,
                    Some(row) if spec_changed => row.generation + 1,
                    Some(row) => row.generation,
                },
                spec_version: current.as_ref().map_or(0, |row| row.spec_version),
                authority: authority_text.clone(),
                managed_by: managed_by.clone(),
                deleting_since: 0,
                cleanup_deadline: 0,
                shard,
                schema_digest: kind.schema.digest(),
                created_by: current
                    .as_ref()
                    .map(|row| row.created_by.clone())
                    .unwrap_or_else(|| identity.principal.clone()),
                created_ms: current.as_ref().map_or(now, |row| row.created_ms),
                updated_by: identity.principal.clone(),
                updated_ms: now,
            };
            let labels_changed = current
                .as_ref()
                .is_none_or(|row| row.labels != input.labels);
            let verb = match &current {
                None => "would be created".to_string(),
                Some(row) if spec_changed => format!(
                    "would change (generation {} → {})",
                    row.generation,
                    row.generation + 1
                ),
                Some(_) if labels_changed => "would change its labels".to_string(),
                Some(row) if row.authority != authority_text => {
                    "would record you as its authorizer".to_string()
                }
                Some(_) => "unchanged".to_string(),
            };
            return Ok(Response::new(ApplyObjectReply {
                object: Some(self.read_proto(&would, Some(&kind), &mut seen)?),
                created: current.is_none(),
                changed: verb != "unchanged",
                diff,
                warnings,
                message: format!("{qualified} {id}: {verb}; dry run, nothing written"),
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
                authority: authority_text,
                managed_by,
                schema_digest: kind.schema.digest(),
                shard,
                principal: identity.principal.clone(),
                now_ms: now_ms(),
                cas,
                max_objects: kind.decl.max_objects(),
            })
            .map_err(|e| map_store_error("Error writing the object", e))?;
        let (row, created, changed) = match outcome {
            PutObject::Written { row, created } => (row, created, true),
            PutObject::Unchanged { row } => (row, false, false),
            PutObject::Conflict { current } => return Err(conflict(current.as_ref())),
            PutObject::Deleting { .. } => {
                return Err(Status::failed_precondition(format!(
                    "{qualified} {id} is being deleted; its spec is frozen"
                )))
            }
            PutObject::TooMany { count } => {
                return Err(Status::resource_exhausted(format!(
                    "{qualified} already holds {count} objects, its maxObjects\nHint: delete \
                     objects of the kind, or ask the app's author to raise maxObjects"
                )))
            }
        };
        let message = format!(
            "{qualified} {id}: {} (generation {}, spec version {})",
            if created {
                "created"
            } else if changed {
                "updated"
            } else {
                "unchanged"
            },
            row.generation,
            row.spec_version
        );
        if changed {
            self.store.events().object_applied(
                &event_ref(&row, kind.access.as_str()),
                row.generation,
                created,
                &identity.principal,
            );
            info!(
                kind = %qualified,
                object = %id,
                uid = row.uid,
                generation = row.generation,
                spec_version = row.spec_version,
                created,
                accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
                "Object applied"
            );
        }
        self.store
            .idempotency_record(&idempotency_key, "apply_object", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(ApplyObjectReply {
            object: Some(self.read_proto(&row, Some(&kind), &mut seen)?),
            created,
            changed,
            diff,
            warnings,
            message,
            deduplicated: false,
            dry_run: false,
        }))
    }

    // ACL: `read` on the kind's access resource on the object's scope,
    // checked before the object is looked up.
    pub(super) async fn get_object_rpc(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        self.gate_objects(&identity, &registry, AclAction::Read)?;
        let kind = self.lookup_kind(&registry, &inner.kind)?;
        let (namespace, name) = self.object_ref(&kind, &inner.name, &inner.namespace)?;
        self.enforce_access(&identity, &kind, AclAction::Read, &namespace, &name)?;
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
                    "{} {} not found\nHint: `legion object list {}{}` lists them",
                    kind.qualified(),
                    object_id(&namespace, &name),
                    kind.qualified(),
                    if namespace.is_empty() {
                        String::new()
                    } else {
                        format!(" -n {namespace}")
                    }
                ))
            })?;
        let mut seen = BTreeMap::new();
        Ok(Response::new(GetObjectReply {
            object: Some(self.read_proto(&row, Some(&kind), &mut seen)?),
            kind: Some(kind_proto(&kind)),
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
        let (filter, named) = if inner.kind.trim().is_empty() {
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
            self.gate_objects(&identity, &registry, AclAction::Read)?;
            let kind = self.lookup_kind(&registry, &inner.kind)?;
            acl::enforce(&identity, kind.access, AclAction::Read, &AclScope::any())?;
            (
                ObjectFilter {
                    app: Some(kind.app.clone()),
                    kind: Some(kind.decl.name.clone()),
                    shards: None,
                },
                Some(kind),
            )
        };
        // Re-resolving the authorizers' tokens reveals what other
        // principals hold now: ACL state, behind `acl:read`.
        if inner.authority_lapsed {
            acl::enforce(
                &identity,
                AclResource::Acl,
                AclAction::Read,
                &AclScope::none(),
            )?;
        }
        let rows = self
            .store
            .app_objects(&filter)
            .map_err(|e| map_store_error("Error listing objects", e))?;
        let statuses = self
            .store
            .app_object_statuses(filter.app.as_deref())
            .map_err(|e| map_store_error("Error reading object statuses", e))?;
        let mut authorizers = BTreeMap::new();
        let mut seen = BTreeMap::new();
        let mut objects = Vec::new();
        for row in rows
            .iter()
            .filter(|row| row.key.namespace.is_empty() || selection.matches(&row.key.namespace))
            .filter(|row| {
                inner
                    .labels
                    .iter()
                    .all(|(key, value)| row.labels.get(key) == Some(value))
            })
        {
            let kind = registry.get(&row.key.app, &row.key.kind);
            let readable = match kind {
                Some(kind) => identity.allows(
                    kind.access,
                    AclAction::Read,
                    &kind.access_scope(&row.key.namespace, &row.key.name),
                ),
                None => identity.allows(
                    AclResource::Object,
                    AclAction::Read,
                    &crate::structs::app_object::access_scope(
                        AclResource::Object,
                        &qualified_kind(&row.key.app, &row.key.kind),
                        &row.key.namespace,
                        &row.key.name,
                    ),
                ),
            };
            if !readable {
                continue;
            }
            let check = if inner.authority_lapsed {
                let check = self.authority_check(row, &mut authorizers);
                if check.0 == AuthorityCheck::HELD {
                    continue;
                }
                Some(check)
            } else {
                None
            };
            let running = self.app_running(&row.key.app, &mut seen);
            let mut object = object_proto(
                row,
                &ProtoContext {
                    kind,
                    status: statuses.get(&row.uid),
                    app_running: running,
                    reveal: false,
                    shard: 0,
                },
            );
            if let (Some((check, detail)), Some(authority)) = (check, object.authority.as_mut()) {
                authority.check = check.to_string();
                authority.check_detail = detail;
            }
            objects.push(object);
        }
        let mut kinds: Vec<ObjectKind> = Vec::new();
        let mut listed = BTreeSet::new();
        if let Some(kind) = &named {
            listed.insert(kind.qualified());
            kinds.push(kind_proto(kind));
        }
        for object in &objects {
            if let Some(kind) = registry.get(&object.app, &object.kind) {
                if listed.insert(kind.qualified()) {
                    kinds.push(kind_proto(kind));
                }
            }
        }
        Ok(Response::new(ListObjectsReply { objects, kinds }))
    }

    /// Whether the principal that last authorized `row` still holds every
    /// permission of its recorded authority (`ListObjects` with
    /// `authority_lapsed`): re-resolved through its token's accessor, as a
    /// request with that token would authenticate now. `authorizers`
    /// caches the resolution per accessor for one listing. Nothing lapses
    /// with ACLs off, or for an object whose authority is empty.
    fn authority_check(
        &self,
        row: &AppObjectRow,
        authorizers: &mut BTreeMap<String, Result<Option<AclIdentity>, Status>>,
    ) -> (&'static str, String) {
        if !self.auth.enabled {
            return (AuthorityCheck::HELD, String::new());
        }
        let authority: AuthorityDoc = serde_json::from_str(&row.authority).unwrap_or_default();
        if authority.permissions.is_empty() {
            return (AuthorityCheck::HELD, String::new());
        }
        let who = authority.authorized_by.clone();
        let Some(accessor) = authority.accessor.as_deref().filter(|a| !a.is_empty()) else {
            return (
                AuthorityCheck::UNKNOWN,
                format!(
                    "{who} authorized it without an ACL token (a JWT or SSO login, or a \
                     workload identity), so its rights cannot be re-resolved"
                ),
            );
        };
        let resolved = authorizers
            .entry(accessor.to_string())
            .or_insert_with(|| self.auth.identity_for_accessor(accessor, &self.store));
        match resolved {
            Ok(None) => (
                AuthorityCheck::LAPSED,
                format!("{who}'s token (accessor {accessor}) was deleted"),
            ),
            Err(status) if status.code() == tonic::Code::Unauthenticated => (
                AuthorityCheck::LAPSED,
                format!("{who}'s token (accessor {accessor}): {}", status.message()),
            ),
            Err(status) => (
                AuthorityCheck::UNKNOWN,
                format!(
                    "{who}'s token (accessor {accessor}) could not be read: {}",
                    status.message()
                ),
            ),
            Ok(Some(holder)) => {
                let missing: Vec<String> = authority
                    .permissions
                    .iter()
                    .filter(|permission| {
                        match (
                            AclResource::parse(&permission.resource),
                            AclAction::parse(&permission.action),
                        ) {
                            (Some(resource), Some(action)) => {
                                !holder.would_allow(resource, action, &permission.scope())
                            }
                            _ => true,
                        }
                    })
                    .map(ResolvedPermission::label)
                    .collect();
                if missing.is_empty() {
                    (AuthorityCheck::HELD, String::new())
                } else {
                    (
                        AuthorityCheck::LAPSED,
                        format!("{who} no longer holds {}", missing.join("; ")),
                    )
                }
            }
        }
    }

    /// Delete one object as `identity`, after `enforce_access`: the managed
    /// check, `requires` against its spec, then the delete (a bounded
    /// cleanup when the kind has one and `now` is not set).
    #[allow(clippy::too_many_arguments)]
    fn delete_object_checked(
        &self,
        identity: &AclIdentity,
        kind: &InstalledKind,
        row: &AppObjectRow,
        expected: Option<u64>,
        now: bool,
        force: bool,
        writer_app: Option<&str>,
    ) -> Result<DeleteObject, Status> {
        let qualified = kind.qualified();
        let id = object_id(&row.key.namespace, &row.key.name);
        let managed = !row.managed_by.is_empty() && writer_app != Some(row.managed_by.as_str());
        if managed && !force {
            return Err(Status::failed_precondition(format!(
                "{qualified} {id} is managed by the {} app\nHint: pass --force to delete it \
                 anyway",
                row.managed_by
            )));
        }
        let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
        let doc = object_document(
            &row.key.name,
            &row.key.namespace,
            &row.labels,
            &kind.schema.filled(&spec),
            &Value::Null,
        );
        self.enforce_requirements(identity, kind, &doc, &row.key.namespace, &id)?;
        let deadline = match kind.cleanup {
            Some(window) if !now && kind.is_active() => {
                Some(now_ms() + i64::try_from(window.as_millis()).unwrap_or(i64::MAX / 2))
            }
            _ => None,
        };
        self.store
            .delete_app_object(&row.key, expected, deadline, now_ms())
            .map_err(|e| map_store_error("Error deleting the object", e))
    }

    // ACL: `write` on the kind's access resource on the object's scope,
    // before the lookup; then the object's `requires` as the caller.
    #[instrument(skip_all, fields(kind, object))]
    pub(super) async fn delete_object_rpc(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let writer_app = self.writing_app(&request, &identity);
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        self.gate_objects(&identity, &registry, AclAction::Write)?;
        let kind = self.lookup_kind(&registry, &inner.kind)?;
        let qualified = kind.qualified();
        let (namespace, name) = self.object_ref(&kind, &inner.name, &inner.namespace)?;
        let id = object_id(&namespace, &name);
        tracing::Span::current().record("kind", qualified.as_str());
        tracing::Span::current().record("object", id.as_str());
        self.enforce_access(&identity, &kind, AclAction::Write, &namespace, &name)?;
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
                ..Default::default()
            }));
        }
        let key = ObjectKey {
            app: kind.app.clone(),
            kind: kind.decl.name.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
        };
        let row = self
            .store
            .app_object(&key)
            .map_err(|e| map_store_error("Error reading the object", e))?
            .ok_or_else(|| Status::not_found(format!("{qualified} {id} not found")))?;
        let outcome = self.delete_object_checked(
            &identity,
            &kind,
            &row,
            inner.expected_version,
            inner.now,
            inner.force,
            writer_app.as_deref(),
        )?;
        let mut seen = BTreeMap::new();
        let access = kind.access.as_str();
        let (message, object, deleted, deleting) = match outcome {
            DeleteObject::Deleted(row) => {
                self.store
                    .events()
                    .object_deleted(&event_ref(&row, access), &identity.principal);
                (
                    format!(
                        "{qualified} {id}: deleted (was generation {}, spec version {})",
                        row.generation, row.spec_version
                    ),
                    self.read_proto(&row, Some(&kind), &mut seen)?,
                    true,
                    false,
                )
            }
            DeleteObject::Deleting(row) => {
                self.store.events().object_deleting(
                    &event_ref(&row, access),
                    &identity.principal,
                    row.cleanup_deadline,
                );
                (
                    format!(
                        "{qualified} {id}: deleting; the {} app has until its cleanup deadline \
                         ({}) to release it (`--now` skips the cleanup)",
                        kind.app,
                        kind.decl.cleanup.clone().unwrap_or_default()
                    ),
                    self.read_proto(&row, Some(&kind), &mut seen)?,
                    false,
                    true,
                )
            }
            DeleteObject::AlreadyDeleting(row) => (
                format!(
                    "{qualified} {id}: already deleting; its app releases it, or its cleanup \
                     deadline passes (`--now` removes it at once)"
                ),
                self.read_proto(&row, Some(&kind), &mut seen)?,
                false,
                true,
            ),
            DeleteObject::Missing => {
                return Err(Status::not_found(format!("{qualified} {id} not found")))
            }
            DeleteObject::Conflict(row) => {
                return Err(Status::aborted(format!(
                    "{qualified} {id} is at spec version {}, not {}: it changed since you read \
                     it\nHint: read it again and delete that version",
                    row.spec_version,
                    inner.expected_version.unwrap_or_default()
                )))
            }
        };
        info!(
            kind = %qualified,
            object = %id,
            deleted,
            deleting,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Object delete"
        );
        self.store
            .idempotency_record(&idempotency_key, "delete_object", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(DeleteObjectReply {
            message,
            deduplicated: false,
            object: Some(object),
            deleted,
            deleting,
        }))
    }

    /// The live objects in `namespace` for a cascade, each checked as
    /// `identity` would be deleting it alone: `write` on its kind's access
    /// and its `requires`. Nothing is deleted unless every one passes.
    pub(super) fn namespace_objects_for_cascade(
        &self,
        identity: &AclIdentity,
        namespace: &str,
    ) -> Result<Vec<(InstalledKind, AppObjectRow)>, Status> {
        let registry = self.object_registry()?;
        let rows = self
            .store
            .app_objects_in_namespace(namespace)
            .map_err(|e| map_store_error("Error reading the namespace's objects", e))?;
        let mut objects = Vec::new();
        for row in rows.into_iter().filter(|row| !row.is_deleting()) {
            let Some(kind) = registry.get(&row.key.app, &row.key.kind).cloned() else {
                return Err(Status::failed_precondition(format!(
                    "{} {} has no recorded kind; delete it with `legion object delete` first",
                    qualified_kind(&row.key.app, &row.key.kind),
                    object_id(&row.key.namespace, &row.key.name)
                )));
            };
            self.enforce_access(
                identity,
                &kind,
                AclAction::Write,
                &row.key.namespace,
                &row.key.name,
            )?;
            let spec: Value = serde_json::from_str(&row.spec).unwrap_or(Value::Null);
            let doc = object_document(
                &row.key.name,
                &row.key.namespace,
                &row.labels,
                &kind.schema.filled(&spec),
                &Value::Null,
            );
            let id = object_id(&row.key.namespace, &row.key.name);
            self.enforce_requirements(identity, &kind, &doc, &row.key.namespace, &id)?;
            objects.push((kind, row));
        }
        Ok(objects)
    }

    /// Delete a cascade's checked objects (a cleanup kind's start their
    /// cleanup): one step line each.
    pub(super) fn cascade_delete_objects(
        &self,
        identity: &AclIdentity,
        objects: Vec<(InstalledKind, AppObjectRow)>,
    ) -> Result<Vec<String>, Status> {
        let mut steps = Vec::new();
        for (kind, row) in objects {
            let deadline = kind
                .cleanup
                .filter(|_| kind.is_active())
                .map(|window| now_ms() + i64::try_from(window.as_millis()).unwrap_or(i64::MAX / 2));
            let label = format!(
                "{} {}",
                kind.qualified(),
                object_id(&row.key.namespace, &row.key.name)
            );
            match self
                .store
                .delete_app_object(&row.key, None, deadline, now_ms())
                .map_err(|e| map_store_error("Error deleting an object", e))?
            {
                DeleteObject::Deleted(row) => {
                    self.store.events().object_deleted(
                        &event_ref(&row, kind.access.as_str()),
                        &identity.principal,
                    );
                    steps.push(format!("Deleted {label}"));
                }
                DeleteObject::Deleting(row) => {
                    self.store.events().object_deleting(
                        &event_ref(&row, kind.access.as_str()),
                        &identity.principal,
                        row.cleanup_deadline,
                    );
                    steps.push(format!(
                        "Started {label}'s cleanup (its app releases it within {})",
                        kind.decl.cleanup.clone().unwrap_or_default()
                    ));
                }
                _ => {}
            }
        }
        Ok(steps)
    }

    /// The app an app-plane object call is about: the calling app (which
    /// may only name itself), or, for a management token (or audit-only
    /// mode), `None`, meaning the request names it. Everyone else is
    /// refused, and the denial audited.
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
                if identity.source.is_acting() {
                    return Err(Status::permission_denied(format!(
                        "{rpc} takes the app's own identity, not one acting for a viewer or an \
                         object ({}): send the call without legion-on-behalf-of or \
                         legion-for-object",
                        identity.principal
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

    /// The kind an app-plane call names: one of the caller's own kinds
    /// (`app`), or any active kind for a management caller.
    fn app_plane_kind(
        &self,
        registry: &ObjectKindRegistry,
        caller: Option<&str>,
        typed: &str,
    ) -> Result<InstalledKind, Status> {
        let typed = typed.trim();
        let kind = match caller {
            Some(app) => {
                let bare = match typed.split_once('/') {
                    Some((owner, kind)) if owner == app => kind,
                    Some((owner, _)) => {
                        return Err(Status::permission_denied(format!(
                            "the {app} app reaches only its own kinds; {typed} is the {owner} \
                             app's"
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
            None => self.lookup_kind(registry, typed)?,
        };
        if !kind.is_active() {
            return Err(Status::failed_precondition(format!(
                "kind {} is orphaned: app {} no longer declares it",
                kind.qualified(),
                kind.app
            )));
        }
        Ok(kind)
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
            let kind = self.app_plane_kind(&registry, Some(&app), typed)?;
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
            // What was sent, per object: what makes it worth sending again.
            let mut sent: Option<BTreeMap<ObjectKey, (u64, u64, String)>> = None;
            let mut sequence = 0u64;
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
                    source.store.app_object_fingerprint(&app),
                    source.store.object_kind_fingerprint(),
                    source.store.app_fingerprint(),
                ) {
                    (Ok((count, version)), Ok((kinds, revision)), Ok(apps)) => {
                        format!("{count}|{version}|{kinds}|{revision}|{apps}")
                    }
                    (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
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
                let marks: BTreeMap<ObjectKey, (u64, u64, String)> = current
                    .iter()
                    .map(|(key, object)| {
                        (
                            key.clone(),
                            (object.uid, object.spec_version, object.state.clone()),
                        )
                    })
                    .collect();
                let mut messages: Vec<ObjectEvent> = Vec::new();
                match &sent {
                    None => {
                        let chunks = watch_chunks(current.into_values().collect());
                        let last = chunks.len() - 1;
                        for (index, upserts) in chunks.into_iter().enumerate() {
                            messages.push(ObjectEvent {
                                snapshot: index == 0,
                                snapshot_done: index == last,
                                upserts,
                                deletes: Vec::new(),
                                sequence: 0,
                            });
                        }
                    }
                    Some(previous) => {
                        let upserts: Vec<AppObject> = current
                            .iter()
                            .filter(|(key, _)| previous.get(*key) != marks.get(*key))
                            .map(|(_, object)| object.clone())
                            .collect();
                        let deletes: Vec<ObjectRef> = previous
                            .iter()
                            .filter(|(key, (uid, _, _))| {
                                marks.get(*key).is_none_or(|(now, _, _)| now != uid)
                            })
                            .map(|(key, (uid, _, _))| ObjectRef {
                                app: key.app.clone(),
                                kind: key.kind.clone(),
                                namespace: key.namespace.clone(),
                                name: key.name.clone(),
                                uid: *uid,
                            })
                            .collect();
                        if upserts.is_empty() && deletes.is_empty() {
                            continue;
                        }
                        let chunks = if upserts.is_empty() {
                            Vec::new()
                        } else {
                            watch_chunks(upserts)
                        };
                        for (index, chunk) in chunks.into_iter().enumerate() {
                            messages.push(ObjectEvent {
                                upserts: chunk,
                                deletes: if index == 0 {
                                    deletes.clone()
                                } else {
                                    Vec::new()
                                },
                                ..Default::default()
                            });
                        }
                        if messages.is_empty() {
                            messages.push(ObjectEvent {
                                deletes,
                                ..Default::default()
                            });
                        }
                    }
                }
                for message in messages {
                    sequence += 1;
                    if tx
                        .send(Ok(ObjectEvent {
                            sequence,
                            ..message
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                sent = Some(marks);
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
        let kind = self.app_plane_kind(&registry, caller.as_deref(), &inner.kind)?;
        let qualified = kind.qualified();
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
        tracing::Span::current().record("kind", qualified.as_str());
        tracing::Span::current().record("object", id.as_str());
        let mut conditions: Vec<ConditionDoc> =
            inner.conditions.iter().map(condition_doc).collect();
        validate_conditions(&mut conditions)
            .map_err(|err| Status::invalid_argument(format!("{qualified} {id}: {err}")))?;
        let fields: Value = match inner.fields_json.trim() {
            "" => json!({}),
            raw => serde_json::from_str(raw).map_err(|err| {
                Status::invalid_argument(format!("{qualified} {id}: status.fields: {err}"))
            })?,
        };
        let fields_text = json_text(&fields);
        if fields_text.len() > MAX_STATUS_FIELDS_BYTES {
            return Err(Status::invalid_argument(format!(
                "{qualified} {id}: status.fields is {} bytes; the limit is \
                 {MAX_STATUS_FIELDS_BYTES} (status is for facts, not telemetry)",
                fields_text.len()
            )));
        }
        kind.check_fields(&fields).map_err(|errors| {
            Status::invalid_argument(format!("{qualified} {id}: {}", errors.join("; ")))
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
        let reply_object = |svc: &Self, row: &AppObjectRow| -> Result<AppObject, Status> {
            let status = svc
                .store
                .app_object_status(row.uid)
                .map_err(|e| map_store_error("Error reading the object's status", e))?;
            Ok(object_proto(
                row,
                &ProtoContext {
                    kind: Some(&kind),
                    status: status.as_ref(),
                    app_running: true,
                    reveal: true,
                    shard: 0,
                },
            ))
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
                object: match current {
                    Some(row) => Some(reply_object(self, &row)?),
                    None => None,
                },
                written: false,
                deduplicated: true,
                message,
            }));
        }
        let previous = self
            .store
            .app_object_status(inner.uid)
            .map_err(|e| map_store_error("Error reading the object's status", e))?
            .map(|row| status_doc(&row));
        stamp_condition_times(&mut conditions, previous.as_ref(), &now_rfc3339());
        let next = StatusDoc {
            observed_generation: inner.observed_generation,
            conditions: conditions.clone(),
            fields: fields.clone(),
        };
        let outcome = self
            .store
            .update_app_object_status(StatusWrite {
                key: key.clone(),
                uid: inner.uid,
                observed_generation: inner.observed_generation,
                conditions: json_text(&serde_json::to_value(&conditions).unwrap_or_default()),
                fields: fields_text,
                principal: identity.principal.clone(),
                now_ms: now_ms(),
            })
            .map_err(|e| map_store_error("Error writing the object's status", e))?;
        let written = match outcome {
            PutStatus::Written { .. } => true,
            PutStatus::Unchanged(_) => false,
            PutStatus::Missing => {
                return Err(Status::not_found(format!(
                    "{qualified} {id} with uid {} not found (deleted, or recreated with another \
                     uid)",
                    inner.uid
                )))
            }
            PutStatus::Ahead { generation } => {
                return Err(Status::aborted(format!(
                    "{qualified} {id}: observed generation {} is ahead of generation {generation} \
                     on this manager; retry (the spec write may still be replicating)",
                    inner.observed_generation
                )))
            }
            PutStatus::Behind { stored } => {
                return Err(Status::aborted(format!(
                    "{qualified} {id}: observed generation {} is behind the stored status's \
                     ({stored}); a newer report already landed",
                    inner.observed_generation
                )))
            }
        };
        let row = self
            .store
            .app_object(&key)
            .map_err(|e| map_store_error("Error reading the object", e))?
            .ok_or_else(|| Status::not_found(format!("{qualified} {id} not found")))?;
        if written {
            crate::manager::telemetry::metrics()
                .object_status_writes_total
                .inc(&[&kind.app]);
            for (condition, from, to) in condition_transitions(previous.as_ref(), &next) {
                let reason = next
                    .condition(&condition)
                    .map(|c| c.reason.clone())
                    .unwrap_or_default();
                self.store.events().object_status_changed(
                    &event_ref(&row, kind.access.as_str()),
                    &condition,
                    &from,
                    &to,
                    &reason,
                    inner.observed_generation,
                );
            }
        }
        let message = format!(
            "{qualified} {id}: status {} (observed generation {})",
            if written { "written" } else { "unchanged" },
            inner.observed_generation
        );
        self.store
            .idempotency_record(&idempotency_key, "update_object_status", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(UpdateObjectStatusReply {
            object: Some(reply_object(self, &row)?),
            written,
            deduplicated: false,
            message,
        }))
    }

    // ACL: app-plane (the workload identity of the app that owns the kind,
    // or management).
    #[instrument(skip_all, fields(kind, object))]
    pub(super) async fn release_object_rpc(
        &self,
        request: Request<ReleaseObjectRequest>,
    ) -> Result<Response<ReleaseObjectReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let caller = self.object_plane_app(&request, &identity, "", "ReleaseObject")?;
        let inner = request.into_inner();
        let registry = self.object_registry()?;
        let kind = self.app_plane_kind(&registry, caller.as_deref(), &inner.kind)?;
        let qualified = kind.qualified();
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
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "release_object",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(ReleaseObjectReply {
                message,
                deduplicated: true,
            }));
        }
        let key = ObjectKey {
            app: kind.app.clone(),
            kind: kind.decl.name.clone(),
            namespace,
            name,
        };
        let row = match self
            .store
            .release_app_object(&key, inner.uid)
            .map_err(|e| map_store_error("Error releasing the object", e))?
        {
            ReleaseObject::Released(row) => row,
            ReleaseObject::Missing => {
                return Err(Status::not_found(format!(
                    "{qualified} {id} with uid {} not found",
                    inner.uid
                )))
            }
            ReleaseObject::NotDeleting(_) => {
                return Err(Status::failed_precondition(format!(
                    "{qualified} {id} is not being deleted; only a deleting object is released"
                )))
            }
        };
        self.store
            .events()
            .object_deleted(&event_ref(&row, kind.access.as_str()), &identity.principal);
        let message = format!("{qualified} {id}: released and deleted");
        self.store
            .idempotency_record(&idempotency_key, "release_object", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(ReleaseObjectReply {
            message,
            deduplicated: false,
        }))
    }
}

/// What a `WatchObjects` task reads from: the store and the kind cache.
struct ObjectWatchSource {
    store: Store,
    object_registry: ObjectRegistryCache,
}

impl ObjectWatchSource {
    /// `app`'s objects of the active kinds it declares (only `kinds`, when
    /// any), in the watched shards, with `writeOnly` values shown: the
    /// owning app is the one reader that sees them.
    fn objects(
        &self,
        app: &str,
        kinds: &BTreeSet<String>,
        shard_count: u32,
        shards: &BTreeSet<u32>,
    ) -> Result<BTreeMap<ObjectKey, AppObject>, Status> {
        let registry = cached_object_registry(&self.store, &self.object_registry, false)?;
        // The stored shard column answers the autoscaler's count in the
        // query; any other count filters here.
        let stored_shards =
            (shard_count == STORED_SHARD_COUNT && !shards.is_empty()).then(|| shards.clone());
        let rows = self
            .store
            .app_objects(&ObjectFilter {
                app: Some(app.to_string()),
                kind: None,
                shards: stored_shards,
            })
            .map_err(|e| map_store_error("Error reading objects", e))?;
        let statuses = self
            .store
            .app_object_statuses(Some(app))
            .map_err(|e| map_store_error("Error reading object statuses", e))?;
        let mut objects = BTreeMap::new();
        for row in rows {
            if !kinds.is_empty() && !kinds.contains(&row.key.kind) {
                continue;
            }
            // An orphaned kind's objects are no longer the app's.
            let Some(kind) = registry
                .get(app, &row.key.kind)
                .filter(|kind| kind.is_active())
            else {
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
            let object = object_proto(
                &row,
                &ProtoContext {
                    kind: Some(kind),
                    status: statuses.get(&row.uid),
                    app_running: true,
                    reveal: true,
                    shard,
                },
            );
            objects.insert(row.key.clone(), object);
        }
        Ok(objects)
    }
}

/// The kinds from `cache`, rebuilt when the kinds or the app records moved.
/// `log` warns about declarations that no longer compile.
fn cached_object_registry(
    store: &Store,
    cache: &ObjectRegistryCache,
    log: bool,
) -> Result<Arc<ObjectKindRegistry>, Status> {
    let (count, revision) = store
        .object_kind_fingerprint()
        .map_err(|e| map_store_error("Error reading object kinds", e))?;
    let apps = store
        .app_fingerprint()
        .map_err(|e| map_store_error("Error reading installed apps", e))?;
    let fingerprint = format!("{count}|{revision}|{apps}");
    if let Some((cached, registry)) = cache
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
    {
        if *cached == fingerprint {
            return Ok(registry.clone());
        }
    }
    let rows = store
        .object_kinds()
        .map_err(|e| map_store_error("Error reading object kinds", e))?;
    let records: BTreeMap<String, AppRecord> = store
        .list_apps()
        .map_err(|e| map_store_error("Error reading installed apps", e))?
        .into_iter()
        .map(|record| (record.name.clone(), record))
        .collect();
    let mut kinds = Vec::new();
    for row in rows {
        let decl: KindDecl = match serde_json::from_str(&row.decl) {
            Ok(decl) => decl,
            Err(err) => {
                if log {
                    warn!(app = %row.app, kind = %row.kind, "Ignoring an object kind whose declaration does not parse: {err}");
                }
                continue;
            }
        };
        let record = records.get(&row.app);
        let state = KindState::parse(&row.state);
        match InstalledKind::new(
            &row.app,
            decl,
            &row.digest,
            state,
            &record.map(AppRecord::display_version).unwrap_or_default(),
            record.is_some_and(|record| record.enabled),
        ) {
            Ok(kind) => kinds.push(kind),
            Err(err) => {
                if log {
                    warn!("Ignoring an object kind: {err}");
                }
            }
        }
    }
    let registry = Arc::new(ObjectKindRegistry::from_kinds(kinds));
    *cache.lock().unwrap_or_else(|poison| poison.into_inner()) =
        Some((fingerprint, registry.clone()));
    Ok(registry)
}

/// The state an object reads as, by name.
#[allow(dead_code)]
pub(crate) fn state_name(state: ObjectState) -> &'static str {
    state.as_str()
}

#[cfg(test)]
mod object_rpcs_tests {
    use super::*;

    #[test]
    fn watch_chunks_bound_count_and_bytes_test() {
        assert_eq!(watch_chunks(Vec::new()), vec![Vec::<AppObject>::new()]);
        let small = |name: &str| AppObject {
            name: name.into(),
            ..Default::default()
        };
        let many: Vec<AppObject> = (0..2_500).map(|i| small(&format!("o{i}"))).collect();
        let sizes: Vec<usize> = watch_chunks(many).iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![1_000, 1_000, 500]);
        let big = |name: &str| AppObject {
            name: name.into(),
            spec_json: "x".repeat(900 * 1024),
            ..Default::default()
        };
        let chunks = watch_chunks(vec![big("a"), big("b"), big("c"), big("d")]);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 2]);
    }

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
    fn an_unknown_kind_is_retryable_only_for_an_apply_test() {
        let lookup = KindLookup::Unknown { suggestion: None };
        let apply = lookup_status(&lookup, "gitops/Repo", true);
        assert_eq!(apply.code(), tonic::Code::Unavailable);
        assert_eq!(
            apply
                .metadata()
                .get(crate::manager::worker_plane::ERROR_CODE_METADATA)
                .and_then(|value| value.to_str().ok()),
            Some(KIND_NOT_INSTALLED)
        );
        assert_eq!(
            lookup_status(&lookup, "repo", false).code(),
            tonic::Code::InvalidArgument
        );
    }
}
