//! App-owned object kinds (app-object-kinds plan; docs/app-objects.md):
//! the kinds an app's manifest declares (`kinds`), the objects people write
//! of them, and the checks both go through.
//!
//! An app declares a **kind** (`Repo`, `ScalingPolicy`) with a JSON Schema
//! for its spec (draft 7, local `$ref`s only, at most 64 KiB: the jobspec
//! extension rules, by the same code), an optional status schema, a scope
//! (`namespaced` or `cluster`), the ACL resource that governs its objects
//! (`access`, default `object`), extra permissions writers need
//! (`writeRequires`), and the columns `legion object list` shows. Kind names
//! are unique cluster-wide, ignoring case; `<app>/<Kind>` always names one
//! app's kind.
//!
//! Everything here is pure: the declarations and their install checks, the
//! registry of installed kinds and how a typed kind resolves against it,
//! parsing an object as a person writes it, the effective spec (defaults
//! filled) and state (`valid`, `invalid`, `orphaned`) computed against the
//! kinds installed *now*, the spec diff a dry run shows, and the watch's
//! shard key.

use crate::structs::acl::{AclAction, AclResource};
use crate::structs::app::{AppManifest, AppRecord};
use crate::structs::app_ui::{UiPermission, ViewContext};
use crate::structs::extensions::{compiled_extension_schema, CompiledSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Longest kind name.
pub const MAX_KIND_NAME_LEN: usize = 63;
/// Most kinds one app declares.
pub const MAX_KINDS_PER_APP: usize = 32;
/// Most `columns` a kind declares.
pub const MAX_KIND_COLUMNS: usize = 8;
/// Largest spec, and largest status, as JSON.
pub const MAX_OBJECT_DOC_BYTES: usize = 256 * 1024;
/// Most labels on one object.
pub const MAX_OBJECT_LABELS: usize = 32;
/// Longest label key, and longest label value.
pub const MAX_LABEL_KEY_LEN: usize = 63;
pub const MAX_LABEL_VALUE_LEN: usize = 256;
/// The path an app serves its object validate call on.
pub const OBJECT_VALIDATE_PATH: &str = "/legion/v1/objects/validate";
/// The name an object validate call's viewer assertion carries (an
/// extension validate call's carries `validate`).
pub const OBJECT_VALIDATE_NAME: &str = "object";
/// The slot an object validate call's assertion context names: not a UI
/// slot, a marker that binds the assertion to an object write.
pub const OBJECT_VALIDATE_SLOT: &str = "object";

/// Legion's own nouns, which no kind may take (in any case, singular or
/// plural): every top-level CLI noun (hyphens dropped), every ACL resource,
/// and the core objects that are not top-level commands, plus the two
/// fields every object has (`spec`, `status`). A test checks every
/// top-level command is covered (app-object-kinds calls entry 9).
pub const RESERVED_KIND_NOUNS: &[&str] = &[
    "acl",
    "action",
    "alloc",
    "allocation",
    "app",
    "artifact",
    "boot",
    "bootticket",
    "capability",
    "checkpoint",
    "doc",
    "eval",
    "evaluation",
    "event",
    "exec",
    "execkey",
    "execticket",
    "extension",
    "grant",
    "group",
    "guestagent",
    "guestresume",
    "identity",
    "instance",
    "job",
    "join",
    "jointoken",
    "keyring",
    "kind",
    "kv",
    "ledger",
    "login",
    "logout",
    "manager",
    "mcp",
    "mesh",
    "namespace",
    "net",
    "network",
    "node",
    "object",
    "operator",
    "plugin",
    "policy",
    "pool",
    "quota",
    "raft",
    "rollout",
    "sandbox",
    "scale",
    "scaling",
    "scheduler",
    "secret",
    "service",
    "session",
    "snapshot",
    "spec",
    "start",
    "state",
    "status",
    "task",
    "template",
    "token",
    "user",
    "volume",
    "worker",
];

/// Whether a kind's objects live in a namespace.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum KindScope {
    /// Each object lives in a namespace, and is reached with ACL rules on
    /// it (the default).
    #[default]
    Namespaced,
    /// Objects have no namespace.
    Cluster,
}

impl KindScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            KindScope::Namespaced => "namespaced",
            KindScope::Cluster => "cluster",
        }
    }

    pub fn is_namespaced(&self) -> bool {
        *self == KindScope::Namespaced
    }
}

/// Which ACL resource governs a kind's objects.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KindAccess {
    /// An ACL resource whose scope fits the kind's: `object` (the default,
    /// for either scope), a namespaced resource for a namespaced kind
    /// (`job`, `scale`, …), a cluster-scoped one for a cluster kind
    /// (`node`, `operator`, …).
    pub resource: String,
}

/// A kind's status: what its app reports.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KindStatus {
    /// JSON Schema (draft 7) every status write is checked against: an
    /// object schema, at most 64 KiB, only local `$ref`s.
    #[schemars(with = "serde_json::Map<String, Value>")]
    pub schema: Value,
}

/// One column `legion object list` shows: a JSON pointer into `{ name,
/// namespace, labels, spec, status }` (`spec` with defaults filled), and a
/// header (the pointer's last segment, upper-cased, when not named).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum KindColumn {
    Pointer(String),
    Named { name: String, pointer: String },
}

impl KindColumn {
    pub fn pointer(&self) -> &str {
        match self {
            KindColumn::Pointer(pointer) => pointer,
            KindColumn::Named { pointer, .. } => pointer,
        }
    }

    /// The column's header.
    pub fn header(&self) -> String {
        match self {
            KindColumn::Named { name, .. } => name.clone(),
            KindColumn::Pointer(pointer) => pointer
                .rsplit('/')
                .next()
                .unwrap_or(pointer)
                .to_ascii_uppercase(),
        }
    }
}

/// One kind an app's manifest declares (`kinds[]`).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KindDecl {
    /// PascalCase and singular (`Repo`): a capital letter, then letters and
    /// digits, at most 63. Unique cluster-wide, ignoring case.
    pub name: String,
    /// One line on what an object of the kind is.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// `namespaced` (default) or `cluster`.
    #[serde(default)]
    pub scope: KindScope,
    /// JSON Schema (draft 7) for the spec: an object schema, at most 64
    /// KiB, with only local `$ref`s. `default`s are filled when an object
    /// is read.
    #[schemars(with = "serde_json::Map<String, Value>")]
    pub schema: Value,
    /// The status the app reports, and its schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<KindStatus>,
    /// Example specs, each checked against the schema at install.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<Value>,
    /// Also ask the app on every apply (`POST /legion/v1/objects/validate`),
    /// for checks a schema cannot express. Fails open.
    #[serde(default)]
    pub validate: bool,
    /// The ACL resource that governs the kind's objects (default `object`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<KindAccess>,
    /// Permissions every writer (apply and delete) must also hold, on the
    /// object's scope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_requires: Vec<UiPermission>,
    /// The columns `legion object list` shows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<KindColumn>,
}

impl KindDecl {
    /// The resource named in `access`, or `object`.
    pub fn access_name(&self) -> &str {
        self.access
            .as_ref()
            .map(|access| access.resource.trim())
            .unwrap_or("object")
    }

    /// The access resource, parsed (an install checked it).
    pub fn access_resource(&self) -> Result<AclResource, String> {
        AclResource::parse(self.access_name()).ok_or_else(|| {
            format!(
                "access.resource {:?} is not an ACL resource ({})",
                self.access_name(),
                AclResource::all().join(", ")
            )
        })
    }

    /// `writeRequires`, parsed.
    pub fn write_requires_parsed(&self) -> Result<Vec<(AclResource, AclAction)>, String> {
        self.write_requires
            .iter()
            .map(UiPermission::parsed)
            .collect()
    }

    /// How the install prompt words the kind's access: `object:write on
    /// gitops/Repo, plus job:write`.
    pub fn access_label(&self, app: &str) -> String {
        let mut text = if self.access_name() == "object" {
            format!("object:write on {app}/{}", self.name)
        } else {
            format!("{}:write", self.access_name())
        };
        if !self.write_requires.is_empty() {
            let extra: Vec<String> = self.write_requires.iter().map(|p| p.label()).collect();
            text.push_str(&format!(", plus {}", extra.join(", ")));
        }
        text
    }
}

/// The manifest's `kinds`, parsed.
pub fn manifest_kinds(manifest: &AppManifest) -> Result<Vec<KindDecl>, String> {
    match &manifest.kinds {
        None => Ok(Vec::new()),
        Some(Value::Array(_)) => {
            serde_json::from_value(manifest.kinds.clone().unwrap_or(Value::Null))
                .map_err(|err| format!("kinds: {err}"))
        }
        Some(_) => Err("`kinds` must be a list of kind declarations".into()),
    }
}

/// `word` lowercased with `-` and `_` dropped, as nouns compare.
fn normalized(word: &str) -> String {
    word.chars()
        .filter(|c| *c != '-' && *c != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

/// The plural spellings of a lowercase noun: `+s`, `+es`, and `y` → `ies`.
fn plurals(noun: &str) -> Vec<String> {
    let mut forms = vec![format!("{noun}s"), format!("{noun}es")];
    if let Some(stem) = noun.strip_suffix('y') {
        forms.push(format!("{stem}ies"));
    }
    forms
}

/// Whether two words name the same noun: equal ignoring case, `-` and `_`,
/// or one a plural of the other.
pub fn same_noun(a: &str, b: &str) -> bool {
    let (a, b) = (normalized(a), normalized(b));
    a == b || plurals(&a).contains(&b) || plurals(&b).contains(&a)
}

/// The reserved noun `name` collides with, if any.
pub fn reserved_noun(name: &str) -> Option<&'static str> {
    RESERVED_KIND_NOUNS
        .iter()
        .copied()
        .find(|noun| same_noun(name, noun))
}

/// A kind name: a capital letter, then letters and digits, at most
/// [`MAX_KIND_NAME_LEN`], and not a reserved noun.
pub fn validate_kind_name(name: &str) -> Result<(), String> {
    let shaped = !name.is_empty()
        && name.len() <= MAX_KIND_NAME_LEN
        && name.starts_with(|c: char| c.is_ascii_uppercase())
        && name.chars().all(|c| c.is_ascii_alphanumeric());
    if !shaped {
        return Err(format!(
            "kind name {name:?} must be PascalCase: a capital letter, then letters and digits, at \
             most {MAX_KIND_NAME_LEN} characters (Repo, ScalingPolicy)"
        ));
    }
    if let Some(noun) = reserved_noun(name) {
        return Err(format!(
            "`{name}` is a Legion noun ({noun}) and is reserved; pick a name that says what the \
             app means (RolloutPolicy, ScalingPolicy)"
        ));
    }
    Ok(())
}

/// Check an app's kind declarations on their own (app-object-kinds plan
/// §6.1): at most [`MAX_KINDS_PER_APP`], names valid, not reserved, and not
/// the same noun as each other; schemas that compile under the extension
/// rules; examples that pass; an `access` resource whose scope fits the
/// kind's; `writeRequires` that name ACL permissions; and columns that are
/// JSON pointers into an object. Collisions with other apps' kinds depend
/// on the cluster: [`ObjectKindRegistry::collision`].
pub fn validate_kind_decls(app: &str, decls: &[KindDecl]) -> Result<(), String> {
    if decls.len() > MAX_KINDS_PER_APP {
        return Err(format!(
            "kinds: {} declared; an app declares at most {MAX_KINDS_PER_APP}",
            decls.len()
        ));
    }
    for (index, decl) in decls.iter().enumerate() {
        let at = format!("kinds ({})", decl.name);
        validate_kind_name(&decl.name).map_err(|err| format!("{at}: {err}"))?;
        if let Some(other) = decls[..index]
            .iter()
            .find(|other| same_noun(&other.name, &decl.name))
        {
            return Err(format!(
                "{at}: the app already declares {}; kind names are unique ignoring case and \
                 plurals",
                other.name
            ));
        }
        let compiled =
            compiled_extension_schema(&decl.schema).map_err(|err| format!("{at}: schema {err}"))?;
        for (n, example) in decl.examples.iter().enumerate() {
            compiled.check(example).map_err(|errors| {
                format!(
                    "{at}: examples[{n}] does not match the kind's schema: {}",
                    errors.join("; ")
                )
            })?;
        }
        if let Some(status) = &decl.status {
            compiled_extension_schema(&status.schema)
                .map_err(|err| format!("{at}: status.schema {err}"))?;
        }
        let resource = decl
            .access_resource()
            .map_err(|err| format!("{at}: {err}"))?;
        if resource != AclResource::Object && resource.is_namespaced() != decl.scope.is_namespaced()
        {
            return Err(format!(
                "{at}: access.resource `{}` is {}, but the kind is {}; a namespaced kind maps onto \
                 a namespaced resource and a cluster kind onto a cluster-scoped one (or leave \
                 access out for the `object` resource)",
                resource.as_str(),
                if resource.is_namespaced() {
                    "namespaced"
                } else {
                    "cluster-scoped"
                },
                decl.scope.as_str()
            ));
        }
        decl.write_requires_parsed()
            .map_err(|err| format!("{at}: writeRequires: {err}"))?;
        if decl.columns.len() > MAX_KIND_COLUMNS {
            return Err(format!(
                "{at}: columns: at most {MAX_KIND_COLUMNS}, got {}",
                decl.columns.len()
            ));
        }
        for column in &decl.columns {
            validate_column_pointer(column.pointer()).map_err(|err| format!("{at}: {err}"))?;
            if column.header().trim().is_empty() {
                return Err(format!("{at}: columns: a column needs a name"));
            }
        }
    }
    Ok(())
}

/// A column pointer: `/spec/...`, `/status/...`, `/labels/...`, `/name`,
/// or `/namespace`.
fn validate_column_pointer(pointer: &str) -> Result<(), String> {
    let first = pointer
        .strip_prefix('/')
        .map(|rest| rest.split('/').next().unwrap_or_default());
    match first {
        Some("spec" | "status" | "labels" | "name" | "namespace") => Ok(()),
        _ => Err(format!(
            "columns: {pointer:?} must be a JSON pointer into the object: /spec/…, /status/…, \
             /labels/…, /name, or /namespace"
        )),
    }
}

/// One installed kind: its declaration, its owner, and its schemas
/// compiled.
#[derive(Debug, Clone)]
pub struct InstalledKind {
    pub app: String,
    /// The manifest's version, or a bundled app's build version.
    pub app_version: String,
    /// The app's manifest digest.
    pub digest: String,
    pub enabled: bool,
    pub decl: KindDecl,
    pub schema: Arc<CompiledSchema>,
    pub status_schema: Option<Arc<CompiledSchema>>,
    pub access: AclResource,
    pub write_requires: Vec<(AclResource, AclAction)>,
}

impl InstalledKind {
    /// `<app>/<Kind>`: what `kinds` selectors match and what always names
    /// the kind.
    pub fn qualified(&self) -> String {
        qualified_kind(&self.app, &self.decl.name)
    }

    /// `<app>@<digest>`, the stamp an object it checked carries.
    pub fn validated_by(&self) -> String {
        format!("{}@{}", self.app, self.digest)
    }

    pub fn is_namespaced(&self) -> bool {
        self.decl.scope.is_namespaced()
    }

    /// The spec with defaults filled, or one message per problem with its
    /// path below `spec`.
    pub fn check_spec(&self, spec: &Value) -> Result<Value, Vec<String>> {
        self.schema
            .check(spec)
            .map_err(|errors| errors.iter().map(|err| prefixed("spec", err)).collect())
    }

    /// A status checked against the status schema (any object without
    /// one).
    pub fn check_status(&self, status: &Value) -> Result<(), Vec<String>> {
        if !status.is_object() {
            return Err(vec!["status must be a JSON object".to_string()]);
        }
        match &self.status_schema {
            None => Ok(()),
            Some(schema) => schema
                .check(status)
                .map(|_| ())
                .map_err(|errors| errors.iter().map(|err| prefixed("status", err)).collect()),
        }
    }
}

/// `spec.branch: …` from `branch: …` (or `spec: …`).
fn prefixed(root: &str, error: &str) -> String {
    crate::structs::extensions::prefixed(root, error)
}

/// `<app>/<Kind>`.
pub fn qualified_kind(app: &str, kind: &str) -> String {
    format!("{app}/{kind}")
}

/// Why a typed kind did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KindLookup {
    /// No installed kind matches; the closest, if any.
    Unknown { suggestion: Option<String> },
    /// `<app>/<Kind>` where the app is installed but declares no such kind,
    /// or is not installed.
    NotDeclared { app: String },
}

/// The kinds installed apps declare, with their schemas compiled.
#[derive(Debug, Clone, Default)]
pub struct ObjectKindRegistry {
    kinds: Vec<InstalledKind>,
}

impl ObjectKindRegistry {
    /// No kinds.
    pub fn empty() -> Self {
        Self::default()
    }

    /// The registry of installed apps' kinds. A record whose declarations no
    /// longer parse or compile contributes nothing, and is reported in the
    /// second value.
    pub fn from_records(records: &[AppRecord]) -> (Self, Vec<String>) {
        let mut kinds = Vec::new();
        let mut problems = Vec::new();
        for record in records {
            match installed_kinds(record) {
                Ok(mut found) => kinds.append(&mut found),
                Err(err) => problems.push(format!("app {}: {err}", record.name)),
            }
        }
        (ObjectKindRegistry { kinds }, problems)
    }

    /// The registry with `record` installed in place of any record of the
    /// same name.
    pub fn with_record(&self, record: &AppRecord) -> Result<Self, String> {
        let mut kinds: Vec<InstalledKind> = self
            .kinds
            .iter()
            .filter(|kind| kind.app != record.name)
            .cloned()
            .collect();
        kinds.extend(installed_kinds(record)?);
        Ok(ObjectKindRegistry { kinds })
    }

    /// The registry without `app`.
    pub fn without_app(&self, app: &str) -> Self {
        ObjectKindRegistry {
            kinds: self
                .kinds
                .iter()
                .filter(|kind| kind.app != app)
                .cloned()
                .collect(),
        }
    }

    pub fn kinds(&self) -> &[InstalledKind] {
        &self.kinds
    }

    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    /// `app`'s kinds.
    pub fn of_app<'a>(&'a self, app: &'a str) -> impl Iterator<Item = &'a InstalledKind> + 'a {
        self.kinds.iter().filter(move |kind| kind.app == app)
    }

    /// `app`'s kind named exactly `kind`.
    pub fn get(&self, app: &str, kind: &str) -> Option<&InstalledKind> {
        self.kinds
            .iter()
            .find(|installed| installed.app == app && installed.decl.name == kind)
    }

    /// The installed kind a typed name means: `Repo`, `repo`, `repos`, or
    /// `gitops/Repo` (`gitops/repos`).
    pub fn resolve(&self, typed: &str) -> Result<&InstalledKind, KindLookup> {
        let typed = typed.trim();
        if let Some((app, kind)) = typed.split_once('/') {
            return self
                .kinds
                .iter()
                .find(|installed| installed.app == app && same_noun(kind, &installed.decl.name))
                .ok_or_else(|| KindLookup::NotDeclared {
                    app: app.to_string(),
                });
        }
        self.kinds
            .iter()
            .find(|installed| same_noun(typed, &installed.decl.name))
            .ok_or_else(|| KindLookup::Unknown {
                suggestion: self.suggest(typed),
            })
    }

    /// The installed kind name closest to `typed`, for a did-you-mean.
    pub fn suggest(&self, typed: &str) -> Option<String> {
        let names: Vec<&str> = self
            .kinds
            .iter()
            .map(|kind| kind.decl.name.as_str())
            .collect();
        crate::structs::spec_fields::suggest_owned(typed, &names).map(|index| names[index].to_string())
    }

    /// The first of `decls` (app `app`'s) whose name another installed app
    /// already declares (ignoring case and plurals), or a bundled manifest
    /// in this binary declares, worded as the refusal.
    pub fn collision(&self, app: &str, decls: &[KindDecl]) -> Option<String> {
        for decl in decls {
            if let Some(owner) = self
                .kinds
                .iter()
                .find(|kind| kind.app != app && same_noun(&kind.decl.name, &decl.name))
            {
                return Some(format!(
                    "{app} declares kind {}, which the {} app already owns (as {})\nHint: the \
                     qualified spelling ({}/{}) always names that app's kind; rename this app's \
                     kind to install it",
                    decl.name,
                    owner.app,
                    owner.decl.name,
                    owner.app,
                    owner.decl.name
                ));
            }
            if let Some(owner) = bundled_kind_owner(&decl.name).filter(|owner| *owner != app) {
                return Some(format!(
                    "{app} declares kind {}, which the bundled {owner} app declares\nHint: rename \
                     this app's kind",
                    decl.name
                ));
            }
        }
        None
    }
}

/// The bundled app (in this binary) declaring a kind named like `name`.
fn bundled_kind_owner(name: &str) -> Option<&'static str> {
    let manifests = crate::apps::bundled_manifests().ok()?;
    manifests.iter().find_map(|manifest| {
        let kinds = manifest_kinds(manifest).ok()?;
        kinds
            .iter()
            .any(|kind| same_noun(&kind.name, name))
            .then_some(manifest.name.as_str())
    })
}

/// An installed record's kinds.
fn installed_kinds(record: &AppRecord) -> Result<Vec<InstalledKind>, String> {
    let decls = manifest_kinds(&record.manifest)?;
    let mut kinds = Vec::new();
    for decl in decls {
        let schema = compiled_extension_schema(&decl.schema)
            .map_err(|err| format!("kind {}: schema {err}", decl.name))?;
        let status_schema = match &decl.status {
            None => None,
            Some(status) => Some(
                compiled_extension_schema(&status.schema)
                    .map_err(|err| format!("kind {}: status.schema {err}", decl.name))?,
            ),
        };
        let access = decl
            .access_resource()
            .map_err(|err| format!("kind {}: {err}", decl.name))?;
        let write_requires = decl
            .write_requires_parsed()
            .map_err(|err| format!("kind {}: writeRequires: {err}", decl.name))?;
        kinds.push(InstalledKind {
            app: record.name.clone(),
            app_version: record.display_version(),
            digest: record.manifest_digest.clone(),
            enabled: record.enabled,
            decl,
            schema,
            status_schema,
            access,
            write_requires,
        });
    }
    Ok(kinds)
}

/// An object as a person writes it (`legion object apply -f`): a kind, a
/// name, a namespace for a namespaced kind, labels, and the spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectInput {
    /// The kind: `Repo`, `repos`, or `gitops/Repo`.
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// The spec, checked against the kind's schema.
    #[serde(default)]
    pub spec: Value,
}

/// Parse one object as written. `status` is the app's, so a document that
/// sets it is refused, as is any unknown field.
pub fn parse_object_input(value: &Value) -> Result<ObjectInput, String> {
    let Some(map) = value.as_object() else {
        return Err("an object is a record: { kind, name, namespace?, labels?, spec }".into());
    };
    if map.contains_key("status") {
        return Err(
            "`status` is written by the app that owns the kind, never applied; leave it out"
                .into(),
        );
    }
    let input: ObjectInput =
        serde_json::from_value(value.clone()).map_err(|err| format!("the object: {err}"))?;
    if input.kind.trim().is_empty() {
        return Err("the object needs a `kind`".into());
    }
    if input.spec.is_null() {
        return Err(format!(
            "{} {}: the object needs a `spec` (a record, `{{}}` for none)",
            input.kind, input.name
        ));
    }
    if !input.spec.is_object() {
        return Err(format!(
            "{} {}: `spec` must be a record",
            input.kind, input.name
        ));
    }
    Ok(input)
}

/// The objects a file holds: one object, or a list of them.
pub fn parse_object_documents(value: &Value) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(items) if items.is_empty() => Err("the file holds no objects".into()),
        Value::Array(items) => Ok(items.clone()),
        Value::Object(_) => Ok(vec![value.clone()]),
        _ => Err("a file holds an object or a list of objects".into()),
    }
}

/// An object name: one DNS label (lowercase letters, digits and `-`,
/// starting and ending with a letter or digit, at most 63).
pub fn validate_object_name(name: &str) -> Result<(), String> {
    crate::structs::namespace::validate_namespace_name(name).map_err(|_| {
        format!(
            "object name {name:?} must be lowercase letters, digits and '-', starting and ending \
             with a letter or digit, at most 63 characters"
        )
    })
}

/// Labels: at most [`MAX_OBJECT_LABELS`], keys a letter then letters,
/// digits, `.`, `-`, `_`, or `/`, values of printable characters.
pub fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), String> {
    if labels.len() > MAX_OBJECT_LABELS {
        return Err(format!(
            "labels: at most {MAX_OBJECT_LABELS}, got {}",
            labels.len()
        ));
    }
    for (key, value) in labels {
        let valid_key = !key.is_empty()
            && key.len() <= MAX_LABEL_KEY_LEN
            && key.starts_with(|c: char| c.is_ascii_alphabetic())
            && key
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/'));
        if !valid_key {
            return Err(format!(
                "label key {key:?} must be a letter followed by letters, digits, '.', '-', '_' or \
                 '/', at most {MAX_LABEL_KEY_LEN} characters"
            ));
        }
        if value.len() > MAX_LABEL_VALUE_LEN || value.chars().any(char::is_control) {
            return Err(format!(
                "label {key}: the value must be at most {MAX_LABEL_VALUE_LEN} printable characters"
            ));
        }
    }
    Ok(())
}

/// Parse a label filter: `key=value` entries, comma-separated or repeated.
pub fn parse_label_filter(entries: &[String]) -> Result<BTreeMap<String, String>, String> {
    let mut filter = BTreeMap::new();
    for entry in entries {
        for part in entry.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| format!("label filter {part:?}: expected key=value"))?;
            filter.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    Ok(filter)
}

/// How an object reads now.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ObjectState {
    /// Its app declares the kind, and the spec passes the current schema.
    Valid,
    /// The spec fails the kind's current schema (an upgrade tightened it).
    Invalid,
    /// Its app was uninstalled, or no longer declares the kind.
    Orphaned,
}

impl ObjectState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectState::Valid => "valid",
            ObjectState::Invalid => "invalid",
            ObjectState::Orphaned => "orphaned",
        }
    }
}

/// An object's spec judged against the kinds installed now: its effective
/// spec (defaults filled; the spec as written when it cannot be judged),
/// its state, and why.
pub fn judge(kind: Option<&InstalledKind>, spec: &Value) -> (Value, ObjectState, String) {
    match kind {
        None => (
            spec.clone(),
            ObjectState::Orphaned,
            "its app no longer declares the kind (uninstalled, or upgraded with --force)"
                .to_string(),
        ),
        Some(kind) => match kind.check_spec(spec) {
            Ok(effective) => (effective, ObjectState::Valid, String::new()),
            Err(errors) => (spec.clone(), ObjectState::Invalid, errors.join("; ")),
        },
    }
}

/// An object's display id: `namespace/name` for a namespaced kind, `name`
/// for a cluster kind.
pub fn object_id(namespace: &str, name: &str) -> String {
    if namespace.is_empty() {
        name.to_string()
    } else {
        format!("{namespace}/{name}")
    }
}

/// The key an object shards by in `WatchObjects`:
/// `<app>/<Kind>:<namespace>/<name>`, or `<app>/<Kind>:<name>` for a
/// cluster kind.
pub fn shard_key(app: &str, kind: &str, namespace: &str, name: &str) -> String {
    format!(
        "{}:{}",
        qualified_kind(app, kind),
        object_id(namespace, name)
    )
}

/// Which of `count` shards an object belongs to: FNV-1a 64 of its
/// [`shard_key`] modulo the count, the hash the autoscaler's shard leases
/// use (`crate::autoscaler::shard`). 0 when `count` is 0 or 1.
pub fn object_shard(app: &str, kind: &str, namespace: &str, name: &str, count: u32) -> u32 {
    if count <= 1 {
        return 0;
    }
    crate::autoscaler::shard::shard_of(&shard_key(app, kind, namespace, name), count)
}

/// One line per field that differs between two specs (or two label maps):
/// `spec.branch: "main" → "release"`, `spec.path: added "jobs"`,
/// `spec.depth: removed 3`. Records are compared field by field; anything
/// else (lists included) whole.
pub fn spec_diff(root: &str, before: &Value, after: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    diff_into(root, before, after, &mut lines);
    lines
}

fn diff_into(at: &str, before: &Value, after: &Value, lines: &mut Vec<String>) {
    match (before, after) {
        (Value::Object(old), Value::Object(new)) => {
            let mut keys: Vec<&String> = old.keys().chain(new.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                let path = format!("{at}.{key}");
                match (old.get(key), new.get(key)) {
                    (Some(a), Some(b)) => diff_into(&path, a, b, lines),
                    (None, Some(b)) => lines.push(format!("{path}: added {}", short(b))),
                    (Some(a), None) => lines.push(format!("{path}: removed {}", short(a))),
                    (None, None) => {}
                }
            }
        }
        (a, b) if a != b => lines.push(format!("{at}: {} → {}", short(a), short(b))),
        _ => {}
    }
}

/// A value for a diff line, cut at 120 characters.
fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 120 {
        format!("{}…", text.chars().take(119).collect::<String>())
    } else {
        text
    }
}

/// The document `columns` point into.
pub fn column_document(
    name: &str,
    namespace: &str,
    labels: &BTreeMap<String, String>,
    effective: &Value,
    status: &Value,
) -> Value {
    json!({
        "name": name,
        "namespace": namespace,
        "labels": labels,
        "spec": effective,
        "status": status,
    })
}

/// A column's cell: a string as itself, anything else as JSON, `-` when
/// the pointer finds nothing.
pub fn column_cell(document: &Value, pointer: &str) -> String {
    match document.pointer(pointer) {
        None | Some(Value::Null) => "-".to_string(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

/// The body of an app's object validate call (`POST
/// /legion/v1/objects/validate`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectValidateCall {
    pub app: String,
    /// The kind as declared (`Repo`).
    pub kind: String,
    /// Empty for a cluster kind.
    pub namespace: String,
    pub name: String,
    /// `create` or `update`.
    pub operation: String,
    /// The spec as written, and with the schema's defaults filled.
    pub spec: Value,
    pub effective: Value,
    /// The stored spec this apply replaces (`null` on create).
    pub previous: Value,
    /// The object's labels after the apply.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// What an app answers an object validate call.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectValidateAnswer {
    /// Problems that refuse the apply.
    #[serde(default)]
    pub errors: Vec<String>,
    /// Notes that pass through to the reply's `warnings`.
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// The viewer-assertion context of an object validate call: it binds the
/// assertion to the object's namespace (and the request body, through the
/// inputs digest).
pub fn object_validate_context(namespace: &str) -> ViewContext {
    ViewContext {
        slot: OBJECT_VALIDATE_SLOT.to_string(),
        namespace: namespace.to_string(),
        ..Default::default()
    }
}

/// The install prompt's lines for a manifest's kinds.
pub fn kinds_prompt(app: &str, decls: &[KindDecl]) -> Vec<String> {
    if decls.is_empty() {
        return Vec::new();
    }
    let mut lines = vec!["Kinds:".to_string()];
    for decl in decls {
        let mut line = format!(
            "  {:<14} {}; written with {}",
            decl.name,
            decl.scope.as_str(),
            decl.access_label(app)
        );
        if decl.validate {
            line.push_str("; checked by the app");
        }
        lines.push(line);
    }
    lines
}

/// A kind as JSON for `-j` outputs and MCP.
pub fn kind_json(kind: &InstalledKind) -> Value {
    let mut map = Map::new();
    map.insert("kind".into(), json!(kind.decl.name));
    map.insert("app".into(), json!(kind.app));
    map.insert("qualified".into(), json!(kind.qualified()));
    map.insert("scope".into(), json!(kind.decl.scope.as_str()));
    map.insert("access".into(), json!(kind.access.as_str()));
    Value::Object(map)
}

#[cfg(test)]
mod app_object_tests {
    use super::*;

    fn decl(name: &str) -> KindDecl {
        KindDecl {
            name: name.to_string(),
            description: String::new(),
            scope: KindScope::Namespaced,
            schema: json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["url"],
                "properties": {
                    "url": { "type": "string" },
                    "branch": { "type": "string", "default": "main" },
                },
            }),
            status: None,
            examples: vec![json!({ "url": "https://x" })],
            validate: false,
            access: None,
            write_requires: Vec::new(),
            columns: Vec::new(),
        }
    }

    fn record(app: &str, kinds: Vec<KindDecl>) -> AppRecord {
        let mut manifest: AppManifest = serde_json::from_value(json!({
            "name": app,
            "version": "0.1.0",
            "job": { "task": { "name": "t", "runtime": "noop" } },
        }))
        .expect("manifest");
        manifest.kinds = Some(serde_json::to_value(kinds).expect("kinds"));
        AppRecord {
            name: app.to_string(),
            source: crate::structs::app::AppSource::File,
            source_ref: String::new(),
            manifest_digest: manifest.digest(),
            enabled: true,
            settings: crate::structs::app::AppSettings::from_manifest(&manifest),
            config: json!({}),
            grant: Default::default(),
            installed_by: String::new(),
            installed_at: String::new(),
            updated_at: String::new(),
            state: Default::default(),
            manifest,
        }
    }

    #[test]
    fn kind_names_are_pascal_case_and_not_legion_nouns_test() {
        assert!(validate_kind_name("Repo").is_ok());
        assert!(validate_kind_name("ScalingPolicy").is_ok());
        assert!(validate_kind_name("NodeGroup").is_ok());
        for bad in ["repo", "Repo-x", "", "9Lives", "Re po"] {
            assert!(validate_kind_name(bad).is_err(), "{bad}");
        }
        for reserved in ["Job", "Jobs", "Policy", "Policies", "Token", "Node", "Events", "Event"] {
            let err = validate_kind_name(reserved).expect_err(reserved);
            assert!(err.contains("reserved"), "{err}");
        }
        assert_eq!(reserved_noun("JoinToken"), Some("jointoken"));
        assert_eq!(reserved_noun("Capabilities"), Some("capability"));
        assert_eq!(reserved_noun("Repo"), None);
    }

    /// Every top-level CLI command is a reserved noun, so a kind never reads
    /// like a command (calls entry 9).
    #[test]
    fn every_top_level_command_is_reserved_test() {
        for command in crate::cmd::cli().get_subcommands() {
            let name = command.get_name();
            if name == "help" {
                continue;
            }
            assert!(
                reserved_noun(name).is_some(),
                "`legion {name}` is not in RESERVED_KIND_NOUNS"
            );
        }
    }

    #[test]
    fn declarations_are_checked_test() {
        assert!(validate_kind_decls("gitops", &[decl("Repo")]).is_ok());
        let err = validate_kind_decls("gitops", &[decl("Repo"), decl("Repos")])
            .expect_err("plural duplicate");
        assert!(err.contains("already declares Repo"), "{err}");

        let mut bad_example = decl("Repo");
        bad_example.examples = vec![json!({ "branch": "x" })];
        let err = validate_kind_decls("gitops", &[bad_example]).expect_err("example");
        assert!(err.contains("examples[0]"), "{err}");

        let mut remote = decl("Repo");
        remote.schema = json!({ "type": "object", "properties": { "a": { "$ref": "https://x/s.json" } } });
        let err = validate_kind_decls("gitops", &[remote]).expect_err("remote ref");
        assert!(err.contains("not local"), "{err}");

        let mut not_object = decl("Repo");
        not_object.schema = json!({ "type": "string" });
        assert!(validate_kind_decls("gitops", &[not_object]).is_err());

        // A namespaced kind maps onto a namespaced resource, a cluster
        // kind onto a cluster-scoped one; `object` fits either.
        let mut scale = decl("Policyish");
        scale.access = Some(KindAccess {
            resource: "scale".into(),
        });
        assert!(validate_kind_decls("a", &[scale.clone()]).is_ok());
        scale.scope = KindScope::Cluster;
        let err = validate_kind_decls("a", &[scale]).expect_err("scope mismatch");
        assert!(err.contains("cluster kind onto a cluster-scoped"), "{err}");
        let mut node = decl("NodeGroup");
        node.scope = KindScope::Cluster;
        node.access = Some(KindAccess {
            resource: "node".into(),
        });
        node.write_requires = vec![UiPermission {
            resource: "operator".into(),
            action: "write".into(),
        }];
        assert!(validate_kind_decls("a", &[node.clone()]).is_ok());
        node.write_requires[0].action = "admin".into();
        assert!(validate_kind_decls("a", &[node]).is_err());
        let mut nowhere = decl("Repo");
        nowhere.access = Some(KindAccess {
            resource: "nope".into(),
        });
        assert!(validate_kind_decls("a", &[nowhere]).is_err());

        let mut columns = decl("Repo");
        columns.columns = vec![
            KindColumn::Pointer("/spec/url".into()),
            KindColumn::Named {
                name: "COMMIT".into(),
                pointer: "/status/commit".into(),
            },
        ];
        assert!(validate_kind_decls("a", &[columns.clone()]).is_ok());
        assert_eq!(columns.columns[0].header(), "URL");
        columns.columns.push(KindColumn::Pointer("spec/x".into()));
        assert!(validate_kind_decls("a", &[columns]).is_err());
    }

    #[test]
    fn kinds_resolve_by_any_case_plural_or_qualified_name_test() {
        let (registry, problems) = ObjectKindRegistry::from_records(&[
            record("gitops", vec![decl("Repo")]),
            record("certs", vec![decl("Certificate"), decl("Policyish")]),
        ]);
        assert!(problems.is_empty(), "{problems:?}");
        for typed in ["Repo", "repo", "repos", "REPOS", "gitops/Repo", "gitops/repos"] {
            assert_eq!(
                registry.resolve(typed).expect(typed).qualified(),
                "gitops/Repo"
            );
        }
        assert_eq!(
            registry.resolve("certificates").expect("plural").decl.name,
            "Certificate"
        );
        assert_eq!(
            registry.resolve("policyishes").expect("es plural").decl.name,
            "Policyish"
        );
        assert_eq!(
            registry.resolve("Rpeo"),
            Err(KindLookup::Unknown {
                suggestion: Some("Repo".into())
            })
        );
        assert_eq!(
            registry.resolve("certs/Repo"),
            Err(KindLookup::NotDeclared {
                app: "certs".into()
            })
        );
        // Another app may not take the name, in any case or as a plural.
        let refusal = registry
            .collision("other", &[decl("Repos")])
            .expect("collision");
        assert!(refusal.contains("the gitops app already owns"), "{refusal}");
        assert!(registry.collision("gitops", &[decl("Repo")]).is_none());
    }

    #[test]
    fn objects_parse_and_judge_against_the_kind_now_test() {
        let input = parse_object_input(&json!({
            "kind": "Repo", "name": "web", "namespace": "team-a",
            "labels": { "team": "a" }, "spec": { "url": "https://x" },
        }))
        .expect("parses");
        assert_eq!(input.namespace.as_deref(), Some("team-a"));
        let err = parse_object_input(&json!({
            "kind": "Repo", "name": "web", "spec": {}, "status": { "ok": true },
        }))
        .expect_err("status");
        assert!(err.contains("written by the app"), "{err}");
        assert!(parse_object_input(&json!({ "kind": "Repo", "name": "web", "spek": {} })).is_err());
        assert!(parse_object_input(&json!({ "kind": "Repo", "name": "web" })).is_err());
        assert!(validate_object_name("web-1").is_ok());
        assert!(validate_object_name("Web").is_err());
        assert_eq!(
            parse_object_documents(&json!([{ "a": 1 }, { "b": 2 }]))
                .expect("list")
                .len(),
            2
        );

        let (registry, _) = ObjectKindRegistry::from_records(&[record("gitops", vec![decl("Repo")])]);
        let kind = registry.resolve("Repo").expect("kind");
        let (effective, state, _) = judge(Some(kind), &json!({ "url": "https://x" }));
        assert_eq!(state, ObjectState::Valid);
        assert_eq!(effective["branch"], "main");
        let (_, state, detail) = judge(Some(kind), &json!({ "branch": "x" }));
        assert_eq!(state, ObjectState::Invalid);
        assert!(detail.contains("spec"), "{detail}");
        assert_eq!(
            judge(None, &json!({})).1,
            ObjectState::Orphaned
        );
    }

    #[test]
    fn labels_and_filters_test() {
        let mut labels = BTreeMap::new();
        labels.insert("team".to_string(), "a".to_string());
        assert!(validate_labels(&labels).is_ok());
        labels.insert("9bad".to_string(), "x".to_string());
        assert!(validate_labels(&labels).is_err());
        let filter = parse_label_filter(&["team=a,tier=web".into(), "x=y".into()]).expect("filter");
        assert_eq!(filter.len(), 3);
        assert!(parse_label_filter(&["team".into()]).is_err());
    }

    #[test]
    fn diffs_name_each_changed_field_test() {
        let lines = spec_diff(
            "spec",
            &json!({ "url": "a", "branch": "main", "depth": 3, "nested": { "x": 1 } }),
            &json!({ "url": "a", "branch": "dev", "path": "jobs", "nested": { "x": 2 } }),
        );
        assert_eq!(
            lines,
            vec![
                "spec.branch: \"main\" → \"dev\"".to_string(),
                "spec.depth: removed 3".to_string(),
                "spec.nested.x: 1 → 2".to_string(),
                "spec.path: added \"jobs\"".to_string(),
            ]
        );
        assert!(spec_diff("spec", &json!({ "a": [1] }), &json!({ "a": [1] })).is_empty());
    }

    /// The shard of an object is stable and uses the autoscaler's hash, so a
    /// replica's leased shards mean the same on both of its watches.
    #[test]
    fn shards_use_the_autoscalers_hash_test() {
        let key = shard_key("autoscaler", "ScalingPolicy", "media", "render");
        assert_eq!(key, "autoscaler/ScalingPolicy:media/render");
        assert_eq!(shard_key("a", "NodeGroup", "", "workers"), "a/NodeGroup:workers");
        let shard = object_shard("autoscaler", "ScalingPolicy", "media", "render", 64);
        assert_eq!(shard, crate::autoscaler::shard::shard_of(&key, 64));
        assert_eq!(object_shard("a", "K", "n", "x", 1), 0);
        assert_eq!(object_shard("a", "K", "n", "x", 0), 0);
    }

    #[test]
    fn columns_read_the_object_document_test() {
        let document = column_document(
            "web",
            "team-a",
            &BTreeMap::new(),
            &json!({ "url": "https://x" }),
            &json!({ "commit": "3f9a", "ok": true }),
        );
        assert_eq!(column_cell(&document, "/spec/url"), "https://x");
        assert_eq!(column_cell(&document, "/status/ok"), "true");
        assert_eq!(column_cell(&document, "/status/missing"), "-");
        assert_eq!(column_cell(&document, "/name"), "web");
    }
}
