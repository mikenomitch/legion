//! App-owned object kinds (app-object-kinds plan; docs/app-objects.md):
//! the kinds an app's manifest declares (`kinds`), the objects people write
//! of them, and the checks both go through.
//!
//! A kind's name is `<app>/<Kind>` (`gitops/Repo`). It declares a JSON
//! Schema for its spec (draft 7, local `$ref`s only, at most 64 KiB, closed
//! by default), an optional schema for the `fields` of its status, a scope
//! (`namespaced` or `cluster`), the ACL resource that governs its objects
//! (`access`: `object`, `job`, `scale`, or `node`), the permissions writing
//! an object needs, resolved from its contents (`requires`), fields an
//! update may not change (`immutable`), a bounded `cleanup` window,
//! `maxObjects`, and the columns `legion object list` shows.
//!
//! Everything here is pure: declarations and their install checks, the
//! registry of kinds and how a typed kind resolves against it, parsing an
//! object as a person writes it, schemas (closing, defaults, errors with
//! paths, `writeOnly` redaction), `requires` resolution into concrete ACL
//! checks, `immutable`, the status envelope and the summary every object
//! reads as, the spec diff, and the watch's shard key.

use crate::structs::acl::{AclAction, AclResource, AclScope};
use crate::structs::app::{digest_value, AppManifest};
use crate::structs::app_ui::ViewContext;
use crate::structs::extensions::{compile_schema, fill_defaults};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

/// Longest kind name (the part after `<app>/`).
pub const MAX_KIND_NAME_LEN: usize = 63;
/// Most kinds one app declares.
pub const MAX_KINDS_PER_APP: usize = 32;
/// Most `columns` a kind declares.
pub const MAX_KIND_COLUMNS: usize = 8;
/// Largest kind schema (spec or status), as JSON.
pub const MAX_KIND_SCHEMA_BYTES: usize = 64 * 1024;
/// Largest spec, after defaults, as JSON.
pub const MAX_SPEC_BYTES: usize = 64 * 1024;
/// Largest status `fields`, as JSON.
pub const MAX_STATUS_FIELDS_BYTES: usize = 16 * 1024;
/// Most conditions in one status.
pub const MAX_CONDITIONS: usize = 16;
/// Longest condition message.
pub const MAX_CONDITION_MESSAGE_BYTES: usize = 1024;
/// Most labels on one object.
pub const MAX_OBJECT_LABELS: usize = 32;
/// Longest label key, and longest label value.
pub const MAX_LABEL_KEY_LEN: usize = 63;
pub const MAX_LABEL_VALUE_LEN: usize = 256;
/// A kind's `maxObjects` when it names none, and the most it may name.
pub const DEFAULT_MAX_OBJECTS: u64 = 10_000;
pub const MAX_MAX_OBJECTS: u64 = 100_000;
/// The longest `cleanup` window.
pub const MAX_CLEANUP: std::time::Duration = std::time::Duration::from_secs(3600);
/// Most concrete permissions one object's `requires` may resolve to.
pub const MAX_RESOLVED_REQUIREMENTS: usize = 64;
/// What every read but the owning app's watch shows for a `writeOnly`
/// value.
pub const REDACTED: &str = "<redacted>";
/// The path an app serves its object validate call on.
pub const OBJECT_VALIDATE_PATH: &str = "/legion/v1/objects/validate";
/// The name an object validate call's viewer assertion carries (an
/// extension validate call's carries `validate`).
pub const OBJECT_VALIDATE_NAME: &str = "object";
/// The slot an object validate call's assertion context names: not a UI
/// slot, a marker that binds the assertion to an object write.
pub const OBJECT_VALIDATE_SLOT: &str = "object";
/// The standard condition types (docs/app-objects.md, "Status").
pub const CONDITION_ACCEPTED: &str = "Accepted";
pub const CONDITION_READY: &str = "Ready";

/// Legion's own nouns, which no kind may take (in any case, singular or
/// plural): every top-level CLI noun (hyphens dropped), every ACL resource,
/// and the core objects that are not top-level commands, plus the two
/// fields every object has (`spec`, `status`). A test checks every
/// top-level command is covered (app-object-kinds calls entry 20).
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
    /// Each object lives in a namespace (the default).
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
    /// `object` (the default), `job` or `scale` (namespaced kinds, checked
    /// namespace-wide), or `node` (cluster kinds).
    pub resource: String,
}

/// A kind's status: the schema of the `fields` its app reports.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KindStatus {
    /// JSON Schema (draft 7) every status's `fields` are checked against:
    /// an object schema, at most 64 KiB, only local `$ref`s, closed by
    /// default.
    #[schemars(with = "serde_json::Map<String, Value>")]
    pub schema: Value,
}

/// One column `legion object list` shows: a JSON pointer into the object
/// (`/spec/url`, `/status/fields/commit`), and a header (the pointer's last
/// segment, upper-cased, when not named).
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

/// One permission writing an object of a kind needs, its scope taken from
/// the object (app-object-kinds plan §6.4). Every pointer points into the
/// object document (`/spec/...`, `/name`, `/namespace`, `/labels/...`); a
/// pointer may name a string or a list of strings.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KindRequirement {
    /// An ACL resource (`job`, `kv`, `operator`, …).
    pub resource: String,
    /// `read` and/or `write`.
    pub actions: Vec<String>,
    /// For a namespaced resource: the namespace(s). Default (and when the
    /// pointer names nothing): the object's own namespace, or every
    /// namespace (`*`) for a cluster kind's object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// For a namespaced resource: the name(s) (job names for `job`).
    /// Default: namespace-wide (`*`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// For `kv`: the key path(s). Required; when the pointer names nothing,
    /// the entry does not apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// For `node`: the pool(s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// For `job`: the priority band claimed (default `normal`, as a submit
    /// defaults it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub band: Option<String>,
    /// For `job`: the job template the work goes through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// For `job`: host-level work (only a `job` rule with `host = true`
    /// covers it).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub host: bool,
    /// The entry applies only when this pointer names a true value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
}

/// One kind an app's manifest declares (`kinds[]`).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KindDecl {
    /// PascalCase and singular (`Repo`): a capital letter, then letters and
    /// digits, at most 63. The kind's name is `<app>/<name>`.
    pub name: String,
    /// One line on what an object of the kind is.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// `namespaced` (default) or `cluster`.
    #[serde(default)]
    pub scope: KindScope,
    /// JSON Schema (draft 7) for the spec: an object schema, at most 64
    /// KiB, with only local `$ref`s, closed by default. `default`s are
    /// filled at write.
    #[schemars(with = "serde_json::Map<String, Value>")]
    pub schema: Value,
    /// The schema of the status `fields` the app reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<KindStatus>,
    /// Example specs, each checked against the schema at install.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<Value>,
    /// Also ask the app on every apply that changes the spec (`POST
    /// /legion/v1/objects/validate`). Advisory: fails open.
    #[serde(default)]
    pub validate: bool,
    /// The ACL resource that governs the kind's objects (default `object`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<KindAccess>,
    /// Permissions writing (apply or delete) an object needs, resolved from
    /// its contents and checked as the writer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<KindRequirement>,
    /// JSON pointers into the object (`/spec/url`) an update may not
    /// change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub immutable: Vec<String>,
    /// How long a delete waits for the app to release the object (`10m`,
    /// at most `1h`); none deletes at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<String>,
    /// At most this many objects of the kind (default 10,000, at most
    /// 100,000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// The columns `legion object list` shows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<KindColumn>,
    /// Reserved for CEL validation rules (Phase O4): refused until then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Vec<serde_json::Map<String, Value>>>")]
    pub rules: Option<Value>,
}

impl KindDecl {
    /// The resource named in `access`, or `object`.
    pub fn access_name(&self) -> &str {
        self.access
            .as_ref()
            .map(|access| access.resource.trim())
            .unwrap_or("object")
    }

    /// The access resource, checked against the pinned set.
    pub fn access_resource(&self) -> Result<AclResource, String> {
        match self.access_name() {
            "object" => Ok(AclResource::Object),
            "job" => Ok(AclResource::Job),
            "scale" => Ok(AclResource::Scale),
            "node" => Ok(AclResource::Node),
            other => Err(format!(
                "access.resource {other:?} cannot govern objects; a kind maps onto object (the \
                 default), job, scale, or node"
            )),
        }
    }

    /// `cleanup`, parsed.
    pub fn cleanup_window(&self) -> Result<Option<std::time::Duration>, String> {
        let Some(raw) = self.cleanup.as_deref() else {
            return Ok(None);
        };
        let window = humantime::parse_duration(raw.trim())
            .map_err(|err| format!("cleanup {raw:?}: {err}"))?;
        if window.is_zero() || window > MAX_CLEANUP {
            return Err(format!(
                "cleanup {raw:?} must be more than 0 and at most 1h"
            ));
        }
        Ok(Some(window))
    }

    /// `maxObjects`, or its default.
    pub fn max_objects(&self) -> u64 {
        self.max_objects.unwrap_or(DEFAULT_MAX_OBJECTS)
    }

    /// How the install prompt words who may write the kind's objects.
    pub fn access_label(&self, app: &str) -> String {
        let mut text = match self.access_name() {
            "object" => format!("object:write on {app}/{}", self.name),
            resource if self.scope.is_namespaced() => {
                format!("{resource}:write in the object's namespace")
            }
            resource => format!("{resource}:write"),
        };
        for requirement in &self.requires {
            text.push_str(&format!(", and {}", requirement_label(requirement)));
        }
        text
    }
}

/// `job:write in the namespace spec.targetNamespace names`.
fn requirement_label(requirement: &KindRequirement) -> String {
    let mut text = format!("{}:{}", requirement.resource, requirement.actions.join("+"));
    if let Some(pointer) = &requirement.namespace {
        text.push_str(&format!(
            " in the namespace {} names",
            dotted(pointer.trim_start_matches('/'))
        ));
    }
    if let Some(pointer) = &requirement.path {
        text.push_str(&format!(" on the key {} names", dotted(pointer.trim_start_matches('/'))));
    }
    if let Some(pointer) = &requirement.pool {
        text.push_str(&format!(" in the pool {} names", dotted(pointer.trim_start_matches('/'))));
    }
    if requirement.host {
        text.push_str(" (host-level)");
    }
    if let Some(pointer) = &requirement.when {
        text.push_str(&format!(" when {} is set", dotted(pointer.trim_start_matches('/'))));
    }
    text
}

/// `spec/target` → `spec.target`.
fn dotted(path: &str) -> String {
    path.replace('/', ".")
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

/// A pointer into the object document: `/spec/…`, `/labels/…`, `/name`,
/// `/namespace`, and (for columns) `/status/…`.
fn validate_object_pointer(pointer: &str, allow_status: bool) -> Result<(), String> {
    let first = pointer
        .strip_prefix('/')
        .map(|rest| rest.split('/').next().unwrap_or_default());
    match first {
        Some("spec" | "labels" | "name" | "namespace") => Ok(()),
        Some("status") if allow_status => Ok(()),
        _ => Err(format!(
            "{pointer:?} must be a JSON pointer into the object: /spec/…, /labels/…, /name, \
             /namespace{}",
            if allow_status { ", or /status/…" } else { "" }
        )),
    }
}

/// Check one `requires` entry on its own.
fn validate_requirement(scope: KindScope, requirement: &KindRequirement) -> Result<(), String> {
    let resource = AclResource::parse(requirement.resource.trim()).ok_or_else(|| {
        format!(
            "{:?} is not an ACL resource ({})",
            requirement.resource,
            AclResource::all().join(", ")
        )
    })?;
    if requirement.actions.is_empty() {
        return Err(format!("{}: `actions` must not be empty", resource.as_str()));
    }
    for action in &requirement.actions {
        AclAction::parse(action.trim()).ok_or_else(|| {
            format!(
                "{}: action {action:?} must be `read` or `write`",
                resource.as_str()
            )
        })?;
    }
    let pointers = [
        ("namespace", &requirement.namespace),
        ("name", &requirement.name),
        ("path", &requirement.path),
        ("pool", &requirement.pool),
        ("band", &requirement.band),
        ("template", &requirement.template),
        ("when", &requirement.when),
    ];
    for (field, pointer) in pointers {
        if let Some(pointer) = pointer {
            validate_object_pointer(pointer, false)
                .map_err(|err| format!("{}: {field}: {err}", resource.as_str()))?;
        }
    }
    let only = |field: &str, set: bool, fits: bool, what: &str| -> Result<(), String> {
        if set && !fits {
            Err(format!(
                "{}: `{field}` applies only to {what}",
                resource.as_str()
            ))
        } else {
            Ok(())
        }
    };
    only(
        "namespace",
        requirement.namespace.is_some(),
        resource.is_namespaced(),
        "namespaced resources",
    )?;
    only(
        "name",
        requirement.name.is_some(),
        resource.is_namespaced(),
        "namespaced resources",
    )?;
    only(
        "path",
        requirement.path.is_some(),
        resource == AclResource::Kv,
        "kv",
    )?;
    only(
        "pool",
        requirement.pool.is_some(),
        resource == AclResource::Node,
        "node",
    )?;
    let is_job = resource == AclResource::Job;
    only("band", requirement.band.is_some(), is_job, "job")?;
    only("template", requirement.template.is_some(), is_job, "job")?;
    only("host", requirement.host, is_job, "job")?;
    if resource == AclResource::Kv && requirement.path.is_none() {
        return Err(
            "kv: a kv requirement names the key it is about with `path` (a pointer into the \
             object)"
                .into(),
        );
    }
    let _ = scope;
    Ok(())
}

/// Check an app's kind declarations on their own (app-object-kinds plan
/// §6.1): at most [`MAX_KINDS_PER_APP`]; names valid, not reserved, and not
/// the same noun as each other (two apps may share one: the kind is
/// `<app>/<Kind>`); schemas that compile, closed; examples that pass; an
/// `access` resource from the pinned set whose scope fits; `requires`,
/// `immutable`, and `columns` pointers into the object; `cleanup` at most an
/// hour; `maxObjects` in range; no `rules` (CEL, Phase O4).
pub fn validate_kind_decls(decls: &[KindDecl]) -> Result<(), String> {
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
                "{at}: the app already declares {}; an app's kind names are unique ignoring case \
                 and plurals",
                other.name
            ));
        }
        if decl.rules.is_some() {
            return Err(format!(
                "{at}: `rules` (CEL validation rules) are not supported yet; use JSON Schema \
                 (draft-7 conditionals included), `immutable`, and the app's Accepted condition"
            ));
        }
        let schema =
            KindSchema::compile(&decl.schema).map_err(|err| format!("{at}: schema {err}"))?;
        for (n, example) in decl.examples.iter().enumerate() {
            schema.check("spec", example).map_err(|errors| {
                format!(
                    "{at}: examples[{n}] does not match the kind's schema: {}",
                    errors.join("; ")
                )
            })?;
        }
        if let Some(status) = &decl.status {
            KindSchema::compile(&status.schema)
                .map_err(|err| format!("{at}: status.schema {err}"))?;
        }
        let resource = decl
            .access_resource()
            .map_err(|err| format!("{at}: {err}"))?;
        if resource != AclResource::Object && resource.is_namespaced() != decl.scope.is_namespaced()
        {
            return Err(format!(
                "{at}: access.resource `{}` is {}, but the kind is {}; job and scale govern \
                 namespaced kinds, node governs cluster kinds (or leave access out for the \
                 `object` resource)",
                resource.as_str(),
                if resource.is_namespaced() {
                    "namespaced"
                } else {
                    "cluster-scoped"
                },
                decl.scope.as_str()
            ));
        }
        for (n, requirement) in decl.requires.iter().enumerate() {
            validate_requirement(decl.scope, requirement)
                .map_err(|err| format!("{at}: requires[{n}]: {err}"))?;
        }
        for pointer in &decl.immutable {
            if !pointer.starts_with("/spec/") && pointer != "/spec" {
                return Err(format!(
                    "{at}: immutable: {pointer:?} must be a JSON pointer into the spec (/spec/…)"
                ));
            }
        }
        decl.cleanup_window()
            .map_err(|err| format!("{at}: {err}"))?;
        if let Some(max) = decl.max_objects {
            if max == 0 || max > MAX_MAX_OBJECTS {
                return Err(format!(
                    "{at}: maxObjects must be 1 to {MAX_MAX_OBJECTS}, got {max}"
                ));
            }
        }
        if decl.columns.len() > MAX_KIND_COLUMNS {
            return Err(format!(
                "{at}: columns: at most {MAX_KIND_COLUMNS}, got {}",
                decl.columns.len()
            ));
        }
        for column in &decl.columns {
            validate_object_pointer(column.pointer(), true)
                .map_err(|err| format!("{at}: columns: {err}"))?;
            if column.header().trim().is_empty() {
                return Err(format!("{at}: columns: a column needs a name"));
            }
        }
    }
    Ok(())
}

/// A kind's schema, compiled: closed (every schema with `properties` and
/// neither `additionalProperties` nor `patternProperties` refuses other
/// fields, except branches of `allOf`, `anyOf`, and `oneOf`), with its
/// `writeOnly` pointers collected.
pub struct KindSchema {
    /// The schema as declared.
    pub raw: Value,
    /// The schema as checked: closed.
    pub closed: Value,
    compiled: jsonschema::JSONSchema,
    /// Pointers below the spec (or status fields) whose values are
    /// `writeOnly`, with `*` for every key or item.
    pub write_only: Vec<Vec<String>>,
}

impl std::fmt::Debug for KindSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KindSchema")
            .field("raw", &self.raw)
            .finish_non_exhaustive()
    }
}

fn kind_schema_cache() -> &'static Mutex<HashMap<String, Arc<KindSchema>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<KindSchema>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The most schemas the cache keeps; past it, it starts over.
const KIND_SCHEMA_CACHE_LIMIT: usize = 512;

/// `schema` with every subschema that has `properties` and neither
/// `additionalProperties` nor `patternProperties` closed, except the
/// branches of a composition.
pub fn close_schema(schema: &Value) -> Value {
    let mut closed = schema.clone();
    close_into(&mut closed, true, 0);
    closed
}

fn close_into(schema: &mut Value, close_here: bool, depth: usize) {
    if depth > 64 {
        return;
    }
    let Value::Object(map) = schema else {
        return;
    };
    if close_here
        && map.contains_key("properties")
        && !map.contains_key("additionalProperties")
        && !map.contains_key("patternProperties")
    {
        map.insert("additionalProperties".into(), Value::Bool(false));
    }
    for key in ["properties", "patternProperties", "definitions", "$defs"] {
        if let Some(Value::Object(children)) = map.get_mut(key) {
            for child in children.values_mut() {
                close_into(child, true, depth + 1);
            }
        }
    }
    for key in ["items", "additionalProperties", "additionalItems", "contains"] {
        match map.get_mut(key) {
            Some(Value::Array(items)) => {
                for item in items {
                    close_into(item, true, depth + 1);
                }
            }
            Some(child @ Value::Object(_)) => close_into(child, true, depth + 1),
            _ => {}
        }
    }
    // A composition's branches each describe part of the object: closing one
    // would refuse the others' fields.
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(Value::Array(branches)) = map.get_mut(key) {
            for branch in branches {
                close_into(branch, false, depth + 1);
            }
        }
    }
    for key in ["not", "if", "then", "else"] {
        if let Some(child) = map.get_mut(key) {
            close_into(child, false, depth + 1);
        }
    }
}

/// Every `writeOnly: true` property's pointer below the schema's root, with
/// `*` for each key of an open map or item of a list.
fn collect_write_only(root: &Value, schema: &Value, at: &mut Vec<String>, out: &mut Vec<Vec<String>>, depth: usize) {
    if depth > 32 {
        return;
    }
    let schema = match schema.get("$ref").and_then(Value::as_str) {
        Some(reference) => match reference.strip_prefix('#') {
            Some("") => root,
            Some(pointer) => match root.pointer(pointer) {
                Some(target) => target,
                None => return,
            },
            None => return,
        },
        None => schema,
    };
    if schema.get("writeOnly").and_then(Value::as_bool) == Some(true) {
        out.push(at.clone());
        return;
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, child) in properties {
            at.push(name.clone());
            collect_write_only(root, child, at, out, depth + 1);
            at.pop();
        }
    }
    for key in ["additionalProperties", "items"] {
        if let Some(child) = schema.get(key).filter(|child| child.is_object()) {
            at.push("*".into());
            collect_write_only(root, child, at, out, depth + 1);
            at.pop();
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = schema.get(key).and_then(Value::as_array) {
            for branch in branches {
                collect_write_only(root, branch, at, out, depth + 1);
            }
        }
    }
}

/// `/a/0/b` → `["a", "0", "b"]`.
fn segments(pointer: &str) -> Vec<String> {
    pointer
        .split('/')
        .skip(1)
        .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// Whether `path` is at or below a `writeOnly` pattern.
fn under_write_only(patterns: &[Vec<String>], path: &[String]) -> bool {
    patterns.iter().any(|pattern| {
        pattern.len() <= path.len()
            && pattern
                .iter()
                .zip(path)
                .all(|(want, have)| want == "*" || want == have)
    })
}

/// `["a", "0", "b"]` → `a[0].b`, below `root`.
fn dotted_path(root: &str, path: &[String]) -> String {
    let mut out = root.to_string();
    for segment in path {
        if segment.parse::<usize>().is_ok() {
            out.push_str(&format!("[{segment}]"));
        } else {
            if !out.is_empty() {
                out.push('.');
            }
            out.push_str(segment);
        }
    }
    out
}

impl KindSchema {
    /// Compile a kind's schema (cached by its digest): a JSON object of at
    /// most 64 KiB describing an object, with only local `$ref`s, closed.
    pub fn compile(schema: &Value) -> Result<Arc<KindSchema>, String> {
        let digest = digest_value(schema);
        if let Some(found) = kind_schema_cache()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&digest)
        {
            return Ok(found.clone());
        }
        if schema.get("type").and_then(Value::as_str) != Some("object") {
            return Err("must describe an object (`type = \"object\"`)".into());
        }
        let closed = close_schema(schema);
        let compiled = compile_schema(&closed, MAX_KIND_SCHEMA_BYTES * 2)?;
        if schema.to_string().len() > MAX_KIND_SCHEMA_BYTES {
            return Err(format!(
                "is {} bytes; the limit is {MAX_KIND_SCHEMA_BYTES}",
                schema.to_string().len()
            ));
        }
        let mut write_only = Vec::new();
        collect_write_only(schema, schema, &mut Vec::new(), &mut write_only, 0);
        let compiled = Arc::new(KindSchema {
            raw: schema.clone(),
            closed,
            compiled,
            write_only,
        });
        let mut cache = kind_schema_cache()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if cache.len() >= KIND_SCHEMA_CACHE_LIMIT {
            cache.clear();
        }
        cache.insert(digest, compiled.clone());
        Ok(compiled)
    }

    /// The schema's digest: what a stored object records it was checked
    /// against.
    pub fn digest(&self) -> String {
        digest_value(&self.raw)
    }

    /// `value` with the schema's defaults filled in fields it lacks.
    // TODO(apps hardening): `fill_defaults` gains `$ref`-cycle and size caps
    // on PR #60's branch (security review M5); objects pick them up with the
    // merge, since they call the same function.
    pub fn filled(&self, value: &Value) -> Value {
        let mut filled = value.clone();
        fill_defaults(&self.raw, &self.raw, &mut filled, 0);
        filled
    }

    /// Fill defaults, then validate: the filled value, or one message per
    /// problem with its path below `root` (`spec.branch: 5 is not of type
    /// "string"`, `spec.brnach: unknown field (did you mean branch?)`). A
    /// problem at or below a `writeOnly` value never echoes it.
    pub fn check(&self, root: &str, value: &Value) -> Result<Value, Vec<String>> {
        let effective = self.filled(value);
        let problems: Vec<String> = match self.compiled.validate(&effective) {
            Ok(()) => Vec::new(),
            Err(errors) => errors
                .flat_map(|err| {
                    let path = segments(&err.instance_path.to_string());
                    match &err.kind {
                        jsonschema::error::ValidationErrorKind::AdditionalProperties {
                            unexpected,
                        } => {
                            let known = self.known_fields(&err.schema_path.to_string());
                            unexpected
                                .iter()
                                .map(|field| {
                                    let mut at = path.clone();
                                    at.push(field.clone());
                                    let hint = crate::structs::spec_fields::suggest_owned(
                                        field,
                                        &known.iter().map(String::as_str).collect::<Vec<_>>(),
                                    )
                                    .map(|index| format!(" (did you mean {}?)", known[index]))
                                    .unwrap_or_default();
                                    format!("{}: unknown field{hint}", dotted_path(root, &at))
                                })
                                .collect::<Vec<_>>()
                        }
                        _ if under_write_only(&self.write_only, &path) => vec![format!(
                            "{}: invalid value (write-only, not shown)",
                            dotted_path(root, &path)
                        )],
                        _ => vec![format!("{}: {err}", dotted_path(root, &path))],
                    }
                })
                .collect(),
        };
        if problems.is_empty() {
            Ok(effective)
        } else {
            Err(problems)
        }
    }

    /// The property names the object schema owning an `additionalProperties`
    /// failure declares, for a did-you-mean.
    fn known_fields(&self, schema_path: &str) -> Vec<String> {
        let parent = schema_path
            .strip_suffix("/additionalProperties")
            .unwrap_or(schema_path);
        let node = if parent.is_empty() {
            Some(&self.closed)
        } else {
            self.closed.pointer(parent)
        };
        node.and_then(|node| node.get("properties"))
            .and_then(Value::as_object)
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// `value` with every `writeOnly` value replaced by [`REDACTED`].
    pub fn redacted(&self, value: &Value) -> Value {
        let mut out = value.clone();
        for pattern in &self.write_only {
            redact_at(&mut out, pattern);
        }
        out
    }

    /// Whether `path` (below the spec) is write-only.
    pub fn is_write_only(&self, path: &[String]) -> bool {
        under_write_only(&self.write_only, path)
    }
}

fn redact_at(value: &mut Value, pattern: &[String]) {
    let Some((head, rest)) = pattern.split_first() else {
        if !value.is_null() {
            *value = Value::String(REDACTED.to_string());
        }
        return;
    };
    match value {
        Value::Object(map) => {
            if head == "*" {
                for child in map.values_mut() {
                    redact_at(child, rest);
                }
            } else if let Some(child) = map.get_mut(head) {
                redact_at(child, rest);
            }
        }
        Value::Array(items) => {
            if head == "*" {
                for child in items {
                    redact_at(child, rest);
                }
            } else if let Some(child) = head.parse::<usize>().ok().and_then(|i| items.get_mut(i)) {
                redact_at(child, rest);
            }
        }
        _ => {}
    }
}

/// Whether a kind is installed or left behind by its app.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum KindState {
    /// Its app is installed and declares it.
    Active,
    /// Its app was uninstalled, or dropped it, with `--force`; its objects
    /// stay, readable and deletable with the kind's recorded access.
    Orphaned,
}

impl KindState {
    pub fn as_str(&self) -> &'static str {
        match self {
            KindState::Active => "active",
            KindState::Orphaned => "orphaned",
        }
    }

    pub fn parse(raw: &str) -> KindState {
        if raw == "orphaned" {
            KindState::Orphaned
        } else {
            KindState::Active
        }
    }
}

/// One kind as the manager knows it: its declaration, owner, state, and
/// schemas compiled.
#[derive(Debug, Clone)]
pub struct InstalledKind {
    pub app: String,
    /// The owning app's version, empty when it is not installed.
    pub app_version: String,
    /// The manifest digest the declaration came from.
    pub digest: String,
    /// Whether the owning app is installed and enabled.
    pub app_enabled: bool,
    pub state: KindState,
    pub decl: KindDecl,
    pub schema: Arc<KindSchema>,
    pub status_schema: Option<Arc<KindSchema>>,
    pub access: AclResource,
    pub cleanup: Option<std::time::Duration>,
}

impl InstalledKind {
    /// Compile a declaration as the kind registry holds it.
    pub fn new(
        app: &str,
        decl: KindDecl,
        digest: &str,
        state: KindState,
        app_version: &str,
        app_enabled: bool,
    ) -> Result<InstalledKind, String> {
        let schema = KindSchema::compile(&decl.schema)
            .map_err(|err| format!("kind {app}/{}: schema {err}", decl.name))?;
        let status_schema = match &decl.status {
            None => None,
            Some(status) => Some(
                KindSchema::compile(&status.schema)
                    .map_err(|err| format!("kind {app}/{}: status.schema {err}", decl.name))?,
            ),
        };
        let access = decl
            .access_resource()
            .map_err(|err| format!("kind {app}/{}: {err}", decl.name))?;
        let cleanup = decl
            .cleanup_window()
            .map_err(|err| format!("kind {app}/{}: {err}", decl.name))?;
        Ok(InstalledKind {
            app: app.to_string(),
            app_version: app_version.to_string(),
            digest: digest.to_string(),
            app_enabled,
            state,
            decl,
            schema,
            status_schema,
            access,
            cleanup,
        })
    }

    /// `<app>/<Kind>`: the kind's name, what `kinds` selectors match.
    pub fn qualified(&self) -> String {
        qualified_kind(&self.app, &self.decl.name)
    }

    pub fn is_namespaced(&self) -> bool {
        self.decl.scope.is_namespaced()
    }

    pub fn is_active(&self) -> bool {
        self.state == KindState::Active
    }

    /// The spec with defaults filled, or one message per problem with its
    /// path below `spec`.
    pub fn check_spec(&self, spec: &Value) -> Result<Value, Vec<String>> {
        self.schema.check("spec", spec)
    }

    /// Status `fields` checked against the status schema (any object
    /// without one).
    pub fn check_fields(&self, fields: &Value) -> Result<(), Vec<String>> {
        if !fields.is_object() {
            return Err(vec!["status.fields must be a JSON object".to_string()]);
        }
        match &self.status_schema {
            None => Ok(()),
            Some(schema) => schema.check("status.fields", fields).map(|_| ()),
        }
    }

    /// The ACL scope reads and writes of an object of this kind are checked
    /// on (app-object-kinds plan §6.4): the object's namespace and name for
    /// the `object` resource (namespace `*` for a cluster kind), the
    /// namespace with name `*` (namespace-wide) for `job` and `scale`, and
    /// no instance attributes for `node`. Always the `<app>/<Kind>`
    /// attribute, which `kinds` selectors match.
    pub fn access_scope(&self, namespace: &str, name: &str) -> AclScope {
        access_scope(self.access, &self.qualified(), namespace, name)
    }
}

/// See [`InstalledKind::access_scope`].
pub fn access_scope(access: AclResource, qualified: &str, namespace: &str, name: &str) -> AclScope {
    match access {
        AclResource::Object => AclScope::object(qualified, namespace, name, true),
        AclResource::Job | AclResource::Scale => AclScope {
            namespace: Some(if namespace.is_empty() {
                "*".to_string()
            } else {
                namespace.to_string()
            }),
            name: Some("*".to_string()),
            kind: Some(qualified.to_string()),
            band: (access == AclResource::Job).then(|| "normal".to_string()),
            ..Default::default()
        },
        _ => AclScope {
            kind: Some(qualified.to_string()),
            ..Default::default()
        },
    }
}

/// `<app>/<Kind>`.
pub fn qualified_kind(app: &str, kind: &str) -> String {
    format!("{app}/{kind}")
}

/// Why a typed kind did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KindLookup {
    /// No kind matches; the closest, if any.
    Unknown { suggestion: Option<String> },
    /// A short name that several kinds share: their qualified names.
    Ambiguous { candidates: Vec<String> },
    /// A short name where a qualified one is required; the one it would
    /// mean, if exactly one.
    Unqualified { candidate: Option<String> },
}

impl KindLookup {
    /// The refusal, worded for `typed`.
    pub fn message(&self, typed: &str) -> String {
        match self {
            KindLookup::Unknown { suggestion } => format!(
                "unknown kind {typed:?}{}\nHint: `legion object kinds` lists the kinds installed \
                 apps declare",
                suggestion
                    .as_ref()
                    .map(|name| format!(" (did you mean {name}?)"))
                    .unwrap_or_default()
            ),
            KindLookup::Ambiguous { candidates } => format!(
                "{typed:?} names {} kinds: {}\nHint: name one: legion object get {} <name>",
                candidates.len(),
                candidates.join(", "),
                candidates.first().map(String::as_str).unwrap_or("<app>/<Kind>")
            ),
            KindLookup::Unqualified { candidate } => match candidate {
                Some(qualified) => format!(
                    "kind {typed:?} must be qualified with its app: kind = \"{qualified}\"\nHint: \
                     a file names the kind it means for good; a short name could mean another \
                     app's kind tomorrow"
                ),
                None => format!(
                    "kind {typed:?} must be qualified with its app: kind = \"<app>/{typed}\"\n\
                     Hint: `legion object kinds` lists the kinds installed apps declare"
                ),
            },
        }
    }
}

/// The kinds the manager knows: active and orphaned, with their schemas
/// compiled.
#[derive(Debug, Clone, Default)]
pub struct ObjectKindRegistry {
    kinds: Vec<InstalledKind>,
}

impl ObjectKindRegistry {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_kinds(kinds: Vec<InstalledKind>) -> Self {
        ObjectKindRegistry { kinds }
    }

    pub fn kinds(&self) -> &[InstalledKind] {
        &self.kinds
    }

    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    /// `app`'s kinds (active and orphaned).
    pub fn of_app<'a>(&'a self, app: &'a str) -> impl Iterator<Item = &'a InstalledKind> + 'a {
        self.kinds.iter().filter(move |kind| kind.app == app)
    }

    /// `app`'s kind named exactly `kind`.
    pub fn get(&self, app: &str, kind: &str) -> Option<&InstalledKind> {
        self.kinds
            .iter()
            .find(|installed| installed.app == app && installed.decl.name == kind)
    }

    /// The kind a qualified name means (`gitops/Repo`, any case, plural
    /// optional after the app): what files and `ApplyObject` take. A short
    /// name is refused, naming the qualified kind when exactly one matches.
    pub fn resolve_qualified(&self, typed: &str) -> Result<&InstalledKind, KindLookup> {
        let typed = typed.trim();
        match typed.split_once('/') {
            Some((app, kind)) => self
                .kinds
                .iter()
                .find(|installed| installed.app == app && same_noun(kind, &installed.decl.name))
                .ok_or_else(|| KindLookup::Unknown {
                    suggestion: self.suggest(typed),
                }),
            None => {
                let matches = self.short_matches(typed);
                Err(KindLookup::Unqualified {
                    candidate: (matches.len() == 1).then(|| matches[0].qualified()),
                })
            }
        }
    }

    /// The kind a name typed at a terminal means: qualified, or a short
    /// name (any case, plural optional) that exactly one kind has.
    pub fn resolve(&self, typed: &str) -> Result<&InstalledKind, KindLookup> {
        let typed = typed.trim();
        if typed.contains('/') {
            return self.resolve_qualified(typed);
        }
        let mut matches = self.short_matches(typed);
        match matches.len() {
            0 => Err(KindLookup::Unknown {
                suggestion: self.suggest(typed),
            }),
            1 => Ok(matches.remove(0)),
            _ => {
                // An active kind wins over orphaned ones of the same name.
                let active: Vec<&InstalledKind> =
                    matches.iter().copied().filter(|kind| kind.is_active()).collect();
                if active.len() == 1 {
                    return Ok(active[0]);
                }
                Err(KindLookup::Ambiguous {
                    candidates: matches.iter().map(|kind| kind.qualified()).collect(),
                })
            }
        }
    }

    fn short_matches(&self, typed: &str) -> Vec<&InstalledKind> {
        self.kinds
            .iter()
            .filter(|installed| same_noun(typed, &installed.decl.name))
            .collect()
    }

    /// The kind closest to `typed`, qualified, for a did-you-mean.
    pub fn suggest(&self, typed: &str) -> Option<String> {
        let names: Vec<String> = self.kinds.iter().map(InstalledKind::qualified).collect();
        let bare = typed.rsplit('/').next().unwrap_or(typed);
        let short: Vec<&str> = self.kinds.iter().map(|kind| kind.decl.name.as_str()).collect();
        crate::structs::spec_fields::suggest_owned(bare, &short).map(|index| names[index].clone())
    }
}

/// An object as a person writes it (`legion object apply -f`): a qualified
/// kind, a name, a namespace for a namespaced kind, labels, and the spec.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectInput {
    /// The kind: `<app>/<Kind>` (`gitops/Repo`).
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
        return Err("the object needs a `kind` (<app>/<Kind>)".into());
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

/// The object as `legion object get -j` shows it, and every pointer in a
/// kind's declaration points into.
pub fn object_document(
    name: &str,
    namespace: &str,
    labels: &BTreeMap<String, String>,
    spec: &Value,
    status: &Value,
) -> Value {
    json!({
        "name": name,
        "namespace": namespace,
        "labels": labels,
        "spec": spec,
        "status": status,
    })
}

/// One permission a `requires` entry resolved to for one object: the
/// concrete check made against its writer, and kept on the object as part
/// of its `authority`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedPermission {
    pub resource: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub band: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub host: bool,
    /// The pointers that supplied the scope (`spec.targetNamespace`), for
    /// messages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<String>,
}

impl ResolvedPermission {
    /// The ACL scope the corresponding core RPC builds for this check.
    pub fn scope(&self) -> AclScope {
        AclScope {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            path: self.path.clone(),
            pool: self.pool.clone(),
            band: self.band.clone(),
            template: self.template.clone(),
            host: self.host,
            ..Default::default()
        }
    }

    /// `job:write in team-b (from spec.targetNamespace)`.
    pub fn label(&self) -> String {
        let mut text = format!("{}:{}", self.resource, self.action);
        if let Some(namespace) = &self.namespace {
            text.push_str(&format!(" in {namespace}"));
        }
        if let Some(name) = self.name.as_ref().filter(|name| name.as_str() != "*") {
            text.push_str(&format!(" on {name}"));
        }
        if let Some(path) = &self.path {
            text.push_str(&format!(" on {path}"));
        }
        if let Some(pool) = &self.pool {
            text.push_str(&format!(" in pool {pool}"));
        }
        if let Some(band) = self.band.as_ref().filter(|band| band.as_str() != "normal") {
            text.push_str(&format!(" in band {band}"));
        }
        if let Some(template) = &self.template {
            text.push_str(&format!(" through template {template}"));
        }
        if self.host {
            text.push_str(" (host-level)");
        }
        if !self.from.is_empty() {
            text.push_str(&format!(" (from {})", self.from.join(", ")));
        }
        text
    }
}

/// The strings a pointer names in the document: a string, or a list of
/// strings. `Ok(None)` when it names nothing (or null).
fn pointer_strings(doc: &Value, pointer: &str) -> Result<Option<Vec<String>>, String> {
    match doc.pointer(pointer) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(vec![text.trim().to_string()])),
        Some(Value::Array(items)) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    Value::String(text) if !text.trim().is_empty() => {
                        out.push(text.trim().to_string())
                    }
                    _ => {
                        return Err(format!(
                            "{} must be a string or a list of strings",
                            dotted(pointer.trim_start_matches('/'))
                        ))
                    }
                }
            }
            Ok((!out.is_empty()).then_some(out))
        }
        Some(_) => Err(format!(
            "{} must be a string or a list of strings",
            dotted(pointer.trim_start_matches('/'))
        )),
    }
}

/// Whether a `when` pointer names a true value.
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(Value::String(text)) => !text.is_empty(),
        Some(Value::Number(number)) => number.as_f64().is_some_and(|n| n != 0.0),
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::Object(map)) => !map.is_empty(),
    }
}

/// Resolve a kind's `requires` against one object (the document carries
/// the effective spec): the concrete permissions its writer must hold, each
/// with the scope the corresponding core RPC builds (app-object-kinds plan
/// §6.4). `namespace` is the object's (empty for a cluster kind).
pub fn resolve_requirements(
    kind: &InstalledKind,
    doc: &Value,
    namespace: &str,
) -> Result<Vec<ResolvedPermission>, String> {
    let mut out = Vec::new();
    for requirement in &kind.decl.requires {
        if let Some(when) = &requirement.when {
            if !truthy(doc.pointer(when)) {
                continue;
            }
        }
        let resource = AclResource::parse(requirement.resource.trim())
            .ok_or_else(|| format!("requires: unknown resource {:?}", requirement.resource))?;
        let mut from = Vec::new();
        let mut pick = |pointer: &Option<String>| -> Result<Option<Vec<String>>, String> {
            match pointer {
                None => Ok(None),
                Some(pointer) => {
                    let found = pointer_strings(doc, pointer)?;
                    if found.is_some() {
                        from.push(dotted(pointer.trim_start_matches('/')));
                    }
                    Ok(found)
                }
            }
        };
        let namespaces: Vec<Option<String>> = if resource.is_namespaced() {
            let named = pick(&requirement.namespace)?;
            match named {
                Some(values) => {
                    for value in &values {
                        crate::structs::namespace::validate_namespace_name(value)?;
                    }
                    values.into_iter().map(Some).collect()
                }
                None if namespace.is_empty() => vec![Some("*".to_string())],
                None => vec![Some(namespace.to_string())],
            }
        } else {
            vec![None]
        };
        let names: Vec<Option<String>> = if resource.is_namespaced() {
            match pick(&requirement.name)? {
                Some(values) => values.into_iter().map(Some).collect(),
                None => vec![Some("*".to_string())],
            }
        } else {
            vec![None]
        };
        let paths: Vec<Option<String>> = if resource == AclResource::Kv {
            match pick(&requirement.path)? {
                Some(values) => values.into_iter().map(Some).collect(),
                // A key the object does not name is not read: the entry
                // does not apply.
                None => continue,
            }
        } else {
            vec![None]
        };
        let pools: Vec<Option<String>> = match pick(&requirement.pool)? {
            Some(values) => values.into_iter().map(Some).collect(),
            None => vec![None],
        };
        let (band, template) = if resource == AclResource::Job {
            let band = match pick(&requirement.band)? {
                Some(values) => crate::structs::Band::parse(&values[0])?
                    .as_str()
                    .to_string(),
                None => crate::structs::Band::default().as_str().to_string(),
            };
            let template = pick(&requirement.template)?.map(|values| values[0].clone());
            (Some(band), template)
        } else {
            (None, None)
        };
        for action in &requirement.actions {
            for namespace in &namespaces {
                for name in &names {
                    for path in &paths {
                        for pool in &pools {
                            out.push(ResolvedPermission {
                                resource: resource.as_str().to_string(),
                                action: action.trim().to_string(),
                                namespace: namespace.clone(),
                                name: name.clone(),
                                path: path.clone(),
                                pool: pool.clone(),
                                band: band.clone(),
                                template: template.clone(),
                                host: requirement.host,
                                from: from.clone(),
                            });
                            if out.len() > MAX_RESOLVED_REQUIREMENTS {
                                return Err(format!(
                                    "requires resolves to more than \
                                     {MAX_RESOLVED_REQUIREMENTS} permissions for this object; \
                                     name fewer namespaces, keys, or pools"
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// The permissions an object's writer was checked for, and who and when:
/// stored on the object (app-object-kinds plan §6.4).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectAuthority {
    pub permissions: Vec<ResolvedPermission>,
    /// The writer's principal and accessor id.
    pub authorized_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accessor: Option<String>,
    pub authorized_ms: i64,
    /// The kind declaration's digest the permissions were resolved under.
    #[serde(default)]
    pub requires_digest: String,
}

/// The digest of a kind's `requires`, so an upgrade can count objects
/// authorized under different requirements.
pub fn requires_digest(decl: &KindDecl) -> String {
    digest_value(&serde_json::to_value(&decl.requires).unwrap_or(Value::Null))
}

/// The `immutable` pointers whose value differs between the stored object
/// and the update (both documents, effective specs).
pub fn immutable_changes(kind: &InstalledKind, before: &Value, after: &Value) -> Vec<String> {
    kind.decl
        .immutable
        .iter()
        .filter(|pointer| before.pointer(pointer) != after.pointer(pointer))
        .map(|pointer| {
            let path = dotted(pointer.trim_start_matches('/'));
            let spec_path: Vec<String> = segments(pointer).into_iter().skip(1).collect();
            if kind.schema.is_write_only(&spec_path) {
                format!("{path} is immutable")
            } else {
                format!(
                    "{path} is immutable (was {})",
                    before
                        .pointer(pointer)
                        .map(short)
                        .unwrap_or_else(|| "unset".to_string())
                )
            }
        })
        .collect()
}

/// One line per field that differs between two specs (or label maps):
/// `spec.branch: "main" → "release"`, `spec.path: added "jobs"`,
/// `spec.depth: removed 3`, `spec.token: changed (redacted)`. Records are
/// compared field by field; anything else (lists included) whole. `secret`
/// says which paths below the root are write-only.
pub fn spec_diff(
    root: &str,
    before: &Value,
    after: &Value,
    secret: &dyn Fn(&[String]) -> bool,
) -> Vec<String> {
    let mut lines = Vec::new();
    diff_into(root, &mut Vec::new(), before, after, secret, &mut lines);
    lines
}

fn diff_into(
    root: &str,
    at: &mut Vec<String>,
    before: &Value,
    after: &Value,
    secret: &dyn Fn(&[String]) -> bool,
    lines: &mut Vec<String>,
) {
    if before == after {
        return;
    }
    let path = dotted_path(root, at);
    if secret(at) {
        let word = match (before.is_null(), after.is_null()) {
            (true, false) => "added",
            (false, true) => "removed",
            _ => "changed",
        };
        lines.push(format!("{path}: {word} (redacted)"));
        return;
    }
    match (before, after) {
        (Value::Object(old), Value::Object(new)) => {
            let mut keys: Vec<&String> = old.keys().chain(new.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                at.push(key.clone());
                match (old.get(key), new.get(key)) {
                    (Some(a), Some(b)) => diff_into(root, at, a, b, secret, lines),
                    (None, Some(b)) => {
                        if secret(at) {
                            lines.push(format!("{}: added (redacted)", dotted_path(root, at)));
                        } else {
                            lines.push(format!("{}: added {}", dotted_path(root, at), short(b)));
                        }
                    }
                    (Some(a), None) => {
                        if secret(at) {
                            lines.push(format!("{}: removed (redacted)", dotted_path(root, at)));
                        } else {
                            lines.push(format!("{}: removed {}", dotted_path(root, at), short(a)));
                        }
                    }
                    (None, None) => {}
                }
                at.pop();
            }
        }
        (a, b) => lines.push(format!("{path}: {} → {}", short(a), short(b))),
    }
}

/// A value for a message, cut at 120 characters.
fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 120 {
        format!("{}…", text.chars().take(119).collect::<String>())
    } else {
        text
    }
}

/// One condition of an object's status (`Ready`, `Accepted`, or the
/// app's own).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectCondition {
    /// PascalCase: `Ready`, `Accepted`, or the app's own.
    #[serde(rename = "type")]
    pub kind: String,
    /// `true`, `false`, or `unknown`.
    pub status: String,
    /// PascalCase, machine-readable (`Synced`, `RepoUnreachable`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// For people, at most 1 KiB.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// When `status` last changed (RFC 3339), kept by the manager.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub since: String,
}

/// An object's status as its app reports it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectStatus {
    pub observed_generation: u64,
    #[serde(default)]
    pub conditions: Vec<ObjectCondition>,
    /// The app's own payload, checked against the kind's status schema.
    #[serde(default)]
    pub fields: Value,
}

impl ObjectStatus {
    pub fn condition(&self, kind: &str) -> Option<&ObjectCondition> {
        self.conditions
            .iter()
            .find(|condition| condition.kind == kind)
    }
}

/// Check the conditions an app reports (types PascalCase and unique,
/// statuses `true`/`false`/`unknown`, the limits) and normalize them.
pub fn validate_conditions(conditions: &mut [ObjectCondition]) -> Result<(), String> {
    if conditions.len() > MAX_CONDITIONS {
        return Err(format!(
            "status: at most {MAX_CONDITIONS} conditions, got {}",
            conditions.len()
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for condition in conditions.iter_mut() {
        let shaped = !condition.kind.is_empty()
            && condition.kind.len() <= 64
            && condition.kind.starts_with(|c: char| c.is_ascii_uppercase())
            && condition.kind.chars().all(|c| c.is_ascii_alphanumeric());
        if !shaped {
            return Err(format!(
                "status: condition type {:?} must be PascalCase (Ready, Accepted), at most 64",
                condition.kind
            ));
        }
        if !seen.insert(condition.kind.clone()) {
            return Err(format!(
                "status: condition {} appears twice",
                condition.kind
            ));
        }
        condition.status = condition.status.trim().to_ascii_lowercase();
        if !matches!(condition.status.as_str(), "true" | "false" | "unknown") {
            return Err(format!(
                "status: condition {}: status {:?} must be true, false, or unknown",
                condition.kind, condition.status
            ));
        }
        if condition.reason.len() > 128
            || condition
                .reason
                .chars()
                .any(|c| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        {
            return Err(format!(
                "status: condition {}: reason {:?} must be letters and digits, at most 128",
                condition.kind, condition.reason
            ));
        }
        if condition.message.len() > MAX_CONDITION_MESSAGE_BYTES {
            return Err(format!(
                "status: condition {}: the message is {} bytes; the limit is \
                 {MAX_CONDITION_MESSAGE_BYTES}",
                condition.kind,
                condition.message.len()
            ));
        }
    }
    Ok(())
}

/// Carry each condition's `since` over from the stored status when its
/// status did not change, and stamp `now` when it did.
pub fn stamp_condition_times(
    conditions: &mut [ObjectCondition],
    previous: Option<&ObjectStatus>,
    now: &str,
) {
    for condition in conditions.iter_mut() {
        let kept = previous
            .and_then(|previous| previous.condition(&condition.kind))
            .filter(|old| old.status == condition.status)
            .map(|old| old.since.clone());
        condition.since = kept.unwrap_or_else(|| now.to_string());
    }
}

/// The condition transitions worth an event: `Ready` or `Accepted`
/// changed status (or appeared).
pub fn condition_transitions(
    before: Option<&ObjectStatus>,
    after: &ObjectStatus,
) -> Vec<(String, String, String)> {
    [CONDITION_READY, CONDITION_ACCEPTED]
        .iter()
        .filter_map(|kind| {
            let new = after.condition(kind)?;
            let old = before
                .and_then(|before| before.condition(kind))
                .map(|old| old.status.clone())
                .unwrap_or_default();
            (old != new.status).then(|| (kind.to_string(), old, new.status.clone()))
        })
        .collect()
}

/// How an object's spec reads against its kind now.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ObjectState {
    /// Its kind is active, and the spec passes the current schema.
    Valid,
    /// The spec fails the kind's current schema (an upgrade tightened it).
    Invalid,
    /// Its kind is orphaned (its app uninstalled or dropped it).
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

/// An object's stored spec judged against its kind now: its effective spec
/// (defaults filled for fields it lacks; the stored spec when it cannot be
/// judged), its state, and why.
pub fn judge(kind: Option<&InstalledKind>, spec: &Value) -> (Value, ObjectState, String) {
    match kind {
        None => (
            spec.clone(),
            ObjectState::Orphaned,
            "no declaration of its kind is recorded".to_string(),
        ),
        Some(kind) => {
            let state_if_valid = if kind.is_active() {
                ObjectState::Valid
            } else {
                ObjectState::Orphaned
            };
            match kind.check_spec(spec) {
                Ok(effective) => (
                    effective,
                    state_if_valid,
                    if kind.is_active() {
                        String::new()
                    } else {
                        format!(
                            "app {} no longer declares the kind (uninstalled, or upgraded with \
                             --force)",
                            kind.app
                        )
                    },
                ),
                Err(errors) if kind.is_active() => (
                    kind.schema.filled(spec),
                    ObjectState::Invalid,
                    errors.join("; "),
                ),
                Err(_) => (
                    spec.clone(),
                    ObjectState::Orphaned,
                    format!("app {} no longer declares the kind", kind.app),
                ),
            }
        }
    }
}

/// What an object's summary is built from.
pub struct SummaryInputs<'a> {
    pub state: ObjectState,
    pub deleting: bool,
    /// Whether the owning app is enabled and has a running allocation.
    pub app_running: bool,
    pub generation: u64,
    pub status: Option<&'a ObjectStatus>,
}

/// An object's summary, whatever its kind (app-object-kinds plan §6.7):
/// `Orphaned`, `Deleting`, `Invalid`, `Stale`, `Pending`, `Rejected:
/// <reason>`, `NotReady: <reason>`, `Ready`, or (an app that reports only
/// `Accepted`) `Accepted`. The second value is the reason or message, when
/// there is one.
pub fn summarize(inputs: &SummaryInputs<'_>) -> (String, String) {
    if inputs.state == ObjectState::Orphaned {
        return ("Orphaned".into(), String::new());
    }
    if inputs.deleting {
        return ("Deleting".into(), String::new());
    }
    if inputs.state == ObjectState::Invalid {
        return ("Invalid".into(), String::new());
    }
    if !inputs.app_running {
        return ("Stale".into(), String::new());
    }
    let Some(status) = inputs.status else {
        return ("Pending".into(), String::new());
    };
    if status.observed_generation < inputs.generation {
        return ("Pending".into(), String::new());
    }
    let explain = |condition: &ObjectCondition| -> String {
        match (condition.reason.is_empty(), condition.message.is_empty()) {
            (false, false) => format!("{}: {}", condition.reason, condition.message),
            (false, true) => condition.reason.clone(),
            (true, false) => condition.message.clone(),
            (true, true) => String::new(),
        }
    };
    if let Some(accepted) = status
        .condition(CONDITION_ACCEPTED)
        .filter(|condition| condition.status == "false")
    {
        let reason = if accepted.reason.is_empty() {
            "Rejected".to_string()
        } else {
            format!("Rejected: {}", accepted.reason)
        };
        return (reason, explain(accepted));
    }
    match status.condition(CONDITION_READY) {
        Some(ready) if ready.status == "true" => ("Ready".into(), explain(ready)),
        Some(ready) if ready.status == "false" => {
            let label = if ready.reason.is_empty() {
                "NotReady".to_string()
            } else {
                format!("NotReady: {}", ready.reason)
            };
            (label, explain(ready))
        }
        Some(ready) => ("Pending".into(), explain(ready)),
        None => match status.condition(CONDITION_ACCEPTED) {
            Some(accepted) if accepted.status == "true" => ("Accepted".into(), explain(accepted)),
            _ => ("Pending".into(), String::new()),
        },
    }
}

/// Whether a summary is settled for `--wait`: `Some(true)` on success,
/// `Some(false)` on a failure, `None` while it may still change.
pub fn summary_settled(summary: &str) -> Option<bool> {
    if summary == "Ready" || summary == "Accepted" {
        Some(true)
    } else if summary.starts_with("Rejected")
        || summary.starts_with("NotReady")
        || summary == "Invalid"
        || summary == "Orphaned"
    {
        Some(false)
    } else {
        None
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

/// The shard count stored (and indexed) with every object: the
/// autoscaler's.
pub const STORED_SHARD_COUNT: u32 = crate::autoscaler::shard::SHARDS;

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
    /// `<app>/<Kind>`.
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
            "  {:<22} {}; written with {}",
            qualified_kind(app, &decl.name),
            decl.scope.as_str(),
            decl.access_label(app)
        );
        if decl.validate {
            line.push_str("; checked by the app (advisory)");
        }
        if let Some(cleanup) = &decl.cleanup {
            line.push_str(&format!("; deletes wait up to {cleanup} for the app"));
        }
        lines.push(line);
    }
    lines
}

/// What changed in one kind between two declarations, for the upgrade
/// prompt: each spec default that changed (old → new), and whether
/// `requires` changed.
pub fn default_changes(before: &KindDecl, after: &KindDecl) -> Vec<(String, Value, Value)> {
    let mut out = Vec::new();
    collect_defaults(&before.schema, &before.schema, "", &mut |path, old| {
        let new = default_at(&after.schema, path);
        if let Some(new) = new {
            if new != *old {
                out.push((path.to_string(), old.clone(), new));
            }
        }
    });
    out
}

fn collect_defaults(root: &Value, schema: &Value, at: &str, f: &mut dyn FnMut(&str, &Value)) {
    if at.len() > 1024 {
        return;
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (name, child) in properties {
        let child = match child.get("$ref").and_then(Value::as_str) {
            Some(reference) => match reference.strip_prefix('#').and_then(|p| root.pointer(p)) {
                Some(target) if target != schema => target,
                _ => continue,
            },
            None => child,
        };
        let path = format!("{at}/{name}");
        if let Some(default) = child.get("default") {
            f(&path, default);
        }
        collect_defaults(root, child, &path, f);
    }
}

fn default_at(schema: &Value, path: &str) -> Option<Value> {
    let mut node = schema;
    for segment in path.split('/').skip(1) {
        node = node.get("properties")?.get(segment)?;
        if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
            node = schema.pointer(reference.strip_prefix('#')?)?;
        }
    }
    node.get("default").cloned()
}

/// A JSON object (map) of `json`, or an empty one.
pub fn as_map(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
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
                "required": ["url"],
                "properties": {
                    "url": { "type": "string" },
                    "branch": { "type": "string", "default": "main" },
                    "token": { "type": "string", "writeOnly": true },
                    "targetNamespace": { "type": "string" },
                    "credentials": { "type": "object", "properties": { "kv": { "type": "string" } } },
                    "allowHost": { "type": "boolean" },
                },
            }),
            status: None,
            examples: vec![json!({ "url": "https://x" })],
            validate: false,
            access: None,
            requires: Vec::new(),
            immutable: Vec::new(),
            cleanup: None,
            max_objects: None,
            columns: Vec::new(),
            rules: None,
        }
    }

    fn kind(app: &str, decl: KindDecl) -> InstalledKind {
        InstalledKind::new(app, decl, "d1", KindState::Active, "0.1.0", true).expect("kind")
    }

    #[test]
    fn kind_names_are_pascal_case_and_not_legion_nouns_test() {
        assert!(validate_kind_name("Repo").is_ok());
        assert!(validate_kind_name("ScalingPolicy").is_ok());
        for bad in ["repo", "Repo-x", "", "9Lives", "Re po"] {
            assert!(validate_kind_name(bad).is_err(), "{bad}");
        }
        for reserved in ["Job", "Jobs", "Policy", "Policies", "Token", "Node", "Events", "Status"] {
            let err = validate_kind_name(reserved).expect_err(reserved);
            assert!(err.contains("reserved"), "{err}");
        }
        assert_eq!(reserved_noun("JoinToken"), Some("jointoken"));
        assert_eq!(reserved_noun("Capabilities"), Some("capability"));
        assert_eq!(reserved_noun("Repo"), None);
    }

    /// Every top-level CLI command is a reserved noun, so a kind never reads
    /// like a command (calls entry 20).
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
        assert!(validate_kind_decls(&[decl("Repo")]).is_ok());
        let err = validate_kind_decls(&[decl("Repo"), decl("Repos")]).expect_err("plural duplicate");
        assert!(err.contains("already declares Repo"), "{err}");

        let mut bad_example = decl("Repo");
        bad_example.examples = vec![json!({ "branch": "x" })];
        assert!(validate_kind_decls(&[bad_example])
            .expect_err("example")
            .contains("examples[0]"));
        let mut typo = decl("Repo");
        typo.examples = vec![json!({ "url": "x", "brnach": "y" })];
        assert!(validate_kind_decls(&[typo]).expect_err("closed").contains("unknown field"));

        let mut remote = decl("Repo");
        remote.schema = json!({ "type": "object", "properties": { "a": { "$ref": "https://x/s.json" } } });
        assert!(validate_kind_decls(&[remote]).expect_err("remote").contains("not local"));

        // The access mapping is pinned, and must fit the scope.
        let mut scale = decl("Policyish");
        scale.access = Some(KindAccess { resource: "scale".into() });
        assert!(validate_kind_decls(&[scale.clone()]).is_ok());
        scale.scope = KindScope::Cluster;
        assert!(validate_kind_decls(&[scale]).is_err());
        let mut acl = decl("Repo");
        acl.access = Some(KindAccess { resource: "acl".into() });
        assert!(validate_kind_decls(&[acl]).expect_err("pinned").contains("object (the default), job, scale, or node"));
        let mut node = decl("NodeGroup");
        node.scope = KindScope::Cluster;
        node.access = Some(KindAccess { resource: "node".into() });
        node.requires = vec![KindRequirement {
            resource: "operator".into(),
            actions: vec!["write".into()],
            ..Default::default()
        }];
        assert!(validate_kind_decls(&[node.clone()]).is_ok());
        node.requires[0].namespace = Some("/spec/x".into());
        assert!(validate_kind_decls(&[node]).expect_err("namespace on operator").contains("namespaced"));

        let mut kv = decl("Repo");
        kv.requires = vec![KindRequirement { resource: "kv".into(), actions: vec!["read".into()], ..Default::default() }];
        assert!(validate_kind_decls(&[kv]).expect_err("kv path").contains("path"));

        let mut immutable = decl("Repo");
        immutable.immutable = vec!["/url".into()];
        assert!(validate_kind_decls(&[immutable]).is_err());
        let mut cleanup = decl("Repo");
        cleanup.cleanup = Some("2h".into());
        assert!(validate_kind_decls(&[cleanup]).is_err());
        let mut max = decl("Repo");
        max.max_objects = Some(MAX_MAX_OBJECTS + 1);
        assert!(validate_kind_decls(&[max]).is_err());
        let mut rules = decl("Repo");
        rules.rules = Some(json!([{ "rule": "self.a" }]));
        assert!(validate_kind_decls(&[rules]).expect_err("rules").contains("not supported yet"));

        let mut columns = decl("Repo");
        columns.columns = vec![
            KindColumn::Pointer("/spec/url".into()),
            KindColumn::Named { name: "COMMIT".into(), pointer: "/status/fields/commit".into() },
        ];
        assert!(validate_kind_decls(&[columns.clone()]).is_ok());
        assert_eq!(columns.columns[0].header(), "URL");
        columns.columns.push(KindColumn::Pointer("spec/x".into()));
        assert!(validate_kind_decls(&[columns]).is_err());
    }

    #[test]
    fn schemas_are_closed_except_composition_branches_test() {
        let closed = close_schema(&json!({
            "type": "object",
            "properties": { "a": { "type": "object", "properties": { "b": {} } } },
            "allOf": [{ "properties": { "c": {} } }],
            "definitions": { "d": { "type": "object", "properties": { "e": {} } } },
        }));
        assert_eq!(closed["additionalProperties"], false);
        assert_eq!(closed["properties"]["a"]["additionalProperties"], false);
        assert!(closed["allOf"][0].get("additionalProperties").is_none());
        assert_eq!(closed["definitions"]["d"]["additionalProperties"], false);
        let open = close_schema(&json!({ "type": "object", "properties": {}, "additionalProperties": true }));
        assert_eq!(open["additionalProperties"], true);

        let schema = KindSchema::compile(&decl("Repo").schema).expect("compiles");
        let err = schema
            .check("spec", &json!({ "url": "x", "brnach": "dev" }))
            .expect_err("unknown");
        assert_eq!(err, vec!["spec.brnach: unknown field (did you mean branch?)".to_string()]);
        let err = schema
            .check("spec", &json!({ "url": "x", "token": 5 }))
            .expect_err("write-only type");
        assert_eq!(err, vec!["spec.token: invalid value (write-only, not shown)".to_string()]);
        let effective = schema.check("spec", &json!({ "url": "x" })).expect("valid");
        assert_eq!(effective["branch"], "main");
        assert_eq!(
            schema.redacted(&json!({ "url": "x", "token": "s3cr3t" }))["token"],
            REDACTED
        );
    }

    #[test]
    fn kinds_resolve_qualified_or_by_unambiguous_short_name_test() {
        let registry = ObjectKindRegistry::from_kinds(vec![
            kind("gitops", decl("Repo")),
            kind("reposync", decl("Repo")),
            kind("certs", decl("Certificate")),
        ]);
        for typed in ["gitops/Repo", "gitops/repos", "gitops/REPO"] {
            assert_eq!(registry.resolve_qualified(typed).expect(typed).qualified(), "gitops/Repo");
        }
        assert_eq!(
            registry.resolve_qualified("Certificate"),
            Err(KindLookup::Unqualified { candidate: Some("certs/Certificate".into()) })
        );
        assert_eq!(registry.resolve("certificates").expect("short").qualified(), "certs/Certificate");
        let ambiguous = registry.resolve("repo").expect_err("ambiguous");
        assert_eq!(
            ambiguous,
            KindLookup::Ambiguous { candidates: vec!["gitops/Repo".into(), "reposync/Repo".into()] }
        );
        assert!(ambiguous.message("repo").contains("names 2 kinds: gitops/Repo, reposync/Repo"));
        assert_eq!(
            registry.resolve("Certifcate"),
            Err(KindLookup::Unknown { suggestion: Some("certs/Certificate".into()) })
        );
        // An orphaned kind yields to an active one of the same short name.
        let mut orphan = kind("old", decl("Certificate"));
        orphan.state = KindState::Orphaned;
        let registry = ObjectKindRegistry::from_kinds(vec![orphan, kind("certs", decl("Certificate"))]);
        assert_eq!(registry.resolve("certificate").expect("active wins").app, "certs");
    }

    #[test]
    fn objects_parse_and_judge_against_the_kind_now_test() {
        let input = parse_object_input(&json!({
            "kind": "gitops/Repo", "name": "web", "namespace": "team-a",
            "labels": { "team": "a" }, "spec": { "url": "https://x" },
        }))
        .expect("parses");
        assert_eq!(input.namespace.as_deref(), Some("team-a"));
        assert!(parse_object_input(&json!({
            "kind": "gitops/Repo", "name": "web", "spec": {}, "status": { "ok": true },
        }))
        .expect_err("status")
        .contains("written by the app"));
        assert!(parse_object_input(&json!({ "kind": "gitops/Repo", "name": "web", "spek": {} })).is_err());
        assert!(validate_object_name("web-1").is_ok());
        assert!(validate_object_name("Web").is_err());

        let repo = kind("gitops", decl("Repo"));
        let (effective, state, _) = judge(Some(&repo), &json!({ "url": "https://x" }));
        assert_eq!(state, ObjectState::Valid);
        assert_eq!(effective["branch"], "main");
        // A stored default is kept: only missing fields are filled.
        let (effective, _, _) = judge(Some(&repo), &json!({ "url": "https://x", "branch": "trunk" }));
        assert_eq!(effective["branch"], "trunk");
        let (_, state, detail) = judge(Some(&repo), &json!({ "branch": "x" }));
        assert_eq!(state, ObjectState::Invalid);
        assert!(detail.contains("spec"), "{detail}");
        let mut orphan = repo.clone();
        orphan.state = KindState::Orphaned;
        assert_eq!(judge(Some(&orphan), &json!({ "url": "x" })).1, ObjectState::Orphaned);
        assert_eq!(judge(None, &json!({})).1, ObjectState::Orphaned);
    }

    #[test]
    fn requirements_resolve_from_the_object_test() {
        let mut repo = decl("Repo");
        repo.requires = vec![
            KindRequirement { resource: "job".into(), actions: vec!["write".into()], namespace: Some("/spec/targetNamespace".into()), ..Default::default() },
            KindRequirement { resource: "kv".into(), actions: vec!["read".into()], path: Some("/spec/credentials/kv".into()), ..Default::default() },
            KindRequirement { resource: "job".into(), actions: vec!["write".into()], host: true, when: Some("/spec/allowHost".into()), ..Default::default() },
            KindRequirement { resource: "operator".into(), actions: vec!["read".into()], ..Default::default() },
        ];
        let repo = kind("gitops", repo);
        let doc = |spec: Value| object_document("web", "team-a", &BTreeMap::new(), &spec, &Value::Null);
        let resolved = resolve_requirements(&repo, &doc(json!({ "url": "x", "targetNamespace": "team-b" })), "team-a").expect("resolves");
        // The KV entry names no key, and the host entry is off: two checks.
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].namespace.as_deref(), Some("team-b"));
        assert_eq!(resolved[0].name.as_deref(), Some("*"));
        assert_eq!(resolved[0].band.as_deref(), Some("normal"));
        assert_eq!(resolved[0].label(), "job:write in team-b (from spec.targetNamespace)");
        assert_eq!(resolved[1].resource, "operator");
        assert!(resolved[1].scope().namespace.is_none());

        let resolved = resolve_requirements(
            &repo,
            &doc(json!({ "url": "x", "credentials": { "kv": "team-b/key" }, "allowHost": true })),
            "team-a",
        )
        .expect("resolves");
        assert_eq!(resolved.len(), 4);
        // No pointer value: the object's own namespace.
        assert_eq!(resolved[0].namespace.as_deref(), Some("team-a"));
        assert_eq!(resolved[1].path.as_deref(), Some("team-b/key"));
        assert!(resolved[2].host && resolved[2].scope().host);

        // A list names several; a number is refused.
        let listed = resolve_requirements(&repo, &doc(json!({ "url": "x", "targetNamespace": ["a", "b"] })), "team-a").expect("list");
        assert_eq!(listed.len(), 3);
        assert!(resolve_requirements(&repo, &doc(json!({ "url": "x", "targetNamespace": 5 })), "team-a").is_err());
        // A cluster kind's object: every namespace.
        assert_eq!(
            resolve_requirements(&repo, &doc(json!({ "url": "x" })), "").expect("cluster")[0].namespace.as_deref(),
            Some("*")
        );
    }

    #[test]
    fn immutable_fields_and_redacted_diffs_test() {
        let mut repo = decl("Repo");
        repo.immutable = vec!["/spec/url".into(), "/spec/token".into()];
        let repo = kind("gitops", repo);
        let doc = |spec: Value| object_document("web", "team-a", &BTreeMap::new(), &spec, &Value::Null);
        let changes = immutable_changes(
            &repo,
            &doc(json!({ "url": "a", "token": "x" })),
            &doc(json!({ "url": "b", "token": "y" })),
        );
        assert_eq!(changes, vec!["spec.url is immutable (was \"a\")".to_string(), "spec.token is immutable".to_string()]);
        let secret = |path: &[String]| repo.schema.is_write_only(path);
        let lines = spec_diff(
            "spec",
            &json!({ "url": "a", "branch": "main", "token": "x", "nested": { "x": 1 } }),
            &json!({ "url": "a", "branch": "dev", "token": "y", "path": "jobs", "nested": { "x": 2 } }),
            &secret,
        );
        assert_eq!(
            lines,
            vec![
                "spec.branch: \"main\" → \"dev\"".to_string(),
                "spec.nested.x: 1 → 2".to_string(),
                "spec.path: added \"jobs\"".to_string(),
                "spec.token: changed (redacted)".to_string(),
            ]
        );
    }

    #[test]
    fn summaries_follow_the_conditions_test() {
        let status = |conditions: Vec<(&str, &str, &str)>, observed: u64| ObjectStatus {
            observed_generation: observed,
            conditions: conditions
                .into_iter()
                .map(|(kind, status, reason)| ObjectCondition {
                    kind: kind.into(),
                    status: status.into(),
                    reason: reason.into(),
                    ..Default::default()
                })
                .collect(),
            fields: json!({}),
        };
        let summary = |state, deleting, running, status: Option<&ObjectStatus>| {
            summarize(&SummaryInputs { state, deleting, app_running: running, generation: 4, status }).0
        };
        let ready = status(vec![("Ready", "true", "Synced")], 4);
        assert_eq!(summary(ObjectState::Valid, false, true, Some(&ready)), "Ready");
        assert_eq!(summary(ObjectState::Valid, false, true, Some(&status(vec![("Ready", "true", "")], 3))), "Pending");
        assert_eq!(summary(ObjectState::Valid, false, true, None), "Pending");
        assert_eq!(
            summary(ObjectState::Valid, false, true, Some(&status(vec![("Ready", "false", "RepoUnreachable")], 4))),
            "NotReady: RepoUnreachable"
        );
        assert_eq!(
            summary(ObjectState::Valid, false, true, Some(&status(vec![("Accepted", "false", "BadUrl"), ("Ready", "true", "")], 4))),
            "Rejected: BadUrl"
        );
        assert_eq!(summary(ObjectState::Valid, false, true, Some(&status(vec![("Accepted", "true", "")], 4))), "Accepted");
        assert_eq!(summary(ObjectState::Valid, false, false, Some(&ready)), "Stale");
        assert_eq!(summary(ObjectState::Invalid, false, true, Some(&ready)), "Invalid");
        assert_eq!(summary(ObjectState::Valid, true, true, Some(&ready)), "Deleting");
        assert_eq!(summary(ObjectState::Orphaned, true, true, Some(&ready)), "Orphaned");
        assert_eq!(summary_settled("Ready"), Some(true));
        assert_eq!(summary_settled("NotReady: X"), Some(false));
        assert_eq!(summary_settled("Pending"), None);

        let mut conditions = vec![ObjectCondition { kind: "Ready".into(), status: "TRUE".into(), ..Default::default() }];
        validate_conditions(&mut conditions).expect("valid");
        assert_eq!(conditions[0].status, "true");
        stamp_condition_times(&mut conditions, Some(&ready), "now");
        assert_eq!(conditions[0].since, "");
        let mut bad = vec![ObjectCondition { kind: "ready".into(), status: "true".into(), ..Default::default() }];
        assert!(validate_conditions(&mut bad).is_err());
        let after = status(vec![("Ready", "false", "X")], 4);
        assert_eq!(
            condition_transitions(Some(&ready), &after),
            vec![("Ready".to_string(), "true".to_string(), "false".to_string())]
        );
        assert!(condition_transitions(Some(&ready), &ready).is_empty());
    }

    #[test]
    fn scopes_follow_the_access_mapping_test() {
        let object = access_scope(AclResource::Object, "gitops/Repo", "team-a", "web");
        assert_eq!((object.namespace.as_deref(), object.name.as_deref()), (Some("team-a"), Some("web")));
        assert_eq!(access_scope(AclResource::Object, "a/K", "", "x").namespace.as_deref(), Some("*"));
        let scale = access_scope(AclResource::Scale, "autoscaler/ScalingPolicy", "media", "render");
        assert_eq!((scale.namespace.as_deref(), scale.name.as_deref(), scale.band.as_deref()), (Some("media"), Some("*"), None));
        let job = access_scope(AclResource::Job, "gitops/Deploy", "team-a", "web");
        assert_eq!(job.band.as_deref(), Some("normal"));
        let node = access_scope(AclResource::Node, "autoscaler/NodeGroup", "", "workers");
        assert!(node.namespace.is_none() && node.name.is_none() && node.pool.is_none());
    }

    #[test]
    fn upgrades_report_changed_defaults_test() {
        let before = decl("Repo");
        let mut after = decl("Repo");
        after.schema["properties"]["branch"]["default"] = json!("trunk");
        assert_eq!(
            default_changes(&before, &after),
            vec![("/branch".to_string(), json!("main"), json!("trunk"))]
        );
        assert!(default_changes(&before, &before).is_empty());
    }

    #[test]
    fn shards_use_the_autoscalers_hash_test() {
        let key = shard_key("autoscaler", "ScalingPolicy", "media", "render");
        assert_eq!(key, "autoscaler/ScalingPolicy:media/render");
        assert_eq!(shard_key("a", "NodeGroup", "", "workers"), "a/NodeGroup:workers");
        assert_eq!(
            object_shard("autoscaler", "ScalingPolicy", "media", "render", 64),
            crate::autoscaler::shard::shard_of(&key, 64)
        );
        assert_eq!(object_shard("a", "K", "n", "x", 1), 0);
    }

    #[test]
    fn labels_columns_and_filters_test() {
        let mut labels = BTreeMap::new();
        labels.insert("team".to_string(), "a".to_string());
        assert!(validate_labels(&labels).is_ok());
        labels.insert("9bad".to_string(), "x".to_string());
        assert!(validate_labels(&labels).is_err());
        assert_eq!(parse_label_filter(&["team=a,tier=web".into(), "x=y".into()]).expect("filter").len(), 3);
        let document = object_document(
            "web",
            "team-a",
            &BTreeMap::new(),
            &json!({ "url": "https://x" }),
            &json!({ "fields": { "commit": "3f9a" } }),
        );
        assert_eq!(column_cell(&document, "/spec/url"), "https://x");
        assert_eq!(column_cell(&document, "/status/fields/commit"), "3f9a");
        assert_eq!(column_cell(&document, "/status/missing"), "-");
    }
}
