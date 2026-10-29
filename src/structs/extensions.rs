//! Jobspec extensions (apps plan §6.6; docs/apps.md, "Jobspec extensions"):
//! the keys installed apps add to the jobspec, and the values jobs set for
//! them.
//!
//! An app's manifest declares at most one **key** per **extension point**
//! (the job, a group written on one of its tasks, a task, a volume entry, a
//! `network.ports` entry, a task's `service` block), each with a JSON Schema
//! (draft 7, local `$ref`s only). Job authors write the key where it
//! belongs, or its qualified spelling `apps.<app>` at the same point, which
//! never collides with anything. At submit, the jobspec walk
//! ([`crate::structs::spec_fields::FieldWalk`]) finds each value's owner in
//! the [`ExtensionRegistry`] built from the installed apps, checks it
//! against the schema with the schema's defaults filled in, and the job
//! version stores it **as written**, by app, in one job-level list
//! ([`ExtensionValue`], `JobConfig.extensions`).
//!
//! Everything here is pure: the manifest's declarations and their install
//! checks, the registry, default filling, and the read-side view of a job's
//! values ([`job_extension_entries`]), whose state (`valid`, `invalid`,
//! `pending`, `orphaned`) and effective value are computed from the apps
//! installed *now*, so an app upgrade that changes a default reaches every
//! value that did not set it, and an uninstalled app's values read as
//! orphaned without anything being rewritten.

use crate::structs::app::{digest_value, AppManifest, AppRecord};
use crate::structs::app_ui::UiPermission;
use crate::structs::job::JobConfig;
use crate::structs::spec_fields::{ExtensionPoint, PENDING_EXTENSIONS_KEY};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

/// The qualified spelling's key at every extension point: `apps = { <app> =
/// { … } }`.
pub const APPS_KEY: &str = "apps";
/// Where a stored job version keeps its resolved extension values (the
/// top level of the stored config, and `job_config.meta`).
pub const EXTENSIONS_KEY: &str = "extensions";
/// Longest extension key.
pub const MAX_KEY_LEN: usize = 64;
/// Largest extension schema, as JSON.
pub const MAX_EXTENSION_SCHEMA_BYTES: usize = 64 * 1024;
/// The path an app serves its validate call on (apps plan §6.6, "At
/// submit").
pub const VALIDATE_PATH: &str = "/legion/v1/extensions/validate";
/// How long a submit waits for an app's validate call before accepting the
/// value with a warning.
pub const VALIDATE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Shards a job's values hash into for `WatchAppExtensions`, unless the
/// request names another count. The same count the autoscaler shards its
/// policies into (autoscaler-v2 plan §6.9).
pub const DEFAULT_SHARD_COUNT: u32 = 64;

/// What setting a key needs beyond `job:write` (`requires` in the
/// manifest), checked as the submitter on the job when the value is new or
/// changed.
pub type ExtensionRequires = UiPermission;

/// One key an app's manifest declares (`extensions[]`).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExtensionDecl {
    /// Where job authors write the key: `job`, `group`, `task`, `volume`,
    /// `port`, or `service`.
    #[schemars(with = "String")]
    pub point: ExtensionPoint,
    /// What job authors write. Defaults to the app's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// One line on what the key does (`legion app extensions`, the jobspec
    /// schema).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// JSON Schema (draft 7) for the value: an object schema, at most 64
    /// KiB, with only local `$ref`s. `default`s are filled when the value
    /// is read.
    #[schemars(with = "serde_json::Map<String, Value>")]
    pub schema: Value,
    /// Example values, each checked against the schema at install.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<Value>,
    /// Also ask the app at submit (`POST /legion/v1/extensions/validate`),
    /// for checks a schema cannot express. Fails open.
    #[serde(default)]
    pub validate: bool,
    /// A permission whoever sets the key must hold, beyond `job:write`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires: Option<ExtensionRequires>,
}

impl ExtensionDecl {
    /// The key as declared (the app's name when the manifest names none).
    pub fn declared_key<'a>(&'a self, app: &'a str) -> &'a str {
        self.key.as_deref().unwrap_or(app)
    }
}

/// Where an extension value sits in the job: the group and task for a
/// task-level point and every point below it, then the volume, port, or
/// service. Empty for the job point.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase", default)]
pub struct ExtensionPath {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub task: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub volume: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub port: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub service: String,
}

impl ExtensionPath {
    /// `postgres/data`: the task (or group) and the named block below it,
    /// as install and upgrade reports print a value's place.
    pub fn short(&self, point: ExtensionPoint) -> String {
        let mut parts = Vec::new();
        match point {
            ExtensionPoint::Job => {}
            ExtensionPoint::Group => parts.push(format!("group {}", self.group)),
            _ => parts.push(self.task.clone()),
        }
        for part in [&self.volume, &self.port, &self.service] {
            if !part.is_empty() {
                parts.push(part.clone());
            }
        }
        parts.join("/")
    }

    /// `task postgres: volume data: ` (or nothing for the job point), the
    /// way submit errors name a place.
    pub fn scope(&self, point: ExtensionPoint) -> String {
        let mut parts = Vec::new();
        if point == ExtensionPoint::Group {
            parts.push(format!("group {}", self.group));
        } else if !self.task.is_empty() {
            parts.push(format!("task {}", self.task));
        }
        if !self.volume.is_empty() {
            parts.push(format!("volume {}", self.volume));
        }
        if !self.port.is_empty() {
            parts.push(format!("port {}", self.port));
        }
        if !self.service.is_empty() {
            parts.push(format!("service {}", self.service));
        }
        parts.into_iter().map(|part| format!("{part}: ")).collect()
    }
}

/// One extension value a job version stores (`extensions[]` in the stored
/// config and `job_config.meta`): resolved at submit to the app that owns
/// it, and kept **as written**. Defaults are filled, and validity decided,
/// when it is read.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionValue {
    /// The app that owns the value. Stored by app, not by key, so a `--key`
    /// rename or a later core field of the same name never orphans it.
    pub app: String,
    /// The point the app declared: `group` for a group key, even though it
    /// is written on a task.
    pub point: ExtensionPoint,
    /// Where it was written.
    #[serde(default, flatten)]
    pub path: ExtensionPath,
    /// The key as the author wrote it: `backup`, or `apps.backup`.
    pub key: String,
    /// The value as written.
    pub value: Value,
    /// `<app>@<manifest digest>`: the manifest whose schema checked it at
    /// submit.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub validated_by: String,
}

/// Two stored values are the same when they say the same thing:
/// `validated_by` records which manifest checked one, so a resubmit after an
/// app upgrade is not a change.
impl PartialEq for ExtensionValue {
    fn eq(&self, other: &Self) -> bool {
        self.app == other.app
            && self.point == other.point
            && self.path == other.path
            && self.key == other.key
            && self.value == other.value
    }
}

impl ExtensionValue {
    /// What identifies a value within a job: its app, point, and path.
    pub fn slot(&self) -> (String, ExtensionPoint, ExtensionPath) {
        (self.app.clone(), self.point, self.path.clone())
    }
}

/// An installed key: one app's declaration, with the operator's `--key`
/// rename applied and its schema compiled.
#[derive(Debug, Clone)]
pub struct InstalledKey {
    pub app: String,
    /// The manifest's version, or `bundled`.
    pub app_version: String,
    /// The app's manifest digest.
    pub digest: String,
    pub enabled: bool,
    pub decl: ExtensionDecl,
    /// The key job authors write (the declared key, or the operator's
    /// rename).
    pub key: String,
    pub schema: Arc<CompiledSchema>,
    /// The key is a core field at its point now (a Legion release added a
    /// field of that name): the core field wins, and the app's values are
    /// written `apps.<app>`.
    pub shadowed: bool,
}

impl InstalledKey {
    /// The point the key is written on: a task for `group` keys.
    pub fn written_on(&self) -> ExtensionPoint {
        written_on(self.decl.point)
    }

    /// `the backup app`, as messages name the owner.
    pub fn owner(&self) -> String {
        format!("the {} app", self.app)
    }

    /// `<app>@<digest>`, the stamp a value it checked carries.
    pub fn validated_by(&self) -> String {
        format!("{}@{}", self.app, self.digest)
    }

    /// Check `value` against the schema with its defaults filled. Returns
    /// the effective value, or one message per problem, each with its path
    /// below the value (`keep: 0 is less than the minimum of 1`).
    pub fn check(&self, value: &Value) -> Result<Value, Vec<String>> {
        self.schema.check(value)
    }
}

impl InstalledKey {
    /// An installed key as `GetJobSchema` reports it (the CLI checks values
    /// against the cluster's keys this way).
    pub fn from_proto(key: &crate::structs::AppExtensionKey) -> Result<InstalledKey, String> {
        let point: ExtensionPoint = serde_json::from_value(json!(key.point))
            .map_err(|_| format!("unknown extension point {:?}", key.point))?;
        let schema: Value = serde_json::from_str(&key.schema_json)
            .map_err(|err| format!("key {}: schema: {err}", key.key))?;
        let examples: Vec<Value> = serde_json::from_str(&key.examples_json).unwrap_or_default();
        let requires = key
            .requires
            .split_once(':')
            .map(|(resource, action)| ExtensionRequires {
                resource: resource.to_string(),
                action: action.to_string(),
            });
        let compiled = compiled_extension_schema(&schema)
            .map_err(|err| format!("key {}: schema {err}", key.key))?;
        Ok(InstalledKey {
            app: key.app.clone(),
            app_version: key.app_version.clone(),
            digest: key.manifest_digest.clone(),
            enabled: key.app_enabled,
            decl: ExtensionDecl {
                point,
                key: Some(key.declared_key.clone()),
                description: key.description.clone(),
                schema,
                examples,
                validate: key.validate,
                requires,
            },
            key: key.key.clone(),
            schema: compiled,
            shadowed: key.shadowed,
        })
    }
}

/// The point a declared point's key is written on.
pub fn written_on(point: ExtensionPoint) -> ExtensionPoint {
    match point {
        ExtensionPoint::Group => ExtensionPoint::Task,
        other => other,
    }
}

/// `volumes`, `ports`, … as messages name the blocks of a point.
pub fn point_plural(point: ExtensionPoint) -> &'static str {
    match point {
        ExtensionPoint::Job => "the job",
        ExtensionPoint::Group => "groups",
        ExtensionPoint::Task => "tasks",
        ExtensionPoint::Volume => "volumes",
        ExtensionPoint::Port => "ports",
        ExtensionPoint::Service => "services",
    }
}

/// A compiled extension (or config) schema, with the raw schema kept for
/// default filling.
pub struct CompiledSchema {
    pub raw: Value,
    compiled: jsonschema::JSONSchema,
}

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledSchema")
            .field("raw", &self.raw)
            .finish_non_exhaustive()
    }
}

impl CompiledSchema {
    /// Fill defaults, then validate; see [`InstalledKey::check`].
    pub fn check(&self, value: &Value) -> Result<Value, Vec<String>> {
        let mut effective = value.clone();
        fill_defaults(&self.raw, &self.raw, &mut effective, 0);
        let problems: Vec<String> = match self.compiled.validate(&effective) {
            Ok(()) => Vec::new(),
            Err(errors) => errors
                .map(|err| {
                    let at = dotted_pointer(&err.instance_path.to_string());
                    if at.is_empty() {
                        err.to_string()
                    } else {
                        format!("{at}: {err}")
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

    /// Whether `value` passes the schema as it stands (no defaults filled).
    pub fn is_valid(&self, value: &Value) -> bool {
        self.compiled.is_valid(value)
    }
}

/// `/items/0/name` → `items[0].name`.
fn dotted_pointer(pointer: &str) -> String {
    let mut out = String::new();
    for segment in pointer.split('/').skip(1) {
        let segment = segment.replace("~1", "/").replace("~0", "~");
        if segment.parse::<usize>().is_ok() {
            out.push_str(&format!("[{segment}]"));
        } else {
            if !out.is_empty() {
                out.push('.');
            }
            out.push_str(&segment);
        }
    }
    out
}

/// A resolver that refuses every `$ref` the schema itself does not hold.
/// `jsonschema` resolves remote refs over HTTP by default; an app's schema
/// is data, and installing one must never make the manager fetch anything.
struct LocalRefsOnly;

impl jsonschema::SchemaResolver for LocalRefsOnly {
    fn resolve(
        &self,
        _root_schema: &Value,
        url: &url::Url,
        original_reference: &str,
    ) -> Result<Arc<Value>, jsonschema::SchemaResolverError> {
        Err(anyhow::anyhow!(
            "$ref {original_reference:?} ({url}) is not local; only refs into the schema itself \
             (\"#/…\") are allowed"
        ))
    }
}

/// The first `$ref` in `value` that does not point into the schema itself.
pub fn first_remote_ref(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get("$ref") {
                if !reference.starts_with('#') {
                    return Some(reference.clone());
                }
            }
            map.values().find_map(first_remote_ref)
        }
        Value::Array(items) => items.iter().find_map(first_remote_ref),
        _ => None,
    }
}

/// Compile a schema an app ships (JSON Schema draft 7): a JSON object of at
/// most `max_bytes`, with only local `$ref`s, compiled with a resolver that
/// refuses any other, so a schema can never make Legion fetch a URL.
pub fn compile_schema(schema: &Value, max_bytes: usize) -> Result<jsonschema::JSONSchema, String> {
    if !schema.is_object() {
        return Err("must be a JSON Schema object".into());
    }
    let size = schema.to_string().len();
    if size > max_bytes {
        return Err(format!("is {size} bytes; the limit is {max_bytes}"));
    }
    if let Some(remote) = first_remote_ref(schema) {
        return Err(format!(
            "$ref {remote:?} is not local; only refs into the schema itself (\"#/…\") are allowed"
        ));
    }
    jsonschema::JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft7)
        .with_resolver(LocalRefsOnly)
        .compile(schema)
        .map_err(|err| format!("is not a valid JSON Schema: {err}"))
}

/// Compiled extension schemas, by the digest of the schema: compiled once
/// per install or upgrade, whichever path reads them.
fn schema_cache() -> &'static Mutex<HashMap<String, Arc<CompiledSchema>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<CompiledSchema>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The most schemas the cache keeps; past it, it starts over.
const SCHEMA_CACHE_LIMIT: usize = 512;

/// An extension schema, compiled (from the cache when this schema was
/// compiled before), after the checks [`validate_decls`] runs at install.
pub fn compiled_extension_schema(schema: &Value) -> Result<Arc<CompiledSchema>, String> {
    let digest = digest_value(schema);
    if let Some(found) = schema_cache()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .get(&digest)
    {
        return Ok(found.clone());
    }
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err("must describe an object (`type = \"object\"`)".into());
    }
    let compiled = Arc::new(CompiledSchema {
        raw: schema.clone(),
        compiled: compile_schema(schema, MAX_EXTENSION_SCHEMA_BYTES)?,
    });
    let mut cache = schema_cache()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if cache.len() >= SCHEMA_CACHE_LIMIT {
        cache.clear();
    }
    cache.insert(digest, compiled.clone());
    Ok(compiled)
}

/// Fill `value`'s missing properties from the schema's `default`s, as
/// Legion reads a value (the validator only validates): every property
/// absent from an object whose schema has a `default` gets it, and present
/// objects and array items are filled the same way. Follows local `$ref`s
/// and `allOf`. Bounded in depth, so a recursive schema cannot loop.
pub fn fill_defaults(root: &Value, schema: &Value, value: &mut Value, depth: usize) {
    if depth > 32 {
        return;
    }
    let Some(schema) = resolve_local_ref(root, schema, 0) else {
        return;
    };
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        for part in all_of {
            fill_defaults(root, part, value, depth + 1);
        }
    }
    match value {
        Value::Object(object) => {
            let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
                return;
            };
            for (name, property) in properties {
                let Some(property) = resolve_local_ref(root, property, 0) else {
                    continue;
                };
                if !object.contains_key(name) {
                    if let Some(default) = property.get("default") {
                        object.insert(name.clone(), default.clone());
                    }
                }
                if let Some(child) = object.get_mut(name) {
                    fill_defaults(root, property, child, depth + 1);
                }
            }
        }
        Value::Array(items) => {
            if let Some(item_schema) = schema.get("items").filter(|items| items.is_object()) {
                for item in items {
                    fill_defaults(root, item_schema, item, depth + 1);
                }
            }
        }
        _ => {}
    }
}

/// Follow a schema's local `$ref` (`#`, `#/definitions/x`, `#/$defs/x`).
fn resolve_local_ref<'a>(root: &'a Value, schema: &'a Value, hops: usize) -> Option<&'a Value> {
    match schema.get("$ref").and_then(Value::as_str) {
        None => Some(schema),
        Some(_) if hops > 16 => None,
        Some(reference) => {
            let pointer = reference.strip_prefix('#')?;
            let target = if pointer.is_empty() {
                root
            } else {
                root.pointer(pointer)?
            };
            resolve_local_ref(root, target, hops + 1)
        }
    }
}

/// The keys an app may not take at any point: the qualified spelling's
/// record, and the maps Legion writes itself.
const RESERVED_KEYS: [&str; 3] = [APPS_KEY, PENDING_EXTENSIONS_KEY, EXTENSIONS_KEY];

/// A key's spelling: a letter, then letters, digits, `_` or `-`, at most
/// [`MAX_KEY_LEN`].
pub fn validate_key_name(key: &str) -> Result<(), String> {
    let valid = !key.is_empty()
        && key.len() <= MAX_KEY_LEN
        && key.starts_with(|c: char| c.is_ascii_alphabetic())
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "key {key:?} must be a letter followed by letters, digits, `_` or `-`, at most \
             {MAX_KEY_LEN} characters"
        ))
    }
}

/// The manifest's `extensions`, parsed.
pub fn manifest_decls(manifest: &AppManifest) -> Result<Vec<ExtensionDecl>, String> {
    match &manifest.extensions {
        None => Ok(Vec::new()),
        Some(Value::Array(_)) => {
            serde_json::from_value(manifest.extensions.clone().unwrap_or(Value::Null))
                .map_err(|err| format!("extensions: {err}"))
        }
        Some(_) => Err("`extensions` must be a list of key declarations".into()),
    }
}

/// Check an app's declarations on their own (apps plan §6.6, "Declaring
/// keys"): at most one key per point (`task` and `group` count as one), a
/// valid key, an object schema of at most 64 KiB with only local `$ref`s
/// that compiles, examples that pass it, and a `requires` that names an ACL
/// permission. `keys` maps a point to the operator's rename (`--key`).
///
/// [`core_collision`] is separate: an install refuses a key that is a core
/// field, but a record installed before a Legion release added the field
/// stays valid (the core field wins, and the app's values use `apps.<app>`).
/// Collisions with other installed apps depend on the cluster and are
/// [`ExtensionRegistry::collision`].
pub fn validate_decls(
    app: &str,
    decls: &[ExtensionDecl],
    keys: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut seen: BTreeMap<ExtensionPoint, ExtensionPoint> = BTreeMap::new();
    for decl in decls {
        let declared = decl.declared_key(app);
        let at = format!("extensions ({} `{declared}`)", decl.point);
        if let Some(previous) = seen.insert(written_on(decl.point), decl.point) {
            return Err(if previous == decl.point {
                format!(
                    "{at}: the app already declares a key on {}; an app declares at most one key \
                     per point (nest several settings in that key's record)",
                    point_plural(decl.point)
                )
            } else {
                format!(
                    "{at}: the app declares both a `task` and a `group` key; both are written on \
                     a task, so an app declares at most one of the two"
                )
            });
        }
        validate_key_name(declared).map_err(|err| format!("{at}: {err}"))?;
        let key = effective_key(app, decl, keys);
        validate_key_name(&key).map_err(|err| format!("{at}: --key: {err}"))?;
        if RESERVED_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "{at}: `{key}` is reserved at every point (it is the qualified spelling's record, \
                 or a map Legion writes)"
            ));
        }
        let compiled =
            compiled_extension_schema(&decl.schema).map_err(|err| format!("{at}: schema {err}"))?;
        for (index, example) in decl.examples.iter().enumerate() {
            compiled.check(example).map_err(|errors| {
                format!(
                    "{at}: examples[{index}] does not match the key's schema: {}",
                    errors.join("; ")
                )
            })?;
        }
        if let Some(requires) = &decl.requires {
            requires
                .parsed()
                .map_err(|err| format!("{at}: requires: {err}"))?;
        }
    }
    for point in keys.keys() {
        let known = decls
            .iter()
            .any(|decl| decl.point.as_str() == point.as_str());
        if !known {
            return Err(format!(
                "--key {point}.…: the app declares no key on {point}"
            ));
        }
    }
    Ok(())
}

/// The first of `decls` whose key (renames applied) is a core field at its
/// point (apps plan §6.6, collision rule 1), worded as the refusal. `except`
/// skips keys an installed version already declares, which an upgrade keeps
/// even after a Legion release made them core fields.
pub fn core_collision(
    app: &str,
    decls: &[ExtensionDecl],
    keys: &BTreeMap<String, String>,
    except: &[(ExtensionPoint, String)],
) -> Option<String> {
    decls.iter().find_map(|decl| {
        let key = effective_key(app, decl, keys);
        let core = crate::structs::job::core_field_names(decl.point).contains(&key.as_str());
        let kept = except
            .iter()
            .any(|(point, existing)| *point == decl.point && *existing == key);
        (core && !kept).then(|| {
            format!(
                "{app} declares `{key}` on {}, which is a core {} field\nHint: install it under \
                 another key: --key {}.{}=<key>",
                point_plural(decl.point),
                field_noun(decl.point),
                decl.point,
                decl.declared_key(app)
            )
        })
    })
}

/// `volume field`, `job field`, as messages name a core field.
fn field_noun(point: ExtensionPoint) -> String {
    match point {
        ExtensionPoint::Group => "task".to_string(),
        other => other.as_str().to_string(),
    }
}

/// The key job authors write for `decl`: the operator's rename for its
/// point, or the declared key.
pub fn effective_key(app: &str, decl: &ExtensionDecl, keys: &BTreeMap<String, String>) -> String {
    keys.get(decl.point.as_str())
        .cloned()
        .unwrap_or_else(|| decl.declared_key(app).to_string())
}

/// Parse `--key volume.backup=pgbackup`: the point, the key the manifest
/// declares there, and the key to install it under.
pub fn parse_key_override(raw: &str) -> Result<(ExtensionPoint, String, String), String> {
    let hint = "expected <point>.<key>=<new key>, like volume.backup=pgbackup";
    let (left, new) = raw
        .split_once('=')
        .ok_or_else(|| format!("--key {raw:?}: {hint}"))?;
    let (point, declared) = left
        .split_once('.')
        .ok_or_else(|| format!("--key {raw:?}: {hint}"))?;
    let point: ExtensionPoint = serde_json::from_value(json!(point.trim())).map_err(|_| {
        format!(
            "--key {raw:?}: {point:?} is not an extension point (job, group, task, volume, port, \
             service)"
        )
    })?;
    validate_key_name(new.trim()).map_err(|err| format!("--key {raw:?}: {err}"))?;
    Ok((point, declared.trim().to_string(), new.trim().to_string()))
}

/// Turn `--key` flags into the record's rename map (point → key), checking
/// each names a key the manifest declares.
pub fn key_overrides(
    app: &str,
    decls: &[ExtensionDecl],
    raw: &[String],
) -> Result<BTreeMap<String, String>, String> {
    let mut keys = BTreeMap::new();
    for entry in raw {
        let (point, declared, new) = parse_key_override(entry)?;
        let decl = decls
            .iter()
            .find(|decl| decl.point == point)
            .ok_or_else(|| format!("--key {entry:?}: {app} declares no key on {point}"))?;
        if decl.declared_key(app) != declared {
            return Err(format!(
                "--key {entry:?}: {app} declares `{}` on {}, not `{declared}`",
                decl.declared_key(app),
                point_plural(point)
            ));
        }
        if new != declared {
            keys.insert(point.as_str().to_string(), new);
        }
    }
    Ok(keys)
}

/// The keys installed apps add, with their schemas compiled: what a submit
/// resolves extension values against.
#[derive(Debug, Clone, Default)]
pub struct ExtensionRegistry {
    keys: Vec<InstalledKey>,
    /// Field names treated as core at a point although the build has no
    /// such field: how tests simulate a Legion release that adds a core
    /// field named like an installed key (apps plan §9 A6).
    simulated_core: BTreeSet<(ExtensionPoint, String)>,
}

impl ExtensionRegistry {
    /// No apps, no keys.
    pub fn empty() -> Self {
        Self::default()
    }

    /// The registry of installed apps' keys. A record whose declarations no
    /// longer parse or compile (written by an older build) contributes
    /// nothing, and is reported in the second value.
    pub fn from_records(records: &[AppRecord]) -> (Self, Vec<String>) {
        let mut keys = Vec::new();
        let mut problems = Vec::new();
        for record in records {
            match installed_keys(record) {
                Ok(mut found) => keys.append(&mut found),
                Err(err) => problems.push(format!("app {}: {err}", record.name)),
            }
        }
        (
            ExtensionRegistry {
                keys,
                simulated_core: BTreeSet::new(),
            },
            problems,
        )
    }

    /// A registry of the given keys (the CLI builds one from GetJobSchema).
    pub fn from_keys(keys: Vec<InstalledKey>) -> Self {
        ExtensionRegistry {
            keys,
            simulated_core: BTreeSet::new(),
        }
    }

    /// Treat `name` as a core field at `point` (tests: a simulated new core
    /// field). Installed keys of that name become shadowed.
    pub fn with_simulated_core_field(mut self, point: ExtensionPoint, name: &str) -> Self {
        self.simulated_core.insert((point, name.to_string()));
        for key in self.keys.iter_mut() {
            if key.written_on() == point && key.key == name {
                key.shadowed = true;
            }
        }
        self
    }

    /// Every installed key.
    pub fn keys(&self) -> &[InstalledKey] {
        &self.keys
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Whether any key asks for the app's validate call.
    pub fn any_validate(&self) -> bool {
        self.keys.iter().any(|key| key.decl.validate)
    }

    /// Whether `name` is a simulated core field at `point`.
    pub fn is_simulated_core(&self, point: ExtensionPoint, name: &str) -> bool {
        self.simulated_core.contains(&(point, name.to_string()))
    }

    /// The key a value written as `key` on a block of `point` belongs to.
    /// Shadowed keys belong to the core field now.
    pub fn owner_of(&self, point: ExtensionPoint, key: &str) -> Option<&InstalledKey> {
        self.keys
            .iter()
            .find(|installed| installed.written_on() == point && installed.key == key)
            .filter(|installed| !installed.shadowed)
    }

    /// `app`'s key on a block of `point` (the qualified spelling).
    pub fn for_app(&self, app: &str, point: ExtensionPoint) -> Option<&InstalledKey> {
        self.keys
            .iter()
            .find(|installed| installed.app == app && installed.written_on() == point)
    }

    /// `app`'s key at the declared `point` (a stored value's owner).
    pub fn for_value(&self, app: &str, point: ExtensionPoint) -> Option<&InstalledKey> {
        self.keys
            .iter()
            .find(|installed| installed.app == app && installed.decl.point == point)
    }

    /// The keys job authors may write on a block of `point`, for
    /// suggestions.
    pub fn keys_at(&self, point: ExtensionPoint) -> Vec<&InstalledKey> {
        self.keys
            .iter()
            .filter(|installed| installed.written_on() == point && !installed.shadowed)
            .collect()
    }

    /// Whether an app is installed (any of its keys is here, or it is
    /// named in `apps`).
    pub fn has_app(&self, app: &str) -> bool {
        self.keys.iter().any(|installed| installed.app == app)
    }

    /// The first collision `app`'s keys (`(point, key)`, renames applied)
    /// would make with another installed app's (apps plan §6.6, rule 2),
    /// worded as the refusal with the `--key` hint. `task` and `group`
    /// count as one point.
    pub fn collision(
        &self,
        app: &str,
        decls: &[ExtensionDecl],
        keys: &BTreeMap<String, String>,
    ) -> Option<String> {
        for decl in decls {
            let key = effective_key(app, decl, keys);
            let Some(owner) = self.keys.iter().find(|installed| {
                installed.app != app
                    && installed.written_on() == written_on(decl.point)
                    && installed.key == key
            }) else {
                continue;
            };
            let owned = if owner.decl.point == decl.point {
                point_plural(decl.point).to_string()
            } else {
                format!(
                    "{} (as a {} key; `task` and `group` keys are both written on a task)",
                    point_plural(written_on(decl.point)),
                    owner.decl.point
                )
            };
            return Some(format!(
                "{app} declares `{key}` on {}, which the {} app already owns on {owned}\nHint: \
                 install it under another key: --key {}.{}={app}",
                point_plural(decl.point),
                owner.app,
                decl.point,
                decl.declared_key(app),
            ));
        }
        None
    }

    /// One line per installed key that a core field now shadows, for the
    /// startup log and `legion app status`.
    pub fn shadowed_notes(&self, app: Option<&str>) -> Vec<String> {
        self.keys
            .iter()
            .filter(|installed| installed.shadowed)
            .filter(|installed| app.is_none_or(|app| installed.app == app))
            .map(|installed| {
                format!(
                    "`{}` on {} is now a core {} field, which wins: job authors write the {} app's \
                     values as `apps.{}`",
                    installed.key,
                    point_plural(installed.decl.point),
                    field_noun(installed.decl.point),
                    installed.app,
                    installed.app
                )
            })
            .collect()
    }

    /// The registry with `record` installed in place of any record of the
    /// same name (what an install or upgrade would make).
    pub fn with_record(&self, record: &AppRecord) -> Result<Self, String> {
        let mut keys: Vec<InstalledKey> = self
            .keys
            .iter()
            .filter(|installed| installed.app != record.name)
            .cloned()
            .collect();
        keys.extend(installed_keys(record)?);
        let mut registry = ExtensionRegistry {
            keys,
            simulated_core: self.simulated_core.clone(),
        };
        let simulated: Vec<(ExtensionPoint, String)> =
            registry.simulated_core.iter().cloned().collect();
        for (point, name) in simulated {
            registry = registry.with_simulated_core_field(point, &name);
        }
        Ok(registry)
    }

    /// The registry without `app` (what an uninstall leaves).
    pub fn without_app(&self, app: &str) -> Self {
        ExtensionRegistry {
            keys: self
                .keys
                .iter()
                .filter(|installed| installed.app != app)
                .cloned()
                .collect(),
            simulated_core: self.simulated_core.clone(),
        }
    }
}

/// An installed record's keys.
fn installed_keys(record: &AppRecord) -> Result<Vec<InstalledKey>, String> {
    let decls = manifest_decls(&record.manifest)?;
    let mut keys = Vec::new();
    for decl in decls {
        let key = effective_key(&record.name, &decl, &record.settings.keys);
        let schema = compiled_extension_schema(&decl.schema)
            .map_err(|err| format!("extension `{key}`: schema {err}"))?;
        let shadowed = crate::structs::job::core_field_names(decl.point).contains(&key.as_str());
        keys.push(InstalledKey {
            app: record.name.clone(),
            app_version: record.display_version(),
            digest: record.manifest_digest.clone(),
            enabled: record.enabled,
            decl,
            key,
            schema,
            shadowed,
        });
    }
    Ok(keys)
}

/// How a value reads now.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionState {
    /// Its app is installed and the value passes the app's current schema.
    Valid,
    /// Its app is installed, and the value fails the current schema (an
    /// upgrade tightened it). The next submit of the job must conform.
    Invalid,
    /// Kept under `--allow-unknown-fields`, and no installed app declares
    /// its key at its point (or the one that does needs a permission the
    /// submitter was never checked for).
    Pending,
    /// Its app was uninstalled, or no longer declares a key at its point.
    Orphaned,
}

impl ExtensionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExtensionState::Valid => "valid",
            ExtensionState::Invalid => "invalid",
            ExtensionState::Pending => "pending",
            ExtensionState::Orphaned => "orphaned",
        }
    }
}

/// One of a job's extension values as it reads now: stored by app, or kept
/// pending, with its effective value and state against the apps installed
/// now.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionEntry {
    /// The owning app: the one that stored it, or the installed app that
    /// claims a pending value. Empty for a pending value no app claims.
    pub app: String,
    pub point: ExtensionPoint,
    pub path: ExtensionPath,
    /// The key as written (`backup`, `apps.backup`).
    pub key: String,
    /// The value as written.
    pub value: Value,
    /// The value with its app's current schema defaults filled.
    pub effective: Value,
    pub state: ExtensionState,
    /// Why it is invalid, orphaned, or pending; `app disabled`; or `adopted
    /// from pending`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub detail: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub validated_by: String,
    /// A pending value an installed app claims (adopted at its install).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub adopted: bool,
}

impl ExtensionEntry {
    /// `orders-db/postgres/data: backup`, as reports print a value.
    pub fn label(&self, job: &str) -> String {
        let place = self.path.short(self.point);
        if place.is_empty() {
            format!("{job}: {}", self.key)
        } else {
            format!("{job}/{place}: {}", self.key)
        }
    }
}

/// Every extension value `config` holds, as it reads against `registry`
/// now: the stored values (valid, invalid, or orphaned), then the pending
/// keys at every point (adopted by an installed app that declares them, or
/// still pending).
pub fn job_extension_entries(
    config: &JobConfig,
    registry: &ExtensionRegistry,
) -> Vec<ExtensionEntry> {
    let mut entries: Vec<ExtensionEntry> = config
        .extensions
        .iter()
        .map(|stored| stored_entry(stored, registry))
        .collect();
    let mut pending = Vec::new();
    collect_pending(
        ExtensionPoint::Job,
        &ExtensionPath::default(),
        &config.pending_extensions,
        &mut pending,
    );
    for task in &config.tasks {
        let at = ExtensionPath {
            group: task.group.clone(),
            task: task.name.clone(),
            ..Default::default()
        };
        collect_pending(
            ExtensionPoint::Task,
            &at,
            &task.pending_extensions,
            &mut pending,
        );
        for volume in &task.volumes {
            let at = ExtensionPath {
                volume: volume.name.clone(),
                ..at.clone()
            };
            collect_pending(
                ExtensionPoint::Volume,
                &at,
                &volume.pending_extensions,
                &mut pending,
            );
        }
        if let Some(network) = &task.network {
            for port in &network.ports {
                let at = ExtensionPath {
                    port: port.name.clone(),
                    ..at.clone()
                };
                collect_pending(
                    ExtensionPoint::Port,
                    &at,
                    &port.pending_extensions,
                    &mut pending,
                );
            }
        }
        if let Some(service) = &task.service {
            let at = ExtensionPath {
                service: service.service_name(&task.name).to_string(),
                ..at.clone()
            };
            collect_pending(
                ExtensionPoint::Service,
                &at,
                &service.pending_extensions,
                &mut pending,
            );
        }
    }
    for (point, path, key, app, value) in pending {
        entries.push(pending_entry(point, path, key, app, value, registry));
    }
    entries
}

/// `(point written on, path, key as written, app named by the qualified
/// spelling, value)` for every pending key.
type PendingItem = (ExtensionPoint, ExtensionPath, String, Option<String>, Value);

fn collect_pending(
    point: ExtensionPoint,
    at: &ExtensionPath,
    pending: &BTreeMap<String, Value>,
    out: &mut Vec<PendingItem>,
) {
    for (key, value) in pending {
        if key == APPS_KEY {
            if let Some(apps) = value.as_object() {
                for (app, value) in apps {
                    out.push((
                        point,
                        at.clone(),
                        format!("{APPS_KEY}.{app}"),
                        Some(app.clone()),
                        value.clone(),
                    ));
                }
                continue;
            }
        }
        out.push((point, at.clone(), key.clone(), None, value.clone()));
    }
}

fn stored_entry(stored: &ExtensionValue, registry: &ExtensionRegistry) -> ExtensionEntry {
    let mut entry = ExtensionEntry {
        app: stored.app.clone(),
        point: stored.point,
        path: stored.path.clone(),
        key: stored.key.clone(),
        value: stored.value.clone(),
        effective: stored.value.clone(),
        state: ExtensionState::Orphaned,
        detail: String::new(),
        validated_by: stored.validated_by.clone(),
        adopted: false,
    };
    match registry.for_value(&stored.app, stored.point) {
        None if registry.has_app(&stored.app) => {
            entry.detail = format!(
                "the {} app no longer declares a key on {}",
                stored.app,
                point_plural(stored.point)
            );
        }
        None => {
            entry.detail = format!("the {} app is not installed", stored.app);
        }
        Some(installed) => judge(&mut entry, installed),
    }
    entry
}

fn pending_entry(
    point: ExtensionPoint,
    path: ExtensionPath,
    key: String,
    qualified: Option<String>,
    value: Value,
    registry: &ExtensionRegistry,
) -> ExtensionEntry {
    let owner = match &qualified {
        Some(app) => registry.for_app(app, point),
        None => registry.owner_of(point, &key),
    };
    let mut entry = ExtensionEntry {
        app: String::new(),
        point,
        path,
        key,
        value: value.clone(),
        effective: value,
        state: ExtensionState::Pending,
        detail: "no installed app declares it".to_string(),
        validated_by: String::new(),
        adopted: false,
    };
    let Some(installed) = owner else {
        return entry;
    };
    entry.point = installed.decl.point;
    if let Some(requires) = &installed.decl.requires {
        // The submitter was never checked for the permission the key needs,
        // so nothing adopts the value until a submit that is.
        entry.app = installed.app.clone();
        entry.detail = format!(
            "{} requires {} to set `{}`; resubmit the job (by someone who holds it) to adopt it",
            installed.owner(),
            requires.label(),
            installed.key
        );
        return entry;
    }
    entry.app = installed.app.clone();
    entry.adopted = true;
    judge(&mut entry, installed);
    if entry.detail.is_empty() {
        entry.detail = "adopted from pending".to_string();
    }
    entry
}

/// Set an owned entry's effective value and state from its app's current
/// schema.
fn judge(entry: &mut ExtensionEntry, installed: &InstalledKey) {
    match installed.check(&entry.value) {
        Ok(effective) => {
            entry.effective = effective;
            entry.state = ExtensionState::Valid;
        }
        Err(errors) => {
            entry.state = ExtensionState::Invalid;
            entry.detail = errors
                .iter()
                .map(|error| prefixed(&entry.key, error))
                .collect::<Vec<_>>()
                .join("; ");
        }
    }
    if !installed.enabled {
        entry.detail = if entry.detail.is_empty() {
            "app disabled".to_string()
        } else {
            format!("{} (app disabled)", entry.detail)
        };
    }
}

/// `backup.keep: …` from `keep: …` (or `backup: …` for a problem with the
/// whole value).
pub fn prefixed(key: &str, error: &str) -> String {
    let (path, message) = match error.split_once(": ") {
        Some((path, message)) if !path.contains(' ') && !path.starts_with('"') => {
            (Some(path), message)
        }
        _ => (None, error),
    };
    match path {
        Some(path) if path.starts_with('[') => format!("{key}{path}: {message}"),
        Some(path) => format!("{key}.{path}: {message}"),
        None => format!("{key}: {message}"),
    }
}

/// The body of an app's validate call (`POST /legion/v1/extensions/
/// validate`): one job's values for the app's keys, with their context.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidateCall {
    /// The job's canonical id (`namespace/name`, or the bare name in
    /// `default`).
    pub job: String,
    pub namespace: String,
    pub values: Vec<ValidateCallValue>,
}

/// One value a validate call asks about.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidateCallValue {
    /// The point the app declared.
    pub point: ExtensionPoint,
    /// Where in the job: the context the app may act on.
    pub path: ExtensionPath,
    /// The key as written.
    pub key: String,
    /// The value as written, and with the schema's defaults filled.
    pub value: Value,
    pub effective: Value,
}

/// What an app answers a validate call: one result per value, in order.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidateAnswer {
    #[serde(default)]
    pub results: Vec<ValidateResult>,
}

/// An app's verdict on one value: errors reject the submit, warnings pass
/// through to the reply.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidateResult {
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// The viewer-assertion context of a validate call for `job_id`: the `job`
/// slot's, so the app can match it against the body it receives.
pub fn validate_context(job_id: &str) -> crate::structs::app_ui::ViewContext {
    let (namespace, name) = crate::structs::namespace::split_job_id(job_id);
    crate::structs::app_ui::ViewContext {
        slot: "job".to_string(),
        namespace: namespace.to_string(),
        job: name.to_string(),
        ..Default::default()
    }
}

/// Which of `count` shards `job_id` hashes into: the first eight bytes of
/// SHA-256 of the canonical job id, modulo the count. Stable across builds
/// and managers, so a sharded app can split its work with one lease scheme
/// (the autoscaler's, autoscaler-v2 plan §6.9).
pub fn shard_of(job_id: &str, count: u32) -> u32 {
    use sha2::Digest;
    if count <= 1 {
        return 0;
    }
    let digest = sha2::Sha256::digest(job_id.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(bytes) % u64::from(count)) as u32
}

/// The extension-point properties an installed key adds to the jobspec
/// schema, for `GetJobSchema` (apps plan §6.6, "Discoverability"): each
/// key as a property at its point, described as `From the backup app,
/// 0.3.1: …`, plus each app under its point's `apps` record. Each key's
/// schema lands in `$defs/ext.<app>.<point>`, with its local `$ref`s
/// rewritten to point there.
pub fn add_keys_to_schema(schema: &mut Value, registry: &ExtensionRegistry) {
    for installed in registry.keys() {
        let def_name = format!("ext.{}.{}", installed.app, installed.decl.point);
        let def_ref = format!("#/$defs/{def_name}");
        let mut definition = installed.decl.schema.clone();
        rebase_refs(&mut definition, &def_ref);
        if let Some(defs) = schema
            .as_object_mut()
            .map(|root| root.entry("$defs").or_insert_with(|| json!({})))
            .and_then(Value::as_object_mut)
        {
            defs.insert(def_name.clone(), definition);
        }
        let mut description = format!("From the {} app, {}", installed.app, installed.app_version);
        if !installed.decl.description.is_empty() {
            description.push_str(": ");
            description.push_str(&installed.decl.description);
        }
        if installed.decl.point == ExtensionPoint::Group {
            description.push_str(" (applies to the whole group; set it on one task of the group)");
        }
        if let Some(requires) = &installed.decl.requires {
            description.push_str(&format!(
                " Setting it requires {} on the job.",
                requires.label()
            ));
        }
        let property = json!({
            "allOf": [{ "$ref": def_ref }],
            "description": description,
            "examples": installed.decl.examples,
        });
        let Some(properties) = point_properties(schema, installed.written_on()) else {
            continue;
        };
        if !installed.shadowed {
            properties.insert(installed.key.clone(), property.clone());
        }
        let apps = properties.entry(APPS_KEY).or_insert_with(|| {
            json!({
                "type": "object",
                "description": "Installed apps' keys in their qualified spelling, `apps.<app>`, \
                    which never collides with a core field.",
                "properties": {},
            })
        });
        if let Some(app_properties) = apps.get_mut("properties").and_then(Value::as_object_mut) {
            app_properties.insert(installed.app.clone(), property);
        }
    }
}

/// The `properties` object of the schema at `point`, created where the
/// core schema does not describe the block.
fn point_properties(schema: &mut Value, point: ExtensionPoint) -> Option<&mut Map<String, Value>> {
    fn object_at<'a>(value: &'a mut Value, key: &str) -> &'a mut Value {
        if !value.is_object() {
            *value = json!({});
        }
        let object = value.as_object_mut().expect("made an object");
        object.entry(key).or_insert_with(|| json!({}))
    }
    fn typed_object(value: &mut Value) {
        if let Some(object) = value.as_object_mut() {
            object.entry("type").or_insert_with(|| json!("object"));
        }
    }
    let target = match point {
        ExtensionPoint::Job => schema,
        _ => {
            let task = object_at(object_at(schema, "$defs"), "task");
            match point {
                ExtensionPoint::Task | ExtensionPoint::Group => task,
                ExtensionPoint::Volume => {
                    let volumes = object_at(object_at(task, "properties"), "volumes");
                    if let Some(object) = volumes.as_object_mut() {
                        object.entry("type").or_insert_with(|| json!("array"));
                    }
                    let items = object_at(volumes, "items");
                    typed_object(items);
                    items
                }
                ExtensionPoint::Port => {
                    let network = object_at(object_at(task, "properties"), "network");
                    typed_object(network);
                    let ports = object_at(object_at(network, "properties"), "ports");
                    if let Some(object) = ports.as_object_mut() {
                        object.entry("type").or_insert_with(|| json!("array"));
                    }
                    let items = object_at(ports, "items");
                    typed_object(items);
                    items
                }
                ExtensionPoint::Service => {
                    let service = object_at(object_at(task, "properties"), "service");
                    typed_object(service);
                    service
                }
                ExtensionPoint::Job => unreachable!("handled above"),
            }
        }
    };
    object_at(target, "properties").as_object_mut()
}

/// Rewrite a schema's local `$ref`s to resolve below `base` once embedded.
fn rebase_refs(value: &mut Value, base: &str) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get_mut("$ref") {
                if let Some(pointer) = reference.strip_prefix('#') {
                    *reference = format!("{base}{pointer}");
                }
            }
            for child in map.values_mut() {
                rebase_refs(child, base);
            }
        }
        Value::Array(items) => {
            for item in items {
                rebase_refs(item, base);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod extensions_tests {
    use super::*;

    fn backup_decl() -> ExtensionDecl {
        serde_json::from_value(json!({
            "point": "volume",
            "key": "backup",
            "description": "Back up this volume on a schedule.",
            "schema": {
                "type": "object",
                "additionalProperties": false,
                "required": ["schedule"],
                "properties": {
                    "schedule": { "type": "string" },
                    "keep": { "type": "integer", "minimum": 1, "default": 7 },
                    "target": { "$ref": "#/definitions/target" },
                },
                "definitions": {
                    "target": {
                        "type": "object",
                        "properties": { "bucket": { "type": "string", "default": "backups" } },
                    },
                },
            },
            "examples": [{ "schedule": "0 3 * * *", "keep": 14 }],
        }))
        .expect("decl parses")
    }

    #[test]
    fn defaults_are_filled_through_refs_and_checked_after_test() {
        let compiled = compiled_extension_schema(&backup_decl().schema).expect("compiles");
        let effective = compiled
            .check(&json!({ "schedule": "@daily", "target": {} }))
            .expect("valid");
        assert_eq!(
            effective,
            json!({ "schedule": "@daily", "keep": 7, "target": { "bucket": "backups" } })
        );
        let errors = compiled
            .check(&json!({ "schedule": "@daily", "keep": 0 }))
            .unwrap_err();
        assert_eq!(errors, vec!["keep: 0 is less than the minimum of 1"]);
        assert_eq!(
            prefixed("backup", &errors[0]),
            "backup.keep: 0 is less than the minimum of 1"
        );
        let errors = compiled.check(&json!({})).unwrap_err();
        assert_eq!(
            prefixed("backup", &errors[0]),
            "backup: \"schedule\" is a required property"
        );
    }

    #[test]
    fn declarations_are_checked_at_install_test() {
        let keys = BTreeMap::new();
        validate_decls("backup", &[backup_decl()], &keys).expect("valid");

        // An example that fails its own schema.
        let mut bad = backup_decl();
        bad.examples = vec![json!({ "keep": 3 })];
        let err = validate_decls("backup", &[bad], &keys).unwrap_err();
        assert!(err.contains("examples[0] does not match"), "{err}");

        // A remote `$ref`, refused before anything is fetched.
        let mut remote = backup_decl();
        remote.schema["properties"]["target"] = json!({ "$ref": "https://example.com/x.json" });
        let err = validate_decls("backup", &[remote], &keys).unwrap_err();
        assert!(err.contains("is not local"), "{err}");

        // A core field at its point: refused at install, kept by an
        // upgrade of an app that already declared it.
        let mut core = backup_decl();
        core.key = Some("readOnly".into());
        validate_decls("backup", &[core.clone()], &keys).expect("valid on its own");
        let err = core_collision("backup", &[core.clone()], &keys, &[]).unwrap();
        assert!(err.contains("is a core volume field"), "{err}");
        assert!(core_collision(
            "backup",
            &[core],
            &keys,
            &[(ExtensionPoint::Volume, "readOnly".into())]
        )
        .is_none());

        // `task` and `group` count as one point.
        let task: ExtensionDecl =
            serde_json::from_value(json!({ "point": "task", "schema": { "type": "object" } }))
                .unwrap();
        let group: ExtensionDecl = serde_json::from_value(
            json!({ "point": "group", "key": "g", "schema": { "type": "object" } }),
        )
        .unwrap();
        let err = validate_decls("backup", &[task, group], &keys).unwrap_err();
        assert!(err.contains("at most one of the two"), "{err}");

        // Not an object schema.
        let mut scalar = backup_decl();
        scalar.schema = json!({ "type": "string" });
        let err = validate_decls("backup", &[scalar], &keys).unwrap_err();
        assert!(err.contains("must describe an object"), "{err}");
    }

    #[test]
    fn key_overrides_parse_and_name_a_declared_key_test() {
        let decls = vec![backup_decl()];
        let keys =
            key_overrides("pgbackup", &decls, &["volume.backup=pgbackup".into()]).expect("valid");
        assert_eq!(keys.get("volume").map(String::as_str), Some("pgbackup"));
        assert!(key_overrides("pgbackup", &decls, &["port.backup=x".into()]).is_err());
        assert!(key_overrides("pgbackup", &decls, &["volume.bkp=x".into()]).is_err());
        assert!(key_overrides("pgbackup", &decls, &["volume=x".into()]).is_err());
    }

    #[test]
    fn shards_are_stable_and_in_range_test() {
        for count in [1, 2, 64] {
            for job in ["web", "prod/api", "legion-system/autoscaler"] {
                let shard = shard_of(job, count);
                assert!(shard < count.max(1));
                assert_eq!(shard, shard_of(job, count));
            }
        }
        assert_eq!(shard_of("anything", 0), 0);
    }

    fn installed(app: &str, decl: Value) -> InstalledKey {
        let decl: ExtensionDecl = serde_json::from_value(decl).expect("decl parses");
        InstalledKey {
            app: app.into(),
            app_version: "1.0.0".into(),
            digest: "d1".into(),
            enabled: true,
            key: decl.declared_key(app).to_string(),
            schema: compiled_extension_schema(&decl.schema).expect("compiles"),
            decl,
            shadowed: false,
        }
    }

    fn registry() -> ExtensionRegistry {
        ExtensionRegistry::from_keys(vec![
            installed("backup", serde_json::to_value(backup_decl()).unwrap()),
            installed(
                "uptime",
                json!({ "point": "job", "schema": { "type": "object",
                    "properties": { "alert": { "type": "string", "default": "#oncall" } } } }),
            ),
            installed(
                "tiers",
                json!({ "point": "group", "key": "tier", "schema": { "type": "object" } }),
            ),
            installed(
                "dns",
                json!({ "point": "port", "schema": { "type": "object" },
                        "requires": { "resource": "net", "action": "write" } }),
            ),
        ])
    }

    fn parse(
        spec: Value,
        mode: crate::structs::spec_fields::UnknownFieldMode,
        registry: &ExtensionRegistry,
    ) -> anyhow::Result<JobConfig> {
        JobConfig::parse_submitted_with(
            spec,
            mode,
            crate::structs::spec_fields::ExtensionCheck::Installed(registry),
        )
        .map(|(config, _)| config)
    }

    fn spec() -> Value {
        json!({
            "name": "orders",
            "uptime": {},
            "tasks": [{
                "name": "pg", "group": "db", "runtime": "exec",
                "tier": {},
                "volumes": [
                    { "name": "data", "backup": { "schedule": "@daily" } },
                    { "name": "logs", "apps": { "backup": { "schedule": "@hourly" } } },
                ],
                "network": { "ports": [{ "name": "http", "dns": {} }] },
            }],
        })
    }

    #[test]
    fn the_walk_resolves_every_point_and_keeps_values_as_written_test() {
        use crate::structs::spec_fields::UnknownFieldMode::Reject;
        let registry = registry();
        let config = parse(spec(), Reject, &registry).expect("parses");
        let found: Vec<(&str, ExtensionPoint, &str, &str)> = config
            .extensions
            .iter()
            .map(|value| {
                (
                    value.app.as_str(),
                    value.point,
                    value.key.as_str(),
                    value.path.volume.as_str(),
                )
            })
            .collect();
        assert_eq!(
            found,
            vec![
                ("backup", ExtensionPoint::Volume, "backup", "data"),
                ("backup", ExtensionPoint::Volume, "apps.backup", "logs"),
                ("dns", ExtensionPoint::Port, "dns", ""),
                ("tiers", ExtensionPoint::Group, "tier", ""),
                ("uptime", ExtensionPoint::Job, "uptime", ""),
            ]
        );
        // As written: the default `keep` is not stored.
        assert_eq!(config.extensions[0].value, json!({ "schedule": "@daily" }));
        assert_eq!(config.extensions[0].validated_by, "backup@d1");
        assert_eq!(config.extensions[3].path.group, "db");
        // A stored config parses back with its values.
        let stored: JobConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).expect("stored");
        assert_eq!(stored.extensions, config.extensions);
    }

    #[test]
    fn the_walk_refuses_bad_values_and_names_installed_keys_test() {
        use crate::structs::spec_fields::UnknownFieldMode::{Allow, Reject};
        let registry = registry();
        let mut bad = spec();
        bad["tasks"][0]["volumes"][0]["backup"]["keep"] = json!(0);
        for mode in [Reject, Allow] {
            let err = parse(bad.clone(), mode, &registry).unwrap_err().to_string();
            assert!(
                err.contains(
                    "orders: task pg: volume data: backup.keep: 0 is less than the minimum of 1"
                ),
                "{err}"
            );
        }
        let mut typo = spec();
        typo["tasks"][0]["volumes"][0] = json!({ "name": "data", "backpu": {} });
        let err = parse(typo.clone(), Reject, &registry)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unknown field `backpu` (did you mean `backup`, the backup app's key?)"),
            "{err}"
        );
        // Under the flag, an unknown key is pending, as before.
        let kept = parse(typo, Allow, &registry).expect("allowed");
        assert!(kept.tasks[0].volumes[0]
            .pending_extensions
            .contains_key("backpu"));
        // Offline, keys at points are unchecked, not errors.
        let (_, record) = JobConfig::parse_submitted_with(
            spec(),
            Reject,
            crate::structs::spec_fields::ExtensionCheck::Offline,
        )
        .expect("offline");
        assert_eq!(record.unchecked.len(), 5, "{record:?}");
    }

    #[test]
    fn entries_read_against_the_apps_installed_now_test() {
        use crate::structs::spec_fields::UnknownFieldMode::Allow;
        let registry = registry();
        let mut config = parse(spec(), Allow, &registry).expect("parses");
        let entries = job_extension_entries(&config, &registry);
        let uptime = entries.iter().find(|entry| entry.app == "uptime").unwrap();
        assert_eq!(uptime.state, ExtensionState::Valid);
        assert_eq!(uptime.effective, json!({ "alert": "#oncall" }));

        // Uninstalled: orphaned, value kept.
        let without = registry.without_app("backup");
        let entries = job_extension_entries(&config, &without);
        let backup = entries.iter().find(|entry| entry.key == "backup").unwrap();
        assert_eq!(backup.state, ExtensionState::Orphaned);
        assert_eq!(backup.detail, "the backup app is not installed");

        // Pending keys: adopted by the app that declares them, unless the
        // key needs a permission no submit checked.
        config.extensions.clear();
        config.tasks[0].volumes[0]
            .pending_extensions
            .insert("backup".into(), json!({ "schedule": "@daily" }));
        config.tasks[0].network.as_mut().unwrap().ports[0]
            .pending_extensions
            .insert("dns".into(), json!({}));
        config
            .pending_extensions
            .insert("nobody".into(), json!(true));
        let entries = job_extension_entries(&config, &registry);
        let adopted = entries.iter().find(|entry| entry.key == "backup").unwrap();
        assert!(adopted.adopted);
        assert_eq!(adopted.state, ExtensionState::Valid);
        assert_eq!(adopted.effective["keep"], 7);
        let held = entries.iter().find(|entry| entry.key == "dns").unwrap();
        assert_eq!(held.state, ExtensionState::Pending);
        assert!(
            held.detail.contains("requires net:write"),
            "{}",
            held.detail
        );
        let unclaimed = entries.iter().find(|entry| entry.key == "nobody").unwrap();
        assert_eq!(unclaimed.state, ExtensionState::Pending);
        assert!(unclaimed.app.is_empty());
    }

    #[test]
    fn collisions_name_the_owner_and_the_rename_test() {
        let registry = registry();
        let task_tier: ExtensionDecl = serde_json::from_value(
            json!({ "point": "task", "key": "tier", "schema": { "type": "object" } }),
        )
        .unwrap();
        let err = registry
            .collision("ranks", std::slice::from_ref(&task_tier), &BTreeMap::new())
            .unwrap();
        assert!(err.contains("which the tiers app already owns"), "{err}");
        assert!(err.contains("--key task.tier=ranks"), "{err}");
        let renamed: BTreeMap<String, String> = [("task".to_string(), "rank".to_string())].into();
        assert!(registry
            .collision("ranks", &[task_tier], &renamed)
            .is_none());
    }

    #[test]
    fn schema_refs_are_rebased_when_embedded_test() {
        let mut schema = backup_decl().schema;
        rebase_refs(&mut schema, "#/$defs/ext.backup.volume");
        assert_eq!(
            schema["properties"]["target"]["$ref"],
            "#/$defs/ext.backup.volume/definitions/target"
        );
    }
}
