//! Apps (apps plan §6.1-§6.3; docs/apps.md): the manifest, the app record,
//! and the pure functions the app reconciler is built from.
//!
//! An **app** is three things: a job in the reserved `legion-system`
//! namespace, a **grant** (what that job's `legion` identity may do), and
//! registrations (UI, jobspec keys) that later phases read. A **manifest**
//! declares all three; an [`AppRecord`] on the Control lane holds the
//! installed manifest, the operator's settings and config, and the grant.
//!
//! Everything here is pure, like [`crate::structs::acl`] and
//! [`crate::structs::grant`]: parsing and validating a manifest, rendering
//! the app's job from a record ([`render_app_job`]), the digest the
//! reconciler compares instead of the stored job, the permission diff an
//! upgrade shows, the code fields whose change needs the app's whole grant,
//! and the build marker bundled apps roll on ([`decide_build_marker`]). The
//! store lives in `crate::storage::app_store`, the reconciler in
//! `crate::manager::apps`.

use crate::prelude::*;
use crate::structs::acl::AclRule;
use crate::structs::namespace::SYSTEM_NAMESPACE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The principal bundled apps' grants are recorded as granted by: the
/// binary itself, at seed time (calls entry 17). The only grant that skips
/// the escalation check.
pub const BOOTSTRAP_PRINCIPAL: &str = "legion:bootstrap";
/// The principal the app reconciler submits as. Its submits are checked
/// against the grant recorded on the app record, not against itself.
pub const RECONCILER_PRINCIPAL: &str = "legion:app-reconciler";
/// The exec `command` that resolves to the worker's own executable (calls
/// entry 12): bundled apps run the node's own binary.
pub const SELF_COMMAND: &str = "legion:self";
/// Where an app's task finds its config: a JSON file, by a path relative to
/// the task directory that the worker resolves to the task's own view.
pub const APP_CONFIG_ENV: &str = "LEGION_APP_CONFIG";
/// The app's name, in its task's environment.
pub const APP_NAME_ENV: &str = "LEGION_APP_NAME";
/// The build marker a bundled app's job was rendered for
/// (`<version>+<buildId>`), in its task's environment. Part of the render,
/// so a new marker is a new job version and the app rolls.
pub const APP_BUILD_ENV: &str = "LEGION_APP_BUILD";
/// The config file's name, below the task directory.
pub const APP_CONFIG_FILE: &str = "legion-app-config.json";
/// The node attribute a worker reports its build version in.
pub const BUILD_VERSION_ATTR: &str = "buildVersion";
/// The node attribute a worker reports its executable's hash in.
pub const BUILD_ID_ATTR: &str = "buildId";
/// Longest app name: the job is `legion-system/<name>`, and the name lands in
/// paths and event messages.
pub const MAX_APP_NAME_LEN: usize = 40;
/// Most replicas an app may run.
pub const MAX_APP_REPLICAS: u32 = 32;
/// Largest config schema a manifest may carry.
pub const MAX_CONFIG_SCHEMA_BYTES: usize = 64 * 1024;
/// Largest config an app may hold (it is rendered into the job).
pub const MAX_APP_CONFIG_BYTES: usize = 256 * 1024;
/// Largest manifest, as JSON.
pub const MAX_MANIFEST_BYTES: usize = 512 * 1024;

/// Where an app's job may run (apps plan §6.2, "Placement as a setting").
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Placement {
    /// The managers' system workers (or a `--run-worker` manager): permit
    /// pools `legion-system` and `default`, constraint
    /// `legion.role = "manager"`.
    #[default]
    Managers,
    /// Ordinary workers: permit pool `default` (or `settings.pool`),
    /// constraint `legion.role is_not_set`.
    Workers,
    /// Either, preferring managers: both pools, an affinity toward
    /// `legion.role = "manager"`.
    Any,
}

impl Placement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Placement::Managers => "managers",
            Placement::Workers => "workers",
            Placement::Any => "any",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim() {
            "managers" => Ok(Placement::Managers),
            "workers" => Ok(Placement::Workers),
            "any" => Ok(Placement::Any),
            other => Err(format!(
                "unknown placement {other:?} (expected managers, workers, or any)"
            )),
        }
    }

    /// Where the install prompt says the app runs.
    fn prose(&self) -> &'static str {
        match self {
            Placement::Managers => "on your managers",
            Placement::Workers => "on your workers",
            Placement::Any => "on your managers or workers",
        }
    }
}

/// A manifest's `config`: the JSON Schema (draft 7) the app's config must
/// match, and the config an install starts from.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppConfigSpec {
    /// JSON Schema, draft 7, describing an object. Only local `$ref`s.
    #[serde(default = "empty_object_schema")]
    pub schema: Value,
    /// The config a fresh install gets. Must match `schema`.
    #[serde(default = "empty_object")]
    pub default: Value,
}

fn empty_object_schema() -> Value {
    json!({ "type": "object" })
}

fn empty_object() -> Value {
    json!({})
}

fn default_replicas() -> u32 {
    1
}

/// One `codeFields` entry: a JSON pointer into the app's config whose value
/// changes the code the app runs (apps plan §6.3, "Config"). A `*` segment
/// matches every key of an object or element of an array. Changing a value
/// at a code field needs the caller to hold the app's whole grant, as
/// `install` does.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum CodeField {
    /// `"/plugins/*/url"`: a code field the app reads itself.
    Pointer(String),
    /// `{ pointer = "/plugins/*/source", artifact = true }`: a code field
    /// whose string value is also fetched as an artifact of the app's job.
    Spec(CodeFieldSpec),
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodeFieldSpec {
    pub pointer: String,
    /// Fetch the value as an artifact into `code/<path>/` of the task
    /// directory, with a sibling `checksum` string as its checksum.
    #[serde(default)]
    pub artifact: bool,
}

impl CodeField {
    pub fn pointer(&self) -> &str {
        match self {
            CodeField::Pointer(pointer) => pointer,
            CodeField::Spec(spec) => &spec.pointer,
        }
    }

    pub fn is_artifact(&self) -> bool {
        matches!(self, CodeField::Spec(spec) if spec.artifact)
    }
}

/// An app manifest (apps plan §6.1; docs/apps.md, "The manifest"): how the
/// app runs, what it may do, and what it shows. Written in Nickel or JSON;
/// unknown fields are errors.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppManifest {
    /// The app's name, and its job's (`legion-system/<name>`): lowercase
    /// letters, digits and `-`, starting with a letter, at most 40.
    pub name: String,
    /// One line on what the app does.
    #[serde(default)]
    pub description: String,
    /// The app's version. Required for installed apps; a bundled app has
    /// none (its version is the build its job was rendered for).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Reserved for apps owned by a tenant namespace, which Legion does not
    /// support yet: setting it is an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Where the app runs by default: `managers` (default), `workers`, or
    /// `any`. The operator can change it (`legion app set --placement`).
    #[serde(default)]
    pub placement: Placement,
    /// How many copies run by default (each task's `count`). Default 1.
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// A jobspec fragment: everything but `name` and `namespace`, which
    /// Legion sets. Its `task` or `tasks` are the app's.
    #[schemars(with = "serde_json::Map<String, Value>")]
    pub job: Value,
    /// What the app's `legion` identity may do: ACL rules, in the shape
    /// `legion acl policy apply` takes. Granted at install only when the
    /// installer holds every one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(with = "Vec<serde_json::Map<String, Value>>")]
    pub permissions: Vec<AclRule>,
    /// UI registrations (apps plan §6.5). Stored as written; rendered from
    /// phase A4 on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<Value>,
    /// Jobspec extension keys (apps plan §6.6). Stored as written; resolved
    /// from phase A6 on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
    /// The app's config: its JSON Schema and its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<AppConfigSpec>,
    /// JSON pointers into the config whose values change the code the app
    /// runs (plugin sources, endpoints it executes).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub code_fields: Vec<CodeField>,
}

/// An app name: lowercase letters, digits and `-`, starting with a letter,
/// not ending with `-`, at most [`MAX_APP_NAME_LEN`].
pub fn validate_app_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_APP_NAME_LEN {
        return Err(format!(
            "app name {name:?} must be 1 to {MAX_APP_NAME_LEN} characters"
        ));
    }
    let valid = name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && !name.ends_with('-');
    if !valid {
        return Err(format!(
            "app name {name:?} must be lowercase letters, digits and '-', starting with a letter"
        ));
    }
    Ok(())
}

impl AppManifest {
    /// Parse a manifest from its JSON (a Nickel manifest is evaluated to JSON
    /// first). Unknown fields are errors.
    pub fn from_json(text: &str) -> Result<Self, String> {
        if text.len() > MAX_MANIFEST_BYTES {
            return Err(format!(
                "the manifest is {} bytes; the limit is {MAX_MANIFEST_BYTES}",
                text.len()
            ));
        }
        serde_json::from_str(text).map_err(|err| format!("invalid app manifest: {err}"))
    }

    /// The config schema, or one that takes any object.
    pub fn config_schema(&self) -> Value {
        self.config
            .as_ref()
            .map(|spec| spec.schema.clone())
            .unwrap_or_else(empty_object_schema)
    }

    /// The config a fresh install starts from.
    pub fn default_config(&self) -> Value {
        self.config
            .as_ref()
            .map(|spec| spec.default.clone())
            .unwrap_or_else(empty_object)
    }

    /// The manifest's digest: canonical JSON, hashed. Equal manifests digest
    /// equally across builds, so a bundled manifest that did not change
    /// between two releases is not "new".
    pub fn digest(&self) -> String {
        digest_value(&serde_json::to_value(self).unwrap_or(Value::Null))
    }

    /// Check a manifest; `bundled` manifests are the ones compiled into the
    /// binary, which carry no `version`.
    pub fn validate(&self, bundled: bool) -> Result<(), String> {
        validate_app_name(&self.name)?;
        if self.namespace.is_some() {
            return Err(
                "`namespace` is reserved for apps owned by a tenant namespace, which Legion does \
                 not support yet; every app runs in legion-system"
                    .into(),
            );
        }
        match (&self.version, bundled) {
            (Some(_), true) => {
                return Err(format!(
                    "bundled app {}: a bundled manifest has no `version` (its version is the \
                     build it runs)",
                    self.name
                ))
            }
            (None, false) => {
                return Err(format!(
                    "app {}: the manifest needs a `version` (for example \"0.1.0\")",
                    self.name
                ))
            }
            (Some(version), false) => {
                if version.trim().is_empty()
                    || version.len() > 64
                    || version.chars().any(char::is_whitespace)
                {
                    return Err(format!(
                        "app {}: `version` {version:?} must be 1 to 64 characters with no spaces",
                        self.name
                    ));
                }
            }
            (None, true) => {}
        }
        if self.replicas == 0 || self.replicas > MAX_APP_REPLICAS {
            return Err(format!(
                "app {}: `replicas` must be 1 to {MAX_APP_REPLICAS}",
                self.name
            ));
        }
        self.validate_job()?;
        let rules = serde_json::to_string(&self.permissions).map_err(|err| err.to_string())?;
        crate::structs::acl::parse_rules(&rules)
            .map_err(|err| format!("app {}: permissions: {err}", self.name))?;
        if let Some(ui) = &self.ui {
            if !ui.is_object() {
                return Err(format!("app {}: `ui` must be a record", self.name));
            }
        }
        if let Some(extensions) = &self.extensions {
            if !extensions.is_array() && !extensions.is_object() {
                return Err(format!(
                    "app {}: `extensions` must be a list of key declarations",
                    self.name
                ));
            }
        }
        let schema = self.config_schema();
        let compiled = compile_config_schema(&schema)
            .map_err(|err| format!("app {}: config.schema: {err}", self.name))?;
        check_config(&compiled, &self.default_config()).map_err(|err| {
            format!(
                "app {}: config.default does not match config.schema: {err}",
                self.name
            )
        })?;
        for field in &self.code_fields {
            validate_pointer(field.pointer())
                .map_err(|err| format!("app {}: codeFields: {err}", self.name))?;
        }
        Ok(())
    }

    fn validate_job(&self) -> Result<(), String> {
        let Some(job) = self.job.as_object() else {
            return Err(format!("app {}: `job` must be a record", self.name));
        };
        for forced in ["name", "namespace"] {
            if job.contains_key(forced) {
                return Err(format!(
                    "app {}: job.{forced} is set by Legion (the job is {SYSTEM_NAMESPACE}/{}); \
                     leave it out",
                    self.name, self.name
                ));
            }
        }
        if !job.contains_key("task") && !job.contains_key("tasks") {
            return Err(format!(
                "app {}: `job` must define `task` or `tasks`",
                self.name
            ));
        }
        let mut sample = Value::Object(job.clone());
        sample["name"] = json!(self.name);
        sample["namespace"] = json!(SYSTEM_NAMESPACE);
        serde_json::from_value::<crate::structs::JobConfig>(sample)
            .map_err(|err| format!("app {}: job: {err}", self.name))?;
        Ok(())
    }

    /// The UI registrations as the install prompt words them.
    pub fn ui_summary(&self) -> Vec<String> {
        let Some(ui) = self.ui.as_ref().and_then(Value::as_object) else {
            return Vec::new();
        };
        let title = |entry: &Value, key: &str| -> String {
            entry
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or("untitled")
                .to_string()
        };
        let entries = |slot: &str| -> Vec<Value> {
            ui.get(slot)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let mut parts = Vec::new();
        for entry in entries("nav") {
            parts.push(format!("a {} page", title(&entry, "title")));
        }
        for entry in entries("overview") {
            parts.push(format!("a {} card on the overview", title(&entry, "title")));
        }
        for entry in entries("job") {
            parts.push(format!("a {} panel on job pages", title(&entry, "title")));
        }
        for entry in entries("task") {
            parts.push(format!("a {} badge on task rows", title(&entry, "badge")));
        }
        for entry in entries("node") {
            parts.push(format!("a {} panel on node pages", title(&entry, "title")));
        }
        for entry in entries("pages") {
            let rpcs: Vec<String> = entry
                .get("rpcs")
                .and_then(Value::as_array)
                .map(|rpcs| {
                    rpcs.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            if rpcs.is_empty() {
                parts.push(format!("a {} page (sandboxed)", title(&entry, "title")));
            } else {
                parts.push(format!(
                    "a {} page (sandboxed; calls {} as the viewer)",
                    title(&entry, "title"),
                    rpcs.join(", ")
                ));
            }
        }
        let actions = entries("actions");
        if !actions.is_empty() {
            let described: Vec<String> = actions
                .iter()
                .map(|action| {
                    let permission = action
                        .get("permission")
                        .map(|permission| {
                            format!(
                                ", needs {}:{}",
                                permission
                                    .get("resource")
                                    .and_then(Value::as_str)
                                    .unwrap_or("?"),
                                permission
                                    .get("action")
                                    .and_then(Value::as_str)
                                    .unwrap_or("?")
                            )
                        })
                        .unwrap_or_default();
                    format!(
                        "{}{permission}",
                        action
                            .get("title")
                            .or_else(|| action.get("name"))
                            .and_then(Value::as_str)
                            .unwrap_or("untitled")
                    )
                })
                .collect();
            let count = if actions.len() == 1 {
                "one action".to_string()
            } else {
                format!("{} actions", actions.len())
            };
            parts.push(format!("{count} ({})", described.join("; ")));
        }
        parts
    }

    /// The UI slots the manifest registers (`nav`, `job`, ...), for `legion
    /// app list`.
    pub fn ui_slots(&self) -> Vec<String> {
        let Some(ui) = self.ui.as_ref().and_then(Value::as_object) else {
            return Vec::new();
        };
        ["nav", "overview", "job", "task", "node", "pages"]
            .iter()
            .filter(|slot| {
                ui.get(**slot)
                    .and_then(Value::as_array)
                    .is_some_and(|entries| !entries.is_empty())
            })
            .map(|slot| slot.to_string())
            .collect()
    }

    /// Whether the app's tasks run the node's own binary (`legion:self`).
    pub fn runs_self(&self) -> bool {
        let mut job = self.job.clone();
        let mut found = false;
        for_each_task(&mut job, |task| {
            let command = task
                .get("config")
                .and_then(|config| config.get("command"))
                .and_then(Value::as_array)
                .and_then(|command| command.first())
                .and_then(Value::as_str);
            if command == Some(SELF_COMMAND) {
                found = true;
            }
        });
        found
    }

    /// How many artifacts the manifest's job declares.
    pub fn artifact_count(&self) -> usize {
        let mut job = self.job.clone();
        let mut count = 0;
        for_each_task(&mut job, |task| {
            count += task
                .get("artifacts")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
        });
        count
    }
}

/// Where an installed app came from.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AppSource {
    /// Compiled into the binary; seeded by the first Control leader.
    Bundled,
    /// `legion app install ./app.ncl`.
    File,
    /// `legion app install https://…`.
    Url,
}

impl AppSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            AppSource::Bundled => "bundled",
            AppSource::File => "file",
            AppSource::Url => "url",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "bundled" => Ok(AppSource::Bundled),
            "file" => Ok(AppSource::File),
            "url" => Ok(AppSource::Url),
            other => Err(format!(
                "unknown app source {other:?} (expected file or url)"
            )),
        }
    }
}

/// CPU (MHz) and memory (MiB) overrides for every task of the app.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppResources {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<u64>,
}

/// The operator's settings for an app (`legion app set`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AppSettings {
    pub replicas: u32,
    pub placement: Placement,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<AppResources>,
    /// The worker pool for `workers` and `any` placement. Default `default`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
}

impl AppSettings {
    /// The manifest's defaults.
    pub fn from_manifest(manifest: &AppManifest) -> Self {
        AppSettings {
            replicas: manifest.replicas,
            placement: manifest.placement,
            resources: None,
            pool: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.replicas == 0 || self.replicas > MAX_APP_REPLICAS {
            return Err(format!(
                "replicas must be 1 to {MAX_APP_REPLICAS}; to stop an app, disable it"
            ));
        }
        if let Some(resources) = &self.resources {
            if resources.cpu == Some(0) || resources.memory == Some(0) {
                return Err("resources.cpu and resources.memory must be at least 1".into());
            }
        }
        if let Some(pool) = &self.pool {
            if self.placement == Placement::Managers {
                return Err(format!(
                    "pool {pool:?} applies to `workers` and `any` placement; apps placed on \
                     managers run on their system workers"
                ));
            }
            if pool.is_empty()
                || !pool
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
            {
                return Err(format!("pool {pool:?} is not a pool name"));
            }
        }
        Ok(())
    }

    /// `replicas 1 → 3, placement managers → any`, for the reconciler's
    /// event.
    pub fn describe_change(&self, after: &AppSettings) -> String {
        let mut parts = Vec::new();
        if self.replicas != after.replicas {
            parts.push(format!("replicas {} → {}", self.replicas, after.replicas));
        }
        if self.placement != after.placement {
            parts.push(format!(
                "placement {} → {}",
                self.placement.as_str(),
                after.placement.as_str()
            ));
        }
        if self.pool != after.pool {
            parts.push(format!(
                "pool {} → {}",
                self.pool.as_deref().unwrap_or("default"),
                after.pool.as_deref().unwrap_or("default")
            ));
        }
        if self.resources != after.resources {
            parts.push("resources".to_string());
        }
        if parts.is_empty() {
            "settings unchanged".to_string()
        } else {
            format!("settings changed ({})", parts.join(", "))
        }
    }
}

/// What an app's `legion` identity was granted, and by whom.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppGrant {
    pub rules: Vec<AclRule>,
    /// The installer (`userpass:alice`), or [`BOOTSTRAP_PRINCIPAL`].
    pub granted_by: String,
    pub granted_at: String,
}

/// The build a bundled app's job is rendered for: the (`buildVersion`,
/// `buildId`) pair every ready eligible node agreed on (apps plan §6.3,
/// "Upgrades").
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct BuildMarker {
    pub version: String,
    pub build_id: String,
}

impl BuildMarker {
    /// `0.1.0+3f9a12c4e1b0`: the version and the first 12 characters of the
    /// build id.
    pub fn short(&self) -> String {
        let id: String = self.build_id.chars().take(12).collect();
        format!("{}+{id}", self.version)
    }
}

impl std::fmt::Display for BuildMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+{}", self.version, self.build_id)
    }
}

/// The reconciler's bookkeeping on an app record.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppReconcileState {
    /// The digest of the last render the reconciler submitted (or found
    /// already stamped on the job).
    pub last_digest: String,
    /// What the last pass did: `created the job`, `reverted a direct edit by
    /// alice`, `stopped (app disabled)`.
    pub last_action: String,
    /// The last pass's error, empty after a success.
    pub last_error: String,
    /// When the last action or error was recorded (RFC3339).
    pub last_at: String,
    /// The build marker the app's job is rendered for (bundled apps).
    pub marker: Option<BuildMarker>,
    /// Why the marker is held, or what the app waits for: `upgrade waiting:
    /// 2 of 3 eligible nodes on 0.15.0 (waiting for mgr-3)`.
    pub marker_reason: String,
    /// What the last record write changed, for the next reconcile's event
    /// (`config changed`); cleared once the reconciler acts on it.
    pub pending_change: String,
}

/// An installed app (apps plan §6.3, "Records").
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AppRecord {
    pub name: String,
    pub source: AppSource,
    /// The file path or URL the manifest came from; empty for bundled apps.
    pub source_ref: String,
    pub manifest: AppManifest,
    pub manifest_digest: String,
    pub enabled: bool,
    pub settings: AppSettings,
    pub config: Value,
    pub grant: AppGrant,
    pub installed_by: String,
    pub installed_at: String,
    pub updated_at: String,
    pub state: AppReconcileState,
}

impl AppRecord {
    /// The canonical id of the app's job.
    pub fn job_id(&self) -> String {
        app_job_id(&self.name)
    }

    pub fn is_bundled(&self) -> bool {
        self.source == AppSource::Bundled
    }

    /// The version `legion app list` shows: the manifest's, or for a bundled
    /// app the build its job was rendered for.
    pub fn display_version(&self) -> String {
        match (&self.manifest.version, &self.state.marker) {
            (Some(version), _) => version.clone(),
            (None, Some(marker)) => marker.version.clone(),
            (None, None) => "-".to_string(),
        }
    }
}

/// The canonical job id of app `name`: `legion-system/<name>`.
pub fn app_job_id(name: &str) -> String {
    crate::structs::namespace::job_id(SYSTEM_NAMESPACE, name)
}

/// The app a job id belongs to, when it is one of `legion-system`'s.
pub fn app_name_of_job(job_id: &str) -> Option<&str> {
    let (namespace, name) = crate::structs::namespace::split_job_id(job_id);
    (namespace == SYSTEM_NAMESPACE).then_some(name)
}

/// The stamp the reconciler puts on the job versions it submits
/// (`managedBy`, in the version's meta). Kept by KV re-renders, cleared by
/// any other write that stores a version (apps plan §6.3).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedBy {
    pub app: String,
    pub digest: String,
}

/// Compile a config schema (JSON Schema draft 7) with the checks the plan
/// asks of every schema an app ships (§6.6): an object schema, at most
/// [`MAX_CONFIG_SCHEMA_BYTES`], and only local `$ref`s, because the
/// validator resolves remote refs over HTTP by default.
pub fn compile_config_schema(schema: &Value) -> Result<jsonschema::JSONSchema, String> {
    if !schema.is_object() {
        return Err("must be a JSON Schema object".into());
    }
    let size = schema.to_string().len();
    if size > MAX_CONFIG_SCHEMA_BYTES {
        return Err(format!(
            "is {size} bytes; the limit is {MAX_CONFIG_SCHEMA_BYTES}"
        ));
    }
    if let Some(remote) = first_remote_ref(schema) {
        return Err(format!(
            "$ref {remote:?} is not local; only refs into the schema itself (\"#/…\") are allowed"
        ));
    }
    jsonschema::JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft7)
        .compile(schema)
        .map_err(|err| format!("is not a valid JSON Schema: {err}"))
}

fn first_remote_ref(value: &Value) -> Option<String> {
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

/// Check a config against a compiled schema; every error, with its path.
pub fn check_config(schema: &jsonschema::JSONSchema, config: &Value) -> Result<(), String> {
    if let Err(errors) = schema.validate(config) {
        let details: Vec<String> = errors
            .map(|err| {
                let at = err.instance_path.to_string();
                if at.is_empty() {
                    err.to_string()
                } else {
                    format!("{at}: {err}")
                }
            })
            .collect();
        return Err(details.join("; "));
    }
    Ok(())
}

/// Validate `config` against `manifest`'s schema, and its size.
pub fn validate_app_config(manifest: &AppManifest, config: &Value) -> Result<(), String> {
    let size = config.to_string().len();
    if size > MAX_APP_CONFIG_BYTES {
        return Err(format!(
            "the config is {size} bytes; the limit is {MAX_APP_CONFIG_BYTES}"
        ));
    }
    let schema = compile_config_schema(&manifest.config_schema())
        .map_err(|err| format!("the app's config schema {err}"))?;
    check_config(&schema, config)
}

fn validate_pointer(pointer: &str) -> Result<(), String> {
    if !pointer.starts_with('/') || pointer.len() < 2 {
        return Err(format!(
            "{pointer:?} is not a JSON pointer (it starts with '/', like \"/plugins/*/source\")"
        ));
    }
    if pointer.split('/').skip(1).any(str::is_empty) {
        return Err(format!("{pointer:?} has an empty segment"));
    }
    Ok(())
}

fn decode_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn encode_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

/// Every value at `pointer` in `config`, with its concrete pointer. A `*`
/// segment matches every key of an object and every element of an array.
pub fn values_at(config: &Value, pointer: &str) -> Vec<(String, Value)> {
    let segments: Vec<String> = pointer.split('/').skip(1).map(decode_segment).collect();
    let mut found = Vec::new();
    collect_at(config, &segments, String::new(), &mut found);
    found
}

fn collect_at(value: &Value, segments: &[String], at: String, found: &mut Vec<(String, Value)>) {
    let Some((head, rest)) = segments.split_first() else {
        found.push((at, value.clone()));
        return;
    };
    match value {
        Value::Object(map) if head == "*" => {
            for (key, child) in map {
                collect_at(child, rest, format!("{at}/{}", encode_segment(key)), found);
            }
        }
        Value::Object(map) => {
            if let Some(child) = map.get(head) {
                collect_at(child, rest, format!("{at}/{}", encode_segment(head)), found);
            }
        }
        Value::Array(items) if head == "*" => {
            for (index, child) in items.iter().enumerate() {
                collect_at(child, rest, format!("{at}/{index}"), found);
            }
        }
        Value::Array(items) => {
            if let Some(child) = head.parse::<usize>().ok().and_then(|i| items.get(i)) {
                collect_at(child, rest, format!("{at}/{head}"), found);
            }
        }
        _ => {}
    }
}

/// The code fields whose values differ between two configs, as concrete
/// pointers: a value added, removed, or changed at any of the manifest's
/// `codeFields` (apps plan §6.3, "Config"; calls entry 5).
pub fn changed_code_fields(manifest: &AppManifest, before: &Value, after: &Value) -> Vec<String> {
    let mut changed = BTreeSet::new();
    for field in &manifest.code_fields {
        let old: BTreeMap<String, Value> = values_at(before, field.pointer()).into_iter().collect();
        let new: BTreeMap<String, Value> = values_at(after, field.pointer()).into_iter().collect();
        for (pointer, value) in &new {
            if old.get(pointer) != Some(value) {
                changed.insert(pointer.clone());
            }
        }
        for pointer in old.keys() {
            if !new.contains_key(pointer) {
                changed.insert(pointer.clone());
            }
        }
    }
    changed.into_iter().collect()
}

/// The artifacts the config's artifact code fields render into the app's
/// job: each string value, fetched into `code/<pointer path>/`, with a
/// sibling `checksum` string as its checksum.
pub fn code_field_artifacts(manifest: &AppManifest, config: &Value) -> Vec<Value> {
    let mut artifacts = Vec::new();
    for field in manifest
        .code_fields
        .iter()
        .filter(|field| field.is_artifact())
    {
        for (pointer, value) in values_at(config, field.pointer()) {
            let Some(source) = value.as_str().filter(|source| !source.trim().is_empty()) else {
                continue;
            };
            let segments: Vec<String> = pointer.split('/').skip(1).map(decode_segment).collect();
            let parent = &segments[..segments.len().saturating_sub(1)];
            let dest: Vec<String> = parent
                .iter()
                .map(|segment| {
                    segment
                        .chars()
                        .map(|c| {
                            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                                c
                            } else {
                                '_'
                            }
                        })
                        .collect()
                })
                .collect();
            let mut artifact = json!({
                "source": source,
                "dest": format!("code/{}/", dest.join("/")),
            });
            let parent_pointer: String = parent
                .iter()
                .map(|segment| format!("/{}", encode_segment(segment)))
                .collect();
            if let Some(checksum) = config
                .pointer(&format!("{parent_pointer}/checksum"))
                .and_then(Value::as_str)
            {
                artifact["checksum"] = json!(checksum);
            }
            artifacts.push(artifact);
        }
    }
    artifacts
}

/// JSON with every object's keys sorted, so equal values encode equally
/// whatever order they were built in.
pub fn canonical_json(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let ordered: BTreeMap<&String, Value> = map
                    .iter()
                    .map(|(key, value)| (key, sorted(value)))
                    .collect();
                let mut out = Map::new();
                for (key, value) in ordered {
                    out.insert(key.clone(), value);
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    // serde_json's map may preserve insertion order when another crate turns
    // on `preserve_order`; inserting in sorted order makes either work.
    sorted(value).to_string()
}

/// SHA-256 of the canonical JSON, first 16 bytes as hex.
pub fn digest_value(value: &Value) -> String {
    use sha2::Digest;
    let bytes = sha2::Sha256::digest(canonical_json(value).as_bytes());
    bytes[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Call `f` on every task object of a jobspec (`task`, a `tasks` list, or a
/// `tasks` map).
pub fn for_each_task(job: &mut Value, mut f: impl FnMut(&mut Map<String, Value>)) {
    if let Some(task) = job.get_mut("task").and_then(Value::as_object_mut) {
        f(task);
    }
    match job.get_mut("tasks") {
        Some(Value::Array(tasks)) => {
            for task in tasks.iter_mut().filter_map(Value::as_object_mut) {
                f(task);
            }
        }
        Some(Value::Object(tasks)) => {
            for task in tasks.values_mut().filter_map(Value::as_object_mut) {
                f(task);
            }
        }
        _ => {}
    }
}

/// A placement rule list as an array of rule objects: the object shorthand
/// (`{ zone = "a" }`) becomes `[{ key = "zone", value = "a" }]`.
fn rules_as_array(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(items)) => items.clone(),
        Some(Value::Object(map)) => map
            .iter()
            .map(|(key, value)| json!({ "key": key, "value": value }))
            .collect(),
        _ => Vec::new(),
    }
}

/// The permit and placement rules a placement renders to (apps plan §6.2,
/// the placement table).
fn placement_rules(settings: &AppSettings) -> (Value, Vec<Value>, Vec<Value>) {
    use crate::structs::config::{MANAGER_ROLE_ATTR, MANAGER_ROLE_VALUE, SYSTEM_WORKER_TYPE};
    let worker_pool = settings.pool.as_deref().unwrap_or("default");
    let either = json!([{
        "key": "pool",
        "operator": "matches",
        "value": format!(
            "^({}|{})$",
            regex::escape(SYSTEM_WORKER_TYPE),
            regex::escape(worker_pool)
        ),
    }]);
    match settings.placement {
        Placement::Managers => (
            json!([{
                "key": "pool",
                "operator": "matches",
                "value": format!("^({}|default)$", regex::escape(SYSTEM_WORKER_TYPE)),
            }]),
            vec![json!({ "key": MANAGER_ROLE_ATTR, "value": MANAGER_ROLE_VALUE })],
            Vec::new(),
        ),
        Placement::Workers => (
            json!([{ "key": "pool", "value": worker_pool }]),
            vec![json!({ "key": MANAGER_ROLE_ATTR, "operator": "is_not_set" })],
            Vec::new(),
        ),
        Placement::Any => (
            either,
            Vec::new(),
            vec![json!({
                "key": MANAGER_ROLE_ATTR,
                "value": MANAGER_ROLE_VALUE,
                "multiplier": 10.0,
            })],
        ),
    }
}

/// The constraint that keeps a bundled app off nodes older than its marker.
pub fn build_marker_constraint(marker: &BuildMarker) -> Value {
    json!({
        "key": BUILD_VERSION_ATTR,
        "operator": "version_matches",
        "value": format!(">={}", marker.version),
    })
}

/// What a render depends on besides the record.
#[derive(Debug, Clone, Copy)]
pub struct RenderContext<'a> {
    /// The build marker, for bundled apps (`None` renders no marker
    /// constraint; the reconciler does not submit a bundled app without
    /// one).
    pub marker: Option<&'a BuildMarker>,
    /// `systemWorker.appUser`: the user exec tasks run as when the manifest
    /// names none.
    pub app_user: &'a str,
}

/// An app's job as the reconciler submits it, and its digest.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedApp {
    /// The jobspec, in the JSON shape `RunJob` takes.
    pub jobspec: Value,
    pub digest: String,
}

/// Render an app's job, a pure function of the record and the context
/// (apps plan §6.3, "Render"):
///
/// - the manifest's `job`, with its name and namespace forced
///   (`legion-system/<app>`);
/// - `replicas` becomes every task's `count`;
/// - placement becomes the permit and the constraints or affinity (§6.2);
/// - `settings.resources` overrides each task's CPU and memory;
/// - exec tasks with no `user` run as `systemWorker.appUser`;
/// - the `legion` identity carries the recorded grant;
/// - the config is a file in the task directory, named by
///   `$LEGION_APP_CONFIG` (exec and container tasks);
/// - artifact code fields become artifacts;
/// - a bundled app gets the build-marker constraint, and the marker in its
///   environment, so a new marker is a new job version.
pub fn render_app_job(record: &AppRecord, ctx: RenderContext<'_>) -> Result<RenderedApp, String> {
    let manifest = &record.manifest;
    let mut job = manifest.job.clone();
    let Some(object) = job.as_object_mut() else {
        return Err(format!(
            "app {}: the manifest's job is not a record",
            record.name
        ));
    };
    object.insert("name".into(), json!(record.name));
    object.insert("namespace".into(), json!(SYSTEM_NAMESPACE));

    let (permit, placement_constraints, placement_affinities) = placement_rules(&record.settings);
    let marker = if record.is_bundled() {
        ctx.marker
    } else {
        None
    };
    let config_json = canonical_json(&record.config);
    let artifacts = code_field_artifacts(manifest, &record.config);
    let grant_rules = serde_json::to_value(&record.grant.rules).map_err(|err| err.to_string())?;

    for_each_task(&mut job, |task| {
        task.insert("count".into(), json!(record.settings.replicas));
        task.insert("permit".into(), permit.clone());

        let mut constraints = rules_as_array(task.get("constraints"));
        constraints.extend(placement_constraints.iter().cloned());
        if let Some(marker) = marker {
            constraints.push(build_marker_constraint(marker));
        }
        task.insert("constraints".into(), Value::Array(constraints));
        if !placement_affinities.is_empty() {
            let mut affinities = rules_as_array(task.get("affinities"));
            affinities.extend(placement_affinities.iter().cloned());
            task.insert("affinities".into(), Value::Array(affinities));
        }

        if let Some(resources) = record.settings.resources {
            let entry = task.entry("resources").or_insert_with(|| json!({}));
            if !entry.is_object() {
                *entry = json!({});
            }
            if let Some(cpu) = resources.cpu {
                entry["cpu"] = json!(cpu);
            }
            if let Some(memory) = resources.memory {
                entry["memory"] = json!(memory);
            }
        }

        let runtime = task
            .get("runtime")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let config = task.entry("config").or_insert_with(|| json!({}));
        if !config.is_object() {
            *config = json!({});
        }
        let config = config.as_object_mut().expect("made an object above");
        if runtime == "exec" && !config.contains_key("user") {
            config.insert("user".into(), json!(ctx.app_user));
        }
        let env = config.entry("env").or_insert_with(|| json!({}));
        if !env.is_object() {
            *env = json!({});
        }
        env[APP_NAME_ENV] = json!(record.name);
        if let Some(marker) = marker {
            env[APP_BUILD_ENV] = json!(marker.to_string());
        }
        if runtime == "exec" || runtime == "container" {
            env[APP_CONFIG_ENV] = json!(APP_CONFIG_FILE);
            let files = config.entry("files").or_insert_with(|| json!([]));
            if !files.is_array() {
                *files = json!([]);
            }
            if let Some(files) = files.as_array_mut() {
                files.retain(|file| {
                    file.get("path").and_then(Value::as_str) != Some(APP_CONFIG_FILE)
                });
                files.push(json!({
                    "path": APP_CONFIG_FILE,
                    "mode": "0400",
                    "content": config_json,
                }));
            }
        }

        let identities = task.entry("identities").or_insert_with(|| json!([]));
        if !identities.is_array() {
            *identities = json!([]);
        }
        if let Some(identities) = identities.as_array_mut() {
            let legion = crate::structs::workload_identity::CONTROL_PLANE_IDENTITY_NAME;
            match identities
                .iter_mut()
                .find(|identity| identity.get("name").and_then(Value::as_str) == Some(legion))
            {
                Some(identity) => {
                    identity["permissions"] = grant_rules.clone();
                    if let Some(identity) = identity.as_object_mut() {
                        identity.remove("policies");
                    }
                }
                None => identities.push(json!({
                    "name": legion,
                    "permissions": grant_rules.clone(),
                })),
            }
        }

        if !artifacts.is_empty() {
            let list = task.entry("artifacts").or_insert_with(|| json!([]));
            if !list.is_array() {
                *list = json!([]);
            }
            if let Some(list) = list.as_array_mut() {
                list.extend(artifacts.iter().cloned());
            }
        }
    });

    let digest = digest_value(&job);
    Ok(RenderedApp {
        jobspec: job,
        digest,
    })
}

/// One node's build, as the marker decision sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeBuild {
    pub node_id: String,
    /// `buildVersion` as reported (`unknown` for a worker that predates it).
    pub version: String,
    /// `buildId` as reported (empty for a worker that predates it).
    pub build_id: String,
    /// The node is up and taking work.
    pub ready: bool,
}

impl NodeBuild {
    fn usable(&self) -> Option<BuildMarker> {
        if !self.ready || self.build_id.trim().is_empty() {
            return None;
        }
        semver::Version::parse(self.version.trim()).ok()?;
        Some(BuildMarker {
            version: self.version.trim().to_string(),
            build_id: self.build_id.trim().to_string(),
        })
    }
}

/// The marker decision: what to render for, and what the app waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerDecision {
    pub marker: Option<BuildMarker>,
    /// Why the marker is held, empty when the eligible nodes agree.
    pub reason: String,
}

/// Decide a bundled app's build marker from its eligible nodes (apps plan
/// §6.3, "Upgrades"; calls entry 14):
///
/// - nodes that are not ready, or report `unknown`, an unparsable version, or
///   no build id, are left out;
/// - when every remaining node reports the same (`buildVersion`, `buildId`),
///   that pair is the marker, whatever it was before (a deliberate downgrade
///   of the whole set moves it back too);
/// - while they disagree, the marker keeps its last value, so a late older
///   node never moves it backwards, and the reason names who is behind the
///   newest build;
/// - with no usable node there is no new information: the marker stays
///   (`None` on a fresh cluster).
pub fn decide_build_marker(
    current: Option<&BuildMarker>,
    eligible: &[NodeBuild],
) -> MarkerDecision {
    let usable: Vec<(&NodeBuild, BuildMarker)> = eligible
        .iter()
        .filter_map(|node| node.usable().map(|build| (node, build)))
        .collect();
    if usable.is_empty() {
        return MarkerDecision {
            marker: current.cloned(),
            reason: if current.is_none() {
                "waiting for an eligible node to report its build".to_string()
            } else {
                String::new()
            },
        };
    }
    let mut counts: BTreeMap<BuildMarker, Vec<&str>> = BTreeMap::new();
    for (node, build) in &usable {
        counts
            .entry(build.clone())
            .or_default()
            .push(node.node_id.as_str());
    }
    if counts.len() == 1 {
        let (marker, _) = counts.into_iter().next().expect("one build");
        return MarkerDecision {
            marker: Some(marker),
            reason: String::new(),
        };
    }
    // The newest build is where the upgrade is going; ties between two
    // builds of one version go to the one more nodes run.
    let target = counts
        .iter()
        .max_by(|(a, a_nodes), (b, b_nodes)| {
            let a_version = semver::Version::parse(&a.version).expect("usable");
            let b_version = semver::Version::parse(&b.version).expect("usable");
            a_version
                .cmp(&b_version)
                .then(a_nodes.len().cmp(&b_nodes.len()))
                .then(b.build_id.cmp(&a.build_id))
        })
        .map(|(build, _)| build.clone())
        .expect("two builds");
    let on_target = counts.get(&target).map(Vec::len).unwrap_or(0);
    let mut behind: Vec<&str> = counts
        .iter()
        .filter(|(build, _)| **build != target)
        .flat_map(|(_, nodes)| nodes.iter().copied())
        .collect();
    behind.sort_unstable();
    let shown: Vec<&str> = behind.iter().copied().take(5).collect();
    let more = if behind.len() > shown.len() {
        format!(", and {} more", behind.len() - shown.len())
    } else {
        String::new()
    };
    let lead = if current.is_some() {
        "upgrade waiting"
    } else {
        "waiting for the eligible nodes to agree on a build"
    };
    MarkerDecision {
        marker: current.cloned(),
        reason: format!(
            "{lead}: {on_target} of {} eligible nodes on {} (waiting for {}{more})",
            usable.len(),
            target.version,
            shown.join(", ")
        ),
    }
}

/// How a set of permissions changed between two manifests (`legion app
/// upgrade`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PermissionDiff {
    pub added: Vec<AclRule>,
    pub removed: Vec<AclRule>,
    pub unchanged: Vec<AclRule>,
}

/// Rule-by-rule difference: a rule is the same when its canonical JSON is.
pub fn permission_diff(before: &[AclRule], after: &[AclRule]) -> PermissionDiff {
    let key = |rule: &AclRule| canonical_json(&serde_json::to_value(rule).unwrap_or(Value::Null));
    let before_keys: BTreeSet<String> = before.iter().map(key).collect();
    let after_keys: BTreeSet<String> = after.iter().map(key).collect();
    PermissionDiff {
        added: after
            .iter()
            .filter(|rule| !before_keys.contains(&key(rule)))
            .cloned()
            .collect(),
        removed: before
            .iter()
            .filter(|rule| !after_keys.contains(&key(rule)))
            .cloned()
            .collect(),
        unchanged: after
            .iter()
            .filter(|rule| before_keys.contains(&key(rule)))
            .cloned()
            .collect(),
    }
}

/// One rule as the install prompt prints it: `kv    read, write   paths:
/// legion/apps/gitops/*`.
pub fn describe_rule(rule: &AclRule) -> String {
    let mut selectors = Vec::new();
    for (label, values) in [
        ("namespaces", &rule.namespaces),
        ("names", &rule.names),
        ("pools", &rule.pools),
        ("paths", &rule.paths),
        ("actions", &rule.action_names),
        ("templates", &rule.templates),
        ("bands", &rule.bands),
    ] {
        if let Some(values) = values {
            selectors.push(format!("{label}: {}", values.join(", ")));
        }
    }
    if rule.host == Some(true) {
        selectors.push("host".to_string());
    }
    let effect = if rule.is_deny() { "deny " } else { "" };
    format!(
        "{effect}{:<9} {:<13} {}",
        rule.resources.join(", "),
        rule.actions.join(", "),
        selectors.join("; ")
    )
    .trim_end()
    .to_string()
}

/// What `legion app install` shows before it asks (apps plan §3, scenario
/// C): what runs where, as whom, the grant, and the UI.
pub fn install_prompt(manifest: &AppManifest, app_user: &str) -> Vec<String> {
    let label = match &manifest.version {
        Some(version) => format!("{} {version}", manifest.name),
        None => manifest.name.clone(),
    };
    let code = if manifest.runs_self() {
        "runs the node's own legion binary".to_string()
    } else {
        let count = manifest.artifact_count();
        match count {
            0 => "runs its own code".to_string(),
            1 => "runs its own code (1 artifact)".to_string(),
            n => format!("runs its own code ({n} artifacts)"),
        }
    };
    let mut users = BTreeSet::new();
    let mut job = manifest.job.clone();
    for_each_task(&mut job, |task| {
        let runtime = task.get("runtime").and_then(Value::as_str).unwrap_or("");
        let user = task
            .get("config")
            .and_then(|config| config.get("user"))
            .and_then(Value::as_str);
        match (runtime, user) {
            ("exec", Some(user)) => users.insert(format!("user {user}")),
            ("exec", None) => users.insert(format!("user {app_user}")),
            (runtime, _) => users.insert(format!("a {runtime} task")),
        };
    });
    let users: Vec<String> = users.into_iter().collect();
    let mut lines = vec![format!(
        "{label} {code} {}, as {},",
        manifest.placement.prose(),
        users.join(" and ")
    )];
    if manifest.permissions.is_empty() {
        lines.push(format!(
            "in {SYSTEM_NAMESPACE}, and asks for no permissions."
        ));
    } else {
        lines.push(format!("in {SYSTEM_NAMESPACE}, and asks for:"));
        for rule in &manifest.permissions {
            lines.push(format!("  {}", describe_rule(rule)));
        }
    }
    let ui = manifest.ui_summary();
    if !ui.is_empty() {
        lines.push(format!("UI: {}", ui.join(", ")));
    }
    if !manifest.code_fields.is_empty() {
        lines.push(format!(
            "Changing its code fields ({}) later needs everything above, as installing does.",
            manifest
                .code_fields
                .iter()
                .map(CodeField::pointer)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines
}

#[cfg(test)]
mod app_tests {
    use super::*;

    fn manifest(extra: Value) -> AppManifest {
        let mut base = json!({
            "name": "gitops",
            "version": "0.2.0",
            "job": {
                "task": {
                    "name": "sync",
                    "runtime": "exec",
                    "artifacts": ["https://example.com/gitops/0.2.0/legion-gitops-linux-amd64.tar.gz"],
                    "config": { "command": ["legion-gitops"] },
                    "resources": { "cpu": 100, "memory": 64 },
                },
            },
            "permissions": [
                { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["team-*"] },
                { "resources": ["kv"], "actions": ["read"], "paths": ["legion/apps/gitops/*"] },
            ],
            "config": {
                "schema": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "repos": { "type": "array", "items": { "type": "string" } },
                        "plugins": {
                            "type": "object",
                            "additionalProperties": {
                                "type": "object",
                                "properties": {
                                    "source": { "type": "string" },
                                    "checksum": { "type": "string" },
                                    "url": { "type": "string" },
                                },
                            },
                        },
                    },
                },
                "default": { "repos": [] },
            },
            "codeFields": ["/plugins/*/url", { "pointer": "/plugins/*/source", "artifact": true }],
        });
        if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object()) {
            for (key, value) in extra {
                base.insert(key.clone(), value.clone());
            }
        }
        serde_json::from_value(base).expect("manifest parses")
    }

    fn record(manifest: AppManifest) -> AppRecord {
        AppRecord {
            name: manifest.name.clone(),
            source: AppSource::Url,
            source_ref: "https://example.com/gitops/0.2.0/app.ncl".into(),
            manifest_digest: manifest.digest(),
            enabled: true,
            settings: AppSettings::from_manifest(&manifest),
            config: manifest.default_config(),
            grant: AppGrant {
                rules: manifest.permissions.clone(),
                granted_by: "userpass:alice".into(),
                granted_at: "2026-10-28T00:00:00Z".into(),
            },
            installed_by: "userpass:alice".into(),
            installed_at: "2026-10-28T00:00:00Z".into(),
            updated_at: "2026-10-28T00:00:00Z".into(),
            state: AppReconcileState::default(),
            manifest,
        }
    }

    const NO_MARKER: RenderContext<'static> = RenderContext {
        marker: None,
        app_user: "nobody",
    };

    #[test]
    fn a_complete_manifest_validates_and_unknown_fields_fail() {
        manifest(json!({})).validate(false).expect("valid");
        let err = serde_json::from_value::<AppManifest>(json!({
            "name": "x", "version": "1", "job": { "task": { "runtime": "noop", "config": {} } },
            "replica": 2,
        }))
        .expect_err("unknown field");
        assert!(err.to_string().contains("replica"), "{err}");
    }

    #[test]
    fn manifest_validation_names_the_problem() {
        let cases = [
            (json!({ "name": "Git_Ops" }), "lowercase"),
            (json!({ "namespace": "team-a" }), "reserved"),
            (json!({ "version": null }), "needs a `version`"),
            (json!({ "replicas": 0 }), "replicas"),
            (
                json!({ "job": { "name": "other", "task": { "runtime": "noop", "config": {} } } }),
                "job.name is set by Legion",
            ),
            (
                json!({ "job": { "priority": "high" } }),
                "`task` or `tasks`",
            ),
            (
                json!({ "permissions": [{ "resources": ["jobs"], "actions": ["read"] }] }),
                "unknown resource",
            ),
            (
                json!({ "config": { "schema": { "$ref": "https://evil.example/schema.json" } } }),
                "not local",
            ),
            (
                json!({ "config": {
                    "schema": { "type": "object", "required": ["repos"] },
                    "default": {},
                } }),
                "config.default does not match",
            ),
            (
                json!({ "codeFields": ["plugins/*/url"] }),
                "not a JSON pointer",
            ),
        ];
        for (extra, expected) in cases {
            let err = manifest(extra.clone())
                .validate(false)
                .expect_err(&format!("{extra} should fail"));
            assert!(err.contains(expected), "{extra}: {err}");
        }
        let err = manifest(json!({}))
            .validate(true)
            .expect_err("bundled manifests carry no version");
        assert!(err.contains("bundled"), "{err}");
    }

    #[test]
    fn the_manifest_schema_generates() {
        let schema = schemars::schema_for!(AppManifest);
        let text = serde_json::to_string(&schema).expect("schema serializes");
        assert!(text.contains("codeFields"), "{text}");
    }

    #[test]
    fn render_forces_identity_placement_user_config_and_count() {
        let mut record = record(manifest(json!({})));
        record.settings.replicas = 3;
        record.config = json!({ "repos": ["git@example.com:a.git"] });
        let rendered = render_app_job(&record, NO_MARKER).expect("renders");
        let job = &rendered.jobspec;
        assert_eq!(job["name"], "gitops");
        assert_eq!(job["namespace"], "legion-system");
        let task = &job["task"];
        assert_eq!(task["count"], 3);
        assert_eq!(task["permit"][0]["operator"], "matches");
        assert_eq!(task["constraints"][0]["key"], "legion.role");
        assert_eq!(task["config"]["user"], "nobody");
        assert_eq!(task["config"]["env"][APP_CONFIG_ENV], APP_CONFIG_FILE);
        assert_eq!(task["config"]["files"][0]["path"], APP_CONFIG_FILE);
        let content: Value = serde_json::from_str(
            task["config"]["files"][0]["content"]
                .as_str()
                .expect("content"),
        )
        .expect("config json");
        assert_eq!(content, record.config);
        assert_eq!(task["identities"][0]["name"], "legion");
        assert_eq!(
            task["identities"][0]["permissions"],
            serde_json::to_value(&record.grant.rules).expect("rules")
        );
        // The render is a job RunJob takes.
        serde_json::from_value::<crate::structs::JobConfig>(job.clone()).expect("parses");
    }

    #[test]
    fn render_is_deterministic_and_the_digest_follows_every_input() {
        let record = record(manifest(json!({})));
        let first = render_app_job(&record, NO_MARKER).expect("renders");
        let second = render_app_job(&record, NO_MARKER).expect("renders");
        assert_eq!(first, second);

        let mut changed = record.clone();
        changed.config = json!({ "repos": ["x"] });
        assert_ne!(
            render_app_job(&changed, NO_MARKER).unwrap().digest,
            first.digest
        );

        let mut changed = record.clone();
        changed.settings.placement = Placement::Any;
        assert_ne!(
            render_app_job(&changed, NO_MARKER).unwrap().digest,
            first.digest
        );

        let mut changed = record.clone();
        changed.grant.rules.pop();
        assert_ne!(
            render_app_job(&changed, NO_MARKER).unwrap().digest,
            first.digest
        );

        // Bookkeeping is not an input.
        let mut changed = record.clone();
        changed.state.last_action = "anything".into();
        changed.updated_at = "later".into();
        assert_eq!(
            render_app_job(&changed, NO_MARKER).unwrap().digest,
            first.digest
        );
    }

    #[test]
    fn placement_renders_the_table() {
        let mut record = record(manifest(json!({})));
        record.settings.placement = Placement::Workers;
        record.settings.pool = Some("ops".into());
        let task = render_app_job(&record, NO_MARKER).unwrap().jobspec["task"].clone();
        assert_eq!(task["permit"], json!([{ "key": "pool", "value": "ops" }]));
        assert_eq!(
            task["constraints"],
            json!([{ "key": "legion.role", "operator": "is_not_set" }])
        );

        record.settings.placement = Placement::Any;
        let task = render_app_job(&record, NO_MARKER).unwrap().jobspec["task"].clone();
        assert_eq!(task["permit"][0]["value"], "^(legion\\-system|ops)$");
        assert_eq!(task["affinities"][0]["key"], "legion.role");
        assert_eq!(task["constraints"], json!([]));
    }

    #[test]
    fn bundled_apps_render_the_marker_and_third_party_apps_never_do() {
        let marker = BuildMarker {
            version: "0.15.0".into(),
            build_id: "abc".into(),
        };
        let ctx = RenderContext {
            marker: Some(&marker),
            app_user: "nobody",
        };
        let mut record = record(manifest(json!({})));
        let third_party = render_app_job(&record, ctx).unwrap();
        assert!(!third_party.jobspec.to_string().contains("version_matches"));

        record.source = AppSource::Bundled;
        let bundled = render_app_job(&record, ctx).unwrap();
        let task = &bundled.jobspec["task"];
        assert!(task["constraints"]
            .as_array()
            .unwrap()
            .contains(&build_marker_constraint(&marker)));
        assert_eq!(task["config"]["env"][APP_BUILD_ENV], "0.15.0+abc");

        // A rebuild with the same version is a different job.
        let rebuilt = BuildMarker {
            version: "0.15.0".into(),
            build_id: "def".into(),
        };
        let again = render_app_job(
            &record,
            RenderContext {
                marker: Some(&rebuilt),
                app_user: "nobody",
            },
        )
        .unwrap();
        assert_ne!(again.digest, bundled.digest);
    }

    #[test]
    fn code_fields_find_changes_and_render_artifacts() {
        let manifest = manifest(json!({}));
        let before = json!({ "repos": [], "plugins": { "a": { "url": "http://a" } } });
        let after = json!({
            "repos": ["new"],
            "plugins": {
                "a": { "url": "http://a" },
                "b": { "source": "https://example.com/b.tar.gz", "checksum": "sha256:00" },
            },
        });
        assert_eq!(
            changed_code_fields(&manifest, &before, &after),
            vec!["/plugins/b/source".to_string()]
        );
        // A non-code change is not one.
        let repos_only = json!({ "repos": ["x"], "plugins": { "a": { "url": "http://a" } } });
        assert!(changed_code_fields(&manifest, &before, &repos_only).is_empty());
        // Removing one is.
        assert_eq!(
            changed_code_fields(&manifest, &before, &json!({ "repos": [] })),
            vec!["/plugins/a/url".to_string()]
        );
        assert_eq!(
            code_field_artifacts(&manifest, &after),
            vec![json!({
                "source": "https://example.com/b.tar.gz",
                "dest": "code/plugins/b/",
                "checksum": "sha256:00",
            })]
        );
    }

    fn node(id: &str, version: &str, build: &str) -> NodeBuild {
        NodeBuild {
            node_id: id.into(),
            version: version.into(),
            build_id: build.into(),
            ready: true,
        }
    }

    fn marker(version: &str, build: &str) -> BuildMarker {
        BuildMarker {
            version: version.into(),
            build_id: build.into(),
        }
    }

    #[test]
    fn the_marker_is_defined_only_when_the_eligible_nodes_agree() {
        let none = decide_build_marker(None, &[]);
        assert_eq!(none.marker, None);
        assert!(none.reason.contains("waiting for an eligible node"));

        let agreed = decide_build_marker(
            None,
            &[node("m1", "0.14.0", "a"), node("m2", "0.14.0", "a")],
        );
        assert_eq!(agreed.marker, Some(marker("0.14.0", "a")));
        assert!(agreed.reason.is_empty());

        let split = decide_build_marker(
            None,
            &[node("m1", "0.14.0", "a"), node("m2", "0.15.0", "b")],
        );
        assert_eq!(split.marker, None);
        assert!(split.reason.contains("agree"), "{}", split.reason);
    }

    #[test]
    fn a_rolling_upgrade_moves_the_marker_once_after_the_last_node() {
        let old = marker("0.14.0", "a");
        let mut nodes = vec![
            node("m1", "0.14.0", "a"),
            node("m2", "0.14.0", "a"),
            node("m3", "0.14.0", "a"),
        ];
        let mut moves = 0;
        let mut current = decide_build_marker(None, &nodes).marker;
        for index in 0..3 {
            nodes[index] = node(&format!("m{}", index + 1), "0.15.0", "b");
            let decision = decide_build_marker(current.as_ref(), &nodes);
            if decision.marker != current {
                moves += 1;
            }
            if index < 2 {
                assert_eq!(decision.marker, Some(old.clone()));
                assert!(
                    decision.reason.starts_with("upgrade waiting"),
                    "{}",
                    decision.reason
                );
            }
            current = decision.marker;
        }
        assert_eq!(moves, 1);
        assert_eq!(current, Some(marker("0.15.0", "b")));
    }

    #[test]
    fn stragglers_hold_the_marker_and_are_named() {
        let current = marker("0.14.0", "a");
        let decision = decide_build_marker(
            Some(&current),
            &[
                node("m1", "0.15.0", "b"),
                node("m2", "0.15.0", "b"),
                node("wkr-7", "0.14.0", "a"),
            ],
        );
        assert_eq!(decision.marker, Some(current));
        assert_eq!(
            decision.reason,
            "upgrade waiting: 2 of 3 eligible nodes on 0.15.0 (waiting for wkr-7)"
        );
    }

    #[test]
    fn a_rebuild_with_a_new_build_id_moves_the_marker() {
        let current = marker("0.15.0", "a");
        let decision = decide_build_marker(Some(&current), &[node("m1", "0.15.0", "b")]);
        assert_eq!(decision.marker, Some(marker("0.15.0", "b")));
    }

    #[test]
    fn an_older_node_joining_later_does_not_move_the_marker_back() {
        let current = marker("0.15.0", "b");
        let decision = decide_build_marker(
            Some(&current),
            &[node("m1", "0.15.0", "b"), node("late", "0.14.0", "a")],
        );
        assert_eq!(decision.marker, Some(current));
    }

    #[test]
    fn unknown_unparsable_and_not_ready_nodes_are_left_out() {
        let mut down = node("down", "0.13.0", "z");
        down.ready = false;
        let decision = decide_build_marker(
            None,
            &[
                node("m1", "0.15.0", "b"),
                node("old", "unknown", ""),
                node("weird", "not-a-version", "c"),
                node("no-id", "0.12.0", ""),
                down,
            ],
        );
        assert_eq!(decision.marker, Some(marker("0.15.0", "b")));
    }

    #[test]
    fn permission_diff_and_prompt() {
        let before = manifest(json!({}));
        let after = manifest(json!({
            "permissions": [
                { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["team-*"] },
                { "resources": ["template"], "actions": ["read"] },
            ],
        }));
        let diff = permission_diff(&before.permissions, &after.permissions);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].resources, vec!["template".to_string()]);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.unchanged.len(), 1);

        let with_ui = manifest(json!({
            "ui": {
                "nav": [{ "title": "GitOps", "view": "repos" }],
                "job": [{ "title": "Sync", "view": "job-sync" }],
                "actions": [{ "name": "sync-now", "title": "Sync now",
                              "permission": { "resource": "job", "action": "write" } }],
            },
        }));
        let prompt = install_prompt(&with_ui, "nobody").join("\n");
        assert!(
            prompt.starts_with(
                "gitops 0.2.0 runs its own code (1 artifact) on your managers, as user nobody,"
            ),
            "{prompt}"
        );
        assert!(prompt.contains("namespaces: team-*"), "{prompt}");
        assert!(prompt.contains("paths: legion/apps/gitops/*"), "{prompt}");
        assert!(
            prompt.contains(
                "UI: a GitOps page, a Sync panel on job pages, one action (Sync now, needs job:write)"
            ),
            "{prompt}"
        );
        assert_eq!(
            with_ui.ui_slots(),
            vec!["nav".to_string(), "job".to_string()]
        );
    }

    #[test]
    fn canonical_json_sorts_keys() {
        let a: Value = serde_json::from_str(r#"{"b":1,"a":{"d":2,"c":3}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"a":{"c":3,"d":2},"b":1}"#).unwrap();
        assert_eq!(canonical_json(&a), canonical_json(&b));
        assert_eq!(canonical_json(&a), r#"{"a":{"c":3,"d":2},"b":1}"#);
        assert_eq!(digest_value(&a).len(), 32);
    }
}
