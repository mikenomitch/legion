//! Strict jobspec fields: capture, then validate.
//!
//! Every jobspec block captures the keys it does not know in a
//! `#[serde(flatten)]` map ([`UnknownFields`]) instead of ignoring them, and
//! one pass over the parsed spec ([`FieldWalk`]) decides what each captured
//! key means *for this request*:
//!
//! - by default every unknown key is a submit error, named with its path
//!   (`web: task web: volume data: unknown field ...`) and the closest known
//!   key as a suggestion;
//! - with `allow_unknown_fields` (`--allow-unknown-fields`), a key that sits
//!   directly on an **extension point** (the job, a task, a volume entry, a
//!   `network.ports` entry, a task's `service` block) is kept, unvalidated, in
//!   that block's `pendingExtensions` map, and a key anywhere else is dropped.
//!   Both are reported in the reply's warnings and recorded on the job
//!   config version ([`UnknownFieldsRecord`]).
//!
//! Capturing rather than `deny_unknown_fields` is what makes the choice
//! per-request: serde cannot combine `flatten` with `deny_unknown_fields`,
//! and a block that refuses unknown keys at parse can neither drop nor keep
//! them later. Extension points keep their values under an explicit
//! `pendingExtensions` key, not inline, so a later core field of the same
//! name can never reinterpret a stored pending value.
//!
//! The public page is docs/jobs-tasks-and-scheduling.md, "Unknown fields".

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::structs::extensions::{
    point_plural, ExtensionPath, ExtensionRegistry, ExtensionValue, InstalledKey, APPS_KEY,
};

/// Keys a jobspec block did not recognize, captured at parse with
/// `#[serde(flatten)]`. Empty after a submit's [`FieldWalk`]: every key in it
/// was either an error, moved to `pendingExtensions`, or dropped.
pub type UnknownFields = BTreeMap<String, Value>;

/// Unknown keys kept at an extension point under `allow_unknown_fields`,
/// stored as written and unvalidated (`pendingExtensions` in the stored
/// config). Nothing in Legion acts on them; they stay visible in
/// `legion job info` until the spec drops them.
pub type PendingExtensions = BTreeMap<String, Value>;

/// The spelling of the pending map in a jobspec and a stored config.
pub const PENDING_EXTENSIONS_KEY: &str = "pendingExtensions";

/// A place in the jobspec where installed apps declare their own keys
/// (internal_docs/plans/legion-apps-plan.md §6.6; `crate::structs::extensions`),
/// and where keys Legion does not know are kept, not dropped, under
/// `allow_unknown_fields`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionPoint {
    /// The top level of the jobspec.
    Job,
    /// A group: declared by an app, but written on one task of the group
    /// (the jobspec has no group objects). No jobspec block is this point.
    Group,
    /// A task.
    Task,
    /// An entry in a task's `volumes`.
    Volume,
    /// An entry in a task's `network.ports`.
    Port,
    /// A task's `service` block.
    Service,
}

impl ExtensionPoint {
    /// The point's name as the docs and the API spell it.
    pub fn as_str(self) -> &'static str {
        match self {
            ExtensionPoint::Job => "job",
            ExtensionPoint::Group => "group",
            ExtensionPoint::Task => "task",
            ExtensionPoint::Volume => "volume",
            ExtensionPoint::Port => "port",
            ExtensionPoint::Service => "service",
        }
    }
}

impl fmt::Display for ExtensionPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a submit treats keys the jobspec does not know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnknownFieldMode {
    /// Every unknown key is an error. The default.
    #[default]
    Reject,
    /// `allow_unknown_fields`: keep keys at extension points as pending,
    /// drop the rest, and report both.
    Allow,
}

impl UnknownFieldMode {
    /// The mode for a request's `allow_unknown_fields` flag.
    pub fn from_allow(allow_unknown_fields: bool) -> Self {
        if allow_unknown_fields {
            UnknownFieldMode::Allow
        } else {
            UnknownFieldMode::Reject
        }
    }
}

/// One unknown key, as the version record and the API report it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnknownFieldEntry {
    /// Where the key's block sits: `task postgres`, `task postgres: volume
    /// data`. Empty at the job's top level.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
    /// The key, dotted from its scope: `backup`, `stop.signall`,
    /// `checks[0].everyy`.
    pub path: String,
    /// For a kept key, the extension point it sits on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point: Option<ExtensionPoint>,
}

impl UnknownFieldEntry {
    /// `task postgres: volume data: ` (or nothing at the top level).
    fn scope_prefix(&self) -> String {
        if self.scope.is_empty() {
            String::new()
        } else {
            format!("{}: ", self.scope)
        }
    }
}

/// What `allow_unknown_fields` did to one submitted jobspec: the keys it
/// kept as pending and the keys it dropped. Stored on the job config version
/// the submit created (`legion job info` shows it), and returned as warnings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnknownFieldsRecord {
    /// Keys at extension points, kept unvalidated in `pendingExtensions`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<UnknownFieldEntry>,
    /// Keys elsewhere, dropped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped: Vec<UnknownFieldEntry>,
    /// Keys at extension points that an offline check (`legion job
    /// validate` with no manager reachable) could not resolve: they may be
    /// keys installed apps add. Never set by a manager.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unchecked: Vec<UnknownFieldEntry>,
}

impl UnknownFieldsRecord {
    /// True when the flag neither kept nor dropped anything.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.dropped.is_empty() && self.unchecked.is_empty()
    }

    /// One warning line per key, in the words `legion job run` prints after
    /// `warning: `. Dropped keys first: they are the ones whose intent is
    /// lost.
    pub fn warnings(&self) -> Vec<String> {
        let dropped = self.dropped.iter().map(|entry| {
            format!(
                "{}dropped unknown field `{}`",
                entry.scope_prefix(),
                entry.path
            )
        });
        let pending = self.pending.iter().map(|entry| {
            format!(
                "{}kept `{}` as pending (no installed app declares it)",
                entry.scope_prefix(),
                entry.path
            )
        });
        let unchecked = self.unchecked.iter().map(|entry| {
            format!(
                "{}`{}` was not checked: it is not a core field, and no manager was reachable \
                 to say whether an installed app declares it",
                entry.scope_prefix(),
                entry.path
            )
        });
        dropped.chain(pending).chain(unchecked).collect()
    }

    /// The `legion job info` line, after `submitted with
    /// --allow-unknown-fields: `: `dropped task web: resources.foo; backup
    /// on task postgres: volume data pending (no installed app declares
    /// it)`.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.dropped.is_empty() {
            let dropped: Vec<String> = self
                .dropped
                .iter()
                .map(|entry| format!("{}{}", entry.scope_prefix(), entry.path))
                .collect();
            parts.push(format!("dropped {}", dropped.join(", ")));
        }
        if !self.pending.is_empty() {
            let pending: Vec<String> = self
                .pending
                .iter()
                .map(|entry| {
                    if entry.scope.is_empty() {
                        format!("{} on the job", entry.path)
                    } else {
                        format!("{} on {}", entry.path, entry.scope)
                    }
                })
                .collect();
            parts.push(format!(
                "{} pending (no installed app declares {})",
                pending.join(", "),
                if pending.len() == 1 { "it" } else { "them" }
            ));
        }
        parts.join("; ")
    }
}

impl From<UnknownFieldsRecord> for crate::structs::UnknownFieldsRecord {
    fn from(record: UnknownFieldsRecord) -> Self {
        let entry = |entry: UnknownFieldEntry| crate::structs::UnknownJobspecField {
            scope: entry.scope,
            path: entry.path,
            point: entry
                .point
                .map(|point| point.as_str().to_string())
                .unwrap_or_default(),
        };
        crate::structs::UnknownFieldsRecord {
            pending: record.pending.into_iter().map(entry).collect(),
            dropped: record.dropped.into_iter().map(entry).collect(),
        }
    }
}

impl From<&crate::structs::UnknownFieldsRecord> for UnknownFieldsRecord {
    fn from(record: &crate::structs::UnknownFieldsRecord) -> Self {
        let entry = |field: &crate::structs::UnknownJobspecField| UnknownFieldEntry {
            scope: field.scope.clone(),
            path: field.path.clone(),
            point: serde_json::from_value(Value::String(field.point.clone())).ok(),
        };
        UnknownFieldsRecord {
            pending: record.pending.iter().map(entry).collect(),
            dropped: record.dropped.iter().map(entry).collect(),
            unchecked: Vec::new(),
        }
    }
}

/// A jobspec carried keys Legion does not know and the request did not
/// allow them, or values for installed apps' keys that fail their schemas.
/// One line per problem, then a hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownFieldsError {
    /// `web: task web: unknown field `autoscael` (did you mean
    /// `autoscale`?)`, one per key, then one per invalid extension value
    /// (`orders-db: task postgres: volume data: backup.keep: 0 is less than
    /// the minimum of 1`).
    pub lines: Vec<String>,
    /// Whether any line is an unknown key (the hint offers the flag only
    /// then: it never excuses an invalid value).
    pub unknown_keys: bool,
}

impl fmt::Display for UnknownFieldsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.lines.join("\n"))?;
        // `\nHint: ` is what the CLI's `split_hint` lifts into the error
        // envelope's `hint`.
        if self.unknown_keys {
            f.write_str(
                "\nHint: fix or remove each field above (`legion app extensions` lists the keys \
                 installed apps add). To submit anyway, pass --allow-unknown-fields \
                 (`allow_unknown_fields` on the RPC): keys on the job, a task, a volume, a port, \
                 or a service are kept as pending, and the rest are dropped",
            )
        } else {
            f.write_str(
                "\nHint: fix each value above against its app's schema (`legion app extensions` \
                 shows them, `legion job schema` has them at their points)",
            )
        }
    }
}

impl std::error::Error for UnknownFieldsError {}

/// A jobspec block: something that captures unknown keys and may hold
/// nested blocks. Implemented by every jobspec struct that carries an
/// [`UnknownFields`] map.
pub(crate) trait SpecBlock {
    /// The keys this block accepts, spelled as a jobspec writes them. Used
    /// only for did-you-mean suggestions; what counts as unknown is decided
    /// by serde itself, so a key missing here costs a suggestion, never a
    /// false error. Traps for misplaced or renamed keys (`kind`, a
    /// task-level `onLost`) are left out, so they are never suggested.
    const FIELDS: &'static [&'static str];

    /// The extension point this block is, if any.
    const POINT: Option<ExtensionPoint> = None;

    /// The keys captured at parse.
    fn unknown_fields_mut(&mut self) -> &mut UnknownFields;

    /// Where kept keys go: `Some` exactly when [`Self::POINT`] is.
    fn pending_extensions_mut(&mut self) -> Option<&mut PendingExtensions> {
        None
    }

    /// Visit the nested blocks.
    fn walk_children(&mut self, _walk: &mut FieldWalk) {}
}

/// How a walk treats keys at extension points that no core field takes.
#[derive(Clone, Copy)]
pub enum ExtensionCheck<'r> {
    /// Resolve them against the keys installed apps declare (a manager's
    /// submit; `legion job validate` with a manager reachable). A key no
    /// installed app declares is unknown.
    Installed(&'r ExtensionRegistry),
    /// No registry to ask (`legion job validate` offline): they are
    /// reported as unchecked, not as errors.
    Offline,
}

/// A named scope the walk enters: how errors name the place (`task web:
/// volume data: ...`) and where an extension value found there sits.
pub(crate) enum Scope {
    /// A task, with its group.
    Task { name: String, group: String },
    /// A `volumes` entry.
    Volume(String),
    /// A `network.ports` entry.
    Port(String),
    /// A task's `service` block, with the name it advertises under (the
    /// task's name when it sets none) and whether the block names it.
    Service { name: String, named: bool },
    /// Any other labelled scope.
    #[allow(dead_code)]
    Label(String),
}

impl Scope {
    fn label(&self) -> String {
        match self {
            Scope::Task { name, .. } => format!("task {name}"),
            Scope::Volume(name) => format!("volume {name}"),
            Scope::Port(name) => format!("port {name}"),
            Scope::Service { name, named: true } => format!("service {name}"),
            Scope::Service { named: false, .. } => "service".to_string(),
            Scope::Label(label) => label.clone(),
        }
    }

    fn enter(&self, at: &mut ExtensionPath) {
        match self {
            Scope::Task { name, group } => {
                at.task = name.clone();
                at.group = group.clone();
            }
            Scope::Volume(name) => at.volume = name.clone(),
            Scope::Port(name) => at.port = name.clone(),
            Scope::Service { name, .. } => at.service = name.clone(),
            Scope::Label(_) => {}
        }
    }
}

/// The pass that decides what each captured key means for one request.
pub(crate) struct FieldWalk<'r> {
    mode: UnknownFieldMode,
    /// Whether a block may arrive with `pendingExtensions` already filled: a
    /// stored config being read back, or a submit under the flag (which
    /// round-trips a config exported with its pending keys). A strict submit
    /// may not, because Legion writes that map, people do not.
    accept_pending: bool,
    /// How keys at extension points resolve; `None` for a stored config,
    /// which never carries one (its values are already resolved).
    extensions: Option<ExtensionCheck<'r>>,
    scope: Vec<String>,
    path: Vec<String>,
    /// Where the walk is, for the extension values it resolves.
    at: ExtensionPath,
    /// Unknown keys.
    errors: Vec<String>,
    /// Extension values that fail their app's schema, and other problems
    /// the flag never excuses.
    value_errors: Vec<String>,
    record: UnknownFieldsRecord,
    resolved: Vec<ExtensionValue>,
}

impl<'r> FieldWalk<'r> {
    /// A walk over a jobspec someone submitted.
    pub(crate) fn submitted(mode: UnknownFieldMode, extensions: ExtensionCheck<'r>) -> Self {
        Self::new(mode, mode == UnknownFieldMode::Allow, Some(extensions))
    }

    /// A walk over a config Legion serialized itself (a stored version read
    /// back, a KV re-render): unknown keys are still errors, and stored
    /// pending maps are accepted.
    pub(crate) fn stored() -> Self {
        Self::new(UnknownFieldMode::Reject, true, None)
    }

    fn new(
        mode: UnknownFieldMode,
        accept_pending: bool,
        extensions: Option<ExtensionCheck<'r>>,
    ) -> Self {
        FieldWalk {
            mode,
            accept_pending,
            extensions,
            scope: Vec::new(),
            path: Vec::new(),
            at: ExtensionPath::default(),
            errors: Vec::new(),
            value_errors: Vec::new(),
            record: UnknownFieldsRecord::default(),
            resolved: Vec::new(),
        }
    }

    /// Whether this walk reads a stored config (whose resolved extension
    /// values it accepts as they are).
    pub(crate) fn is_stored(&self) -> bool {
        self.extensions.is_none()
    }

    /// The jobspec's top-level block.
    pub(crate) fn root<B: SpecBlock>(&mut self, block: &mut B) {
        self.visit(block);
    }

    /// A nested block under key `key` of the current block.
    pub(crate) fn block<B: SpecBlock>(&mut self, key: &str, block: &mut B) {
        self.path.push(key.to_string());
        self.visit(block);
        self.path.pop();
    }

    /// An optional nested block.
    pub(crate) fn opt<B: SpecBlock>(&mut self, key: &str, block: Option<&mut B>) {
        if let Some(block) = block {
            self.block(key, block);
        }
    }

    /// A list of blocks, each addressed `key[i]`.
    pub(crate) fn list<B: SpecBlock>(&mut self, key: &str, blocks: &mut [B]) {
        for (index, block) in blocks.iter_mut().enumerate() {
            self.block(&format!("{key}[{index}]"), block);
        }
    }

    /// A map of blocks, each addressed `key.name`.
    pub(crate) fn map<B: SpecBlock>(&mut self, key: &str, blocks: &mut BTreeMap<String, B>) {
        for (name, block) in blocks.iter_mut() {
            self.block(&format!("{key}.{name}"), block);
        }
    }

    /// A block that opens a named scope, `task web` or `volume data`: its
    /// errors read `task web: volume data: unknown field ...` rather than
    /// `volumes[0].x`, and extension values found in it carry its name.
    pub(crate) fn scoped<B: SpecBlock>(&mut self, scope: Scope, block: &mut B) {
        let outer = std::mem::take(&mut self.path);
        let outer_at = self.at.clone();
        scope.enter(&mut self.at);
        self.scope.push(scope.label());
        self.visit(block);
        self.scope.pop();
        self.path = outer;
        self.at = outer_at;
    }

    fn visit<B: SpecBlock>(&mut self, block: &mut B) {
        let scope = self.scope.join(": ");
        let point = B::POINT;
        if let Some(pending) = block.pending_extensions_mut() {
            if !pending.is_empty() {
                if self.accept_pending {
                    for key in pending.keys() {
                        self.record.pending.push(UnknownFieldEntry {
                            scope: scope.clone(),
                            path: key.clone(),
                            point,
                        });
                    }
                } else {
                    let keys: Vec<String> = pending.keys().map(|key| format!("`{key}`")).collect();
                    self.errors.push(format!(
                        "{}`{PENDING_EXTENSIONS_KEY}` holds keys Legion kept under \
                         --allow-unknown-fields, and is not written by hand: write {} directly, \
                         and pass --allow-unknown-fields to keep {} as pending",
                        prefix(&scope),
                        keys.join(", "),
                        if keys.len() == 1 { "it" } else { "them" }
                    ));
                }
            }
        }
        let captured = std::mem::take(block.unknown_fields_mut());
        for (key, value) in captured {
            // Keys directly on an extension point may be installed apps'.
            let (Some(point), Some(check)) = (point, self.extensions) else {
                self.unknown::<B>(&scope, point, key, value, &[], block);
                continue;
            };
            match check {
                ExtensionCheck::Offline => {
                    self.record.unchecked.push(UnknownFieldEntry {
                        scope: scope.clone(),
                        path: self.dotted(&key),
                        point: Some(point),
                    });
                }
                ExtensionCheck::Installed(registry) => {
                    if key == APPS_KEY {
                        self.qualified::<B>(&scope, point, value, registry, block);
                    } else if registry.is_simulated_core(point, &key) {
                        // A simulated new core field: serde would have
                        // consumed the key, so the walk never sees it.
                    } else if let Some(installed) = registry.owner_of(point, &key) {
                        self.resolve(&scope, installed, key, value);
                    } else {
                        let candidates: Vec<(String, Option<String>)> = registry
                            .keys_at(point)
                            .into_iter()
                            .map(|installed| (installed.key.clone(), Some(installed.app.clone())))
                            .collect();
                        self.unknown::<B>(&scope, Some(point), key, value, &candidates, block);
                    }
                }
            }
        }
        block.walk_children(self);
    }

    /// `apps = { <app> = { … } }`: each app's value under the qualified
    /// spelling.
    fn qualified<B: SpecBlock>(
        &mut self,
        scope: &str,
        point: ExtensionPoint,
        value: Value,
        registry: &ExtensionRegistry,
        block: &mut B,
    ) {
        let Value::Object(apps) = value else {
            self.value_errors.push(format!(
                "{}`{APPS_KEY}` holds installed apps' keys in their qualified spelling, one \
                 record per app (`{APPS_KEY} = {{ backup = {{ … }} }}`)",
                prefix(scope)
            ));
            return;
        };
        for (app, value) in apps {
            let written = format!("{APPS_KEY}.{app}");
            match registry.for_app(&app, point) {
                Some(installed) => self.resolve(scope, installed, written, value),
                None => {
                    let path = self.dotted(&written);
                    let why = if registry.has_app(&app) {
                        format!("the {app} app declares no key on {}", point_plural(point))
                    } else {
                        format!("no app named {app} is installed")
                    };
                    match self.mode {
                        UnknownFieldMode::Reject => {
                            self.errors
                                .push(format!("{}unknown field `{path}`: {why}", prefix(scope)));
                        }
                        UnknownFieldMode::Allow => {
                            if let Some(pending) = block.pending_extensions_mut() {
                                let apps = pending
                                    .entry(APPS_KEY.to_string())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Some(apps) = apps.as_object_mut() {
                                    apps.insert(app.clone(), value);
                                }
                            }
                            self.record.pending.push(UnknownFieldEntry {
                                scope: scope.to_string(),
                                path,
                                point: Some(point),
                            });
                        }
                    }
                }
            }
        }
    }

    /// Check a value an installed key owns, and keep it as written.
    fn resolve(&mut self, scope: &str, installed: &InstalledKey, written: String, value: Value) {
        let at = match installed.decl.point {
            ExtensionPoint::Job => ExtensionPath::default(),
            ExtensionPoint::Group | ExtensionPoint::Task => ExtensionPath {
                group: self.at.group.clone(),
                task: self.at.task.clone(),
                ..Default::default()
            },
            _ => self.at.clone(),
        };
        if let Some(other) = self.resolved.iter().find(|resolved| {
            resolved.app == installed.app
                && resolved.point == installed.decl.point
                && resolved.path == at
        }) {
            self.value_errors.push(format!(
                "{}`{}` and `{written}` both set {}'s key; write one",
                prefix(scope),
                other.key,
                installed.owner()
            ));
            return;
        }
        if let Err(errors) = installed.check(&value) {
            for error in errors {
                self.value_errors.push(format!(
                    "{}{}",
                    prefix(scope),
                    crate::structs::extensions::prefixed(&self.dotted(&written), &error)
                ));
            }
            return;
        }
        self.resolved.push(ExtensionValue {
            app: installed.app.clone(),
            point: installed.decl.point,
            path: at,
            key: written,
            value,
            validated_by: installed.validated_by(),
        });
    }

    /// A key nothing takes: an error, or under the flag pending (at an
    /// extension point) or dropped.
    fn unknown<B: SpecBlock>(
        &mut self,
        scope: &str,
        point: Option<ExtensionPoint>,
        key: String,
        value: Value,
        installed: &[(String, Option<String>)],
        block: &mut B,
    ) {
        let path = self.dotted(&key);
        match self.mode {
            UnknownFieldMode::Reject => {
                let mut candidates: Vec<(String, Option<String>)> = B::FIELDS
                    .iter()
                    .map(|field| (field.to_string(), None))
                    .collect();
                if point.is_some() {
                    candidates.push((APPS_KEY.to_string(), None));
                }
                candidates.extend(installed.iter().cloned());
                let names: Vec<&str> = candidates.iter().map(|(name, _)| name.as_str()).collect();
                let line = match suggest_owned(&key, &names) {
                    Some(index) => {
                        let (known, app) = &candidates[index];
                        let owner = app
                            .as_ref()
                            .map(|app| format!(", the {app} app's key"))
                            .unwrap_or_default();
                        format!(
                            "unknown field `{path}` (did you mean `{}`{owner}?)",
                            self.dotted(known)
                        )
                    }
                    // At an extension point on a manager, say both checks
                    // came up empty.
                    None => match (point, self.extensions) {
                        (Some(point), Some(ExtensionCheck::Installed(_)))
                            if self.path.is_empty() =>
                        {
                            format!(
                                "unknown field `{path}`: not a {} field, and no installed app \
                                 declares a `{key}` key on {}",
                                point.as_str(),
                                point_plural(point)
                            )
                        }
                        _ => format!("unknown field `{path}`"),
                    },
                };
                self.errors.push(format!("{}{line}", prefix(scope)));
            }
            UnknownFieldMode::Allow => match (point, block.pending_extensions_mut()) {
                (Some(point), Some(pending)) => {
                    pending.insert(key, value);
                    self.record.pending.push(UnknownFieldEntry {
                        scope: scope.to_string(),
                        path,
                        point: Some(point),
                    });
                }
                _ => self.record.dropped.push(UnknownFieldEntry {
                    scope: scope.to_string(),
                    path,
                    point: None,
                }),
            },
        }
    }

    /// `key`, under the current dotted path.
    fn dotted(&self, key: &str) -> String {
        if self.path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{key}", self.path.join("."))
        }
    }

    /// A group key is written on one task of its group (apps plan §6.6):
    /// the same app's group key on two tasks of one group is an error.
    fn check_group_keys(&mut self) {
        let mut seen: BTreeMap<(String, String), String> = BTreeMap::new();
        let mut lines = Vec::new();
        for value in self
            .resolved
            .iter()
            .filter(|value| value.point == ExtensionPoint::Group)
        {
            if let Some(first) = seen.insert(
                (value.app.clone(), value.path.group.clone()),
                value.path.task.clone(),
            ) {
                lines.push(format!(
                    "group {}: the {} app's group key is set on tasks {first} and {}; a group key \
                     applies to the whole group, so write it on one task of the group",
                    value.path.group, value.app, value.path.task
                ));
            }
        }
        self.value_errors.extend(lines);
    }

    /// Record a problem the flag never excuses (a group key on two tasks of
    /// one group, a hand-written `extensions` list).
    pub(crate) fn value_error(&mut self, line: String) {
        self.value_errors.push(line);
    }

    /// The outcome: the record of what was kept and dropped and the
    /// extension values resolved, or, when the request did not allow
    /// unknown keys and there were some (or a value failed its schema), an
    /// error naming each one. `job` prefixes every error line, the way the
    /// plan's examples read (`web: task web: unknown field ...`).
    pub(crate) fn finish(
        mut self,
        job: &str,
    ) -> Result<(UnknownFieldsRecord, Vec<ExtensionValue>), UnknownFieldsError> {
        self.check_group_keys();
        if self.errors.is_empty() && self.value_errors.is_empty() {
            return Ok((self.record, self.resolved));
        }
        let unknown_keys = !self.errors.is_empty();
        let lines = self
            .errors
            .into_iter()
            .chain(self.value_errors)
            .map(|line| {
                if job.is_empty() {
                    line
                } else {
                    format!("{job}: {line}")
                }
            })
            .collect();
        Err(UnknownFieldsError {
            lines,
            unknown_keys,
        })
    }
}

fn prefix(scope: &str) -> String {
    if scope.is_empty() {
        String::new()
    } else {
        format!("{scope}: ")
    }
}

/// The known key closest to `key`, if one is close enough to be a likely
/// typo: within an edit distance of a third of the key's length (at least
/// one), counting a swap of two adjacent letters as one edit, and ignoring
/// case, `_` and `-` (so `kill_after` suggests `killAfter`). Ties go to the
/// first key in `fields`.
pub fn suggest(key: &str, fields: &[&'static str]) -> Option<&'static str> {
    let wanted = normalize(key);
    if wanted.is_empty() {
        return None;
    }
    let limit = (wanted.chars().count() / 3).max(1);
    fields
        .iter()
        .map(|field| (strsim::osa_distance(&wanted, &normalize(field)), *field))
        .filter(|(distance, _)| *distance <= limit)
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, field)| field)
}

/// [`suggest`] over names that are not `'static` (installed apps' keys):
/// the index of the closest one.
pub fn suggest_owned(key: &str, fields: &[&str]) -> Option<usize> {
    let wanted = normalize(key);
    if wanted.is_empty() {
        return None;
    }
    let limit = (wanted.chars().count() / 3).max(1);
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| (strsim::osa_distance(&wanted, &normalize(field)), index))
        .filter(|(distance, _)| *distance <= limit)
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, index)| index)
}

fn normalize(key: &str) -> String {
    key.chars()
        .filter(|c| *c != '_' && *c != '-')
        .flat_map(char::to_lowercase)
        .collect()
}

/// Implement [`SpecBlock`] for a struct whose captured map is the field
/// `unknown_fields`.
///
/// ```ignore
/// spec_block!(StopConfig, ["signal", "killAfter", "drain"]);
/// spec_block!(StatefulConfig, ["mode", ...], |self, walk| {
///     walk.opt("storage", self.storage.as_mut());
/// });
/// ```
macro_rules! spec_block {
    ($ty:ty, [$($field:literal),* $(,)?]) => {
        impl $crate::structs::spec_fields::SpecBlock for $ty {
            const FIELDS: &'static [&'static str] = &[$($field),*];
            fn unknown_fields_mut(&mut self) -> &mut $crate::structs::spec_fields::UnknownFields {
                &mut self.unknown_fields
            }
        }
    };
    ($ty:ty, [$($field:literal),* $(,)?], |$this:ident, $walk:ident| $body:block) => {
        impl $crate::structs::spec_fields::SpecBlock for $ty {
            const FIELDS: &'static [&'static str] = &[$($field),*];
            fn unknown_fields_mut(&mut self) -> &mut $crate::structs::spec_fields::UnknownFields {
                &mut self.unknown_fields
            }
            fn walk_children(&mut self, $walk: &mut $crate::structs::spec_fields::FieldWalk) {
                let $this = self;
                $body
            }
        }
    };
}
pub(crate) use spec_block;

/// Implement [`SpecBlock`] for an extension point: a struct with both
/// `unknown_fields` and `pending_extensions`.
macro_rules! extension_point {
    ($ty:ty, $point:expr, [$($field:literal),* $(,)?]) => {
        $crate::structs::spec_fields::extension_point!($ty, $point, [$($field),*], |_this, _walk| {});
    };
    ($ty:ty, $point:expr, [$($field:literal),* $(,)?], |$this:ident, $walk:ident| $body:block) => {
        impl $crate::structs::spec_fields::SpecBlock for $ty {
            const FIELDS: &'static [&'static str] = &[$($field),*];
            const POINT: Option<$crate::structs::spec_fields::ExtensionPoint> = Some($point);
            fn unknown_fields_mut(&mut self) -> &mut $crate::structs::spec_fields::UnknownFields {
                &mut self.unknown_fields
            }
            fn pending_extensions_mut(
                &mut self,
            ) -> Option<&mut $crate::structs::spec_fields::PendingExtensions> {
                Some(&mut self.pending_extensions)
            }
            fn walk_children(&mut self, $walk: &mut $crate::structs::spec_fields::FieldWalk) {
                let $this = self;
                $body
            }
        }
    };
}
pub(crate) use extension_point;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// No apps installed.
    static NONE: std::sync::LazyLock<ExtensionRegistry> =
        std::sync::LazyLock::new(ExtensionRegistry::empty);

    #[derive(Default)]
    struct Leaf {
        unknown_fields: UnknownFields,
    }
    spec_block!(Leaf, ["signal", "killAfter"]);

    #[derive(Default)]
    struct Point {
        unknown_fields: UnknownFields,
        pending_extensions: PendingExtensions,
        leaf: Leaf,
    }
    extension_point!(
        Point,
        ExtensionPoint::Volume,
        ["name", "leaf"],
        |this, walk| {
            walk.block("leaf", &mut this.leaf);
        }
    );

    fn point_with(unknown: &[(&str, Value)], leaf_unknown: &[(&str, Value)]) -> Point {
        let mut point = Point::default();
        for (key, value) in unknown {
            point.unknown_fields.insert(key.to_string(), value.clone());
        }
        for (key, value) in leaf_unknown {
            point
                .leaf
                .unknown_fields
                .insert(key.to_string(), value.clone());
        }
        point
    }

    #[test]
    fn suggest_finds_typos_test() {
        let fields = &["autoscale", "count", "killAfter", "memoryMax"];
        assert_eq!(suggest("autoscael", fields), Some("autoscale"));
        assert_eq!(suggest("kill_after", fields), Some("killAfter"));
        assert_eq!(suggest("KillAfter", fields), Some("killAfter"));
        assert_eq!(suggest("memoryMax2", fields), Some("memoryMax"));
        assert_eq!(suggest("cnt", fields), None);
        assert_eq!(suggest("backup", fields), None);
        assert_eq!(suggest("", fields), None);
    }

    #[test]
    fn reject_names_each_key_with_its_path_and_suggestion_test() {
        let mut point = point_with(&[("nmae", json!("x"))], &[("signall", json!("TERM"))]);
        let mut walk =
            FieldWalk::submitted(UnknownFieldMode::Reject, ExtensionCheck::Installed(&NONE));
        walk.scoped(Scope::Label("task web".into()), &mut Leaf::default());
        walk.scoped(Scope::Volume("data".into()), &mut point);
        let err = walk.finish("web").unwrap_err();
        assert_eq!(
            err.lines,
            vec![
                "web: volume data: unknown field `nmae` (did you mean `name`?)".to_string(),
                "web: volume data: unknown field `leaf.signall` (did you mean `leaf.signal`?)"
                    .to_string(),
            ]
        );
        assert!(err.to_string().contains("\nHint: "), "{err}");
    }

    #[test]
    fn allow_keeps_point_keys_and_drops_the_rest_test() {
        let mut point = point_with(
            &[("backup", json!({ "keep": 3 }))],
            &[("signall", json!("TERM"))],
        );
        let mut walk =
            FieldWalk::submitted(UnknownFieldMode::Allow, ExtensionCheck::Installed(&NONE));
        walk.scoped(Scope::Volume("data".into()), &mut point);
        let (record, _) = walk.finish("web").expect("allowed");
        assert_eq!(point.pending_extensions["backup"], json!({ "keep": 3 }));
        assert!(point.unknown_fields.is_empty());
        assert!(point.leaf.unknown_fields.is_empty());
        assert_eq!(
            record.pending,
            vec![UnknownFieldEntry {
                scope: "volume data".into(),
                path: "backup".into(),
                point: Some(ExtensionPoint::Volume),
            }]
        );
        assert_eq!(
            record.dropped,
            vec![UnknownFieldEntry {
                scope: "volume data".into(),
                path: "leaf.signall".into(),
                point: None,
            }]
        );
        assert_eq!(
            record.warnings(),
            vec![
                "volume data: dropped unknown field `leaf.signall`".to_string(),
                "volume data: kept `backup` as pending (no installed app declares it)".to_string(),
            ]
        );
        assert_eq!(
            record.summary(),
            "dropped volume data: leaf.signall; backup on volume data pending (no installed \
             app declares it)"
        );
    }

    #[test]
    fn a_strict_submit_refuses_a_hand_written_pending_map_test() {
        let mut point = Point::default();
        point
            .pending_extensions
            .insert("backup".into(), json!(true));
        let mut walk =
            FieldWalk::submitted(UnknownFieldMode::Reject, ExtensionCheck::Installed(&NONE));
        walk.scoped(Scope::Volume("data".into()), &mut point);
        let err = walk.finish("web").unwrap_err();
        assert!(
            err.lines[0].contains("`pendingExtensions` holds keys"),
            "{err}"
        );

        // A stored config reads its pending map back, and a submit under the
        // flag keeps (and reports) it.
        for mut walk in [
            FieldWalk::stored(),
            FieldWalk::submitted(UnknownFieldMode::Allow, ExtensionCheck::Installed(&NONE)),
        ] {
            walk.scoped(Scope::Volume("data".into()), &mut point);
            let (record, _) = walk.finish("web").expect("accepted");
            assert_eq!(record.pending.len(), 1);
        }
    }
}
