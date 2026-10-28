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
use serde_json::Value;

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

/// A place in the jobspec where keys Legion does not know are kept, not
/// dropped, under `allow_unknown_fields`: the places apps will declare their
/// own keys (internal_docs/plans/legion-apps-plan.md §6.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionPoint {
    /// The top level of the jobspec.
    Job,
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
}

impl UnknownFieldsRecord {
    /// True when the flag neither kept nor dropped anything.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.dropped.is_empty()
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
        dropped.chain(pending).collect()
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
        }
    }
}

/// A jobspec carried keys Legion does not know, and the request did not
/// allow them. One line per key, then a hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownFieldsError {
    /// `web: task web: unknown field `autoscael` (did you mean
    /// `autoscale`?)`, one per key.
    pub lines: Vec<String>,
}

impl fmt::Display for UnknownFieldsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.lines.join("\n"))?;
        // `\nHint: ` is what the CLI's `split_hint` lifts into the error
        // envelope's `hint`.
        f.write_str(
            "\nHint: fix or remove each field above. To submit anyway, pass \
             --allow-unknown-fields (`allow_unknown_fields` on the RPC): keys on the job, a \
             task, a volume, a port, or a service are kept as pending, and the rest are dropped",
        )
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

/// The pass that decides what each captured key means for one request.
pub(crate) struct FieldWalk {
    mode: UnknownFieldMode,
    /// Whether a block may arrive with `pendingExtensions` already filled: a
    /// stored config being read back, or a submit under the flag (which
    /// round-trips a config exported with its pending keys). A strict submit
    /// may not, because Legion writes that map, people do not.
    accept_pending: bool,
    scope: Vec<String>,
    path: Vec<String>,
    errors: Vec<String>,
    record: UnknownFieldsRecord,
}

impl FieldWalk {
    /// A walk over a jobspec someone submitted.
    pub(crate) fn submitted(mode: UnknownFieldMode) -> Self {
        Self::new(mode, mode == UnknownFieldMode::Allow)
    }

    /// A walk over a config Legion serialized itself (a stored version read
    /// back, a KV re-render): unknown keys are still errors, and stored
    /// pending maps are accepted.
    pub(crate) fn stored() -> Self {
        Self::new(UnknownFieldMode::Reject, true)
    }

    fn new(mode: UnknownFieldMode, accept_pending: bool) -> Self {
        FieldWalk {
            mode,
            accept_pending,
            scope: Vec::new(),
            path: Vec::new(),
            errors: Vec::new(),
            record: UnknownFieldsRecord::default(),
        }
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
    /// `volumes[0].x`.
    pub(crate) fn scoped<B: SpecBlock>(&mut self, label: String, block: &mut B) {
        let outer = std::mem::take(&mut self.path);
        self.scope.push(label);
        self.visit(block);
        self.scope.pop();
        self.path = outer;
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
            let path = self.dotted(&key);
            match self.mode {
                UnknownFieldMode::Reject => {
                    let hint = suggest(&key, B::FIELDS)
                        .map(|known| format!(" (did you mean `{}`?)", self.dotted(known)))
                        .unwrap_or_default();
                    self.errors
                        .push(format!("{}unknown field `{path}`{hint}", prefix(&scope)));
                }
                UnknownFieldMode::Allow => match (point, block.pending_extensions_mut()) {
                    (Some(point), Some(pending)) => {
                        pending.insert(key, value);
                        self.record.pending.push(UnknownFieldEntry {
                            scope: scope.clone(),
                            path,
                            point: Some(point),
                        });
                    }
                    _ => self.record.dropped.push(UnknownFieldEntry {
                        scope: scope.clone(),
                        path,
                        point: None,
                    }),
                },
            }
        }
        block.walk_children(self);
    }

    /// `key`, under the current dotted path.
    fn dotted(&self, key: &str) -> String {
        if self.path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{key}", self.path.join("."))
        }
    }

    /// The outcome: the record of what was kept and dropped, or, when the
    /// request did not allow unknown keys and there were some, an error
    /// naming each one. `job` prefixes every error line, the way the plan's
    /// examples read (`web: task web: unknown field ...`).
    pub(crate) fn finish(self, job: &str) -> Result<UnknownFieldsRecord, UnknownFieldsError> {
        if self.errors.is_empty() {
            return Ok(self.record);
        }
        let lines = self
            .errors
            .into_iter()
            .map(|line| {
                if job.is_empty() {
                    line
                } else {
                    format!("{job}: {line}")
                }
            })
            .collect();
        Err(UnknownFieldsError { lines })
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
        let mut walk = FieldWalk::submitted(UnknownFieldMode::Reject);
        walk.scoped("task web".into(), &mut Leaf::default());
        walk.scoped("volume data".into(), &mut point);
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
        let mut walk = FieldWalk::submitted(UnknownFieldMode::Allow);
        walk.scoped("volume data".into(), &mut point);
        let record = walk.finish("web").expect("allowed");
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
        let mut walk = FieldWalk::submitted(UnknownFieldMode::Reject);
        walk.scoped("volume data".into(), &mut point);
        let err = walk.finish("web").unwrap_err();
        assert!(
            err.lines[0].contains("`pendingExtensions` holds keys"),
            "{err}"
        );

        // A stored config reads its pending map back, and a submit under the
        // flag keeps (and reports) it.
        for mut walk in [
            FieldWalk::stored(),
            FieldWalk::submitted(UnknownFieldMode::Allow),
        ] {
            walk.scoped("volume data".into(), &mut point);
            let record = walk.finish("web").expect("accepted");
            assert_eq!(record.pending.len(), 1);
        }
    }
}
