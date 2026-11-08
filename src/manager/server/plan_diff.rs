//! The field-level diff `PlanJob` returns (`PlanJobReply.diff`;
//! revisions-and-watches plan §9, review AI5): what a submit would change in
//! a job's stored spec, path by path, like `kubectl diff` or Nomad's
//! `job plan`.

use super::*;
use crate::structs::JobFieldChange;
use serde_json::Value;

impl ManagerDaemonService {
    /// `PlanJobReply.diff` for a changed spec: the stored version `existing`
    /// against the planned `planned`, with values the stored version
    /// rendered from KV keys `identity` cannot read redacted (the planned
    /// spec's own references were checked before it was resolved).
    pub(super) fn plan_field_diff(
        &self,
        identity: &AclIdentity,
        existing: &JobConfig,
        planned: &JobConfig,
    ) -> Vec<JobFieldChange> {
        let (Ok(old), Ok(new)) = (
            serde_json::to_value(existing),
            serde_json::to_value(planned),
        ) else {
            return Vec::new();
        };
        let hidden: Vec<String> = self
            .store
            .job_kv_references_for_job(&existing.name)
            .unwrap_or_default()
            .into_iter()
            .filter(|reference| reference.job_config_id == existing.id())
            .filter(|reference| {
                !identity.allows(
                    AclResource::Kv,
                    AclAction::Read,
                    &AclScope::path(&reference.kv_key),
                )
            })
            .map(|reference| reference.json_pointer)
            .collect();
        let redact = |pointer: &str| {
            hidden.iter().any(|secret| {
                secret == pointer
                    || secret.starts_with(&format!("{pointer}/"))
                    || pointer.starts_with(&format!("{secret}/"))
                    // A whole-document change covers everything in it.
                    || pointer.is_empty()
            })
        };
        job_field_diff(&old, &new, &redact)
    }
}

/// The paths that differ between the stored spec `old` and the planned spec
/// `new` (both serialized `JobConfig`s), in path order. Records are compared
/// key by key; lists whose elements all carry a `name` (tasks, ports,
/// services) are compared by name, other lists of equal length by index,
/// and anything else whole. The stored `version` is not compared: it is the
/// version's number, not its content. `redact` says which JSON pointers of
/// the stored spec hold a value the caller may not see (a KV reference to a
/// key it cannot read); a change there is reported without values.
pub(crate) fn job_field_diff(
    old: &Value,
    new: &Value,
    redact: &dyn Fn(&str) -> bool,
) -> Vec<JobFieldChange> {
    let strip = |value: &Value| {
        let mut value = value.clone();
        if let Value::Object(map) = &mut value {
            map.remove("version");
        }
        value
    };
    let (old, new) = (strip(old), strip(new));
    let mut out = Vec::new();
    walk(
        &mut Vec::new(),
        Some(String::new()),
        Some(&old),
        Some(&new),
        redact,
        &mut out,
    );
    out
}

fn escape(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn named(items: &[Value]) -> Option<Vec<(&str, &Value)>> {
    items
        .iter()
        .map(|item| {
            item.get("name")
                .and_then(Value::as_str)
                .map(|name| (name, item))
        })
        .collect()
}

fn render(value: Option<&Value>) -> String {
    value.map(Value::to_string).unwrap_or_default()
}

fn walk(
    path: &mut Vec<String>,
    old_pointer: Option<String>,
    old: Option<&Value>,
    new: Option<&Value>,
    redact: &dyn Fn(&str) -> bool,
    out: &mut Vec<JobFieldChange>,
) {
    if old == new {
        return;
    }
    let child_pointer = |key: &str| {
        old_pointer
            .as_ref()
            .map(|pointer| format!("{pointer}/{}", escape(key)))
    };
    match (old, new) {
        (Some(Value::Object(a)), Some(Value::Object(b))) => {
            let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                path.push(key.clone());
                walk(
                    path,
                    child_pointer(key),
                    a.get(key),
                    b.get(key),
                    redact,
                    out,
                );
                path.pop();
            }
            return;
        }
        (Some(Value::Array(a)), Some(Value::Array(b))) => {
            if let (Some(old_named), Some(new_named)) = (named(a), named(b)) {
                // Names that repeat within a list cannot be paired by name.
                let unique = |items: &[(&str, &Value)]| {
                    let mut names: Vec<&str> = items.iter().map(|(name, _)| *name).collect();
                    names.sort_unstable();
                    names.dedup();
                    names.len() == items.len()
                };
                let mut names: Vec<&str> = old_named
                    .iter()
                    .chain(new_named.iter())
                    .map(|(name, _)| *name)
                    .collect();
                names.sort_unstable();
                names.dedup();
                if unique(&old_named) && unique(&new_named) {
                    for name in names {
                        let old_at = old_named.iter().position(|(n, _)| *n == name);
                        let new_at = new_named.iter().find(|(n, _)| *n == name);
                        path.push(name.to_string());
                        walk(
                            path,
                            old_at.and_then(|index| child_pointer(&index.to_string())),
                            old_at.map(|index| old_named[index].1),
                            new_at.map(|(_, value)| *value),
                            redact,
                            out,
                        );
                        path.pop();
                    }
                    return;
                }
            }
            if a.len() == b.len() {
                for (index, (x, y)) in a.iter().zip(b.iter()).enumerate() {
                    path.push(index.to_string());
                    walk(
                        path,
                        child_pointer(&index.to_string()),
                        Some(x),
                        Some(y),
                        redact,
                        out,
                    );
                    path.pop();
                }
                return;
            }
        }
        _ => {}
    }
    let change = match (old, new) {
        (None, Some(_)) => "added",
        (Some(_), None) => "removed",
        _ => "changed",
    };
    let redacted = old_pointer
        .as_deref()
        .is_some_and(|pointer| old.is_some() && redact(pointer));
    out.push(JobFieldChange {
        path: path.join("."),
        change: change.to_string(),
        old_json: if redacted { String::new() } else { render(old) },
        new_json: if redacted { String::new() } else { render(new) },
        redacted,
    });
}

#[cfg(test)]
mod plan_diff_test {
    use super::*;
    use serde_json::json;

    fn summary(changes: &[JobFieldChange]) -> Vec<(String, String, String, String)> {
        changes
            .iter()
            .map(|change| {
                (
                    change.path.clone(),
                    change.change.clone(),
                    change.old_json.clone(),
                    change.new_json.clone(),
                )
            })
            .collect()
    }

    #[test]
    fn a_diff_names_each_changed_path_with_tasks_by_name() {
        let old = json!({
            "name": "web",
            "version": 3,
            "priority": "normal",
            "tasks": [
                { "name": "api", "count": 2, "env": { "LOG": "info" } },
                { "name": "worker", "count": 1 },
            ],
        });
        let new = json!({
            "name": "web",
            "priority": "high",
            "tasks": [
                { "name": "worker", "count": 1 },
                { "name": "api", "count": 3, "env": { "LOG": "info", "MODE": "x" } },
                { "name": "cron", "count": 1 },
            ],
        });
        let diff = job_field_diff(&old, &new, &|_| false);
        assert_eq!(
            summary(&diff),
            vec![
                (
                    "priority".into(),
                    "changed".into(),
                    "\"normal\"".into(),
                    "\"high\"".into()
                ),
                (
                    "tasks.api.count".into(),
                    "changed".into(),
                    "2".into(),
                    "3".into()
                ),
                (
                    "tasks.api.env.MODE".into(),
                    "added".into(),
                    String::new(),
                    "\"x\"".into()
                ),
                (
                    "tasks.cron".into(),
                    "added".into(),
                    String::new(),
                    "{\"count\":1,\"name\":\"cron\"}".into()
                ),
            ],
            "the version and the task order are not changes"
        );
        assert!(job_field_diff(&old, &old, &|_| false).is_empty());
    }

    #[test]
    fn a_value_from_an_unreadable_kv_key_is_redacted() {
        let old = json!({ "tasks": [{ "name": "api", "env": { "TOKEN": "s3cret" } }] });
        let new = json!({ "tasks": [{ "name": "api", "env": { "TOKEN": "n3w" } }] });
        let diff = job_field_diff(&old, &new, &|pointer| pointer == "/tasks/0/env/TOKEN");
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "tasks.api.env.TOKEN");
        assert!(diff[0].redacted);
        assert!(diff[0].old_json.is_empty() && diff[0].new_json.is_empty());
    }
}
