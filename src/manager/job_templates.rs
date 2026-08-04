//! Job-template manifest parsing, variable validation, and sandboxed
//! server-side Nickel rendering.
//!
//! A template is one self-contained Nickel file that evaluates to a record
//! with optional `description` and `variables` fields plus a
//! `job | not_exported = fun vars => ...` function returning the jobspec.
//! Exporting the file yields the manifest (`not_exported` fields are dropped
//! on export, which is what keeps `job` out of the published manifest);
//! rendering applies `template.job` to the resolved variable values, handed
//! in as a native Nickel record through the evaluator's `vars` binding — no
//! string parsing, so numbers stay numbers and booleans stay booleans.
//!
//! Rendering runs on the manager, so it is sandboxed: imports are rejected
//! before evaluation starts, source and variable payloads are size-capped,
//! and evaluation runs on its own thread under a wall-clock timeout. See
//! docs/job-templates.md.

use crate::config_lang;
use crate::prelude::*;
use crate::storage::store::JobTemplateVariableRecord;

use std::time::Duration;

/// Maximum accepted template source size.
pub const MAX_SOURCE_BYTES: usize = 512 * 1024;
/// Maximum accepted `variables_json` payload size.
pub const MAX_VARIABLES_BYTES: usize = 64 * 1024;
/// Wall-clock budget for one Nickel evaluation.
pub const RENDER_TIMEOUT: Duration = Duration::from_secs(5);

const MAX_NAME_LEN: usize = 128;

pub const VARIABLE_TYPES: &[&str] = &[
    "string",
    "text",
    "number",
    "boolean",
    "select",
    "multi_select",
    "json",
];

/// The metadata half of a template document (everything but `job`).
#[derive(Debug, Clone)]
pub struct TemplateManifest {
    pub description: Option<String>,
    pub variables: Vec<JobTemplateVariableRecord>,
}

/// Template names follow job-name rules: lowercase alphanumerics and
/// dashes, starting and ending alphanumeric.
pub fn validate_template_name(name: &str) -> R<()> {
    if name.is_empty() {
        bail!("template name must not be empty");
    }
    if name.len() > MAX_NAME_LEN {
        bail!("template name must be at most {MAX_NAME_LEN} characters");
    }
    let valid_chars = name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !valid_chars || name.starts_with('-') || name.ends_with('-') {
        bail!(
            "template name '{name}' is invalid: use lowercase letters, digits, and dashes, \
             starting and ending with a letter or digit"
        );
    }
    Ok(())
}

fn check_source_size(source: &str) -> R<()> {
    if source.len() > MAX_SOURCE_BYTES {
        bail!(
            "template source is {} bytes; the maximum is {} bytes",
            source.len(),
            MAX_SOURCE_BYTES
        );
    }
    Ok(())
}

/// Evaluates the manifest half of a template document.
///
/// The wrapper treats the whole source as one expression (a Nickel file is an
/// expression, possibly preceded by `let` bindings, so parenthesizing it is
/// always valid) and projects out the metadata plus shape checks. It reads
/// `job` through record access rather than exporting the template directly,
/// so the `not_exported` annotation that hides `job` from the manifest does
/// not hide it from these checks.
pub fn parse_manifest(source: &str) -> R<TemplateManifest> {
    check_source_size(source)?;
    let snippet = format!(
        r#"let template = (
{source}
) in
if !(std.is_record template) then
  std.fail_with "template must evaluate to a record"
else {{
  description =
    if std.record.has_field "description" template then template.description else null,
  variables = if std.record.has_field "variables" template then template.variables else [],
  hasJob = std.record.has_field "job" template,
  jobIsFunction =
    std.record.has_field "job" template && std.is_function template.job,
}}"#
    );
    let manifest_json = config_lang::eval_sandboxed("template-manifest", &snippet, None)
        .map_err(|err| anyhow!("invalid template source: {err}"))?;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RawManifest {
        description: Option<Value>,
        variables: Value,
        has_job: bool,
        job_is_function: bool,
    }
    let raw: RawManifest = serde_json::from_str(&manifest_json)
        .map_err(|err| anyhow!("invalid template manifest: {err}"))?;

    if !raw.has_job {
        bail!("template must declare a `job | not_exported = fun vars => ...` function field");
    }
    if !raw.job_is_function {
        bail!("template field `job` must be a function taking the variables record");
    }
    let description = match raw.description {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text),
        Some(other) => bail!("template `description` must be a string, got {other}"),
    };
    let variables: Vec<JobTemplateVariableRecord> =
        serde_json::from_value(raw.variables).map_err(|err| {
            anyhow!(
                "template `variables` must be a list of variable objects \
                 (key, type, label, description, required, default, options): {err}"
            )
        })?;
    validate_variable_definitions(&variables)?;
    Ok(TemplateManifest {
        description,
        variables,
    })
}

/// Definition-time checks on the declared variables.
fn validate_variable_definitions(defs: &[JobTemplateVariableRecord]) -> R<()> {
    let mut seen = std::collections::HashSet::new();
    for def in defs {
        let key = def.key.as_str();
        let valid_key = !key.is_empty()
            && key
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid_key {
            bail!("variable key '{key}' is invalid: use [a-zA-Z_][a-zA-Z0-9_]*");
        }
        if !seen.insert(key.to_string()) {
            bail!("variable key '{key}' is declared more than once");
        }
        if !VARIABLE_TYPES.contains(&def.var_type.as_str()) {
            bail!(
                "variable '{key}' has unknown type '{}' (expected one of: {})",
                def.var_type,
                VARIABLE_TYPES.join(", ")
            );
        }
        let selectish = matches!(def.var_type.as_str(), "select" | "multi_select");
        if selectish && def.options.is_empty() {
            bail!(
                "variable '{key}' of type '{}' must declare options",
                def.var_type
            );
        }
        if !selectish && !def.options.is_empty() {
            bail!(
                "variable '{key}' of type '{}' must not declare options",
                def.var_type
            );
        }
        if let Some(default) = &def.default {
            coerce_value(def, default.clone()).map_err(|err| {
                anyhow!("variable '{key}' default does not match its type: {err}")
            })?;
        }
    }
    Ok(())
}

/// Coerces one provided value to a variable's declared type.
///
/// Strings are leniently accepted for `number`, `boolean`, and
/// `multi_select` (comma-separated) so `--var key=value` flags stay
/// ergonomic; everything else must already be the right JSON shape.
fn coerce_value(def: &JobTemplateVariableRecord, value: Value) -> R<Value> {
    let key = &def.key;
    match def.var_type.as_str() {
        "string" | "text" => match value {
            Value::String(_) => Ok(value),
            other => bail!("expected a string, got {other}"),
        },
        "number" => match value {
            Value::Number(_) => Ok(value),
            Value::String(text) => {
                let parsed: f64 = text
                    .trim()
                    .parse()
                    .map_err(|_| anyhow!("expected a number, got \"{text}\""))?;
                serde_json::Number::from_f64(parsed)
                    .map(Value::Number)
                    .ok_or_else(|| anyhow!("number for '{key}' is not finite"))
            }
            other => bail!("expected a number, got {other}"),
        },
        "boolean" => match value {
            Value::Bool(_) => Ok(value),
            Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => bail!("expected true or false, got \"{text}\""),
            },
            other => bail!("expected a boolean, got {other}"),
        },
        "select" => match value {
            Value::String(choice) => {
                if def.options.iter().any(|option| option == &choice) {
                    Ok(Value::String(choice))
                } else {
                    bail!(
                        "\"{choice}\" is not one of the allowed options ({})",
                        def.options.join(", ")
                    )
                }
            }
            other => bail!("expected one of the allowed options, got {other}"),
        },
        "multi_select" => {
            let choices: Vec<String> = match value {
                Value::Array(items) => items
                    .into_iter()
                    .map(|item| match item {
                        Value::String(text) => Ok(text),
                        other => bail!("expected a list of strings, got element {other}"),
                    })
                    .collect::<R<_>>()?,
                Value::String(text) => text
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(str::to_string)
                    .collect(),
                other => bail!("expected a list of strings, got {other}"),
            };
            for choice in &choices {
                if !def.options.iter().any(|option| option == choice) {
                    bail!(
                        "\"{choice}\" is not one of the allowed options ({})",
                        def.options.join(", ")
                    );
                }
            }
            Ok(Value::Array(
                choices.into_iter().map(Value::String).collect(),
            ))
        }
        "json" => Ok(value),
        other => bail!("unknown variable type '{other}'"),
    }
}

/// Merges provided values with defaults, validates and coerces them, and
/// returns the complete `vars` record `template.job` is applied to.
/// Every declared key is present; optional variables with no default render
/// as `null`. Unknown provided keys are rejected (typo protection).
pub fn resolve_variable_values(
    defs: &[JobTemplateVariableRecord],
    variables_json: &str,
) -> R<Map<String, Value>> {
    if variables_json.len() > MAX_VARIABLES_BYTES {
        bail!(
            "variables payload is {} bytes; the maximum is {} bytes",
            variables_json.len(),
            MAX_VARIABLES_BYTES
        );
    }
    let provided: Map<String, Value> = if variables_json.trim().is_empty() {
        Map::new()
    } else {
        serde_json::from_str(variables_json).map_err(|err| {
            anyhow!("variables must be a JSON object keyed by variable key: {err}")
        })?
    };

    for key in provided.keys() {
        if !defs.iter().any(|def| &def.key == key) {
            bail!("unknown variable '{key}' (not declared by this template version)");
        }
    }

    let mut resolved = Map::new();
    for def in defs {
        let value = match provided.get(&def.key) {
            Some(Value::Null) | None => match &def.default {
                Some(default) => default.clone(),
                None if def.is_required() => {
                    bail!("variable '{}' is required", def.key)
                }
                None => Value::Null,
            },
            Some(value) => coerce_value(def, value.clone())
                .map_err(|err| anyhow!("variable '{}': {err}", def.key))?,
        };
        resolved.insert(def.key.clone(), value);
    }
    Ok(resolved)
}

/// Renders `template.job vars` and returns the jobspec JSON.
fn render(source: &str, vars: &Map<String, Value>) -> R<String> {
    check_source_size(source)?;
    let snippet = format!(
        r#"let template = (
{source}
) in
template.job vars"#
    );
    config_lang::eval_sandboxed("template-render", &snippet, Some(vars))
        .map_err(|err| anyhow!("template rendering failed: {err}"))
}

/// Renders on a dedicated thread under `RENDER_TIMEOUT`.
///
/// Nickel is Turing-complete and its evaluator cannot be interrupted, so on
/// timeout the render thread is abandoned (it keeps a core busy until the
/// evaluation finishes or the process exits) and the caller gets an error.
/// Call from sync/blocking context.
pub fn render_with_timeout(source: &str, vars: &Map<String, Value>) -> R<String> {
    let source = source.to_string();
    let vars = vars.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("template-render".into())
        .spawn(move || {
            let _ = sender.send(render(&source, &vars));
        })
        .map_err(|err| anyhow!("failed to spawn render thread: {err}"))?;
    match receiver.recv_timeout(RENDER_TIMEOUT) {
        Ok(result) => result,
        Err(_) => {
            warn!(
                timeout_secs = RENDER_TIMEOUT.as_secs(),
                "template render timed out; abandoning render thread"
            );
            bail!(
                "template rendering exceeded the {}s limit",
                RENDER_TIMEOUT.as_secs()
            )
        }
    }
}

#[cfg(test)]
mod job_templates_test {
    use super::*;

    const VALID_TEMPLATE: &str = r#"
{
  description = "demo",
  variables = [
    { key = "name", type = "string" },
    { key = "replicas", type = "number", "default" = 2 },
    { key = "debug", type = "boolean", "default" = false },
    { key = "quality", type = "select", options = ["draft", "final"], "default" = "draft" },
    { key = "regions", type = "multi_select", options = ["us", "eu"], "default" = [] },
    { key = "extra", type = "json", required = false },
  ],
  job | not_exported = fun vars => {
    name = "demo-%{vars.name}",
    tasks = [{ name = "main", runtime = "noop", count = vars.replicas }],
  },
}
"#;

    fn defs() -> Vec<JobTemplateVariableRecord> {
        parse_manifest(VALID_TEMPLATE)
            .expect("valid template")
            .variables
    }

    #[test]
    fn parses_manifest_and_hides_job() {
        let manifest = parse_manifest(VALID_TEMPLATE).expect("valid template");
        assert_eq!(manifest.description.as_deref(), Some("demo"));
        assert_eq!(manifest.variables.len(), 6);
        assert!(manifest.variables[0].is_required());
        assert!(!manifest.variables[1].is_required());
    }

    #[test]
    fn rejects_manifests_without_a_job_function() {
        let err = parse_manifest(r#"{ description = "x" }"#).unwrap_err();
        assert!(err.to_string().contains("job"), "{err}");
        let err = parse_manifest(r#"{ job = "not a function" }"#).unwrap_err();
        assert!(err.to_string().contains("must be a function"), "{err}");
        let err = parse_manifest("[1, 2]").unwrap_err();
        assert!(err.to_string().contains("record"), "{err}");
        assert!(parse_manifest("this is not nickel {{{").is_err());
    }

    #[test]
    fn rejects_bad_variable_definitions() {
        let bad_type = r#"{ variables = [{ key = "a", type = "enum" }], job | not_exported = fun vars => {} }"#;
        assert!(parse_manifest(bad_type).is_err());
        let dup = r#"{ variables = [
            { key = "a", type = "string" }, { key = "a", type = "number" }
        ], job | not_exported = fun vars => {} }"#;
        assert!(parse_manifest(dup).is_err());
        let no_options = r#"{ variables = [{ key = "a", type = "select" }], job | not_exported = fun vars => {} }"#;
        assert!(parse_manifest(no_options).is_err());
        let bad_default = r#"{ variables = [{ key = "a", type = "number", "default" = "nope" }], job | not_exported = fun vars => {} }"#;
        assert!(parse_manifest(bad_default).is_err());
    }

    #[test]
    fn manifest_omits_the_job_function() {
        let json = config_lang::eval_sandboxed("t", VALID_TEMPLATE, None).expect("export");
        let value: Value = serde_json::from_str(&json).expect("json");
        assert!(
            value.get("job").is_none(),
            "`job` must be hidden from the published manifest: {value}"
        );
        assert_eq!(value["description"], json!("demo"));
    }

    #[test]
    fn resolves_and_coerces_values() {
        let resolved = resolve_variable_values(
            &defs(),
            r#"{"name": "web", "replicas": "3", "debug": "true", "regions": "us,eu"}"#,
        )
        .expect("valid values");
        assert_eq!(resolved["name"], json!("web"));
        assert_eq!(resolved["replicas"], json!(3.0));
        assert_eq!(resolved["debug"], json!(true));
        assert_eq!(resolved["quality"], json!("draft"));
        assert_eq!(resolved["regions"], json!(["us", "eu"]));
        assert_eq!(resolved["extra"], Value::Null);
    }

    #[test]
    fn rejects_missing_required_unknown_and_mistyped_values() {
        let missing = resolve_variable_values(&defs(), "{}").unwrap_err();
        assert!(
            missing.to_string().contains("'name' is required"),
            "{missing}"
        );
        let unknown = resolve_variable_values(&defs(), r#"{"name": "x", "typo": 1}"#).unwrap_err();
        assert!(
            unknown.to_string().contains("unknown variable"),
            "{unknown}"
        );
        let bad_choice =
            resolve_variable_values(&defs(), r#"{"name": "x", "quality": "ultra"}"#).unwrap_err();
        assert!(
            bad_choice.to_string().contains("allowed options"),
            "{bad_choice}"
        );
        let bad_number =
            resolve_variable_values(&defs(), r#"{"name": "x", "replicas": "many"}"#).unwrap_err();
        assert!(bad_number.to_string().contains("number"), "{bad_number}");
    }

    #[test]
    fn renders_with_native_types() {
        let vars = resolve_variable_values(&defs(), r#"{"name": "web", "replicas": 3}"#)
            .expect("valid values");
        let rendered = render(VALID_TEMPLATE, &vars).expect("render");
        let spec: Value = serde_json::from_str(&rendered).expect("json");
        assert_eq!(spec["name"], json!("demo-web"));
        assert_eq!(spec["tasks"][0]["count"], json!(3));
    }

    #[test]
    fn blocks_imports() {
        let importing = r#"
{
  variables = [],
  job | not_exported = fun vars => { name = import "/etc/passwd" },
}
"#;
        let vars = Map::new();
        let err = render(importing, &vars).unwrap_err();
        assert!(err.to_string().contains("imports are not allowed"), "{err}");
        // Rejected at definition time too, not only at render time.
        let err = parse_manifest(importing).unwrap_err();
        assert!(err.to_string().contains("imports are not allowed"), "{err}");
    }

    #[test]
    fn validates_template_names() {
        assert!(validate_template_name("render-video").is_ok());
        assert!(validate_template_name("").is_err());
        assert!(validate_template_name("-leading").is_err());
        assert!(validate_template_name("trailing-").is_err());
        assert!(validate_template_name("No-Caps").is_err());
        assert!(validate_template_name(&"x".repeat(200)).is_err());
    }
}
