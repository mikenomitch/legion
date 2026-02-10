use crate::prelude::*;
use crate::task_plugins::{ContainerTaskPlugin, ExecTaskPlugin, TaskPlugin};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct PluginSchemaDoc {
    pub name: String,
    pub schema: Value,
}

#[derive(Debug, Clone)]
struct PropertyDoc {
    path: String,
    type_name: String,
    required: bool,
    description: String,
    default: Option<Value>,
}

pub fn builtin_task_plugin_schemas() -> Vec<PluginSchemaDoc> {
    let mut schemas = vec![
        PluginSchemaDoc {
            name: "container".to_string(),
            schema: ContainerTaskPlugin.schema(),
        },
        PluginSchemaDoc {
            name: "exec".to_string(),
            schema: ExecTaskPlugin.schema(),
        },
    ];
    schemas.sort_by(|left, right| left.name.cmp(&right.name));
    schemas
}

pub fn render_builtin_task_plugin_schemas_markdown() -> R<String> {
    let schemas = builtin_task_plugin_schemas();
    let mut output = String::new();
    output.push_str("# Task Plugin Config Schemas\n\n");
    output.push_str(
        "Generated from typed Rust plugin config structs and their derived JSON Schema.\n\n",
    );

    for schema_doc in schemas {
        output.push_str(&format!("## `{}`\n\n", schema_doc.name));
        output.push_str("| Field | Type | Required | Default | Description |\n");
        output.push_str("| --- | --- | --- | --- | --- |\n");

        let properties = collect_properties(&schema_doc.schema)?;
        for property in properties {
            let default = property
                .default
                .as_ref()
                .map(format_json_value)
                .unwrap_or_else(|| "-".to_string());
            let required = if property.required { "yes" } else { "no" };
            let description = if property.description.trim().is_empty() {
                "-"
            } else {
                &property.description
            };
            output.push_str(&format!(
                "| `{}` | `{}` | {} | `{}` | {} |\n",
                property.path,
                property.type_name,
                required,
                markdown_escape(&default),
                markdown_escape(description),
            ));
        }
        output.push('\n');
    }

    Ok(output)
}

pub fn validate_builtin_task_plugin_schemas() -> R<()> {
    for schema_doc in builtin_task_plugin_schemas() {
        let properties = collect_properties(&schema_doc.schema)?;
        if properties.is_empty() {
            bail!(
                "task plugin schema {} did not expose any properties",
                schema_doc.name
            );
        }

        let property_map = properties
            .iter()
            .map(|property| (property.path.clone(), property))
            .collect::<BTreeMap<_, _>>();

        for property in &properties {
            if property.description.trim().is_empty() {
                bail!(
                    "task plugin schema {} is missing description for {}",
                    schema_doc.name,
                    property.path
                );
            }
        }

        for (path, expected_default) in expected_defaults(&schema_doc.name) {
            let property = property_map.get(path).with_context(|| {
                format!(
                    "task plugin schema {} missing expected property {}",
                    schema_doc.name, path
                )
            })?;
            let actual_default = property.default.as_ref().with_context(|| {
                format!(
                    "task plugin schema {} property {} is missing a default",
                    schema_doc.name, path
                )
            })?;
            if actual_default != &expected_default {
                bail!(
                    "task plugin schema {} property {} default mismatch: expected {}, got {}",
                    schema_doc.name,
                    path,
                    format_json_value(&expected_default),
                    format_json_value(actual_default)
                );
            }
        }
    }

    Ok(())
}

fn expected_defaults(plugin_name: &str) -> BTreeMap<&'static str, Value> {
    match plugin_name {
        "exec" => BTreeMap::from([
            ("env", json!({})),
            ("landlock.read_only_paths", json!([])),
            ("landlock.read_write_paths", json!([])),
        ]),
        "container" => BTreeMap::from([("env", json!({}))]),
        _ => BTreeMap::new(),
    }
}

fn collect_properties(root_schema: &Value) -> R<Vec<PropertyDoc>> {
    let Some(root_object) = find_object_schema(root_schema, root_schema)? else {
        bail!("schema root does not define an object");
    };

    let mut properties = BTreeMap::<String, PropertyDoc>::new();
    collect_properties_inner(root_schema, root_object, "", &mut properties)?;
    Ok(properties.into_values().collect())
}

fn collect_properties_inner(
    root_schema: &Value,
    object_schema: &Value,
    prefix: &str,
    properties: &mut BTreeMap<String, PropertyDoc>,
) -> R<()> {
    let object_schema = resolve_schema(root_schema, object_schema)?;
    let required = object_schema
        .get("required")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();

    let Some(object_properties) = object_schema.get("properties").and_then(Value::as_object) else {
        return Ok(());
    };

    for (name, property_schema) in object_properties {
        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}.{name}")
        };
        let resolved_property = resolve_schema(root_schema, property_schema)?;
        let description = resolved_property
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let type_name = describe_type(root_schema, property_schema)?;
        let default = resolved_property.get("default").cloned();

        properties.entry(path.clone()).or_insert(PropertyDoc {
            path: path.clone(),
            type_name,
            required: required.contains(name.as_str()),
            description,
            default,
        });

        if let Some(nested) = find_object_schema(root_schema, property_schema)? {
            collect_properties_inner(root_schema, nested, &path, properties)?;
        }
    }

    Ok(())
}

fn find_object_schema<'a>(root_schema: &'a Value, schema: &'a Value) -> R<Option<&'a Value>> {
    let resolved = resolve_schema(root_schema, schema)?;
    if resolved
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "object")
        || resolved
            .get("properties")
            .and_then(Value::as_object)
            .is_some()
    {
        return Ok(Some(resolved));
    }

    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(options) = resolved.get(key).and_then(Value::as_array) {
            for option in options {
                if let Some(object_schema) = find_object_schema(root_schema, option)? {
                    return Ok(Some(object_schema));
                }
            }
        }
    }

    Ok(None)
}

fn resolve_schema<'a>(root_schema: &'a Value, schema: &'a Value) -> R<&'a Value> {
    let mut current = schema;
    let mut hops = 0_u8;
    while let Some(reference) = current.get("$ref").and_then(Value::as_str) {
        if !reference.starts_with("#/") {
            bail!("unsupported non-local schema reference {}", reference);
        }

        let pointer = &reference[1..];
        current = root_schema
            .pointer(pointer)
            .with_context(|| format!("failed to resolve schema reference {}", reference))?;
        hops = hops.saturating_add(1);
        if hops > 32 {
            bail!(
                "schema reference depth exceeded while resolving {}",
                reference
            );
        }
    }
    Ok(current)
}

fn describe_type(root_schema: &Value, schema: &Value) -> R<String> {
    let resolved = resolve_schema(root_schema, schema)?;
    if let Some(kind) = resolved.get("type") {
        if let Some(kind) = kind.as_str() {
            return Ok(kind.to_string());
        }
        if let Some(kinds) = kind.as_array() {
            let labels = kinds
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>();
            if !labels.is_empty() {
                return Ok(labels.join(" | "));
            }
        }
    }

    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(options) = resolved.get(key).and_then(Value::as_array) {
            let mut labels = BTreeSet::<String>::new();
            for option in options {
                labels.insert(describe_type(root_schema, option)?);
            }
            if !labels.is_empty() {
                return Ok(labels.into_iter().collect::<Vec<_>>().join(" | "));
            }
        }
    }

    if resolved
        .get("properties")
        .and_then(Value::as_object)
        .is_some()
    {
        return Ok("object".to_string());
    }
    if resolved.get("items").is_some() {
        return Ok("array".to_string());
    }
    if resolved.get("enum").and_then(Value::as_array).is_some() {
        return Ok("enum".to_string());
    }

    Ok("unknown".to_string())
}

fn markdown_escape(input: &str) -> String {
    input.replace('|', "\\|").replace('\n', " ")
}

fn format_json_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}
