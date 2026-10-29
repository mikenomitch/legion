//! `legion object`: objects of the kinds installed apps declare
//! (docs/app-objects.md).
//!
//! ```console
//! $ legion object kinds
//! $ legion object explain gitops/Repo.spec.branch
//! $ legion object apply -f repo.ncl --wait
//! $ legion object get repo storefront -n team-a
//! $ legion object list repos -A
//! $ legion object delete repo storefront -n team-a
//! ```

use std::path::Path;
use std::time::{Duration, Instant};

use crate::cmd::helpers::{
    active_namespace, connect_manager_client, json_flag, list_namespace_flags,
    lists_many_namespaces, namespace_flag, namespace_selection, AuthedManagerClient,
};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::app_object::{
    column_cell, object_document, parse_label_filter, parse_object_documents, same_noun,
    summary_settled,
};
use crate::structs::{
    AppObject, ApplyObjectReply, ApplyObjectRequest, DeleteObjectRequest, GetObjectRequest,
    ListObjectKindsRequest, ListObjectsRequest, ObjectKind,
};

/// How long `--wait` waits by default.
const WAIT_DEFAULT: Duration = Duration::from_secs(120);

fn kind_arg() -> Arg {
    Arg::new("kind").required(true).value_name("KIND").help(
        "The kind: <app>/<Kind> (gitops/Repo), or a short name exactly one kind has (repo, repos)",
    )
}

fn name_arg() -> Arg {
    Arg::new("name")
        .required(true)
        .value_name("NAME")
        .help("The object's name, or namespace/name")
}

fn wait_args() -> [Arg; 2] {
    [
        Arg::new("wait")
            .long("wait")
            .action(ArgAction::SetTrue)
            .help("Wait until the object's app has reconciled it (apply) or released it (delete)"),
        Arg::new("timeout")
            .long("timeout")
            .value_name("DURATION")
            .requires("wait")
            .help("How long --wait waits (default 2m)"),
    ]
}

fn expected_version_arg() -> Arg {
    Arg::new("expected-version")
        .long("expected-version")
        .value_name("N")
        .value_parser(clap::value_parser!(u64))
        .help("Only if the object is at this spec version (0: only if it does not exist)")
}

fn force_arg(what: &'static str) -> Arg {
    Arg::new("force")
        .long("force")
        .action(ArgAction::SetTrue)
        .help(what)
}

/// `legion object`.
pub fn config() -> Command {
    Command::new("object")
        .about("Objects of the kinds installed apps declare (docs/app-objects.md)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("kinds")
                .about("List object kinds: each kind's app, scope, access, requirements, and state")
                .arg(
                    Arg::new("app")
                        .long("app")
                        .value_name("APP")
                        .help("Only this app's kinds"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("explain")
                .about("Describe a kind, or one of its fields: type, default, required, immutable, write-only, examples")
                .arg(
                    Arg::new("target")
                        .required(true)
                        .value_name("KIND[.FIELD]")
                        .help("A kind, or a field path below it: gitops/Repo, repo.spec.branch"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("apply")
                .about("Create or update objects from a Nickel or JSON file (one object or a list)")
                .long_about(
                    "Each object is { kind = \"<app>/<Kind>\", name, namespace?, labels?, spec }. \
                     The manager checks the spec against the kind's schema (unknown fields are \
                     errors), what writing it requires of you, and asks the app (advice only). \
                     --dry-run shows the field diff and writes nothing.",
                )
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .required(true)
                        .value_name("FILE")
                        .value_hint(ValueHint::FilePath)
                        .help("The object file (.ncl or .json)"),
                )
                .arg(namespace_flag())
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("wait")
                        .help("Validate and show what would change, without writing"),
                )
                .arg(expected_version_arg())
                .arg(force_arg("Change objects an app manages"))
                .args(wait_args())
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one object: spec, status, summary, and who wrote it")
                .arg(kind_arg())
                .arg(name_arg())
                .arg(namespace_flag())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("list")
                .about("List objects of a kind (or of every kind) with their status and the kind's columns")
                .arg(
                    Arg::new("kind")
                        .value_name("KIND")
                        .help("The kind; omit for every kind"),
                )
                .args(list_namespace_flags())
                .arg(
                    Arg::new("label")
                        .short('l')
                        .long("label")
                        .value_name("KEY=VALUE")
                        .action(ArgAction::Append)
                        .help("Only objects with this label (repeatable, or comma-separated)"),
                )
                .arg(
                    Arg::new("authority-lapsed")
                        .long("authority-lapsed")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Only objects whose recorded authorizer no longer holds every \
                             permission it was checked for (lapsed), or cannot be re-resolved \
                             (unknown); needs acl:read. Re-applying an object re-authorizes it",
                        ),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete an object; a kind with a cleanup window lets its app release it first")
                .arg(kind_arg())
                .arg(name_arg())
                .arg(namespace_flag())
                .arg(expected_version_arg())
                .arg(
                    Arg::new("now")
                        .long("now")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("wait")
                        .help("Skip the kind's cleanup window: remove the object at once"),
                )
                .arg(force_arg("Delete an object an app manages"))
                .args(wait_args())
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
}

/// Handle `legion object ...`.
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("kinds", sub)) => kinds(sub).await,
        Some(("explain", sub)) => explain(sub).await,
        Some(("apply", sub)) => apply(sub).await,
        Some(("get", sub)) => get(sub).await,
        Some(("list", sub)) => list(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        _ => unreachable!(),
    }
}

fn opt(matches: &ArgMatches, key: &str) -> Option<String> {
    matches.try_get_one::<String>(key).ok().flatten().cloned()
}

fn wait_timeout(format: OutputFormat, matches: &ArgMatches) -> R<Duration> {
    match opt(matches, "timeout") {
        None => Ok(WAIT_DEFAULT),
        Some(text) => humantime::parse_duration(text.trim()).map_err(|err| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("--timeout {text:?}: {err}"),
                    Some("write it like 2m"),
                ),
            )
        }),
    }
}

/// How long ago `at_ms` was, briefly: `12s`, `5m`, `3h`, `2d`.
fn age(at_ms: i64) -> String {
    if at_ms <= 0 {
        return "-".to_string();
    }
    let seconds = ((chrono::Utc::now().timestamp_millis() - at_ms) / 1000).max(0);
    match seconds {
        0..=119 => format!("{seconds}s"),
        120..=7199 => format!("{}m", seconds / 60),
        7200..=172_799 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// The summary with its detail: `NotReady: RepoUnreachable (no route)`.
pub fn summary_line(object: &AppObject) -> String {
    if object.summary_detail.is_empty() {
        object.summary.clone()
    } else {
        format!("{} ({})", object.summary, object.summary_detail)
    }
}

/// The document a kind's columns point into, for one object.
fn column_doc(object: &AppObject) -> Value {
    let effective: Value = serde_json::from_str(&object.effective_json).unwrap_or(Value::Null);
    let status = match &object.status {
        Some(status) => json!({
            "observedGeneration": status.observed_generation,
            "conditions": status.conditions.iter().map(|c| json!({
                "type": c.r#type, "status": c.status, "reason": c.reason, "message": c.message,
            })).collect::<Vec<_>>(),
            "fields": serde_json::from_str::<Value>(&status.fields_json).unwrap_or(Value::Null),
        }),
        None => Value::Null,
    };
    let labels: std::collections::BTreeMap<String, String> = object
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    object_document(
        &object.name,
        &object.namespace,
        &labels,
        &effective,
        &status,
    )
}

/// Wait until `kind`/`name`'s app has reconciled `generation`: its summary
/// settles (`Ready` or `Accepted` succeeds; `Rejected`, `NotReady`,
/// `Invalid`, `Orphaned` fail). Shared with the MCP apply tool.
pub async fn wait_for_ready(
    client: &mut AuthedManagerClient,
    kind: &str,
    namespace: &str,
    name: &str,
    generation: u64,
    timeout: Duration,
) -> Result<AppObject, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let reply = client
            .get_object(GetObjectRequest {
                kind: kind.to_string(),
                name: name.to_string(),
                namespace: namespace.to_string(),
            })
            .await
            .map_err(|status| status.message().to_string())?
            .into_inner();
        if let Some(object) = reply.object {
            let caught_up = object
                .status
                .as_ref()
                .is_some_and(|status| status.observed_generation >= generation);
            match summary_settled(&object.summary) {
                Some(true) if caught_up => return Ok(object),
                Some(false)
                    if caught_up || object.summary == "Invalid" || object.summary == "Orphaned" =>
                {
                    return Err(format!(
                        "{} {}: {}",
                        object.qualified_kind,
                        object.id,
                        summary_line(&object)
                    ))
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{} {} is still {} after {}s (generation {generation}, observed {}); the \
                     object is stored, and its app has not reported on it yet",
                    object.qualified_kind,
                    object.id,
                    summary_line(&object),
                    timeout.as_secs(),
                    object
                        .status
                        .as_ref()
                        .map(|status| status.observed_generation.to_string())
                        .unwrap_or_else(|| "none".to_string())
                ));
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until `kind`/`name` is gone (its cleanup released or expired).
async fn wait_for_gone(
    client: &mut AuthedManagerClient,
    kind: &str,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .get_object(GetObjectRequest {
                kind: kind.to_string(),
                name: name.to_string(),
                namespace: namespace.to_string(),
            })
            .await
        {
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(()),
            Err(status) => return Err(status.message().to_string()),
            Ok(_) if Instant::now() >= deadline => {
                return Err(format!(
                    "{kind} {name} is still being deleted after {}s; its app has not released it, \
                     and its cleanup deadline removes it at the latest",
                    timeout.as_secs()
                ))
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

/// `legion object kinds`.
async fn kinds(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list object kinds",
        client
            .list_object_kinds(ListObjectKindsRequest {
                app: opt(matches, "app").unwrap_or_default(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    if reply.kinds.is_empty() {
        println!("no object kinds (an app declares them in its manifest's `kinds`)");
        return Ok(());
    }
    #[derive(tabled::Tabled)]
    struct Row {
        #[tabled(rename = "KIND")]
        kind: String,
        #[tabled(rename = "SCOPE")]
        scope: String,
        #[tabled(rename = "ACCESS")]
        access: String,
        #[tabled(rename = "STATE")]
        state: String,
        #[tabled(rename = "DESCRIPTION")]
        description: String,
    }
    crate::cmd::helpers::print_table(
        reply
            .kinds
            .iter()
            .map(|kind| Row {
                kind: kind.qualified.clone(),
                scope: kind.scope.clone(),
                access: if kind.requires.is_empty() {
                    kind.access.clone()
                } else {
                    format!("{} (+ {})", kind.access, kind.requires.join("; "))
                },
                state: kind.state.clone(),
                description: kind.description.clone(),
            })
            .collect(),
    );
    Ok(())
}

/// The kind a typed name means among `kinds`: qualified, or a short name
/// exactly one has.
fn pick_kind<'a>(kinds: &'a [ObjectKind], typed: &str) -> Result<&'a ObjectKind, String> {
    let typed = typed.trim();
    if let Some((app, kind)) = typed.split_once('/') {
        return kinds
            .iter()
            .find(|found| found.app == app && same_noun(kind, &found.name))
            .ok_or_else(|| format!("unknown kind {typed:?} (`legion object kinds` lists them)"));
    }
    let matches: Vec<&ObjectKind> = kinds
        .iter()
        .filter(|found| same_noun(typed, &found.name))
        .collect();
    match matches.len() {
        0 => Err(format!(
            "unknown kind {typed:?} (`legion object kinds` lists them)"
        )),
        1 => Ok(matches[0]),
        _ => Err(format!(
            "{typed:?} names {} kinds: {}; name one",
            matches.len(),
            matches
                .iter()
                .map(|kind| kind.qualified.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// A field's description, from its schema.
fn describe_field(
    path: &str,
    schema: &Value,
    required: bool,
    immutable: bool,
    lines: &mut Vec<String>,
) {
    let kind = schema
        .get("type")
        .map(|value| match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        })
        .unwrap_or_else(|| "any".to_string());
    let mut head = format!("{path}: {kind}");
    if required {
        head.push_str(", required");
    }
    if let Some(default) = schema.get("default") {
        head.push_str(&format!(", default {default}"));
    }
    if immutable {
        head.push_str(", immutable");
    }
    if schema.get("writeOnly").and_then(Value::as_bool) == Some(true) {
        head.push_str(", write-only (redacted on reads)");
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        head.push_str(&format!(
            ", one of {}",
            values
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.push(head);
    if let Some(description) = schema.get("description").and_then(Value::as_str) {
        lines.push(format!("  {description}"));
    }
}

/// The fields one level below `schema`, described.
fn describe_properties(
    prefix: &str,
    schema: &Value,
    immutable: &[String],
    lines: &mut Vec<String>,
) {
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, child) in properties {
            let path = format!("{prefix}.{name}");
            let pointer = format!("/{}", path.replace('.', "/"));
            describe_field(
                &path,
                child,
                required.contains(&name.as_str()),
                immutable.contains(&pointer),
                lines,
            );
        }
    }
}

/// `legion object explain`.
async fn explain(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let target = matches
        .get_one::<String>("target")
        .expect("required")
        .trim()
        .to_string();
    let (kind_part, field) = match target.split_once('.') {
        Some((kind, field)) => (kind.to_string(), Some(field.to_string())),
        None => (target.clone(), None),
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read object kinds",
        client
            .list_object_kinds(ListObjectKindsRequest::default())
            .await,
    )?
    .into_inner();
    let kind = pick_kind(&reply.kinds, &kind_part).map_err(|message| {
        output::fail_local(format, CliError::local("InvalidArgument", message, None))
    })?;
    let schema: Value = serde_json::from_str(&kind.schema_json).unwrap_or(Value::Null);
    let status_schema: Value =
        serde_json::from_str(&kind.status_schema_json).unwrap_or(Value::Null);
    let document = json!({
        "type": "object",
        "properties": { "spec": schema.clone(), "status": { "type": "object", "properties": { "fields": status_schema.clone() } } },
    });
    let mut node = &document;
    let mut parent_required = false;
    if let Some(field) = &field {
        for segment in field.split('.') {
            let next = node
                .get("properties")
                .and_then(|properties| properties.get(segment))
                .or_else(|| {
                    node.get("items")
                        .and_then(|items| items.get("properties"))
                        .and_then(|p| p.get(segment))
                });
            parent_required = node
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(segment)));
            node = next.ok_or_else(|| {
                output::fail_local(
                    format,
                    CliError::local(
                        "InvalidArgument",
                        format!("{} has no field {field}", kind.qualified),
                        Some("`legion object explain <kind>` lists its fields"),
                    ),
                )
            })?;
            if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
                if let Some(target) = reference
                    .strip_prefix('#')
                    .and_then(|pointer| schema.pointer(pointer))
                {
                    node = target;
                }
            }
        }
    }
    if format.is_json() {
        output::print_success(&json!({
            "kind": kind,
            "field": field,
            "schema": node,
        }));
        return Ok(());
    }
    let mut lines = Vec::new();
    match &field {
        None => {
            lines.push(format!("{}: {}", kind.qualified, kind.description));
            lines.push(format!(
                "Scope: {}. Access: {}{}.",
                kind.scope,
                kind.access,
                if kind.requires.is_empty() {
                    String::new()
                } else {
                    format!("; writing one also needs {}", kind.requires.join(", "))
                }
            ));
            if !kind.cleanup.is_empty() {
                lines.push(format!(
                    "Deleting waits up to {} for the app to release an object.",
                    kind.cleanup
                ));
            }
            lines.push(format!("At most {} objects.", kind.max_objects));
            if kind.state != "active" {
                lines.push(format!(
                    "Orphaned: app {} no longer declares it; its objects are read-only.",
                    kind.app
                ));
            }
            lines.push("Fields:".to_string());
            describe_properties("spec", &schema, &kind.immutable, &mut lines);
            if !status_schema.is_null() {
                lines.push("Status fields (reported by the app):".to_string());
                describe_properties("status.fields", &status_schema, &[], &mut lines);
            }
            let examples: Vec<Value> =
                serde_json::from_str(&kind.examples_json).unwrap_or_default();
            if !examples.is_empty() {
                lines.push("Examples:".to_string());
                for example in examples {
                    lines.push(format!("  spec = {example}"));
                }
            }
        }
        Some(field) => {
            let pointer = format!("/{}", field.replace('.', "/"));
            describe_field(
                &format!("{}.{field}", kind.qualified),
                node,
                parent_required,
                kind.immutable.contains(&pointer),
                &mut lines,
            );
            if node.get("properties").is_some() {
                describe_properties(field, node, &kind.immutable, &mut lines);
            }
        }
    }
    for line in lines {
        println!("{line}");
    }
    Ok(())
}

/// `legion object apply`.
async fn apply(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = Path::new(matches.get_one::<String>("file").expect("required"));
    let text = crate::config_lang::eval_file(path).map_err(|err| {
        output::fail_local(
            format,
            CliError::local(
                "invalid_object_file",
                format!("failed to evaluate {}: {err:#}", path.display()),
                None,
            ),
        )
    })?;
    let documents = serde_json::from_str::<Value>(&text)
        .map_err(|err| err.to_string())
        .and_then(|value| parse_object_documents(&value))
        .map_err(|err| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("{}: {err}", path.display()),
                    Some("a file holds { kind = \"<app>/<Kind>\", name, spec } or a list of them"),
                ),
            )
        })?;
    let expected = matches.get_one::<u64>("expected-version").copied();
    if expected.is_some() && documents.len() > 1 {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "--expected-version applies to a file with one object",
                None,
            ),
        ));
    }
    let wait = matches.get_flag("wait");
    let timeout = wait_timeout(format, matches)?;
    let key = opt(matches, "idempotency-key");
    let mut client = connect_manager_client().await?;
    let mut replies: Vec<ApplyObjectReply> = Vec::new();
    for (index, document) in documents.iter().enumerate() {
        let reply = output::resolve(
            format,
            &format!(
                "failed to apply object {} of {}",
                index + 1,
                documents.len()
            ),
            client
                .apply_object(ApplyObjectRequest {
                    object_json: document.to_string(),
                    namespace: active_namespace(matches).unwrap_or_default(),
                    dry_run: matches.get_flag("dry-run"),
                    expected_version: expected,
                    idempotency_key: key.as_ref().map(|key| {
                        if documents.len() == 1 {
                            key.clone()
                        } else {
                            format!("{key}/{index}")
                        }
                    }),
                    force: matches.get_flag("force"),
                })
                .await,
        )?
        .into_inner();
        if !format.is_json() {
            println!("{}", reply.message);
            for line in &reply.diff {
                println!("  {line}");
            }
            for warning in &reply.warnings {
                eprintln!("warning: {warning}");
            }
        }
        replies.push(reply);
    }
    let mut waited = Vec::new();
    let mut failures = Vec::new();
    if wait {
        for reply in &replies {
            let Some(object) = &reply.object else {
                continue;
            };
            match wait_for_ready(
                &mut client,
                &object.qualified_kind,
                &object.namespace,
                &object.name,
                object.generation,
                timeout,
            )
            .await
            {
                Ok(object) => {
                    if !format.is_json() {
                        println!(
                            "{} {}: {}",
                            object.qualified_kind,
                            object.id,
                            summary_line(&object)
                        );
                    }
                    waited.push(object);
                }
                Err(err) => failures.push(err),
            }
        }
    }
    if !failures.is_empty() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "FailedPrecondition",
                format!("the objects were stored, and: {}", failures.join("; ")),
                Some("legion object get shows each object's status"),
            ),
        ));
    }
    if format.is_json() {
        output::print_success(&json!({ "objects": replies, "waited": waited }));
    }
    Ok(())
}

/// `legion object get`.
async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to get the object",
        client
            .get_object(GetObjectRequest {
                kind: matches.get_one::<String>("kind").expect("required").clone(),
                name: matches.get_one::<String>("name").expect("required").clone(),
                namespace: active_namespace(matches).unwrap_or_default(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    let Some(object) = reply.object else {
        return Ok(());
    };
    println!(
        "{} {} ({})  {}",
        object.qualified_kind,
        object.id,
        object.state,
        summary_line(&object)
    );
    println!(
        "uid {}  generation {}{}  spec version {}",
        object.uid,
        object.generation,
        object
            .status
            .as_ref()
            .map(|status| format!(" (observed {})", status.observed_generation))
            .unwrap_or_default(),
        object.spec_version
    );
    if !object.detail.is_empty() {
        println!("detail: {}", object.detail);
    }
    if !object.labels.is_empty() {
        let labels: Vec<String> = object
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        println!("labels: {}", labels.join(", "));
    }
    if !object.managed_by.is_empty() {
        println!(
            "managed by app {} (changing it needs --force)",
            object.managed_by
        );
    }
    println!(
        "created by {} {} ago; updated by {} {} ago",
        object.created_by,
        age(object.created_at_unix_ms),
        object.updated_by,
        age(object.updated_at_unix_ms)
    );
    if object.deleting {
        println!(
            "deleting: since {} ago; its app releases it, or it is removed at its cleanup deadline",
            age(object.deleting_since_unix_ms)
        );
    }
    if let Some(authority) = object
        .authority
        .as_ref()
        .filter(|a| !a.permissions.is_empty())
    {
        println!(
            "authority (checked against {}): {}",
            authority.authorized_by,
            authority.permissions.join("; ")
        );
    }
    println!("spec:");
    let effective: Value = serde_json::from_str(&object.effective_json).unwrap_or(Value::Null);
    for line in serde_json::to_string_pretty(&effective)
        .unwrap_or_default()
        .lines()
    {
        println!("  {line}");
    }
    match &object.status {
        None => println!("status: none reported yet"),
        Some(status) => {
            println!(
                "status (reported by {} {} ago):",
                status.updated_by,
                age(status.updated_at_unix_ms)
            );
            for condition in &status.conditions {
                println!(
                    "  {}={}{}{}",
                    condition.r#type,
                    condition.status,
                    if condition.reason.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", condition.reason)
                    },
                    if condition.message.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", condition.message)
                    }
                );
            }
            let fields: Value = serde_json::from_str(&status.fields_json).unwrap_or(Value::Null);
            if fields.as_object().is_some_and(|map| !map.is_empty()) {
                println!("  fields: {fields}");
            }
        }
    }
    Ok(())
}

/// `legion object list`.
async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let labels = matches
        .get_many::<String>("label")
        .map(|values| values.cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let labels = parse_label_filter(&labels)
        .map_err(|err| output::fail_local(format, CliError::local("InvalidArgument", err, None)))?;
    let kind = opt(matches, "kind").unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list objects",
        client
            .list_objects(ListObjectsRequest {
                kind: kind.clone(),
                namespaces: namespace_selection(matches),
                labels: labels.into_iter().collect(),
                authority_lapsed: matches.get_flag("authority-lapsed"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    let lapsed = matches.get_flag("authority-lapsed");
    if reply.objects.is_empty() {
        println!(
            "{}",
            if lapsed {
                "no objects with lapsed authority"
            } else {
                "no objects"
            }
        );
        return Ok(());
    }
    let many_kinds = kind.is_empty();
    let columns = if many_kinds {
        Vec::new()
    } else {
        reply
            .kinds
            .first()
            .map(|kind| kind.columns.clone())
            .unwrap_or_default()
    };
    let many_namespaces = lists_many_namespaces(matches);
    let mut header = Vec::new();
    if many_kinds {
        header.push("KIND".to_string());
    }
    header.push("NAME".to_string());
    if many_namespaces || many_kinds {
        header.push("NAMESPACE".to_string());
    }
    header.extend(["STATUS".to_string(), "AGE".to_string()]);
    if lapsed {
        header.extend(["AUTHORITY".to_string(), "AUTHORIZED BY".to_string()]);
    }
    header.extend(columns.iter().map(|column| column.name.clone()));
    let mut builder = tabled::builder::Builder::default();
    builder.push_record(header);
    for object in &reply.objects {
        let doc = column_doc(object);
        let mut row = Vec::new();
        if many_kinds {
            row.push(object.qualified_kind.clone());
        }
        row.push(object.name.clone());
        if many_namespaces || many_kinds {
            row.push(if object.namespace.is_empty() {
                "-".to_string()
            } else {
                object.namespace.clone()
            });
        }
        row.push(object.summary.clone());
        row.push(age(object.created_at_unix_ms));
        if lapsed {
            let authority = object.authority.clone().unwrap_or_default();
            row.push(if authority.check_detail.is_empty() {
                authority.check
            } else {
                format!("{}: {}", authority.check, authority.check_detail)
            });
            row.push(authority.authorized_by);
        }
        row.extend(
            columns
                .iter()
                .map(|column| column_cell(&doc, &column.pointer)),
        );
        builder.push_record(row);
    }
    let mut table = builder.build();
    table.with(tabled::settings::Style::blank());
    println!("{table}");
    Ok(())
}

/// `legion object delete`.
async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let kind = matches.get_one::<String>("kind").expect("required").clone();
    let name = matches.get_one::<String>("name").expect("required").clone();
    let namespace = active_namespace(matches).unwrap_or_default();
    let timeout = wait_timeout(format, matches)?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to delete the object",
        client
            .delete_object(DeleteObjectRequest {
                kind: kind.clone(),
                name: name.clone(),
                namespace: namespace.clone(),
                expected_version: matches.get_one::<u64>("expected-version").copied(),
                idempotency_key: opt(matches, "idempotency-key"),
                now: matches.get_flag("now"),
                force: matches.get_flag("force"),
            })
            .await,
    )?
    .into_inner();
    if !format.is_json() {
        println!("{}", reply.message);
    }
    if matches.get_flag("wait") && reply.deleting {
        let (kind, namespace, name) = match &reply.object {
            Some(object) => (
                object.qualified_kind.clone(),
                object.namespace.clone(),
                object.name.clone(),
            ),
            None => (kind, namespace, name),
        };
        if let Err(err) = wait_for_gone(&mut client, &kind, &namespace, &name, timeout).await {
            return Err(output::fail_local(
                format,
                CliError::local("DeadlineExceeded", err, None),
            ));
        }
        if !format.is_json() {
            println!("{kind} {name}: released and deleted");
        }
    }
    if format.is_json() {
        output::print_success(&reply);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_parse_test() {
        let command = config();
        command.clone().debug_assert();
        let apply = command
            .clone()
            .try_get_matches_from([
                "object",
                "apply",
                "-f",
                "r.ncl",
                "--wait",
                "--timeout",
                "1m",
            ])
            .expect("apply");
        let (_, apply) = apply.subcommand().expect("sub");
        assert!(apply.get_flag("wait"));
        assert!(command
            .clone()
            .try_get_matches_from(["object", "apply", "-f", "r.ncl", "--dry-run", "--wait"])
            .is_err());
        assert!(command
            .clone()
            .try_get_matches_from(["object", "delete", "repo", "web", "--now", "--wait"])
            .is_err());
        let list = command
            .try_get_matches_from(["object", "list", "repos", "-A", "-l", "team=a"])
            .expect("list");
        let (_, list) = list.subcommand().expect("sub");
        assert_eq!(namespace_selection(list), vec!["*".to_string()]);
    }

    #[test]
    fn kinds_are_picked_by_qualified_or_unique_short_name_test() {
        let kind = |app: &str, name: &str| ObjectKind {
            app: app.into(),
            name: name.into(),
            qualified: format!("{app}/{name}"),
            ..Default::default()
        };
        let kinds = vec![
            kind("gitops", "Repo"),
            kind("reposync", "Repo"),
            kind("certs", "Issuer"),
        ];
        assert_eq!(
            pick_kind(&kinds, "issuers").expect("short").qualified,
            "certs/Issuer"
        );
        assert_eq!(
            pick_kind(&kinds, "gitops/repos")
                .expect("qualified")
                .qualified,
            "gitops/Repo"
        );
        assert!(pick_kind(&kinds, "repo")
            .expect_err("ambiguous")
            .contains("names 2 kinds"));
        assert!(pick_kind(&kinds, "nope").is_err());
    }
}
