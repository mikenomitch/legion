//! `legion net plugin` — WASM network plugins on the proxy-wasm ABI
//! (docs/network-plugins.md).
//!
//! `new` scaffolds a plugin crate and its object file; `test` runs a module
//! (a local `.wasm`, or a stored plugin's) against one synthetic request
//! without a cluster; `apply`, `list`, `describe`, `status`, and `delete`
//! manage stored plugin objects. `test` and `apply --dry-run` are read-only.
//! Every verb takes `--json`/`-j` and emits the standard
//! [`crate::cmd::output`] envelope.

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::netplugin::NetPluginObject;
use crate::structs::{
    DeleteNetPluginRequest, GetNetPluginRequest, GetNetPluginStatusRequest, ListNetPluginsRequest,
    NetPlugin, UpsertNetPluginRequest,
};
use crate::worker::netd::plugins::harness::{self, TestInput, TestReport};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tabled::{Table, Tabled};

/// Stages a bare module runs at when `test` has no object to read them from.
const TEST_DEFAULT_STAGES: &str = "request,body,response,done";

pub fn config() -> Command {
    Command::new("plugin")
        .about("WASM network plugins on the proxy-wasm ABI (docs/network-plugins.md)")
        .subcommand_required(true)
        .subcommand(
            Command::new("new")
                .about("Scaffold a plugin crate (the proxy-wasm Rust SDK) and its object file")
                .arg(Arg::new("name").required(true).help("Plugin name"))
                .arg(
                    Arg::new("dir")
                        .long("dir")
                        .help("Where to create it (default ./<name>)")
                        .value_hint(ValueHint::DirPath),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("test")
                .about("Run a module or a stored plugin against one synthetic request, locally")
                .arg(
                    Arg::new("module")
                        .required(true)
                        .help("A .wasm file, or the name of a stored plugin"),
                )
                .arg(
                    Arg::new("request")
                        .long("request")
                        .required(true)
                        .help("'METHOD URL', e.g. 'POST https://api.github.com/repos/x/issues'"),
                )
                .arg(Arg::new("header").long("header").short('H').action(ArgAction::Append).help("Request header k=v (repeatable)"))
                .arg(Arg::new("body").long("body").help("Request body, or @file"))
                .arg(Arg::new("label").long("label").action(ArgAction::Append).help("Calling task label k=v (repeatable)"))
                .arg(Arg::new("job").long("job").default_value("legion-plugin-test").help("Calling job id"))
                .arg(Arg::new("task").long("task").default_value("main").help("Calling task name"))
                .arg(Arg::new("config").long("config").help("The reference's config: JSON, or @file (.json or .ncl)"))
                .arg(
                    Arg::new("object")
                        .long("object")
                        .help("Plugin object file (.ncl or .json) for a .wasm module: its stages and limits")
                        .value_hint(ValueHint::FilePath),
                )
                .arg(
                    Arg::new("stages")
                        .long("stages")
                        .help(format!("Stages for a bare .wasm module (default {TEST_DEFAULT_STAGES})")),
                )
                .arg(Arg::new("response").long("response").default_value("200").help("The upstream's status, for the response stage"))
                .arg(Arg::new("response-body").long("response-body").help("The upstream's body, or @file"))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("apply")
                .about("Create or replace a plugin object from a Nickel or JSON file")
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .required(true)
                        .help("Plugin object file (.ncl or .json)")
                        .value_hint(ValueHint::FilePath),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Validate (and re-check the policies that use it) without writing"),
                )
                .arg(
                    Arg::new("idempotency-key")
                        .long("idempotency-key")
                        .help("Retry-safe key: a repeat returns the first call's result"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("list")
                .about("List stored plugin objects")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("describe")
                .about("Print one stored plugin object and the policies that use it")
                .arg(Arg::new("name").required(true).help("Plugin name"))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("status")
                .about("Where a plugin runs and how it is doing, per node")
                .arg(Arg::new("name").required(true).help("Plugin name"))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a stored plugin (refused while a policy uses it)")
                .arg(Arg::new("name").required(true).help("Plugin name"))
                .arg(json_flag()),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("new", sub)) => new(sub),
        Some(("test", sub)) => test(sub).await,
        Some(("apply", sub)) => apply(sub).await,
        Some(("list", sub)) => list(sub).await,
        Some(("describe", sub)) => describe(sub).await,
        Some(("status", sub)) => status(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        _ => unreachable!(),
    }
}

fn local_error(format: OutputFormat, code: &str, message: impl Into<String>) -> anyhow::Error {
    output::fail_local(format, CliError::local(code, message, None))
}

/// `@file` or the literal value.
fn text_or_file(value: &str) -> R<Vec<u8>> {
    match value.strip_prefix('@') {
        Some(path) => std::fs::read(path).with_context(|| format!("reading {path}")),
        None => Ok(value.as_bytes().to_vec()),
    }
}

fn pairs(
    values: Option<clap::parser::ValuesRef<'_, String>>,
    what: &str,
) -> R<Vec<(String, String)>> {
    values
        .into_iter()
        .flatten()
        .map(|entry| {
            entry
                .split_once('=')
                .or_else(|| entry.split_once(':'))
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .ok_or_else(|| anyhow!("{what} {entry:?} must be key=value"))
        })
        .collect()
}

fn load_object_file(path: &Path) -> R<NetPluginObject> {
    let text = crate::config_lang::eval_file(path)
        .with_context(|| format!("failed to evaluate {}", path.display()))?;
    let mut object: NetPluginObject =
        serde_json::from_str(&text).with_context(|| format!("{}", path.display()))?;
    object.normalize_and_validate()?;
    Ok(object)
}

/// Fetch a stored plugin's module for a local test: `file://` and
/// `http(s)://` sources, checked against the checksum.
async fn fetch_module(object: &NetPluginObject) -> R<Vec<u8>> {
    let source = &object.module.source;
    let bytes = if let Some(path) = source.strip_prefix("file://") {
        std::fs::read(path).with_context(|| format!("reading {path}"))?
    } else if source.starts_with("https://") || source.starts_with("http://") {
        reqwest::get(source)
            .await
            .and_then(|r| r.error_for_status())
            .with_context(|| {
                format!(
                    "fetching {}",
                    crate::structs::artifact::redact_source(source)
                )
            })?
            .bytes()
            .await?
            .to_vec()
    } else {
        bail!(
            "`test` fetches file:// and http(s):// modules itself; download {} and pass the .wasm file",
            crate::structs::artifact::redact_source(source)
        );
    };
    crate::wasm::module::verify_checksum(&bytes, &object.module.checksum)
        .map_err(|err| anyhow!(err))?;
    Ok(bytes)
}

/// The object and module bytes a test runs: a local `.wasm` (with an
/// object file, or stages, or the defaults), or a stored plugin's object and
/// its module, fetched and checked against its checksum.
pub(crate) async fn load_for_test(
    target: &str,
    object_file: Option<&str>,
    stages: Option<&str>,
) -> R<(NetPluginObject, Vec<u8>)> {
    let path = Path::new(target);
    if path.is_file() {
        let module = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        if let Some(file) = object_file {
            return Ok((load_object_file(Path::new(file))?, module));
        }
        let stages: Vec<String> = stages
            .unwrap_or(TEST_DEFAULT_STAGES)
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().replace('_', "-"))
            .unwrap_or_else(|| "plugin".into());
        let mut object: NetPluginObject = serde_json::from_value(json!({
            "name": name,
            "module": {
                "source": format!("file://{}", path.display()),
                "checksum": format!("sha256:{:x}", <sha2::Sha256 as sha2::Digest>::digest(&module)),
            },
            "stages": stages,
        }))?;
        object.normalize_and_validate()?;
        return Ok((object, module));
    }
    let mut client = connect_manager_client().await?;
    let reply = client
        .get_net_plugin(GetNetPluginRequest {
            name: target.to_string(),
        })
        .await
        .map_err(|status| {
            anyhow!(
                "{target} is neither a .wasm file nor a stored plugin: {}",
                status.message()
            )
        })?
        .into_inner();
    let plugin = reply.plugin.unwrap_or_default();
    let object: NetPluginObject = serde_json::from_str(&plugin.object_json)?;
    let module = fetch_module(&object).await?;
    Ok((object, module))
}

async fn test(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let target = matches.get_one::<String>("module").expect("required");
    let request = matches.get_one::<String>("request").expect("required");
    let Some((method, url)) = request.trim().split_once(char::is_whitespace) else {
        return Err(local_error(
            format,
            "invalid_request",
            format!("--request must be 'METHOD URL', got {request:?}"),
        ));
    };
    let (object, module) = match load_for_test(
        target,
        matches.get_one::<String>("object").map(String::as_str),
        matches.get_one::<String>("stages").map(String::as_str),
    )
    .await
    {
        Ok(loaded) => loaded,
        Err(err) => {
            return Err(local_error(
                format,
                "plugin_unavailable",
                format!("{err:#}"),
            ))
        }
    };
    let config = match matches.get_one::<String>("config") {
        None => None,
        Some(value) => {
            let text = match value.strip_prefix('@') {
                Some(file) => crate::config_lang::eval_file(file)?,
                None => value.clone(),
            };
            match serde_json::from_str::<Value>(&text) {
                Ok(config) => Some(config),
                Err(err) => {
                    return Err(local_error(
                        format,
                        "invalid_config",
                        format!("--config: {err}"),
                    ))
                }
            }
        }
    };
    if let Err(err) = object.check_config("--config", config.as_ref()) {
        return Err(local_error(format, "invalid_config", err.to_string()));
    }
    let input = TestInput {
        method: method.to_string(),
        url: url.trim().to_string(),
        headers: pairs(matches.get_many::<String>("header"), "--header")?,
        body: matches
            .get_one::<String>("body")
            .map(|b| text_or_file(b))
            .transpose()?,
        labels: pairs(matches.get_many::<String>("label"), "--label")?
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        job: matches
            .get_one::<String>("job")
            .cloned()
            .unwrap_or_default(),
        task: matches
            .get_one::<String>("task")
            .cloned()
            .unwrap_or_default(),
        config,
        response_status: matches
            .get_one::<String>("response")
            .and_then(|s| s.parse().ok())
            .unwrap_or(200),
        response_body: matches
            .get_one::<String>("response-body")
            .map(|b| text_or_file(b))
            .transpose()?
            .unwrap_or_default(),
    };
    let report = match harness::run(object, &module, input).await {
        Ok(report) => report,
        Err(err) => return Err(local_error(format, "plugin_failed", err)),
    };
    if format.is_json() {
        output::print_success(&report);
        return Ok(());
    }
    print_report(&report);
    Ok(())
}

fn fuel_text(fuel: u64) -> String {
    if fuel >= 1_000_000 {
        format!("{:.1}M fuel", fuel as f64 / 1e6)
    } else if fuel >= 1_000 {
        format!("{:.1}K fuel", fuel as f64 / 1e3)
    } else {
        format!("{fuel} fuel")
    }
}

fn print_report(report: &TestReport) {
    for stage in &report.stages {
        let cost = format!("({} µs, {})", stage.micros, fuel_text(stage.fuel));
        match stage.outcome.as_str() {
            "continue" => println!("✓ {}: continue   {cost}", stage.stage),
            other => {
                let mark = if other == "answered" && stage.status.is_some_and(|s| s < 400) {
                    "✓"
                } else {
                    "✗"
                };
                let verb = if other == "failed" {
                    "failed"
                } else {
                    "answered"
                };
                println!(
                    "{mark} {}: {} {verb} by plugin {}{}   {cost}",
                    stage.stage,
                    stage.status.unwrap_or_default(),
                    report.plugin,
                    stage
                        .reason
                        .as_deref()
                        .map(|r| format!(" — {r}"))
                        .unwrap_or_default()
                );
            }
        }
        for change in &stage.changes {
            println!("    {change}");
        }
    }
    println!(
        "client gets {}{}",
        report.status,
        if report.body.is_empty() {
            String::new()
        } else {
            format!(": {}", report.body)
        }
    );
}

fn new(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches
        .get_one::<String>("name")
        .expect("required")
        .trim()
        .to_string();
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(local_error(
            format,
            "invalid_name",
            "a plugin name is letters, digits, '-' and '_'",
        ));
    }
    let dir = matches
        .get_one::<String>("dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(&name));
    if dir.exists()
        && std::fs::read_dir(&dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(true)
    {
        return Err(local_error(
            format,
            "exists",
            format!("{} already exists and is not empty", dir.display()),
        ));
    }
    let crate_name = name.replace('-', "_");
    let files = scaffold(&name, &crate_name);
    std::fs::create_dir_all(dir.join("src"))?;
    let mut written = Vec::new();
    for (relative, contents) in files {
        let path = dir.join(relative);
        std::fs::write(&path, contents)?;
        written.push(path.display().to_string());
    }
    let wasm = format!("target/wasm32-wasip1/release/{crate_name}.wasm");
    if format.is_json() {
        output::print_success(
            &json!({ "plugin": name, "dir": dir.display().to_string(), "files": written, "module": wasm }),
        );
        return Ok(());
    }
    println!("Created plugin {name} in {}:", dir.display());
    for file in &written {
        println!("  {file}");
    }
    println!();
    println!("Next:");
    println!(
        "  cd {} && cargo build --release --target wasm32-wasip1",
        dir.display()
    );
    println!("  legion net plugin test {wasm} --request 'POST https://api.example.com/x' --label tier=free");
    println!("  # host the .wasm, set module.source and module.checksum (sha256sum {wasm}) in {name}.ncl");
    println!("  legion net plugin apply -f {name}.ncl --dry-run");
    Ok(())
}

/// The files `new` writes.
fn scaffold(name: &str, crate_name: &str) -> Vec<(String, String)> {
    let cargo = format!(
        r#"[package]
name = "{crate_name}"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
proxy-wasm = "0.2.5"

[profile.release]
opt-level = "s"
lto = true
codegen-units = 1
strip = true
panic = "abort"
"#
    );
    let lib = r#"// A Legion network plugin on the proxy-wasm ABI (docs/network-plugins.md).
//
// Facts about the calling task are properties: legion.job, legion.task,
// legion.identity, legion.labels.<key>, legion.destination, legion.upstream,
// legion.direction. Answer with send_http_response to stop a request.
use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_http_context(|_, _| -> Box<dyn HttpContext> { Box::new(Plugin) });
}}

struct Plugin;
impl Context for Plugin {}
impl HttpContext for Plugin {
    fn on_http_request_headers(&mut self, _: usize, _: bool) -> Action {
        let tier = self.get_property(vec!["legion", "labels", "tier"]);
        let method = self.get_http_request_header(":method");
        if tier.as_deref() == Some(b"free".as_slice()) && method.as_deref() != Some("GET") {
            self.send_http_response(403, vec![("legion-denied-reason", "the free tier is read-only")], None);
            return Action::Pause;
        }
        Action::Continue
    }
}
"#
    .to_string();
    let object = format!(
        r#"# legion net plugin apply -f {name}.ncl
{{
    name = "{name}",
    description = "",
    module = {{
        # Where nodes fetch the built module (https://, s3://, or file://),
        # and its sha256 (`sha256sum target/wasm32-wasip1/release/{crate_name}.wasm`).
        source = "https://artifacts.example.com/{name}.wasm",
        checksum = "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    }},
    stages = ["request"],
    usableBy = "policies",
    limits = {{ memory = "16Mi", deadline = "10ms" }},
}}
"#
    );
    vec![
        ("Cargo.toml".to_string(), cargo),
        ("src/lib.rs".to_string(), lib),
        (
            ".gitignore".to_string(),
            "target/\nCargo.lock\n".to_string(),
        ),
        (format!("{name}.ncl"), object),
    ]
}

fn plugin_json(plugin: &NetPlugin) -> Value {
    let object: Value = serde_json::from_str(&plugin.object_json).unwrap_or(Value::Null);
    json!({
        "name": plugin.name,
        "revision": plugin.revision,
        "stages": plugin.stages,
        "usableBy": plugin.usable_by,
        "moduleChecksum": plugin.module_checksum,
        "createdAt": plugin.created_at,
        "updatedAt": plugin.updated_at,
        "referencedBy": plugin.referenced_by,
        "object": object,
    })
}

async fn apply(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = Path::new(matches.get_one::<String>("file").expect("required"));
    let dry_run = matches.get_flag("dry-run");
    // Validate locally first so a bad file fails with the field name before
    // a round trip; the manager validates again.
    let object = match load_object_file(path) {
        Ok(object) => object,
        Err(err) => return Err(local_error(format, "invalid_plugin", format!("{err:#}"))),
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "apply net plugin",
        client
            .upsert_net_plugin(UpsertNetPluginRequest {
                object_json: serde_json::to_string(&object)?,
                dry_run,
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            })
            .await,
    )?
    .into_inner();
    let plugin = reply.plugin.unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({
            "dryRun": dry_run,
            "deduplicated": reply.deduplicated,
            "plugin": plugin_json(&plugin),
        }));
        return Ok(());
    }
    let verb = match (dry_run, reply.deduplicated) {
        (true, _) => "Would apply",
        (false, true) => "Already applied",
        (false, false) => "Applied",
    };
    println!(
        "{verb} plugin {} (stages {}, usable by {}){}",
        plugin.name,
        plugin.stages.join(", "),
        plugin.usable_by,
        if dry_run {
            String::new()
        } else {
            format!(", revision {}", plugin.revision)
        }
    );
    if !plugin.referenced_by.is_empty() {
        println!(
            "Used by policy object(s) {}; their nodes load the new revision on their next heartbeat.",
            plugin.referenced_by.join(", ")
        );
    }
    Ok(())
}

#[derive(Tabled)]
struct PluginRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "STAGES")]
    stages: String,
    #[tabled(rename = "USABLE BY")]
    usable_by: String,
    #[tabled(rename = "REVISION")]
    revision: u64,
    #[tabled(rename = "MODULE")]
    module: String,
    #[tabled(rename = "UPDATED")]
    updated: String,
}

fn short_checksum(checksum: &str) -> String {
    let hex = checksum.strip_prefix("sha256:").unwrap_or(checksum);
    format!("sha256:{}", &hex[..hex.len().min(12)])
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list net plugins",
        client.list_net_plugins(ListNetPluginsRequest {}).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "generation": reply.generation,
            "plugins": reply.plugins.iter().map(plugin_json).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    if reply.plugins.is_empty() {
        println!("No network plugins. Scaffold one with `legion net plugin new <name>`.");
        return Ok(());
    }
    let rows: Vec<PluginRow> = reply
        .plugins
        .iter()
        .map(|p| PluginRow {
            name: p.name.clone(),
            stages: p.stages.join(","),
            usable_by: p.usable_by.clone(),
            revision: p.revision,
            module: short_checksum(&p.module_checksum),
            updated: p.updated_at.clone(),
        })
        .collect();
    println!("{}", Table::new(rows));
    Ok(())
}

async fn describe(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "describe net plugin",
        client.get_net_plugin(GetNetPluginRequest { name }).await,
    )?
    .into_inner();
    let plugin = reply.plugin.unwrap_or_default();
    if format.is_json() {
        output::print_success(&plugin_json(&plugin));
        return Ok(());
    }
    println!(
        "{} (revision {}, stages {}, usable by {}) updated {}",
        plugin.name,
        plugin.revision,
        plugin.stages.join(", "),
        plugin.usable_by,
        plugin.updated_at
    );
    if plugin.referenced_by.is_empty() {
        println!("No policy object uses it.");
    } else {
        println!(
            "Used by policy object(s): {}",
            plugin.referenced_by.join(", ")
        );
    }
    let object: Value = serde_json::from_str(&plugin.object_json).unwrap_or(Value::Null);
    println!("{}", serde_json::to_string_pretty(&object)?);
    Ok(())
}

#[derive(Tabled)]
struct StatusRow {
    #[tabled(rename = "NODE")]
    node: String,
    #[tabled(rename = "REVISION")]
    revision: u64,
    #[tabled(rename = "STATE")]
    state: String,
    #[tabled(rename = "CALLS")]
    calls: u64,
    #[tabled(rename = "ERRORS")]
    errors: u64,
    #[tabled(rename = "P99")]
    p99: String,
    #[tabled(rename = "INSTANCES")]
    instances: u64,
    #[tabled(rename = "LAST ERROR")]
    last_error: String,
}

async fn status(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "net plugin status",
        client
            .get_net_plugin_status(GetNetPluginStatusRequest { name: name.clone() })
            .await,
    )?
    .into_inner();
    let plugin = reply.plugin.unwrap_or_default();
    if format.is_json() {
        let nodes: Vec<Value> = reply
            .nodes
            .iter()
            .map(|n| {
                json!({
                    "node": n.node_id,
                    "revision": n.revision,
                    "moduleChecksum": n.module_checksum,
                    "state": n.state,
                    "error": n.error,
                    "calls": n.calls,
                    "errors": n.errors,
                    "lastError": n.last_error,
                    "p99Micros": n.p99_micros,
                    "instances": n.instances,
                    "current": n.revision == plugin.revision,
                })
            })
            .collect();
        output::print_success(&json!({ "plugin": plugin_json(&plugin), "nodes": nodes }));
        return Ok(());
    }
    println!(
        "{} revision {} ({})",
        plugin.name,
        plugin.revision,
        short_checksum(&plugin.module_checksum)
    );
    if reply.nodes.is_empty() {
        println!("No node runs it: no running allocation uses it yet.");
        return Ok(());
    }
    let rows: Vec<StatusRow> = reply
        .nodes
        .iter()
        .map(|n| StatusRow {
            node: n.node_id.clone(),
            revision: n.revision,
            state: if n.error.is_empty() {
                n.state.clone()
            } else {
                format!("{}: {}", n.state, n.error)
            },
            calls: n.calls,
            errors: n.errors,
            p99: format!("{} µs", n.p99_micros),
            instances: n.instances,
            last_error: n.last_error.clone(),
        })
        .collect();
    println!("{}", Table::new(rows));
    Ok(())
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    output::resolve(
        format,
        "delete net plugin",
        client
            .delete_net_plugin(DeleteNetPluginRequest { name: name.clone() })
            .await,
    )?;
    if format.is_json() {
        output::print_success(&json!({ "deleted": name }));
    } else {
        println!("Deleted plugin {name}.");
    }
    Ok(())
}

#[cfg(test)]
mod net_plugin_cli_test {
    use super::*;

    #[test]
    fn the_scaffold_parses_as_an_object_once_filled_in_test() {
        let files = scaffold("my-plugin", "my_plugin");
        let ncl = &files.iter().find(|(p, _)| p.ends_with(".ncl")).unwrap().1;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("my-plugin.ncl");
        std::fs::write(&path, ncl).unwrap();
        let object = load_object_file(&path).unwrap();
        assert_eq!(object.name, "my-plugin");
        assert_eq!(object.stages, vec!["request"]);
        assert!(files
            .iter()
            .any(|(p, c)| p == "Cargo.toml" && c.contains("name = \"my_plugin\"")));
    }

    #[test]
    fn every_verb_takes_json_test() {
        let command = config();
        for sub in command.get_subcommands() {
            assert!(
                sub.get_arguments().any(|a| a.get_id() == "json"),
                "net plugin {} has no --json",
                sub.get_name()
            );
        }
    }
}
