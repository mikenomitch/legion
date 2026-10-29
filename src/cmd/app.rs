//! `legion app` — apps: Legion's add-ons, each a job in `legion-system` plus
//! a manifest, a grant, and registrations (docs/apps.md).
//!
//! Every subcommand but `run` takes `--json`/`-j` and emits the standard
//! [`crate::cmd::output`] envelope. `view` renders an app's declarative
//! views ([`crate::cmd::app_view`]). `run` is the in-task entrypoint bundled
//! apps' jobs execute (`legion:self app run <name>`), a long-running process
//! like `legion start`.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::app::{app_job_id, AppManifest};
use crate::structs::{
    App, GetAppRequest, InstallAppRequest, ListAppsRequest, SetAppConfigRequest,
    SetAppEnabledRequest, UninstallAppRequest, UpdateAppSettingsRequest, UpgradeAppRequest,
};
use std::path::Path;
use tabled::Tabled;

const ABOUT: &str = "Apps: Legion's add-ons. Each is a job in the legion-system namespace, a \
grant (what that job may do in Legion), and registrations, kept running by the app reconciler \
(docs/apps.md). The autoscaler is bundled; others install from a file or URL.";

/// Largest manifest `install` fetches from a URL.
const MAX_FETCH_BYTES: u64 = 1024 * 1024;

#[derive(Tabled)]
struct AppRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "VERSION")]
    version: String,
    #[tabled(rename = "SOURCE")]
    source: String,
    #[tabled(rename = "STATUS")]
    status: String,
    #[tabled(rename = "PLACEMENT")]
    placement: String,
    #[tabled(rename = "REPLICAS")]
    replicas: String,
    #[tabled(rename = "UI")]
    ui: String,
    #[tabled(rename = "DETAIL")]
    detail: String,
}

fn name_arg() -> Arg {
    Arg::new("name")
        .required(true)
        .value_name("APP")
        .help("The app's name")
}

fn dry_run_flag(help: &'static str) -> Arg {
    Arg::new("dry-run")
        .long("dry-run")
        .action(ArgAction::SetTrue)
        .help(help)
}

fn yes_flag(help: &'static str) -> Arg {
    Arg::new("yes")
        .long("yes")
        .short('y')
        .action(ArgAction::SetTrue)
        .help(help)
}

pub fn config() -> Command {
    Command::new("app")
        .about(ABOUT)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List installed apps: version, status, placement, replicas, and UI slots")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("status")
                .about(
                    "Show one app: its record, grant, job status, the reconciler's last action \
                     and error, and what it waits for",
                )
                .arg(name_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("logs")
                .about("Fetch an app's task logs (`legion job logs legion-system/<app>`)")
                .arg(name_arg())
                .arg(
                    Arg::new("stream")
                        .long("stream")
                        .value_name("STREAM")
                        .value_parser(["stdout", "stderr", "all"])
                        .default_value("all")
                        .help("Which stream to read: stdout, stderr, or all"),
                )
                .arg(
                    Arg::new("tail")
                        .long("tail")
                        .action(ArgAction::SetTrue)
                        .help("The trailing bytes of each stream instead of the leading ones"),
                )
                .arg(
                    Arg::new("follow")
                        .long("follow")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Stream new output until interrupted (not with --json)"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("config")
                .about("An app's config: what it is, change it, and its schema")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("get")
                        .about("Print an app's config, as JSON (valid input for `config set`)")
                        .arg(name_arg())
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("set")
                        .about(
                            "Replace an app's config from a Nickel or JSON file, validated \
                             against its schema. The app's job rolls with it. Changing a code \
                             field needs the app's whole grant",
                        )
                        .arg(name_arg())
                        .arg(
                            Arg::new("file")
                                .long("file")
                                .short('f')
                                .required(true)
                                .value_name("FILE")
                                .value_hint(ValueHint::FilePath)
                                .help("The config (.ncl or .json)"),
                        )
                        .arg(dry_run_flag(
                            "Validate the config and say what would change; change nothing",
                        ))
                        .arg(idempotency_arg())
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("schema")
                        .about("Print an app's config schema (JSON Schema, draft 7)")
                        .arg(name_arg())
                        .arg(json_flag()),
                ),
        )
        .subcommand(
            Command::new("set")
                .about("Change an app's settings; its job rolls with them")
                .arg(name_arg())
                .arg(
                    Arg::new("replicas")
                        .long("replicas")
                        .value_name("N")
                        .value_parser(clap::value_parser!(u32))
                        .help("Copies of each task, 1-32"),
                )
                .arg(
                    Arg::new("placement")
                        .long("placement")
                        .value_name("WHERE")
                        .value_parser(["managers", "workers", "any"])
                        .help("managers (their system workers), workers, or any"),
                )
                .arg(
                    Arg::new("pool")
                        .long("pool")
                        .value_name("POOL")
                        .help("The worker pool for workers and any placement (empty clears it)"),
                )
                .arg(
                    Arg::new("cpu")
                        .long("cpu")
                        .value_name("MHZ")
                        .value_parser(clap::value_parser!(u64))
                        .help("CPU per task, in MHz (0 clears the override)"),
                )
                .arg(
                    Arg::new("memory")
                        .long("memory")
                        .value_name("MIB")
                        .value_parser(clap::value_parser!(u64))
                        .help("Memory per task, in MiB (0 clears the override)"),
                )
                .arg(dry_run_flag(
                    "Validate the change and say what would change; change nothing",
                ))
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("enable")
                .about(
                    "Enable an app: its job runs again. On an enabled app, resubmits a job that \
                     does not run healthy at its current version (a failed upgrade, a failed job)",
                )
                .arg(name_arg())
                .arg(dry_run_flag("Say what enabling would do; change nothing"))
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("disable")
                .about("Disable an app: its job stops; its record, config, and grant are kept")
                .arg(name_arg())
                .arg(dry_run_flag("Say what disabling would do; change nothing"))
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("install")
                .about(
                    "Install an app from a manifest file or URL. Shows what it runs, where, as \
                     whom, and what it asks for, then asks; your token must hold every \
                     permission it asks for",
                )
                .arg(
                    Arg::new("source")
                        .required(true)
                        .value_name("FILE|URL")
                        .value_hint(ValueHint::AnyPath)
                        .help("A manifest (.ncl or .json), or an http(s) URL of one"),
                )
                .arg(
                    Arg::new("config")
                        .long("config")
                        .value_name("FILE")
                        .value_hint(ValueHint::FilePath)
                        .help("The app's initial config (.ncl or .json); default its manifest's"),
                )
                .arg(yes_flag("Grant and install without asking"))
                .arg(
                    Arg::new("key")
                        .long("key")
                        .value_name("POINT.KEY=NEW")
                        .action(ArgAction::Append)
                        .help(
                            "Install a jobspec key under another name, when another app owns it \
                             (volume.backup=pgbackup); job authors then write the new key",
                        ),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Show the prompt and whether your token holds the grant; install nothing"),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("upgrade")
                .about(
                    "Replace an installed app's manifest. Shows the permission diff; new \
                     permissions need approval",
                )
                .arg(name_arg())
                .arg(
                    Arg::new("source")
                        .required(true)
                        .value_name("FILE|URL")
                        .value_hint(ValueHint::AnyPath)
                        .help("The new manifest (.ncl or .json), or an http(s) URL of one"),
                )
                .arg(
                    Arg::new("config")
                        .long("config")
                        .value_name("FILE")
                        .value_hint(ValueHint::FilePath)
                        .help("A config for the new manifest; default the stored one"),
                )
                .arg(yes_flag("Approve new permissions without asking"))
                .arg(
                    Arg::new("force")
                        .long("force")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Upgrade even though jobs set a jobspec key, or objects exist of a \
                             kind, that the new manifest no longer declares; they stay stored, \
                             flagged orphaned",
                        ),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Show the permission diff; upgrade nothing"),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("uninstall")
                .about("Uninstall an app: stop its job, revoke its grant, drop its record")
                .arg(name_arg())
                .arg(
                    Arg::new("force")
                        .long("force")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Uninstall even though jobs set its jobspec keys or objects of its \
                             kinds exist; they stay stored, flagged orphaned",
                        ),
                )
                .arg(
                    Arg::new("purge")
                        .long("purge")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Also delete the app's KV keys under legion/apps/<app>/ (needs \
                             kv:write there)",
                        ),
                )
                .arg(dry_run_flag(
                    "Say what uninstalling would stop, revoke, orphan, and delete; change nothing",
                ))
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("extensions")
                .about(
                    "List the jobspec keys installed apps add: each key's point, owner, \
                     description, and examples",
                )
                .arg(
                    Arg::new("name")
                        .value_name("APP")
                        .help("Only this app's keys"),
                )
                .arg(json_flag()),
        )
        .subcommand(crate::cmd::app_view::command())
        .subcommand(
            Command::new("run")
                .about(
                    "Run a bundled app in this process: the entrypoint its job runs \
                     (`legion:self app run <app>`). Reads $LEGION_APP_CONFIG, serves its HTTP \
                     port with /healthz, and stops on SIGTERM",
                )
                .arg(name_arg())
                .arg(
                    Arg::new("listen")
                        .long("listen")
                        .value_name("ADDR")
                        .help("Listen here instead of every interface on $LEGION_PORT_API"),
                )
                .arg(
                    Arg::new("config")
                        .long("config")
                        .value_name("FILE")
                        .value_hint(ValueHint::FilePath)
                        .help("Read the config (JSON) here instead of $LEGION_APP_CONFIG"),
                ),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("status", sub)) => status(sub).await,
        Some(("logs", sub)) => logs(sub).await,
        Some(("config", sub)) => match sub.subcommand() {
            Some(("get", sub)) => config_get(sub).await,
            Some(("set", sub)) => config_set(sub).await,
            Some(("schema", sub)) => config_schema(sub).await,
            _ => unreachable!("subcommand_required"),
        },
        Some(("set", sub)) => set(sub).await,
        Some(("enable", sub)) => set_enabled(sub, true).await,
        Some(("disable", sub)) => set_enabled(sub, false).await,
        Some(("install", sub)) => install(sub).await,
        Some(("upgrade", sub)) => upgrade(sub).await,
        Some(("uninstall", sub)) => uninstall(sub).await,
        Some(("extensions", sub)) => extensions(sub).await,
        Some(("view", sub)) => crate::cmd::app_view::handle(sub).await,
        Some(("run", sub)) => {
            crate::apps::run_app(
                sub.get_one::<String>("name").expect("required"),
                crate::apps::RunOptions {
                    listen: sub.get_one::<String>("listen").cloned(),
                    config: sub.get_one::<String>("config").map(Into::into),
                },
            )
            .await
        }
        _ => unreachable!("subcommand_required"),
    }
}

fn name_of(matches: &ArgMatches) -> String {
    matches
        .get_one::<String>("name")
        .expect("required")
        .trim()
        .to_string()
}

fn parse_json_field(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or(Value::Null)
}

/// The JSON form of an app: the proto message, with its JSON-in-string
/// fields parsed.
pub(crate) fn app_json(app: &App) -> Value {
    let mut value = serde_json::to_value(app).unwrap_or(Value::Null);
    if let Some(object) = value.as_object_mut() {
        object.insert("manifest".into(), parse_json_field(&app.manifest_json));
        object.insert("config".into(), parse_json_field(&app.config_json));
        object.remove("manifestJson");
        object.remove("configJson");
        if let Some(keys) = object
            .get_mut("extensionKeys")
            .and_then(Value::as_array_mut)
        {
            for key in keys.iter_mut().filter_map(Value::as_object_mut) {
                for (raw, parsed) in [("schemaJson", "schema"), ("examplesJson", "examples")] {
                    let value = key
                        .remove(raw)
                        .and_then(|value| value.as_str().map(parse_json_field))
                        .unwrap_or(Value::Null);
                    key.insert(parsed.into(), value);
                }
            }
        }
        if let Some(grant) = object.get_mut("grant").and_then(Value::as_object_mut) {
            let rules = grant
                .get("rulesJson")
                .and_then(Value::as_str)
                .map(parse_json_field)
                .unwrap_or(Value::Null);
            grant.insert("rules".into(), rules);
            grant.remove("rulesJson");
        }
    }
    value
}

fn status_label(app: &App) -> String {
    app.status.clone()
}

fn placement_label(app: &App) -> String {
    let Some(settings) = app.settings.as_ref() else {
        return "-".to_string();
    };
    match settings.pool.as_deref() {
        Some(pool) if settings.placement != "managers" => {
            format!("{} ({pool})", settings.placement)
        }
        _ => settings.placement.clone(),
    }
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list apps",
        client.list_apps(ListAppsRequest {}).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "apps": reply.apps.iter().map(app_json).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    if reply.apps.is_empty() {
        println!("No apps visible to this token.");
        return Ok(());
    }
    let rows = reply
        .apps
        .iter()
        .map(|app| AppRow {
            name: app.name.clone(),
            version: app.version.clone(),
            source: app.source.clone(),
            status: status_label(app),
            placement: placement_label(app),
            replicas: format!("{}/{}", app.running, app.desired),
            ui: if app.ui_slots.is_empty() {
                "-".to_string()
            } else {
                app.ui_slots.join(", ")
            },
            detail: {
                let mut detail = app.status_detail.clone();
                if !app.running_on.is_empty() && app.status == "running" && detail.is_empty() {
                    detail = format!("on {}", app.running_on.join(", "));
                }
                detail
            },
        })
        .collect::<Vec<_>>();
    print_table(rows);
    for app in &reply.apps {
        for note in &app.notes {
            println!("{}: note: {note}", app.name);
        }
    }
    Ok(())
}

fn print_rules(rules_json: &str, indent: &str) {
    let rules: Vec<crate::structs::acl::AclRule> =
        serde_json::from_str(rules_json).unwrap_or_default();
    if rules.is_empty() {
        println!("{indent}(no permissions)");
    }
    for rule in rules {
        println!("{indent}{}", crate::structs::app::describe_rule(&rule));
    }
}

async fn status(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = name_of(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "get app",
        client.get_app(GetAppRequest { name }).await,
    )?
    .into_inner();
    let app = reply.app.unwrap_or_default();
    if format.is_json() {
        output::print_success(&app_json(&app));
        return Ok(());
    }
    println!("{} ({})  {}", app.name, app.source, app.status);
    if !app.description.is_empty() {
        println!("  {}", app.description);
    }
    if !app.status_detail.is_empty() {
        println!("  Status:     {} ({})", app.status, app.status_detail);
    }
    let on = if app.running_on.is_empty() {
        String::new()
    } else {
        format!(" on {}", app.running_on.join(", "))
    };
    println!(
        "  Job:        {}, {}/{} running{on}",
        app.job_id, app.running, app.desired
    );
    println!("  Version:    {}", app.version);
    if let Some(settings) = &app.settings {
        let mut line = format!(
            "{}, {} replica(s)",
            placement_label(&app),
            settings.replicas
        );
        if let Some(cpu) = settings.cpu {
            line.push_str(&format!(", cpu {cpu} MHz"));
        }
        if let Some(memory) = settings.memory {
            line.push_str(&format!(", memory {memory} MiB"));
        }
        println!("  Settings:   {line}");
    }
    if !app.source_ref.is_empty() {
        println!("  From:       {}", app.source_ref);
    }
    println!(
        "  Installed:  by {} at {}",
        app.installed_by, app.installed_at
    );
    if app.uid != 0 {
        println!("  Runs as:    uid {} (its own)", app.uid);
    }
    if let Some(grant) = &app.grant {
        println!(
            "  Grant:      by {} at {}",
            grant.granted_by, grant.granted_at
        );
        print_rules(&grant.rules_json, "    ");
    }
    if let Some(reconcile) = &app.reconcile {
        if !reconcile.build_marker.is_empty() {
            println!("  Build:      {}", reconcile.build_marker);
        }
        if !reconcile.marker_reason.is_empty() {
            println!("  Waiting:    {}", reconcile.marker_reason);
        }
        if !reconcile.last_action.is_empty() {
            println!(
                "  Reconcile:  {} ({})",
                reconcile.last_action, reconcile.last_at
            );
        }
        if !reconcile.last_digest.is_empty() {
            let stamped = if reconcile.job_digest == reconcile.last_digest {
                "the job carries it"
            } else if reconcile.job_digest.is_empty() {
                "the job's latest version was submitted by someone else"
            } else {
                "the job carries an older one"
            };
            println!("  Digest:     {} ({stamped})", reconcile.last_digest);
        }
        if !reconcile.last_error.is_empty() {
            println!("  Last error: {}", reconcile.last_error);
        }
    }
    if !app.ui_slots.is_empty() {
        println!("  UI:         {}", app.ui_slots.join(", "));
    }
    if !app.extension_keys.is_empty() {
        let keys: Vec<String> = app
            .extension_keys
            .iter()
            .map(|key| format!("{} on {}", key.key, key.point))
            .collect();
        println!("  Jobspec:    {}", keys.join(", "));
    }
    for note in &app.notes {
        println!("  Note:       {note}");
    }
    Ok(())
}

async fn logs(matches: &ArgMatches) -> R<()> {
    let job = app_job_id(&name_of(matches));
    let mut argv: Vec<String> = vec!["job".into(), "logs".into(), job];
    argv.push("--stream".into());
    argv.push(
        matches
            .get_one::<String>("stream")
            .cloned()
            .unwrap_or_else(|| "all".into()),
    );
    for flag in ["tail", "follow", "json"] {
        if matches.get_flag(flag) {
            argv.push(format!("--{flag}"));
        }
    }
    let job_matches = crate::cmd::job::config()
        .try_get_matches_from(argv)
        .context("forwarding to `legion job logs`")?;
    crate::cmd::job::handle(&job_matches).await
}

async fn get_app(format: OutputFormat, name: String) -> R<App> {
    let mut client = connect_manager_client().await?;
    Ok(output::resolve(
        format,
        "get app",
        client.get_app(GetAppRequest { name }).await,
    )?
    .into_inner()
    .app
    .unwrap_or_default())
}

async fn config_get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let app = get_app(format, name_of(matches)).await?;
    let config = parse_json_field(&app.config_json);
    if format.is_json() {
        output::print_success(&json!({ "app": app.name, "config": config }));
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&config).unwrap_or_default()
        );
    }
    Ok(())
}

async fn config_schema(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let app = get_app(format, name_of(matches)).await?;
    let manifest: Option<AppManifest> = serde_json::from_str(&app.manifest_json).ok();
    let schema = manifest
        .map(|manifest| manifest.config_schema())
        .unwrap_or(Value::Null);
    if format.is_json() {
        output::print_success(&json!({ "app": app.name, "schema": schema }));
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema).unwrap_or_default()
        );
    }
    Ok(())
}

/// Evaluate a local Nickel or JSON file to JSON.
fn eval_local(format: OutputFormat, path: &str, code: &str) -> R<String> {
    crate::config_lang::eval_file(Path::new(path)).map_err(|err| {
        output::fail_local(
            format,
            CliError::local(code, format!("failed to evaluate {path}: {err:#}"), None),
        )
    })
}

async fn config_set(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = name_of(matches);
    let file = matches.get_one::<String>("file").expect("required");
    let config_json = eval_local(format, file, "invalid_config_file")?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "set app config",
        client
            .set_app_config(SetAppConfigRequest {
                name,
                config_json,
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
                dry_run: matches.get_flag("dry-run"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "dryRun": matches.get_flag("dry-run"),
            "deduplicated": reply.deduplicated,
            "codeFieldsChanged": reply.code_fields_changed,
            "app": reply.app.as_ref().map(app_json),
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

async fn set(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = name_of(matches);
    let request = UpdateAppSettingsRequest {
        name,
        replicas: matches.get_one::<u32>("replicas").copied(),
        placement: matches.get_one::<String>("placement").cloned(),
        cpu: matches.get_one::<u64>("cpu").copied(),
        memory: matches.get_one::<u64>("memory").copied(),
        pool: matches.get_one::<String>("pool").cloned(),
        idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
        dry_run: matches.get_flag("dry-run"),
    };
    if request.replicas.is_none()
        && request.placement.is_none()
        && request.cpu.is_none()
        && request.memory.is_none()
        && request.pool.is_none()
    {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "nothing to set",
                Some("pass --replicas, --placement, --pool, --cpu, or --memory"),
            ),
        ));
    }
    let mut client = connect_manager_client().await?;
    let dry_run = request.dry_run;
    let reply = output::resolve(
        format,
        "update app settings",
        client.update_app_settings(request).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "dryRun": dry_run,
            "deduplicated": reply.deduplicated,
            "app": reply.app.as_ref().map(app_json),
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

async fn set_enabled(matches: &ArgMatches, enabled: bool) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        if enabled { "enable app" } else { "disable app" },
        client
            .set_app_enabled(SetAppEnabledRequest {
                name: name_of(matches),
                enabled,
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
                dry_run: matches.get_flag("dry-run"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "dryRun": matches.get_flag("dry-run"),
            "deduplicated": reply.deduplicated,
            "app": reply.app.as_ref().map(app_json),
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

/// A manifest from a file or an http(s) URL, evaluated to JSON, with the
/// source kind and reference InstallApp records.
async fn load_manifest(format: OutputFormat, source: &str) -> R<(String, String, String)> {
    if source.starts_with("http://") || source.starts_with("https://") {
        let url = source.to_string();
        let fetched = tokio::task::spawn_blocking(move || fetch_manifest(&url))
            .await
            .context("fetching the manifest")?;
        let text = fetched.map_err(|err| {
            output::fail_local(
                format,
                CliError::local(
                    "Unavailable",
                    format!("failed to fetch {source}: {err:#}"),
                    Some("check the URL, or download the manifest and install the file"),
                ),
            )
        })?;
        // A remote manifest is JSON or self-contained Nickel: imports would
        // read files on this machine on the URL's say-so.
        let json = if source.split('?').next().unwrap_or("").ends_with(".json") {
            text
        } else {
            crate::config_lang::eval_sandboxed(source, &text, None).map_err(|err| {
                output::fail_local(
                    format,
                    CliError::local(
                        "invalid_manifest",
                        format!("failed to evaluate {source}: {err:#}"),
                        Some("a manifest fetched from a URL must be self-contained (no imports)"),
                    ),
                )
            })?
        };
        return Ok((json, "url".into(), source.to_string()));
    }
    let json = eval_local(format, source, "invalid_manifest")?;
    Ok((json, "file".into(), source.to_string()))
}

/// GET a manifest, at most [`MAX_FETCH_BYTES`].
fn fetch_manifest(url: &str) -> R<String> {
    use std::io::Read;
    let response = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .get(url)
        .send()?
        .error_for_status()?;
    let mut body = Vec::new();
    response.take(MAX_FETCH_BYTES + 1).read_to_end(&mut body)?;
    if body.len() as u64 > MAX_FETCH_BYTES {
        bail!("the manifest is larger than {MAX_FETCH_BYTES} bytes");
    }
    String::from_utf8(body).context("the manifest is not UTF-8")
}

/// Ask a yes/no question on the terminal. `false` unless the answer is yes.
fn confirm(question: &str) -> R<bool> {
    use std::io::{IsTerminal, Write};
    if !std::io::stdin().is_terminal() {
        bail!("{question} needs an answer, and stdin is not a terminal; pass --yes to agree");
    }
    print!("{question} [y/N] ");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn optional_config(format: OutputFormat, matches: &ArgMatches) -> R<Option<String>> {
    matches
        .get_one::<String>("config")
        .map(|path| eval_local(format, path, "invalid_config_file"))
        .transpose()
}

async fn install(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let source = matches.get_one::<String>("source").expect("required");
    let (manifest_json, kind, source_ref) = load_manifest(format, source).await?;
    let config_json = optional_config(format, matches)?;
    let dry_run = matches.get_flag("dry-run");
    let yes = matches.get_flag("yes");
    let mut client = connect_manager_client().await?;
    let keys: Vec<String> = matches
        .get_many::<String>("key")
        .map(|keys| keys.cloned().collect())
        .unwrap_or_default();
    let request = |dry_run: bool| InstallAppRequest {
        manifest_json: manifest_json.clone(),
        source: kind.clone(),
        source_ref: source_ref.clone(),
        dry_run,
        config_json: config_json.clone(),
        idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
        keys: keys.clone(),
    };
    let preview = output::resolve(
        format,
        "check the app install",
        client.install_app(request(true)).await,
    )?
    .into_inner();
    if dry_run {
        if format.is_json() {
            output::print_success(&json!({
                "dryRun": true,
                "prompt": preview.prompt,
                "uncovered": preview.uncovered,
                "app": preview.app.as_ref().map(app_json),
            }));
        } else {
            for line in &preview.prompt {
                println!("{line}");
            }
            if preview.uncovered.is_empty() {
                println!("Your token holds all of these. (dry run: nothing installed)");
            } else {
                println!("Your token cannot grant this: {}", preview.uncovered);
            }
        }
        return Ok(());
    }
    if !format.is_json() {
        for line in &preview.prompt {
            println!("{line}");
        }
    }
    if !preview.uncovered.is_empty() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "PermissionDenied",
                preview.uncovered.clone(),
                Some(
                    "have someone who holds it install the app, or ask for it to be granted to you",
                ),
            ),
        ));
    }
    if !yes {
        if format.is_json() {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    "installing an app grants it permissions; pass --yes to agree (or --dry-run to see what it asks for)",
                    None,
                ),
            ));
        }
        let agreed =
            confirm("Your token holds all of these. Grant them and install?").map_err(|err| {
                output::fail_local(
                    format,
                    CliError::local("InvalidArgument", format!("{err:#}"), None),
                )
            })?;
        if !agreed {
            println!("Not installed.");
            return Ok(());
        }
    }
    let reply = output::resolve(
        format,
        "install app",
        client.install_app(request(false)).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "prompt": reply.prompt,
            "app": reply.app.as_ref().map(app_json),
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

async fn upgrade(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = name_of(matches);
    let source = matches.get_one::<String>("source").expect("required");
    let (manifest_json, _kind, source_ref) = load_manifest(format, source).await?;
    let config_json = optional_config(format, matches)?;
    let dry_run = matches.get_flag("dry-run");
    let yes = matches.get_flag("yes");
    let mut client = connect_manager_client().await?;
    let force = matches.get_flag("force");
    let request = |dry_run: bool, approve: bool| UpgradeAppRequest {
        name: name.clone(),
        manifest_json: manifest_json.clone(),
        source_ref: source_ref.clone(),
        approve,
        dry_run,
        idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
        config_json: config_json.clone(),
        force,
    };
    let preview = output::resolve(
        format,
        "check the app upgrade",
        client.upgrade_app(request(true, false)).await,
    )?
    .into_inner();
    let diff = preview.diff.clone().unwrap_or_default();
    let added: Vec<crate::structs::acl::AclRule> =
        serde_json::from_str(&diff.added_json).unwrap_or_default();
    if dry_run || !format.is_json() {
        if format.is_json() {
            output::print_success(&json!({
                "dryRun": true,
                "message": preview.message,
                "prompt": preview.prompt,
                "uncovered": preview.uncovered,
                "orphanedJobs": preview.orphaned_jobs,
                "diff": {
                    "added": parse_json_field(&diff.added_json),
                    "removed": parse_json_field(&diff.removed_json),
                    "unchanged": parse_json_field(&diff.unchanged_json),
                },
            }));
            return Ok(());
        }
        for line in &preview.prompt {
            println!("{line}");
        }
        println!("Permission changes:");
        print_rules_with_mark(&diff.added_json, "  + ");
        print_rules_with_mark(&diff.removed_json, "  - ");
        if added.is_empty()
            && serde_json::from_str::<Vec<Value>>(&diff.removed_json)
                .unwrap_or_default()
                .is_empty()
        {
            println!("  (none)");
        }
        if dry_run {
            println!("{}", preview.message);
            return Ok(());
        }
    }
    // A token that cannot grant the new manifest, or an upgrade that would
    // orphan values without --force, is refused before anything is asked.
    if !preview.uncovered.is_empty() || (!preview.orphaned_jobs.is_empty() && !force) {
        let (code, message, hint) = if preview.uncovered.is_empty() {
            (
                "FailedPrecondition",
                preview.message.trim_start_matches("dry run: ").to_string(),
                None,
            )
        } else {
            (
                "PermissionDenied",
                preview.uncovered.clone(),
                Some(
                    "have someone who holds it upgrade the app, or ask for it to be granted to you",
                ),
            )
        };
        return Err(output::fail_local(
            format,
            CliError::local(code, message, hint),
        ));
    }
    let approve = if added.is_empty() || yes {
        true
    } else if format.is_json() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                format!(
                    "the new manifest asks for {} permission(s) the installed version does not have; pass --yes to approve",
                    added.len()
                ),
                None,
            ),
        ));
    } else {
        let agreed = confirm("Approve the new permissions and upgrade?").map_err(|err| {
            output::fail_local(
                format,
                CliError::local("InvalidArgument", format!("{err:#}"), None),
            )
        })?;
        if !agreed {
            println!("Not upgraded.");
            return Ok(());
        }
        true
    };
    let reply = output::resolve(
        format,
        "upgrade app",
        client.upgrade_app(request(false, approve)).await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "upgraded": reply.upgraded,
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "orphanedJobs": reply.orphaned_jobs,
            "app": reply.app.as_ref().map(app_json),
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

fn print_rules_with_mark(rules_json: &str, mark: &str) {
    let rules: Vec<crate::structs::acl::AclRule> =
        serde_json::from_str(rules_json).unwrap_or_default();
    for rule in rules {
        println!("{mark}{}", crate::structs::app::describe_rule(&rule));
    }
}

async fn uninstall(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "uninstall app",
        client
            .uninstall_app(UninstallAppRequest {
                name: name_of(matches),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
                force: matches.get_flag("force"),
                purge: matches.get_flag("purge"),
                dry_run: matches.get_flag("dry-run"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "dryRun": matches.get_flag("dry-run"),
            "revokedGrants": reply.revoked_grants,
            "deduplicated": reply.deduplicated,
            "orphanedJobs": reply.orphaned_jobs,
            "kvKeys": reply.kv_keys,
            "purged": reply.purged,
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

/// `legion app extensions [app]`: every jobspec key installed apps add.
async fn extensions(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let only = matches
        .get_one::<String>("name")
        .map(|name| name.trim().to_string());
    let mut client = connect_manager_client().await?;
    let apps = match &only {
        Some(name) => vec![output::resolve(
            format,
            "get app",
            client.get_app(GetAppRequest { name: name.clone() }).await,
        )?
        .into_inner()
        .app
        .unwrap_or_default()],
        None => {
            output::resolve(
                format,
                "list apps",
                client.list_apps(ListAppsRequest::default()).await,
            )?
            .into_inner()
            .apps
        }
    };
    let keys: Vec<&crate::structs::AppExtensionKey> = apps
        .iter()
        .flat_map(|app| app.extension_keys.iter())
        .collect();
    if format.is_json() {
        let keys: Vec<Value> = apps
            .iter()
            .flat_map(|app| {
                app_json(app)
                    .get("extensionKeys")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        output::print_success(&json!({ "keys": keys }));
        return Ok(());
    }
    if keys.is_empty() {
        println!("No installed app adds jobspec keys.");
        return Ok(());
    }
    for key in keys {
        let mut flags = Vec::new();
        if key.validate {
            flags.push("the app also checks values at submit".to_string());
        }
        if !key.requires.is_empty() {
            flags.push(format!("setting it requires {}", key.requires));
        }
        if !key.app_enabled {
            flags.push("app disabled".to_string());
        }
        let renamed = if key.declared_key != key.key {
            format!(" (declared `{}`)", key.declared_key)
        } else {
            String::new()
        };
        println!(
            "{} on {}  ({} app {}){renamed}",
            key.key, key.point, key.app, key.app_version
        );
        if !key.description.is_empty() {
            println!("  {}", key.description);
        }
        if key.point == "group" {
            println!("  Written on one task of a group; applies to the whole group.");
        }
        if key.shadowed {
            println!(
                "  `{}` is now a core field at this point: write `apps.{}` instead.",
                key.key, key.app
            );
        } else {
            println!("  Also written `apps.{}`.", key.app);
        }
        if !flags.is_empty() {
            println!("  {}", flags.join("; "));
        }
        let examples = parse_json_field(&key.examples_json);
        for example in examples.as_array().into_iter().flatten() {
            println!("  e.g. {} = {example}", key.key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_command_tree_parses() {
        let cli = crate::cmd::cli();
        for argv in [
            vec!["legion", "app", "list", "-j"],
            vec!["legion", "app", "status", "autoscaler"],
            vec!["legion", "app", "logs", "autoscaler", "-f"],
            vec!["legion", "app", "config", "get", "autoscaler"],
            vec![
                "legion",
                "app",
                "config",
                "set",
                "autoscaler",
                "-f",
                "a.ncl",
            ],
            vec!["legion", "app", "config", "schema", "autoscaler", "-j"],
            vec![
                "legion",
                "app",
                "set",
                "autoscaler",
                "--replicas",
                "3",
                "--placement",
                "any",
            ],
            vec!["legion", "app", "enable", "autoscaler"],
            vec!["legion", "app", "disable", "autoscaler", "-j"],
            vec![
                "legion",
                "app",
                "install",
                "https://example.com/app.ncl",
                "--yes",
            ],
            vec![
                "legion",
                "app",
                "upgrade",
                "gitops",
                "./app.ncl",
                "--dry-run",
            ],
            vec!["legion", "app", "uninstall", "gitops"],
            vec![
                "legion",
                "app",
                "run",
                "autoscaler",
                "--listen",
                "127.0.0.1:0",
            ],
        ] {
            cli.clone()
                .try_get_matches_from(&argv)
                .unwrap_or_else(|err| panic!("{argv:?}: {err}"));
        }
        assert!(cli
            .clone()
            .try_get_matches_from(["legion", "app", "set", "x", "--placement", "moon"])
            .is_err());
    }

    #[test]
    fn app_json_parses_the_embedded_json() {
        let app = App {
            name: "x".into(),
            manifest_json: r#"{"name":"x"}"#.into(),
            config_json: r#"{"a":1}"#.into(),
            grant: Some(crate::structs::AppGrantInfo {
                rules_json: r#"[{"resources":["node"],"actions":["read"]}]"#.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let value = app_json(&app);
        assert_eq!(value["manifest"]["name"], "x");
        assert_eq!(value["config"]["a"], 1);
        assert_eq!(value["grant"]["rules"][0]["resources"][0], "node");
        assert!(value.get("manifestJson").is_none());
    }
}
