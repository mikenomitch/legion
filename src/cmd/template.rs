//! `legion template` — manage and run cluster-resident job templates.
//!
//! Templates are versioned Nickel documents with declared, typed variables,
//! rendered server-side (see docs/job-templates.md). `create` uploads a new
//! version, `run` renders one with `--var` values and submits the result as
//! a job, and `render` is the dry-run twin of `run`.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::job::{
    asap_flag, idempotency_arg, priority_arg, priority_from_matches, timeout_arg, wait_flag,
    wait_for_convergence, wait_timeout,
};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    DeleteJobTemplateRequest, GetJobTemplateRequest, ListJobTemplatesRequest,
    RenderJobTemplateRequest, RunJobTemplateRequest, UpsertJobTemplateRequest,
};
use tabled::Tabled;

pub fn config() -> Command {
    let name_arg = Arg::new("name")
        .value_name("NAME")
        .required(true)
        .help("Template name");
    let version_arg = Arg::new("version")
        .long("version")
        .value_name("N")
        .value_parser(clap::value_parser!(u32))
        .help("Template version (default: latest)");
    let var_args = [
        Arg::new("var")
            .long("var")
            .value_name("KEY=VALUE")
            .action(ArgAction::Append)
            .help(
                "Set a template variable (string form; the manager coerces to the declared type)",
            ),
        Arg::new("var-json")
            .long("var-json")
            .value_name("KEY=JSON")
            .action(ArgAction::Append)
            .help("Set a template variable to a JSON value (for lists and json-typed variables)"),
        Arg::new("vars-file")
            .long("vars-file")
            .value_name("PATH")
            .value_hint(ValueHint::FilePath)
            .help("JSON file with a variables object; --var/--var-json override its keys"),
    ];

    Command::new("template")
        .about("Manage and run job templates stored on the cluster")
        .subcommand_required(true)
        .subcommand(
            Command::new("create")
                .about("Upload a template file as the next version of NAME")
                .arg(name_arg.clone())
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .value_name("PATH")
                        .required(true)
                        .value_hint(ValueHint::FilePath)
                        .help("Path to the template Nickel source"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("list")
                .about("List templates")
                .arg(
                    Arg::new("query")
                        .short('q')
                        .long("query")
                        .value_name("TEXT")
                        .help("Filter by name/description substring"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one template version, its variables, and history")
                .disable_version_flag(true)
                .arg(name_arg.clone())
                .arg(version_arg.clone())
                .arg(
                    Arg::new("source")
                        .long("source")
                        .action(ArgAction::SetTrue)
                        .help("Print the raw Nickel source instead of the summary"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("render")
                .about("Render a template with variables without submitting (dry run)")
                .disable_version_flag(true)
                .arg(name_arg.clone())
                .arg(version_arg.clone())
                .args(var_args.clone())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("run")
                .about("Render a template with variables and submit the resulting job")
                .disable_version_flag(true)
                .arg(name_arg.clone())
                .arg(version_arg)
                .args(var_args)
                .arg(priority_arg())
                .arg(asap_flag())
                .arg(
                    Arg::new("force-reschedule")
                        .long("force-reschedule")
                        .help("Force a reschedule even if the rendered spec is unchanged")
                        .action(ArgAction::SetTrue),
                )
                .arg(idempotency_arg())
                .arg(wait_flag())
                .arg(timeout_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a template and all of its versions")
                .arg(name_arg)
                .arg(json_flag()),
        )
}

pub async fn handle(arg_matches: &ArgMatches) -> R<()> {
    match arg_matches.subcommand() {
        Some(("create", matches)) => create_template(matches).await,
        Some(("list", matches)) => list_templates(matches).await,
        Some(("get", matches)) => get_template(matches).await,
        Some(("render", matches)) => render_template(matches).await,
        Some(("run", matches)) => run_template(matches).await,
        Some(("delete", matches)) => delete_template(matches).await,
        _ => unreachable!("subcommand_required"),
    }
}

fn required_name(matches: &ArgMatches) -> String {
    matches
        .get_one::<String>("name")
        .expect("name is required")
        .clone()
}

/// Assembles the variables object: --vars-file first, then --var (raw
/// strings; the manager coerces them to declared types), then --var-json.
fn variables_json_from_matches(matches: &ArgMatches, format: OutputFormat) -> R<String> {
    let mut variables = Map::new();

    if let Some(path) = matches.get_one::<String>("vars-file") {
        let content = std::fs::read_to_string(path).map_err(|error| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("could not read vars file {path}: {error}"),
                    Some("pass a JSON file containing an object keyed by variable name"),
                ),
            )
        })?;
        let parsed: Map<String, Value> = serde_json::from_str(&content).map_err(|error| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("vars file {path} is not a JSON object: {error}"),
                    None,
                ),
            )
        })?;
        variables.extend(parsed);
    }

    for (flag, json_valued) in [("var", false), ("var-json", true)] {
        for entry in matches
            .get_many::<String>(flag)
            .unwrap_or_default()
            .cloned()
            .collect::<Vec<_>>()
        {
            let Some((key, raw)) = entry.split_once('=') else {
                return Err(output::fail_local(
                    format,
                    CliError::local(
                        "InvalidArgument",
                        format!("--{flag} {entry} is not KEY=VALUE"),
                        None,
                    ),
                ));
            };
            let value = if json_valued {
                serde_json::from_str(raw).map_err(|error| {
                    output::fail_local(
                        format,
                        CliError::local(
                            "InvalidArgument",
                            format!("--var-json {key} is not valid JSON: {error}"),
                            Some("quote strings: --var-json key='\"text\"'"),
                        ),
                    )
                })?
            } else {
                Value::String(raw.to_string())
            };
            variables.insert(key.to_string(), value);
        }
    }

    Ok(serde_json::to_string(&variables)?)
}

#[derive(Tabled)]
struct TemplatePrint {
    name: String,
    latest: u32,
    variables: u32,
    description: String,
    updated_at: String,
}

#[derive(Tabled)]
struct VariablePrint {
    key: String,
    #[tabled(rename = "type")]
    var_type: String,
    required: bool,
    default: String,
    options: String,
}

#[instrument(err, skip_all)]
async fn create_template(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = required_name(matches);
    let path = matches.get_one::<String>("file").expect("file is required");

    // Templates are the one config surface JSON cannot serve: a template
    // declares `job` as a *function* of its variables, and JSON has no
    // functions. Say so here rather than letting the manager's Nickel parser
    // report a syntax error on a file the user believes is valid.
    if crate::config_lang::format_for(path) == crate::config_lang::ConfigFormat::Json {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                format!("{path} is JSON, but a job template must be Nickel"),
                Some(
                    "a template declares `job` as a function of its variables \
                     (`job | not_exported = fun vars => …`), which JSON cannot express",
                ),
            ),
        ));
    }

    let source_nickel = std::fs::read_to_string(path).map_err(|error| {
        output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                format!("could not read template file {path}: {error}"),
                None,
            ),
        )
    })?;

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to create template",
        client
            .upsert_job_template(tonic::Request::new(UpsertJobTemplateRequest {
                name: name.clone(),
                source_nickel,
            }))
            .await,
    )?;
    let template = response.into_inner().template.unwrap_or_default();

    if format.is_json() {
        output::print_success(&json!({
            "name": template.name,
            "version": template.version,
            "variables": template.variables.len(),
        }));
    } else {
        println!(
            "Stored template {} version {} ({} variables)",
            template.name,
            template.version,
            template.variables.len()
        );
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn list_templates(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to list templates",
        client
            .list_job_templates(tonic::Request::new(ListJobTemplatesRequest {
                query: matches.get_one::<String>("query").cloned(),
            }))
            .await,
    )?;
    let templates = response.into_inner().templates;

    if format.is_json() {
        output::print_success(&templates);
    } else if templates.is_empty() {
        println!("No templates");
    } else {
        print_table(
            templates
                .into_iter()
                .map(|template| TemplatePrint {
                    name: template.name,
                    latest: template.latest_version,
                    variables: template.variable_count,
                    description: template.description.unwrap_or_default(),
                    updated_at: template.updated_at,
                })
                .collect(),
        );
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn get_template(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to get template",
        client
            .get_job_template(tonic::Request::new(GetJobTemplateRequest {
                name: required_name(matches),
                version: matches.get_one::<u32>("version").copied(),
            }))
            .await,
    )?;
    let reply = response.into_inner();
    let template = reply.template.unwrap_or_default();

    if format.is_json() {
        output::print_success(&json!({
            "template": template,
            "versions": reply.versions,
        }));
        return Ok(());
    }
    if matches.get_flag("source") {
        println!("{}", template.source_nickel);
        return Ok(());
    }

    println!(
        "{} v{} (latest v{}), created {} by {}",
        template.name,
        template.version,
        template.latest_version,
        template.created_at,
        if template.created_by.is_empty() {
            "-"
        } else {
            &template.created_by
        },
    );
    if let Some(description) = &template.description {
        println!("{description}");
    }
    if template.variables.is_empty() {
        println!("No variables");
    } else {
        print_table(
            template
                .variables
                .into_iter()
                .map(|variable| VariablePrint {
                    key: variable.key,
                    var_type: variable.r#type,
                    required: variable.required,
                    default: variable.default_json.unwrap_or_default(),
                    options: variable.options.join(", "),
                })
                .collect(),
        );
    }
    if reply.versions.len() > 1 {
        let versions: Vec<String> = reply
            .versions
            .iter()
            .map(|info| format!("v{}", info.version))
            .collect();
        println!("Versions: {}", versions.join(", "));
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn render_template(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let variables_json = variables_json_from_matches(matches, format)?;
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to render template",
        client
            .render_job_template(tonic::Request::new(RenderJobTemplateRequest {
                name: required_name(matches),
                version: matches.get_one::<u32>("version").copied(),
                variables_json,
            }))
            .await,
    )?;
    let reply = response.into_inner();

    if format.is_json() {
        let rendered: Value = serde_json::from_str(&reply.rendered_jobspec)
            .unwrap_or(Value::String(reply.rendered_jobspec));
        output::print_success(&json!({
            "jobName": reply.job_name,
            "jobspec": rendered,
        }));
    } else {
        println!("{}", reply.rendered_jobspec);
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn run_template(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let variables_json = variables_json_from_matches(matches, format)?;
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to run template",
        client
            .run_job_template(tonic::Request::new(RunJobTemplateRequest {
                name: required_name(matches),
                version: matches.get_one::<u32>("version").copied(),
                variables_json,
                priority: priority_from_matches(matches),
                force_reschedule: matches.get_flag("force-reschedule"),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?;
    let reply = response.into_inner();

    let wait = if matches.get_flag("wait") && !reply.job_name.is_empty() {
        Some(
            wait_for_convergence(&mut client, &reply.job_name, wait_timeout(matches), format)
                .await?,
        )
    } else {
        None
    };

    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "jobName": reply.job_name,
            "jobConfigId": reply.job_config_id,
            "wait": wait,
        }));
    } else {
        println!("{} (job {})", reply.message, reply.job_name);
        if let Some(wait) = wait {
            println!(
                "wait: converged={} success={} status={}",
                wait.converged, wait.success, wait.status
            );
        }
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn delete_template(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to delete template",
        client
            .delete_job_template(tonic::Request::new(DeleteJobTemplateRequest {
                name: required_name(matches),
            }))
            .await,
    )?;
    let reply = response.into_inner();

    if format.is_json() {
        output::print_success(&json!({ "message": reply.message }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}
