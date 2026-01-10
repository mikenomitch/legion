use std::path::{Path, PathBuf};
use std::vec;

use clap::value_parser;

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::getter_rs::Client as GetterClient;
use crate::prelude::*;
use crate::structs::{
    DestroyJobRequest, GetJobInfoRequest, GetJobSpecRequest, JobPlanPrint, JobPrint,
    ListJobsRequest, PlanJobRequest, RunJobRequest, StopJobRequest, Submission,
};
use jsonnet::JsonnetVm;
use serde_json::to_string;
use tempfile::TempDir;
use url::Url;

pub fn config() -> Command {
    let run_cmd = Command::new("run")
        .about("Run a job")
        .arg(
            Arg::new("path")
                .value_name("PATH")
                .help("Path or URL to the job specification to run")
                .value_hint(ValueHint::AnyPath),
        )
        .arg(priority_arg())
        .arg(asap_flag())
        .arg(
            Arg::new("force-reschedule")
                .long("force-reschedule")
                .help("Force a reschedule even if the job spec is unchanged")
                .action(ArgAction::SetTrue),
        )
        .args(jsonnet_var_args());

    let stop_cmd = Command::new("stop")
        .about("Stop a job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to stop"),
        )
        .arg(priority_arg())
        .arg(asap_flag());

    let destroy_cmd = Command::new("destroy")
        .about("Destroy a job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to remove"),
        )
        .arg(priority_arg())
        .arg(asap_flag());

    let info_cmd = Command::new("info")
        .about("Get job info")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to inspect")
                .required(true),
        )
        .arg(json_flag());

    let plan_cmd = Command::new("plan")
        .about("Preview which nodes allocations would land on")
        .arg(
            Arg::new("path")
                .value_name("PATH")
                .help("Path or URL to the job specification to plan")
                .required(true)
                .value_hint(ValueHint::AnyPath),
        )
        .arg(json_flag())
        .args(jsonnet_var_args());

    let spec_cmd = Command::new("spec")
        .about("Get job spec used to define the job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to fetch the spec for"),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list").about("List all jobs").arg(json_flag());

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(run_cmd)
        .subcommand(info_cmd)
        .subcommand(plan_cmd)
        .subcommand(list_cmd)
        .subcommand(stop_cmd)
        .subcommand(spec_cmd)
        .subcommand(destroy_cmd)
}

fn priority_arg() -> Arg {
    Arg::new("priority")
        .long("priority")
        .value_name("PRIORITY")
        .help("Priority to use for the request (higher runs sooner)")
        .value_parser(value_parser!(i64))
        .conflicts_with("asap")
}

fn asap_flag() -> Arg {
    Arg::new("asap")
        .long("asap")
        .help("Force the request to run ASAP by setting priority to 100")
        .action(ArgAction::SetTrue)
        .conflicts_with("priority")
}

fn priority_from_matches(matches: &ArgMatches) -> Option<i64> {
    if matches.get_flag("asap") {
        Some(100)
    } else {
        matches.get_one::<i64>("priority").copied()
    }
}

#[derive(Default)]
struct JsonnetVars {
    ext_vars: Vec<(String, String)>,
    ext_codes: Vec<(String, String)>,
    tla_vars: Vec<(String, String)>,
    tla_codes: Vec<(String, String)>,
}

impl JsonnetVars {}

fn jsonnet_var_args() -> Vec<Arg> {
    vec![
        Arg::new("var")
            .long("var")
            .value_name("KEY=VALUE")
            .help("Set a Jsonnet external string variable (std.extVar)")
            .action(ArgAction::Append),
        Arg::new("var-code")
            .long("var-code")
            .value_name("KEY=CODE")
            .help("Set a Jsonnet external code variable (std.extVar)")
            .action(ArgAction::Append),
        Arg::new("arg")
            .long("arg")
            .value_name("KEY=VALUE")
            .help("Set a Jsonnet top-level string argument")
            .action(ArgAction::Append),
        Arg::new("arg-code")
            .long("arg-code")
            .value_name("KEY=CODE")
            .help("Set a Jsonnet top-level code argument")
            .action(ArgAction::Append),
    ]
}

fn parse_key_value(input: &str) -> R<(String, String)> {
    let (key, value) = input.split_once('=').context("expected KEY=VALUE")?;
    if key.is_empty() {
        bail!("jsonnet variable key cannot be empty");
    }
    Ok((key.to_string(), value.to_string()))
}

fn jsonnet_vars_from_matches(matches: &ArgMatches) -> R<JsonnetVars> {
    let mut vars = JsonnetVars::default();

    if let Some(values) = matches.get_many::<String>("var") {
        for value in values {
            vars.ext_vars.push(parse_key_value(value)?);
        }
    }

    if let Some(values) = matches.get_many::<String>("var-code") {
        for value in values {
            vars.ext_codes.push(parse_key_value(value)?);
        }
    }

    if let Some(values) = matches.get_many::<String>("arg") {
        for value in values {
            vars.tla_vars.push(parse_key_value(value)?);
        }
    }

    if let Some(values) = matches.get_many::<String>("arg-code") {
        for value in values {
            vars.tla_codes.push(parse_key_value(value)?);
        }
    }

    Ok(vars)
}

fn apply_jsonnet_vars(vm: &mut JsonnetVm, vars: &JsonnetVars) {
    for (key, value) in &vars.ext_vars {
        vm.ext_var(key, value);
    }
    for (key, value) in &vars.ext_codes {
        vm.ext_code(key, value);
    }
    for (key, value) in &vars.tla_vars {
        vm.tla_var(key, value);
    }
    for (key, value) in &vars.tla_codes {
        vm.tla_code(key, value);
    }
}

struct ResolvedJobspec {
    _temp_dir: TempDir,
    path: PathBuf,
}

fn resolve_jobspec_source(source: &str) -> R<ResolvedJobspec> {
    let temp_dir = tempfile::tempdir().context("create temporary directory for jobspec")?;
    let client = GetterClient::default();
    let fetched_path = client
        .get(source, temp_dir.path())
        .with_context(|| format!("fetch jobspec from {source}"))?;
    let resolved_path = ensure_jobspec_file(source, &fetched_path)?;

    Ok(ResolvedJobspec {
        _temp_dir: temp_dir,
        path: resolved_path,
    })
}

async fn resolve_jobspec_source_async(source: &str) -> R<ResolvedJobspec> {
    let source = source.to_string();
    tokio::task::spawn_blocking(move || resolve_jobspec_source(&source))
        .await
        .context("resolve jobspec source task panicked")?
}

fn ensure_jobspec_file(source: &str, fetched_path: &Path) -> R<PathBuf> {
    if fetched_path.is_file() {
        return Ok(fetched_path.to_path_buf());
    }

    if fetched_path.is_dir() {
        if let Some(subpath) = extract_jobspec_subpath(source)? {
            let candidate = fetched_path.join(subpath);
            if candidate.is_file() {
                return Ok(candidate);
            }
            bail!(
                "jobspec path {} does not exist in fetched directory",
                candidate.display()
            );
        }
    }

    bail!(
        "jobspec source {source} resolved to a directory; append #<relative path> to select a file"
    )
}

fn extract_jobspec_subpath(source: &str) -> R<Option<PathBuf>> {
    if !source.contains("://") {
        return Ok(None);
    }

    let url = Url::parse(source).context("parse jobspec URL")?;
    let Some(fragment) = url.fragment() else {
        return Ok(None);
    };

    let fragment = fragment.trim_start_matches('/');
    if fragment.is_empty() {
        return Ok(None);
    }

    Ok(Some(PathBuf::from(fragment)))
}

fn render_jobspec(job_path: &Path, vars: &JsonnetVars) -> R<(String, Vec<Submission>)> {
    let mut vm = JsonnetVm::new();
    let mut submissions = vec![];
    let job_path_str = job_path.to_string_lossy();

    let content = std::fs::read_to_string(job_path)?;
    let initial_sub = Submission {
        path: job_path_str.to_string(),
        content,
        is_primary: true,
    };
    submissions.push(initial_sub);

    vm.import_callback(|_vm, base, rel| {
        let path = std::path::Path::new(base).join(rel);
        let path_str = path.to_str().unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();

        let sub = Submission {
            path: path_str.to_string(),
            content: contents.to_string(),
            is_primary: false,
        };

        submissions.push(sub);

        Ok((base.into(), contents.to_string()))
    });

    apply_jsonnet_vars(&mut vm, vars);

    let rendered = vm
        .evaluate_file(job_path)
        .map_err(|e| anyhow!("failed to evaluate job spec {}: {e}", job_path.display()))?
        .to_string();

    Ok((rendered, submissions))
}

pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", matches)) => run_job(matches).await,
        Some(("stop", matches)) => stop_job(matches).await,
        Some(("spec", matches)) => get_job_spec(matches).await,
        Some(("destroy", matches)) => destroy_job(matches).await,
        Some(("list", matches)) => list_jobs(matches).await,
        Some(("info", matches)) => get_info(matches).await,
        Some(("plan", matches)) => plan_job(matches).await,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_conflicts_with_asap() {
        let cmd = config();
        let result = cmd.try_get_matches_from(["job", "run", "--priority", "75", "--asap"]);
        assert!(result.is_err());
    }
}

#[instrument(err, skip_all)]
async fn stop_job(arg_matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);
    let priority = priority_from_matches(arg_matches);

    let request = tonic::Request::new(StopJobRequest {
        name: job_name.to_owned(),
        priority,
    });

    let response = client.stop_job(request).await?;
    println!("{}", response.into_inner().message);

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_job_spec(arg_matches: &ArgMatches) -> R<()> {
    let is_json = arg_matches.get_flag("json");

    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);

    let request = tonic::Request::new(GetJobSpecRequest {
        name: job_name.to_owned(),
    });

    let response = client.get_job_spec(request).await?;

    let subs = response.into_inner().submissions;

    if is_json {
        if let Ok(json) = to_string(&subs) {
            println!("{json}");
        }
    } else {
        subs.iter().for_each(|s| {
            println!("=============================");
            println!("{}:\n", s.path);
            println!("{}", s.content);
            println!("=============================");
        });
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn destroy_job(arg_matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);
    let priority = priority_from_matches(arg_matches);

    let request = tonic::Request::new(DestroyJobRequest {
        name: job_name.to_owned(),
        priority,
    });

    let response = client.destroy_job(request).await?;
    println!("{}", response.into_inner().message);

    Ok(())
}

#[instrument(err, skip_all)]
async fn plan_job(arg_matches: &ArgMatches) -> R<()> {
    let is_json = arg_matches.get_flag("json");
    let mut client = connect_manager_client().await?;
    let path = arg_matches
        .get_one::<String>("path")
        .map(String::as_str)
        .context("missing jobspec path")?;
    let vars = jsonnet_vars_from_matches(arg_matches)?;
    let resolved = resolve_jobspec_source_async(path).await?;
    let (rendered, _submissions) = render_jobspec(&resolved.path, &vars)?;

    let request = tonic::Request::new(PlanJobRequest {
        parsed_jobspec: rendered,
    });

    let response = client.plan_job(request).await?;
    if is_json {
        let reply = response.into_inner();
        if let Ok(json) = to_string(&reply) {
            println!("{json}");
        }
        return Ok(());
    } else {
        let reply = response.into_inner();
        let allocations = reply.allocations;
        if !reply.message.is_empty() {
            println!("{}", reply.message);
        }
        let printable_allocations: Vec<JobPlanPrint> =
            allocations.into_iter().map(Into::into).collect();
        if printable_allocations.is_empty() {
            println!("No allocations planned");
        } else {
            print_table(printable_allocations);
        }
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn run_job(arg_matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;

    let job_path = arg_matches
        .get_one::<String>("path")
        .map_or("./configs/jobs/demo.jsonnet", String::as_str);
    let priority = priority_from_matches(arg_matches);
    let vars = jsonnet_vars_from_matches(arg_matches)?;
    let resolved = resolve_jobspec_source_async(job_path).await?;
    let (rendered, submissions) = render_jobspec(&resolved.path, &vars)?;

    let request = tonic::Request::new(RunJobRequest {
        parsed_jobspec: rendered,
        submissions,
        priority,
        force_reschedule: arg_matches.get_flag("force-reschedule"),
    });

    let response = client.run_job(request).await.context("failed to run job")?;

    println!("{}", response.into_inner().message);

    Ok(())
}

#[instrument(err, skip_all)]
async fn list_jobs(arg_matches: &ArgMatches) -> R<()> {
    let is_json = arg_matches.get_flag("json");

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(ListJobsRequest {});
    let response = client.list_jobs(request).await?;
    let jobs_list = response.into_inner().jobs;

    if is_json {
        if let Ok(json) = to_string(&jobs_list) {
            println!("{json}");
        }
    } else {
        let printable_jobs: Vec<JobPrint> = jobs_list.into_iter().map(Into::into).collect();
        print_table(printable_jobs);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_info(arg_matches: &ArgMatches) -> R<()> {
    let is_json = arg_matches.get_flag("json");

    let mut client = connect_manager_client().await?;
    let job_name = arg_matches.get_one::<String>("name").context("need name")?;
    let request = tonic::Request::new(GetJobInfoRequest {
        name: job_name.to_owned(),
    });
    let response = client.get_job_info(request).await?;

    if is_json {
        if let Ok(json) = to_string(&response.into_inner().job) {
            println!("{json}");
        }
    } else {
        let printable_job: JobPrint = response.into_inner().job.context("no job found")?.into();
        print_table(vec![printable_job]);
    }

    Ok(())
}
