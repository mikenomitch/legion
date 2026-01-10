use std::vec;

use clap::value_parser;

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::prelude::*;
use crate::structs::{
    DestroyJobRequest, GetJobInfoRequest, GetJobSpecRequest, JobPlanPrint, JobPrint,
    ListJobsRequest, PlanJobRequest, RunJobRequest, StopJobRequest, Submission,
};
use jsonnet::JsonnetVm;
use serde_json::to_string;

pub fn config() -> Command {
    let run_cmd = Command::new("run")
        .about("Run a job")
        .arg(
            Arg::new("path")
                .value_name("PATH")
                .help("Path to the job specification to run")
                .value_hint(ValueHint::FilePath),
        )
        .arg(priority_arg())
        .arg(asap_flag())
        .arg(
            Arg::new("force-reschedule")
                .long("force-reschedule")
                .help("Force a reschedule even if the job spec is unchanged")
                .action(ArgAction::SetTrue),
        );

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
                .help("Path to the job specification to plan")
                .value_hint(ValueHint::FilePath)
                .required(true),
        )
        .arg(json_flag());

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
    let job_path = arg_matches
        .get_one::<String>("path")
        .context("path is required")?;
    let (rendered, _) = render_job_spec(job_path)?;

    let request = tonic::Request::new(PlanJobRequest {
        parsed_jobspec: rendered,
    });

    let response = client.plan_job(request).await?.into_inner();
    if is_json {
        if let Ok(json) = to_string(&response) {
            println!("{json}");
        }
    } else {
        println!("{}", response.message);
        let printable_allocations: Vec<JobPlanPrint> =
            response.allocations.into_iter().map(Into::into).collect();
        print_table(printable_allocations);
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

    let (rendered, submissions) = render_job_spec(job_path)?;

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

fn render_job_spec(job_path: &str) -> R<(String, Vec<Submission>)> {
    let mut vm = JsonnetVm::new();
    let mut submissions = vec![];

    let content = std::fs::read_to_string(job_path)?;
    let initial_sub = Submission {
        path: job_path.to_string(),
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

    let rendered = vm
        .evaluate_file(job_path)
        .map_err(|e| anyhow!("failed to evaluate job spec {job_path}: {e}"))?
        .to_string();

    Ok((rendered, submissions))
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
