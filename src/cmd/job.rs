use std::vec;

use crate::prelude::*;

use crate::cmd::helpers::print_table;
use crate::structs::manager_client::ManagerClient;
use crate::structs::{
    DestroyJobRequest, GetJobInfoRequest, GetJobSpecRequest, JobPrint, ListJobsRequest,
    RunJobRequest, StopJobRequest, Submission,
};
use jsonnet::JsonnetVm;

pub fn config() -> Command {
    let run_cmd = Command::new("run")
        .about("Run a job")
        .arg(arg!(path: [PATH]));

    let stop_cmd = Command::new("stop")
        .about("Stop a job")
        .arg(arg!(name: [NAME]));

    let destroy_cmd = Command::new("destroy")
        .about("Destroy a job")
        .arg(arg!(name: [NAME]));

    let info_cmd = Command::new("info")
        .about("Get job info")
        .arg(arg!(name: [NAME]));

    let spec_cmd = Command::new("spec")
        .about("Get job spec used to define the job")
        .arg(arg!(name: [NAME]));

    let list_cmd = Command::new("list").about("List all jobs");

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(run_cmd)
        .subcommand(info_cmd)
        .subcommand(list_cmd)
        .subcommand(stop_cmd)
        .subcommand(spec_cmd)
        .subcommand(destroy_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", matches)) => run_job(matches).await,
        Some(("stop", matches)) => stop_job(matches).await,
        Some(("spec", matches)) => get_job_spec(matches).await,
        Some(("destroy", matches)) => destroy_job(matches).await,
        Some(("list", _)) => list_jobs().await,
        Some(("info", matches)) => get_info(matches).await,
        _ => unreachable!(),
    }
}

#[instrument(err, skip_all)]
async fn stop_job(arg_matches: &clap::ArgMatches) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);

    let request = tonic::Request::new(StopJobRequest {
        name: job_name.to_owned(),
    });

    let response = client.stop_job(request).await?;
    println!("{}", response.into_inner().message);

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_job_spec(arg_matches: &clap::ArgMatches) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);

    let request = tonic::Request::new(GetJobSpecRequest {
        name: job_name.to_owned(),
    });

    let response = client.get_job_spec(request).await?;

    response.into_inner().submissions.iter().for_each(|s| {
        println!("=============================");
        println!("{}:\n", s.path);
        println!("{}", s.content);
        println!("=============================");
    });

    Ok(())
}

#[instrument(err, skip_all)]
async fn destroy_job(arg_matches: &clap::ArgMatches) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);

    let request = tonic::Request::new(DestroyJobRequest {
        name: job_name.to_owned(),
    });

    let response = client.destroy_job(request).await?;
    println!("{}", response.into_inner().message);

    Ok(())
}

#[instrument(err, skip_all)]
async fn run_job(arg_matches: &clap::ArgMatches) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;

    let job_path = arg_matches
        .get_one::<String>("path")
        .map_or("./configs/jobs/demo.jsonnet", String::as_str);

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

    let res = vm.evaluate_file(job_path);

    match res {
        Ok(s) => {
            let request = tonic::Request::new(RunJobRequest {
                parsed_jobspec: s.to_string(),
                submissions,
            });

            let response = client.run_job(request).await?;

            println!("{}", response.into_inner().message);

            Ok(())
        }
        Err(e) => {
            error!("Error: {}", e.to_string());
            Err(e.to_string().into())
        }
    }
}

#[instrument(err, skip_all)]
async fn list_jobs() -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let request = tonic::Request::new(ListJobsRequest {});
    let response = client.list_jobs(request).await?;
    let jobs_list = response.into_inner().jobs;
    let printable_jobs: Vec<JobPrint> = jobs_list.into_iter().map(Into::into).collect();
    print_table(printable_jobs);

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_info(arg_matches: &clap::ArgMatches) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let job_name = arg_matches.get_one::<String>("name").ok_or("need name")?;
    let request = tonic::Request::new(GetJobInfoRequest {
        name: job_name.to_owned(),
    });
    let response = client.get_job_info(request).await?;

    let printable_job: JobPrint = response.into_inner().job.ok_or("no job found")?.into();
    // println!("{:?}", printable_job);
    print_table(vec![printable_job]);

    Ok(())
}
