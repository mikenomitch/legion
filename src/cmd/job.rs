use crate::prelude::*;

use crate::structs::manager_client::ManagerClient;
use crate::structs::{
    DestroyJobRequest, GetJobSpecRequest, GetJobStatusRequest, Job, ListJobsRequest, RunJobRequest,
    StopJobRequest, Submission,
};
use jsonnet::JsonnetVm;

pub fn config() -> Command {
    let job_run_cmd = Command::new("run")
        .about("Run a job")
        .arg(arg!(path: [PATH]));

    let job_stop_cmd = Command::new("stop")
        .about("Stop a job")
        .arg(arg!(name: [NAME]));

    let job_destroy_cmd = Command::new("destroy")
        .about("Destroy a job")
        .arg(arg!(name: [NAME]));

    let job_status_cmd = Command::new("status")
        .about("Get job status")
        .arg(arg!(name: [NAME]));

    let job_spec_cmd = Command::new("spec")
        .about("Get job spec used to define the job")
        .arg(arg!(name: [NAME]));

    let job_list_cmd = Command::new("list").about("List all jobs");

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(job_run_cmd)
        .subcommand(job_status_cmd)
        .subcommand(job_list_cmd)
        .subcommand(job_stop_cmd)
        .subcommand(job_spec_cmd)
        .subcommand(job_destroy_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", matches)) => run_job(matches).await,
        Some(("stop", matches)) => stop_job(matches).await,
        Some(("spec", matches)) => get_job_spec(matches).await,
        Some(("destroy", matches)) => destroy_job(matches).await,
        Some(("list", _)) => list_jobs().await,
        Some(("status", matches)) => get_status(matches).await,
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
    println!("RESPONSE={:?}", response);

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
    println!("RESPONSE={:?}", response.into_inner().submissions);

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
    println!("RESPONSE={:?}", response);

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
        println!("IMPORT: base={:?}, rel={:?}", base, rel);
        let path = std::path::Path::new(base).join(rel);
        let path_str = path.to_str().unwrap();
        println!("PATH={:?}", path_str);
        let contents = std::fs::read_to_string(&path).unwrap();

        let sub = Submission {
            path: path_str.to_string(),
            content: contents.to_string(),
            is_primary: false,
        };

        submissions.push(sub);

        println!("RAW_CONTENT={:?}", contents);
        Ok((base.into(), contents.to_string()))
    });

    let res = vm.evaluate_file(job_path);

    println!("SUBMISSIONS={:?}", submissions);

    match res {
        Ok(s) => {
            let request = tonic::Request::new(RunJobRequest {
                parsed_jobspec: s.to_string(),
                submissions,
            });

            let response = client.run_job(request).await?;
            println!("RESPONSE={:?}", response);
            Ok(())
        }
        Err(e) => {
            error!("{}", e);
            Err("Oh no".into())
        }
    }
}

#[instrument(err, skip_all)]
async fn list_jobs() -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let request = tonic::Request::new(ListJobsRequest {});
    let response = client.list_jobs(request).await?;
    let jobs_list = response.into_inner().jobs;
    let jobs: Vec<Job> = jobs_list.into_iter().map(|j| j.into()).collect();

    println!("{:?}", jobs);

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_status(arg_matches: &clap::ArgMatches) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let job_name = arg_matches.get_one::<String>("name").ok_or("need name")?;
    let request = tonic::Request::new(GetJobStatusRequest {
        name: job_name.to_owned(),
    });
    let response = client.get_job_status(request).await?;
    println!("{:?}", response.into_inner().job.unwrap());

    Ok(())
}
