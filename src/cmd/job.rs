use clap::{arg, Command};

pub fn config() -> Command {
    let job_run_cmd = Command::new("run")
        .about("Run a job")
        .arg(arg!(path: [PATH]));
    let job_status_cmd = Command::new("status").about("Get job status");

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(job_run_cmd)
        .subcommand(job_status_cmd)
}

pub fn handle(sub_matches: &clap::ArgMatches) {
    match sub_matches.subcommand() {
        Some(("run", _)) => println!("Running job"),
        Some(("status", _)) => println!("Getting job status"),
        _ => unreachable!(),
    }
}
