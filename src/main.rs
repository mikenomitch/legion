use clap::Parser;
use colored::*;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(name = "cargo")]
#[command(bin_name = "cargo")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    name: String,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

fn main() {
    // SUB-COMMMANDS
    // AGENT COMMAND
    let agent_cmd = clap::Command::new("agent").about("Start an agent");

    // JOB COMMAND
    let job_run_cmd = clap::Command::new("run").about("Run a job");
    let job_status_cmd = clap::Command::new("status").about("Get job status");

    let job_cmd = clap::Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(job_run_cmd)
        .subcommand(job_status_cmd);

    // TOP LEVEL COMMAND
    let cmd = clap::Command::new("legion")
        .about("A simple, exensible, fast orchestrator")
        .bin_name("legion")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(agent_cmd)
        .subcommand(job_cmd);

    let matches = cmd.get_matches();
    let matches = match matches.subcommand() {
        Some(("example", matches)) => matches,
        _ => unreachable!("clap should ensure we don't get here"),
    };

    let manifest_path = matches
        .get_one::<std::path::PathBuf>("manifest-path")
        .unwrap()
        .to_str()
        .unwrap();

    println!("{}", manifest_path.blue());
}
