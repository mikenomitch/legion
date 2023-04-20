mod cmd;
mod constants;
mod manager;
mod raft;
mod types;
use clap::Command;
use std::ffi::OsString;
use types::Res;

fn cli() -> Command {
    Command::new("legion")
        .about("A simple, exensible, fast orchestrator")
        .bin_name("legion")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(cmd::job::config())
        .subcommand(cmd::start::config())
}

#[tokio::main]
async fn main() -> Res<()> {
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("start", sub_matches)) => cmd::start::handle(sub_matches).await?,
        Some(("job", sub_matches)) => cmd::job::handle(sub_matches).await?,
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .get_many::<OsString>("")
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            println!("Calling to {ext:?} with {args:?}");
        }
        _ => unreachable!(),
    };

    Ok(())
}
