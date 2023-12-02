mod certs;
mod cmd;
mod constants;
mod manager;
mod plugins;
mod storage;
mod util;
mod worker;

use clap::Command;
use log::debug;
use std::ffi::OsString;
use util::R;

fn cli() -> Command {
    Command::new("legion")
        .about("A simple, exensible, fast orchestrator")
        .bin_name("legion")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(cmd::job::config())
        .subcommand(cmd::start::config())
        .subcommand(cmd::join_token::config())
}

#[tokio::main]
async fn main() -> R<()> {
    env_logger::init();

    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("start", sub_matches)) => cmd::start::handle(sub_matches).await?,
        Some(("job", sub_matches)) => cmd::job::handle(sub_matches).await?,
        Some(("join-token", sub_matches)) => cmd::join_token::handle(sub_matches).await?,
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .get_many::<OsString>("")
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            debug!("Calling to {ext:?} with {args:?}");
        }
        _ => unreachable!(),
    };

    Ok(())
}
