mod certs;
mod cmd;
mod constants;
mod manager;
mod prelude;
mod storage;
mod structs;
mod tracing_util;
mod util;
mod worker;

use crate::prelude::*;
use crate::tracing_util::init_tracing;

use std::ffi::OsString;

fn cli() -> Command {
    Command::new("legion")
        .about("A simple, exensible, fast orchestrator")
        .bin_name("legion")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(cmd::job::config())
        .subcommand(cmd::node::config())
        .subcommand(cmd::start::config())
        .subcommand(cmd::join_token::config())
        .subcommand(cmd::join::config())
}

#[tokio::main]
async fn main() -> R<()> {
    init_tracing()?;

    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("start", sub_matches)) => cmd::start::handle(sub_matches).await?,
        Some(("job", sub_matches)) => cmd::job::handle(sub_matches).await?,
        Some(("node", sub_matches)) => cmd::node::handle(sub_matches).await?,
        Some(("join-token", sub_matches)) => cmd::join_token::handle(sub_matches).await?,
        Some(("join", sub_matches)) => cmd::join::handle(sub_matches).await?,
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .get_many::<OsString>("")
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            info!("Calling to {ext:?} with {args:?}");
        }
        _ => unreachable!(),
    };

    Ok(())
}
