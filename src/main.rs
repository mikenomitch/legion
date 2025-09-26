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

fn cli() -> Command {
    Command::new("legion")
        .about("A simple, exensible, fast orchestrator")
        .bin_name("legion")
        .version(env!("CARGO_PKG_VERSION"))
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(cmd::job::config())
        .subcommand(cmd::node::config())
        .subcommand(cmd::start::config())
        .subcommand(cmd::join_token::config())
        .subcommand(cmd::join::config())
        .subcommand(cmd::ledger::config())
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
        Some(("ledger", sub_matches)) => cmd::ledger::handle(sub_matches).await?,
        None => unreachable!(),
        Some((_, _)) => unreachable!(),
    };

    Ok(())
}
