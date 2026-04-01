use legion::cmd;
use legion::prelude::*;
use legion::tracing_util::init_tracing;

fn cli() -> Command {
    Command::new("legion")
        .about("A simple, exensible, fast orchestrator")
        .bin_name("legion")
        .version(env!("CARGO_PKG_VERSION"))
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(cmd::docs::config())
        .subcommand(cmd::job::config())
        .subcommand(cmd::node::config())
        .subcommand(cmd::start::config())
        .subcommand(cmd::join_token::config())
        .subcommand(cmd::join::config())
        .subcommand(cmd::ledger::config())
        .subcommand(cmd::kv::config())
        .subcommand(cmd::state::config())
        .subcommand(cmd::raft::config())
}

#[tokio::main]
async fn main() -> R<()> {
    let matches = cli().get_matches();
    let enable_o11y = matches!(
        matches.subcommand(),
        Some(("start", sub_matches))
            if matches!(
                sub_matches.subcommand(),
                Some(("manager", args)) | Some(("worker", args)) if args.get_flag("o11y")
            )
    );
    init_tracing(enable_o11y)?;

    match matches.subcommand() {
        Some(("docs", sub_matches)) => cmd::docs::handle(sub_matches).await?,
        Some(("start", sub_matches)) => cmd::start::handle(sub_matches).await?,
        Some(("job", sub_matches)) => cmd::job::handle(sub_matches).await?,
        Some(("node", sub_matches)) => cmd::node::handle(sub_matches).await?,
        Some(("join-token", sub_matches)) => cmd::join_token::handle(sub_matches).await?,
        Some(("join", sub_matches)) => cmd::join::handle(sub_matches).await?,
        Some(("ledger", sub_matches)) => cmd::ledger::handle(sub_matches).await?,
        Some(("kv", sub_matches)) => cmd::kv::handle(sub_matches).await?,
        Some(("state", sub_matches)) => cmd::state::handle(sub_matches).await?,
        Some(("raft", sub_matches)) => cmd::raft::handle(sub_matches).await?,
        None => unreachable!(),
        Some((_, _)) => unreachable!(),
    };

    Ok(())
}
