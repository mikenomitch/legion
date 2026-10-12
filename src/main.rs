use legion::cmd;
use legion::prelude::*;
use legion::tracing_util::init_tracing_with_options;

#[tokio::main]
async fn main() -> R<()> {
    let matches = cmd::cli().get_matches();
    let enable_o11y = matches!(
        matches.subcommand(),
        Some(("start", sub_matches))
            if matches!(
                sub_matches.subcommand(),
                Some(("manager", args)) | Some(("worker", args)) if args.get_flag("o11y")
            )
    );
    // The MCP server speaks JSON-RPC on stdout, so console logs must go to
    // stderr to avoid corrupting the protocol stream.
    let mcp_mode = matches!(matches.subcommand(), Some(("mcp", _)));
    // OTLP trace export is limited to long-running servers.
    let server_mode = matches!(
        matches.subcommand(),
        Some(("start", sub_matches))
            if matches!(
                sub_matches.subcommand(),
                Some(("manager", _)) | Some(("worker", _))
            )
    );
    init_tracing_with_options(enable_o11y, mcp_mode, server_mode)?;

    match matches.subcommand() {
        Some(("capabilities", sub_matches)) => cmd::capabilities::handle(sub_matches).await?,
        Some(("mcp", sub_matches)) => cmd::mcp::handle(sub_matches).await?,
        Some(("docs", sub_matches)) => cmd::docs::handle(sub_matches).await?,
        Some(("start", sub_matches)) => cmd::start::handle(sub_matches).await?,
        Some(("job", sub_matches)) => cmd::job::handle(sub_matches).await?,
        Some(("template", sub_matches)) => cmd::template::handle(sub_matches).await?,
        Some(("instance", sub_matches)) => cmd::instance::handle(sub_matches).await?,
        Some(("node", sub_matches)) => cmd::node::handle(sub_matches).await?,
        Some(("join-token", sub_matches)) => cmd::join_token::handle(sub_matches).await?,
        Some(("boot-ticket", sub_matches)) => cmd::boot_ticket::handle(sub_matches).await?,
        Some(("sandbox", sub_matches)) => cmd::sandbox::handle(sub_matches).await?,
        Some(("session", sub_matches)) => cmd::session::handle(sub_matches).await?,
        Some(("join", sub_matches)) => cmd::join::handle(sub_matches).await?,
        Some(("ledger", sub_matches)) => cmd::ledger::handle(sub_matches).await?,
        Some(("events", sub_matches)) => cmd::events::handle(sub_matches).await?,
        Some(("kv", sub_matches)) => cmd::kv::handle(sub_matches).await?,
        Some(("keyring", sub_matches)) => cmd::keyring::handle(sub_matches).await?,
        Some(("service", sub_matches)) => cmd::service::handle(sub_matches).await?,
        Some(("net", sub_matches)) => cmd::net::handle(sub_matches).await?,
        Some(("guest-agent", sub_matches)) => cmd::guest_agent::handle(sub_matches).await?,
        Some(("guest-resume", sub_matches)) => cmd::guest_resume::handle(sub_matches).await?,
        Some(("volume", sub_matches)) => cmd::volume::handle(sub_matches).await?,
        Some(("snapshot", sub_matches)) => cmd::snapshot::handle(sub_matches).await?,
        Some(("state", sub_matches)) => cmd::state::handle(sub_matches).await?,
        Some(("raft", sub_matches)) => cmd::raft::handle(sub_matches).await?,
        Some(("scheduler", sub_matches)) => cmd::scheduler::handle(sub_matches).await?,
        Some(("acl", sub_matches)) => cmd::acl::handle(sub_matches).await?,
        Some(("exec", sub_matches)) => cmd::exec::handle(sub_matches).await?,
        Some(("exec-ticket", sub_matches)) => cmd::exec_ticket::handle(sub_matches).await?,
        Some(("exec-key", sub_matches)) => cmd::exec_key::handle(sub_matches).await?,
        Some(("action", sub_matches)) => cmd::action::handle(sub_matches).await?,
        Some(("login", sub_matches)) => cmd::login::handle_login(sub_matches).await?,
        Some(("logout", sub_matches)) => cmd::login::handle_logout(sub_matches).await?,
        None => unreachable!(),
        Some((_, _)) => unreachable!(),
    };

    Ok(())
}
