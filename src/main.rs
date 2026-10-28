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
    let mcp_mode = matches!(matches.subcommand(), Some(("mcp", _)));
    // Servers: every `start` subcommand (manager, worker, gateway) and
    // `join`, which runs a worker. They log to stdout, where journald or a
    // container runtime reads it, keep JSON file logs, and may export OTLP.
    let server_mode = matches!(matches.subcommand_name(), Some("start" | "join"));
    // Everything else logs to stderr and writes no files: a one-shot
    // command's stdout is its output (`--json` envelopes among it), and the
    // MCP server's stdout is the JSON-RPC stream.
    init_tracing_with_options(enable_o11y, !server_mode, server_mode)?;

    // Long-running processes keep Rust's default of ignoring SIGPIPE, so a
    // peer hanging up surfaces as an EPIPE error they can handle. That
    // includes the gateway: its copy loop `splice`s into client sockets,
    // which raises SIGPIPE when the client has gone. Every other command is
    // a one-shot CLI whose stdout may be piped into `head`.
    // `legion app run` is a bundled app's process, run by its task.
    let app_run = matches!(
        matches.subcommand(),
        Some(("app", sub_matches)) if sub_matches.subcommand_name() == Some("run")
    );
    let long_running = server_mode
        || mcp_mode
        || app_run
        || matches!(
            matches.subcommand_name(),
            Some("guest-agent" | "guest-resume")
        );
    if !long_running {
        restore_default_sigpipe();
    }

    match matches.subcommand() {
        Some(("capabilities", sub_matches)) => cmd::capabilities::handle(sub_matches).await?,
        Some(("mcp", sub_matches)) => cmd::mcp::handle(sub_matches).await?,
        Some(("docs", sub_matches)) => cmd::docs::handle(sub_matches).await?,
        Some(("start", sub_matches)) => cmd::start::handle(sub_matches).await?,
        Some(("job", sub_matches)) => cmd::job::handle(sub_matches).await?,
        Some(("scaling", sub_matches)) => cmd::scaling::handle(sub_matches).await?,
        Some(("app", sub_matches)) => cmd::app::handle(sub_matches).await?,
        Some(("namespace", sub_matches)) => cmd::namespace::handle(sub_matches).await?,
        Some(("template", sub_matches)) => cmd::template::handle(sub_matches).await?,
        Some(("instance", sub_matches)) => cmd::instance::handle(sub_matches).await?,
        Some(("node", sub_matches)) => cmd::node::handle(sub_matches).await?,
        Some(("scaling", sub_matches)) => cmd::scaling::handle(sub_matches).await?,
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

/// Let the kernel end a one-shot CLI quietly when its reader goes away.
///
/// Rust ignores SIGPIPE at startup, so `legion raft status | head` turned the
/// closed pipe into an EPIPE that `println!` panics on. Restoring the default
/// disposition makes the CLI exit like `cat` or `grep` do: killed by SIGPIPE,
/// which the shell reports as status 141 (128 + 13). That is the convention
/// `set -o pipefail` scripts already expect, and it covers every print site
/// without threading write errors through each command.
fn restore_default_sigpipe() {
    #[cfg(unix)]
    // SAFETY: called once at startup, before this process writes to stdout.
    // The gRPC connection is unaffected: std and mio write TCP sockets with
    // MSG_NOSIGNAL (Linux) or SO_NOSIGPIPE (macOS), so a dropped manager
    // connection is still an error, not a signal.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}
