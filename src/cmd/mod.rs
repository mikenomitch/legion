// The cmd directory contains all the subcommands for the CLI.

pub mod acl;
pub mod action;
pub mod app;
pub mod boot_ticket;
pub mod capabilities;
pub mod docs;
pub mod events;
pub mod exec;
pub mod exec_key;
pub mod exec_ticket;
pub mod guest_agent;
pub mod guest_resume;
pub mod helpers;
pub mod instance;
pub mod job;
pub mod join;
pub mod join_token;
pub mod keyring;
pub mod kv;
pub mod ledger;
pub mod login;
pub mod mcp;
pub mod namespace;
pub mod namespace_policy;
pub mod net;
pub mod net_plugin;
pub mod node;
pub mod oidc;
pub mod output;
pub mod raft;
pub mod sandbox;
pub mod scaling;
pub mod scaling_demand;
pub mod scheduler;
pub mod service;
pub mod session;
pub mod snapshot;
pub mod start;
pub mod state;
pub mod template;
pub mod volume;

use crate::prelude::*;

/// The whole `legion` command tree. It lives in the library rather than in
/// `main.rs` so tests can walk it — every command must accept `--json`
/// (AGENTS.md, "Agent-Friendly Contributions"; see `tests` below).
pub fn cli() -> Command {
    helpers::with_namespace_flags(base_cli())
}

fn base_cli() -> Command {
    Command::new("legion")
        .about("A simple, extensible, fast orchestrator")
        .bin_name("legion")
        .version(env!("CARGO_PKG_VERSION"))
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(capabilities::config())
        .subcommand(mcp::config())
        .subcommand(docs::config())
        .subcommand(job::config())
        .subcommand(scaling::config())
        .subcommand(app::config())
        .subcommand(namespace::config())
        .subcommand(template::config())
        .subcommand(instance::config())
        .subcommand(node::config())
        .subcommand(scaling::config())
        .subcommand(start::config())
        .subcommand(join_token::config())
        .subcommand(boot_ticket::config())
        .subcommand(sandbox::config())
        .subcommand(session::config())
        .subcommand(join::config())
        .subcommand(ledger::config())
        .subcommand(events::config())
        .subcommand(kv::config())
        .subcommand(keyring::config())
        .subcommand(service::config())
        .subcommand(net::config())
        .subcommand(guest_agent::config())
        .subcommand(guest_resume::config())
        .subcommand(volume::config())
        .subcommand(snapshot::config())
        .subcommand(state::config())
        .subcommand(raft::config())
        .subcommand(scheduler::config())
        .subcommand(acl::config())
        .subcommand(exec::config())
        .subcommand(exec_ticket::config())
        .subcommand(exec_key::config())
        .subcommand(action::config())
        .subcommand(login::login_config())
        .subcommand(login::logout_config())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Commands that are not request/response operations, so a JSON envelope
    /// has nothing to describe: long-running daemons, a protocol server
    /// whose stdout IS the JSON-RPC stream, and the in-guest helpers.
    const NO_ENVELOPE: &[&str] = &[
        "legion start",
        "legion start manager",
        "legion start worker",
        "legion start gateway",
        "legion join",
        "legion mcp serve",
        // The in-task entrypoint of a bundled app: a long-running process.
        "legion app run",
        "legion guest-agent",
        "legion guest-resume",
    ];

    fn walk(command: &Command, path: &str, missing: &mut Vec<String>) {
        let path = if path.is_empty() {
            command.get_name().to_string()
        } else {
            format!("{path} {}", command.get_name())
        };
        let subcommands: Vec<&Command> = command
            .get_subcommands()
            .filter(|sub| sub.get_name() != "help")
            .collect();
        // A command runs on its own when it has no subcommands, or when it
        // does not require one (`legion join-token` is `create`).
        let runnable = subcommands.is_empty() || !command.is_subcommand_required_set();
        let has_json = command
            .get_arguments()
            .any(|arg| arg.get_id() == "json" && arg.get_long() == Some("json"));
        if runnable && !has_json && path != "legion" && !NO_ENVELOPE.contains(&path.as_str()) {
            missing.push(path.clone());
        }
        for sub in subcommands {
            walk(sub, &path, missing);
        }
    }

    /// The namespace flag goes exactly where a namespace means something: on
    /// commands that act on a job, never on cluster-scoped ones, where it
    /// would look like a scope and silently be ignored (kubectl#1413).
    #[test]
    fn namespace_flags_only_on_namespaced_commands() {
        let cli = cli();
        cli.clone().debug_assert();
        let find = |path: &[&str]| -> Command {
            let mut command = cli.clone();
            for name in path {
                command = command
                    .find_subcommand(name)
                    .unwrap_or_else(|| panic!("no subcommand {name}"))
                    .clone();
            }
            command
        };
        let has_namespace = |command: &Command| {
            command
                .get_arguments()
                .any(|arg| arg.get_id() == "namespace")
        };
        for path in [
            &["job", "run"][..],
            &["job", "info"],
            &["job", "list"],
            &["instance", "list"],
            &["snapshot", "list"],
            &["service", "resolve"],
            &["net", "policy", "show"],
        ] {
            assert!(has_namespace(&find(path)), "{path:?} should take -n");
        }
        for path in [
            &["node", "list"][..],
            &["acl", "policy", "list"],
            &["kv", "get"],
            &["namespace", "list"],
        ] {
            assert!(!has_namespace(&find(path)), "{path:?} must not take -n");
        }
        assert!(cli
            .clone()
            .try_get_matches_from(["legion", "node", "list", "-n", "prod"])
            .is_err());
    }

    /// AGENTS.md: "Every CLI command must accept `--json`/`-j`". This walks
    /// the real tree, so a new command without the flag fails here.
    #[test]
    fn every_command_accepts_json() {
        let mut missing = Vec::new();
        walk(&cli(), "", &mut missing);
        assert!(
            missing.is_empty(),
            "commands without --json (add `crate::cmd::helpers::json_flag()` and emit the \
             envelope through `crate::cmd::output`): {missing:#?}"
        );
    }

    #[test]
    fn the_command_tree_is_well_formed() {
        cli().debug_assert();
    }
}
