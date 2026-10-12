use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;

pub fn config() -> Command {
    Command::new("raft")
        .about("Inspect and operate manager Raft control-plane state")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("status")
                .about("Show local manager raft status")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("history")
                .about("Show recent raft events")
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('n')
                        .value_parser(clap::value_parser!(u32))
                        .default_value("25")
                        .help("Maximum number of events to return"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("command")
                .about("Issue a raft command")
                .arg(
                    Arg::new("name")
                        .required(true)
                        .value_name("COMMAND")
                        .help("Command name (for example trigger-election, transfer-leader)"),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .value_name("NODE_ID")
                        .help("Optional target node id"),
                )
                .arg(
                    Arg::new("actor")
                        .long("actor")
                        .value_name("ACTOR")
                        .default_value("cli")
                        .help("Actor tag written into raft history"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("join-token")
                .about("Issue a raft manager join token")
                .arg(
                    Arg::new("actor")
                        .long("actor")
                        .value_name("ACTOR")
                        .default_value("cli")
                        .help("Actor tag written into raft history"),
                )
                .arg(json_flag()),
        )
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;
    match sub_matches.subcommand() {
        Some(("status", args)) => {
            let format = OutputFormat::from_matches(args);
            let reply = output::resolve(
                format,
                "failed to fetch raft status",
                client
                    .get_raft_status(crate::structs::GetRaftStatusRequest {})
                    .await,
            )?
            .into_inner();
            if format.is_json() {
                output::print_success(&reply.status);
                return Ok(());
            }
            let status = reply.status.context("raft status missing from response")?;
            println!(
                "enabled={} backend={} node={} role={} leader={} term={} commit={} applied={}",
                status.enabled,
                status.backend,
                status.node_id,
                status.role,
                status.leader_id.as_deref().unwrap_or("-"),
                status.term,
                status.commit_index,
                status.applied_index
            );
            println!(
                "voters={} learners={}",
                status.voters.join(","),
                status.learners.join(",")
            );
            // Per-lane quorum health (plan §7-C3): rendered in BOTH
            // topologies — in `single` the one control group's line reads
            // without a lane prefix. The ⚠ advisory appears when the voter
            // count is even or below raft.expectedVoters.
            for line in quorum_lines(&status) {
                println!("{line}");
            }
            // Per-lane detail matters when more than one raft group is
            // active (the `lanes` topology, where each lane elects its own
            // leader); `single` topology has one group whose numbers are
            // the headline lines above. When lanes disagree on a leader,
            // the headline leader column above already summarizes them
            // per lane (`control=…,scheduler=…,status=…`).
            if status.lanes.len() > 1 {
                for lane in &status.lanes {
                    println!(
                        "lane={} role={} leader={} term={} commit={} applied={}",
                        lane.lane,
                        lane.role,
                        lane.leader_id.as_deref().unwrap_or("-"),
                        lane.term,
                        lane.commit_index,
                        lane.applied_index
                    );
                }
            }
        }
        Some(("history", args)) => {
            let limit = args.get_one::<u32>("limit").copied().unwrap_or(25);
            let format = OutputFormat::from_matches(args);
            let reply = output::resolve(
                format,
                "failed to fetch raft history",
                client
                    .list_raft_history(crate::structs::ListRaftHistoryRequest {
                        limit: Some(limit),
                    })
                    .await,
            )?
            .into_inner();
            if format.is_json() {
                output::print_success(&reply.events);
                return Ok(());
            }
            for event in reply.events {
                println!(
                    "#{} {} [{}] actor={} target={} term={} leader={} {}",
                    event.event_id,
                    event.created_at,
                    event.event_type,
                    event.actor,
                    event.target.unwrap_or_else(|| "-".to_string()),
                    event.term,
                    event.leader_id.unwrap_or_else(|| "-".to_string()),
                    event.message,
                );
            }
        }
        Some(("command", args)) => {
            let command = args
                .get_one::<String>("name")
                .context("missing command")?
                .to_string();
            let target_node_id = args.get_one::<String>("target").cloned();
            let actor = args
                .get_one::<String>("actor")
                .cloned()
                .unwrap_or_else(|| "cli".to_string());
            let format = OutputFormat::from_matches(args);
            let reply = output::resolve(
                format,
                "failed to issue raft command",
                client
                    .issue_raft_command(crate::structs::IssueRaftCommandRequest {
                        command,
                        target_node_id,
                        actor: Some(actor),
                    })
                    .await,
            )?
            .into_inner();
            if format.is_json() {
                output::print_success(&reply);
                return Ok(());
            }
            let status = reply.status.context("raft status missing from response")?;
            let event = reply.event.context("raft event missing from response")?;
            println!(
                "{} -> role={} leader={} term={} ({})",
                reply.command,
                status.role,
                status.leader_id.unwrap_or_else(|| "-".to_string()),
                status.term,
                event.message,
            );
            // Quorum/parity advisory produced by the command (JC-29: the
            // command succeeded; the warning is ergonomic, not an error).
            if let Some(warning) = &reply.warning {
                println!("⚠ {warning}");
            }
        }
        Some(("join-token", args)) => {
            let actor = args
                .get_one::<String>("actor")
                .cloned()
                .unwrap_or_else(|| "cli".to_string());
            let format = OutputFormat::from_matches(args);
            let reply = output::resolve(
                format,
                "failed to issue raft join token",
                client
                    .get_raft_join_token(crate::structs::GetRaftJoinTokenRequest {
                        actor: Some(actor),
                    })
                    .await,
            )?
            .into_inner();
            if format.is_json() {
                output::print_success(&reply);
                return Ok(());
            }
            println!("token={}", reply.token);
            println!("expires_at={}", reply.expires_at);
        }
        _ => {}
    }
    Ok(())
}

/// The per-lane quorum lines of `legion raft status` (plan §7-C3): one line
/// per active raft group — `voters N (quorum Q) — survives F failure(s)` —
/// with a `⚠` + the plain-language advisory appended when the manager
/// reports one (a membership change still in its joint configuration, an
/// even voter count, or fewer voters than `raft.expectedVoters`). Under `single` topology the one control group's
/// line carries no lane prefix; under `lanes` each line names its lane.
fn quorum_lines(status: &crate::structs::RaftStatus) -> Vec<String> {
    status
        .lanes
        .iter()
        .map(|lane| {
            let prefix = if status.lanes.len() > 1 {
                format!("lane {}: ", lane.lane)
            } else {
                String::new()
            };
            let mut line = format!(
                "{prefix}voters {} (quorum {}) — survives {} failure(s)",
                lane.voters, lane.quorum, lane.survives
            );
            if let Some(warning) = &lane.warning {
                line.push_str(&format!(" ⚠ {warning}"));
            }
            line
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{RaftLaneStatus, RaftStatus};

    fn lane(name: &str, voters: u64, warning: Option<&str>) -> RaftLaneStatus {
        RaftLaneStatus {
            lane: name.to_string(),
            role: "primary".to_string(),
            leader_id: Some("mgr-a".to_string()),
            term: 1,
            commit_index: 1,
            applied_index: 1,
            voters,
            quorum: voters / 2 + 1,
            survives: (voters - 1) / 2,
            warning: warning.map(str::to_string),
        }
    }

    fn status_with_lanes(lanes: Vec<RaftLaneStatus>) -> RaftStatus {
        RaftStatus {
            enabled: true,
            backend: "raft".to_string(),
            node_id: "mgr-a".to_string(),
            cluster_id: "legion-raft-test".to_string(),
            role: "primary".to_string(),
            leader_id: Some("mgr-a".to_string()),
            term: 1,
            commit_index: 1,
            applied_index: 1,
            lease_held: true,
            voters: vec!["mgr-a".to_string()],
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 900,
            raft_library: "openraft-0.9".to_string(),
            lanes,
        }
    }

    /// `single` topology: one unprefixed quorum line, healthy 3-voter shape.
    #[test]
    fn quorum_line_renders_for_single_topology() {
        let status = status_with_lanes(vec![lane("control", 3, None)]);
        let lines = quorum_lines(&status);
        assert_eq!(lines, vec!["voters 3 (quorum 2) — survives 1 failure(s)"]);
    }

    /// `lanes` topology: one prefixed line per lane, with the ⚠ advisory on
    /// an even-voter lane.
    #[test]
    fn quorum_lines_render_for_lanes_topology_with_warning() {
        let advisory = "2 voters need both alive to commit — this cluster survives zero \
                        failures. Use 1, 3, or 5 voters; add one more manager or remove one.";
        let status = status_with_lanes(vec![
            lane("control", 2, Some(advisory)),
            lane("scheduler", 2, Some(advisory)),
            lane("status", 2, Some(advisory)),
        ]);
        let lines = quorum_lines(&status);
        assert_eq!(lines.len(), 3);
        assert!(
            lines[0].starts_with("lane control: voters 2 (quorum 2) — survives 0 failure(s)"),
            "{}",
            lines[0]
        );
        for line in &lines {
            assert!(line.contains("⚠"), "{line}");
            assert!(line.contains("Use 1, 3, or 5 voters"), "{line}");
        }
        assert!(lines[1].starts_with("lane scheduler: "), "{}", lines[1]);
        assert!(lines[2].starts_with("lane status: "), "{}", lines[2]);
    }
}
