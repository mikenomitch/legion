//! `legion events` — list and follow the scheduler's event stream.
//!
//! The manager journals an event whenever the scheduler does something
//! observable (evals, placements, node/job lifecycle, rollouts). `events list`
//! reads a window of that journal with cursor-based pagination; `events
//! follow` tails it live over the `StreamEvents` server-streaming RPC.
//!
//! In JSON mode `events list` emits the standard envelope once, while `events
//! follow` emits one `{ "ok": true, "data": <event> }` envelope per line
//! (NDJSON) so agents can consume the stream incrementally.

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{ListEventsRequest, SchedulerEvent, StreamEventsRequest};

fn filter_args() -> [Arg; 6] {
    [
        Arg::new("kind")
            .long("kind")
            .help("Filter by event kind, exact or category prefix (e.g. 'eval' matches eval.*)"),
        Arg::new("job").long("job").help("Filter by exact job ID"),
        Arg::new("node")
            .long("node")
            .help("Filter by exact node ID"),
        Arg::new("level")
            .long("level")
            .value_parser(["info", "warn", "error"])
            .help("Filter by severity level"),
        Arg::new("instance")
            .long("instance")
            .help("Filter to events about one stateful instance (its metadata or its allocations)"),
        Arg::new("allocation")
            .long("allocation")
            .value_parser(clap::value_parser!(i64))
            .help("Filter by exact allocation ID"),
    ]
}

fn after_id_arg() -> Arg {
    Arg::new("after-id")
        .long("after-id")
        .value_parser(clap::value_parser!(i64))
        .help("Only events with ID greater than this cursor")
}

pub fn config() -> Command {
    let list_cmd = Command::new("list")
        .about("List recent scheduler events (oldest first)")
        .arg(after_id_arg())
        .arg(
            Arg::new("limit")
                .long("limit")
                .value_parser(clap::value_parser!(u32))
                .help("Maximum events to return (default 100, max 1000)"),
        )
        .args(filter_args())
        .arg(json_flag());

    let follow_cmd = Command::new("follow")
        .about("Stream scheduler events live (Ctrl-C to stop)")
        .arg(after_id_arg())
        .args(filter_args())
        .arg(json_flag());

    Command::new("events")
        .about("Inspect the scheduler event stream")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(list_cmd)
        .subcommand(follow_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", matches)) => list_events(matches).await,
        Some(("follow", matches)) => follow_events(matches).await,
        _ => unreachable!(),
    }
}

fn opt_string(matches: &ArgMatches, key: &str) -> Option<String> {
    matches.get_one::<String>(key).cloned()
}

async fn list_events(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let request = ListEventsRequest {
        after_id: matches.get_one::<i64>("after-id").copied(),
        limit: matches.get_one::<u32>("limit").copied(),
        kind: opt_string(matches, "kind"),
        job_id: opt_string(matches, "job"),
        node_id: opt_string(matches, "node"),
        level: opt_string(matches, "level"),
        instance_id: opt_string(matches, "instance"),
        allocation_id: matches.get_one::<i64>("allocation").copied(),
    };

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list scheduler events",
        client.list_events(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }

    if reply.events.is_empty() {
        println!("No scheduler events recorded.");
        return Ok(());
    }
    for event in &reply.events {
        println!("{}", format_event_line(event));
    }
    println!(
        "\n{} event(s); latest event id: {}",
        reply.events.len(),
        reply.latest_id
    );

    Ok(())
}

async fn follow_events(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let request = StreamEventsRequest {
        after_id: matches.get_one::<i64>("after-id").copied(),
        kind: opt_string(matches, "kind"),
        job_id: opt_string(matches, "job"),
        node_id: opt_string(matches, "node"),
        level: opt_string(matches, "level"),
        instance_id: opt_string(matches, "instance"),
        allocation_id: matches.get_one::<i64>("allocation").copied(),
    };

    let mut client = connect_manager_client().await?;
    let mut stream = output::resolve(
        format,
        "failed to open scheduler event stream",
        client.stream_events(request).await,
    )?
    .into_inner();

    if !format.is_json() {
        eprintln!("Following scheduler events (Ctrl-C to stop)...");
    }

    loop {
        match stream.message().await {
            Ok(Some(event)) => {
                if format.is_json() {
                    // NDJSON: one success envelope per event line.
                    println!("{}", serde_json::json!({ "ok": true, "data": event }));
                } else {
                    println!("{}", format_event_line(&event));
                }
                // stdout is block-buffered when piped; a live tail must
                // deliver each event as soon as it is printed.
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
            Ok(None) => return Ok(()),
            Err(status) => {
                return output::resolve(
                    format,
                    "scheduler event stream failed",
                    Err::<(), tonic::Status>(status),
                )
                .map(|_| ());
            }
        }
    }
}

/// One human-readable line per event:
/// `<time>  <LEVEL>  <kind>  <scopes>  <message>`
fn format_event_line(event: &SchedulerEvent) -> String {
    let mut scopes = Vec::new();
    if let Some(job_id) = event.job_id.as_deref() {
        scopes.push(format!("job={job_id}"));
    }
    if let Some(node_id) = event.node_id.as_deref() {
        scopes.push(format!("node={node_id}"));
    }
    if let Some(rollout_id) = event.rollout_id {
        scopes.push(format!("rollout={rollout_id}"));
    }
    if let Some(allocation_id) = event.allocation_id {
        scopes.push(format!("alloc={allocation_id}"));
    }
    let scopes = if scopes.is_empty() {
        String::new()
    } else {
        format!("  [{}]", scopes.join(" "))
    };

    format!(
        "{}  {:<5}  {:<24}{}  {}",
        event.created_at,
        event.level.to_uppercase(),
        event.kind,
        scopes,
        event.message
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_list_with_filters() {
        let matches = config()
            .try_get_matches_from([
                "events",
                "list",
                "--kind",
                "eval",
                "--job",
                "demo",
                "--level",
                "warn",
                "--after-id",
                "42",
                "--limit",
                "10",
                "--json",
            ])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "list");
        assert_eq!(sub.get_one::<String>("kind").unwrap(), "eval");
        assert_eq!(sub.get_one::<i64>("after-id").copied(), Some(42));
        assert_eq!(sub.get_one::<u32>("limit").copied(), Some(10));
        assert!(sub.get_flag("json"));
    }

    #[test]
    fn config_parses_follow() {
        let matches = config()
            .try_get_matches_from(["events", "follow", "--node", "node-1"])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "follow");
        assert_eq!(sub.get_one::<String>("node").unwrap(), "node-1");
        assert!(!sub.get_flag("json"));
    }

    #[test]
    fn config_rejects_unknown_level() {
        assert!(config()
            .try_get_matches_from(["events", "list", "--level", "noisy"])
            .is_err());
    }

    #[test]
    fn event_line_includes_scopes_and_message() {
        let event = SchedulerEvent {
            id: 7,
            kind: "allocation.placed".into(),
            level: "info".into(),
            job_id: Some("demo".into()),
            node_id: Some("node-1".into()),
            eval_id: None,
            allocation_id: Some(12),
            rollout_id: None,
            message: "Placed 1 allocation(s) for job demo".into(),
            metadata_json: "{}".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        let line = format_event_line(&event);
        assert!(line.contains("INFO"));
        assert!(line.contains("allocation.placed"));
        assert!(line.contains("job=demo"));
        assert!(line.contains("node=node-1"));
        assert!(line.contains("alloc=12"));
        assert!(line.contains("Placed 1 allocation(s)"));
    }
}
