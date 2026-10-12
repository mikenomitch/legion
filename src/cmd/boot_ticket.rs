//! `legion boot-ticket` — mint and inspect boot tickets (docs/edge-boot.md).
//!
//! A boot ticket is a short-lived, narrow capability that lets an edge node
//! start exactly one workload without calling a manager. `create` prints the
//! signed token **once** — the manager keeps only its metadata, so a lost
//! ticket is re-minted, never recovered.
//!
//! Revocation is by TTL only: the fast path cannot consult a revocation list
//! without becoming the round trip the ticket exists to avoid. Operators who
//! need immediate revocation set short TTLs and re-mint.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::edge_boot::Gate;
use crate::structs::{BootTicket, IssueBootTicketRequest, ListBootTicketsRequest};
use tabled::Tabled;

pub fn config() -> Command {
    let create_cmd = Command::new("create")
        .about("Mint a boot ticket authorizing one edge start")
        .arg(
            Arg::new("job")
                .value_name("JOB")
                .required(true)
                .help("Job the ticket may start work for"),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .required(true)
                .help("Task within that job (a ticket names one task, never a group)"),
        )
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("INSTANCE")
                .help("Exact instance the ticket may start (the narrowest capability)"),
        )
        .arg(
            Arg::new("id-prefix")
                .long("id-prefix")
                .value_name("PREFIX")
                .conflicts_with("id")
                .help("Any instance with this prefix — one ticket per cohort, wider blast radius"),
        )
        .arg(
            Arg::new("pool")
                .long("pool")
                .value_name("POOL")
                .required(true)
                .help("Node pool the ticket may be redeemed in"),
        )
        .arg(
            Arg::new("gate")
                .long("gate")
                .value_name("GATE")
                .help("Ceiling on how far a node may get before ratification: stage|boot|serve"),
        )
        .arg(
            Arg::new("job-config")
                .long("job-config")
                .value_name("ID")
                .help("Pin to an exact job config (default: the job's current config)"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("TTL")
                .help("Ticket lifetime, e.g. 5m (default: edgeBoot.defaultTicketTtl)"),
        )
        .arg(
            Arg::new("uses")
                .long("uses")
                .value_name("N")
                .help("Redemptions allowed (default: 1 — single use)"),
        )
        .arg(
            Arg::new("idempotency-key")
                .long("idempotency-key")
                .value_name("KEY")
                .help("Retry-safe mint: a repeat with the same key returns the same ticket"),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list")
        .about("List outstanding boot tickets (token bytes are never shown)")
        .arg(
            Arg::new("job")
                .long("job")
                .value_name("JOB")
                .help("Restrict to one job"),
        )
        .arg(json_flag());

    Command::new("boot-ticket")
        .about("Mint and inspect boot tickets for edge starts")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(create_cmd)
        .subcommand(list_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("create", arg_matches)) => create(arg_matches).await,
        Some(("list", arg_matches)) => list(arg_matches).await,
        _ => {
            println!("Usage: legion boot-ticket <create|list>");
            Ok(())
        }
    }
}

async fn create(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = arg_matches
        .get_one::<String>("job")
        .cloned()
        .context("Must provide a job")?;
    let task = arg_matches
        .get_one::<String>("task")
        .cloned()
        .context("Must provide --task")?;
    let pool = arg_matches
        .get_one::<String>("pool")
        .cloned()
        .context("Must provide --pool")?;
    let gate = match arg_matches.get_one::<String>("gate") {
        Some(raw) => Gate::parse(raw)?,
        None => Gate::Boot,
    };
    let uses = match arg_matches.get_one::<String>("uses") {
        Some(raw) => raw.parse::<u32>().context("--uses must be a number")?,
        None => 1,
    };

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to issue boot ticket",
        client
            .issue_boot_ticket(tonic::Request::new(IssueBootTicketRequest {
                job,
                task,
                job_config_id: arg_matches
                    .get_one::<String>("job-config")
                    .cloned()
                    .unwrap_or_default(),
                instance_id: arg_matches
                    .get_one::<String>("id")
                    .cloned()
                    .unwrap_or_default(),
                instance_prefix: arg_matches
                    .get_one::<String>("id-prefix")
                    .cloned()
                    .unwrap_or_default(),
                pool,
                gate: gate.to_wire(),
                ttl: arg_matches
                    .get_one::<String>("ttl")
                    .cloned()
                    .unwrap_or_default(),
                uses,
                idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?;
    let reply = response.into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "jti": reply.jti,
            "ticket": reply.ticket,
            "jobConfigId": reply.job_config_id,
            "expiresAtUnix": reply.expires_at_unix,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("Boot ticket created ({}).\n", reply.jti);
        println!("This is the only time the ticket is shown:\n");
        println!("  {}\n", reply.ticket);
        println!("Pinned to job config: {}", reply.job_config_id);
        println!("Expires (unix): {}", reply.expires_at_unix);
        if reply.deduplicated {
            println!("(replayed an earlier mint for the same idempotency key)");
        }
    }
    Ok(())
}

#[derive(Tabled)]
struct BootTicketPrint {
    jti: String,
    job: String,
    task: String,
    scope: String,
    pool: String,
    gate: String,
    uses_left: u32,
    expires_at: i64,
}

fn ticket_json(ticket: &BootTicket) -> Value {
    json!({
        "jti": ticket.jti,
        "job": ticket.job,
        "task": ticket.task,
        "jobConfigId": ticket.job_config_id,
        "instanceId": ticket.instance_id,
        "instancePrefix": ticket.instance_prefix,
        "pool": ticket.pool,
        "gate": Gate::from_wire(ticket.gate).as_str(),
        "usesRemaining": ticket.uses_remaining,
        "expiresAtUnix": ticket.expires_at_unix,
        "accessorId": ticket.accessor_id,
        "createdAt": ticket.created_at,
    })
}

async fn list(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = arg_matches
        .get_one::<String>("job")
        .cloned()
        .unwrap_or_default();

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to list boot tickets",
        client
            .list_boot_tickets(tonic::Request::new(ListBootTicketsRequest { job }))
            .await,
    )?;
    let tickets = response.into_inner().tickets;

    if format.is_json() {
        output::print_success(&tickets.iter().map(ticket_json).collect::<Vec<_>>());
    } else if tickets.is_empty() {
        println!("No outstanding boot tickets");
    } else {
        print_table(
            tickets
                .into_iter()
                .map(|ticket| BootTicketPrint {
                    scope: if ticket.instance_id.is_empty() {
                        format!("{}*", ticket.instance_prefix)
                    } else {
                        ticket.instance_id.clone()
                    },
                    gate: Gate::from_wire(ticket.gate).as_str().to_string(),
                    jti: ticket.jti,
                    job: ticket.job,
                    task: ticket.task,
                    pool: ticket.pool,
                    uses_left: ticket.uses_remaining,
                    expires_at: ticket.expires_at_unix,
                })
                .collect(),
        );
    }
    Ok(())
}
