//! `legion sandbox start` — start a workload directly on an edge node from a
//! boot ticket (docs/edge-boot.md).
//!
//! This is the only Legion command that talks to a **worker** rather than a
//! manager, and that is the point: the whole saving is not making the
//! client→manager round trip. The node verifies the ticket offline, admits the
//! start against its own free resources, and boots; ratification happens in
//! parallel and its verdict arrives on the same stream.
//!
//! The caller may act on `booted` and must be prepared for `refused` at any
//! point until `ratified`.

use crate::cmd::helpers::{json_flag, worker_plane_client};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::edge_boot::{Gate, RefusalReason};
use crate::structs::StartClaimRequest;

pub fn config() -> Command {
    let start_cmd = Command::new("start")
        .about("Start a sandbox on an edge node using a boot ticket")
        .arg(
            Arg::new("job")
                .value_name("JOB")
                .required(true)
                .help("Job the ticket authorizes (informational; the ticket is authoritative)"),
        )
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("INSTANCE")
                .required(true)
                .help("Instance id to start (must match the ticket's scope)"),
        )
        .arg(
            Arg::new("node")
                .long("node")
                .value_name("ADDR")
                .required(true)
                .help(
                    "Worker address to start on, e.g. https://10.0.1.7:4848. The worker plane \
                     is always TLS; its certificate is verified against the cluster CA",
                ),
        )
        .arg(
            Arg::new("node-name")
                .long("node-name")
                .value_name("NODE")
                .help(
                    "Node id the worker at --node must prove it is (its certificate names \
                     <NODE>.node.legion). Without it, any node of this cluster is accepted",
                ),
        )
        .arg(Arg::new("ca").long("ca").value_name("FILE").help(
            "Cluster CA bundle (PEM) to verify the worker against. Default: fetched \
                     from the manager with your token (`legion net ca show`)",
        ))
        .arg(
            Arg::new("ticket")
                .long("ticket")
                .value_name("TOKEN")
                .required(true)
                .help("Boot ticket from `legion boot-ticket create`"),
        )
        .arg(
            Arg::new("gate")
                .long("gate")
                .value_name("GATE")
                .help("Requested gate; capped by the ticket and the task (stage|boot|serve)"),
        )
        .arg(
            Arg::new("claim-id")
                .long("claim-id")
                .value_name("ID")
                .help("Retry-safe claim id; a repeat joins the existing claim"),
        )
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .action(clap::ArgAction::SetTrue)
                .help("Verify the ticket and local admission without starting anything"),
        )
        .arg(json_flag());

    Command::new("sandbox")
        .about("Start workloads directly on edge nodes (docs/edge-boot.md)")
        .subcommand(start_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("start", arg_matches)) => start(arg_matches).await,
        _ => {
            println!(
                "Usage: legion sandbox start <JOB> --id <INSTANCE> --node <ADDR> --ticket <T>"
            );
            Ok(())
        }
    }
}

async fn start(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let instance_id = arg_matches
        .get_one::<String>("id")
        .cloned()
        .context("Must provide --id")?;
    let node = arg_matches
        .get_one::<String>("node")
        .cloned()
        .context("Must provide --node")?;
    let ticket = arg_matches
        .get_one::<String>("ticket")
        .cloned()
        .context("Must provide --ticket")?;
    let gate = match arg_matches.get_one::<String>("gate") {
        Some(raw) => Gate::parse(raw)?,
        None => Gate::Off,
    };

    let mut client = worker_plane_client(
        &node,
        arg_matches
            .get_one::<String>("node-name")
            .map(String::as_str),
        arg_matches.get_one::<String>("ca").map(String::as_str),
    )
    .await?;

    let response = output::resolve(
        format,
        "Failed to start sandbox on the edge node",
        client
            .start_claim(tonic::Request::new(StartClaimRequest {
                ticket,
                instance_id: instance_id.clone(),
                claim_id: arg_matches
                    .get_one::<String>("claim-id")
                    .cloned()
                    .unwrap_or_default(),
                gate: gate.to_wire(),
                dry_run: arg_matches.get_flag("dry-run"),
            }))
            .await,
    )?;
    let mut stream = response.into_inner();

    let mut frames = Vec::new();
    let mut refused = None;
    while let Some(frame) = stream.message().await? {
        let reason = RefusalReason::from_wire(frame.reason);
        let rendered = json!({
            "claimId": frame.claim_id,
            "stage": frame.stage,
            "allocationId": frame.allocation_id,
            "gate": Gate::from_wire(frame.gate).as_str(),
            "reason": (frame.stage == "refused").then(|| reason.as_str()),
            "detail": frame.detail,
        });
        if frame.stage == "refused" {
            refused = Some((reason, frame.detail.clone()));
        }
        if !format.is_json() {
            match frame.detail.is_empty() {
                true => println!("{}", frame.stage),
                false => println!("{}: {}", frame.stage, frame.detail),
            }
        }
        frames.push(rendered);
    }

    if format.is_json() {
        output::print_success(&json!({ "instanceId": instance_id, "frames": frames }));
    }
    // A refusal is a real failure for the caller — the sandbox is being
    // killed — so it must not exit 0 and look like a start.
    if let Some((reason, detail)) = refused {
        bail!("sandbox start refused ({reason}): {detail}");
    }
    Ok(())
}
