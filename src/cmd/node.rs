use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::ForgetNodeRequest;
use crate::structs::{
    GetHeartbeatCadenceRequest, GetNodeContentReply, GetNodeContentRequest, GetNodeRequest,
    GetNodeUsageReply, GetNodeUsageRequest, GetNodesRequest, ListNodeUsageReply,
    ListNodeUsageRequest, Node, NodePrint, RestartWorkerRequest, SetNodeAvailabilityRequest,
    UpdateNodeDynamicRequest,
};

/// Optional idempotency key for mutating node operations; the manager
/// deduplicates retried requests carrying the same key.
fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

pub fn config() -> Command {
    let get_nodes_cmd = Command::new("list").about("List nodes").arg(json_flag());

    let node_claims_cmd = Command::new("claims")
        .about("List a node's live provisional (unratified) allocations")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to inspect")
                .required(true),
        )
        .arg(json_flag());

    let node_usage_cmd = Command::new("usage")
        .about("Show a node's capacity, reservations, and measured usage per allocation")
        .long_about(
            "\
        Show a node's capacity, reservations, and measured usage per allocation\n\
        \n\
        Capacity and reservations come from the state store; measured usage\n\
        comes from the node's cgroups, reported every 10 seconds by workers\n\
        with resourceEnforcement.enabled. See docs/resource-enforcement.md.\n\
        \n\
        Without a node, show one summary line per node (ListNodeUsage).\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to inspect; omit for every node")
                .required(false),
        )
        .arg(json_flag());

    let node_content_cmd = Command::new("content")
        .about("Show what a node holds for content locality: advertised content and snapshots")
        .long_about(
            "\
        Show what a node holds for content locality\n\
        \n\
        Lists the content the node's worker advertises to this manager's\n\
        location index (1 MiB and up), which snapshot each entry belongs to,\n\
        and the node's snapshots with how many of their parts it holds. The\n\
        index is soft state held by the manager that receives the node's\n\
        heartbeats. See docs/artifacts.md, \"Content locality\".\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to inspect")
                .required(true),
        )
        .arg(json_flag());

    let node_info_cmd = Command::new("info")
        .about("Get node info")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to inspect")
                .required(true),
        )
        .arg(json_flag());

    let update_node_cmd = Command::new("update")
        .about(
            "\
        Update node attributes\n\
        \n\
        Example: legion node update <id> -a <key1>=<value1> <key2>=<value2>\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to update")
                .required(true),
        )
        .arg(
            Arg::new("attr")
                .short('a')
                .long("attr")
                .value_name("KEY=VALUE")
                .help("Dynamic attributes to update on the node")
                .num_args(1..)
                .action(ArgAction::Append),
        )
        .arg(json_flag());

    let restart_node_cmd = Command::new("restart")
        .about("Restart a worker node")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to restart")
                .required(true),
        )
        .arg(json_flag());

    let cordon_node_cmd = Command::new("cordon")
        .about("Mark a node unschedulable; existing allocations keep running")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to cordon")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let drain_node_cmd = Command::new("drain")
        .about(
            "\
        Mark a node unschedulable and evict its allocations\n\
        \n\
        Stateless allocations are moved to other nodes immediately. Stateful\n\
        allocations are protected by their stateful.onLost policy and stay on\n\
        the node (use `legion instance replace` to force-move one).\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to drain")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let uncordon_node_cmd = Command::new("uncordon")
        .about("Restore a cordoned/drained node to the scheduling pool")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to uncordon")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let forget_node_cmd = Command::new("forget")
        .about("Remove a dead node's record and revoke its node token")
        .long_about(
            "\
        Remove a node's record and revoke its node token, in one write.\n\
        \n\
        For a machine that is gone and will not come back. A running worker\n\
        on a forgotten node can no longer heartbeat or re-register under its\n\
        old identity. Refused, unless --force, while the node still has\n\
        allocations, is live and not cordoned, or holds node-local volumes.\n\
        See docs/architecture-managers-workers.md, \"Forgetting a node\".\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to forget")
                .required(true),
        )
        .arg(
            Arg::new("force")
                .long("force")
                .action(ArgAction::SetTrue)
                .help(
                    "The machine is gone for good: forget it whatever its state, releasing its \
                     allocations for replacement at once",
                ),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let cadence_cmd = Command::new("cadence")
        .about("Report whether this fleet is sustaining its heartbeat cadence")
        .long_about(
            "\
        Report whether this fleet is sustaining its heartbeat cadence\n\
        \n\
        Answers the question an operator has at 3am — are the machines gone,\n\
        or is the control plane just behind? — and hands back the\n\
        `heartbeatSeconds` value to set for a fleet this size on this control\n\
        plane. See docs/heartbeat-cadence-and-scale.md.\
        ",
        )
        .arg(json_flag());

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(cadence_cmd)
        .subcommand(update_node_cmd)
        .subcommand(node_info_cmd)
        .subcommand(node_usage_cmd)
        .subcommand(node_content_cmd)
        .subcommand(node_claims_cmd)
        .subcommand(restart_node_cmd)
        .subcommand(cordon_node_cmd)
        .subcommand(drain_node_cmd)
        .subcommand(uncordon_node_cmd)
        .subcommand(forget_node_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", arg_matches)) => {
            let format = OutputFormat::from_matches(arg_matches);
            get_nodes(format).await
        }
        Some(("update", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let attrs = arg_matches
                .get_many::<String>("attr")
                .context("Must provide attribute(s) to update")?;

            let mut dynamic_diff = Map::new();

            let mut error_msg = None;
            attrs.into_iter().for_each(|attr| {
                let mut split = attr.split('=');

                if let Some(key_path) = split.next() {
                    if let Some(value) = split.next() {
                        dynamic_diff.insert(key_path.into(), value.into());
                    } else {
                        error_msg = Some(format!(
                            "Invalid attribute: {attr} - must be of format KEY=VALUE"
                        ));
                    }
                } else {
                    error_msg = Some(format!(
                        "Invalid attribute: {attr} - must be of format KEY=VALUE"
                    ));
                }
            });

            if let Some(msg) = error_msg {
                bail!(msg);
            }

            let format = OutputFormat::from_matches(arg_matches);
            update_node(id.into(), dynamic_diff, format).await
        }
        Some(("cadence", arg_matches)) => {
            let format = OutputFormat::from_matches(arg_matches);
            get_heartbeat_cadence(format).await
        }
        Some(("claims", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;
            let format = OutputFormat::from_matches(arg_matches);
            get_node_claims(id.to_string(), format).await
        }
        Some(("info", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let format = OutputFormat::from_matches(arg_matches);
            get_node_info(id.to_string(), format).await
        }
        Some(("usage", arg_matches)) => {
            let format = OutputFormat::from_matches(arg_matches);
            match arg_matches.get_one::<String>("id") {
                Some(id) => get_node_usage(id.to_string(), format).await,
                None => list_node_usage(format).await,
            }
        }
        Some(("content", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;
            let format = OutputFormat::from_matches(arg_matches);
            get_node_content(id.to_string(), format).await
        }
        Some(("restart", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let format = OutputFormat::from_matches(arg_matches);
            restart_node(id.to_string(), format).await
        }
        Some((command @ ("cordon" | "drain" | "uncordon"), arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;
            let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();

            // cordon = unschedulable, keep allocations; drain = unschedulable
            // + evict; uncordon = schedulable again.
            let (availability, evict) = match command {
                "cordon" => ("drained", false),
                "drain" => ("drained", true),
                _ => ("available", false),
            };

            let format = OutputFormat::from_matches(arg_matches);
            set_node_availability(id.to_string(), availability, evict, idempotency_key, format)
                .await
        }
        Some(("forget", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;
            let format = OutputFormat::from_matches(arg_matches);
            forget_node(
                id.to_string(),
                arg_matches.get_flag("force"),
                arg_matches.get_one::<String>("idempotency-key").cloned(),
                format,
            )
            .await
        }
        _ => unreachable!(),
    }
}

#[instrument(skip_all)]
async fn get_nodes(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetNodesRequest {
        ..Default::default()
    });
    let reply = output::resolve(
        format,
        "failed to list nodes",
        client.get_nodes(request).await,
    )?
    .into_inner();
    let nodes: Vec<Node> = reply.nodes.into_iter().map(Into::into).collect();

    if format.is_json() {
        output::print_success(&nodes);
    } else {
        let printable_nodes: Vec<NodePrint> = nodes.into_iter().map(Into::into).collect();
        print_table(printable_nodes);
    }

    Ok(())
}

/// `legion node cadence` — is the fleet keeping up, and what to set if not.
///
/// Deliberately a command rather than only a metric: an operator debugging
/// "why did detection take 90s" needs the answer without a Prometheus scrape,
/// and cadence changes make cluster behavior time-varying.
#[instrument(skip_all)]
async fn get_heartbeat_cadence(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to fetch heartbeat cadence",
        client
            .get_heartbeat_cadence(tonic::Request::new(GetHeartbeatCadenceRequest {}))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "activeNodes": reply.active_nodes,
            "servedIntervalSeconds": reply.served_interval_seconds,
            "lostWindowSeconds": reply.lost_window_seconds,
            "missedBeatsBeforeLost": reply.missed_beats_before_lost,
            "freshnessRatio": reply.freshness_ratio,
            "freshnessMeasurable": reply.freshness_measurable,
            "sustainableNodesEstimate": reply.sustainable_nodes_estimate,
            "suggestedHeartbeatSeconds": reply.suggested_heartbeat_seconds,
            "adaptiveEnabled": reply.adaptive_enabled,
            "atMaxInterval": reply.at_max_interval,
            "isLivenessOwner": reply.is_liveness_owner,
        }));
        return Ok(());
    }

    if !reply.is_liveness_owner {
        println!(
            "This manager does not own node liveness (it does not lead the status lane).\n\
             Ask the manager that does — `legion raft status` names it."
        );
        return Ok(());
    }

    println!("Fleet             {} nodes", reply.active_nodes);
    println!(
        "Served interval   {}s{}",
        reply.served_interval_seconds,
        if reply.adaptive_enabled {
            if reply.at_max_interval {
                "  (adaptive, AT MAX — add control-plane capacity)"
            } else {
                "  (adaptive)"
            }
        } else {
            "  (workers use their worker-type heartbeatSeconds)"
        }
    );
    println!(
        "Lost window       {}s  ({} missed beats)",
        reply.lost_window_seconds, reply.missed_beats_before_lost
    );
    if reply.freshness_measurable {
        // The danger threshold and the ratio are in the same units, which is
        // what makes this explainable rather than a number to look up.
        let false_loss_at =
            f64::from(reply.served_interval_seconds) / f64::from(reply.lost_window_seconds.max(1));
        let verdict = if reply.freshness_ratio < false_loss_at {
            " ── FALSE LOSS IN PROGRESS"
        } else if reply.freshness_ratio < 0.5 {
            " ── FALLING BEHIND"
        } else {
            ""
        };
        println!(
            "Freshness         {:.2}{}  (false loss below {:.2})",
            reply.freshness_ratio, verdict, false_loss_at
        );
        println!(
            "Sustainable       ~{} nodes at {}s on this control plane",
            reply.sustainable_nodes_estimate, reply.served_interval_seconds
        );
        println!(
            "Suggested         heartbeatSeconds: {}   (lost window {}s)",
            reply.suggested_heartbeat_seconds,
            reply.suggested_heartbeat_seconds * reply.missed_beats_before_lost
        );
    } else {
        println!(
            "Freshness         not measurable on a fleet this small (under {} nodes)",
            crate::prelude::HEARTBEAT_FRESHNESS_MIN_FLEET
        );
    }

    Ok(())
}

/// `legion node usage <NODE>` — what the node has, what is reserved on it,
/// and what its allocations actually use.
#[instrument(skip_all)]
async fn get_node_usage(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to fetch node usage",
        client
            .get_node_usage(tonic::Request::new(GetNodeUsageRequest { node_id }))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply);
    } else {
        print!(
            "{}",
            render_node_usage(&reply, chrono::Utc::now().timestamp())
        );
    }
    Ok(())
}

/// `legion node usage` with no node — one summary line per node, from
/// ListNodeUsage.
#[instrument(skip_all)]
async fn list_node_usage(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list node usage",
        client
            .list_node_usage(tonic::Request::new(ListNodeUsageRequest::default()))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
    } else {
        print!("{}", render_node_usage_list(&reply));
    }
    Ok(())
}

/// Human rendering of a `ListNodeUsage` reply.
fn render_node_usage_list(reply: &ListNodeUsageReply) -> String {
    use tabled::{settings::Style, Table, Tabled};

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "NODE")]
        node: String,
        #[tabled(rename = "STATUS")]
        status: String,
        #[tabled(rename = "MEMORY")]
        capacity: String,
        #[tabled(rename = "RESERVED")]
        reserved: String,
        #[tabled(rename = "USED")]
        used: String,
        #[tabled(rename = "CPU")]
        cpu: String,
        #[tabled(rename = "PRESSURE")]
        pressure: String,
    }
    if reply.nodes.is_empty() {
        return "No nodes.\n".to_string();
    }
    let percent = |part: f64, whole: f64| {
        if whole > 0.0 {
            format!("{:.0}%", part / whole * 100.0)
        } else {
            "-".to_string()
        }
    };
    let rows: Vec<Row> = reply
        .nodes
        .iter()
        .map(|node| {
            let memory = node.memory.clone().unwrap_or_default();
            let cpu = node.cpu.clone().unwrap_or_default();
            let ratio = memory.oversubscribe_ratio;
            let physical = if ratio > 0.0 {
                memory.capacity / ratio
            } else {
                memory.capacity
            };
            Row {
                node: node.node_id.clone(),
                status: node.status.clone(),
                capacity: if memory.capacity <= 0.0 {
                    "-".to_string()
                } else if ratio > 1.0 {
                    format!("{physical:.0} MiB ×{ratio}")
                } else {
                    format!("{:.0} MiB", memory.capacity)
                },
                reserved: percent(memory.reserved, memory.capacity),
                used: if node.usage_reported {
                    percent(memory.used, physical)
                } else {
                    "-".to_string()
                },
                cpu: cpu_capacity_label(&cpu),
                pressure: match memory.pressure_response.as_ref() {
                    _ if !node.usage_reported => "-".to_string(),
                    Some(status) if !status.enabled => "off".to_string(),
                    Some(status) if status.under_pressure => "UNDER PRESSURE".to_string(),
                    Some(_) => "ok".to_string(),
                    None => "unknown".to_string(),
                },
            }
        })
        .collect();
    let mut out = Table::new(rows).with(Style::blank()).to_string();
    out.push('\n');
    if reply.truncated {
        out.push_str("Showing the first 500 nodes by id.\n");
    }
    out
}

/// CPU capacity as MHz, or as a core count on a node that reports no usable
/// clock speed (Apple Silicon), or `-`.
fn cpu_capacity_label(cpu: &crate::structs::NodeResourceUsage) -> String {
    if cpu.capacity > 0.0 {
        format!("{:.0} MHz", cpu.capacity)
    } else if cpu.cores > 0.0 {
        format!("{:.0} cores", cpu.cores)
    } else {
        "-".to_string()
    }
}

/// `legion node content <NODE>` — the node's advertised content and the
/// snapshots it holds.
#[instrument(skip_all)]
async fn get_node_content(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to fetch node content",
        client
            .get_node_content(tonic::Request::new(GetNodeContentRequest { node_id }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
    } else {
        print!("{}", render_node_content(&reply));
    }
    Ok(())
}

/// Human rendering of a `GetNodeContent` reply.
fn render_node_content(reply: &GetNodeContentReply) -> String {
    use crate::cmd::helpers::usage_display;
    use tabled::{settings::Style, Table, Tabled};

    let mut out = format!(
        "Node {}  publish: {}  checkpoint: {}\n",
        reply.node_id,
        if reply.can_publish { "yes" } else { "no" },
        if reply.can_checkpoint { "yes" } else { "no" },
    );
    if !reply.index_reported {
        out.push_str(
            "No content report from this node reached this manager (no content store, an \
             older worker, a manager that just started, or heartbeats sent elsewhere).\n",
        );
    } else {
        out.push_str(&format!(
            "Advertises {} entries, {} MiB (epoch {})\n",
            reply.entries.len(),
            usage_display::mib(reply.advertised_bytes),
            reply.epoch
        ));
    }

    #[derive(Tabled)]
    struct EntryRow {
        #[tabled(rename = "DIGEST")]
        digest: String,
        #[tabled(rename = "MiB")]
        mib: String,
        #[tabled(rename = "SNAPSHOT")]
        snapshot: String,
        #[tabled(rename = "ROLE")]
        role: String,
        #[tabled(rename = "HOLDERS")]
        holders: u32,
    }
    if !reply.entries.is_empty() {
        let rows: Vec<EntryRow> = reply
            .entries
            .iter()
            .map(|entry| EntryRow {
                digest: entry.digest.chars().take(19).collect(),
                mib: usage_display::mib(entry.bytes),
                snapshot: or_dash(&entry.snapshot_id),
                role: or_dash(&entry.role),
                holders: entry.holders,
            })
            .collect();
        out.push('\n');
        out.push_str(&Table::new(rows).with(Style::blank()).to_string());
        out.push('\n');
    }

    #[derive(Tabled)]
    struct SnapshotRow {
        #[tabled(rename = "SNAPSHOT")]
        snapshot: String,
        #[tabled(rename = "JOB")]
        job: String,
        #[tabled(rename = "KIND")]
        kind: String,
        #[tabled(rename = "STATE")]
        state: String,
        #[tabled(rename = "CAPTURED HERE")]
        captured: &'static str,
        #[tabled(rename = "PARTS HERE")]
        parts: String,
    }
    if reply.snapshots.is_empty() {
        out.push_str("\nNo snapshots on this node.\n");
        return out;
    }
    let rows: Vec<SnapshotRow> = reply
        .snapshots
        .iter()
        .filter_map(|row| {
            let snapshot = row.snapshot.as_ref()?;
            Some(SnapshotRow {
                snapshot: if snapshot.name.is_empty() {
                    snapshot.snapshot_id.clone()
                } else {
                    format!("{} ({})", snapshot.name, snapshot.snapshot_id)
                },
                job: snapshot.job_id.clone(),
                kind: snapshot.kind.clone(),
                state: snapshot.state.clone(),
                captured: if row.captured_here { "yes" } else { "no" },
                parts: if row.parts_total == 0 {
                    "-".to_string()
                } else {
                    format!("{}/{}", row.parts_here, row.parts_total)
                },
            })
        })
        .collect();
    out.push('\n');
    out.push_str(&Table::new(rows).with(Style::blank()).to_string());
    out.push('\n');
    out
}

fn or_dash(value: &str) -> String {
    if value.is_empty() {
        "-".to_string()
    } else {
        value.to_string()
    }
}

/// Human rendering of a `GetNodeUsage` reply. Split out so the layout is
/// tested without a cluster.
fn render_node_usage(reply: &GetNodeUsageReply, now_unix: i64) -> String {
    use crate::cmd::helpers::usage_display;
    use tabled::{settings::Style, Table, Tabled};

    let percent = |part: f64, whole: f64| {
        if whole > 0.0 {
            format!(" ({:.0}%)", part / whole * 100.0)
        } else {
            String::new()
        }
    };
    let amount = |value: f64, unit: &str| {
        if value > 0.0 {
            format!("{value:.0} {unit}")
        } else {
            "-".to_string()
        }
    };

    #[derive(Tabled)]
    struct ResourceRow {
        #[tabled(rename = "RESOURCE")]
        resource: &'static str,
        #[tabled(rename = "CAPACITY")]
        capacity: String,
        #[tabled(rename = "RESERVED")]
        reserved: String,
        #[tabled(rename = "USED")]
        used: String,
        #[tabled(rename = "PRESSURE")]
        pressure: String,
    }

    let mut out = String::new();
    let freshness = if reply.usage_reported {
        format!("usage sampled {}s ago", reply.report_age_seconds)
    } else {
        "no usage reported (resource enforcement off, an older worker, or no report \
         reached this manager yet)"
            .to_string()
    };
    out.push_str(&format!(
        "Node {} ({})  {freshness}\n\n",
        reply.node_id, reply.status
    ));

    let memory = reply.memory.clone().unwrap_or_default();
    let cpu = reply.cpu.clone().unwrap_or_default();
    // Capacity is already multiplied by the ratio; show the physical figure
    // and the multiplier, so "96 GiB ×1.5" reads as what the node has and
    // how hard it is packed.
    let capacity = |resource: &crate::structs::NodeResourceUsage, unit: &str| {
        let ratio = resource.oversubscribe_ratio;
        if ratio > 1.0 {
            format!("{} ×{ratio}", amount(resource.capacity / ratio, unit))
        } else {
            amount(resource.capacity, unit)
        }
    };
    let used = |resource: &crate::structs::NodeResourceUsage, unit: &str| {
        if reply.usage_reported {
            format!(
                "{}{}",
                amount(resource.used, unit),
                percent(resource.used, resource.capacity)
            )
        } else {
            "-".to_string()
        }
    };
    let pressure = if !reply.usage_reported || memory.physical_bytes == 0 {
        "-".to_string()
    } else {
        let free = format!("free {} MiB", usage_display::mib(memory.available_bytes));
        let numbers = if memory.pressure_available {
            format!("{free}, stall {:.1}%", memory.pressure_some_avg10)
        } else {
            free
        };
        // The response's own verdict, when the worker reports one. Older
        // workers report only the numbers.
        match memory.pressure_response.as_ref() {
            Some(status) if !status.enabled => format!("response off ({numbers})"),
            Some(status) if status.under_pressure => format!(
                "UNDER PRESSURE ({numbers}; acts below {} MiB free)",
                usage_display::mib(status.min_free_bytes)
            ),
            Some(_) => format!("ok ({numbers})"),
            None => numbers,
        }
    };
    let rows = vec![
        ResourceRow {
            resource: "memory",
            capacity: capacity(&memory, "MiB"),
            reserved: format!(
                "{}{}",
                amount(memory.reserved, "MiB"),
                percent(memory.reserved, memory.capacity)
            ),
            used: used(&memory, "MiB"),
            pressure,
        },
        ResourceRow {
            resource: "cpu",
            capacity: if cpu.capacity > 0.0 {
                capacity(&cpu, "MHz")
            } else {
                cpu_capacity_label(&cpu)
            },
            reserved: format!(
                "{}{}",
                amount(cpu.reserved, "MHz"),
                percent(cpu.reserved, cpu.capacity)
            ),
            used: used(&cpu, "MHz"),
            pressure: String::new(),
        },
    ];
    out.push_str(&Table::new(rows).with(Style::blank()).to_string());
    out.push('\n');
    out.push_str(&render_suspended_capacity(reply));

    #[derive(Tabled)]
    struct AllocationRow {
        #[tabled(rename = "ALLOCATION")]
        allocation: i64,
        #[tabled(rename = "JOB")]
        job: String,
        #[tabled(rename = "INSTANCE")]
        instance: String,
        #[tabled(rename = "RES/MAX MiB")]
        reserved: String,
        #[tabled(rename = "USED MiB")]
        used: String,
        #[tabled(rename = "PEAK MiB")]
        peak: String,
        #[tabled(rename = "CPU")]
        cpu: String,
        #[tabled(rename = "IDLE")]
        idle: String,
    }
    if reply.allocations.is_empty() {
        out.push_str("\nNo allocations on this node.\n");
        return out;
    }
    let rows: Vec<AllocationRow> = reply
        .allocations
        .iter()
        .map(|row| {
            let (used, cpu, idle) = usage_display::columns(row.usage.as_ref(), now_unix);
            let instance = row.instance_id.clone().unwrap_or_else(|| "-".into());
            AllocationRow {
                allocation: row.allocation_id,
                job: row.job_id.clone(),
                // A held charge of a suspended instance, not running work.
                instance: if row.suspended {
                    format!("{instance} (suspended, held)")
                } else {
                    instance
                },
                reserved: if row.memory_max_mib > 0.0 {
                    format!("{:.0}/{:.0}", row.reserved_memory_mib, row.memory_max_mib)
                } else {
                    format!("{:.0}", row.reserved_memory_mib)
                },
                used,
                peak: row
                    .usage
                    .as_ref()
                    .map(|usage| usage_display::mib(usage.memory_peak_bytes))
                    .unwrap_or_else(|| "-".into()),
                cpu,
                idle,
            }
        })
        .collect();
    out.push('\n');
    out.push_str(&Table::new(rows).with(Style::blank()).to_string());
    out.push('\n');
    out.push_str(&render_pressure_order(reply));
    out
}

/// The suspended-instance summary under the resource table (docs/
/// suspend-and-resume.md, "Capacity for resumes"): how many suspended
/// instances keep their checkpoint here, what they hold of `reserved`, and
/// the `snapshots` budget their checkpoints fill. Empty when the node has
/// none and no budget report.
fn render_suspended_capacity(reply: &GetNodeUsageReply) -> String {
    use crate::worker::pressure::human_bytes;
    let Some(suspended) = reply.suspended.as_ref() else {
        return String::new();
    };
    let mut out = String::new();
    if suspended.instances > 0 || suspended.holding > 0 {
        let plural = if suspended.instances == 1 { "" } else { "s" };
        let mut line = format!(
            "\nsuspended: {} instance{plural}, {:.0} MiB held",
            suspended.instances, suspended.held_memory_mib
        );
        if suspended.held_cpu_mhz > 0.0 {
            line.push_str(&format!(" and {:.0} MHz", suspended.held_cpu_mhz));
        }
        if suspended.holding != suspended.instances {
            line.push_str(&format!(" ({} holding)", suspended.holding));
        }
        out.push_str(&line);
        out.push('\n');
    }
    if let Some(budget) = suspended.snapshot_budget.as_ref() {
        use crate::storage::content_index::{checkpoint_room, RoomLimit};
        let room = checkpoint_room(budget);
        // When the disk, not the budget, is what binds, say so with the
        // numbers behind it.
        let disk = match (room.limit, budget.disk_free_bytes) {
            (RoomLimit::Disk, Some(free)) => format!(
                ", limited by disk: {} free + {} evictable - {} minFreeDisk",
                human_bytes(free),
                human_bytes(budget.reclaimable_bytes),
                human_bytes(budget.min_free_disk_bytes)
            ),
            _ => String::new(),
        };
        out.push_str(&format!(
            "checkpoints: {} suspended here, snapshots budget {} pinned of {} ({} free{disk}{})\n",
            human_bytes(suspended.checkpoint_bytes),
            human_bytes(budget.pinned_bytes),
            human_bytes(budget.budget_bytes),
            human_bytes(room.bytes),
            if room.bytes == 0 {
                ", FULL: no new memory-checkpoint instances placed here"
            } else {
                ""
            },
        ));
    } else if suspended.checkpoint_bytes > 0 {
        out.push_str(&format!(
            "checkpoints: {} suspended here (no snapshots budget reported)\n",
            human_bytes(suspended.checkpoint_bytes)
        ));
    }
    out
}

/// The "who goes first" preview: every allocation the node's memory pressure
/// response would act on, in the order it would act (docs/
/// resource-enforcement.md, "Memory pressure"). Empty when the node reported
/// no order: nothing is bursting, the response is off, or an older worker.
fn render_pressure_order(reply: &GetNodeUsageReply) -> String {
    use tabled::{settings::Style, Table, Tabled};

    #[derive(Tabled)]
    struct OrderRow {
        #[tabled(rename = "#")]
        rank: u32,
        #[tabled(rename = "ALLOCATION")]
        allocation: String,
        #[tabled(rename = "BAND")]
        band: String,
        #[tabled(rename = "RES/USED MiB")]
        reserved_used: String,
        #[tabled(rename = "STEP")]
        steps: String,
    }
    let mut ranked: Vec<_> = reply
        .allocations
        .iter()
        .filter(|row| row.pressure_rank > 0)
        .collect();
    if ranked.is_empty() {
        return String::new();
    }
    ranked.sort_by_key(|row| row.pressure_rank);
    let rows: Vec<OrderRow> = ranked
        .into_iter()
        .map(|row| OrderRow {
            rank: row.pressure_rank,
            allocation: format!("{} {}", row.allocation_id, row.job_id),
            band: row.band.clone(),
            reserved_used: format!(
                "{:.0}/{}",
                row.reserved_memory_mib,
                row.usage
                    .as_ref()
                    .map(|usage| crate::cmd::helpers::usage_display::mib(
                        usage.memory_current_bytes
                    ))
                    .unwrap_or_else(|| "-".into())
            ),
            steps: row.pressure_steps.join(", then "),
        })
        .collect();
    let mut out = String::from("\nUNDER PRESSURE, IN ORDER\n");
    out.push_str(&Table::new(rows).with(Style::blank()).to_string());
    out.push('\n');
    out
}

#[instrument(skip_all)]
async fn update_node(
    node_id: String,
    dynamic_diff: Map<String, Value>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;

    let request = tonic::Request::new(UpdateNodeDynamicRequest {
        id: node_id,
        dynamic_diff: serde_json::to_string(&dynamic_diff)?,
    });

    let reply = output::resolve(
        format,
        "failed to update node",
        client.update_node_dynamic(request).await,
    )?
    .into_inner();
    let node = reply.node.map(Node::from);

    if format.is_json() {
        output::print_success(&node);
    } else {
        match node {
            Some(node) => node.print_long(),
            None => println!("Node not found"),
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn get_node_info(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetNodeRequest { id: node_id });
    let reply = output::resolve(
        format,
        "failed to fetch node info",
        client.get_node(request).await,
    )?
    .into_inner();
    let node = reply.node.map(Node::from);

    if format.is_json() {
        output::print_success(&node);
    } else {
        match node {
            Some(node) => node.print_long(),
            None => println!("Node not found"),
        }
    }

    Ok(())
}

/// `legion node claims <NODE>` — what this node has started that the store
/// has not recorded yet (docs/edge-boot.md).
///
/// Deliberately a separate view from `node info`: unratified work is live,
/// short-lived state reported on the last heartbeat, not part of the node
/// record, and mixing the two would suggest a durability it does not have.
#[instrument(skip_all)]
async fn get_node_claims(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetNodeRequest { id: node_id });
    let reply = output::resolve(
        format,
        "failed to fetch node claims",
        client.get_node(request).await,
    )?
    .into_inner();

    let claims = reply
        .provisional_claims
        .into_iter()
        .map(|claim| {
            json!({
                "claimId": claim.claim_id,
                "jobConfigId": claim.job_config_id,
                "taskName": claim.task_name,
                "instanceId": claim.instance_id,
                "gate": crate::structs::edge_boot::Gate::from_wire(claim.gate).as_str(),
                "stage": claim.stage,
                "leaseDeadlineUnix": claim.lease_deadline_unix,
                "resources": claim.resources,
            })
        })
        .collect::<Vec<_>>();

    if format.is_json() {
        output::print_success(&claims);
    } else if claims.is_empty() {
        println!("No provisional allocations on this node");
    } else {
        for claim in &claims {
            println!(
                "{}  {}  instance={}  gate={}  stage={}  lease_expires_unix={}",
                claim["claimId"].as_str().unwrap_or_default(),
                claim["taskName"].as_str().unwrap_or_default(),
                claim["instanceId"].as_str().unwrap_or_default(),
                claim["gate"].as_str().unwrap_or_default(),
                claim["stage"].as_str().unwrap_or_default(),
                claim["leaseDeadlineUnix"],
            );
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn restart_node(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(RestartWorkerRequest { node_id });
    let reply = output::resolve(
        format,
        "failed to restart worker",
        client.restart_worker(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({ "message": reply.message }));
    } else {
        println!("{}", reply.message);
    }

    Ok(())
}

#[instrument(skip_all)]
async fn set_node_availability(
    node_id: String,
    availability: &str,
    evict: bool,
    idempotency_key: Option<String>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(SetNodeAvailabilityRequest {
        node_id,
        availability: availability.to_string(),
        evict,
        idempotency_key,
    });
    let reply = output::resolve(
        format,
        "failed to set node availability",
        client.set_node_availability(request).await,
    )?
    .into_inner();
    let node = reply.node.map(Node::from);

    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "node": node,
        }));
    } else {
        println!("{}", reply.message);
    }

    Ok(())
}

#[instrument(skip_all)]
async fn forget_node(
    node_id: String,
    force: bool,
    idempotency_key: Option<String>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to forget node",
        client
            .forget_node(tonic::Request::new(ForgetNodeRequest {
                node_id,
                force,
                idempotency_key,
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "allocationsRemoved": reply.allocations_removed,
            "tokensRevoked": reply.tokens_revoked,
            "volumesOrphaned": reply.volumes_orphaned,
            "snapshotsLost": reply.snapshots_lost,
        }));
        return Ok(());
    }
    println!("{}", reply.message);
    if reply.allocations_removed > 0 {
        println!(
            "  {} allocation(s) removed; the scheduler places them elsewhere",
            reply.allocations_removed
        );
    }
    if reply.snapshots_lost > 0 {
        println!(
            "  {} snapshot(s) stored on it are now lost",
            reply.snapshots_lost
        );
    }
    if reply.volumes_orphaned > 0 {
        println!(
            "  {} node-local volume(s) stay pinned to it; replace their instances \
             (`legion instance replace`)",
            reply.volumes_orphaned
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_forget_command_parses() -> R<()> {
        let matches = config().try_get_matches_from([
            "node",
            "forget",
            "node-1",
            "--force",
            "--idempotency-key",
            "k1",
            "-j",
        ])?;
        let (name, forget) = matches.subcommand().context("subcommand")?;
        assert_eq!(name, "forget");
        assert_eq!(
            forget.get_one::<String>("id").map(String::as_str),
            Some("node-1")
        );
        assert!(forget.get_flag("force"));
        assert_eq!(
            forget
                .get_one::<String>("idempotency-key")
                .map(String::as_str),
            Some("k1")
        );
        Ok(())
    }

    #[test]
    fn node_restart_command_parses() -> R<()> {
        let matches = config().try_get_matches_from(["node", "restart", "node-1"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "restart");
        let id = args.get_one::<String>("id").context("Missing id")?;
        assert_eq!(id, "node-1");
        Ok(())
    }

    #[test]
    fn node_cordon_drain_uncordon_commands_parse() -> R<()> {
        for command in ["cordon", "drain", "uncordon"] {
            let matches = config().try_get_matches_from([
                "node",
                command,
                "node-1",
                "--idempotency-key",
                "key-1",
                "--json",
            ])?;
            let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
            assert_eq!(subcommand, command);
            let id = args.get_one::<String>("id").context("Missing id")?;
            assert_eq!(id, "node-1");
            let key = args
                .get_one::<String>("idempotency-key")
                .context("Missing idempotency key")?;
            assert_eq!(key, "key-1");
        }
        Ok(())
    }

    #[test]
    fn node_usage_without_a_node_and_node_content_parse() -> R<()> {
        let matches = config().try_get_matches_from(["node", "usage", "-j"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "usage");
        assert!(args.get_one::<String>("id").is_none());

        let matches = config().try_get_matches_from(["node", "content", "w-03"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "content");
        assert_eq!(
            args.get_one::<String>("id").map(String::as_str),
            Some("w-03")
        );
        Ok(())
    }

    /// The fleet table shows the physical memory with its ratio, used against
    /// physical, cores where MHz is unknown, and the pressure verdict.
    #[test]
    fn node_usage_list_renders_one_line_per_node() {
        use crate::structs::{MemoryPressureStatus, NodeResourceUsage};
        let reply = ListNodeUsageReply {
            nodes: vec![
                GetNodeUsageReply {
                    node_id: "w-03".into(),
                    status: "ready".into(),
                    usage_reported: true,
                    memory: Some(NodeResourceUsage {
                        capacity: 1536.0,
                        reserved: 768.0,
                        used: 512.0,
                        oversubscribe_ratio: 1.5,
                        pressure_response: Some(MemoryPressureStatus {
                            enabled: true,
                            under_pressure: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    cpu: Some(NodeResourceUsage {
                        capacity: 0.0,
                        cores: 10.0,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                GetNodeUsageReply {
                    node_id: "mac".into(),
                    status: "ready".into(),
                    ..Default::default()
                },
            ],
            truncated: true,
        };
        let out = render_node_usage_list(&reply);
        assert!(out.contains("1024 MiB ×1.5"), "{out}");
        assert!(out.contains("50%"), "{out}");
        assert!(out.contains("10 cores"), "{out}");
        assert!(out.contains("UNDER PRESSURE"), "{out}");
        assert!(out.contains("first 500 nodes"), "{out}");
    }

    #[test]
    fn node_content_renders_entries_and_snapshots() {
        use crate::structs::{NodeContentEntry, NodeSnapshotContent, SnapshotInfo};
        let reply = GetNodeContentReply {
            node_id: "w-2".into(),
            index_reported: true,
            epoch: 3,
            entries: vec![NodeContentEntry {
                digest: "sha256:0123456789abcdef0123".into(),
                bytes: 512 * 1024 * 1024,
                snapshot_id: "snap-a".into(),
                role: "memory".into(),
                holders: 2,
            }],
            advertised_bytes: 512 * 1024 * 1024,
            can_publish: true,
            can_checkpoint: true,
            snapshots: vec![NodeSnapshotContent {
                snapshot: Some(SnapshotInfo {
                    snapshot_id: "snap-a".into(),
                    job_id: "web".into(),
                    kind: "memory".into(),
                    state: "published".into(),
                    ..Default::default()
                }),
                captured_here: false,
                parts_here: 1,
                parts_total: 2,
                bytes_here: 512 * 1024 * 1024,
            }],
        };
        let out = render_node_content(&reply);
        assert!(
            out.contains("Advertises 1 entries, 512 MiB (epoch 3)"),
            "{out}"
        );
        assert!(out.contains("snap-a"), "{out}");
        assert!(out.contains("1/2"), "{out}");

        let empty = render_node_content(&GetNodeContentReply {
            node_id: "w-9".into(),
            ..Default::default()
        });
        assert!(empty.contains("No content report"), "{empty}");
        assert!(empty.contains("No snapshots on this node."), "{empty}");
    }

    #[test]
    fn node_usage_command_parses() -> R<()> {
        let matches = config().try_get_matches_from(["node", "usage", "w-03", "-j"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "usage");
        assert_eq!(
            args.get_one::<String>("id").map(String::as_str),
            Some("w-03")
        );
        assert!(args.get_flag("json"));
        Ok(())
    }

    #[test]
    fn node_usage_renders_resources_and_allocations() {
        use crate::structs::{AllocationUsage, NodeAllocationUsage, NodeResourceUsage};
        const MIB: u64 = 1024 * 1024;
        let reply = GetNodeUsageReply {
            node_id: "w-03".into(),
            status: "ready".into(),
            usage_reported: true,
            sampled_at_unix_ms: 1_000_000,
            report_age_seconds: 4,
            memory: Some(NodeResourceUsage {
                capacity: 8192.0,
                reserved: 4096.0,
                used: 2048.0,
                physical_bytes: 8192 * MIB,
                available_bytes: 5000 * MIB,
                pressure_some_avg10: 0.4,
                pressure_available: true,
                oversubscribe_ratio: 1.0,
                pressure_response: Some(crate::structs::MemoryPressureStatus {
                    enabled: true,
                    min_free_bytes: 400 * MIB,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            cpu: Some(NodeResourceUsage {
                capacity: 8000.0,
                reserved: 2000.0,
                used: 1000.0,
                ..Default::default()
            }),
            allocations: vec![
                NodeAllocationUsage {
                    allocation_id: 7,
                    job_id: "workspace".into(),
                    instance_id: Some("user-42".into()),
                    reserved_memory_mib: 512.0,
                    reserved_cpu_mhz: 500.0,
                    usage: Some(AllocationUsage {
                        allocation_id: 7,
                        memory_current_bytes: 1830 * MIB,
                        memory_peak_bytes: 1900 * MIB,
                        cpu_millicores: 250,
                        last_activity_unix: 839,
                        ..Default::default()
                    }),
                    idle_seconds: 161,
                    memory_max_mib: 4096.0,
                    band: "normal".into(),
                    pressure_rank: 1,
                    pressure_steps: vec!["reclaim".into(), "evict-bursting".into()],
                    suspended: false,
                },
                NodeAllocationUsage {
                    allocation_id: 9,
                    job_id: "batch".into(),
                    reserved_memory_mib: 256.0,
                    ..Default::default()
                },
            ],
            suspended: None,
        };
        let out = render_node_usage(&reply, 1_000);
        assert!(out.contains("usage sampled 4s ago"), "{out}");
        assert!(out.contains("4096 MiB (50%)"), "{out}");
        assert!(out.contains("2048 MiB (25%)"), "{out}");
        assert!(out.contains("ok (free 5000 MiB, stall 0.4%)"), "{out}");
        assert!(out.contains("512/4096"), "{out}");
        assert!(out.contains("UNDER PRESSURE, IN ORDER"), "{out}");
        assert!(out.contains("reclaim, then evict-bursting"), "{out}");
        assert!(out.contains("512/1830"), "{out}");
        assert!(!out.contains('×'), "no ratio at 1.0: {out}");
        assert!(out.contains("1830"), "{out}");
        assert!(out.contains("2m41s"), "{out}");
        assert!(out.contains("user-42"), "{out}");

        // An oversubscribed node under pressure: physical capacity and the
        // multiplier, and the response's verdict.
        let mut packed = reply.clone();
        if let Some(memory) = packed.memory.as_mut() {
            memory.capacity = 12288.0;
            memory.oversubscribe_ratio = 1.5;
            if let Some(status) = memory.pressure_response.as_mut() {
                status.under_pressure = true;
            }
        }
        let out = render_node_usage(&packed, 1_000);
        assert!(out.contains("8192 MiB ×1.5"), "{out}");
        assert!(out.contains("UNDER PRESSURE (free 5000 MiB"), "{out}");

        let unreported = GetNodeUsageReply {
            usage_reported: false,
            ..reply
        };
        let out = render_node_usage(&unreported, 1_000);
        assert!(out.contains("no usage reported"), "{out}");
        assert!(!out.contains("UNDER PRESSURE ("), "{out}");
    }

    #[test]
    fn node_usage_renders_suspended_capacity_and_the_budget() {
        use crate::structs::{NodeAllocationUsage, SnapshotBudget, SuspendedCapacity};
        const GIB: u64 = 1024 * 1024 * 1024;
        let reply = GetNodeUsageReply {
            node_id: "w-03".into(),
            status: "ready".into(),
            allocations: vec![NodeAllocationUsage {
                allocation_id: 4,
                job_id: "agents".into(),
                instance_id: Some("user-17".into()),
                reserved_memory_mib: 512.0,
                suspended: true,
                ..Default::default()
            }],
            suspended: Some(SuspendedCapacity {
                instances: 3,
                holding: 1,
                held_memory_mib: 512.0,
                held_cpu_mhz: 250.0,
                checkpoint_bytes: 6 * GIB,
                snapshot_budget: Some(SnapshotBudget {
                    budget_bytes: 50 * GIB,
                    used_bytes: 12 * GIB,
                    pinned_bytes: 12 * GIB,
                    disk_free_bytes: Some(400 * GIB),
                    min_free_disk_bytes: 5 * GIB,
                    reclaimable_bytes: 0,
                }),
            }),
            ..Default::default()
        };
        let out = render_node_usage(&reply, 1_000);
        assert!(
            out.contains("suspended: 3 instances, 512 MiB held and 250 MHz (1 holding)"),
            "{out}"
        );
        assert!(
            out.contains("checkpoints: 6 GiB suspended here, snapshots budget 12 GiB pinned of 50 GiB (38 GiB free)"),
            "{out}"
        );
        assert!(out.contains("user-17 (suspended, held)"), "{out}");

        let mut full = reply.clone();
        if let Some(budget) = full
            .suspended
            .as_mut()
            .and_then(|suspended| suspended.snapshot_budget.as_mut())
        {
            budget.pinned_bytes = budget.budget_bytes;
        }
        let out = render_node_usage(&full, 1_000);
        assert!(
            out.contains("FULL: no new memory-checkpoint instances"),
            "{out}"
        );

        // A nearly full disk binds before the budget does.
        let mut tight = reply.clone();
        if let Some(budget) = tight
            .suspended
            .as_mut()
            .and_then(|suspended| suspended.snapshot_budget.as_mut())
        {
            budget.disk_free_bytes = Some(9 * GIB);
            budget.reclaimable_bytes = GIB;
        }
        let out = render_node_usage(&tight, 1_000);
        assert!(
            out.contains(
                "(5 GiB free, limited by disk: 9 GiB free + 1 GiB evictable - 5 GiB minFreeDisk)"
            ),
            "{out}"
        );

        let none = GetNodeUsageReply {
            suspended: Some(SuspendedCapacity::default()),
            ..Default::default()
        };
        assert!(!render_node_usage(&none, 1_000).contains("suspended:"));
    }
}
