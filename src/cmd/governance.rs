//! `legion audit`, `legion approval`, and `legion usage`: the agent-governance
//! CLI (docs/audit-journal.md, docs/approvals-and-rate-limits.md,
//! docs/usage-accounting.md).
//!
//! Also home of the approval id a retried command presents: `--approval`
//! (a global flag), `LEGION_APPROVAL`, or, inside the MCP server, a per-call
//! task-local. The client interceptor attaches it as `legion-approval-id`.

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    Approval, AuditEvent, DecideApprovalRequest, GetApprovalRequest, GetUsageRequest,
    ListApprovalsRequest, ListAuditEventsRequest, UsageSummary,
};
use std::sync::OnceLock;
use tabled::Tabled;

static CLI_APPROVAL: OnceLock<String> = OnceLock::new();

tokio::task_local! {
    /// The approval id one MCP tool call retries with.
    pub static TASK_APPROVAL: String;
}

/// Records `--approval` for every request this CLI process sends.
pub fn set_cli_approval(id: &str) {
    let _ = CLI_APPROVAL.set(id.trim().to_string());
}

/// The approval id to present: this task's (MCP), `--approval`, or
/// `LEGION_APPROVAL`.
pub fn current_approval_id() -> Option<String> {
    TASK_APPROVAL
        .try_with(Clone::clone)
        .ok()
        .or_else(|| CLI_APPROVAL.get().cloned())
        .or_else(|| std::env::var("LEGION_APPROVAL").ok())
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
}

/// The global `--approval <ID>` flag.
pub fn approval_flag() -> Arg {
    Arg::new("approval")
        .long("approval")
        .value_name("APPROVAL_ID")
        .global(true)
        .help(
            "Retry a write gated by an approval rule with this granted approval \
             (also LEGION_APPROVAL)",
        )
}

fn since_arg(default: &'static str) -> Arg {
    Arg::new("since")
        .long("since")
        .value_name("WHEN")
        .default_value(default)
        .help("Start: RFC3339, or a duration back from now (1h, 7d)")
}

fn until_arg() -> Arg {
    Arg::new("until")
        .long("until")
        .value_name("WHEN")
        .help("End (exclusive): RFC3339, or a duration back from now")
}

/// `legion audit`.
pub fn audit_config() -> Command {
    Command::new("audit")
        .about("Query the durable audit journal of writes and denials (needs audit:read)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List audit records, newest first")
                .arg(
                    Arg::new("accessor")
                        .long("accessor")
                        .value_name("ACCESSOR_ID"),
                )
                .arg(
                    Arg::new("principal")
                        .long("principal")
                        .value_name("PRINCIPAL"),
                )
                .arg(Arg::new("operation").long("operation").value_name("RPC"))
                .arg(
                    Arg::new("target")
                        .long("target")
                        .value_name("PREFIX")
                        .help("Target prefix, e.g. job:prod/ or kv:team/"),
                )
                .arg(
                    Arg::new("in-namespace")
                        .long("in-namespace")
                        .value_name("NAMESPACE")
                        .help("Only targets in this namespace"),
                )
                .arg(
                    Arg::new("outcome")
                        .long("outcome")
                        .value_name("OUTCOME")
                        .help("ok, PermissionDenied, APPROVAL_REQUIRED, RATE_LIMITED, ..."),
                )
                .arg(
                    Arg::new("kind")
                        .long("kind")
                        .value_name("KIND")
                        .help("write, denied, or audited"),
                )
                .arg(since_arg("24h"))
                .arg(until_arg())
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_name("N")
                        .value_parser(clap::value_parser!(u32))
                        .default_value("50"),
                )
                .arg(
                    Arg::new("follow")
                        .long("follow")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Keep printing new records as they are journaled"),
                )
                .arg(json_flag()),
        )
}

/// `legion approval`.
pub fn approval_config() -> Command {
    let id = || Arg::new("id").value_name("APPROVAL_ID").required(true);
    let reason = || {
        Arg::new("reason")
            .long("reason")
            .value_name("TEXT")
            .help("Why, recorded on the request and in the audit journal")
    };
    Command::new("approval")
        .about("List and decide approval requests filed by governance.approvals gates")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List approval requests (yours only without approval:read)")
                .arg(
                    Arg::new("status")
                        .long("status")
                        .value_name("STATUS")
                        .help("pending, approved, denied, consumed, or expired"),
                )
                .arg(
                    Arg::new("mine")
                        .long("mine")
                        .action(ArgAction::SetTrue)
                        .help("Only requests this token filed"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one approval request")
                .arg(id())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("approve")
                .about("Approve a pending request (needs approval:write; not your own)")
                .arg(id())
                .arg(reason())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("deny")
                .about("Deny a pending request (needs approval:write; not your own)")
                .arg(id())
                .arg(reason())
                .arg(json_flag()),
        )
}

/// `legion usage`.
pub fn usage_config() -> Command {
    usage_command(
        "usage",
        "Time-integrated reserved usage per namespace, job, or submitter",
    )
}

/// The usage command body, shared with `legion namespace usage`.
pub fn usage_command(name: &'static str, about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .arg(
            Arg::new("namespaces")
                .long("namespaces")
                .value_name("NAMESPACE")
                .action(ArgAction::Append)
                .help("Namespaces or globs (repeatable); default: every one you can read"),
        )
        .arg(
            Arg::new("job-filter")
                .long("job")
                .value_name("JOB")
                .help("Only this job (ns/name)"),
        )
        .arg(
            Arg::new("submitter")
                .long("submitter")
                .value_name("ACCESSOR_OR_PRINCIPAL")
                .help("Only jobs last submitted by this accessor or principal"),
        )
        .arg(
            Arg::new("by")
                .long("by")
                .value_name("GROUP")
                .default_value("namespace")
                .value_parser(["namespace", "job", "submitter", "hour"]),
        )
        .arg(since_arg("7d"))
        .arg(until_arg())
        .arg(json_flag())
}

pub async fn handle_audit(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", args)) => audit_list(args).await,
        _ => unreachable!(),
    }
}

pub async fn handle_approval(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", args)) => approval_list(args).await,
        Some(("get", args)) => approval_get(args).await,
        Some(("approve", args)) => approval_decide(args, true).await,
        Some(("deny", args)) => approval_decide(args, false).await,
        _ => unreachable!(),
    }
}

pub async fn handle_usage(matches: &ArgMatches) -> R<()> {
    usage(matches, Vec::new()).await
}

fn opt(args: &ArgMatches, id: &str) -> String {
    args.get_one::<String>(id).cloned().unwrap_or_default()
}

#[derive(Tabled)]
struct AuditRow {
    time: String,
    operation: String,
    principal: String,
    accessor: String,
    target: String,
    outcome: String,
}

fn audit_row(event: &AuditEvent) -> AuditRow {
    let mut principal = event.principal.clone();
    if !event.parent_accessor_id.is_empty() {
        principal = format!(
            "{principal} (child of {})",
            short(&event.parent_accessor_id)
        );
    }
    let mut outcome = event.outcome.clone();
    if !event.approval_id.is_empty() {
        outcome = format!("{outcome} approval={}", event.approval_id);
    }
    AuditRow {
        time: event.time.clone(),
        operation: event.operation.clone(),
        principal,
        accessor: short(&event.accessor_id),
        target: event.target.clone(),
        outcome,
    }
}

fn short(id: &str) -> String {
    if id.len() > 10 {
        format!("{}…", &id[..8])
    } else {
        id.to_string()
    }
}

async fn audit_list(args: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(args);
    let mut request = ListAuditEventsRequest {
        accessor_id: opt(args, "accessor"),
        principal: opt(args, "principal"),
        operation: opt(args, "operation"),
        target_prefix: opt(args, "target"),
        namespace: opt(args, "in-namespace"),
        outcome: opt(args, "outcome"),
        kind: opt(args, "kind"),
        since: opt(args, "since"),
        until: opt(args, "until"),
        limit: args.get_one::<u32>("limit").copied().unwrap_or(50),
        ..Default::default()
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to list audit events",
        client.list_audit_events(request.clone()).await,
    )?
    .into_inner();
    if !args.get_flag("follow") {
        if format.is_json() {
            output::print_success(&json!({
                "events": reply.events,
                "nextPageToken": reply.next_page_token,
                "lastCursor": reply.last_cursor,
            }));
        } else if reply.events.is_empty() {
            println!("No audit records match");
        } else {
            print_table(reply.events.iter().map(audit_row).collect());
        }
        return Ok(());
    }
    // Follow: print what exists oldest-first, then poll for newer records.
    // In JSON mode each record is one line (`{"ok":true,"data":<event>}`).
    let print = |event: &AuditEvent| {
        if format.is_json() {
            println!("{}", json!({ "ok": true, "data": event }));
        } else {
            let row = audit_row(event);
            println!(
                "{}  {}  {}  {}  {}  {}",
                row.time, row.operation, row.principal, row.accessor, row.target, row.outcome
            );
        }
    };
    for event in reply.events.iter().rev() {
        print(event);
    }
    request.after_cursor = reply.last_cursor;
    request.limit = 500;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let reply = output::resolve(
            format,
            "Failed to follow audit events",
            client.list_audit_events(request.clone()).await,
        )?
        .into_inner();
        for event in &reply.events {
            print(event);
        }
        request.after_cursor = reply.last_cursor;
    }
}

#[derive(Tabled)]
struct ApprovalRow {
    id: String,
    status: String,
    operation: String,
    target: String,
    requester: String,
    rule: String,
    expires_at: String,
}

fn approval_row(approval: &Approval) -> ApprovalRow {
    ApprovalRow {
        id: approval.id.clone(),
        status: approval.status.clone(),
        operation: approval.operation.clone(),
        target: approval.target.clone(),
        requester: approval.requester_principal.clone(),
        rule: approval.rule.clone(),
        expires_at: approval.expires_at.clone(),
    }
}

async fn approval_list(args: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(args);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to list approvals",
        client
            .list_approvals(ListApprovalsRequest {
                status: opt(args, "status"),
                mine: args.get_flag("mine"),
                limit: 0,
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply.approvals);
    } else if reply.approvals.is_empty() {
        println!("No approval requests");
    } else {
        print_table(reply.approvals.iter().map(approval_row).collect());
    }
    Ok(())
}

fn print_approval(approval: &Approval) {
    println!("ID:          {}", approval.id);
    println!("Status:      {}", approval.status);
    println!(
        "Operation:   {} ({})",
        approval.operation, approval.permission
    );
    println!("Target:      {}", approval.target);
    println!("Rule:        {}", approval.rule);
    println!(
        "Requester:   {} {}",
        approval.requester_principal,
        if approval.on_behalf_of.is_empty() {
            String::new()
        } else {
            format!("({})", approval.on_behalf_of)
        }
    );
    println!("Digest:      {}", approval.request_digest);
    println!("Created:     {}", approval.created_at);
    println!("Expires:     {}", approval.expires_at);
    if !approval.decided_by.is_empty() {
        println!(
            "Decided:     by {} at {}{}",
            approval.decided_by,
            approval.decided_at,
            if approval.reason.is_empty() {
                String::new()
            } else {
                format!(" ({})", approval.reason)
            }
        );
    }
    if !approval.consumed_at.is_empty() {
        println!("Used:        {}", approval.consumed_at);
    }
}

async fn approval_get(args: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(args);
    let mut client = connect_manager_client().await?;
    let approval = output::resolve(
        format,
        "Failed to get approval",
        client
            .get_approval(GetApprovalRequest {
                id: opt(args, "id"),
            })
            .await,
    )?
    .into_inner()
    .approval
    .unwrap_or_default();
    if format.is_json() {
        output::print_success(&approval);
    } else {
        print_approval(&approval);
    }
    Ok(())
}

async fn approval_decide(args: &ArgMatches, approve: bool) -> R<()> {
    let format = OutputFormat::from_matches(args);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        if approve {
            "Failed to approve"
        } else {
            "Failed to deny"
        },
        client
            .decide_approval(DecideApprovalRequest {
                id: opt(args, "id"),
                approve,
                reason: opt(args, "reason"),
                idempotency_key: None,
            })
            .await,
    )?
    .into_inner();
    let approval = reply.approval.unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({ "approval": approval, "deduplicated": reply.deduplicated }));
    } else {
        println!(
            "Approval {} is now {}. {}",
            approval.id,
            approval.status,
            if approve {
                format!(
                    "The requester retries the same command with --approval {} before {}.",
                    approval.id, approval.expires_at
                )
            } else {
                String::new()
            }
        );
    }
    Ok(())
}

#[derive(Tabled)]
struct UsageRow {
    key: String,
    cpu_core_hours: String,
    memory_gib_hours: String,
    cores_hours: String,
    alloc_hours: String,
    other: String,
}

fn usage_key(row: &UsageSummary, by: &str) -> String {
    match by {
        "job" => format!("{}/{}", row.namespace, row.job),
        "submitter" => {
            if row.submitter.is_empty() {
                "(unknown)".to_string()
            } else {
                row.submitter.clone()
            }
        }
        "hour" => format!("{} {}", row.bucket_start, row.namespace),
        _ => row.namespace.clone(),
    }
}

/// `legion usage` / `legion namespace usage <NS>` (`namespaces` preset).
pub async fn usage(args: &ArgMatches, mut namespaces: Vec<String>) -> R<()> {
    let format = OutputFormat::from_matches(args);
    if let Some(more) = args.get_many::<String>("namespaces") {
        namespaces.extend(more.cloned());
    }
    let by = args
        .get_one::<String>("by")
        .cloned()
        .unwrap_or_else(|| "namespace".into());
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to read usage",
        client
            .get_usage(GetUsageRequest {
                namespaces,
                job: opt(args, "job-filter"),
                submitter: opt(args, "submitter"),
                since: opt(args, "since"),
                until: opt(args, "until"),
                group_by: by.clone(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    println!("Reserved usage {} .. {}", reply.since, reply.until);
    if reply.rows.is_empty() {
        println!("No usage recorded in this window");
        return Ok(());
    }
    print_table(
        reply
            .rows
            .iter()
            .map(|row| UsageRow {
                key: usage_key(row, &by),
                cpu_core_hours: format!("{:.2}", row.cpu_core_hours),
                memory_gib_hours: format!("{:.2}", row.memory_gib_hours),
                cores_hours: format!("{:.2}", row.cores_hours),
                alloc_hours: format!("{:.2}", row.allocation_hours),
                other: row
                    .other_hours
                    .iter()
                    .map(|(name, hours)| format!("{name}={hours:.2}"))
                    .collect::<Vec<_>>()
                    .join(" "),
            })
            .collect(),
    );
    Ok(())
}
