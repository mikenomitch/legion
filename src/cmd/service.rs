//! `legion service` — the service-discovery CLI surface.
//!
//! Three read-only subcommands sit on top of `ListServices` / `ResolveService`:
//!
//! - `list` — every declared service with healthy/total endpoint counts.
//! - `resolve` — a service's endpoints; `--one` prints a single random
//!   `address:port` so shell scripts can skip `jq`.
//! - `doctor` — walks the discovery layers (declared → placed → node → port →
//!   health) and names the first one that fails, including the service's
//!   connect intentions (`service.allow`) when it declares any.
//!
//! Every subcommand takes `--json`/`-j` and emits the standard
//! [`crate::cmd::output`] envelope so agents never have to scrape prose.

use crate::cmd::helpers::{connect_manager_client, json_flag, AuthedManagerClient};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    ListServiceEdgesRequest, ListServicesRequest, ResolveServiceRequest, ServiceEdge,
    ServiceEndpoint, ServiceSummary,
};
use rand::seq::SliceRandom;
use serde::Serialize;
use tabled::{Table, Tabled};

/// Layer verdicts used by `doctor`. `info` lines are observations (they never
/// fail the walk), `warn` means "degraded but still resolvable", and `fail`
/// stops the walk at the layer that is actually broken.
const STATUS_OK: &str = "ok";
const STATUS_INFO: &str = "info";
const STATUS_WARN: &str = "warn";
const STATUS_FAIL: &str = "fail";

#[derive(Tabled)]
struct ServiceRow {
    #[tabled(rename = "SERVICE")]
    service: String,
    #[tabled(rename = "JOB")]
    job: String,
    #[tabled(rename = "TASK")]
    task: String,
    #[tabled(rename = "HEALTHY")]
    healthy: String,
    #[tabled(rename = "TAGS")]
    tags: String,
}

#[derive(Tabled)]
struct EndpointRow {
    #[tabled(rename = "ADDRESS")]
    address: String,
    #[tabled(rename = "PUBLIC")]
    public: String,
    #[tabled(rename = "ALLOC")]
    alloc: i64,
    #[tabled(rename = "NODE")]
    node: String,
    #[tabled(rename = "HEALTHY")]
    healthy: String,
    #[tabled(rename = "STATUS")]
    status: String,
}

pub fn config() -> Command {
    Command::new("service")
        .about("Service discovery: list, resolve, and diagnose services")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("Lists all declared services with endpoint health counts")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("resolve")
                .about("Resolves a service to its healthy endpoint addresses")
                .arg(
                    Arg::new("service")
                        .help("Service name to resolve")
                        .required(true),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .help("Include unhealthy endpoints"),
                )
                .arg(
                    Arg::new("one")
                        .long("one")
                        .action(ArgAction::SetTrue)
                        .help("Print a single randomly picked endpoint address:port (scripts)"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("edges")
                .about("The service graph: who calls whom through the datapath, with request rate, errors, denials and latency")
                .arg(Arg::new("service").help("Only edges into or out of this service (optional)"))
                .arg(
                    Arg::new("window")
                        .long("window")
                        .default_value("5m")
                        .help("Window the counters cover (1m to 15m)"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("doctor")
                .about("Walks the discovery layers for a service and names the failing one")
                .arg(
                    Arg::new("service")
                        .help("Service name to diagnose")
                        .required(true),
                )
                .arg(json_flag()),
        )
}

#[instrument(skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("resolve", sub)) => resolve(sub).await,
        Some(("doctor", sub)) => doctor(sub).await,
        Some(("edges", sub)) => edges(sub).await,
        _ => unreachable!(),
    }
}

/// Machine-readable projection of one service-graph edge.
fn edge_json(edge: &ServiceEdge) -> Value {
    json!({
        "callerJob": edge.caller_job,
        "callerTask": edge.caller_task,
        "service": edge.service,
        "tag": edge.tag,
        "protocol": edge.protocol,
        "declared": edge.declared,
        "allowed": edge.allowed,
        "requests": edge.requests,
        "errors": edge.errors,
        "denied": edge.denied,
        "rps": edge.rps,
        "errorRate": edge.error_rate,
        "latencyMsAvg": edge.latency_ms_avg,
        "latencyMsMax": edge.latency_ms_max,
        "bytesOut": edge.bytes_out,
        "bytesIn": edge.bytes_in,
        "lastSeenUnix": edge.last_seen_unix,
    })
}

#[derive(Tabled)]
struct EdgeRow {
    #[tabled(rename = "CALLER")]
    caller: String,
    #[tabled(rename = "SERVICE")]
    service: String,
    #[tabled(rename = "TAG")]
    tag: String,
    #[tabled(rename = "STATE")]
    state: String,
    #[tabled(rename = "RPS")]
    rps: String,
    #[tabled(rename = "ERRORS")]
    errors: String,
    #[tabled(rename = "DENIED")]
    denied: String,
    #[tabled(rename = "LATENCY avg/max")]
    latency: String,
    #[tabled(rename = "LAST SEEN")]
    last_seen: String,
}

fn edge_state(edge: &ServiceEdge) -> &'static str {
    if !edge.allowed {
        "not allowed"
    } else if edge.denied > 0 {
        "denials"
    } else if edge.requests == 0 {
        if edge.declared {
            "declared, silent"
        } else {
            "silent"
        }
    } else if edge.error_rate >= 0.05 {
        "failing"
    } else if edge.error_rate > 0.0 {
        "some errors"
    } else {
        "healthy"
    }
}

async fn edges(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let service = matches
        .get_one::<String>("service")
        .cloned()
        .unwrap_or_default();
    let window_secs = matches
        .get_one::<String>("window")
        .and_then(|w| crate::structs::netpolicy::parse_duration(w).ok())
        .map(|d| d.as_secs() as u32)
        .unwrap_or(300);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "service edges",
        client
            .list_service_edges(ListServiceEdgesRequest {
                service,
                window_secs,
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "windowSecs": reply.window_secs,
            "edges": reply.edges.iter().map(edge_json).collect::<Vec<_>>(),
        }));
        return Ok(());
    }
    if reply.edges.is_empty() {
        println!(
            "no edges in the last {}s: nothing declares an upstream, and nothing called one",
            reply.window_secs
        );
        return Ok(());
    }
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let rows: Vec<EdgeRow> = reply
        .edges
        .iter()
        .map(|edge| EdgeRow {
            caller: format!("{}/{}", edge.caller_job, edge.caller_task),
            service: edge.service.clone(),
            tag: if edge.tag.is_empty() {
                "-".into()
            } else {
                edge.tag.clone()
            },
            state: format!(
                "{}{}",
                edge_state(edge),
                if edge.declared { " (declared)" } else { "" }
            ),
            rps: format!("{:.2}", edge.rps),
            errors: format!("{} ({:.1}%)", edge.errors, edge.error_rate * 100.0),
            denied: edge.denied.to_string(),
            latency: if edge.protocol == "http" && edge.requests > 0 {
                format!("{:.0}ms/{}ms", edge.latency_ms_avg, edge.latency_ms_max)
            } else {
                "-".into()
            },
            last_seen: if edge.last_seen_unix == 0 {
                "never".into()
            } else {
                format!("{}s ago", (now - edge.last_seen_unix).max(0))
            },
        })
        .collect();
    println!("{}", Table::new(rows));
    Ok(())
}

/// Machine-readable projection of a service summary.
fn service_json(service: &ServiceSummary) -> Value {
    json!({
        "service": service.service,
        "jobId": service.job_id,
        "taskName": service.task_name,
        "healthyCount": service.healthy_count,
        "totalCount": service.total_count,
        "tags": service.tags,
        // Connect intentions; empty means every job may connect.
        "allow": service.allow,
    })
}

/// Machine-readable projection of one endpoint.
fn endpoint_json(endpoint: &ServiceEndpoint) -> Value {
    json!({
        "service": endpoint.service,
        "address": endpoint.address,
        "publicAddress": (!endpoint.public_address.is_empty()).then_some(&endpoint.public_address),
        "port": endpoint.port,
        "allocationId": endpoint.allocation_id,
        "nodeId": endpoint.node_id,
        "healthy": endpoint.healthy,
        "healthStatus": endpoint.health_status,
        "tags": endpoint.tags,
    })
}

/// `host:port` form of an endpoint — what a client actually dials.
fn endpoint_address(endpoint: &ServiceEndpoint) -> String {
    format!("{}:{}", endpoint.address, endpoint.port)
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let services = output::resolve(
        format,
        "failed to list services",
        client
            .list_services(tonic::Request::new(ListServicesRequest {}))
            .await,
    )?
    .into_inner()
    .services;

    if format.is_json() {
        let json = services.iter().map(service_json).collect::<Vec<_>>();
        output::print_success(&json);
        return Ok(());
    }

    if services.is_empty() {
        println!("No services declared. Add a `service` block to a task to declare one.");
        return Ok(());
    }

    let rows = services.into_iter().map(|service| ServiceRow {
        service: service.service,
        job: service.job_id,
        task: service.task_name,
        healthy: format!("{}/{}", service.healthy_count, service.total_count),
        tags: service.tags.join(","),
    });
    println!("{}", Table::new(rows));
    Ok(())
}

async fn fetch_endpoints(
    client: &mut AuthedManagerClient,
    service: &str,
    include_unhealthy: bool,
    format: OutputFormat,
) -> R<Vec<ServiceEndpoint>> {
    let response = output::resolve(
        format,
        "failed to resolve service",
        client
            .resolve_service(tonic::Request::new(ResolveServiceRequest {
                service: service.to_string(),
                include_unhealthy,
            }))
            .await,
    )?;
    Ok(response.into_inner().endpoints)
}

/// Pick one endpoint uniformly at random.
///
/// Random rather than "the first" so repeated script invocations spread load
/// the same way the node-local forwarder's default `round_robin` strategy
/// does — `--one` is meant to be called in a loop.
fn pick_one(endpoints: &[ServiceEndpoint]) -> Option<&ServiceEndpoint> {
    endpoints.choose(&mut rand::thread_rng())
}

async fn resolve(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let service = matches
        .get_one::<String>("service")
        .context("service name required")?;
    let include_unhealthy = matches.get_flag("all");
    let mut client = connect_manager_client().await?;
    let endpoints = fetch_endpoints(&mut client, service, include_unhealthy, format).await?;

    if matches.get_flag("one") {
        return resolve_one(service, &endpoints, include_unhealthy, format);
    }

    if format.is_json() {
        let json = endpoints.iter().map(endpoint_json).collect::<Vec<_>>();
        output::print_success(&json);
        return Ok(());
    }

    if endpoints.is_empty() {
        println!(
            "No {}endpoints for service '{service}'. Try `legion service doctor {service}`.",
            if include_unhealthy { "" } else { "healthy " }
        );
        return Ok(());
    }

    let rows = endpoints.into_iter().map(|endpoint| EndpointRow {
        address: format!("{}:{}", endpoint.address, endpoint.port),
        public: if endpoint.public_address.is_empty() {
            "-".to_string()
        } else {
            format!("{}:{}", endpoint.public_address, endpoint.port)
        },
        alloc: endpoint.allocation_id,
        node: endpoint.node_id,
        healthy: if endpoint.healthy { "yes" } else { "no" }.to_string(),
        status: endpoint.health_status,
    });
    println!("{}", Table::new(rows));
    Ok(())
}

/// `--one`: emit exactly one endpoint so a script can write
/// `curl "http://$(legion service resolve web --one)"` without `jq`.
///
/// Resolving nothing is an error here (rather than an empty table): a script
/// substituting an empty string into a URL fails in a much more confusing
/// place than the command that produced it.
fn resolve_one(
    service: &str,
    endpoints: &[ServiceEndpoint],
    include_unhealthy: bool,
    format: OutputFormat,
) -> R<()> {
    let Some(endpoint) = pick_one(endpoints) else {
        return Err(output::fail_local(
            format,
            CliError::local(
                "NotFound",
                format!(
                    "no {}endpoints for service '{service}'",
                    if include_unhealthy { "" } else { "healthy " }
                ),
                Some("run `legion service doctor <SERVICE>` to find the failing layer"),
            ),
        ));
    };

    if format.is_json() {
        output::print_success(&endpoint_json(endpoint));
    } else {
        println!("{}", endpoint_address(endpoint));
    }
    Ok(())
}

/// One layer of the `doctor` walk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct DoctorCheck {
    /// Stable layer id: `declared`, `intentions`, `placement`, `nodes`,
    /// `ports`, `health`, `endpoints`.
    layer: &'static str,
    /// One of `ok`, `info`, `warn`, `fail`.
    status: &'static str,
    /// One-line verdict for this layer.
    summary: String,
    /// Supporting detail and fix hints.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    details: Vec<String>,
}

impl DoctorCheck {
    fn new(layer: &'static str, status: &'static str, summary: impl Into<String>) -> Self {
        DoctorCheck {
            layer,
            status,
            summary: summary.into(),
            details: Vec::new(),
        }
    }

    fn with_details(mut self, details: Vec<String>) -> Self {
        self.details = details;
        self
    }
}

/// Full `doctor` report. The walk stops at the first failing layer, so
/// `checks` is exactly the prefix of layers that were reachable.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Diagnosis {
    /// Service that was diagnosed.
    service: String,
    /// True when no layer failed.
    ok: bool,
    /// Connect intentions declared by the service (`service.allow`). Empty
    /// means every job may connect.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    allow: Vec<String>,
    /// Layer verdicts in walk order.
    checks: Vec<DoctorCheck>,
    /// `address:port` of every healthy endpoint, when the walk got that far.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    endpoints: Vec<String>,
}

async fn doctor(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let service = matches
        .get_one::<String>("service")
        .context("service name required")?;

    let mut client = connect_manager_client().await?;
    let services = output::resolve(
        format,
        "failed to list services",
        client
            .list_services(tonic::Request::new(ListServicesRequest {}))
            .await,
    )?
    .into_inner()
    .services;

    // Only reach for endpoints once the service is known to exist — an
    // undeclared service is diagnosed from the summary list alone. Intentions
    // ride along on the summary, so the whole report needs only `job:read`.
    let (endpoints, allow) = match services.iter().find(|entry| entry.service == *service) {
        Some(summary) => {
            let allow = summary.allow.clone();
            let endpoints = fetch_endpoints(&mut client, service, true, format).await?;
            (endpoints, allow)
        }
        None => (Vec::new(), Vec::new()),
    };

    let diagnosis = diagnose(service, &services, &endpoints, &allow);
    if format.is_json() {
        output::print_success(&diagnosis);
        return Ok(());
    }

    print_diagnosis(&diagnosis);
    Ok(())
}

/// Build the layered diagnosis. Pure so the whole walk is unit-testable.
fn diagnose(
    service: &str,
    services: &[ServiceSummary],
    endpoints: &[ServiceEndpoint],
    allow: &[String],
) -> Diagnosis {
    let (checks, resolved) = walk_layers(service, services, endpoints, allow);
    Diagnosis {
        service: service.to_string(),
        ok: !checks.iter().any(|check| check.status == STATUS_FAIL),
        allow: allow.to_vec(),
        checks,
        endpoints: resolved,
    }
}

fn walk_layers(
    service: &str,
    services: &[ServiceSummary],
    endpoints: &[ServiceEndpoint],
    allow: &[String],
) -> (Vec<DoctorCheck>, Vec<String>) {
    let mut checks = Vec::new();

    // Layer 1: is the service declared at all?
    let Some(summary) = services.iter().find(|entry| entry.service == service) else {
        let mut details = Vec::new();
        let known = services
            .iter()
            .map(|entry| entry.service.as_str())
            .collect::<Vec<_>>();
        if !known.is_empty() {
            details.push(format!("declared services: {}", known.join(", ")));
        }
        details.push("declare one by adding `service: { port: <port> }` to a task".to_string());
        checks.push(
            DoctorCheck::new(
                "declared",
                STATUS_FAIL,
                format!("service '{service}' is not declared by any job"),
            )
            .with_details(details),
        );
        return (checks, Vec::new());
    };
    checks.push(DoctorCheck::new(
        "declared",
        STATUS_OK,
        format!(
            "service '{service}' is declared            (job '{}', task '{}')",
            summary.job_id, summary.task_name
        ),
    ));

    // Connect intentions. An empty `allow` means "everyone", so only say
    // something when the service actually restricts its consumers.
    if let Some(check) = intentions_check(allow) {
        checks.push(check);
    }

    // Layer 2: are there any endpoints (allocations) at all?
    if endpoints.is_empty() {
        checks.push(
            DoctorCheck::new(
                "placement",
                STATUS_FAIL,
                "no allocations placed for this service",
            )
            .with_details(vec![
                "the job may still be scheduling or failed placement; check `legion job list`"
                    .to_string(),
            ]),
        );
        return (checks, Vec::new());
    }
    checks.push(DoctorCheck::new(
        "placement",
        STATUS_OK,
        format!("{} allocation(s) placed", endpoints.len()),
    ));

    // Layer 3: node liveness (endpoint health_status carries node state).
    let dead_node = endpoints
        .iter()
        .filter(|endpoint| endpoint.health_status.starts_with("node "))
        .collect::<Vec<_>>();
    if dead_node.len() == endpoints.len() {
        checks.push(
            DoctorCheck::new("nodes", STATUS_FAIL, "all endpoints are on dead/lost nodes")
                .with_details(
                    dead_node
                        .iter()
                        .map(|endpoint| {
                            format!(
                                "alloc {} : {}; the scheduler will reschedule when capacity allows",
                                endpoint.allocation_id, endpoint.health_status
                            )
                        })
                        .collect(),
                ),
        );
        return (checks, Vec::new());
    }
    checks.push(DoctorCheck::new("nodes", STATUS_OK, "node(s) alive"));

    // Layer 4: was a port assigned?
    let missing_port = endpoints
        .iter()
        .filter(|endpoint| endpoint.port == 0)
        .count();
    if missing_port == endpoints.len() {
        checks.push(
            DoctorCheck::new(
                "ports",
                STATUS_FAIL,
                "no port assigned to the service's port label",
            )
            .with_details(vec![format!(
                "check that service.port matches a network.ports entry name on task '{}'",
                summary.task_name
            )]),
        );
        return (checks, Vec::new());
    }
    checks.push(DoctorCheck::new("ports", STATUS_OK, "port assigned"));

    // Layer 5: check health.
    let unhealthy = endpoints
        .iter()
        .filter(|endpoint| !endpoint.healthy)
        .collect::<Vec<_>>();
    if unhealthy.is_empty() {
        checks.push(DoctorCheck::new(
            "health",
            STATUS_OK,
            "health checks passing",
        ));
    } else {
        let all_failing = unhealthy.len() == endpoints.len();
        let mut details = unhealthy
            .iter()
            .map(|endpoint| {
                format!(
                    "alloc {} on {}: {}",
                    endpoint.allocation_id,
                    endpoint.node_id,
                    if endpoint.health_status.is_empty() {
                        "no health report yet (still starting?)"
                    } else {
                        &endpoint.health_status
                    }
                )
            })
            .collect::<Vec<_>>();
        details
            .push("the task may not be listening yet; check task state and checks config".into());
        let summary_line = if all_failing {
            "all endpoints failing health checks".to_string()
        } else {
            format!(
                "{}/{} endpoints failing health checks",
                unhealthy.len(),
                endpoints.len()
            )
        };
        checks.push(
            DoctorCheck::new(
                "health",
                if all_failing {
                    STATUS_FAIL
                } else {
                    STATUS_WARN
                },
                summary_line,
            )
            .with_details(details),
        );
        if all_failing {
            return (checks, Vec::new());
        }
    }

    let resolved = endpoints
        .iter()
        .filter(|endpoint| endpoint.healthy)
        .map(endpoint_address)
        .collect::<Vec<_>>();
    checks.push(DoctorCheck::new(
        "endpoints",
        STATUS_OK,
        format!(
            "service resolves to {} healthy endpoint(s):",
            resolved.len()
        ),
    ));
    (checks, resolved)
}

/// The `service.allow` line, when the service declares intentions.
fn intentions_check(allow: &[String]) -> Option<DoctorCheck> {
    if allow.is_empty() {
        return None;
    }
    let list = allow.join(", ");
    if allow.iter().any(|entry| entry == "*") {
        return Some(DoctorCheck::new(
            "intentions",
            STATUS_INFO,
            format!("service.allow = {list}  (wildcard: every job may connect)"),
        ));
    }
    Some(
        DoctorCheck::new(
            "intentions",
            STATUS_INFO,
            format!("service.allow = {list}  (only these job ids may connect)"),
        )
        .with_details(vec![
            "any other job's `connect` upstream for this service resolves to an empty \
             endpoint list"
                .to_string(),
            "add the consumer's job id (or \"*\") to service.allow to permit it".to_string(),
        ]),
    )
}

fn status_glyph(status: &str) -> &'static str {
    match status {
        STATUS_OK => "✓",
        STATUS_INFO => "ℹ",
        STATUS_WARN => "‼",
        _ => "✗",
    }
}

fn print_diagnosis(diagnosis: &Diagnosis) {
    for check in &diagnosis.checks {
        println!("{} {}", status_glyph(check.status), check.summary);
        for detail in &check.details {
            println!("  → {detail}");
        }
    }
    for address in &diagnosis.endpoints {
        println!("    {address}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(service: &str, job: &str, task: &str) -> ServiceSummary {
        ServiceSummary {
            service: service.to_string(),
            job_id: job.to_string(),
            task_name: task.to_string(),
            healthy_count: 0,
            total_count: 0,
            tags: Vec::new(),
            allow: Vec::new(),
            meta: Default::default(),
        }
    }

    fn endpoint(alloc: i64, port: u32, healthy: bool, health_status: &str) -> ServiceEndpoint {
        ServiceEndpoint {
            service: "web".to_string(),
            job_id: "api".to_string(),
            task_name: "web".to_string(),
            allocation_id: alloc,
            node_id: format!("node-{alloc}"),
            address: format!("10.0.0.{alloc}"),
            port,
            healthy,
            health_status: health_status.to_string(),
            tags: Vec::new(),
            meta: Default::default(),
            port_label: String::new(),
            mesh_tls: false,
            public_address: String::new(),
        }
    }

    #[test]
    fn resolve_config_parses_one_and_all_flags() {
        let matches = config()
            .try_get_matches_from(["service", "resolve", "web", "--one", "--all", "-j"])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "resolve");
        assert_eq!(sub.get_one::<String>("service").unwrap(), "web");
        assert!(sub.get_flag("one"));
        assert!(sub.get_flag("all"));
        assert!(sub.get_flag("json"));
    }

    #[test]
    fn resolve_config_defaults_one_flag_off() {
        let matches = config()
            .try_get_matches_from(["service", "resolve", "web"])
            .unwrap();
        let (_, sub) = matches.subcommand().unwrap();
        assert!(!sub.get_flag("one"));
    }

    #[test]
    fn doctor_config_accepts_json_flag() {
        let matches = config()
            .try_get_matches_from(["service", "doctor", "web", "--json"])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "doctor");
        assert!(sub.get_flag("json"));
    }

    #[test]
    fn pick_one_returns_none_for_empty_list() {
        assert!(pick_one(&[]).is_none());
    }

    #[test]
    fn pick_one_only_returns_members_and_spreads_across_them() {
        let endpoints = vec![
            endpoint(1, 8080, true, "healthy"),
            endpoint(2, 8080, true, "healthy"),
        ];
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let picked = pick_one(&endpoints).expect("endpoint");
            assert!(endpoints
                .iter()
                .any(|e| e.allocation_id == picked.allocation_id));
            seen.insert(picked.allocation_id);
        }
        assert_eq!(seen.len(), 2, "random pick should reach both endpoints");
    }

    #[test]
    fn endpoint_address_joins_host_and_port() {
        assert_eq!(
            endpoint_address(&endpoint(3, 20003, true, "")),
            "10.0.0.3:20003"
        );
    }

    #[test]
    fn diagnose_reports_undeclared_service_and_lists_known_ones() {
        let services = vec![summary("cache", "redis", "redis")];
        let diagnosis = diagnose("web", &services, &[], &[]);
        assert!(!diagnosis.ok);
        assert_eq!(diagnosis.checks.len(), 1);
        assert_eq!(diagnosis.checks[0].layer, "declared");
        assert_eq!(diagnosis.checks[0].status, STATUS_FAIL);
        assert!(diagnosis.checks[0]
            .details
            .iter()
            .any(|detail| detail.contains("declared services: cache")));
    }

    #[test]
    fn diagnose_flags_missing_placement() {
        let services = vec![summary("web", "api", "web")];
        let diagnosis = diagnose("web", &services, &[], &[]);
        assert!(!diagnosis.ok);
        let layers: Vec<&str> = diagnosis.checks.iter().map(|c| c.layer).collect();
        assert_eq!(layers, vec!["declared", "placement"]);
        assert_eq!(diagnosis.checks[1].status, STATUS_FAIL);
    }

    #[test]
    fn diagnose_stops_at_dead_nodes() {
        let services = vec![summary("web", "api", "web")];
        let endpoints = vec![endpoint(1, 8080, false, "node lost")];
        let diagnosis = diagnose("web", &services, &endpoints, &[]);
        assert!(!diagnosis.ok);
        let last = diagnosis.checks.last().unwrap();
        assert_eq!(last.layer, "nodes");
        assert_eq!(last.status, STATUS_FAIL);
    }

    #[test]
    fn diagnose_stops_at_unassigned_ports() {
        let services = vec![summary("web", "api", "web")];
        let endpoints = vec![endpoint(1, 0, false, "starting")];
        let diagnosis = diagnose("web", &services, &endpoints, &[]);
        let last = diagnosis.checks.last().unwrap();
        assert_eq!(last.layer, "ports");
        assert_eq!(last.status, STATUS_FAIL);
        assert!(last.details[0].contains("service.port"));
    }

    #[test]
    fn diagnose_warns_on_partial_health_but_still_resolves() {
        let services = vec![summary("web", "api", "web")];
        let endpoints = vec![
            endpoint(1, 8080, true, "healthy"),
            endpoint(2, 8080, false, "connection refused"),
        ];
        let diagnosis = diagnose("web", &services, &endpoints, &[]);
        assert!(diagnosis.ok, "a partially healthy service still resolves");
        let health = diagnosis
            .checks
            .iter()
            .find(|check| check.layer == "health")
            .unwrap();
        assert_eq!(health.status, STATUS_WARN);
        assert_eq!(diagnosis.endpoints, vec!["10.0.0.1:8080".to_string()]);
    }

    #[test]
    fn diagnose_reports_all_healthy_endpoints() {
        let services = vec![summary("web", "api", "web")];
        let endpoints = vec![
            endpoint(1, 8080, true, "healthy"),
            endpoint(2, 8080, true, "healthy"),
        ];
        let diagnosis = diagnose("web", &services, &endpoints, &[]);
        assert!(diagnosis.ok);
        assert_eq!(diagnosis.checks.last().unwrap().layer, "endpoints");
        assert_eq!(
            diagnosis.endpoints,
            vec!["10.0.0.1:8080".to_string(), "10.0.0.2:8080".to_string()]
        );
    }

    #[test]
    fn diagnose_prints_allow_list_when_the_service_restricts_consumers() {
        let services = vec![summary("web", "api", "web")];
        let endpoints = vec![endpoint(1, 8080, true, "healthy")];
        let allow = vec!["gateway".to_string(), "batch".to_string()];
        let diagnosis = diagnose("web", &services, &endpoints, &allow);
        let intentions = diagnosis
            .checks
            .iter()
            .find(|check| check.layer == "intentions")
            .expect("intentions layer");
        assert_eq!(intentions.status, STATUS_INFO);
        assert!(intentions.summary.contains("gateway, batch"));
        assert!(!intentions.details.is_empty());
        assert_eq!(diagnosis.allow, allow);
        assert!(
            diagnosis.ok,
            "intentions are informational, never a failure"
        );
    }

    #[test]
    fn diagnose_omits_intentions_when_allow_is_empty() {
        let services = vec![summary("web", "api", "web")];
        let endpoints = vec![endpoint(1, 8080, true, "healthy")];
        let diagnosis = diagnose("web", &services, &endpoints, &[]);
        assert!(diagnosis
            .checks
            .iter()
            .all(|check| check.layer != "intentions"));
    }

    #[test]
    fn intentions_check_calls_out_the_wildcard() {
        let check = intentions_check(&["*".to_string()]).expect("check");
        assert!(check.summary.contains("every job may connect"));
        assert!(check.details.is_empty());
    }

    #[test]
    fn intentions_come_from_the_service_summary() {
        // Intentions are read off `ListServices` (job:read), not out of the
        // state store: a caller without `state:read` must still be able to
        // tell "no intentions declared" from "your job is not on the list".
        let mut gated = summary("web", "api", "web");
        gated.allow = vec!["gateway".to_string()];
        let diagnosis = diagnose(
            "web",
            &[gated.clone()],
            &[endpoint(1, 8080, true, "")],
            &gated.allow,
        );
        assert_eq!(diagnosis.allow, vec!["gateway".to_string()]);
        assert!(diagnosis
            .checks
            .iter()
            .any(|check| check.layer == "intentions"));

        // A service that declares none says nothing, as before.
        let open = summary("web", "api", "web");
        let diagnosis = diagnose(
            "web",
            std::slice::from_ref(&open),
            &[endpoint(1, 8080, true, "")],
            &open.allow,
        );
        assert!(diagnosis.allow.is_empty());
        assert!(!diagnosis
            .checks
            .iter()
            .any(|check| check.layer == "intentions"));
    }

    #[test]
    fn status_glyphs_cover_every_status() {
        assert_eq!(status_glyph(STATUS_OK), "✓");
        assert_eq!(status_glyph(STATUS_INFO), "ℹ");
        assert_eq!(status_glyph(STATUS_WARN), "‼");
        assert_eq!(status_glyph(STATUS_FAIL), "✗");
    }
}
