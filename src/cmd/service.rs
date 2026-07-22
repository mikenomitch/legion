use crate::cmd::helpers::{connect_manager_client, friendly_status, json_flag};
use crate::prelude::*;
use crate::structs::{ListServicesRequest, ResolveServiceRequest, ServiceEndpoint};
use tabled::{Table, Tabled};

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
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("doctor")
                .about("Walks the discovery layers for a service and names the failing one")
                .arg(
                    Arg::new("service")
                        .help("Service name to diagnose")
                        .required(true),
                ),
        )
}

#[instrument(err, skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("resolve", sub)) => resolve(sub).await,
        Some(("doctor", sub)) => doctor(sub).await,
        _ => unreachable!(),
    }
}

async fn list(matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = client
        .list_services(tonic::Request::new(ListServicesRequest {}))
        .await
        .map_err(|status| friendly_status("failed to list services", status))?;
    let services = response.into_inner().services;

    if matches.get_flag("json") {
        let json = services
            .iter()
            .map(|service| {
                json!({
                    "service": service.service,
                    "jobId": service.job_id,
                    "taskName": service.task_name,
                    "healthyCount": service.healthy_count,
                    "totalCount": service.total_count,
                    "tags": service.tags,
                })
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string(&json)?);
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

async fn fetch_endpoints(service: &str, include_unhealthy: bool) -> R<Vec<ServiceEndpoint>> {
    let mut client = connect_manager_client().await?;
    let response = client
        .resolve_service(tonic::Request::new(ResolveServiceRequest {
            service: service.to_string(),
            include_unhealthy,
        }))
        .await
        .map_err(|status| friendly_status("failed to resolve service", status))?;
    Ok(response.into_inner().endpoints)
}

async fn resolve(matches: &ArgMatches) -> R<()> {
    let service = matches
        .get_one::<String>("service")
        .context("service name required")?;
    let include_unhealthy = matches.get_flag("all");
    let endpoints = fetch_endpoints(service, include_unhealthy).await?;

    if matches.get_flag("json") {
        let json = endpoints
            .iter()
            .map(|endpoint| {
                json!({
                    "service": endpoint.service,
                    "address": endpoint.address,
                    "port": endpoint.port,
                    "allocationId": endpoint.allocation_id,
                    "nodeId": endpoint.node_id,
                    "healthy": endpoint.healthy,
                    "healthStatus": endpoint.health_status,
                    "tags": endpoint.tags,
                })
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string(&json)?);
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
        alloc: endpoint.allocation_id,
        node: endpoint.node_id,
        healthy: if endpoint.healthy { "yes" } else { "no" }.to_string(),
        status: endpoint.health_status,
    });
    println!("{}", Table::new(rows));
    Ok(())
}

async fn doctor(matches: &ArgMatches) -> R<()> {
    let service = matches
        .get_one::<String>("service")
        .context("service name required")?;

    let mut client = connect_manager_client().await?;
    let services = client
        .list_services(tonic::Request::new(ListServicesRequest {}))
        .await
        .map_err(|status| friendly_status("failed to list services", status))?
        .into_inner()
        .services;

    // Layer 1: is the service declared at all?
    let Some(summary) = services.iter().find(|entry| entry.service == *service) else {
        println!("✗ service '{service}' is not declared by any job");
        let known = services
            .iter()
            .map(|entry| entry.service.as_str())
            .collect::<Vec<_>>();
        if !known.is_empty() {
            println!("  → declared services: {}", known.join(", "));
        }
        println!("  → declare one by adding `service: {{ port: <port> }}` to a task");
        return Ok(());
    };
    println!(
        "✓ service '{service}' is declared            (job '{}', task '{}')",
        summary.job_id, summary.task_name
    );

    // Layer 2: are there any endpoints (allocations) at all?
    let endpoints = fetch_endpoints(service, true).await?;
    if endpoints.is_empty() {
        println!("✗ no allocations placed for this service");
        println!("  → the job may still be scheduling or failed placement; check:");
        println!("    legion job list");
        return Ok(());
    }
    println!("✓ {} allocation(s) placed", endpoints.len());

    // Layer 3: node liveness (endpoint health_status carries node state).
    let dead_node = endpoints
        .iter()
        .filter(|endpoint| endpoint.health_status.starts_with("node "))
        .collect::<Vec<_>>();
    if dead_node.len() == endpoints.len() {
        println!("✗ all endpoints are on dead/lost nodes");
        for endpoint in &dead_node {
            println!(
                "  → alloc {} : {}; the scheduler will reschedule when capacity allows",
                endpoint.allocation_id, endpoint.health_status
            );
        }
        return Ok(());
    }
    println!("✓ node(s) alive");

    // Layer 4: was a port assigned?
    let missing_port = endpoints
        .iter()
        .filter(|endpoint| endpoint.port == 0)
        .collect::<Vec<_>>();
    if missing_port.len() == endpoints.len() {
        println!("✗ no port assigned to the service's port label");
        println!(
            "  → check that service.port matches a network.ports entry name on task '{}'",
            summary.task_name
        );
        return Ok(());
    }
    println!("✓ port assigned");

    // Layer 5: check health.
    let unhealthy = endpoints
        .iter()
        .filter(|endpoint| !endpoint.healthy)
        .collect::<Vec<_>>();
    if !unhealthy.is_empty() {
        if unhealthy.len() == endpoints.len() {
            println!("✗ all endpoints failing health checks");
        } else {
            println!(
                "‼ {}/{} endpoints failing health checks",
                unhealthy.len(),
                endpoints.len()
            );
        }
        for endpoint in &unhealthy {
            println!(
                "  → alloc {} on {}: {}",
                endpoint.allocation_id,
                endpoint.node_id,
                if endpoint.health_status.is_empty() {
                    "no health report yet (still starting?)"
                } else {
                    &endpoint.health_status
                }
            );
        }
        println!("  → the task may not be listening yet; check task state and checks config");
        if unhealthy.len() == endpoints.len() {
            return Ok(());
        }
    } else {
        println!("✓ health checks passing");
    }

    let healthy = endpoints
        .iter()
        .filter(|endpoint| endpoint.healthy)
        .collect::<Vec<_>>();
    println!(
        "✓ service resolves to {} healthy endpoint(s):",
        healthy.len()
    );
    for endpoint in healthy {
        println!("    {}:{}", endpoint.address, endpoint.port);
    }
    Ok(())
}
