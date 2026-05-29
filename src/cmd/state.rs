use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetStateStoreSchemaRequest, QueryStateStoreRequest, UpsertSavedStateQueryRequest,
};
use tabled::Tabled;

/// Curated library of read-only introspection queries that are useful to
/// agents diagnosing cluster state. Installed by `legion state seed-queries`
/// and then available via the saved-query APIs / UI State Explorer.
pub const AGENT_SAVED_QUERIES: &[(&str, &str)] = &[
    (
        "agent:unhealthy-allocations",
        "SELECT a.id AS allocation_id, a.job_config_id, a.node_id, a.status, h.healthy, h.status AS health_status \
         FROM allocation a LEFT JOIN allocation_health h ON h.allocation_id = a.id \
         WHERE COALESCE(h.healthy, 1) = 0 OR a.status NOT IN ('running','complete') \
         ORDER BY a.id DESC;",
    ),
    (
        "agent:jobs-overview",
        "SELECT j.id AS job, j.status, COUNT(a.id) AS allocations \
         FROM job j \
         LEFT JOIN job_config jc ON jc.job_id = j.id \
         LEFT JOIN allocation a ON a.job_config_id = jc.id \
         GROUP BY j.id, j.status ORDER BY j.id;",
    ),
    (
        "agent:node-capacity",
        "SELECT id AS node, pool, status, availability, last_heartbeat FROM node ORDER BY id;",
    ),
    (
        "agent:active-rollouts",
        "SELECT id AS rollout_id, job_id, status, incomplete, from_job_config_id, to_job_config_id \
         FROM rollout WHERE status NOT IN ('completed','failed','cancelled','completed_incomplete') \
         ORDER BY id DESC;",
    ),
    (
        "agent:recent-failed-evals",
        "SELECT id AS eval_id, kind, status, job_id, job_config_id, node_name, priority \
         FROM eval WHERE status = 'failed' ORDER BY id DESC LIMIT 50;",
    ),
    (
        "agent:pending-evals",
        "SELECT id AS eval_id, kind, status, job_id, priority FROM eval \
         WHERE status IN ('pending','in_flight') ORDER BY priority DESC, id ASC;",
    ),
];

pub fn config() -> Command {
    let query_cmd = Command::new("query")
        .about("Run a SQL query directly against the manager state store")
        .arg(
            Arg::new("sql")
                .value_name("SQL")
                .help("SQL statement to execute")
                .required(true),
        )
        .arg(
            Arg::new("allow-write")
                .long("allow-write")
                .help("Allow write/mutating SQL statements")
                .action(ArgAction::SetTrue),
        )
        .arg(json_flag());

    let schema_cmd = Command::new("schema")
        .about("Show state store schema")
        .arg(json_flag());

    let seed_cmd = Command::new("seed-queries")
        .about("Install the curated library of agent introspection queries")
        .arg(json_flag());

    Command::new("state")
        .about("Inspect the manager state store")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(query_cmd)
        .subcommand(schema_cmd)
        .subcommand(seed_cmd)
}

#[derive(Tabled)]
struct QueryRowPrint {
    row: String,
}

#[derive(Tabled)]
struct SchemaPrint {
    kind: String,
    object: String,
    columns: String,
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("query", args)) => {
            let sql = args.get_one::<String>("sql").context("Must provide SQL")?;
            let allow_write = args.get_flag("allow-write");
            let format = OutputFormat::from_matches(args);
            run_query(sql, allow_write, format).await
        }
        Some(("schema", args)) => {
            let format = OutputFormat::from_matches(args);
            show_schema(format).await
        }
        Some(("seed-queries", args)) => {
            let format = OutputFormat::from_matches(args);
            seed_queries(format).await
        }
        _ => unreachable!(),
    }
}

/// Install the curated agent introspection query library into the manager.
async fn seed_queries(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let mut installed = Vec::new();
    for (name, sql) in AGENT_SAVED_QUERIES {
        output::resolve(
            format,
            "failed to save query",
            client
                .upsert_saved_state_query(tonic::Request::new(UpsertSavedStateQueryRequest {
                    name: name.to_string(),
                    sql: sql.to_string(),
                }))
                .await,
        )?;
        installed.push(*name);
    }

    if format.is_json() {
        output::print_success(&json!({
            "installed": installed,
            "count": installed.len(),
        }));
    } else {
        println!("Installed {} saved queries:", installed.len());
        for name in installed {
            println!("  {name}");
        }
    }
    Ok(())
}

async fn run_query(sql: &str, allow_write: bool, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to query state store",
        client
            .query_state_store(tonic::Request::new(QueryStateStoreRequest {
                sql: sql.to_string(),
                allow_write,
            }))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        let rows: Vec<Value> = response
            .rows
            .iter()
            .filter_map(|row| serde_json::from_str::<Value>(&row.json).ok())
            .collect();
        output::print_success(&json!({
            "statementType": response.statement_type,
            "columns": response.columns,
            "rows": rows,
            "affectedRows": response.affected_rows,
        }));
        return Ok(());
    }

    if !response.rows.is_empty() {
        print_table(
            response
                .rows
                .iter()
                .map(|row| QueryRowPrint {
                    row: row.json.clone(),
                })
                .collect(),
        );
    } else {
        println!(
            "{} statement executed. affected rows: {}",
            response.statement_type, response.affected_rows
        );
    }

    Ok(())
}

async fn show_schema(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to load state store schema",
        client
            .get_state_store_schema(tonic::Request::new(GetStateStoreSchemaRequest {}))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&response);
        return Ok(());
    }

    let rows = response
        .objects
        .into_iter()
        .map(|object| SchemaPrint {
            kind: object.kind,
            object: object.name,
            columns: object
                .columns
                .iter()
                .map(|column| format!("{}:{}", column.name, column.data_type))
                .collect::<Vec<String>>()
                .join(", "),
        })
        .collect();
    print_table(rows);
    Ok(())
}
