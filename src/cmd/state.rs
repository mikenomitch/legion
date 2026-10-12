use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetStateBackupRequest, GetStateStoreSchemaRequest, QueryStateStoreRequest,
    UpsertSavedStateQueryRequest,
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
        // Liveness (status/heartbeat) lives in node_health; `node` keeps
        // identity/pool/availability (plan §7-C2 node split).
        "SELECT node.id AS node, node.pool, COALESCE(nh.status, 'active') AS status, \
         node.availability, COALESCE(nh.last_heartbeat, '') AS last_heartbeat \
         FROM node LEFT JOIN node_health nh ON nh.node_id = node.id ORDER BY node.id;",
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
         WHERE status IN ('pending','in_flight','blocked') ORDER BY priority DESC, id ASC;",
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
                .help(
                    "Allow row-mutating SQL statements (INSERT/UPDATE/DELETE). In raft mode \
                     schema changes (DDL) are refused: replication carries row changes only, \
                     so a DDL statement would apply on one node — ship it as a migration",
                )
                .action(ArgAction::SetTrue),
        )
        .arg(json_flag());

    let schema_cmd = Command::new("schema")
        .about("Show state store schema")
        .arg(json_flag());

    let seed_cmd = Command::new("seed-queries")
        .about("Install the curated library of agent introspection queries")
        .arg(json_flag());

    let backup_cmd = Command::new("backup")
        .about(
            "Write a consistent server-side backup of the state store (VACUUM INTO; \
             all-lane-leader-only in raft mode)",
        )
        .long_about(
            "Write a consistent server-side backup of the state store (VACUUM INTO).\n\n\
             The backup is a point-in-time image of THIS node's applied state — the whole \
             database, every lane's tables, as of one wall-clock moment. It is NOT a \
             cluster-wide snapshot at a single raft index: under `topology: lanes` the groups \
             apply independently, so the image sits at a different applied index per lane and \
             the reported applied index is the highest across them. That is what restore \
             consumes: `legion start manager --restore-from <path>` seeds a FRESH cluster from \
             the file (see docs/multi-raft.md and docs/upgrades.md).\n\n\
             In raft mode the command runs only on the node that leads every lane (leadership \
             is co-located); anywhere else it fails with the leader's address.",
        )
        .arg(Arg::new("out").long("out").value_name("PATH").help(
            "Server-side destination path (must not exist; defaults to a timestamped \
                     file under <storePath>/backups/)",
        ))
        .arg(json_flag());

    Command::new("state")
        .about("Inspect the manager state store")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(query_cmd)
        .subcommand(schema_cmd)
        .subcommand(seed_cmd)
        .subcommand(backup_cmd)
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
        Some(("backup", args)) => {
            let out = args.get_one::<String>("out").cloned();
            let format = OutputFormat::from_matches(args);
            run_backup(out, format).await
        }
        _ => unreachable!(),
    }
}

/// Ask the manager to write a server-side state backup (`VACUUM INTO`).
async fn run_backup(out: Option<String>, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "failed to create state backup",
        client
            .get_state_backup(tonic::Request::new(GetStateBackupRequest { out_path: out }))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "path": response.path,
            "bytes": response.bytes,
            "appliedIndex": response.applied_index,
            "encryption": response.encryption.as_ref().map(|enc| json!({
                "enabled": enc.enabled,
                "rootKeyProvider": enc.root_key_provider,
                "rootKeyLocation": enc.root_key_location,
                "rootKeyIds": enc.root_key_ids,
                "plaintextRows": enc.plaintext_rows,
            })),
        }));
        return Ok(());
    }

    println!(
        "Backup written on the manager host (point-in-time image of this node's applied state):"
    );
    println!("  path:          {}", response.path);
    println!("  bytes:         {}", response.bytes);
    println!("  applied index: {}", response.applied_index);
    println!("Restore with: legion start manager --restore-from <path> (see docs/upgrades.md)");
    // What the file does NOT contain, said where the file is reported
    // (docs/kv-encryption.md): a backup without its root key restores to a
    // locked keyring.
    if let Some(enc) = response.encryption.as_ref().filter(|enc| enc.enabled) {
        let ids = if enc.root_key_ids.is_empty() {
            "(none yet)".to_string()
        } else {
            enc.root_key_ids.join(", ")
        };
        println!(
            "KV values in this backup are encrypted. Root key {ids} ({} provider, {}) is NOT \
             included — keep it with the backup.",
            enc.root_key_provider, enc.root_key_location
        );
        if enc.plaintext_rows > 0 {
            println!(
                "  {} value(s) are still plaintext rows; `legion keyring rewrap` encrypts them.",
                enc.plaintext_rows
            );
        }
    }
    Ok(())
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
