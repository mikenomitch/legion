use crate::cmd::helpers::{connect_manager_client, friendly_status, json_flag, print_table};
use crate::prelude::*;
use crate::structs::{GetStateStoreSchemaRequest, QueryStateStoreRequest};
use serde_json::{json, Value};
use tabled::Tabled;

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

    Command::new("state")
        .about("Inspect the manager state store")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(query_cmd)
        .subcommand(schema_cmd)
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
            let as_json = args.get_flag("json");
            run_query(sql, allow_write, as_json).await
        }
        Some(("schema", args)) => {
            let as_json = args.get_flag("json");
            show_schema(as_json).await
        }
        _ => unreachable!(),
    }
}

async fn run_query(sql: &str, allow_write: bool, as_json: bool) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = client
        .query_state_store(tonic::Request::new(QueryStateStoreRequest {
            sql: sql.to_string(),
            allow_write,
        }))
        .await
        .map_err(|status| friendly_status("failed to query state store", status))?
        .into_inner();

    if as_json {
        let rows: Vec<Value> = response
            .rows
            .into_iter()
            .filter_map(|row| serde_json::from_str::<Value>(&row.json).ok())
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "statementType": response.statement_type,
                "columns": response.columns,
                "rows": rows,
                "affectedRows": response.affected_rows,
            }))?
        );
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

async fn show_schema(as_json: bool) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = client
        .get_state_store_schema(tonic::Request::new(GetStateStoreSchemaRequest {}))
        .await
        .map_err(|status| friendly_status("failed to load state store schema", status))?
        .into_inner();

    if as_json {
        println!("{}", serde_json::to_string_pretty(&response)?);
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
