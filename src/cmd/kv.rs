use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    DeleteKvRequest, GetKvRequest, KvEntry, KvValueType, ListKvRequest, PatchKvRequest,
    PutKvRequest,
};
use tabled::Tabled;

pub fn config() -> Command {
    let put_cmd = Command::new("put")
        .about("Store or update a key/value entry")
        .arg(
            Arg::new("key")
                .value_name("KEY")
                .help("Key to store (use / for nested paths)")
                .required(true),
        )
        .arg(
            Arg::new("value")
                .value_name("VALUE")
                .help("Value to store")
                .required(true),
        )
        .arg(
            Arg::new("type")
                .long("type")
                .short('t')
                .value_name("TYPE")
                .help("Value type: string, number, json")
                .default_value("string"),
        )
        .arg(
            Arg::new("expect")
                .long("expect")
                .value_name("VERSION")
                .help("Expected version for optimistic concurrency"),
        )
        .arg(
            Arg::new("lock-id")
                .long("lock-id")
                .value_name("LOCK_ID")
                .help("Lock identifier to enforce"),
        )
        .arg(json_flag());

    let get_cmd = Command::new("get")
        .about("Fetch a key/value entry")
        .arg(
            Arg::new("key")
                .value_name("KEY")
                .help("Key to fetch")
                .required(true),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list")
        .about("List key/value entries")
        .arg(
            Arg::new("prefix")
                .value_name("PREFIX")
                .help("Prefix to filter keys")
                .required(false),
        )
        .arg(json_flag());

    let delete_cmd = Command::new("delete")
        .about("Delete a key/value entry")
        .arg(
            Arg::new("key")
                .value_name("KEY")
                .help("Key to delete")
                .required(true),
        )
        .arg(
            Arg::new("expect")
                .long("expect")
                .value_name("VERSION")
                .help("Expected version for optimistic concurrency"),
        )
        .arg(
            Arg::new("lock-id")
                .long("lock-id")
                .value_name("LOCK_ID")
                .help("Lock identifier to enforce"),
        )
        .arg(json_flag());

    let patch_cmd = Command::new("patch")
        .about("Apply a JSON patch to a key/value entry")
        .arg(
            Arg::new("key")
                .value_name("KEY")
                .help("Key to patch")
                .required(true),
        )
        .arg(
            Arg::new("patch")
                .value_name("PATCH")
                .help("JSON patch payload")
                .required(true),
        )
        .arg(
            Arg::new("expect")
                .long("expect")
                .value_name("VERSION")
                .help("Expected version for optimistic concurrency"),
        )
        .arg(
            Arg::new("lock-id")
                .long("lock-id")
                .value_name("LOCK_ID")
                .help("Lock identifier to enforce"),
        )
        .arg(json_flag());

    Command::new("kv")
        .about("Manage key/value entries")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(put_cmd)
        .subcommand(get_cmd)
        .subcommand(list_cmd)
        .subcommand(delete_cmd)
        .subcommand(patch_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("put", arg_matches)) => {
            let key = arg_matches
                .get_one::<String>("key")
                .context("Must provide key")?;
            let value = arg_matches
                .get_one::<String>("value")
                .context("Must provide value")?;
            let value_type = parse_value_type(
                arg_matches
                    .get_one::<String>("type")
                    .map(|value| value.as_str())
                    .unwrap_or("string"),
            )?;
            let expected_version = arg_matches
                .get_one::<String>("expect")
                .map(|value| value.parse::<i64>())
                .transpose()
                .context("Invalid expected version")?;
            let lock_id = arg_matches.get_one::<String>("lock-id");
            let format = OutputFormat::from_matches(arg_matches);

            put_kv(key, value, value_type, expected_version, lock_id, format).await
        }
        Some(("get", arg_matches)) => {
            let key = arg_matches
                .get_one::<String>("key")
                .context("Must provide key")?;
            let format = OutputFormat::from_matches(arg_matches);
            get_kv(key, format).await
        }
        Some(("list", arg_matches)) => {
            let prefix = arg_matches
                .get_one::<String>("prefix")
                .map(String::as_str)
                .unwrap_or("");
            let format = OutputFormat::from_matches(arg_matches);
            list_kv(prefix, format).await
        }
        Some(("delete", arg_matches)) => {
            let key = arg_matches
                .get_one::<String>("key")
                .context("Must provide key")?;
            let expected_version = arg_matches
                .get_one::<String>("expect")
                .map(|value| value.parse::<i64>())
                .transpose()
                .context("Invalid expected version")?;
            let lock_id = arg_matches.get_one::<String>("lock-id");
            let format = OutputFormat::from_matches(arg_matches);
            delete_kv(key, expected_version, lock_id, format).await
        }
        Some(("patch", arg_matches)) => {
            let key = arg_matches
                .get_one::<String>("key")
                .context("Must provide key")?;
            let patch = arg_matches
                .get_one::<String>("patch")
                .context("Must provide patch payload")?;
            let expected_version = arg_matches
                .get_one::<String>("expect")
                .map(|value| value.parse::<i64>())
                .transpose()
                .context("Invalid expected version")?;
            let lock_id = arg_matches.get_one::<String>("lock-id");
            let format = OutputFormat::from_matches(arg_matches);
            patch_kv(key, patch, expected_version, lock_id, format).await
        }
        _ => unreachable!(),
    }
}

fn parse_value_type(value: &str) -> R<KvValueType> {
    match value.to_lowercase().as_str() {
        "string" => Ok(KvValueType::String),
        "number" => Ok(KvValueType::Number),
        "json" => Ok(KvValueType::Json),
        _ => Err(anyhow!(
            "Invalid value type: {value}. Must be string, number, or json"
        )),
    }
}

fn value_type_label(value_type: i32) -> &'static str {
    match KvValueType::try_from(value_type).unwrap_or(KvValueType::Unspecified) {
        KvValueType::String => "string",
        KvValueType::Number => "number",
        KvValueType::Json => "json",
        KvValueType::Unspecified => "unspecified",
    }
}

#[derive(Tabled)]
struct KvEntryPrint {
    key: String,
    value_type: String,
    version: i64,
    updated_at: String,
}

#[instrument(err, skip_all)]
async fn put_kv(
    key: &str,
    value: &str,
    value_type: KvValueType,
    expected_version: Option<i64>,
    lock_id: Option<&String>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = PutKvRequest {
        key: key.to_string(),
        value: value.to_string(),
        value_type: value_type as i32,
        expected_version,
        lock_id: lock_id.cloned(),
    };

    let response = output::resolve(
        format,
        "failed to put kv entry",
        client.put_kv(tonic::Request::new(request)).await,
    )?;
    let entry = response
        .into_inner()
        .entry
        .context("manager did not return kv entry")?;
    print_entry(&entry, format)?;

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_kv(key: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = GetKvRequest {
        key: key.to_string(),
    };
    let response = output::resolve(
        format,
        "failed to get kv entry",
        client.get_kv(tonic::Request::new(request)).await,
    )?;
    match response.into_inner().entry {
        Some(entry) => print_entry(&entry, format)?,
        None if format.is_json() => output::print_success(&Value::Null),
        None => println!("kv entry not found"),
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn list_kv(prefix: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = ListKvRequest {
        prefix: prefix.to_string(),
    };
    let response = output::resolve(
        format,
        "failed to list kv entries",
        client.list_kv(tonic::Request::new(request)).await,
    )?;
    let entries = response.into_inner().entries;
    if format.is_json() {
        output::print_success(&entries);
        return Ok(());
    }

    let printable: Vec<KvEntryPrint> = entries
        .into_iter()
        .map(|entry| KvEntryPrint {
            key: entry.key.clone(),
            value_type: value_type_label(entry.value_type).to_string(),
            version: entry.version,
            updated_at: entry.updated_at.clone(),
        })
        .collect();
    print_table(printable);

    Ok(())
}

#[instrument(err, skip_all)]
async fn delete_kv(
    key: &str,
    expected_version: Option<i64>,
    lock_id: Option<&String>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = DeleteKvRequest {
        key: key.to_string(),
        expected_version,
        lock_id: lock_id.cloned(),
    };
    let response = output::resolve(
        format,
        "failed to delete kv entry",
        client.delete_kv(tonic::Request::new(request)).await,
    )?;
    if format.is_json() {
        output::print_success(&response.into_inner());
    } else {
        println!("kv entry deleted");
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn patch_kv(
    key: &str,
    patch: &str,
    expected_version: Option<i64>,
    lock_id: Option<&String>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = PatchKvRequest {
        key: key.to_string(),
        patch_json: patch.to_string(),
        expected_version,
        lock_id: lock_id.cloned(),
    };
    let response = output::resolve(
        format,
        "failed to patch kv entry",
        client.patch_kv(tonic::Request::new(request)).await,
    )?;
    let entry = response
        .into_inner()
        .entry
        .context("manager did not return kv entry")?;
    print_entry(&entry, format)?;

    Ok(())
}

fn print_entry(entry: &KvEntry, format: OutputFormat) -> R<()> {
    if format.is_json() {
        output::print_success(entry);
        return Ok(());
    }

    println!("Key: {}", entry.key);
    println!("Type: {}", value_type_label(entry.value_type));
    println!("Version: {}", entry.version);
    println!("Updated: {}", entry.updated_at);
    println!("Value:\n{}", entry.value);

    Ok(())
}
