//! `legion keyring`: the operator surface of KV encryption at rest
//! (docs/kv-encryption.md). Most clusters never need any of it — encryption
//! is on by default with a root key the manager mints itself — so every
//! verb here is either a status check or a deliberate key operation.

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::storage::keyring::{generate_root_key_line, test_exec_provider};
use crate::structs::{
    GetKeyringStatusReply, GetKeyringStatusRequest, ResetKeyringRequest, RewrapKvRequest,
    RewrapRootKeyRequest, RotateDataKeyRequest,
};

const RESET_FLAG: &str = "i-understand-encrypted-values-will-be-lost";

fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

pub fn config() -> Command {
    let status_cmd = Command::new("status")
        .about("Show KV encryption on this manager: root key, data keys, what to do next")
        .long_about(
            "Show the KV encryption keyring as THIS manager sees it: whether encryption is on, \
             where the root key lives, whether every data key is unwrapped here, how many \
             values each key protects, how many rows are still plaintext, and the one thing \
             to do next if anything needs doing. The root key is per-manager state, so ask \
             each manager when diagnosing a raft cluster.",
        )
        .arg(json_flag());

    let rotate_cmd = Command::new("rotate")
        .about("Rotate the data key now (values are re-encrypted in the background)")
        .long_about(
            "Create a new active data key in one replicated write. The previous key is \
             retired but kept, so nothing becomes unreadable; the manager then re-encrypts \
             every value under the new key in the background and removes the retired key \
             once nothing references it. Rotation is automatic every \
             encryption.dataKeyRotationAge (default 30 days); use this after an offboarding \
             or a suspected exposure.",
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let rewrap_cmd = Command::new("rewrap")
        .about("Re-encrypt every value under the active data key now")
        .long_about(
            "Re-encrypt every KV value under the active data key, batch by batch, until \
             nothing is pending — plaintext rows from before encryption was enabled included. \
             Values do not change (no version bump, no job restarts). With --plaintext, decrypt \
             every value into a plaintext row instead: the step before setting \
             encryption.enabled = false or downgrading to a build without the keyring.",
        )
        .arg(
            Arg::new("plaintext")
                .long("plaintext")
                .help("Decrypt every value into a plaintext row instead of re-encrypting")
                .action(ArgAction::SetTrue),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let root_key_generate = Command::new("generate")
        .about("Print a new root key line (runs locally; nothing is changed)")
        .long_about(
            "Print one new root key as a base64 line. Put it FIRST in the root key file on \
             every manager (or in LEGION_ROOT_KEY for the env provider), then run `legion \
             keyring root-key rewrap` so the data keys are wrapped under it, then remove the \
             old line. Nothing on the cluster changes until you do that.",
        )
        .arg(json_flag());

    let root_key_rewrap = Command::new("rewrap")
        .about("Re-wrap every data key under the current root key")
        .long_about(
            "Re-wrap every data key under the root key provider's CURRENT key — the first \
             line of the key file, the env variable's first key, or the KMS key the exec \
             provider reports — in one replicated write. No value is touched. After this, \
             older root keys wrap nothing and can be deleted everywhere.",
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let root_key_cmd = Command::new("root-key")
        .about("Generate or rotate the root key")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(root_key_generate)
        .subcommand(root_key_rewrap);

    let provider_test = Command::new("test")
        .about("Check an exec root-key provider command before pointing a manager at it")
        .long_about(
            "Run a candidate `encryption.rootKey.command` the way the manager will — describe, \
             a wrap/unwrap round trip, a tampered ciphertext must be refused, and wrapping \
             must be randomized — and report each check. Runs locally; the manager is not \
             involved. Everything after `--` is the command.",
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_name("DURATION")
                .default_value("10s")
                .help("How long one provider call may take"),
        )
        .arg(
            Arg::new("command")
                .value_name("COMMAND")
                .num_args(1..)
                .trailing_var_arg(true)
                .required(true)
                .help("The provider command and its arguments"),
        )
        .arg(json_flag());

    let provider_cmd = Command::new("provider")
        .about("Work with exec root-key providers (KMS helpers)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(provider_test);

    let reset_cmd = Command::new("reset")
        .about("Break glass: the root key is gone — delete the values it protected and start over")
        .long_about(
            "Only for a root key that is gone for good. Deletes every KV value this manager \
             cannot decrypt, drops the data keys it cannot unwrap, starts a fresh keyring, and \
             prints exactly which keys were lost. Jobs that used a deleted value fail with \
             `missing_kv_dependency` and recover when you put the value back. Refuses to run \
             without the confirmation flag.",
        )
        .arg(
            Arg::new(RESET_FLAG)
                .long(RESET_FLAG)
                .help("Required confirmation: the undecryptable values are deleted, not recovered")
                .action(ArgAction::SetTrue),
        )
        .arg(json_flag());

    Command::new("keyring")
        .about("KV encryption at rest: status, key rotation, recovery")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(status_cmd)
        .subcommand(rotate_cmd)
        .subcommand(rewrap_cmd)
        .subcommand(root_key_cmd)
        .subcommand(provider_cmd)
        .subcommand(reset_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("status", args)) => status(OutputFormat::from_matches(args)).await,
        Some(("rotate", args)) => {
            rotate(
                args.get_one::<String>("idempotency-key").cloned(),
                OutputFormat::from_matches(args),
            )
            .await
        }
        Some(("rewrap", args)) => {
            rewrap(
                args.get_flag("plaintext"),
                args.get_one::<String>("idempotency-key").cloned(),
                OutputFormat::from_matches(args),
            )
            .await
        }
        Some(("root-key", args)) => match args.subcommand() {
            Some(("generate", args)) => root_key_generate(OutputFormat::from_matches(args)),
            Some(("rewrap", args)) => {
                root_key_rewrap(
                    args.get_one::<String>("idempotency-key").cloned(),
                    OutputFormat::from_matches(args),
                )
                .await
            }
            _ => unreachable!(),
        },
        Some(("provider", args)) => match args.subcommand() {
            Some(("test", args)) => {
                let command: Vec<String> = args
                    .get_many::<String>("command")
                    .map(|values| values.cloned().collect())
                    .unwrap_or_default();
                let timeout = args
                    .get_one::<String>("timeout")
                    .map(|value| humantime::parse_duration(value))
                    .transpose()
                    .context("--timeout is not a duration")?
                    .unwrap_or(std::time::Duration::from_secs(10));
                provider_test(command, timeout, OutputFormat::from_matches(args))
            }
            _ => unreachable!(),
        },
        Some(("reset", args)) => {
            let format = OutputFormat::from_matches(args);
            if !args.get_flag(RESET_FLAG) {
                return Err(output::fail_local(
                    format,
                    CliError::local(
                        "FailedPrecondition",
                        "keyring reset deletes every KV value this manager cannot decrypt",
                        Some(&format!(
                            "re-run with --{RESET_FLAG} once you are sure the root key is gone \
                             for good; `legion keyring status` shows which keys are locked"
                        )),
                    ),
                ));
            }
            reset(format).await
        }
        _ => unreachable!(),
    }
}

fn status_json(reply: &GetKeyringStatusReply) -> Value {
    json!({
        "manager": reply.manager,
        "enabled": reply.enabled,
        "state": reply.state,
        "rootKey": {
            "provider": reply.root_key_provider,
            "location": reply.root_key_location,
            "id": reply.root_key_id,
        },
        "keys": reply.keys.iter().map(|key| json!({
            "keyId": key.key_id,
            "state": key.state,
            "algorithm": key.algorithm,
            "createdAt": key.created_at,
            "retiredAt": key.retired_at,
            "rootKeyId": key.root_key_id,
            "rootProvider": key.root_provider,
            "valueCount": key.value_count,
            "unwrapped": key.unwrapped,
            "error": key.error,
        })).collect::<Vec<_>>(),
        "plaintextRows": reply.plaintext_rows,
        "encryptedRows": reply.encrypted_rows,
        "rewrap": reply.rewrap.as_ref().map(|rewrap| json!({
            "pending": rewrap.pending,
            "running": rewrap.running,
            "lastError": rewrap.last_error,
        })),
        "dataKeyRotationAge": reply.data_key_rotation_age,
        "activeKeyAgeSeconds": reply.active_key_age_seconds,
        "hint": reply.hint,
    })
}

fn human_age(seconds: i64) -> String {
    let seconds = seconds.max(0) as u64;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    if seconds < 3_600 {
        return format!("{}m", seconds / 60);
    }
    if seconds < 86_400 {
        return format!("{}h", seconds / 3_600);
    }
    format!("{}d", seconds / 86_400)
}

fn unix_to_rfc3339(unix: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(unix)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| unix.to_string())
}

/// Human rendering of the status reply. Reads top to bottom as "is it on,
/// where is the key, is it unlocked, what do I do".
pub fn render_status(reply: &GetKeyringStatusReply) -> String {
    let mut out = String::new();
    let state_line = match reply.state.as_str() {
        "unlocked" => "unlocked (every data key is available here)".to_string(),
        "locked" => "LOCKED — a data key cannot be unwrapped on this manager".to_string(),
        "pending_upgrade" => {
            "pending upgrade (values stored as before until every manager is upgraded)".to_string()
        }
        "uninitialized" => "not initialized yet".to_string(),
        "disabled" => "disabled".to_string(),
        other => other.to_string(),
    };
    out.push_str(&format!(
        "Encryption:   {}\n",
        if reply.enabled {
            "enabled (aes-256-gcm)"
        } else {
            "disabled (encryption.enabled = false)"
        }
    ));
    out.push_str(&format!(
        "Root key:     {}  {}  {}\n",
        reply.root_key_provider,
        if reply.root_key_id.is_empty() {
            "(no key material)".to_string()
        } else {
            reply.root_key_id.clone()
        },
        reply.root_key_location
    ));
    out.push_str(&format!("State:        {state_line}\n"));
    out.push_str(&format!("Manager:      {}\n", reply.manager));
    if reply.keys.is_empty() {
        out.push_str("Data keys:    none\n");
    } else {
        out.push_str("Data keys:\n");
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        for key in &reply.keys {
            // The checkpoint key seals published checkpoints, not KV values.
            let uses = if key.state == "checkpoint" {
                "seals published checkpoints".to_string()
            } else {
                format!("values {}", key.value_count)
            };
            let mut line = format!(
                "  {}  {:<8} created {}  age {}  {uses}",
                key.key_id,
                key.state,
                unix_to_rfc3339(key.created_at),
                human_age(now - key.created_at),
            );
            if !key.unwrapped {
                line.push_str(&format!("  LOCKED: {}", key.error));
            }
            out.push_str(&line);
            out.push('\n');
        }
    }
    out.push_str(&format!(
        "Values:       {} encrypted, {} plaintext\n",
        reply.encrypted_rows, reply.plaintext_rows
    ));
    if let Some(rewrap) = &reply.rewrap {
        let mut line = if rewrap.running {
            format!("Rewrap:       running, {} pending", rewrap.pending)
        } else if rewrap.pending > 0 {
            format!("Rewrap:       {} pending", rewrap.pending)
        } else {
            "Rewrap:       nothing pending".to_string()
        };
        if !rewrap.last_error.is_empty() {
            line.push_str(&format!("  (last error: {})", rewrap.last_error));
        }
        out.push_str(&line);
        out.push('\n');
    }
    out.push_str(&format!(
        "Rotation:     every {} (active key age {})\n",
        reply.data_key_rotation_age,
        human_age(reply.active_key_age_seconds)
    ));
    if !reply.hint.is_empty() {
        out.push_str(&format!("\nNext step:    {}\n", reply.hint));
    }
    out
}

async fn status(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read keyring status",
        client
            .get_keyring_status(tonic::Request::new(GetKeyringStatusRequest {}))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&status_json(&reply));
        return Ok(());
    }
    print!("{}", render_status(&reply));
    Ok(())
}

async fn rotate(idempotency_key: Option<String>, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to rotate the data key",
        client
            .rotate_data_key(tonic::Request::new(RotateDataKeyRequest {
                idempotency_key,
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "keyId": reply.key_id,
            "retiredKeyId": reply.retired_key_id,
            "deduplicated": reply.deduplicated,
        }));
        return Ok(());
    }
    if reply.deduplicated {
        println!(
            "Already rotated (deduplicated): active data key {}",
            reply.key_id
        );
    } else {
        println!("Rotated: new active data key {}", reply.key_id);
        if !reply.retired_key_id.is_empty() {
            println!(
                "Retired {} — values under it are re-encrypted in the background; `legion \
                 keyring status` shows progress",
                reply.retired_key_id
            );
        }
    }
    Ok(())
}

async fn rewrap(plaintext: bool, idempotency_key: Option<String>, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to rewrap kv values",
        client
            .rewrap_kv(tonic::Request::new(RewrapKvRequest {
                plaintext,
                idempotency_key,
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "rewrapped": reply.rewrapped,
            "remaining": reply.remaining,
            "plaintext": plaintext,
            "collectedKeys": reply.collected_keys,
            "deduplicated": reply.deduplicated,
        }));
        return Ok(());
    }
    if plaintext {
        println!(
            "Converted {} value(s) to plaintext rows ({} remaining)",
            reply.rewrapped, reply.remaining
        );
        if reply.remaining == 0 {
            println!(
                "Every value is plaintext now; encryption.enabled = false (or a downgrade) is safe."
            );
        }
    } else {
        println!(
            "Re-encrypted {} value(s) under the active data key ({} remaining)",
            reply.rewrapped, reply.remaining
        );
        if !reply.collected_keys.is_empty() {
            println!(
                "Removed retired data key(s): {}",
                reply.collected_keys.join(", ")
            );
        }
    }
    Ok(())
}

fn root_key_generate(format: OutputFormat) -> R<()> {
    let line = generate_root_key_line()?;
    if format.is_json() {
        output::print_success(&json!({ "rootKey": line }));
        return Ok(());
    }
    println!("{line}");
    eprintln!();
    eprintln!("Put this line FIRST in the root key file on every manager (older lines stay so");
    eprintln!("existing data keys still unwrap), or in LEGION_ROOT_KEY for the env provider.");
    eprintln!("Then run `legion keyring root-key rewrap`, and remove the old line afterwards.");
    Ok(())
}

async fn root_key_rewrap(idempotency_key: Option<String>, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to rewrap the keyring under the current root key",
        client
            .rewrap_root_key(tonic::Request::new(RewrapRootKeyRequest {
                idempotency_key,
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "keys": reply.keys,
            "rootKeyId": reply.root_key_id,
            "skipped": reply.skipped,
            "deduplicated": reply.deduplicated,
        }));
        return Ok(());
    }
    println!(
        "{} data key(s) now wrapped under root key {}",
        reply.keys, reply.root_key_id
    );
    if !reply.skipped.is_empty() {
        println!(
            "Skipped (locked on this manager, still under their old root key): {}",
            reply.skipped.join(", ")
        );
    } else {
        println!("Older root keys wrap nothing now; remove them from every manager.");
    }
    Ok(())
}

fn provider_test(
    command: Vec<String>,
    timeout: std::time::Duration,
    format: OutputFormat,
) -> R<()> {
    let checks = test_exec_provider(&command, timeout);
    let passed = checks.iter().all(|check| check.passed);
    if format.is_json() {
        let data = json!({
            "command": command,
            "passed": passed,
            "checks": checks.iter().map(|check| json!({
                "name": check.name,
                "passed": check.passed,
                "detail": check.detail,
            })).collect::<Vec<_>>(),
        });
        if passed {
            output::print_success(&data);
            return Ok(());
        }
        return Err(output::fail_local(
            format,
            CliError::local(
                "FailedPrecondition",
                format!(
                    "provider failed {} check(s): {}",
                    checks.iter().filter(|c| !c.passed).count(),
                    checks
                        .iter()
                        .filter(|c| !c.passed)
                        .map(|c| format!("{} ({})", c.name, c.detail))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
                Some("fix the provider command and run `legion keyring provider test` again"),
            ),
        ));
    }
    println!("Testing root key provider: {}", command.join(" "));
    for check in &checks {
        println!(
            "  [{}] {:<28} {}",
            if check.passed { "ok" } else { "FAIL" },
            check.name,
            check.detail
        );
    }
    if passed {
        println!("\nProvider is usable. Configure it with:");
        println!(
            "  encryption = {{ rootKey = {{ provider = \"exec\", command = [{}] }} }}",
            command
                .iter()
                .map(|part| format!("\"{part}\""))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(())
    } else {
        bail!(
            "provider failed {} check(s); do not point a manager at it yet",
            checks.iter().filter(|c| !c.passed).count()
        )
    }
}

async fn reset(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to reset the keyring",
        client
            .reset_keyring(tonic::Request::new(ResetKeyringRequest {
                confirm: crate::manager::server::KEYRING_RESET_CONFIRMATION.to_string(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "deletedValues": reply.deleted_values,
            "droppedKeys": reply.dropped_keys,
            "activeKeyId": reply.active_key_id,
        }));
        return Ok(());
    }
    println!(
        "Keyring reset: {} value(s) deleted, {} data key(s) dropped, active data key {}",
        reply.deleted_values.len(),
        reply.dropped_keys.len(),
        reply.active_key_id
    );
    if !reply.deleted_values.is_empty() {
        println!("Put these values back with `legion kv put`:");
        for key in &reply.deleted_values {
            println!("  {key}");
        }
    }
    Ok(())
}
