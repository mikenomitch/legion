//! `legion scaling apply | list | delete`: standalone scaling policies
//! (docs/autoscaling.md, "Standalone policies").
//!
//! ```console
//! $ legion scaling apply render-farm.ncl
//! scaling policy media/render-farm: created (v1)
//! $ legion scaling list -A
//! $ legion scaling delete render-farm -n media
//! ```

use std::path::Path;
use std::time::{Duration, Instant};

use crate::cmd::helpers::{
    active_namespace, connect_manager_client, json_flag, list_namespace_flags, namespace_flag,
    namespace_selection, print_table,
};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    ApplyScalingPolicyRequest, DeleteScalingPolicyRequest, GetScalingStatusRequest,
    ListScalingPoliciesRequest,
};
use tabled::Tabled;

/// How long `apply --wait` waits for the autoscaler's verdict by default.
const WAIT_DEFAULT: Duration = Duration::from_secs(60);

/// `legion scaling apply`.
pub fn apply_command() -> Command {
    Command::new("apply")
        .about("Create or update a standalone scaling policy or a node group from a Nickel or JSON file")
        .long_about(
            "A standalone policy scales something that is not a Legion task, through a target \
             plugin installed in the autoscaler's app config (or the built-in noop target). \
             The file is the jobspec autoscale record plus name, namespace, description and \
             target = { plugin, config }, with min, max and checks required. A node group \
             (target = { nodes = { pool, workerType, shape, plugin, config } }, with max \
             required) is cluster-scoped and needs operator:write and node:write \
             (docs/autoscaling.md, \"Node groups\").",
        )
        .arg(
            Arg::new("file")
                .required(true)
                .value_name("FILE")
                .help("Policy file (.ncl or .json)")
                .value_hint(ValueHint::FilePath),
        )
        .arg(namespace_flag())
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .action(ArgAction::SetTrue)
                .help("Validate and show what would change, without writing"),
        )
        .arg(
            Arg::new("wait")
                .long("wait")
                .action(ArgAction::SetTrue)
                .conflicts_with("dry-run")
                .help("Wait until the autoscaler has evaluated the new version, and report whether it found it valid (plugin kinds and policyConfigSchema)"),
        )
        .arg(
            Arg::new("wait-timeout")
                .long("wait-timeout")
                .value_name("DURATION")
                .requires("wait")
                .help("How long --wait waits (default 60s)"),
        )
        .arg(idempotency_arg())
        .arg(json_flag())
}

/// `legion scaling list`.
pub fn list_command() -> Command {
    Command::new("list")
        .about(
            "Every scaling policy: autoscaled tasks and standalone policies, with mode and bounds",
        )
        .args(list_namespace_flags())
        .arg(
            Arg::new("detail")
                .long("detail")
                .action(ArgAction::SetTrue)
                .help("Include each standalone policy as written (with --json)"),
        )
        .arg(json_flag())
}

/// `legion scaling delete`.
pub fn delete_command() -> Command {
    Command::new("delete")
        .about("Delete a standalone scaling policy or a node group")
        .arg(
            Arg::new("name")
                .required(true)
                .value_name("NAME")
                .help("The policy's name (`name` or `namespace/name`)"),
        )
        .arg(namespace_flag())
        .arg(idempotency_arg())
        .arg(json_flag())
}

fn opt(matches: &ArgMatches, key: &str) -> Option<String> {
    matches.try_get_one::<String>(key).ok().flatten().cloned()
}

/// `legion scaling apply`.
pub async fn apply(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = Path::new(matches.get_one::<String>("file").expect("required"));
    let text = crate::config_lang::eval_file(path).map_err(|err| {
        output::fail_local(
            format,
            CliError::local(
                "invalid_scaling_policy_file",
                format!("failed to evaluate {}: {err:#}", path.display()),
                None,
            ),
        )
    })?;
    let namespace = active_namespace(matches)
        .unwrap_or_else(|| crate::structs::namespace::DEFAULT_NAMESPACE.to_string());
    // Parsed here too, so a typo fails before any network call.
    let parsed = serde_json::from_str::<Value>(&text)
        .map_err(|err| err.to_string())
        .and_then(|spec| crate::autoscaler::policy::parse(&spec, &namespace));
    match parsed {
        Ok(_) => {}
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("{}: {err}", path.display()),
                    Some("see docs/autoscaling.md, \"Standalone policies\""),
                ),
            ))
        }
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to apply the scaling policy",
        client
            .apply_scaling_policy(ApplyScalingPolicyRequest {
                spec_json: text,
                namespace: namespace.clone(),
                dry_run: matches.get_flag("dry-run"),
                idempotency_key: opt(matches, "idempotency-key"),
            })
            .await,
    )?
    .into_inner();

    let mut verdict: Option<(String, String)> = None;
    if matches.get_flag("wait") && !reply.deduplicated {
        let timeout = match opt(matches, "wait-timeout") {
            None => WAIT_DEFAULT,
            Some(text) => humantime::parse_duration(text.trim()).map_err(|err| {
                output::fail_local(
                    format,
                    CliError::local(
                        "InvalidArgument",
                        format!("--wait-timeout {text:?}: {err}"),
                        Some("write it like 2m"),
                    ),
                )
            })?,
        };
        verdict = Some(wait_for_evaluation(&mut client, &reply.id, &reply.digest, timeout).await?);
    }

    if format.is_json() {
        let mut data = serde_json::to_value(&reply)?;
        if let Some((state, summary)) = &verdict {
            data["evaluation"] = json!({ "state": state, "summary": summary });
        }
        if verdict
            .as_ref()
            .is_some_and(|(state, _)| state == "invalid")
        {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "FailedPrecondition",
                    format!(
                        "scaling policy {} was stored, and the autoscaler found it invalid: {}",
                        reply.id,
                        verdict.map(|(_, summary)| summary).unwrap_or_default()
                    ),
                    Some("fix the policy and apply it again; legion scaling explain shows the reason"),
                ),
            ));
        }
        output::print_success(&data);
        return Ok(());
    }
    println!("{}", reply.message);
    for line in &reply.diff {
        println!("  {line}");
    }
    match verdict {
        Some((state, summary)) if state == "invalid" => Err(anyhow!(
            "the autoscaler found scaling policy {} invalid: {summary}",
            reply.id
        )),
        Some((state, summary)) => {
            println!("autoscaler: {state}: {summary}");
            Ok(())
        }
        None => Ok(()),
    }
}

/// Poll `GetScalingStatus` until the autoscaler reports an evaluation of
/// the policy at `digest`: `(state, summary)`.
async fn wait_for_evaluation(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    id: &str,
    digest: &str,
    timeout: Duration,
) -> R<(String, String)> {
    let deadline = Instant::now() + timeout;
    loop {
        let reply = client
            .get_scaling_status(GetScalingStatusRequest {
                policy: id.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|status| {
                crate::cmd::helpers::friendly_status("failed to read scaling status", status)
            })?
            .into_inner();
        if let Some(target) = reply.targets.first() {
            let evaluated = target.last_evaluation_unix_ms > 0
                && (target.policy_digest == digest || digest.is_empty())
                && target.state != "not-evaluated"
                && target.state != "stale";
            if evaluated {
                return Ok((target.state.clone(), target.summary.clone()));
            }
        }
        if Instant::now() >= deadline {
            let app = reply.app.map(|app| app.message).unwrap_or_default();
            bail!(
                "the autoscaler has not evaluated scaling policy {id} within {}s ({app}). The \
                 policy is stored; `legion scaling explain --standalone {id}` shows it once it \
                 runs",
                timeout.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[derive(Tabled)]
struct ListRow {
    #[tabled(rename = "POLICY")]
    id: String,
    #[tabled(rename = "KIND")]
    kind: String,
    #[tabled(rename = "NAMESPACE")]
    namespace: String,
    #[tabled(rename = "MODE")]
    mode: String,
    #[tabled(rename = "BOUNDS")]
    bounds: String,
    #[tabled(rename = "TARGET")]
    target: String,
}

/// `legion scaling list`.
pub async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list scaling policies",
        client
            .list_scaling_policies(ListScalingPoliciesRequest {
                namespaces: namespace_selection(matches),
                detail: matches.get_flag("detail"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    if reply.policies.is_empty() {
        println!("no scaling policies (autoscale on a task, or legion scaling apply, adds one)");
        return Ok(());
    }
    print_table(
        reply
            .policies
            .iter()
            .map(|policy| ListRow {
                id: policy.id.clone(),
                kind: policy.kind.clone(),
                namespace: policy.namespace.clone(),
                mode: if policy.state == "ok" {
                    policy.mode.clone()
                } else {
                    policy.state.clone()
                },
                bounds: if policy.state == "ok" {
                    format!("{}..{}", policy.min, policy.max)
                } else {
                    "-".to_string()
                },
                target: if policy.state == "ok" {
                    policy.target.clone()
                } else {
                    policy.error.clone()
                },
            })
            .collect(),
    );
    Ok(())
}

/// `legion scaling delete`.
pub async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches.get_one::<String>("name").expect("required").clone();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to delete the scaling policy",
        client
            .delete_scaling_policy(DeleteScalingPolicyRequest {
                name,
                namespace: active_namespace(matches).unwrap_or_default(),
                idempotency_key: opt(matches, "idempotency-key"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    println!("{}", reply.message);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_parse_test() {
        let apply = apply_command()
            .try_get_matches_from([
                "apply",
                "p.ncl",
                "-n",
                "media",
                "--wait",
                "--wait-timeout",
                "2m",
            ])
            .unwrap();
        assert!(apply.get_flag("wait"));
        assert!(apply_command()
            .try_get_matches_from(["apply", "p.ncl", "--dry-run", "--wait"])
            .is_err());
        assert!(apply_command()
            .try_get_matches_from(["apply", "p.ncl", "--wait-timeout", "1m"])
            .is_err());
        let list = list_command().try_get_matches_from(["list", "-A"]).unwrap();
        assert_eq!(namespace_selection(&list), vec!["*".to_string()]);
    }
}
