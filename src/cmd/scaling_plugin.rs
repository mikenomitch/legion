//! `legion scaling plugin test`: the scaling-plugin conformance suite
//! (docs/autoscaling-plugins.md, "Testing a plugin").
//!
//! ```console
//! $ legion scaling plugin test python3 calendar_strategy.py --policy-config '{"replicas": 6}'
//! $ legion scaling plugin test http://forecast.internal:8080/rpc
//! ```
//!
//! It runs locally against the program or URL given, the way the
//! autoscaler would, and needs no cluster.

use std::time::Duration;

use crate::apps::autoscaler::conformance::{self, Outcome, SuiteOptions};
use crate::apps::autoscaler::rpc::Endpoint;
use crate::cmd::helpers::{json_flag, print_table};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use tabled::Tabled;

/// `legion scaling plugin`.
pub fn plugin_command() -> Command {
    Command::new("plugin")
        .about("Scaling plugins: run the conformance suite against one")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("test")
                .about("Run the legion-scaling/1 conformance suite against a plugin program or URL")
                .long_about(
                    "Starts the plugin the way the autoscaler does and checks the handshake, every \
                     method its kinds declare, an unknown method, a malformed request, the \
                     per-call timeouts, and (for a program) a crash and restart. target/scale \
                     is asked for the count target/status reports, so nothing changes.",
                )
                .arg(
                    Arg::new("plugin")
                        .required(true)
                        .num_args(1..)
                        .trailing_var_arg(true)
                        .allow_hyphen_values(true)
                        .value_name("COMMAND | URL")
                        .help("The plugin program and its arguments, or an http(s):// URL"),
                )
                .arg(
                    Arg::new("config")
                        .long("config")
                        .value_name("JSON")
                        .help("The plugin-level config initialize sends, as the operator would install it"),
                )
                .arg(
                    Arg::new("policy-config")
                        .long("policy-config")
                        .value_name("JSON")
                        .help("The policy-level config strategy and target methods get (a strategy's settings, a target's config)"),
                )
                .arg(
                    Arg::new("query")
                        .long("query")
                        .value_name("QUERY")
                        .default_value("conformance")
                        .help("The query source/query sends"),
                )
                .arg(
                    Arg::new("timeout")
                        .long("timeout")
                        .value_name("DURATION")
                        .help("One timeout for every call instead of the protocol's (10s; 120s for targets)"),
                )
                .arg(json_flag()),
        )
}

#[derive(Tabled)]
struct CheckRow {
    #[tabled(rename = "CHECK")]
    name: String,
    #[tabled(rename = "RESULT")]
    outcome: String,
    #[tabled(rename = "MS")]
    millis: u64,
    #[tabled(rename = "DETAIL")]
    detail: String,
}

fn json_arg(format: OutputFormat, matches: &ArgMatches, key: &str) -> R<Value> {
    match matches.get_one::<String>(key) {
        None => Ok(Value::Null),
        Some(text) => serde_json::from_str(text).map_err(|err| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("--{key} is not JSON: {err}"),
                    Some("write it like '{\"replicas\": 6}'"),
                ),
            )
        }),
    }
}

/// `legion scaling plugin <sub>`.
pub async fn handle_plugin(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("test", sub)) => test(sub).await,
        _ => unreachable!("subcommand required"),
    }
}

async fn test(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let plugin: Vec<String> = matches
        .get_many::<String>("plugin")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    let endpoint = match plugin.as_slice() {
        [url] if url.starts_with("http://") || url.starts_with("https://") => {
            Endpoint::Url(url.clone())
        }
        _ => Endpoint::Command {
            argv: plugin.clone(),
            dir: None,
        },
    };
    let timeout = match matches.get_one::<String>("timeout") {
        None => None,
        Some(text) => Some(
            humantime::parse_duration(text.trim())
                .ok()
                .filter(|timeout| !timeout.is_zero())
                .ok_or_else(|| {
                    output::fail_local(
                        format,
                        CliError::local(
                            "InvalidArgument",
                            format!("--timeout {text:?} is not a duration"),
                            Some("write it like 5s"),
                        ),
                    )
                })?,
        ),
    };
    let options = SuiteOptions {
        config: json_arg(format, matches, "config")?,
        policy_config: json_arg(format, matches, "policy-config")?,
        query: matches
            .get_one::<String>("query")
            .cloned()
            .unwrap_or_default(),
        timeout: timeout.map(|timeout: Duration| timeout),
    };
    let report = conformance::run(endpoint, options).await;
    if format.is_json() {
        if report.passed {
            output::print_success(&report);
            return Ok(());
        }
        let failed: Vec<String> = report
            .checks
            .iter()
            .filter(|check| check.outcome == Outcome::Fail)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect();
        return Err(output::fail_local(
            format,
            CliError::local(
                "FailedPrecondition",
                format!(
                    "{} is not a conformant legion-scaling/1 plugin: {}",
                    report.plugin_ref,
                    failed.join("; ")
                ),
                Some("run without --json to see every check; docs/autoscaling-plugins.md lists what each one needs"),
            ),
        ));
    }
    if let Some(plugin) = &report.plugin {
        println!(
            "{} {} ({}){}",
            plugin.name,
            plugin.version,
            plugin.kinds.join(", "),
            if plugin.nodes { ", node targets" } else { "" }
        );
    }
    print_table(
        report
            .checks
            .iter()
            .map(|check| CheckRow {
                name: check.name.clone(),
                outcome: check.outcome.to_string(),
                millis: check.millis,
                detail: check.detail.clone(),
            })
            .collect(),
    );
    let failed = report
        .checks
        .iter()
        .filter(|check| check.outcome == Outcome::Fail)
        .count();
    if report.passed {
        println!("{}: conformant", report.plugin_ref);
        Ok(())
    } else {
        Err(anyhow!(
            "{}: {failed} check(s) failed; see docs/autoscaling-plugins.md",
            report.plugin_ref
        ))
    }
}
