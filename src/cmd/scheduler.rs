//! `legion scheduler` — read-only introspection of the scheduler's health.
//!
//! Today the group holds one verb, `diagnose`, which answers "is my
//! scheduler healthy, and if not, what is wrong" in one command with the
//! remedy attached. Its own group rather than a `legion operator` verb
//! because the natural neighbours (`scheduler status`, `scheduler phases`)
//! are scheduler-shaped, not operator-shaped.
//!
//! Nothing here mutates the cluster. See
//! docs/diagnosing-scheduler-performance.md.

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{DiagnoseSchedulerReply, DiagnoseSchedulerRequest, SchedulerFinding};
use serde_json::{json, Map, Value};

/// Default sampling window. Long enough that a low-rate cluster produces
/// non-zero deltas, short enough to run mid-incident.
const DEFAULT_SAMPLE: &str = "30s";

/// Matches `MAX_DIAGNOSE_SAMPLE_SECONDS` on the manager; checked client-side
/// too so an obvious mistake fails immediately instead of after the manager
/// silently clamps it.
const MAX_SAMPLE_SECONDS: u64 = 300;

pub fn config() -> Command {
    let diagnose_cmd = Command::new("diagnose")
        .about("Report whether the scheduler is healthy, and what to change if not")
        .long_about(
            "\
        Report whether the scheduler is healthy, and what to change if not\n\
        \n\
        Samples the manager's metrics twice and reports the delta, so every\n\
        number is a rate over a stated window rather than a since-boot\n\
        average. Each failing check names what it RULED OUT as well as what\n\
        it found, and checks that passed are printed too — a passing check's\n\
        number is what stops the next responder re-investigating a cause\n\
        somebody already excluded.\n\
        \n\
        Read-only: nothing here mutates the cluster or tunes a knob.\n\
        See docs/diagnosing-scheduler-performance.md.\
        ",
        )
        .arg(
            Arg::new("sample")
                .long("sample")
                .value_name("DURATION")
                .default_value(DEFAULT_SAMPLE)
                .help("Sampling window (e.g. 30s, 2m). `0s` reports since-boot values"),
        )
        .arg(
            Arg::new("watch")
                .long("watch")
                .action(ArgAction::SetTrue)
                .help("Re-sample and reprint until interrupted"),
        )
        .arg(json_flag());

    Command::new("scheduler")
        .about("Inspect the scheduler")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(diagnose_cmd)
}

pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("diagnose", arg_matches)) => {
            let format = OutputFormat::from_matches(arg_matches);
            let sample = arg_matches
                .get_one::<String>("sample")
                .map(String::as_str)
                .unwrap_or(DEFAULT_SAMPLE);
            let sample_seconds = parse_sample(sample, format)?;
            let watch = arg_matches.get_flag("watch");
            diagnose(sample_seconds, watch, format).await
        }
        _ => unreachable!("subcommand_required"),
    }
}

/// Parse `--sample` as a humantime duration and reduce it to whole seconds.
fn parse_sample(raw: &str, format: OutputFormat) -> R<u32> {
    let duration = humantime::parse_duration(raw).map_err(|error| {
        output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                format!("invalid --sample value {raw:?}: {error}"),
                Some("use a duration like 30s, 2m, or 0s for since-boot values"),
            ),
        )
    })?;
    let seconds = duration.as_secs();
    if seconds > MAX_SAMPLE_SECONDS {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                format!("--sample {raw} is longer than the {MAX_SAMPLE_SECONDS}s maximum"),
                Some("every signature this command detects resolves inside 30s"),
            ),
        ));
    }
    Ok(seconds as u32)
}

/// `legion scheduler diagnose` — one command, one verdict, the remedy
/// computed rather than left as a formula.
#[instrument(err, skip_all)]
async fn diagnose(sample_seconds: u32, watch: bool, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;

    loop {
        let result = client
            .diagnose_scheduler(tonic::Request::new(DiagnoseSchedulerRequest {
                sample_seconds,
            }))
            .await;

        // A newer CLI against an older manager gets `Unimplemented`. Saying
        // so beats surfacing a bare gRPC error, because the fix (upgrade the
        // manager, or read the doc and do it by hand) is not guessable from
        // "unknown method".
        if let Err(status) = &result {
            if status.code() == tonic::Code::Unimplemented {
                return Err(output::fail_local(
                    format,
                    CliError::local(
                        "Unimplemented",
                        "this manager predates `legion scheduler diagnose`",
                        Some(
                            "upgrade the manager (see docs/upgrades.md), or work through \
                             docs/diagnosing-scheduler-performance.md by hand",
                        ),
                    ),
                ));
            }
        }

        let reply =
            output::resolve(format, "failed to diagnose the scheduler", result)?.into_inner();

        if format.is_json() {
            output::print_success(&reply_to_json(&reply));
        } else {
            print_report(&reply);
        }

        if !watch {
            return Ok(());
        }
        // `--sample 0s --watch` would spin: the RPC returns instantly, so
        // the loop needs its own pause.
        if sample_seconds == 0 {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        if !format.is_json() {
            println!();
        }
    }
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

fn evidence_to_json(evidence: &[crate::structs::SchedulerEvidence]) -> Value {
    let mut map = Map::new();
    for item in evidence {
        map.insert(item.key.clone(), Value::String(item.value.clone()));
    }
    Value::Object(map)
}

fn finding_to_json(finding: &SchedulerFinding) -> Value {
    let mut value = json!({
        "code": finding.code,
        "severity": finding.severity,
        "title": finding.title,
        "detail": finding.detail,
        "evidence": evidence_to_json(&finding.evidence),
        "ruledOut": finding.ruled_out.iter().map(|ruled| json!({
            "cause": ruled.cause,
            "evidence": evidence_to_json(&ruled.evidence),
        })).collect::<Vec<_>>(),
        "doc": finding.doc,
    });
    if finding.secondary {
        value["secondary"] = Value::Bool(true);
    }
    if let Some(action) = &finding.action {
        let mut map = Map::new();
        map.insert("kind".into(), Value::String(action.kind.clone()));
        if !action.field.is_empty() {
            map.insert("field".into(), Value::String(action.field.clone()));
            map.insert("current".into(), Value::String(action.current.clone()));
            map.insert("direction".into(), Value::String(action.direction.clone()));
            map.insert("suggested".into(), Value::String(action.suggested.clone()));
        }
        map.insert("text".into(), Value::String(action.text.clone()));
        value["action"] = Value::Object(map);
    }
    value
}

fn reply_to_json(reply: &DiagnoseSchedulerReply) -> Value {
    json!({
        "verdict": reply.verdict,
        "sampleSeconds": reply.sample_seconds,
        "isSchedulerLaneLeader": reply.is_scheduler_lane_leader,
        "findings": reply.findings.iter().map(finding_to_json).collect::<Vec<_>>(),
        "passed": reply.passed.iter().map(finding_to_json).collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------
// Human output
// ---------------------------------------------------------------------------

fn print_report(reply: &DiagnoseSchedulerReply) {
    let window = if reply.sample_seconds == 0 {
        "since boot".to_string()
    } else {
        format!("sampled {}s", reply.sample_seconds)
    };
    println!(
        "Scheduler                 {:<10} ({window})",
        reply.verdict.to_uppercase()
    );
    if !reply.is_scheduler_lane_leader {
        println!(
            "\n  This manager does not lead the scheduler lane, so its scheduler counters\n  \
             describe a process that is not scheduling. Ask the manager that does —\n  \
             `legion raft status` names it. Everything below is what this one can see."
        );
    }
    println!();

    for finding in &reply.findings {
        print_failing(finding);
    }
    for finding in &reply.passed {
        println!("  {:<34}{}", format!("✓ {}", finding.code), finding.title);
    }

    println!("\nNothing here mutates the cluster. See docs/diagnosing-scheduler-performance.md.");
}

fn print_failing(finding: &SchedulerFinding) {
    println!(
        "  {:<34}{}{}",
        format!("✗ {}", finding.code),
        finding.severity,
        if finding.secondary {
            "  (may be secondary — fix heartbeat cadence first)"
        } else {
            ""
        }
    );
    for line in wrap(&finding.title, 72) {
        println!("    {line}");
    }
    for line in wrap(&finding.detail, 72) {
        println!("    {line}");
    }
    if !finding.evidence.is_empty() {
        let rendered = finding
            .evidence
            .iter()
            .map(|item| format!("{}={}", item.key, item.value))
            .collect::<Vec<_>>()
            .join("; ");
        for line in wrap(&rendered, 72) {
            println!("    {line}");
        }
    }
    if !finding.ruled_out.is_empty() {
        let rendered = finding
            .ruled_out
            .iter()
            .map(|ruled| {
                let evidence = ruled
                    .evidence
                    .iter()
                    .map(|item| format!("{}={}", item.key, item.value))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} ({evidence})", ruled.cause)
            })
            .collect::<Vec<_>>()
            .join("; ");
        for (idx, line) in wrap(&rendered, 68).into_iter().enumerate() {
            if idx == 0 {
                println!("    Ruled out: {line}");
            } else {
                println!("               {line}");
            }
        }
    }
    if let Some(action) = &finding.action {
        for (idx, line) in wrap(&action.text, 68).into_iter().enumerate() {
            if idx == 0 {
                println!("    → {line}");
            } else {
                println!("      {line}");
            }
        }
    }
    if !finding.doc.is_empty() {
        println!("      {}", finding.doc);
    }
    println!();
}

/// Greedy word wrap. Terminals are not all 200 columns wide, and a finding
/// whose evidence scrolls off the right edge is a finding nobody reads.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
        } else if current.len() + 1 + word.len() <= width {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(std::mem::take(&mut current));
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

#[cfg(test)]
mod scheduler_cmd_test {
    use super::*;

    #[test]
    fn sample_accepts_durations_and_zero_test() {
        assert_eq!(parse_sample("30s", OutputFormat::Human).unwrap(), 30);
        assert_eq!(parse_sample("2m", OutputFormat::Human).unwrap(), 120);
        assert_eq!(parse_sample("0s", OutputFormat::Human).unwrap(), 0);
    }

    #[test]
    fn sample_rejects_garbage_and_absurd_windows_test() {
        assert!(parse_sample("banana", OutputFormat::Human).is_err());
        assert!(parse_sample("1h", OutputFormat::Human).is_err());
    }

    #[test]
    fn wrap_never_drops_a_word_test() {
        let text =
            "the report always carries the discriminator, including for the causes it ruled out";
        let wrapped = wrap(text, 20);
        assert!(wrapped
            .iter()
            .all(|line| line.len() <= 20 || !line.contains(' ')));
        assert_eq!(wrapped.join(" "), text);
    }

    #[test]
    fn json_shape_carries_the_documented_keys_test() {
        let reply = DiagnoseSchedulerReply {
            verdict: "degraded".into(),
            sample_seconds: 30,
            is_scheduler_lane_leader: true,
            findings: vec![SchedulerFinding {
                code: "plan_rejection_storm".into(),
                severity: "critical".into(),
                title: "31.9% of plans rejected".into(),
                detail: "each rejection is a discarded plan".into(),
                evidence: vec![crate::structs::SchedulerEvidence {
                    key: "rejectionShare".into(),
                    value: "0.319".into(),
                }],
                ruled_out: vec![crate::structs::SchedulerRuledOut {
                    cause: "pool_full".into(),
                    evidence: vec![crate::structs::SchedulerEvidence {
                        key: "candidateRankP50".into(),
                        value: "0.0".into(),
                    }],
                }],
                action: Some(crate::structs::SchedulerAction {
                    kind: "config".into(),
                    field: "evalThreads".into(),
                    current: "16".into(),
                    direction: "decrease".into(),
                    suggested: "8".into(),
                    text: "Reduce evalThreads to 8".into(),
                }),
                doc: "docs/diagnosing-scheduler-performance.md#x".into(),
                secondary: false,
            }],
            passed: Vec::new(),
        };

        let value = reply_to_json(&reply);
        assert_eq!(value["verdict"], "degraded");
        assert_eq!(value["sampleSeconds"], 30);
        let finding = &value["findings"][0];
        assert_eq!(finding["code"], "plan_rejection_storm");
        assert_eq!(finding["evidence"]["rejectionShare"], "0.319");
        assert_eq!(finding["ruledOut"][0]["cause"], "pool_full");
        assert_eq!(finding["action"]["field"], "evalThreads");
        assert_eq!(finding["action"]["direction"], "decrease");
        assert!(finding.get("secondary").is_none());
    }
}
