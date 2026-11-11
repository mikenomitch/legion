//! `legion scheduler` — read-only diagnostics for the scheduling loop.
//!
//! Today that is one subcommand, `concurrency`, which answers the question no
//! other command answers: are placements slow because the scheduler is holding
//! itself back, or because the cluster is full? The two have opposite remedies
//! ("wait" versus "add nodes"), and a time-varying control loop is otherwise
//! unexplainable at 3am — the same argument `legion node cadence` makes for
//! heartbeats.

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::GetSchedulerConcurrencyRequest;

pub fn config() -> Command {
    let concurrency_cmd = Command::new("concurrency")
        .about("Report how many evals this manager will plan at once, and why")
        .long_about(
            "\
        Report how many evals this manager will plan at once, and why\n\
        \n\
        The manager measures its own plan-apply conflict rate and tunes how\n\
        many evals it PLANS concurrently, between a floor and `evalThreads`.\n\
        This command shows the current limit, the conflict and fallback rates\n\
        behind it, the goodput it is protecting, and — critically — whether\n\
        the cluster's conflicts are concurrency-driven (back off and wait) or\n\
        capacity-driven (add nodes).\n\
        \n\
        See docs/adaptive-scheduler-concurrency.md.\
        ",
        )
        .arg(json_flag());

    Command::new("scheduler")
        .about("Inspect the scheduling loop")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(concurrency_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("concurrency", arg_matches)) => {
            get_scheduler_concurrency(OutputFormat::from_matches(arg_matches)).await
        }
        _ => bail!("Unsupported scheduler subcommand"),
    }
}

#[instrument(skip_all)]
async fn get_scheduler_concurrency(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to fetch scheduler concurrency",
        client
            .get_scheduler_concurrency(tonic::Request::new(GetSchedulerConcurrencyRequest {}))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "mode": reply.mode,
            "limit": reply.limit,
            "floor": reply.floor,
            "ceiling": reply.ceiling,
            "evalThreads": reply.eval_threads,
            "inFlight": reply.in_flight,
            "state": reply.state,
            "secondsInState": reply.seconds_in_state,
            "conflictRate": reply.conflict_rate,
            "fallbackRate": reply.fallback_rate,
            "goodput": reply.goodput,
            "windowApplied": reply.window_applied,
            "windowFallback": reply.window_fallback,
            "windowRejected": reply.window_rejected,
            "lastAdjustmentDirection": reply.last_adjustment_direction,
            "lastAdjustmentFrom": reply.last_adjustment_from,
            "lastAdjustmentTo": reply.last_adjustment_to,
            "lastAdjustmentSecondsAgo": reply.last_adjustment_seconds_ago,
            "lastAdjustmentGoodputDelta": reply.last_adjustment_goodput_delta,
            "admissionSkips": reply.admission_skips,
            "isSchedulerLaneLeader": reply.is_scheduler_lane_leader,
        }));
        return Ok(());
    }

    if !reply.is_scheduler_lane_leader {
        println!(
            "This manager does not plan (it does not lead the scheduler lane).\n\
             Ask the manager that does — `legion raft status` names it."
        );
        return Ok(());
    }
    if reply.mode == "off" {
        println!(
            "Adaptive scheduler concurrency is off \
             (schedulerSettings.adaptiveConcurrency.mode).\n\
             evalThreads = {} is the fixed number of planners.",
            reply.eval_threads
        );
        return Ok(());
    }

    let shadow = if reply.mode == "enforce" {
        "adaptive"
    } else {
        "shadow — observe mode actuates nothing"
    };
    println!(
        "Admitted planners  {} of {}          ({}, {} for {}s)",
        reply.limit,
        reply.ceiling,
        shadow,
        reply.state.to_uppercase(),
        reply.seconds_in_state
    );
    let attempts = reply.window_applied + reply.window_fallback + reply.window_rejected;
    println!(
        "Conflict rate      {:.1}%            (rejected {} / attempts {}, 500ms window)",
        reply.conflict_rate * 100.0,
        reply.window_rejected,
        attempts
    );
    println!(
        "Fallback rate      {:.1}%            (races absorbed by candidate lists)",
        reply.fallback_rate * 100.0
    );
    println!("Goodput            {:.0} placements/sec", reply.goodput);
    if reply.last_adjustment_direction.is_empty() {
        println!("State              {}", reply.state);
    } else {
        println!(
            "State              {}        (last {} {}->{}, {}s ago, goodput {}{:.0}%)",
            reply.state,
            reply.last_adjustment_direction,
            reply.last_adjustment_from,
            reply.last_adjustment_to,
            reply.last_adjustment_seconds_ago,
            if reply.last_adjustment_goodput_delta >= 0.0 {
                "+"
            } else {
                ""
            },
            reply.last_adjustment_goodput_delta * 100.0
        );
    }
    println!(
        "Floor / ceiling    {} / {}           (evalThreads = {})",
        reply.floor, reply.ceiling, reply.eval_threads
    );

    // The one thing an operator must not have to infer: which of the two very
    // different problems they have.
    if reply.state == "saturated" {
        println!(
            "\nSATURATED — backing off did not improve goodput, so these conflicts are \
             capacity-driven,\nnot concurrency-driven. Add nodes; waiting will not help."
        );
    }
    println!("\nNon-planning evals (job stops) are not throttled.");
    if reply.admission_skips > 0 {
        println!(
            "{} non-planning eval(s) have been claimed ahead of the queue head while throttled.",
            reply.admission_skips
        );
    }

    Ok(())
}
