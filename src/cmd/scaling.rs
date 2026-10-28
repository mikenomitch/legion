//! `legion scaling` — the autoscaler's CLI (docs/autoscaling.md).
//!
//! Each verb lives in its own module and registers here, so the verbs
//! later phases add (`status`, `explain`, `history`, `apply`, ...) slot in
//! beside `demand` without touching it.

use crate::prelude::*;

pub fn config() -> Command {
    Command::new("scaling")
        .about("Autoscaling: capacity demand for node groups")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(crate::cmd::scaling_demand::demand_command())
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("demand", matches)) => crate::cmd::scaling_demand::handle_demand(matches).await,
        _ => unreachable!(),
    }
}
