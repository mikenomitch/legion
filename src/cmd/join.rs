use crate::prelude::*;
use crate::worker::agent::run_worker;

pub fn config() -> Command {
    Command::new("join").about("Join a cluster").arg(
        Arg::new("token")
            .value_name("TOKEN")
            .help("Join token issued by the manager")
            .required(true),
    )
}

#[instrument(err, skip_all)]
pub async fn handle(arg_matches: &ArgMatches) -> R<()> {
    let token = arg_matches
        .get_one::<String>("token")
        .context("Must provide token")?;
    run_worker(token).await
}
