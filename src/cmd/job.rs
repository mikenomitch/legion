use clap::Command;

pub fn config() -> Command {
    Command::new("start").about("Start an agent (manager or worker)")
}

pub fn handle(sub_matches: &clap::ArgMatches) {
    match sub_matches.subcommand() {
        Some(("run", _)) => println!("Running job"),
        Some(("status", _)) => println!("Getting job status"),
        _ => unreachable!(),
    }
}
