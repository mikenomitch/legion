use clap::Command;

pub fn config() -> Command {
    Command::new("start").about("Start an agent (manager or worker)")
}

pub fn handle(_sub_matches: &clap::ArgMatches) {
    run_server()
}

fn run_server() {
    println!("Starting agent")
}
