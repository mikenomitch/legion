use clap::Parser;
use colored::*;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(name = "cargo")]
#[command(bin_name = "cargo")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    name: String,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

fn main() {
    let cmd = clap::Command::new("legion")
        .bin_name("legion")
        .subcommand_required(true)
        .subcommand(
            clap::command!("example").arg(
                clap::arg!(--"manifest-path" <PATH>)
                    .value_parser(clap::value_parser!(std::path::PathBuf)),
            ),
        );

    let matches = cmd.get_matches();
    let matches = match matches.subcommand() {
        Some(("example", matches)) => matches,
        _ => unreachable!("clap should ensure we don't get here"),
    };

    let manifest_path = matches
        .get_one::<std::path::PathBuf>("manifest-path")
        .unwrap()
        .to_str()
        .unwrap();

    println!("{}", manifest_path.blue());
}
