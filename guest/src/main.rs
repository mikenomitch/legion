//! `legion-guest`: the static binary the worker copies into a sandbox guest
//! as `/legion/legion`. The init script runs it as `/legion/legion
//! guest-agent ...` or `/legion/legion guest-resume ...`, exactly as it would
//! run the worker's own `legion` binary.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(err) = legion_guest::cli::run(&args) {
        eprintln!("legion-guest: {err:#}");
        std::process::exit(1);
    }
}
