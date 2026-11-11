use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use legion_isolate::compile;
use legion_isolate::engine::{self, EngineParams};
use legion_proto::isolate_host::PROTOCOL_VERSION;

/// Legion's isolate host and module compiler. Started by a worker; not meant
/// to be run by hand except `engine-info`.
#[derive(Parser)]
#[command(name = "legion-isolate", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the engine version and the artifact compatibility key for these
    /// engine settings, as one JSON object.
    EngineInfo(EngineArgs),
    /// Validate a component and compile it to an artifact a host can load.
    Compile {
        /// The component (`.wasm`).
        #[arg(long)]
        input: PathBuf,
        /// Where to write the artifact (`.cwasm`); written atomically.
        #[arg(long)]
        output: PathBuf,
        #[command(flatten)]
        engine: EngineArgs,
    },
}

/// The settings compiled code depends on. A host's `HostConfig` carries the
/// same three; compile with the values the host will run with.
#[derive(Args, Clone)]
struct EngineArgs {
    /// Linear-memory layout: guard, mpk, or small.
    #[arg(long, default_value = "guard", value_parser = engine::parse_layout)]
    layout: legion_proto::isolate_host::SlotLayout,
    /// The largest linear memory one isolate may have, in bytes.
    #[arg(long, default_value_t = 128 << 20)]
    slot_bytes: u64,
    /// Compile without Spectre mitigations on heap and table accesses.
    #[arg(long)]
    no_spectre_mitigations: bool,
}

impl EngineArgs {
    fn params(&self) -> EngineParams {
        EngineParams {
            layout: self.layout,
            slot_bytes: self.slot_bytes,
            spectre: !self.no_spectre_mitigations,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::EngineInfo(args) => {
            let p = args.params();
            match engine::compat_key(&p) {
                Ok(key) => {
                    let out = serde_json::json!({
                        "engineVersion": engine::ENGINE_VERSION,
                        "compatKey": key,
                        "protocolVersion": PROTOCOL_VERSION,
                        "target": engine::TARGET,
                        "layout": p.layout_name(),
                        "mpkAvailable": engine::mpk_available(),
                    });
                    println!("{out}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("legion-isolate engine-info: {e:#}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Compile {
            input,
            output,
            engine,
        } => match compile::compile_file(&engine.params(), &input, &output) {
            Ok(report) => {
                println!("{}", serde_json::to_string(&report).unwrap());
                ExitCode::SUCCESS
            }
            Err(e) => {
                println!("{}", e.to_json());
                ExitCode::from(1)
            }
        },
    }
}
