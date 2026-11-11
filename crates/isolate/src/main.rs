use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use legion_isolate::engine::{self, EngineParams};
use legion_isolate::{compile, confine};
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
        /// Skip confinement of the compiler process (tests only).
        #[arg(long, hide = true)]
        no_confine: bool,
    },
    /// Run as a worker's isolate host on inherited sockets.
    Host(HostArgs),
}

#[derive(Args)]
struct HostArgs {
    /// The socket the host serves `IsolateHost` on.
    #[arg(long)]
    control_fd: i32,
    /// The socket the host calls `IsolateWorker` on.
    #[arg(long)]
    worker_fd: i32,
    /// A descriptor carrying the `HostConfig` as JSON, read to its end.
    #[arg(long)]
    config_fd: i32,
    /// Test only: after confinement, try a forbidden action and report.
    #[cfg(feature = "confinement-test")]
    #[arg(long)]
    test_attempt: Option<String>,
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
        Command::Host(args) => legion_isolate::run_host::run(legion_isolate::run_host::Args {
            control_fd: args.control_fd,
            worker_fd: args.worker_fd,
            config_fd: args.config_fd,
            #[cfg(feature = "confinement-test")]
            test_attempt: args.test_attempt,
        }),
        Command::Compile {
            input,
            output,
            engine,
            no_confine,
        } => {
            let confinement = if no_confine {
                None
            } else {
                Some(confine_compiler(&input, &output))
            };
            match compile::compile_file(&engine.params(), &input, &output) {
                Ok(report) => {
                    let mut v = serde_json::to_value(&report).unwrap();
                    if let Some(c) = confinement {
                        v["confinement"] = c;
                    }
                    println!("{v}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    println!("{}", e.to_json());
                    ExitCode::from(1)
                }
            }
        }
    }
}

/// Best-effort confinement of the compiler: its own network namespace, no
/// new privileges, Landlock read on the input's directory and write on the
/// output's, and an address-space cap. Reported in the JSON it prints.
fn confine_compiler(input: &std::path::Path, output: &std::path::Path) -> serde_json::Value {
    let dir = |p: &std::path::Path| {
        p.parent()
            .filter(|d| !d.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf()
    };
    let plan = confine::Plan {
        read_dirs: vec![dir(input)],
        write_dirs: vec![dir(output)],
        seccomp: false,
        namespaces: true,
    };
    let mut notes = Vec::new();
    if let Err(e) = confine::limit_address_space(64 << 30) {
        notes.push(e);
    }
    let c = confine::confine(legion_proto::isolate_host::Confine::BestEffort, &plan)
        .unwrap_or_default();
    notes.extend(c.notes.iter().cloned());
    serde_json::json!({
        "networkNamespace": c.network_namespace,
        "noNewPrivs": c.no_new_privs,
        "landlock": c.landlock,
        "notes": notes,
    })
}
