use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use legion_isolate::engine::{self, EngineParams};
use legion_isolate::{compile, confine};
use legion_proto::isolate_host::{Confine, Confinement, PROTOCOL_VERSION};

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
        /// How strictly to confine the compiler: `enforce` refuses to
        /// compile when a layer cannot be applied, `best-effort` reports
        /// what applied, `off` applies nothing (development only). The
        /// worker passes its `isolate.confine`.
        #[arg(long, default_value = "enforce", value_parser = parse_confine)]
        confine: Confine,
        /// End the compile (SIGALRM) after this many seconds, whatever its
        /// parent does: the worker passes its compile timeout, so a compile
        /// never outlives a worker that died by more than this. 0: none.
        #[arg(long, default_value_t = 0)]
        deadline_secs: u32,
        /// Test only: after confinement, try a forbidden action and report.
        #[cfg(feature = "confinement-test")]
        #[arg(long)]
        test_attempt: Option<String>,
        /// Test only: leave these confinement layers out.
        #[cfg(feature = "confinement-test")]
        #[arg(long, value_delimiter = ',')]
        test_skip: Vec<String>,
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
    /// Test only: leave these confinement layers out (seccomp, pidns,
    /// scopes, mount, landlock).
    #[cfg(feature = "confinement-test")]
    #[arg(long, value_delimiter = ',')]
    test_skip: Vec<String>,
}

fn parse_confine(s: &str) -> Result<Confine, String> {
    match s {
        "enforce" => Ok(Confine::Enforce),
        "best-effort" => Ok(Confine::BestEffort),
        "off" => Ok(Confine::Off),
        other => Err(format!(
            "unknown confinement {other:?}: use enforce, best-effort, or off"
        )),
    }
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
            #[cfg(feature = "confinement-test")]
            test_skip: args.test_skip,
        }),
        Command::Compile {
            input,
            output,
            engine,
            confine: level,
            deadline_secs,
            #[cfg(feature = "confinement-test")]
            test_attempt,
            #[cfg(feature = "confinement-test")]
            test_skip,
        } => {
            confine::close_fds_except(&[0, 1, 2]);
            // Before anything that could take long, and before the
            // PID-namespace fork (the alarm then fires in the parent, whose
            // death takes the compiling child with it).
            confine::set_deadline(deadline_secs);
            #[allow(unused_mut)]
            let mut plan = match compiler_plan(&input, &output) {
                Ok(p) => p,
                Err(e) => {
                    println!(
                        "{}",
                        serde_json::json!({ "ok": false, "kind": "io", "error": e })
                    );
                    return ExitCode::from(1);
                }
            };
            #[cfg(feature = "confinement-test")]
            {
                plan.skip = test_skip;
            }
            let (input, output) = (
                plan.read_files[0].clone(),
                plan.write_dirs[0].join(output.file_name().unwrap_or_default()),
            );
            let mut notes = Vec::new();
            if level != Confine::Off {
                if let Err(e) = confine::limit_address_space(COMPILE_ADDRESS_SPACE) {
                    notes.push(e);
                }
            }
            let c = match confine::confine(level, &plan, || {}) {
                Ok(c) => c,
                Err(e) => {
                    println!(
                        "{}",
                        serde_json::json!({ "ok": false, "kind": "confinement", "error": e })
                    );
                    return ExitCode::from(3);
                }
            };
            #[cfg(feature = "confinement-test")]
            if let Some(what) = &test_attempt {
                return legion_isolate::run_host::attempt(what, &plan);
            }
            match compile::compile_file(&engine.params(), &input, &output) {
                Ok(report) => {
                    let mut v = serde_json::to_value(&report).unwrap();
                    v["confinement"] = confinement_json(&c, notes);
                    println!("{v}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    let mut v = e.to_json();
                    v["confinement"] = confinement_json(&c, notes);
                    println!("{v}");
                    ExitCode::from(1)
                }
            }
        }
    }
}

/// The compiler's address-space cap (the worker sets its own, lower, one
/// before exec; this one never raises it).
const COMPILE_ADDRESS_SPACE: u64 = 64 << 30;

/// What the compiler may see: the input file (read-only) and the output's
/// directory (read-write), both resolved before confinement, nothing else
/// of the node's filesystem (a mount root, Landlock), with the compiler's
/// seccomp allowlist, in its own user, network, PID, mount, IPC, and UTS
/// namespaces.
fn compiler_plan(input: &Path, output: &Path) -> Result<confine::Plan, String> {
    let input =
        std::fs::canonicalize(input).map_err(|e| format!("reading {}: {e}", input.display()))?;
    let dir = output
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let dir =
        std::fs::canonicalize(dir).map_err(|e| format!("writing {}: {e}", output.display()))?;
    if output.file_name().is_none() {
        return Err(format!("writing {}: not a file name", output.display()));
    }
    Ok(confine::Plan {
        read_files: vec![input],
        write_dirs: vec![dir],
        seccomp: Some(confine::Filter::Compiler),
        namespaces: true,
        ..Default::default()
    })
}

/// The layers that applied, as the compiler's JSON reports them.
fn confinement_json(c: &Confinement, mut notes: Vec<String>) -> serde_json::Value {
    notes.extend(c.notes.iter().cloned());
    serde_json::json!({
        "userNamespace": c.user_namespace,
        "networkNamespace": c.network_namespace,
        "pidNamespace": c.pid_namespace,
        "mountRoot": c.mount_root,
        "noNewPrivs": c.no_new_privs,
        "landlock": c.landlock,
        "landlockScopes": c.landlock_scopes,
        "seccomp": c.seccomp,
        "notes": notes,
    })
}
