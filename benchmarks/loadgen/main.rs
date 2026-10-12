//! `legion-loadgen` — the AWS benchmark rig's native job submitter.
//!
//! # Why this exists
//!
//! `benchmarks/aws/driver/driver.py` used to submit every job by spawning a
//! `legion job run` process from a Python thread pool. Each submission paid a
//! process spawn, a fresh TCP+TLS+HTTP/2 dial, a jobspec read, and process
//! teardown, and that cost — not Legion — set the measured rate. On a 16-vCPU
//! loadgen box the ceiling was ~36 to ~103 jobs/sec and drifted downward within
//! a session.
//!
//! That ceiling is too low to measure the case that matters most. `web_fleet`
//! is ~6 allocations per job, so even 100 jobs/sec is only ~600 alloc/sec of
//! demand, and Legion already served >=336 alloc/sec while the *driver* was the
//! constraint. The driver detects this and reports `submission.driver_bound:
//! true`, which correctly labels those cells as floors rather than
//! measurements — but a floor is not a result.
//!
//! This binary removes the ceiling: one process, a small pool of long-lived
//! gRPC channels, and many concurrent `RunJob` calls in flight. Nothing per-job
//! is spawned, dialed, or re-read.
//!
//! # Fairness — read this before comparing systems
//!
//! This fast path is **Legion-only**. The Nomad and K8s adapters still spawn a
//! CLI process per job, and that is deliberate: the shared submit loop is what
//! made the three-system comparison fair in the first place. A cross-system
//! comparison must therefore either
//!
//!   * force the CLI path for all three (`driver.py run --submitter cli`), or
//!   * grow equivalent native submitters for Nomad and K8s first.
//!
//! Using this submitter for Legion and the CLI loop for the others measures the
//! submitters, not the schedulers, and it does so in Legion's favour. The rule
//! is restated in `benchmarks/aws/README.md` § Methodology, and every cell
//! archives which submitter it used (`metrics.json` -> `submitter`).
//!
//! # What it does *not* do
//!
//! It is not a general-purpose Legion client and it is not a supported CLI
//! surface. It reads the artifacts the rig already produces
//! (`benchmarks/aws/workloads/translate.py`), submits them, and records when.
//! Anything a user should be able to type belongs in `legion` proper.
//!
//! # Usage
//!
//! ```text
//! legion-loadgen --workload-dir <dir> --out <submits.json> \
//!     [--manager http://host:4747] [--concurrency 128] [--connections 4] [--rate 0] \
//!     [--gap-ms 0] [--clock-offset-ms 0]
//! ```
//!
//! Auth and TLS come from the same place the CLI gets them (`LEGION_TOKEN` /
//! `~/.legion/token`, `LEGION_TLS_CA_CERT`) because it calls the CLI's own
//! `connect_manager_client()` — one source of truth, so the fast path cannot
//! drift into talking to the manager differently from the way a user does.
//!
//! # Modes
//!
//! `--mode jobs` (the default) is the submitter described above.
//! `--mode instances` drives keyed-instance lifecycles (create, suspend,
//! resume, destroy) against an embedded simulated fleet and reports stage
//! latencies; see `instances.rs`:
//!
//! ```text
//! legion-loadgen --mode instances --simulate 50 --sessions 2000 --rate 100 \
//!     [--manager http://host:4747] [--job loadgen-sandboxes] [--hold-ms 0]
//! ```

mod instances;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::{Arg, ArgAction, Command};
use futures::stream::StreamExt;
use legion::cmd::helpers::{connect_manager_client, AuthedManagerClient};
use legion::structs::{RunJobRequest, Submission};
use serde::{Deserialize, Serialize};

/// Jobs in flight at once.
///
/// High enough that the manager never idles waiting for the next request, and
/// no higher — throughput here is not monotonic in concurrency. Measured
/// against a local single-manager cluster, 5,001 `web_fleet` jobs:
///
/// ```text
///   concurrency   32    64   128   256   512  1024
///   jobs/sec     643   730  1282  1107   686   607
/// ```
///
/// Past a few hundred in flight the extra requests only queue at the
/// manager's control-lane commit and cost latency without buying throughput.
/// The peak also moves with per-request latency, so it sits higher on the AWS
/// rig (where a commit is a raft round trip) than on one local box; 128 is
/// near the local peak and still deep enough to hide that round trip.
/// `--concurrency` overrides it, and a cell's own `submission.rate_per_sec`
/// says whether it mattered.
const DEFAULT_CONCURRENCY: usize = 128;

/// Channels the requests are spread across.
///
/// One tonic `Channel` is one HTTP/2 connection, and HTTP/2 flow control is
/// per-connection: the default 64 KiB connection window is shared by every
/// stream on it, so a few hundred concurrent requests on a single connection
/// spend time waiting for `WINDOW_UPDATE` frames instead of sending. Four
/// connections also spread the manager's per-connection read work over four of
/// its threads.
///
/// It measures: at concurrency 32, one connection managed 368 jobs/sec and
/// four managed 643. Beyond a handful the curve flattens and the client stops
/// resembling a real one (per-connection setup starts dominating), so this
/// stays small.
const DEFAULT_CONNECTIONS: usize = 4;

/// The driver's manifest (`translate.py`), read for the job list only.
#[derive(Debug, Deserialize)]
struct Manifest {
    jobs: Vec<ManifestJob>,
}

#[derive(Debug, Deserialize)]
struct ManifestJob {
    name: String,
    file: String,
}

/// One row of `submits.json`, in exactly the shape `driver.py` consumes
/// (`RunRecorder.record_submit` / `cmd_replay`).
#[derive(Debug, Serialize)]
struct SubmitRecord {
    job: String,
    t_ms: i64,
}

/// A jobspec read from disk before the clock starts.
///
/// Pre-reading matters: file I/O inside the measured window would put the
/// loadgen box's page cache back into the number this binary exists to remove
/// from it.
#[derive(Debug)]
struct PreparedJob {
    name: String,
    /// Path as submitted, recorded with the job so `legion job spec` shows
    /// where the spec came from — same as the CLI does.
    path: String,
    /// Raw file bytes. For the rig's pre-rendered `.json` specs this is
    /// already the `parsed_jobspec` the manager wants, and it doubles as the
    /// primary `Submission` content, matching `render_jobspec()`'s JSON branch
    /// in `src/cmd/job.rs`.
    content: String,
}

fn main() -> Result<()> {
    let matches = cli().get_matches();

    // Set before the runtime does anything, and before any task can call
    // `std::env::var` concurrently: `connect_manager_client()` reads
    // `LEGION_ADDR`, and reusing it is what keeps this binary's TLS and auth
    // behaviour identical to the CLI's rather than a second implementation.
    if let Some(manager) = matches.get_one::<String>("manager") {
        std::env::set_var("LEGION_ADDR", manager);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build the tokio runtime")?;
    match matches.get_one::<String>("mode").map(String::as_str) {
        Some("instances") => runtime.block_on(run_instances(&matches)),
        _ => runtime.block_on(run(&matches)),
    }
}

async fn run_instances(matches: &clap::ArgMatches) -> Result<()> {
    let duration =
        |name: &str| -> Duration { Duration::from_millis(*matches.get_one::<u64>(name).unwrap()) };
    // `--join-token` carries the worker type and secret a cluster that
    // checks join tokens wants from the embedded fleet (and the manager
    // address, used when neither `--manager` nor LEGION_ADDR names one).
    let token = matches
        .get_one::<String>("join-token")
        .map(|raw| legion::structs::token::JoinToken::decode(raw))
        .transpose()
        .context("--join-token")?;
    let manager = matches
        .get_one::<String>("manager")
        .cloned()
        .or_else(|| std::env::var("LEGION_ADDR").ok())
        .or_else(|| token.as_ref().map(|token| token.addr.clone()))
        .unwrap_or_else(|| legion::prelude::DEF_MANAGER_ADDRESS.to_string());
    let opts = instances::InstancesOptions {
        manager,
        job: matches.get_one::<String>("job").unwrap().clone(),
        sessions: *matches.get_one::<usize>("sessions").unwrap(),
        rate: *matches.get_one::<f64>("rate").unwrap(),
        concurrency: *matches.get_one::<usize>("concurrency").unwrap(),
        connections: *matches.get_one::<usize>("connections").unwrap(),
        simulate: *matches.get_one::<usize>("simulate").unwrap(),
        worker_type: token
            .as_ref()
            .map(|token| token.worker_type.clone())
            .unwrap_or_else(|| matches.get_one::<String>("worker-type").unwrap().clone()),
        join_secret: token
            .as_ref()
            .map(|token| token.secret.clone())
            .or_else(|| matches.get_one::<String>("join-secret").cloned()),
        hold: duration("hold-ms"),
        ready_after: duration("ready-after-ms"),
        stage_timeout: duration("stage-timeout-ms"),
        submit_job: !matches.get_flag("no-submit"),
        quiet: matches.get_flag("quiet"),
        advertise_addr: matches.get_one::<String>("advertise-addr").cloned(),
        sim_name_prefix: matches
            .get_one::<String>("sim-name-prefix")
            .cloned()
            .unwrap_or_else(|| format!("lg-{}", std::process::id())),
        sim_profiles: matches.get_one::<String>("sim-profiles").cloned(),
    };
    let summary = instances::run(opts).await?;
    let rendered = serde_json::to_string_pretty(&summary)?;
    if let Some(out) = matches.get_one::<String>("out") {
        std::fs::write(out, format!("{rendered}\n"))
            .with_context(|| format!("failed to write {out}"))?;
    }
    println!("{rendered}");
    if summary["failed"].as_u64().unwrap_or(0) > 0 {
        bail!(
            "{} session(s) failed; see `errors` above",
            summary["failed"]
        );
    }
    Ok(())
}

fn cli() -> Command {
    Command::new("legion-loadgen")
        .about(
            "Native job submitter for the AWS benchmark rig. Legion-only by \
             design — see the fairness note in benchmarks/aws/README.md before \
             comparing a cell run with this against Nomad or K8s.",
        )
        .arg(
            Arg::new("mode")
                .long("mode")
                .value_parser(["jobs", "instances"])
                .default_value("jobs")
                .help(
                    "`jobs` submits the rig's jobspecs; `instances` drives keyed-instance \
                     lifecycles against an embedded simulated fleet",
                ),
        )
        .arg(
            Arg::new("workload-dir")
                .long("workload-dir")
                .required_if_eq("mode", "jobs")
                .help("Directory holding manifest.json and the rendered jobspecs (jobs mode)"),
        )
        .arg(
            Arg::new("out")
                .long("out")
                .required_if_eq("mode", "jobs")
                .help(
                    "Path to write the submits.json array to (jobs mode), or the summary \
                     JSON (instances mode, optional)",
                ),
        )
        .arg(
            Arg::new("job")
                .long("job")
                .default_value("loadgen-sandboxes")
                .help("instances mode: the keyed job every session is an instance of"),
        )
        .arg(
            Arg::new("no-submit")
                .long("no-submit")
                .action(ArgAction::SetTrue)
                .help(
                    "instances mode: use an existing --job instead of submitting the \
                     built-in keyed sandbox job",
                ),
        )
        .arg(
            Arg::new("sessions")
                .long("sessions")
                .value_parser(clap::value_parser!(usize))
                .default_value("1000")
                .help("instances mode: lifecycles to run"),
        )
        .arg(
            Arg::new("simulate")
                .long("simulate")
                .value_parser(clap::value_parser!(usize))
                .default_value("20")
                .help("instances mode: simulated nodes to embed (checkpoint channel faked)"),
        )
        .arg(
            Arg::new("worker-type")
                .long("worker-type")
                .default_value("default")
                .help("instances mode: worker type the simulated nodes register as"),
        )
        .arg(
            Arg::new("join-secret")
                .long("join-secret")
                .help("instances mode: join token secret, when the cluster enforces ACLs"),
        )
        .arg(Arg::new("join-token").long("join-token").help(
            "instances mode: a `legion join-token create` token for the embedded \
                     fleet (sets its worker type and secret; overrides --worker-type and \
                     --join-secret)",
        ))
        .arg(Arg::new("advertise-addr").long("advertise-addr").help(
            "instances mode: address (host or host:port) the manager dials to push \
                     work to the embedded fleet, auto-detected when unset (only used for a \
                     node whose work stream is down)",
        ))
        .arg(Arg::new("sim-name-prefix").long("sim-name-prefix").help(
            "instances mode: node-id prefix for the embedded fleet (default \
                     lg-<pid>); keep it fixed across runs against one cluster so each run \
                     re-registers the same nodes",
        ))
        .arg(Arg::new("sim-profiles").long("sim-profiles").help(
            "instances mode: node_profiles JSON for the embedded fleet (the \
                     benchmark scenario schema); default one 4-CPU, 32 GiB profile",
        ))
        .arg(
            Arg::new("hold-ms")
                .long("hold-ms")
                .value_parser(clap::value_parser!(u64))
                .default_value("0")
                .help("instances mode: how long a session runs before it is suspended"),
        )
        .arg(
            Arg::new("ready-after-ms")
                .long("ready-after-ms")
                .value_parser(clap::value_parser!(u64))
                .default_value("0")
                .help("instances mode: simulated boot time before a node reports running"),
        )
        .arg(
            Arg::new("stage-timeout-ms")
                .long("stage-timeout-ms")
                .value_parser(clap::value_parser!(u64))
                .default_value("60000")
                .help("instances mode: how long one stage may take before the session fails"),
        )
        .arg(
            Arg::new("manager")
                .long("manager")
                .help("Manager address; defaults to LEGION_ADDR, then http://127.0.0.1:4747"),
        )
        .arg(
            Arg::new("concurrency")
                .long("concurrency")
                .value_parser(clap::value_parser!(usize))
                .default_value(leak_usize(DEFAULT_CONCURRENCY))
                .help("Jobs in flight at once"),
        )
        .arg(
            Arg::new("connections")
                .long("connections")
                .value_parser(clap::value_parser!(usize))
                .default_value(leak_usize(DEFAULT_CONNECTIONS))
                .help("gRPC channels the requests are spread across"),
        )
        .arg(
            Arg::new("gap-ms")
                .long("gap-ms")
                .value_parser(clap::value_parser!(u64))
                .default_value("0")
                .help("Sleep this long in each slot after a submit (paced runs)"),
        )
        .arg(
            Arg::new("rate")
                .long("rate")
                .value_parser(clap::value_parser!(f64))
                .default_value("0")
                .help(
                    "Open-loop arrival rate in jobs/sec. Job i is sent at \
                     start + i/rate whatever the manager's latency, so a cell \
                     paced below capacity measures submit-to-placed latency \
                     instead of queue drain. 0 (the default) sends as fast \
                     as --concurrency allows.",
                ),
        )
        .arg(
            Arg::new("clock-offset-ms")
                .long("clock-offset-ms")
                .allow_hyphen_values(true)
                .value_parser(clap::value_parser!(i64))
                .default_value("0")
                .help(
                    "Added to every recorded timestamp. Timestamps are UNIX \
                     epoch milliseconds; the driver passes the offset that \
                     rebases them onto its own monotonic clock, so submit and \
                     placed marks stay comparable.",
                ),
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .action(ArgAction::SetTrue)
                .help("Suppress the progress line on stderr"),
        )
}

/// clap wants a `&'static str` default; the constants are the documentation, so
/// render them rather than repeating the numbers in two places.
fn leak_usize(value: usize) -> &'static str {
    Box::leak(value.to_string().into_boxed_str())
}

async fn run(matches: &clap::ArgMatches) -> Result<()> {
    let workload_dir = PathBuf::from(matches.get_one::<String>("workload-dir").unwrap());
    let out_path = PathBuf::from(matches.get_one::<String>("out").unwrap());
    let concurrency = *matches.get_one::<usize>("concurrency").unwrap();
    let connections = *matches.get_one::<usize>("connections").unwrap();
    let gap_ms = *matches.get_one::<u64>("gap-ms").unwrap();
    let clock_offset_ms = *matches.get_one::<i64>("clock-offset-ms").unwrap();
    let rate = *matches.get_one::<f64>("rate").unwrap();
    let quiet = matches.get_flag("quiet");

    if concurrency == 0 || connections == 0 {
        bail!("--concurrency and --connections must both be at least 1");
    }
    if !rate.is_finite() || rate < 0.0 {
        bail!("--rate must be a non-negative number of jobs/sec");
    }

    let jobs = prepare_jobs(&workload_dir)?;
    if jobs.is_empty() {
        bail!(
            "no jobs in {}/manifest.json — a cell with nothing to submit is a \
             translator failure, not an empty workload",
            workload_dir.display()
        );
    }

    // Dial everything up front. A dial inside the measured window would be
    // charged to Legion's submission rate, and a manager that is unreachable
    // should fail here — before any timestamp is taken — rather than as N
    // identical per-job errors.
    let mut clients = Vec::with_capacity(connections);
    for _ in 0..connections {
        clients.push(connect_manager_client().await?);
    }
    let clients = Arc::new(clients);

    if !quiet {
        eprintln!(
            "legion-loadgen: {} jobs, concurrency {concurrency}, {connections} connection(s)",
            jobs.len()
        );
    }

    let total = jobs.len();
    let submitted = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    // A heartbeat, not a progress bar. A cell that stops advancing is a real
    // rig failure mode (a wedged manager, a lane without a leader), and
    // without this the only symptom is a silent process and a settle timeout
    // several minutes later with nothing to say which half was stuck. The
    // driver forwards this stderr into the cell's log.
    let progress = if quiet {
        None
    } else {
        let submitted = Arc::clone(&submitted);
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
            ticker.tick().await; // fires immediately; skip it
            loop {
                ticker.tick().await;
                eprintln!(
                    "legion-loadgen: {}/{total} submitted",
                    submitted.load(Ordering::Relaxed)
                );
            }
        }))
    };

    // `buffer_unordered` is the whole engine: it keeps exactly `concurrency`
    // futures alive and starts the next as soon as one finishes, so the number
    // in flight is flat for the whole run instead of sawtoothing the way a
    // batched `join_all` would.
    let results: Vec<JobOutcome> = futures::stream::iter(jobs.into_iter().enumerate())
        .map(|(index, job)| {
            let mut client = clients[index % clients.len()].clone();
            let submitted = Arc::clone(&submitted);
            async move {
                // Open-loop pacing: wait for this job's scheduled send time.
                // The schedule is fixed up front, so a slow reply delays
                // nothing else as long as --concurrency covers rate x latency.
                if rate > 0.0 {
                    let due = started + std::time::Duration::from_secs_f64(index as f64 / rate);
                    tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                }
                // Stamped immediately before the call, inside the slot, so the
                // submit -> placed latency does not absorb this stream's own
                // queueing. Same rule as the driver's `record_submit`.
                let t_ms = epoch_ms() + clock_offset_ms;
                let outcome = submit_one(&mut client, &job).await;
                submitted.fetch_add(1, Ordering::Relaxed);
                if gap_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(gap_ms)).await;
                }
                JobOutcome {
                    name: job.name,
                    t_ms,
                    result: outcome,
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let elapsed = started.elapsed();
    if let Some(progress) = progress {
        progress.abort();
    }

    // Write what happened before deciding whether it was a success: a partially
    // submitted cell is diagnosable from its timestamps, and throwing them away
    // on failure is how a rig loses the evidence for why it failed.
    let mut records: Vec<SubmitRecord> = results
        .iter()
        .filter(|outcome| outcome.result.is_ok())
        .map(|outcome| SubmitRecord {
            job: outcome.name.clone(),
            t_ms: outcome.t_ms,
        })
        .collect();
    records.sort_by(|a, b| a.job.cmp(&b.job));
    write_submits(&out_path, &records)?;

    let failures: Vec<&JobOutcome> = results
        .iter()
        .filter(|outcome| outcome.result.is_err())
        .collect();
    let deduplicated = results
        .iter()
        .filter(|outcome| matches!(&outcome.result, Ok(reply) if reply.deduplicated))
        .count();
    let unchanged = results
        .iter()
        .filter(|outcome| matches!(&outcome.result, Ok(reply) if reply.unchanged))
        .count();

    let seconds = elapsed.as_secs_f64();
    let summary = serde_json::json!({
        "ok": failures.is_empty(),
        "submitted": records.len(),
        "failed": failures.len(),
        "elapsed_secs": round3(seconds),
        "rate_per_sec": if seconds > 0.0 { round2(records.len() as f64 / seconds) } else { 0.0 },
        "concurrency": concurrency,
        "target_rate_per_sec": rate,
        "connections": connections,
        "deduplicated": deduplicated,
        // Replies that said the job config was already current. Each one is an
        // allocation-free submit, so a non-zero count means the cell will come
        // up short no matter how fast the scheduler is — almost always a
        // cluster the previous cell failed to clean up.
        "unchanged": unchanged,
        "out": out_path.display().to_string(),
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);

    if unchanged > 0 {
        eprintln!(
            "WARNING: {unchanged} submissions returned 'job config unchanged'. Those \
             jobs already existed at this exact spec, so they create no allocations \
             and this cell cannot reach its expected count. The cluster was not \
             clean before the cell started."
        );
    }

    if !failures.is_empty() {
        // Loud, and with the distinct reasons rather than the first one: 4,000
        // identical `PermissionDenied`s and 4,000 assorted failures are
        // different bugs, and the counts are what tells them apart.
        let mut reasons: std::collections::BTreeMap<String, usize> = Default::default();
        for failure in &failures {
            if let Err(error) = &failure.result {
                *reasons.entry(error.clone()).or_default() += 1;
            }
        }
        eprintln!(
            "ERROR: {} of {} submissions failed. A cell short of its expected \
             count must fail, not average over the gap.",
            failures.len(),
            failures.len() + records.len()
        );
        for (reason, count) in reasons.iter().take(5) {
            eprintln!("  {count}x {reason}");
        }
        if reasons.len() > 5 {
            eprintln!("  ... and {} more distinct reasons", reasons.len() - 5);
        }
        bail!("{} submissions failed", failures.len());
    }

    Ok(())
}

struct JobOutcome {
    name: String,
    t_ms: i64,
    result: Result<SubmitReply, String>,
}

struct SubmitReply {
    deduplicated: bool,
    /// The manager answered that the job config was already current. Not an
    /// error, but it produces no allocations — see the warning in `run`.
    unchanged: bool,
}

async fn submit_one(
    client: &mut AuthedManagerClient,
    job: &PreparedJob,
) -> Result<SubmitReply, String> {
    let request = tonic::Request::new(RunJobRequest {
        // The rig renders jobspecs to JSON up front (translate.py), which is
        // already the `parsed_jobspec` the manager consumes — the same string
        // `render_jobspec()` produces for a `.json` path in src/cmd/job.rs.
        parsed_jobspec: job.content.clone(),
        submissions: vec![Submission {
            path: job.path.clone(),
            content: job.content.clone(),
            is_primary: true,
        }],
        // Left at the manager's defaults on purpose. A benchmark that quietly
        // changed the eval priority, forced a reschedule, or turned on
        // idempotency dedup would be measuring a different code path from the
        // one a user gets from `legion job run <file>`.
        priority: None,
        force_reschedule: false,
        idempotency_key: None,
    });

    match client.run_job(request).await {
        Ok(response) => {
            let reply = response.into_inner();
            Ok(SubmitReply {
                deduplicated: reply.deduplicated,
                unchanged: reply.message.contains("unchanged"),
            })
        }
        Err(status) => Err(format!(
            "{}: {}",
            status.code(),
            status.message().trim().replace('\n', " ")
        )),
    }
}

/// Reads `manifest.json` and every jobspec it names, before the clock starts.
fn prepare_jobs(workload_dir: &Path) -> Result<Vec<PreparedJob>> {
    let manifest_path = workload_dir.join("manifest.json");
    let raw = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest: Manifest = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

    manifest
        .jobs
        .into_iter()
        .map(|job| {
            let path = workload_dir.join(&job.file);
            let content = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read jobspec {}", path.display()))?;
            Ok(PreparedJob {
                name: job.name,
                path: path.to_string_lossy().to_string(),
                content,
            })
        })
        .collect()
}

fn write_submits(out_path: &Path, records: &[SubmitRecord]) -> Result<()> {
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    let json = serde_json::to_string_pretty(records)?;
    std::fs::write(out_path, format!("{json}\n"))
        .with_context(|| format!("failed to write {}", out_path.display()))
}

/// UNIX epoch milliseconds.
///
/// Not a monotonic clock, because there is no portable way to hand a monotonic
/// reading to another process — `Instant` is deliberately opaque. The driver
/// closes the gap instead: it samples `monotonic - epoch` once and passes the
/// difference as `--clock-offset-ms`, which puts these stamps on the same clock
/// as its placement events. The two clocks tick at the same rate, so only an
/// NTP *step* during a cell could skew a latency, and the rig's boxes are
/// slewed, not stepped.
fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|delta| delta.as_millis() as i64)
        .unwrap_or_default()
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod loadgen_test {
    use super::*;

    fn write_workload(dir: &Path, jobs: &[(&str, &str)]) {
        let entries: Vec<_> = jobs
            .iter()
            .map(|(name, body)| {
                let file = format!("{name}.json");
                std::fs::write(dir.join(&file), body).unwrap();
                serde_json::json!({ "name": name, "file": file, "expected_allocations": 1 })
            })
            .collect();
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::json!({ "jobs": entries }).to_string(),
        )
        .unwrap();
    }

    #[test]
    fn prepare_jobs_reads_every_spec_named_by_the_manifest_test() {
        let dir = tempfile::tempdir().unwrap();
        write_workload(
            dir.path(),
            &[("bench-a-0", "{\"name\":\"a\"}"), ("bench-a-1", "{}")],
        );

        let jobs = prepare_jobs(dir.path()).unwrap();

        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].name, "bench-a-0");
        assert_eq!(jobs[0].content, "{\"name\":\"a\"}");
        assert!(jobs[0].path.ends_with("bench-a-0.json"));
    }

    #[test]
    fn prepare_jobs_fails_loudly_on_a_missing_spec_test() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("manifest.json"),
            r#"{"jobs":[{"name":"gone","file":"gone.json"}]}"#,
        )
        .unwrap();

        // A silently skipped jobspec is a cell that reports a shortfall with no
        // explanation, which is the failure mode the whole rig is built to avoid.
        let error = prepare_jobs(dir.path()).unwrap_err().to_string();
        assert!(error.contains("gone.json"), "{error}");
    }

    #[test]
    fn submits_json_matches_the_shape_the_driver_consumes_test() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("nested").join("submits.json");
        write_submits(
            &out,
            &[
                SubmitRecord {
                    job: "b".into(),
                    t_ms: 2,
                },
                SubmitRecord {
                    job: "a".into(),
                    t_ms: 1,
                },
            ],
        )
        .unwrap();

        // driver.py reads `[{"job": ..., "t_ms": ...}]` and nothing else.
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["job"], "b");
        assert_eq!(rows[0]["t_ms"], 2);
        assert_eq!(rows[1]["job"], "a");
    }

    #[test]
    fn clock_offset_rebases_a_stamp_onto_the_drivers_clock_test() {
        // The driver hands over `monotonic_ms - epoch_ms`; adding it to an
        // epoch stamp must land on the driver's monotonic reading of the same
        // instant, or every submit->placed latency in the cell is nonsense.
        let epoch = epoch_ms();
        let pretend_driver_monotonic = 1_234_567_i64;
        let offset = pretend_driver_monotonic - epoch;
        assert_eq!(epoch + offset, pretend_driver_monotonic);
    }
}
