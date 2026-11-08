//! Watches and blocking reads on the CLI (revisions-and-watches plan;
//! docs/watches-and-blocking-queries.md).
//!
//! - `legion job watch [NAME]` and `legion kv watch [PREFIX]` tail the
//!   `Manager.Watch` stream: one line per changed object, or with `--json`
//!   one `{ "ok": true, "data": <WatchEvent> }` envelope per line (NDJSON,
//!   like `legion events follow`). `--from R` resumes after a revision; a
//!   revision older than the manager's journal fails with the typed
//!   `ResyncRequired` error, whose hint says to list again and watch from
//!   the list's revision. `--timeout` stops after that many seconds, for
//!   scripts and agents.
//! - `--index R`, `--wait SECONDS`, and `--consistent` on `legion job info`
//!   and `legion kv get` set the blocking-read headers.

use crate::cmd::helpers::{connect_manager_client, json_flag, namespace_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{WatchChange, WatchEvent, WatchKind, WatchRequest};

/// `--index`, `--wait`, and `--consistent`: the blocking-read flags.
pub fn read_flags() -> [Arg; 3] {
    [
        Arg::new("index").long("index").value_name("REVISION").help(
            "Block until this changes after REVISION (a `revision` from an earlier \
                 --json reply), then print it",
        ),
        Arg::new("wait")
            .long("wait")
            .value_name("SECONDS")
            .requires("index")
            .value_parser(clap::value_parser!(u32))
            .help("How long --index blocks, in seconds (default and maximum 300)"),
        Arg::new("consistent")
            .long("consistent")
            .action(ArgAction::SetTrue)
            .help("Read through the leader, including every write committed before the read"),
    ]
}

/// Set the blocking-read headers `read_flags` asked for on `request`.
pub fn apply_read_flags<T>(request: &mut tonic::Request<T>, matches: &ArgMatches) -> R<()> {
    let metadata = request.metadata_mut();
    if let Some(index) = matches
        .try_get_one::<String>("index")
        .ok()
        .flatten()
        .filter(|index| !index.trim().is_empty())
    {
        metadata.insert(
            crate::manager::server::MIN_REVISION_HEADER,
            index
                .trim()
                .parse()
                .map_err(|_| anyhow!("--index {index:?} is not a revision"))?,
        );
    }
    if let Some(wait) = matches.try_get_one::<u32>("wait").ok().flatten() {
        metadata.insert(
            crate::manager::server::WAIT_HEADER,
            wait.to_string().parse().expect("digits are ascii"),
        );
    }
    if matches
        .try_get_one::<bool>("consistent")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
    {
        metadata.insert(
            crate::manager::server::CONSISTENCY_HEADER,
            "linearizable".parse().expect("ascii"),
        );
    }
    Ok(())
}

fn watch_args(command: Command) -> Command {
    command
        .arg(
            Arg::new("from")
                .long("from")
                .value_name("REVISION")
                .help("Resume after this revision (from an earlier reply or watch line)"),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_name("SECONDS")
                .value_parser(clap::value_parser!(u64))
                .help("Stop after this many seconds (default: until interrupted)"),
        )
        .arg(json_flag())
}

/// `legion job watch [NAME]`.
pub fn job_watch_cmd() -> Command {
    watch_args(
        Command::new("watch")
            .about(
                "Print each change to a job (or every job) as it commits: status, versions, \
                 allocations",
            )
            .arg(
                Arg::new("name")
                    .value_name("NAME")
                    .help("The job to watch (default: every job you can read)"),
            )
            .arg(
                Arg::new("allocations")
                    .long("allocations")
                    .action(ArgAction::SetTrue)
                    .help("Watch the job's allocations instead of the job"),
            )
            .arg(
                Arg::new("node")
                    .long("node")
                    .value_name("NODE")
                    .requires("allocations")
                    .help("With --allocations: only allocations on this node"),
            )
            .arg(namespace_flag()),
    )
}

/// `legion kv watch [PREFIX]`.
pub fn kv_watch_cmd() -> Command {
    watch_args(
        Command::new("watch")
            .about("Print each write or delete of a key under a prefix as it commits")
            .arg(
                Arg::new("prefix")
                    .value_name("PREFIX")
                    .help("Key prefix to watch (default: every key you can read)"),
            ),
    )
}

/// Run `legion job watch`.
pub async fn job_watch(matches: &ArgMatches) -> R<()> {
    let allocations = matches.get_flag("allocations");
    let job = match matches.get_one::<String>("name") {
        Some(name) => crate::cmd::helpers::job_ref(matches, name)?,
        None => String::new(),
    };
    let request = WatchRequest {
        kind: if allocations {
            WatchKind::Allocation
        } else {
            WatchKind::Job
        } as i32,
        namespace: if job.is_empty() {
            matches
                .get_one::<String>("namespace")
                .cloned()
                .unwrap_or_default()
        } else {
            String::new()
        },
        job,
        node_id: matches
            .get_one::<String>("node")
            .cloned()
            .unwrap_or_default(),
        prefix: String::new(),
        from_revision: matches
            .get_one::<String>("from")
            .cloned()
            .unwrap_or_default(),
    };
    run_watch(matches, request).await
}

/// Run `legion kv watch`.
pub async fn kv_watch(matches: &ArgMatches) -> R<()> {
    let request = WatchRequest {
        kind: WatchKind::Kv as i32,
        prefix: matches
            .get_one::<String>("prefix")
            .cloned()
            .unwrap_or_default(),
        from_revision: matches
            .get_one::<String>("from")
            .cloned()
            .unwrap_or_default(),
        ..Default::default()
    };
    run_watch(matches, request).await
}

async fn run_watch(matches: &ArgMatches, request: WatchRequest) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let deadline = matches
        .get_one::<u64>("timeout")
        .map(|seconds| tokio::time::Instant::now() + std::time::Duration::from_secs(*seconds));
    let mut client = connect_manager_client().await?;
    let mut stream = output::resolve(
        format,
        "failed to open the watch",
        client.watch(tonic::Request::new(request)).await,
    )?
    .into_inner();
    if !format.is_json() {
        eprintln!("Watching (Ctrl-C to stop)...");
    }
    loop {
        let next = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, stream.message()).await {
                Ok(next) => next,
                Err(_) => return Ok(()),
            },
            None => stream.message().await,
        };
        match next {
            Ok(Some(event)) => {
                if event.resync_required {
                    return Err(output::fail_local(format, resync_error(&event)));
                }
                if format.is_json() {
                    println!("{}", serde_json::json!({ "ok": true, "data": event }));
                } else {
                    print_event(&event);
                }
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
            Ok(None) => return Ok(()),
            Err(status) => {
                return output::resolve(
                    format,
                    "the watch failed",
                    Err::<(), tonic::Status>(status),
                )
                .map(|_| ());
            }
        }
    }
}

/// Job statuses past which a just-submitted job needs nothing more from the
/// cluster to be "up": something is live, or every task finished or waits
/// for its schedule.
const SETTLED_OK: [&str; 5] = ["running", "complete", "scheduled", "idle", "suspended"];
/// Job statuses that end a wait as a failure.
const SETTLED_FAILED: [&str; 2] = ["failed", "stopped"];

/// Wait, with blocking reads, until `job` settles (revisions-and-watches
/// plan §9): its rollout (if one is in flight) finishes, and its status reads
/// `running`, `complete`, `scheduled`, `idle`, or `suspended` (success) or
/// `failed`/`stopped` (failure). Gives up after `timeout` with what it last
/// read. Never prints: the MCP server calls it.
pub async fn wait_for_job_settled(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job: &str,
    timeout: std::time::Duration,
) -> Result<serde_json::Value, String> {
    let started = std::time::Instant::now();
    // An update moves through a rollout first; the existing rollout wait
    // knows when it is done (a fresh job has none and returns at once).
    let rollout = crate::cmd::job::wait_for_convergence(client, job, timeout, OutputFormat::Human)
        .await
        .map_err(|err| err.to_string())?;
    let mut revision = String::new();
    loop {
        let remaining = timeout.saturating_sub(started.elapsed());
        let mut request = tonic::Request::new(crate::structs::GetJobInfoRequest {
            name: job.to_string(),
        });
        if !revision.is_empty() {
            let metadata = request.metadata_mut();
            metadata.insert(
                crate::manager::server::MIN_REVISION_HEADER,
                revision.parse().map_err(|_| "bad revision".to_string())?,
            );
            metadata.insert(
                crate::manager::server::WAIT_HEADER,
                remaining
                    .as_secs()
                    .max(1)
                    .to_string()
                    .parse()
                    .expect("digits"),
            );
        }
        let reply = client
            .get_job_info(request)
            .await
            .map_err(|status| {
                serde_json::to_string(&CliError::from_status("failed to read the job", &status))
                    .unwrap_or_else(|_| status.message().to_string())
            })?
            .into_inner();
        let job_info = reply.job.unwrap_or_default();
        let status = job_info.status.clone();
        let settled_ok = SETTLED_OK.contains(&status.as_str())
            && (rollout.converged || rollout.rollout_id.is_none());
        let failed = SETTLED_FAILED.contains(&status.as_str());
        let out_of_time = started.elapsed() >= timeout;
        if settled_ok || failed || out_of_time {
            return Ok(serde_json::json!({
                "settled": settled_ok || failed,
                "success": settled_ok && rollout.success,
                "status": status,
                "version": job_info.version,
                "revision": reply.revision,
                "rollout": rollout,
                "waitedSeconds": started.elapsed().as_secs(),
            }));
        }
        if reply.revision.is_empty() {
            // A manager that reports no revision cannot block: poll.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        revision = reply.revision;
    }
}

/// The typed error for a resume older than the manager's journal.
pub fn resync_error(event: &WatchEvent) -> CliError {
    CliError::local(
        "ResyncRequired",
        format!(
            "the manager cannot resume this watch: its journal starts after the revision you \
             gave (the oldest it can serve is {})",
            event.oldest_revision
        ),
        Some(
            "list again (`legion job info`, `legion kv list`; the --json reply carries \
             `revision`) and watch from that revision with --from",
        ),
    )
}

fn print_event(event: &WatchEvent) {
    if event.progress {
        eprintln!("at revision {}", event.revision);
        return;
    }
    for change in &event.changes {
        println!("{}", change_line(change));
    }
}

/// One human-readable line per changed object.
pub fn change_line(change: &WatchChange) -> String {
    let verb = if change.deleted { "deleted" } else { "changed" };
    match WatchKind::try_from(change.kind).unwrap_or(WatchKind::Unspecified) {
        WatchKind::Job => match &change.job_info {
            Some(job) => format!(
                "{}  job {}  {verb}  status={} version={}",
                change.revision, change.key, job.status, job.version
            ),
            None => format!("{}  job {}  {verb}", change.revision, change.key),
        },
        WatchKind::Allocation => match &change.allocation {
            Some(allocation) => format!(
                "{}  allocation {}  {verb}  job={} task={} node={} status={}",
                change.revision,
                change.key,
                allocation.job_id,
                allocation.task_name,
                allocation.node_id,
                allocation.status
            ),
            None => format!(
                "{}  allocation {}  {verb}  job={} node={}",
                change.revision, change.key, change.job, change.node_id
            ),
        },
        WatchKind::Kv => match &change.kv {
            Some(entry) => format!(
                "{}  kv {}  {verb}  version={}",
                change.revision, change.key, entry.version
            ),
            None => format!("{}  kv {}  {verb}", change.revision, change.key),
        },
        WatchKind::Unspecified => format!("{}  {}  {verb}", change.revision, change.key),
    }
}

#[cfg(test)]
mod watch_cli_test {
    use super::*;

    #[test]
    fn read_flags_become_headers() {
        let command = Command::new("info").args(read_flags());
        let matches = command
            .try_get_matches_from(["info", "--index", "42", "--wait", "30", "--consistent"])
            .unwrap();
        let mut request = tonic::Request::new(());
        apply_read_flags(&mut request, &matches).unwrap();
        let header = |key: &str| {
            request
                .metadata()
                .get(key)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        assert_eq!(
            header(crate::manager::server::MIN_REVISION_HEADER).as_deref(),
            Some("42")
        );
        assert_eq!(
            header(crate::manager::server::WAIT_HEADER).as_deref(),
            Some("30")
        );
        assert_eq!(
            header(crate::manager::server::CONSISTENCY_HEADER).as_deref(),
            Some("linearizable")
        );
    }

    #[test]
    fn watch_commands_parse() {
        let matches = job_watch_cmd()
            .try_get_matches_from([
                "watch",
                "web",
                "--allocations",
                "--node",
                "n1",
                "--from",
                "7",
            ])
            .unwrap();
        assert!(matches.get_flag("allocations"));
        assert_eq!(
            matches.get_one::<String>("from").map(String::as_str),
            Some("7")
        );
        assert!(kv_watch_cmd()
            .try_get_matches_from(["watch", "app/", "--timeout", "5", "--json"])
            .is_ok());
    }
}
