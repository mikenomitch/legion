//! `legion gitops`: deploy jobs from git repositories with the bundled
//! gitops app (docs/gitops.md).
//!
//! ```console
//! $ legion gitops add web --url https://github.com/acme/deploy --path jobs --wait
//! $ legion gitops status -A
//! $ legion gitops status web
//! $ legion gitops sync web --wait
//! ```
//!
//! A Repo is an ordinary app object (`gitops/Repo`): `legion object
//! apply|get|list|delete` work on it too. These commands are the short way:
//! `add` writes one from flags, `status` shows the deployed commit and each
//! job, and `sync` asks gitops to look at the repository now (it sets the
//! `gitops/sync` label, which the app echoes once it has synced) and, with
//! `--wait`, waits until the commit is deployed and healthy.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::apps::gitops::{
    InventoryEntry, JobState, RepoSpec, RepoStatus, QUALIFIED_KIND, SYNC_LABEL,
};
use crate::cmd::helpers::{
    active_namespace, connect_manager_client, json_flag, list_namespace_flags,
    lists_many_namespaces, namespace_flag, namespace_selection, AuthedManagerClient,
};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{AppObject, ApplyObjectRequest, GetObjectRequest, ListObjectsRequest};
use serde::Serialize;

/// How long `sync` waits for gitops to pick a request up, and `--wait`
/// for the commit to deploy, by default.
const WAIT_DEFAULT: Duration = Duration::from_secs(300);

/// One Repo as the commands report it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepoReport {
    pub namespace: String,
    pub name: String,
    pub url: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub path: String,
    pub interval: String,
    pub suspend: bool,
    /// The manager's summary: `Ready`, `Pending` (a rollout under way, or
    /// not synced yet), `NotReady: <reason>`, `Rejected: <reason>`,
    /// `Stale` (gitops is not running), ...
    pub summary: String,
    /// The summary's reason and message.
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_request: Option<String>,
    /// The jobs it deployed.
    pub jobs: Vec<InventoryEntry>,
}

impl RepoReport {
    pub fn of(object: &AppObject) -> RepoReport {
        let spec: Option<RepoSpec> = serde_json::from_str(&object.effective_json).ok();
        let status = RepoStatus::of(object);
        RepoReport {
            namespace: object.namespace.clone(),
            name: object.name.clone(),
            url: spec.as_ref().map(|s| s.url.clone()).unwrap_or_default(),
            reference: spec
                .as_ref()
                .map(|s| s.reference.clone())
                .unwrap_or_default(),
            path: spec.as_ref().map(|s| s.path.clone()).unwrap_or_default(),
            interval: spec
                .as_ref()
                .map(|s| s.interval.clone())
                .unwrap_or_default(),
            suspend: spec.as_ref().is_some_and(|s| s.suspend),
            summary: object.summary.clone(),
            detail: object.summary_detail.clone(),
            commit: status.commit,
            latest: status.latest,
            subject: status.subject,
            synced_at: status.synced_at,
            sync_request: status.sync_request,
            jobs: status.jobs,
        }
    }

    fn short(&self) -> String {
        self.commit
            .as_deref()
            .map(crate::apps::gitops::git::short)
            .unwrap_or_else(|| "-".into())
    }

    /// `Ready (Synced: at 3f9a1c2: 2 jobs deployed)`.
    fn summary_line(&self) -> String {
        if self.detail.is_empty() {
            self.summary.clone()
        } else {
            format!("{} ({})", self.summary, self.detail)
        }
    }

    /// Whether the summary says the Repo is deployed and healthy (`Some(true)`),
    /// stuck (`Some(false)`), or still moving (`None`: a rollout, or not
    /// synced yet).
    fn settled(&self) -> Option<bool> {
        match self.summary.as_str() {
            "Ready" => Some(true),
            summary
                if summary.starts_with("NotReady")
                    || summary.starts_with("Rejected")
                    || summary == "Invalid"
                    || summary == "Orphaned" =>
            {
                Some(false)
            }
            _ => None,
        }
    }
}

pub fn config() -> Command {
    let wait = Arg::new("wait")
        .long("wait")
        .action(ArgAction::SetTrue)
        .help("Wait until the commit is deployed and every job's rollout has finished");
    let timeout = Arg::new("timeout")
        .long("timeout")
        .value_name("DURATION")
        .help("How long to wait (default 5m)");
    Command::new("gitops")
        .about("Deploy jobs from git repositories: the bundled gitops app (docs/gitops.md)")
        .long_about(
            "A Repo points gitops at a git repository: every .ncl and .json jobspec in its \
             path is deployed into the Repo's namespace, and kept deployed as the repository \
             changes. Repos are gitops/Repo objects, so `legion object` works on them too.",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("add")
                .about("Create a Repo from flags, and (with --wait) watch its first deploy")
                .arg(
                    Arg::new("name")
                        .required(true)
                        .value_name("NAME")
                        .help("The Repo's name (a DNS label)"),
                )
                .arg(
                    Arg::new("url")
                        .long("url")
                        .required(true)
                        .value_name("URL")
                        .help("The repository: an https:// URL"),
                )
                .arg(
                    Arg::new("ref")
                        .long("ref")
                        .value_name("REF")
                        .help("A branch, a tag, or a full commit SHA (default main)"),
                )
                .arg(
                    Arg::new("path")
                        .long("path")
                        .value_name("DIR")
                        .help("The directory of jobspecs in the repository (default: its root)"),
                )
                .arg(
                    Arg::new("interval")
                        .long("interval")
                        .value_name("DURATION")
                        .help("How often to look for a new commit (default 1m, at least 10s)"),
                )
                .arg(
                    Arg::new("jobs")
                        .long("jobs")
                        .value_name("JOB,...")
                        .value_delimiter(',')
                        .help("Only these jobs may be deployed (writing the Repo then needs job:write on them alone)"),
                )
                .arg(
                    Arg::new("host-level")
                        .long("host-level")
                        .action(ArgAction::SetTrue)
                        .help("Its jobs include host-level tasks, such as exec (needs job:write with host = true)"),
                )
                .arg(
                    Arg::new("no-prune")
                        .long("no-prune")
                        .action(ArgAction::SetTrue)
                        .help("Leave a job running when its file leaves the repository, or the Repo is deleted"),
                )
                .arg(
                    Arg::new("credentials-kv")
                        .long("credentials-kv")
                        .value_name("KEY")
                        .help("The KV key holding an HTTPS token for a private repository"),
                )
                .arg(
                    Arg::new("credentials-username")
                        .long("credentials-username")
                        .value_name("USER")
                        .requires("credentials-kv")
                        .help("The username sent with the token (default x-access-token)"),
                )
                .arg(
                    Arg::new("kv-key")
                        .long("kv-key")
                        .value_name("KEY")
                        .action(ArgAction::Append)
                        .help("A KV key the repository's jobs read (repeatable)"),
                )
                .arg(namespace_flag())
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Check the Repo (schema, rules, what writing it requires) and write nothing"),
                )
                .arg(wait.clone())
                .arg(timeout.clone().requires("wait"))
                .arg(crate::cmd::job::idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("status")
                .about("Show Repos, or one Repo: its deployed commit and each job's state")
                .arg(
                    Arg::new("name")
                        .value_name("NAME")
                        .help("One Repo, by name (or namespace/name)"),
                )
                .args(list_namespace_flags())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("sync")
                .about("Look at a Repo's repository now and deploy what changed")
                .long_about(
                    "Sets the Repo's gitops/sync label (an apply: you need what writing the Repo \
                     needs) and waits until gitops has synced it. With --wait, waits on until \
                     the commit is deployed and every rollout has finished, and fails if one \
                     did not: the step a CI pipeline runs after pushing a deploy commit.",
                )
                .arg(
                    Arg::new("name")
                        .required(true)
                        .value_name("NAME")
                        .help("The Repo's name (or namespace/name)"),
                )
                .arg(namespace_flag())
                .arg(wait)
                .arg(timeout)
                .arg(json_flag()),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("add", sub)) => add(sub).await,
        Some(("status", sub)) => status(sub).await,
        Some(("sync", sub)) => sync(sub).await,
        _ => unreachable!("subcommand_required"),
    }
}

fn opt(matches: &ArgMatches, key: &str) -> Option<String> {
    matches.try_get_one::<String>(key).ok().flatten().cloned()
}

fn timeout_of(format: OutputFormat, matches: &ArgMatches) -> R<Duration> {
    match opt(matches, "timeout") {
        None => Ok(WAIT_DEFAULT),
        Some(text) => humantime::parse_duration(text.trim()).map_err(|err| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("--timeout {text:?}: {err}"),
                    Some("write it like 5m"),
                ),
            )
        }),
    }
}

/// `namespace/name` or `name` with `-n`.
fn split_name(matches: &ArgMatches, typed: &str) -> (String, String) {
    match typed.split_once('/') {
        Some((namespace, name)) => (namespace.to_string(), name.to_string()),
        None => (
            active_namespace(matches).unwrap_or_default(),
            typed.to_string(),
        ),
    }
}

/// Build a Repo object from `add`'s flags.
fn repo_object(matches: &ArgMatches) -> Value {
    let mut spec = Map::new();
    spec.insert("url".into(), json!(opt(matches, "url").unwrap_or_default()));
    for (flag, field) in [("ref", "ref"), ("path", "path"), ("interval", "interval")] {
        if let Some(value) = opt(matches, flag) {
            spec.insert(field.into(), json!(value));
        }
    }
    if let Some(jobs) = matches.get_many::<String>("jobs") {
        let jobs: Vec<&String> = jobs.filter(|job| !job.trim().is_empty()).collect();
        if !jobs.is_empty() {
            spec.insert("jobs".into(), json!(jobs));
        }
    }
    if matches.get_flag("host-level") {
        spec.insert("hostLevel".into(), json!(true));
    }
    if matches.get_flag("no-prune") {
        spec.insert("prune".into(), json!(false));
    }
    if let Some(kv) = opt(matches, "credentials-kv") {
        let mut credentials = json!({ "kv": kv });
        if let Some(username) = opt(matches, "credentials-username") {
            credentials["username"] = json!(username);
        }
        spec.insert("credentials".into(), credentials);
    }
    if let Some(keys) = matches.get_many::<String>("kv-key") {
        spec.insert("kvKeys".into(), json!(keys.collect::<Vec<_>>()));
    }
    let typed = opt(matches, "name").unwrap_or_default();
    let (namespace, name) = split_name(matches, &typed);
    let mut object = json!({ "kind": QUALIFIED_KIND, "name": name, "spec": spec });
    if !namespace.is_empty() {
        object["namespace"] = json!(namespace);
    }
    object
}

/// `legion gitops add`.
async fn add(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let object = repo_object(matches);
    let dry_run = matches.get_flag("dry-run");
    let wait = matches.get_flag("wait");
    let timeout = timeout_of(format, matches)?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to add the Repo",
        client
            .apply_object(ApplyObjectRequest {
                object_json: object.to_string(),
                dry_run,
                // Create only: `legion object apply` changes an existing one.
                expected_version: Some(0),
                idempotency_key: opt(matches, "idempotency-key"),
                ..Default::default()
            })
            .await,
    )?
    .into_inner();
    if !format.is_json() {
        println!("{}", reply.message);
        for line in &reply.diff {
            println!("  {line}");
        }
        for warning in &reply.warnings {
            eprintln!("warning: {warning}");
        }
    }
    let stored = reply.object.clone();
    let mut report = stored.as_ref().map(RepoReport::of);
    if wait && !dry_run {
        if let Some(object) = &stored {
            let deadline = Instant::now() + timeout;
            let settled = wait_settled(
                &mut client,
                &object.namespace,
                &object.name,
                deadline,
                format,
            )
            .await;
            match settled {
                Ok(done) => report = Some(done),
                Err(err) => {
                    return Err(output::fail_local(
                        format,
                        CliError::local(
                            "FailedPrecondition",
                            format!("the Repo was created, and {err}"),
                            Some("legion gitops status shows each job"),
                        ),
                    ))
                }
            }
        }
    }
    if format.is_json() {
        output::print_success(&json!({
            "created": reply.created,
            "dryRun": dry_run,
            "message": reply.message,
            "warnings": reply.warnings,
            "repo": report,
        }));
    } else if wait && !dry_run {
        if let Some(report) = &report {
            println!("{}: {}", report.name, report.summary_line());
            print_jobs(report);
        }
    } else if !dry_run {
        println!(
            "gitops deploys it within moments; `legion gitops status {}` shows how it went",
            object["name"].as_str().unwrap_or_default()
        );
    }
    Ok(())
}

/// Read one Repo.
pub async fn get_repo(
    client: &mut AuthedManagerClient,
    namespace: &str,
    name: &str,
) -> Result<AppObject, tonic::Status> {
    client
        .get_object(GetObjectRequest {
            kind: QUALIFIED_KIND.into(),
            name: name.into(),
            namespace: namespace.into(),
        })
        .await?
        .into_inner()
        .object
        .ok_or_else(|| tonic::Status::not_found(format!("Repo {name} not found")))
}

/// Every Repo in `namespaces` (names or globs; empty for the default).
pub async fn list_repos(
    client: &mut AuthedManagerClient,
    namespaces: Vec<String>,
) -> Result<Vec<RepoReport>, tonic::Status> {
    Ok(client
        .list_objects(ListObjectsRequest {
            kind: QUALIFIED_KIND.into(),
            namespaces,
            ..Default::default()
        })
        .await?
        .into_inner()
        .objects
        .iter()
        .map(RepoReport::of)
        .collect())
}

/// `legion gitops status`.
async fn status(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    if let Some(typed) = opt(matches, "name") {
        let (namespace, name) = split_name(matches, &typed);
        let object = output::resolve(
            format,
            "failed to get the Repo",
            get_repo(&mut client, &namespace, &name).await,
        )?;
        let report = RepoReport::of(&object);
        if format.is_json() {
            output::print_success(&report);
        } else {
            print_detail(&report);
        }
        return Ok(());
    }
    let repos = output::resolve(
        format,
        "failed to list Repos",
        list_repos(&mut client, namespace_selection(matches)).await,
    )?;
    if format.is_json() {
        output::print_success(&json!({ "repos": repos }));
        return Ok(());
    }
    if repos.is_empty() {
        println!(
            "no Repos; `legion gitops add <name> --url <repository>` deploys one (docs/gitops.md)"
        );
        return Ok(());
    }
    let many = lists_many_namespaces(matches);
    let mut builder = tabled::builder::Builder::default();
    let mut header = Vec::new();
    if many {
        header.push("NAMESPACE");
    }
    header.extend(["NAME", "STATUS", "REF", "COMMIT", "JOBS", "SYNCED"]);
    builder.push_record(header);
    for repo in &repos {
        let mut row = Vec::new();
        if many {
            row.push(repo.namespace.clone());
        }
        row.extend([
            repo.name.clone(),
            repo.summary.clone(),
            repo.reference.clone(),
            repo.short(),
            repo.jobs.len().to_string(),
            repo.synced_at
                .as_deref()
                .map(ago)
                .unwrap_or_else(|| "-".into()),
        ]);
        builder.push_record(row);
    }
    let mut table = builder.build();
    table.with(tabled::settings::Style::blank());
    println!("{table}");
    Ok(())
}

/// How long ago an RFC 3339 time was: `40s ago`, `4m ago`, `3h ago`.
fn ago(at: &str) -> String {
    let Ok(at) = chrono::DateTime::parse_from_rfc3339(at) else {
        return at.to_string();
    };
    let seconds = (chrono::Utc::now() - at.with_timezone(&chrono::Utc))
        .num_seconds()
        .max(0);
    match seconds {
        0..=119 => format!("{seconds}s ago"),
        120..=7199 => format!("{}m ago", seconds / 60),
        7200..=172_799 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

fn print_detail(report: &RepoReport) {
    println!(
        "{QUALIFIED_KIND} {}/{}: {}",
        report.namespace,
        report.name,
        report.summary_line()
    );
    println!(
        "  repository  {} (ref {}, path {}, every {}{})",
        report.url,
        report.reference,
        report.path,
        report.interval,
        if report.suspend { ", suspended" } else { "" }
    );
    match &report.commit {
        Some(_) => println!(
            "  commit      {}{}{}",
            report.short(),
            report
                .subject
                .as_deref()
                .map(|subject| format!(" {subject}"))
                .unwrap_or_default(),
            report
                .synced_at
                .as_deref()
                .map(|at| format!(", synced {}", ago(at)))
                .unwrap_or_default()
        ),
        None => println!("  commit      none deployed yet"),
    }
    if let Some(latest) = report
        .latest
        .as_deref()
        .filter(|latest| Some(*latest) != report.commit.as_deref())
    {
        println!(
            "  latest      {} is not deployed (see the status above)",
            crate::apps::gitops::git::short(latest)
        );
    }
    print_jobs(report);
}

fn print_jobs(report: &RepoReport) {
    if report.jobs.is_empty() {
        return;
    }
    let mut builder = tabled::builder::Builder::default();
    builder.push_record(["JOB", "FILE", "STATE", "DETAIL"]);
    for job in &report.jobs {
        builder.push_record([
            job.name.clone(),
            job.file.clone(),
            job.state.as_str().to_string(),
            job.detail.clone().unwrap_or_default(),
        ]);
    }
    let mut table = builder.build();
    table.with(tabled::settings::Style::blank());
    println!("{table}");
}

/// Ask gitops to sync a Repo now: set its `gitops/sync` label to a fresh
/// token (an apply, retried on a concurrent write). Returns the token,
/// which the app echoes as `syncRequest` once it has synced.
pub async fn request_sync(
    client: &mut AuthedManagerClient,
    namespace: &str,
    name: &str,
) -> Result<String, tonic::Status> {
    let token = chrono::Utc::now().timestamp_millis().to_string();
    let mut attempts = 0;
    loop {
        let object = get_repo(client, namespace, name).await?;
        let spec: Value = serde_json::from_str(&object.spec_json)
            .map_err(|err| tonic::Status::internal(format!("the Repo's spec: {err}")))?;
        if spec.get("suspend").and_then(Value::as_bool) == Some(true) {
            return Err(tonic::Status::failed_precondition(format!(
                "Repo {} is suspended (spec.suspend): gitops does not sync it; apply it with \
                 suspend = false first",
                object.id
            )));
        }
        let mut labels: BTreeMap<String, String> = object.labels.clone().into_iter().collect();
        labels.insert(SYNC_LABEL.into(), token.clone());
        let mut document = json!({
            "kind": QUALIFIED_KIND,
            "name": object.name,
            "spec": spec,
            "labels": labels,
        });
        if !object.namespace.is_empty() {
            document["namespace"] = json!(object.namespace);
        }
        match client
            .apply_object(ApplyObjectRequest {
                object_json: document.to_string(),
                expected_version: Some(object.spec_version),
                ..Default::default()
            })
            .await
        {
            Ok(_) => return Ok(token),
            Err(status) if status.code() == tonic::Code::Aborted && attempts < 3 => {
                attempts += 1;
            }
            Err(status) => return Err(status),
        }
    }
}

/// Wait until gitops has handled sync request `token`.
pub async fn wait_handled(
    client: &mut AuthedManagerClient,
    namespace: &str,
    name: &str,
    token: &str,
    deadline: Instant,
) -> Result<RepoReport, String> {
    loop {
        let object = get_repo(client, namespace, name)
            .await
            .map_err(|status| status.message().to_string())?;
        let report = RepoReport::of(&object);
        if report.sync_request.as_deref() == Some(token) {
            return Ok(report);
        }
        if Instant::now() >= deadline {
            let why = if report.summary == "Stale" {
                "gitops is not running (`legion app status gitops`)".to_string()
            } else {
                format!("it reads {}", report.summary_line())
            };
            return Err(format!(
                "gitops has not picked up the sync request for {}/{} yet: {why}",
                report.namespace, report.name
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until a Repo is deployed and healthy (`Ready`), or stuck
/// (`NotReady`, `Rejected`): a `Pending` Repo is still rolling out. Prints
/// progress to stderr for a person.
pub async fn wait_settled(
    client: &mut AuthedManagerClient,
    namespace: &str,
    name: &str,
    deadline: Instant,
    format: OutputFormat,
) -> Result<RepoReport, String> {
    let mut last = String::new();
    loop {
        let object = get_repo(client, namespace, name)
            .await
            .map_err(|status| status.message().to_string())?;
        let report = RepoReport::of(&object);
        let line = report.summary_line();
        // Progress for a person; the settled line is the caller's to print.
        if !format.is_json() && line != last && report.settled().is_none() {
            eprintln!("{}: {line}", report.name);
            last = line;
        }
        match report.settled() {
            Some(true) => return Ok(report),
            Some(false) => {
                return Err(format!(
                    "{}/{} is {}",
                    report.namespace,
                    report.name,
                    report.summary_line()
                ))
            }
            None => {}
        }
        if Instant::now() >= deadline {
            let moving: Vec<String> = report
                .jobs
                .iter()
                .filter(|job| job.state != JobState::Deployed)
                .map(|job| format!("{} {}", job.name, job.state.as_str()))
                .collect();
            return Err(format!(
                "{}/{} is still {} ({})",
                report.namespace,
                report.name,
                report.summary_line(),
                if moving.is_empty() {
                    "waiting for gitops".to_string()
                } else {
                    moving.join(", ")
                }
            ));
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
}

/// `legion gitops sync`.
async fn sync(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let typed = opt(matches, "name").unwrap_or_default();
    let (namespace, name) = split_name(matches, &typed);
    let timeout = timeout_of(format, matches)?;
    let deadline = Instant::now() + timeout;
    let mut client = connect_manager_client().await?;
    let token = output::resolve(
        format,
        "failed to request a sync",
        request_sync(&mut client, &namespace, &name).await,
    )?;
    if !format.is_json() {
        eprintln!("{name}: sync requested");
    }
    let handled = wait_handled(&mut client, &namespace, &name, &token, deadline)
        .await
        .map_err(|err| {
            output::fail_local(
                format,
                CliError::local("DeadlineExceeded", err, Some("--timeout waits longer")),
            )
        })?;
    let report = if matches.get_flag("wait") {
        wait_settled(&mut client, &namespace, &name, deadline, format)
            .await
            .map_err(|err| {
                output::fail_local(
                    format,
                    CliError::local(
                        "FailedPrecondition",
                        err,
                        Some("legion gitops status shows each job"),
                    ),
                )
            })?
    } else {
        handled
    };
    if format.is_json() {
        output::print_success(&report);
    } else {
        println!("{}: {}", report.name, report.summary_line());
        print_jobs(&report);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(args: &[&str]) -> ArgMatches {
        let mut argv = vec!["gitops"];
        argv.extend_from_slice(args);
        config().try_get_matches_from(argv).expect("parses")
    }

    #[test]
    fn add_builds_a_repo_from_flags_test() {
        let parsed = matches(&[
            "add",
            "team-a/web",
            "--url",
            "https://github.com/acme/deploy",
            "--path",
            "jobs",
            "--jobs",
            "web,worker",
            "--host-level",
            "--no-prune",
            "--credentials-kv",
            "secret/token",
            "--kv-key",
            "secret/db",
            "--kv-key",
            "secret/cache",
        ]);
        let (_, sub) = parsed.subcommand().expect("add");
        let object = repo_object(sub);
        assert_eq!(object["kind"], "gitops/Repo");
        assert_eq!(object["namespace"], "team-a");
        assert_eq!(object["name"], "web");
        assert_eq!(
            object["spec"],
            json!({
                "url": "https://github.com/acme/deploy",
                "path": "jobs",
                "jobs": ["web", "worker"],
                "hostLevel": true,
                "prune": false,
                "credentials": { "kv": "secret/token" },
                "kvKeys": ["secret/db", "secret/cache"],
            })
        );
        // The spec the flags build is one the app parses.
        serde_json::from_value::<RepoSpec>(object["spec"].clone()).expect("a Repo spec");
        let parsed = matches(&["add", "web", "--url", "https://x/y"]);
        let (_, sub) = parsed.subcommand().expect("add");
        assert_eq!(repo_object(sub)["spec"], json!({ "url": "https://x/y" }));
    }

    #[test]
    fn reports_say_when_a_repo_is_settled_test() {
        let object = AppObject {
            namespace: "team-a".into(),
            name: "web".into(),
            effective_json: json!({ "url": "https://x/y", "ref": "main", "path": ".",
                                    "interval": "1m", "prune": true, "suspend": false,
                                    "hostLevel": false })
            .to_string(),
            summary: "Pending".into(),
            summary_detail: "Progressing: at 3f9a1c2: rolling out web".into(),
            status: Some(crate::structs::ObjectStatus {
                fields_json: json!({ "commit": format!("3f9a1c2{}", "0".repeat(33)),
                                     "jobs": [{ "name": "web", "file": "web.ncl", "digest": "d",
                                                "state": "progressing" }] })
                .to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let report = RepoReport::of(&object);
        assert_eq!(report.settled(), None);
        assert_eq!(report.short(), "3f9a1c2");
        assert_eq!(report.jobs[0].state, JobState::Progressing);
        let ready = RepoReport {
            summary: "Ready".into(),
            ..report.clone()
        };
        assert_eq!(ready.settled(), Some(true));
        let failed = RepoReport {
            summary: "NotReady: RolloutFailed".into(),
            ..report
        };
        assert_eq!(failed.settled(), Some(false));
    }
}
