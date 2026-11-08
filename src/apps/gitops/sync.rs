//! One sync of one Repo (docs/gitops.md, "How a sync works"; gitops plan
//! §6.3).
//!
//! 1. **Resolve** the ref with `git ls-remote`.
//! 2. **Fetch and render** the commit, once per commit (the render is kept in
//!    memory): every file, or nothing.
//! 3. **Decide** per job, from the Repo's inventory (its status) and the
//!    job as it is: submit it when it is missing, its file changed, or
//!    someone else's version replaced it; leave a stopped job stopped; refuse
//!    a job another Repo deployed.
//! 4. **Preflight** every submit with `PlanJob`; a refusal deploys nothing.
//! 5. **Apply** with `RunJob`.
//! 6. **Prune** jobs that left git (stop, never destroy).
//! 7. **Health** from each job's rollout, or its status.
//!
//! Every Legion call goes through [`RepoApi`], which the app implements
//! with a client that acts for the Repo (`legion-for-object`), so the
//! manager holds each call to what the Repo's writer was checked for.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use tonic::Status;

use super::git::{self, Remote};
use super::render::{render_tree, RenderedJob, Renderer};
use super::{InventoryEntry, JobState, RepoSpec, RepoStatus, MAX_JOBS};
use crate::apps::objects::ObjectOutcome;
use crate::cmd::helpers::AuthedManagerClient;
use crate::prelude::*;
use crate::structs::namespace::job_id;
use crate::structs::{
    object_stamp_label, ActingFor, GetJobInfoRequest, GetKvRequest, ListJobSpecHistoryRequest,
    ListRolloutsRequest, PlanJobRequest, Rollout, RunJobRequest, StopJobRequest,
};

/// How soon a Repo whose jobs are rolling out is looked at again.
pub const PROGRESS_REQUEUE: Duration = Duration::from_secs(5);
/// The shortest interval a Repo may set.
pub const MIN_INTERVAL: Duration = Duration::from_secs(10);

/// A job as gitops sees it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobView {
    /// `Job.status`: `running`, `stopped`, `failed`, ...
    pub status: String,
    /// `GetJobInfoReply.managed_by.object`: the object its latest version
    /// was written for. Unset when a person (or anything not acting for an
    /// object) wrote it, and always with ACLs off.
    pub stamp: Option<String>,
    /// Its latest config version's id.
    pub version: Option<String>,
}

/// The Legion calls a sync makes. The app implements them with a client
/// that acts for the Repo; tests with whatever they need.
#[async_trait::async_trait]
pub trait RepoApi: Send + Sync {
    /// The job with canonical id `job`, or `None` when it does not exist.
    async fn job(&self, job: &str) -> Result<Option<JobView>, Status>;
    /// `PlanJob`: what a submit would say, without one.
    async fn plan(&self, spec: &Value, namespace: &str) -> Result<(), Status>;
    /// `RunJob`: the job config version the submit produced or matched
    /// (empty when it cannot be read back).
    async fn submit(&self, job: &RenderedJob, namespace: &str) -> Result<String, Status>;
    async fn stop(&self, job: &str) -> Result<(), Status>;
    /// The job's most recent rollout.
    async fn latest_rollout(&self, job: &str) -> Result<Option<Rollout>, Status>;
    /// A KV value, or `None` when the key does not exist.
    async fn read_kv(&self, key: &str) -> Result<Option<String>, Status>;
}

#[async_trait::async_trait]
impl RepoApi for AuthedManagerClient {
    async fn job(&self, job: &str) -> Result<Option<JobView>, Status> {
        let info = match self
            .clone()
            .get_job_info(GetJobInfoRequest {
                name: job.to_string(),
            })
            .await
        {
            Ok(reply) => reply.into_inner(),
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => return Err(status),
        };
        let version = self
            .clone()
            .list_job_spec_history(ListJobSpecHistoryRequest {
                name: job.to_string(),
            })
            .await?
            .into_inner()
            .items
            .into_iter()
            .next()
            .map(|item| item.config_id);
        Ok(Some(JobView {
            status: info.job.map(|job| job.status).unwrap_or_default(),
            stamp: info
                .managed_by
                .map(|managed| managed.object)
                .filter(|object| !object.is_empty()),
            version,
        }))
    }

    async fn plan(&self, spec: &Value, namespace: &str) -> Result<(), Status> {
        self.clone()
            .plan_job(PlanJobRequest {
                parsed_jobspec: spec.to_string(),
                namespace: Some(namespace.to_string()),
                allow_unknown_fields: false,
            })
            .await?;
        Ok(())
    }

    async fn submit(&self, job: &RenderedJob, namespace: &str) -> Result<String, Status> {
        self.clone()
            .run_job(RunJobRequest {
                parsed_jobspec: job.spec.to_string(),
                submissions: job.submissions.clone(),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            })
            .await?;
        // RunJob does not say which version it produced (or matched): the
        // latest one is it.
        Ok(self
            .clone()
            .list_job_spec_history(ListJobSpecHistoryRequest {
                name: job_id(namespace, &job.name),
            })
            .await?
            .into_inner()
            .items
            .into_iter()
            .next()
            .map(|item| item.config_id)
            .unwrap_or_default())
    }

    async fn stop(&self, job: &str) -> Result<(), Status> {
        self.clone()
            .stop_job(StopJobRequest {
                name: job.to_string(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    async fn latest_rollout(&self, job: &str) -> Result<Option<Rollout>, Status> {
        Ok(self
            .clone()
            .list_rollouts(ListRolloutsRequest {
                job_id: Some(job.to_string()),
                active_only: false,
                ..Default::default()
            })
            .await?
            .into_inner()
            .rollouts
            .into_iter()
            .max_by_key(|rollout| rollout.id))
    }

    async fn read_kv(&self, key: &str) -> Result<Option<String>, Status> {
        match self
            .clone()
            .get_kv(GetKvRequest {
                key: key.to_string(),
                metadata_only: false,
            })
            .await
        {
            Ok(reply) => Ok(reply.into_inner().entry.map(|entry| entry.value)),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }
}

/// The Repo a sync is for.
#[derive(Debug, Clone, PartialEq)]
pub struct RepoRef {
    pub namespace: String,
    pub name: String,
    pub uid: u64,
    /// What jobs written for it are stamped with
    /// (`gitops/Repo/<namespace>/<name>#<uid>`).
    pub stamp: String,
}

impl RepoRef {
    pub fn new(namespace: &str, name: &str, uid: u64) -> RepoRef {
        RepoRef {
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid,
            stamp: format!("{}/{}/{namespace}/{name}#{uid}", super::APP, super::KIND),
        }
    }

    pub fn from_object(object: &crate::structs::AppObject) -> RepoRef {
        RepoRef::new(&object.namespace, &object.name, object.uid)
    }

    /// `team-a/storefront`.
    pub fn id(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    fn job_id(&self, job: &str) -> String {
        job_id(&self.namespace, job)
    }

    /// Whether `stamp` is this Repo's: its own, or an earlier Repo of the
    /// same namespace and name (deleted with `--now` and created again).
    fn owns_stamp(&self, stamp: &str) -> bool {
        if stamp == self.stamp {
            return true;
        }
        ActingFor::parse(&format!("{stamp}@0")).is_ok_and(|actor| {
            actor.app == super::APP
                && actor.kind == super::KIND
                && actor.namespace == self.namespace
                && actor.name == self.name
        })
    }
}

/// Where a Repo stands after a sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// Every job deployed: `Ready=true`.
    Ready,
    /// Applied, and rolling out: `Ready=unknown`, which the object reads
    /// as `Pending` and `--wait` waits through.
    Progressing,
    /// Something is wrong (a fetch, a render, a refusal, a failed
    /// rollout): `Ready=false`.
    NotReady,
    /// The spec cannot work as written: `Accepted=false`.
    Rejected,
}

/// What a sync concluded: the Repo's main condition and when to look again.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub health: Health,
    pub reason: String,
    pub message: String,
    pub requeue: Duration,
}

impl Verdict {
    pub fn new(
        health: Health,
        reason: &str,
        message: impl Into<String>,
        requeue: Duration,
    ) -> Verdict {
        Verdict {
            health,
            reason: reason.into(),
            message: message.into(),
            requeue,
        }
    }

    fn ready(reason: &str, message: impl Into<String>, requeue: Duration) -> Verdict {
        Verdict::new(Health::Ready, reason, message, requeue)
    }

    fn progressing(reason: &str, message: impl Into<String>, requeue: Duration) -> Verdict {
        Verdict::new(Health::Progressing, reason, message, requeue)
    }

    fn not_ready(reason: &str, message: impl Into<String>, requeue: Duration) -> Verdict {
        Verdict::new(Health::NotReady, reason, message, requeue)
    }

    fn rejected(reason: &str, message: impl Into<String>, requeue: Duration) -> Verdict {
        Verdict::new(Health::Rejected, reason, message, requeue)
    }

    /// A short label for people: `Ready`, `Progressing`, `NotReady:
    /// <reason>`, or `Rejected: <reason>`.
    pub fn label(&self) -> String {
        match self.health {
            Health::Ready => "Ready".to_string(),
            Health::Progressing => "Progressing".to_string(),
            Health::NotReady => format!("NotReady: {}", self.reason),
            Health::Rejected => format!("Rejected: {}", self.reason),
        }
    }

    /// The outcome the reconcile loop writes: the conditions, `status` as
    /// the fields, and the requeue.
    pub fn outcome(&self, status: &RepoStatus) -> ObjectOutcome {
        let outcome = match self.health {
            Health::Ready => ObjectOutcome::ready().because(&self.reason, &self.message),
            Health::Progressing => ObjectOutcome::progressing(&self.reason, &self.message),
            Health::NotReady => ObjectOutcome::not_ready(&self.reason, &self.message),
            Health::Rejected => ObjectOutcome::rejected(&self.reason, &self.message),
        };
        outcome
            .with_fields(serde_json::to_value(status).unwrap_or_else(|_| json!({})))
            .requeue_after(self.requeue)
    }
}

/// What a sync produced: the verdict, and the status to report.
#[derive(Debug, Clone, PartialEq)]
pub struct Synced {
    pub verdict: Verdict,
    pub status: RepoStatus,
}

/// What one job of a commit needs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Decision {
    /// Submit it, and why: `created`, `recreated`, `adopted`, `updated`,
    /// `restored` (someone else's version replaced it).
    Submit(&'static str),
    /// It is as the inventory says (or stopped by hand): leave it.
    Keep,
    /// Another object's version: who.
    Conflict(String),
}

/// Decide what `job` needs, from its inventory entry and the job as it is.
pub(crate) fn decide(
    job: &RenderedJob,
    entry: Option<&InventoryEntry>,
    observed: Option<&JobView>,
    repo: &RepoRef,
) -> Decision {
    let Some(observed) = observed else {
        return Decision::Submit(if entry.is_some() {
            "recreated"
        } else {
            "created"
        });
    };
    if let Some(other) = observed
        .stamp
        .as_deref()
        .filter(|stamp| !repo.owns_stamp(stamp))
    {
        return Decision::Conflict(object_stamp_label(other));
    }
    let Some(entry) = entry else {
        return Decision::Submit("adopted");
    };
    if entry.digest != job.digest {
        return Decision::Submit("updated");
    }
    // `legion job stop` is the responder's tool: a stopped job stays
    // stopped until its file changes (gitops plan calls entry 3).
    if observed.status == "stopped" {
        return Decision::Keep;
    }
    match (&observed.version, &entry.version) {
        (Some(now), Some(ours)) if now != ours => Decision::Submit("restored"),
        _ => Decision::Keep,
    }
}

/// Whether the job `observed` is still the one `entry` recorded: stamped
/// with the Repo, or (ACLs off, nothing stamped) at the version gitops
/// submitted.
fn still_ours(observed: &JobView, entry: &InventoryEntry, repo: &RepoRef) -> bool {
    match &observed.stamp {
        Some(stamp) => repo.owns_stamp(stamp),
        None => entry.version.is_some() && observed.version.as_deref() == entry.version.as_deref(),
    }
}

/// Check a commit's jobs against the Repo: its namespace, `spec.jobs`, one
/// definition per job, and the per-Repo limit.
pub(crate) fn check_jobs(
    jobs: &[RenderedJob],
    spec: &RepoSpec,
    namespace: &str,
) -> Result<(), String> {
    if jobs.len() > MAX_JOBS {
        return Err(format!(
            "{} jobs; a Repo deploys at most {MAX_JOBS} (split the repository's jobs across Repos with different paths)",
            jobs.len()
        ));
    }
    let mut problems = Vec::new();
    let mut seen: HashMap<&str, &str> = HashMap::new();
    for job in jobs {
        if let Some(other) = job.namespace.as_deref().filter(|other| *other != namespace) {
            problems.push(format!(
                "{} deploys {} into namespace {other}, and this Repo deploys only into its own \
                 ({namespace}): write a Repo in {other} for it",
                job.file, job.name
            ));
        }
        if let Some(listed) = &spec.jobs {
            if !listed.iter().any(|name| name == &job.name) {
                problems.push(format!(
                    "{} defines job {}, which spec.jobs does not list",
                    job.file, job.name
                ));
            }
        }
        if let Some(first) = seen.insert(&job.name, &job.file) {
            problems.push(format!(
                "job {} is defined twice, in {first} and {}",
                job.name, job.file
            ));
        }
        problems.extend(beyond_authority(job, spec));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("; "))
    }
}

/// What a job asks for that a Repo's authority can never cover, found
/// before anything is submitted: `PlanJob` is a read and does not check
/// the grants a submit claims, so the preflight cannot see these, and the
/// submit would refuse them halfway through a commit.
///
/// - A host-level task (an `exec` task, ...) needs `hostLevel`, which asks
///   the Repo's writer for host-level `job:write`.
/// - A priority band other than `normal`: a Repo's authority records the
///   `normal` band only.
///
/// A jobspec that does not parse here is left to the manager, whose
/// preflight names the problem.
fn beyond_authority(job: &RenderedJob, spec: &RepoSpec) -> Vec<String> {
    let Ok((config, _)) = crate::structs::JobConfig::parse_submitted_str(
        &job.spec.to_string(),
        crate::structs::spec_fields::UnknownFieldMode::Allow,
        crate::structs::spec_fields::ExtensionCheck::Offline,
    ) else {
        return Vec::new();
    };
    let mut problems = Vec::new();
    if !spec.host_level {
        if let Some(reason) = config
            .tasks
            .iter()
            .find_map(crate::task_plugins::task_host_level_reason)
        {
            problems.push(format!(
                "{} defines job {}, which has a host-level task ({reason}): set hostLevel = \
                 true on the Repo (writing it then needs job:write with host = true)",
                job.file, job.name
            ));
        }
    }
    if config.priority != crate::structs::Band::default() {
        problems.push(format!(
            "{} gives job {} the {} priority band, and a Repo deploys at the normal band only",
            job.file,
            job.name,
            config.priority.as_str()
        ));
    }
    problems
}

/// Parse a Repo's interval.
pub fn parse_interval(text: &str) -> Result<Duration, String> {
    let interval = humantime::parse_duration(text.trim())
        .map_err(|err| format!("interval {text:?} is not a duration (\"1m\", \"30s\"): {err}"))?;
    if interval < MIN_INTERVAL {
        return Err(format!(
            "interval {text:?} is below the minimum of {}s",
            MIN_INTERVAL.as_secs()
        ));
    }
    Ok(interval)
}

/// One commit, fetched and rendered.
#[derive(Debug, Clone)]
pub struct Commit {
    pub commit: String,
    pub subject: String,
    pub jobs: Vec<RenderedJob>,
}

/// Why a commit could not be had.
#[derive(Debug, Clone, PartialEq)]
enum CommitError {
    /// git could not fetch it: retried next sync.
    Fetch(String),
    /// It fetched, and does not render: kept until the commit changes.
    Render(String),
}

/// The render kept for a Repo: for which URL, resolved commit, and path,
/// and what came of it (a render error is kept too; a fetch error is not,
/// so it is retried).
struct CachedRender {
    key: (String, String, String),
    result: Result<Arc<Commit>, CommitError>,
}

/// The sync engine: one per app process.
pub struct Syncer {
    renderer: Renderer,
    cache_dir: PathBuf,
    allow_file_urls: bool,
    renders: tokio::sync::Mutex<HashMap<u64, CachedRender>>,
}

impl Syncer {
    pub fn new(renderer: Renderer, cache_dir: PathBuf, allow_file_urls: bool) -> Syncer {
        Syncer {
            renderer,
            cache_dir,
            allow_file_urls,
            renders: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn allow_file_urls(&self) -> bool {
        self.allow_file_urls
    }

    /// The global git config `file://` remotes are read with, written once
    /// into the cache directory.
    fn file_remote_config(&self) -> R<PathBuf> {
        let path = self.cache_dir.join("gitconfig");
        if !path.exists() {
            std::fs::create_dir_all(&self.cache_dir)
                .with_context(|| format!("failed to create {}", self.cache_dir.display()))?;
            std::fs::write(&path, git::FILE_REMOTE_CONFIG)
                .with_context(|| format!("failed to write {}", path.display()))?;
        }
        Ok(path)
    }

    /// Drop what is kept for a Repo that is gone.
    pub async fn forget(&self, uid: u64) {
        self.renders.lock().await.remove(&uid);
        let _ = std::fs::remove_dir_all(self.cache_dir.join(uid.to_string()));
    }

    /// The commit `resolved` of `remote`, fetched and rendered, or the
    /// render kept from an earlier sync of the same commit.
    async fn commit(
        &self,
        uid: u64,
        remote: &Remote,
        resolved: &str,
        refname: &str,
        path: &str,
    ) -> Result<Arc<Commit>, CommitError> {
        let key = (
            remote.url().to_string(),
            resolved.to_string(),
            path.to_string(),
        );
        if let Some(cached) = self.renders.lock().await.get(&uid) {
            if cached.key == key {
                return cached.result.clone();
            }
        }
        let dir = self.cache_dir.join(uid.to_string());
        let fetched = remote.fetch(refname, &dir).await;
        let result = match fetched {
            Err(err) => {
                let _ = std::fs::remove_dir_all(&dir);
                return Err(CommitError::Fetch(format!("{err:#}")));
            }
            Ok(fetched) => render_tree(&self.renderer, &dir, path)
                .await
                .map(|jobs| {
                    Arc::new(Commit {
                        commit: fetched.commit.clone(),
                        subject: fetched.subject.clone(),
                        jobs,
                    })
                })
                .map_err(|err| {
                    CommitError::Render(format!("{}: {err:#}", git::short(&fetched.commit)))
                }),
        };
        // The render holds everything a sync needs; the checkout can go.
        let _ = std::fs::remove_dir_all(&dir);
        self.renders.lock().await.insert(
            uid,
            CachedRender {
                key,
                result: result.clone(),
            },
        );
        result
    }

    /// Sync `repo` once. `previous` is the status it reported last (its
    /// inventory); `request` is the `gitops/sync` label's value, echoed in
    /// the status so `legion gitops sync` knows its request was handled.
    #[instrument(skip_all, fields(repo = %repo.id(), uid = repo.uid))]
    pub async fn sync(
        &self,
        api: &dyn RepoApi,
        repo: &RepoRef,
        spec: &RepoSpec,
        previous: &RepoStatus,
        request: Option<&str>,
    ) -> Synced {
        let mut status = previous.clone();
        if let Some(token) = request {
            status.sync_request = Some(token.to_string());
        }
        let done = |verdict: Verdict, status: RepoStatus| Synced { verdict, status };
        let retry = Duration::from_secs(60);
        let interval = match parse_interval(&spec.interval) {
            Ok(interval) => interval,
            Err(message) => {
                return done(Verdict::rejected("InvalidInterval", message, retry), status)
            }
        };
        let scheme =
            match git::check_url(&spec.url, spec.credentials.is_some(), self.allow_file_urls) {
                Ok(scheme) => scheme,
                Err(refusal) => {
                    return done(
                        Verdict::rejected(refusal.reason, refusal.message, interval),
                        status,
                    )
                }
            };
        let running = match &status.short {
            Some(short) => format!("jobs stay at {short}"),
            None => "nothing is deployed".to_string(),
        };
        if spec.suspend {
            return done(
                Verdict::not_ready(
                    "Suspended",
                    format!("syncing is suspended (spec.suspend); {running}"),
                    interval,
                ),
                status,
            );
        }

        // The token, read as the Repo: its writer was checked for kv:read
        // on the key.
        let credential = match &spec.credentials {
            None => None,
            Some(credentials) => match api.read_kv(&credentials.kv).await {
                Ok(Some(token)) if !token.trim().is_empty() => {
                    Some((credentials.username.clone(), token.trim().to_string()))
                }
                Ok(_) => {
                    return done(
                        Verdict::not_ready(
                            "CredentialsMissing",
                            format!(
                                "the KV key {} that should hold the repository's token is \
                                 missing or empty; {running}",
                                credentials.kv
                            ),
                            interval,
                        ),
                        status,
                    )
                }
                Err(err) => {
                    return done(
                        Verdict::not_ready(
                            "CredentialsUnavailable",
                            format!(
                                "reading the token at {}: {}; {running}",
                                credentials.kv,
                                err.message()
                            ),
                            interval,
                        ),
                        status,
                    )
                }
            },
        };
        let mut remote = Remote::new(&spec.url, &scheme, credential);
        if scheme == "file" {
            match self.file_remote_config() {
                Ok(path) => remote = remote.with_global_config(path),
                Err(err) => {
                    return done(
                        Verdict::not_ready("FetchFailed", format!("{err:#}; {running}"), interval),
                        status,
                    )
                }
            }
        }

        // 1. Resolve.
        let (resolved, refname) = match remote.resolve(&spec.reference).await {
            Ok(found) => found,
            Err(err) => {
                return done(
                    Verdict::not_ready("FetchFailed", format!("{err:#}; {running}"), interval),
                    status,
                )
            }
        };

        // 2. Fetch and render.
        let commit = match self
            .commit(repo.uid, &remote, &resolved, &refname, &spec.path)
            .await
        {
            Ok(commit) => commit,
            Err(failure) => {
                status.latest = Some(resolved.clone());
                let (reason, message) = match failure {
                    CommitError::Fetch(message) => ("FetchFailed", message),
                    CommitError::Render(message) => ("RenderFailed", message),
                };
                return done(
                    Verdict::not_ready(reason, format!("{message}; {running}"), interval),
                    status,
                );
            }
        };
        status.latest = Some(commit.commit.clone());
        let at = git::short(&commit.commit);
        if let Err(problems) = check_jobs(&commit.jobs, spec, &repo.namespace) {
            return done(
                Verdict::not_ready(
                    "RenderFailed",
                    format!("{at}: {problems}; {running}"),
                    interval,
                ),
                status,
            );
        }
        if commit.jobs.is_empty() && !previous.jobs.is_empty() {
            return done(
                Verdict::not_ready(
                    "RenderFailed",
                    format!(
                        "{at} has no jobs in {:?}, where this Repo deployed {}; a commit that \
                         removes every job is taken for a mistake (a moved directory, a wrong \
                         path) and not applied: delete the Repo to stop its jobs; {running}",
                        spec.path,
                        previous.jobs.len()
                    ),
                    interval,
                ),
                status,
            );
        }

        // 3. Decide.
        let mut plans: Vec<(&RenderedJob, Option<JobView>, Decision)> = Vec::new();
        let mut conflicts = Vec::new();
        for job in &commit.jobs {
            let observed = match api.job(&repo.job_id(&job.name)).await {
                Ok(observed) => observed,
                Err(err) => {
                    return done(
                        Verdict::not_ready(
                            "ApplyFailed",
                            format!(
                                "{at}: reading job {}: {}; {running}",
                                job.name,
                                err.message()
                            ),
                            interval,
                        ),
                        status,
                    )
                }
            };
            let entry = previous.jobs.iter().find(|entry| entry.name == job.name);
            match decide(job, entry, observed.as_ref(), repo) {
                Decision::Conflict(owner) => conflicts.push(format!(
                    "job {} is deployed by {owner}, and a job belongs to one Repo",
                    job.name
                )),
                decision => plans.push((job, observed, decision)),
            }
        }
        if !conflicts.is_empty() {
            return done(
                Verdict::not_ready(
                    "ApplyFailed",
                    format!(
                        "{at}: nothing deployed: {}; {running}",
                        conflicts.join("; ")
                    ),
                    interval,
                ),
                status,
            );
        }

        // 4. Preflight.
        let mut refused = Vec::new();
        for (job, _, decision) in &plans {
            if matches!(decision, Decision::Submit(_)) {
                if let Err(err) = api.plan(&job.spec, &repo.namespace).await {
                    refused.push(format!("{}: {}", job.name, err.message()));
                }
            }
        }
        if !refused.is_empty() {
            return done(
                Verdict::not_ready(
                    "ApplyFailed",
                    format!("{at}: nothing deployed: {}; {running}", refused.join("; ")),
                    interval,
                ),
                status,
            );
        }

        // 5. Apply.
        let mut inventory: Vec<InventoryEntry> = Vec::new();
        let mut submitted: HashSet<String> = HashSet::new();
        let mut apply_failed: Option<String> = None;
        for (job, _, decision) in &plans {
            let entry = previous.jobs.iter().find(|entry| entry.name == job.name);
            match decision {
                Decision::Submit(why) if apply_failed.is_none() => {
                    match api.submit(job, &repo.namespace).await {
                        Ok(version) => {
                            info!(job = %job.name, why, commit = %at, "Deployed");
                            submitted.insert(job.name.clone());
                            inventory.push(InventoryEntry {
                                name: job.name.clone(),
                                file: job.file.clone(),
                                digest: job.digest.clone(),
                                version: Some(version).filter(|v| !v.is_empty()),
                                state: JobState::Progressing,
                                detail: None,
                            });
                        }
                        Err(err) => {
                            apply_failed = Some(format!("{}: {}", job.name, err.message()));
                            inventory.extend(entry.cloned());
                        }
                    }
                }
                Decision::Submit(_) => inventory.extend(entry.cloned()),
                Decision::Keep | Decision::Conflict(_) => {
                    if let Some(entry) = entry {
                        inventory.push(InventoryEntry {
                            file: job.file.clone(),
                            ..entry.clone()
                        });
                    }
                }
            }
        }
        let rendered: HashSet<&str> = commit.jobs.iter().map(|job| job.name.as_str()).collect();
        let gone: Vec<&InventoryEntry> = previous
            .jobs
            .iter()
            .filter(|entry| !rendered.contains(entry.name.as_str()))
            .collect();
        if let Some(failure) = apply_failed {
            // Keep what left git in the inventory: the next sync prunes it.
            inventory.extend(gone.into_iter().cloned());
            status.deployed = inventory.len() as u32;
            status.jobs = inventory;
            let applied = if submitted.is_empty() {
                "nothing was deployed".to_string()
            } else {
                let mut names: Vec<&str> = submitted.iter().map(String::as_str).collect();
                names.sort_unstable();
                format!("{} deployed before it", names.join(", "))
            };
            return done(
                Verdict::not_ready(
                    "ApplyFailed",
                    format!("{at}: {failure}; {applied}"),
                    interval,
                ),
                status,
            );
        }

        // 6. Prune.
        let mut prune_failures = Vec::new();
        for entry in gone {
            if !spec.prune {
                continue;
            }
            match self.prune(api, repo, entry).await {
                Ok(()) => info!(job = %entry.name, "Stopped: its file left the repository"),
                Err(err) => {
                    prune_failures.push(format!("{}: {err}", entry.name));
                    inventory.push(InventoryEntry {
                        detail: Some(format!("left the repository; stopping it failed: {err}")),
                        ..entry.clone()
                    });
                }
            }
        }

        // The commit is applied.
        if status.commit.as_deref() != Some(commit.commit.as_str()) || !submitted.is_empty() {
            status.synced_at = Some(now_rfc3339());
        }
        status.commit = Some(commit.commit.clone());
        status.short = Some(at.clone());
        status.subject = Some(commit.subject.clone()).filter(|s| !s.is_empty());

        // 7. Health.
        for entry in inventory.iter_mut() {
            if !rendered.contains(entry.name.as_str()) {
                continue;
            }
            let fresh = submitted.contains(&entry.name);
            let observed = plans
                .iter()
                .find(|(job, _, _)| job.name == entry.name)
                .and_then(|(_, observed, _)| observed.clone());
            let (state, detail) = self
                .health(api, repo, entry, if fresh { None } else { observed })
                .await;
            entry.state = state;
            entry.detail = detail;
        }
        status.deployed = inventory
            .iter()
            .filter(|entry| rendered.contains(entry.name.as_str()))
            .count() as u32;
        status.jobs = inventory;
        let verdict = verdict_for(&status, &at, &prune_failures, interval);
        done(verdict, status)
    }

    /// Stop a job whose file left git, when it is still the Repo's.
    async fn prune(
        &self,
        api: &dyn RepoApi,
        repo: &RepoRef,
        entry: &InventoryEntry,
    ) -> Result<(), String> {
        let id = repo.job_id(&entry.name);
        let observed = api
            .job(&id)
            .await
            .map_err(|err| err.message().to_string())?;
        let Some(observed) = observed else {
            return Ok(());
        };
        if !still_ours(&observed, entry, repo) {
            info!(job = %entry.name, "Not stopping a job that left the repository: someone else's version replaced it");
            return Ok(());
        }
        if observed.status == "stopped" {
            return Ok(());
        }
        api.stop(&id).await.map_err(|err| err.message().to_string())
    }

    /// A job's state: its latest rollout when that rollout is for the
    /// version gitops submitted, else its status. `observed` is the job as
    /// read this sync (`None`: read it again, after a submit).
    async fn health(
        &self,
        api: &dyn RepoApi,
        repo: &RepoRef,
        entry: &InventoryEntry,
        observed: Option<JobView>,
    ) -> (JobState, Option<String>) {
        let id = repo.job_id(&entry.name);
        let observed = match observed {
            Some(observed) => Some(observed),
            None => match api.job(&id).await {
                Ok(observed) => observed,
                Err(err) => {
                    return (
                        JobState::Pending,
                        Some(format!("could not read the job: {}", err.message())),
                    )
                }
            },
        };
        let Some(observed) = observed else {
            return (JobState::Pending, Some("not found".into()));
        };
        match observed.status.as_str() {
            "stopped" => {
                return (
                    JobState::Stopped,
                    Some("stopped by hand: gitops leaves it stopped until its file changes".into()),
                )
            }
            "failed" => {
                return (
                    JobState::Failed,
                    Some(format!("the job failed (`legion job status {id}`)")),
                )
            }
            _ => {}
        }
        let settled = entry.state == JobState::Deployed;
        if !settled {
            if let Ok(Some(rollout)) = api.latest_rollout(&id).await {
                if entry.version.as_deref() == Some(rollout.to_job_config_id.as_str()) {
                    return rollout_state(&rollout);
                }
            }
        }
        match observed.status.as_str() {
            "running" | "complete" | "scheduled" | "idle" | "suspended" => {
                (JobState::Deployed, None)
            }
            "blocked" => (
                JobState::Pending,
                Some("waiting for room to place it (`legion job status`)".into()),
            ),
            "pending" => (JobState::Pending, Some("waiting to be placed".into())),
            other => (JobState::Progressing, Some(other.to_string())),
        }
    }

    /// Clean up after a deleted Repo: with `prune`, stop every job it
    /// deployed that is still its own. An error retries until the kind's
    /// cleanup window ends.
    #[instrument(skip_all, fields(repo = %repo.id(), uid = repo.uid))]
    pub async fn cleanup(
        &self,
        api: &dyn RepoApi,
        repo: &RepoRef,
        spec: &RepoSpec,
        previous: &RepoStatus,
    ) -> R<()> {
        self.forget(repo.uid).await;
        if !spec.prune {
            info!(
                jobs = previous.jobs.len(),
                "Repo deleted with prune off: its jobs keep running"
            );
            return Ok(());
        }
        let mut failures = Vec::new();
        for entry in &previous.jobs {
            if let Err(err) = self.prune(api, repo, entry).await {
                failures.push(format!("{}: {err}", entry.name));
            }
        }
        if failures.is_empty() {
            info!(
                jobs = previous.jobs.len(),
                "Repo deleted: its jobs are stopped"
            );
            Ok(())
        } else {
            bail!("stopping the Repo's jobs: {}", failures.join("; "))
        }
    }
}

/// A rollout's verdict on a job.
fn rollout_state(rollout: &Rollout) -> (JobState, Option<String>) {
    let message = Some(rollout.message.trim())
        .filter(|message| !message.is_empty())
        .map(|message| format!(": {message}"))
        .unwrap_or_default();
    match rollout.status.as_str() {
        "completed" => (JobState::Deployed, None),
        "failed" | "cancelled" | "completed_incomplete" => (
            JobState::Failed,
            Some(format!(
                "rollout {} {}{message}",
                rollout.id,
                rollout.status.replace('_', " ")
            )),
        ),
        "paused" => (
            JobState::Progressing,
            Some(format!(
                "rollout {} is paused: `legion job rollout advance` moves it on",
                rollout.id
            )),
        ),
        other => (
            JobState::Progressing,
            Some(format!("rollout {} {other}", rollout.id)),
        ),
    }
}

/// The Repo's verdict from its jobs' states.
fn verdict_for(
    status: &RepoStatus,
    at: &str,
    prune_failures: &[String],
    interval: Duration,
) -> Verdict {
    let names = |state: JobState| -> Vec<String> {
        status
            .jobs
            .iter()
            .filter(|entry| entry.state == state)
            .map(|entry| match &entry.detail {
                Some(detail) if state == JobState::Failed => format!("{} ({detail})", entry.name),
                _ => entry.name.clone(),
            })
            .collect()
    };
    let deployed = names(JobState::Deployed).len();
    let stopped = names(JobState::Stopped).len();
    let failed = names(JobState::Failed);
    if !failed.is_empty() {
        return Verdict::not_ready(
            "RolloutFailed",
            format!(
                "at {at}: {} failed; {deployed} of {} jobs deployed",
                failed.join(", "),
                status.deployed
            ),
            interval,
        );
    }
    let mut moving = names(JobState::Progressing);
    moving.extend(names(JobState::Pending));
    if !moving.is_empty() {
        return Verdict::progressing(
            "Progressing",
            format!(
                "at {at}: rolling out {} ({deployed} of {} jobs deployed)",
                moving.join(", "),
                status.deployed
            ),
            PROGRESS_REQUEUE.min(interval),
        );
    }
    if !prune_failures.is_empty() {
        return Verdict::not_ready(
            "PruneFailed",
            format!(
                "at {at}: could not stop jobs that left the repository: {}",
                prune_failures.join("; ")
            ),
            interval,
        );
    }
    let mut message = match status.deployed {
        0 => format!("at {at}: no jobs"),
        1 => format!("at {at}: 1 job deployed"),
        n => format!("at {at}: {n} jobs deployed"),
    };
    if stopped > 0 {
        message = format!(
            "at {at}: {deployed} of {} jobs deployed, {stopped} stopped by hand",
            status.deployed
        );
    }
    Verdict::ready("Synced", message, interval)
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(name: &str, digest: &str) -> RenderedJob {
        RenderedJob {
            file: format!("{name}.ncl"),
            name: name.to_string(),
            namespace: None,
            spec: json!({ "name": name }),
            digest: digest.to_string(),
            submissions: Vec::new(),
        }
    }

    fn entry(name: &str, digest: &str, version: &str) -> InventoryEntry {
        InventoryEntry {
            name: name.to_string(),
            file: format!("{name}.ncl"),
            digest: digest.to_string(),
            version: Some(version.to_string()),
            state: JobState::Deployed,
            detail: None,
        }
    }

    fn view(status: &str, stamp: Option<&str>, version: &str) -> JobView {
        JobView {
            status: status.to_string(),
            stamp: stamp.map(str::to_string),
            version: Some(version.to_string()),
        }
    }

    #[test]
    fn decisions_follow_the_inventory_and_the_job_test() {
        let repo = RepoRef::new("team-a", "web", 7);
        let ours = repo.stamp.clone();
        assert_eq!(ours, "gitops/Repo/team-a/web#7");
        let web = job("web", "d1");
        let recorded = entry("web", "d1", "v1");
        // Missing.
        assert_eq!(decide(&web, None, None, &repo), Decision::Submit("created"));
        assert_eq!(
            decide(&web, Some(&recorded), None, &repo),
            Decision::Submit("recreated")
        );
        // As recorded.
        let current = view("running", Some(&ours), "v1");
        assert_eq!(
            decide(&web, Some(&recorded), Some(&current), &repo),
            Decision::Keep
        );
        // Its file changed.
        let changed = job("web", "d2");
        assert_eq!(
            decide(&changed, Some(&recorded), Some(&current), &repo),
            Decision::Submit("updated")
        );
        // A person submitted over it (no stamp, a new version).
        let replaced = view("running", None, "v2");
        assert_eq!(
            decide(&web, Some(&recorded), Some(&replaced), &repo),
            Decision::Submit("restored")
        );
        // Stopped by hand: left alone until its file changes.
        let stopped = view("stopped", Some(&ours), "v1");
        assert_eq!(
            decide(&web, Some(&recorded), Some(&stopped), &repo),
            Decision::Keep
        );
        assert_eq!(
            decide(&changed, Some(&recorded), Some(&stopped), &repo),
            Decision::Submit("updated")
        );
        // Not in the inventory: adopted, unless another Repo's.
        let unmanaged = view("running", None, "v9");
        assert_eq!(
            decide(&web, None, Some(&unmanaged), &repo),
            Decision::Submit("adopted")
        );
        let other = view("running", Some("gitops/Repo/team-a/api#9"), "v9");
        assert_eq!(
            decide(&web, None, Some(&other), &repo),
            Decision::Conflict("gitops for Repo team-a/api".into())
        );
        // An earlier Repo of the same name (deleted with --now, created
        // again) is this one.
        let earlier = view("running", Some("gitops/Repo/team-a/web#3"), "v1");
        assert_eq!(
            decide(&web, Some(&recorded), Some(&earlier), &repo),
            Decision::Keep
        );
    }

    #[test]
    fn jobs_are_checked_against_the_repo_test() {
        let spec: RepoSpec =
            serde_json::from_value(json!({ "url": "https://x/y", "jobs": ["web", "api"] }))
                .unwrap();
        let mut foreign = job("api", "d");
        foreign.namespace = Some("prod".into());
        let err = check_jobs(
            &[
                job("web", "d"),
                job("worker", "d"),
                foreign,
                job("web", "e"),
            ],
            &spec,
            "team-a",
        )
        .unwrap_err();
        assert!(
            err.contains("worker.ncl defines job worker, which spec.jobs does not list"),
            "{err}"
        );
        assert!(err.contains("into namespace prod"), "{err}");
        assert!(err.contains("job web is defined twice"), "{err}");
        let mut same = job("api", "d");
        same.namespace = Some("team-a".into());
        assert!(check_jobs(&[job("web", "d"), same], &spec, "team-a").is_ok());
    }

    #[test]
    fn what_no_authority_covers_is_caught_before_submitting_test() {
        let spec: RepoSpec = serde_json::from_value(json!({ "url": "https://x/y" })).unwrap();
        let mut runner = job("runner", "d");
        runner.spec = json!({ "name": "runner", "task": { "name": "main", "runtime": "exec",
                              "config": { "command": ["true"] } } });
        let err = check_jobs(&[runner.clone()], &spec, "team-a").unwrap_err();
        assert!(
            err.contains("host-level task") && err.contains("hostLevel"),
            "{err}"
        );
        let host: RepoSpec =
            serde_json::from_value(json!({ "url": "https://x/y", "hostLevel": true })).unwrap();
        assert!(check_jobs(&[runner], &host, "team-a").is_ok());
        let mut urgent = job("urgent", "d");
        urgent.spec = json!({ "name": "urgent", "priority": "critical",
                              "task": { "name": "main", "runtime": "container",
                                        "config": { "image": "alpine" } } });
        let err = check_jobs(&[urgent], &spec, "team-a").unwrap_err();
        assert!(err.contains("critical priority band"), "{err}");
    }

    #[test]
    fn intervals_have_a_floor_test() {
        assert_eq!(parse_interval("1m").unwrap(), Duration::from_secs(60));
        assert!(parse_interval("5s").unwrap_err().contains("minimum"));
        assert!(parse_interval("soon")
            .unwrap_err()
            .contains("not a duration"));
    }

    #[test]
    fn rollouts_decide_a_jobs_state_test() {
        let rollout = |status: &str| Rollout {
            id: 4,
            status: status.into(),
            message: "health checks failed".into(),
            ..Default::default()
        };
        assert_eq!(rollout_state(&rollout("completed")).0, JobState::Deployed);
        let (state, detail) = rollout_state(&rollout("failed"));
        assert_eq!(state, JobState::Failed);
        assert_eq!(detail.unwrap(), "rollout 4 failed: health checks failed");
        assert_eq!(rollout_state(&rollout("paused")).0, JobState::Progressing);
        assert_eq!(rollout_state(&rollout("running")).0, JobState::Progressing);
    }

    #[test]
    fn verdicts_summarize_the_jobs_test() {
        let mut status = RepoStatus {
            deployed: 2,
            jobs: vec![entry("web", "d", "v"), entry("api", "d", "v")],
            ..Default::default()
        };
        let verdict = verdict_for(&status, "3f9a1c2", &[], Duration::from_secs(60));
        assert_eq!(verdict.label(), "Ready");
        assert_eq!(verdict.message, "at 3f9a1c2: 2 jobs deployed");
        status.jobs[1].state = JobState::Stopped;
        let verdict = verdict_for(&status, "3f9a1c2", &[], Duration::from_secs(60));
        assert_eq!(
            verdict.message,
            "at 3f9a1c2: 1 of 2 jobs deployed, 1 stopped by hand"
        );
        status.jobs[1].state = JobState::Progressing;
        let verdict = verdict_for(&status, "3f9a1c2", &[], Duration::from_secs(60));
        assert_eq!(verdict.label(), "Progressing");
        assert_eq!(verdict.health, Health::Progressing);
        assert_eq!(verdict.requeue, PROGRESS_REQUEUE);
        status.jobs[0].state = JobState::Failed;
        status.jobs[0].detail = Some("rollout 4 failed".into());
        let verdict = verdict_for(&status, "3f9a1c2", &[], Duration::from_secs(60));
        assert_eq!(verdict.label(), "NotReady: RolloutFailed");
        assert!(
            verdict.message.contains("web (rollout 4 failed) failed"),
            "{}",
            verdict.message
        );
    }
}
